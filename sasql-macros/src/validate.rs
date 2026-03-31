//! Compile-time SQL validation via PostgreSQL PREPARE.
//!
//! Connects to the database specified by `SASQL_DATABASE_URL` and validates
//! each query by preparing it. Introspects column types and nullability
//! from `pg_catalog`.

use tokio::runtime::Runtime;
use tokio_postgres::Client;

use crate::parse::{Param, ParsedQuery};

/// Metadata about a single result column, resolved from PostgreSQL.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name as returned by PostgreSQL.
    pub name: String,
    /// PostgreSQL type OID.
    pub pg_oid: u32,
    /// PostgreSQL type name (e.g. `"int4"`, `"text"`).
    pub pg_type_name: String,
    /// Whether this column can be NULL.
    pub is_nullable: bool,
    /// The Rust type string for code generation (e.g. `"i32"`, `"Option<String>"`).
    pub rust_type: String,
}

/// Result of validating a query against PostgreSQL.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Output columns (for SELECT or RETURNING queries).
    pub columns: Vec<ColumnInfo>,
    /// PostgreSQL OIDs of the expected parameter types.
    pub param_pg_oids: Vec<u32>,
}

/// Validate a parsed query against a live PostgreSQL instance.
///
/// Uses `client.prepare()` which:
/// 1. Validates SQL syntax
/// 2. Validates table/column existence
/// 3. Returns column metadata and parameter types
pub fn validate_query(
    parsed: &ParsedQuery,
    rt: &Runtime,
    client: &Client,
) -> Result<ValidationResult, String> {
    rt.block_on(validate_async(parsed, client))
}

async fn validate_async(
    parsed: &ParsedQuery,
    client: &Client,
) -> Result<ValidationResult, String> {
    // Prepare the query — this validates syntax, tables, columns, types.
    let stmt = client
        .prepare(&parsed.positional_sql)
        .await
        .map_err(|e| format_pg_error(&e, parsed))?;

    // Extract parameter type OIDs
    let param_pg_oids: Vec<u32> = stmt.params().iter().map(|t| t.oid()).collect();

    // Extract column metadata
    let mut columns = Vec::with_capacity(stmt.columns().len());
    for col in stmt.columns() {
        let pg_oid = col.type_().oid();
        let pg_type_name = col.type_().name().to_owned();
        let name = col.name().to_owned();

        // Determine nullability from pg_catalog
        let is_nullable = resolve_nullability(client, col).await;

        // Map PG OID to Rust type
        let base_rust_type = sasql_core::types::rust_type_for_oid(pg_oid).ok_or_else(|| {
            format!(
                "column \"{name}\": unsupported PostgreSQL type `{pg_type_name}` (OID {pg_oid}). \
                 Enable the appropriate feature flag or cast to a supported type."
            )
        })?;

        let rust_type = if is_nullable {
            format!("Option<{base_rust_type}>")
        } else {
            base_rust_type.to_owned()
        };

        columns.push(ColumnInfo {
            name,
            pg_oid,
            pg_type_name,
            is_nullable,
            rust_type,
        });
    }

    Ok(ValidationResult {
        columns,
        param_pg_oids,
    })
}

/// Determine whether a column is nullable by checking pg_catalog.
///
/// For columns from a real table, queries `pg_attribute.attnotnull`.
/// For computed expressions (aggregates, functions), defaults to nullable (safe).
async fn resolve_nullability(client: &Client, col: &tokio_postgres::Column) -> bool {
    // table_oid = 0 means the column is computed (not from a table).
    // Computed columns are treated as nullable — the safe default.
    let table_oid = col.table_oid();
    let col_id = col.column_id();

    // table_oid/column_id are None for computed columns (expressions, aggregates).
    let (table_oid, col_id) = match (table_oid, col_id) {
        (Some(t), Some(c)) if t != 0 && c != 0 => (t, c as i16),
        _ => return true, // computed → nullable (safe default)
    };

    // Query pg_attribute for the NOT NULL constraint
    let query = "SELECT NOT attnotnull FROM pg_attribute WHERE attrelid = $1 AND attnum = $2";
    match client.query_opt(query, &[&table_oid, &col_id]).await {
        Ok(Some(row)) => row.get::<_, bool>(0),
        _ => true, // if we can't determine, assume nullable (safe)
    }
}

/// Check that user-declared parameter types match what PostgreSQL expects.
pub fn check_param_types(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
) -> Result<(), String> {
    if parsed.params.len() != validation.param_pg_oids.len() {
        return Err(format!(
            "parameter count mismatch: query has {} parameters but PostgreSQL \
             expects {}. Check your $name: Type declarations.",
            parsed.params.len(),
            validation.param_pg_oids.len()
        ));
    }

    for (param, &pg_oid) in parsed.params.iter().zip(&validation.param_pg_oids) {
        if !sasql_core::types::is_param_compatible(&param.rust_type, pg_oid) {
            let pg_name = sasql_core::types::pg_name_for_oid(pg_oid)
                .unwrap_or("unknown");
            return Err(format!(
                "type mismatch for parameter `${}`: declared `{}` but PostgreSQL \
                 expects `{}` (OID {})",
                param.name, param.rust_type, pg_name, pg_oid
            ));
        }
    }

    Ok(())
}

/// Format a PostgreSQL error into a developer-friendly compile error message.
fn format_pg_error(e: &tokio_postgres::Error, parsed: &ParsedQuery) -> String {
    let msg = e.to_string();

    // Extract the PostgreSQL error code if available
    if let Some(db_err) = e.as_db_error() {
        let detail = db_err.detail().unwrap_or("");
        let hint = db_err.hint().unwrap_or("");
        let position = db_err.position();

        let mut out = format!("PostgreSQL error: {}", db_err.message());

        if !detail.is_empty() {
            out.push_str(&format!("\n  detail: {detail}"));
        }
        if !hint.is_empty() {
            out.push_str(&format!("\n  hint: {hint}"));
        }
        if let Some(pos) = position {
            out.push_str(&format!("\n  position: {pos:?}"));
            out.push_str(&format!("\n  SQL: {}", parsed.positional_sql));
        }

        out
    } else {
        format!("PostgreSQL error: {msg}\n  SQL: {}", parsed.positional_sql)
    }
}

/// Verify parameter declarations in the parsed query.
/// Called before connecting to PG — catches obvious errors early.
pub fn check_param_declarations(params: &[Param]) -> Result<(), String> {
    // Check for duplicate parameter names
    for (i, p) in params.iter().enumerate() {
        for other in &params[i + 1..] {
            if p.name == other.name {
                return Err(format!(
                    "duplicate parameter name `${}`. Each parameter must have a unique name.",
                    p.name
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_param_names_detected() {
        let params = vec![
            Param { name: "id".into(), rust_type: "i32".into(), position: 1 },
            Param { name: "id".into(), rust_type: "i32".into(), position: 2 },
        ];
        let result = check_param_declarations(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("duplicate parameter name"));
    }

    #[test]
    fn unique_param_names_pass() {
        let params = vec![
            Param { name: "id".into(), rust_type: "i32".into(), position: 1 },
            Param { name: "name".into(), rust_type: "&str".into(), position: 2 },
        ];
        assert!(check_param_declarations(&params).is_ok());
    }

    #[test]
    fn empty_params_pass() {
        assert!(check_param_declarations(&[]).is_ok());
    }
}
