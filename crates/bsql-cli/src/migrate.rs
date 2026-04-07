use bsql_driver_postgres::{Config, Connection};

use crate::cache::CachedQuery;

#[derive(Debug)]
pub struct MigrationCheckResult {
    pub total_queries: usize,
    pub passed: usize,
    pub failed: Vec<FailedQuery>,
}

#[derive(Debug)]
pub struct FailedQuery {
    pub sql: String,
    #[allow(dead_code)]
    pub sql_hash: u64,
    pub error: String,
}

/// Connect to the database, create a shadow schema replicating `public`,
/// apply the migration SQL to the shadow, then PREPARE every cached query
/// against the post-migration schema. Returns which queries passed and
/// which broke.
pub fn check_migration(
    database_url: &str,
    migration_sql: &str,
    cached_queries: &[CachedQuery],
) -> Result<MigrationCheckResult, String> {
    let config = Config::from_url(database_url)
        .map_err(|e| format!("invalid database URL: {e}"))?;
    let mut conn = Connection::connect(&config)
        .map_err(|e| format!("connection failed: {e}"))?;

    // 1. Drop any leftover shadow schema from a previous failed run.
    conn.simple_query("DROP SCHEMA IF EXISTS __bsql_shadow CASCADE")
        .map_err(|e| format!("failed to drop stale shadow schema: {e}"))?;

    // 2. Create the shadow schema.
    conn.simple_query("CREATE SCHEMA __bsql_shadow")
        .map_err(|e| format!("failed to create shadow schema: {e}"))?;

    // 3. Clone the current public schema structure into the shadow.
    let tables = get_public_tables(&mut conn)?;
    for table in &tables {
        let sql = format!(
            "CREATE TABLE __bsql_shadow.\"{}\" (LIKE public.\"{}\" INCLUDING ALL)",
            table, table
        );
        if let Err(e) = conn.simple_query(&sql) {
            eprintln!("  warning: could not clone table {}: {}", table, e);
        }
    }

    // Also clone views so queries referencing views still validate.
    let views = get_public_views(&mut conn)?;
    for (view_name, view_def) in &views {
        // Rewrite the view definition to reference the shadow schema.
        // The stored definition uses unqualified table names, so we
        // rely on search_path to resolve them.
        let sql = format!(
            "CREATE OR REPLACE VIEW __bsql_shadow.\"{}\" AS {}",
            view_name, view_def
        );
        // Set search_path temporarily so the view body resolves to shadow tables.
        let _ = conn.simple_query("SET search_path TO __bsql_shadow, public");
        if let Err(e) = conn.simple_query(&sql) {
            eprintln!("  warning: could not clone view {}: {}", view_name, e);
        }
        let _ = conn.simple_query("SET search_path TO public");
    }

    // 4. Apply the migration to the shadow schema.
    conn.simple_query("SET search_path TO __bsql_shadow, public")
        .map_err(|e| format!("failed to set search_path: {e}"))?;
    conn.simple_query(migration_sql)
        .map_err(|e| format!("migration failed: {e}"))?;

    // 5. Validate each cached query via PREPARE.
    let mut result = MigrationCheckResult {
        total_queries: cached_queries.len(),
        passed: 0,
        failed: Vec::new(),
    };

    for query in cached_queries {
        let prepare_sql = format!("PREPARE __bsql_check AS {}", query.normalized_sql);
        match conn.simple_query(&prepare_sql) {
            Ok(_) => {
                result.passed += 1;
                let _ = conn.simple_query("DEALLOCATE __bsql_check");
            }
            Err(e) => {
                result.failed.push(FailedQuery {
                    sql: query.normalized_sql.clone(),
                    sql_hash: query.sql_hash,
                    error: e.to_string(),
                });
                // DEALLOCATE may fail if PREPARE failed — ignore.
                let _ = conn.simple_query("DEALLOCATE IF EXISTS __bsql_check");
            }
        }
    }

    // 6. Cleanup.
    conn.simple_query("SET search_path TO public")
        .map_err(|e| format!("failed to reset search_path: {e}"))?;
    conn.simple_query("DROP SCHEMA IF EXISTS __bsql_shadow CASCADE")
        .map_err(|e| format!("failed to drop shadow schema: {e}"))?;

    Ok(result)
}

fn get_public_tables(conn: &mut Connection) -> Result<Vec<String>, String> {
    let rows = conn
        .simple_query_rows(
            "SELECT tablename FROM pg_tables \
             WHERE schemaname = 'public' \
             ORDER BY tablename",
        )
        .map_err(|e| format!("failed to list tables: {e}"))?;

    Ok(rows
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect())
}

fn get_public_views(conn: &mut Connection) -> Result<Vec<(String, String)>, String> {
    let rows = conn
        .simple_query_rows(
            "SELECT viewname, definition FROM pg_views \
             WHERE schemaname = 'public' \
             ORDER BY viewname",
        )
        .map_err(|e| format!("failed to list views: {e}"))?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let mut cols = row.into_iter();
            let name = cols.next()??;
            let def = cols.next()??;
            Some((name, def))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg_url() -> Option<String> {
        std::env::var("BSQL_DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("DATABASE_URL").ok())
    }

    #[test]
    fn check_empty_migration_no_queries() {
        let Some(url) = pg_url() else { return };
        let queries: Vec<CachedQuery> = vec![];
        let result = check_migration(&url, "", &queries).unwrap();
        assert_eq!(result.total_queries, 0);
        assert_eq!(result.passed, 0);
        assert!(result.failed.is_empty());
    }

    #[test]
    fn check_invalid_url() {
        let result = check_migration("not-a-url", "", &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid database URL"));
    }

    #[test]
    fn check_unreachable_host() {
        let result =
            check_migration("postgres://user:pass@192.0.2.1:5432/db", "", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn check_noop_migration_select_one() {
        let Some(url) = pg_url() else { return };
        let queries = vec![crate::cache::CachedQuery {
            sql_hash: 1,
            normalized_sql: "SELECT 1 AS n".to_owned(),
            columns: vec![],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            bsql_version: "0.20.1".to_owned(),
        }];
        let result = check_migration(&url, "", &queries).unwrap();
        assert_eq!(result.total_queries, 1);
        assert_eq!(result.passed, 1);
        assert!(result.failed.is_empty());
    }

    #[test]
    fn check_migration_breaks_query() {
        let Some(url) = pg_url() else { return };
        // Create a table, cache a query against it, then migrate by dropping it.
        let config = Config::from_url(&url).unwrap();
        let mut conn = Connection::connect(&config).unwrap();

        // Setup: create a table in public
        let _ = conn.simple_query("DROP TABLE IF EXISTS __bsql_test_tbl");
        conn.simple_query("CREATE TABLE __bsql_test_tbl (id int PRIMARY KEY, name text)")
            .unwrap();

        let queries = vec![crate::cache::CachedQuery {
            sql_hash: 2,
            normalized_sql: "SELECT id, name FROM __bsql_test_tbl".to_owned(),
            columns: vec![],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            bsql_version: "0.20.1".to_owned(),
        }];

        // Migration drops the table
        let result =
            check_migration(&url, "DROP TABLE IF EXISTS __bsql_test_tbl", &queries).unwrap();
        assert_eq!(result.total_queries, 1);
        assert_eq!(result.failed.len(), 1);
        assert!(result.failed[0].error.contains("does not exist")
            || result.failed[0].error.contains("relation"));

        // Cleanup
        let _ = conn.simple_query("DROP TABLE IF EXISTS __bsql_test_tbl");
    }

    #[test]
    fn check_migration_rename_column_breaks_query() {
        let Some(url) = pg_url() else { return };
        let config = Config::from_url(&url).unwrap();
        let mut conn = Connection::connect(&config).unwrap();

        let _ = conn.simple_query("DROP TABLE IF EXISTS __bsql_rename_test");
        conn.simple_query("CREATE TABLE __bsql_rename_test (id int, old_col text)")
            .unwrap();

        let queries = vec![crate::cache::CachedQuery {
            sql_hash: 3,
            normalized_sql: "SELECT old_col FROM __bsql_rename_test".to_owned(),
            columns: vec![],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            bsql_version: "0.20.1".to_owned(),
        }];

        let result = check_migration(
            &url,
            "ALTER TABLE __bsql_rename_test RENAME COLUMN old_col TO new_col",
            &queries,
        )
        .unwrap();
        assert_eq!(result.failed.len(), 1);

        // Cleanup
        let _ = conn.simple_query("DROP TABLE IF EXISTS __bsql_rename_test");
    }

    #[test]
    fn check_safe_migration_add_column() {
        let Some(url) = pg_url() else { return };
        let config = Config::from_url(&url).unwrap();
        let mut conn = Connection::connect(&config).unwrap();

        let _ = conn.simple_query("DROP TABLE IF EXISTS __bsql_addcol_test");
        conn.simple_query("CREATE TABLE __bsql_addcol_test (id int)")
            .unwrap();

        let queries = vec![crate::cache::CachedQuery {
            sql_hash: 4,
            normalized_sql: "SELECT id FROM __bsql_addcol_test".to_owned(),
            columns: vec![],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            bsql_version: "0.20.1".to_owned(),
        }];

        // Adding a column should not break existing queries
        let result = check_migration(
            &url,
            "ALTER TABLE __bsql_addcol_test ADD COLUMN name text",
            &queries,
        )
        .unwrap();
        assert_eq!(result.passed, 1);
        assert!(result.failed.is_empty());

        // Cleanup
        let _ = conn.simple_query("DROP TABLE IF EXISTS __bsql_addcol_test");
    }

    #[test]
    fn shadow_schema_cleaned_up_on_success() {
        let Some(url) = pg_url() else { return };
        check_migration(&url, "", &[]).unwrap();

        // Verify shadow schema was dropped
        let config = Config::from_url(&url).unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        let rows = conn
            .simple_query_rows(
                "SELECT 1 FROM information_schema.schemata \
                 WHERE schema_name = '__bsql_shadow'",
            )
            .unwrap();
        assert!(rows.is_empty(), "shadow schema should be cleaned up");
    }
}
