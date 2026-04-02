//! SQLite declared-type to Rust type resolution.
//!
//! SQLite uses type affinity rules (section 3.1 of the SQLite docs) to
//! determine storage class from the declared type in CREATE TABLE. This
//! module maps those declared types to Rust types for code generation.
//!
//! Unlike PostgreSQL (which has strict OID-based typing), SQLite's type
//! system is flexible: any column can store any value. The declared type
//! is a hint, not a constraint. We use it as the best available signal
//! for generating typed Rust code.

/// Resolve a SQLite declared type to a Rust type string.
///
/// Follows SQLite's type affinity rules:
/// 1. Contains "INT" -> INTEGER affinity -> `i64`
/// 2. Contains "CHAR", "CLOB", or "TEXT" -> TEXT affinity -> `String`
/// 3. Contains "BLOB" (or no type) -> BLOB affinity -> `Vec<u8>`
/// 4. Contains "REAL", "FLOA", or "DOUB" -> REAL affinity -> `f64`
/// 5. Otherwise -> NUMERIC affinity -> `String` (safe default)
///
/// Special cases:
/// - `BOOLEAN` / `BOOL` -> `bool` (stored as INTEGER 0/1)
/// - No declared type -> `String` (text affinity, safe default)
pub fn resolve_sqlite_type(declared_type: Option<&str>) -> &'static str {
    let dt = match declared_type {
        Some(dt) if !dt.is_empty() => dt,
        _ => return "String", // no declared type -> text affinity -> String
    };

    // Uppercase for case-insensitive matching
    let upper = dt.to_ascii_uppercase();

    // Boolean check first (before INT check, since BOOL doesn't contain INT)
    if upper == "BOOLEAN" || upper == "BOOL" {
        return "bool";
    }

    // Feature-gated types: DATETIME/TIMESTAMP, DATE, TIME, UUID, DECIMAL/NUMERIC
    // Check these BEFORE the affinity rules since "DATETIME" contains "INT".
    if upper == "DATETIME" || upper == "TIMESTAMP" {
        #[cfg(feature = "time")]
        return "::time::PrimitiveDateTime";
        #[cfg(feature = "chrono")]
        return "::chrono::NaiveDateTime";
        // Without time/chrono feature, fall through to String
        #[cfg(not(any(feature = "time", feature = "chrono")))]
        return "String";
    }
    if upper == "DATE" {
        #[cfg(feature = "time")]
        return "::time::Date";
        #[cfg(feature = "chrono")]
        return "::chrono::NaiveDate";
        #[cfg(not(any(feature = "time", feature = "chrono")))]
        return "String";
    }
    if upper == "TIME" {
        #[cfg(feature = "time")]
        return "::time::Time";
        #[cfg(feature = "chrono")]
        return "::chrono::NaiveTime";
        #[cfg(not(any(feature = "time", feature = "chrono")))]
        return "String";
    }
    if upper == "UUID" {
        #[cfg(feature = "uuid")]
        return "::uuid::Uuid";
        #[cfg(not(feature = "uuid"))]
        return "String";
    }
    if upper == "DECIMAL"
        || upper.starts_with("DECIMAL(")
        || upper == "NUMERIC"
        || upper.starts_with("NUMERIC(")
    {
        #[cfg(feature = "decimal")]
        return "::rust_decimal::Decimal";
        // Without decimal feature, fall through to String via affinity rules
    }

    // SQLite type affinity rules (in order from the docs)
    if upper.contains("INT") {
        return "i64";
    }
    if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        return "String";
    }
    if upper.contains("BLOB") {
        return "Vec<u8>";
    }
    if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        return "f64";
    }

    // NUMERIC affinity -> could be integer or real, default to String
    "String"
}

/// Check whether a user-declared Rust parameter type is compatible with a
/// SQLite column. SQLite doesn't type parameters at prepare time, so we
/// accept any type that has a `SqliteEncode` impl.
#[allow(dead_code)]
pub fn is_sqlite_param_compatible(rust_type: &str) -> bool {
    matches!(
        rust_type,
        "bool"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "f32"
            | "f64"
            | "&str"
            | "String"
            | "&[u8]"
            | "Vec<u8>"
    )
}

/// Check whether a Rust type with an `Option<>` wrapper has a valid inner
/// type for SQLite parameters.
#[allow(dead_code)]
pub fn is_sqlite_option_param_compatible(rust_type: &str) -> bool {
    if let Some(inner) = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        is_sqlite_param_compatible(inner)
    } else {
        is_sqlite_param_compatible(rust_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- resolve_sqlite_type ---

    #[test]
    fn integer_types() {
        assert_eq!(resolve_sqlite_type(Some("INTEGER")), "i64");
        assert_eq!(resolve_sqlite_type(Some("INT")), "i64");
        assert_eq!(resolve_sqlite_type(Some("TINYINT")), "i64");
        assert_eq!(resolve_sqlite_type(Some("SMALLINT")), "i64");
        assert_eq!(resolve_sqlite_type(Some("MEDIUMINT")), "i64");
        assert_eq!(resolve_sqlite_type(Some("BIGINT")), "i64");
        assert_eq!(resolve_sqlite_type(Some("UNSIGNED BIG INT")), "i64");
        assert_eq!(resolve_sqlite_type(Some("INT2")), "i64");
        assert_eq!(resolve_sqlite_type(Some("INT8")), "i64");
    }

    #[test]
    fn text_types() {
        assert_eq!(resolve_sqlite_type(Some("TEXT")), "String");
        assert_eq!(resolve_sqlite_type(Some("CHARACTER(20)")), "String");
        assert_eq!(resolve_sqlite_type(Some("VARCHAR(255)")), "String");
        assert_eq!(
            resolve_sqlite_type(Some("VARYING CHARACTER(255)")),
            "String"
        );
        assert_eq!(resolve_sqlite_type(Some("NCHAR(55)")), "String");
        assert_eq!(resolve_sqlite_type(Some("NATIVE CHARACTER(70)")), "String");
        assert_eq!(resolve_sqlite_type(Some("NVARCHAR(100)")), "String");
        assert_eq!(resolve_sqlite_type(Some("CLOB")), "String");
    }

    #[test]
    fn blob_types() {
        assert_eq!(resolve_sqlite_type(Some("BLOB")), "Vec<u8>");
    }

    #[test]
    fn real_types() {
        assert_eq!(resolve_sqlite_type(Some("REAL")), "f64");
        assert_eq!(resolve_sqlite_type(Some("DOUBLE")), "f64");
        assert_eq!(resolve_sqlite_type(Some("DOUBLE PRECISION")), "f64");
        assert_eq!(resolve_sqlite_type(Some("FLOAT")), "f64");
    }

    #[test]
    fn boolean_types() {
        assert_eq!(resolve_sqlite_type(Some("BOOLEAN")), "bool");
        assert_eq!(resolve_sqlite_type(Some("BOOL")), "bool");
    }

    #[test]
    fn numeric_affinity_defaults_to_string() {
        // When the `decimal` feature is enabled, NUMERIC/DECIMAL map to rust_decimal
        #[cfg(feature = "decimal")]
        {
            assert_eq!(
                resolve_sqlite_type(Some("NUMERIC")),
                "::rust_decimal::Decimal"
            );
            assert_eq!(
                resolve_sqlite_type(Some("DECIMAL(10,5)")),
                "::rust_decimal::Decimal"
            );
        }
        #[cfg(not(feature = "decimal"))]
        {
            assert_eq!(resolve_sqlite_type(Some("NUMERIC")), "String");
            assert_eq!(resolve_sqlite_type(Some("DECIMAL(10,5)")), "String");
        }
    }

    #[test]
    fn no_type_defaults_to_string() {
        assert_eq!(resolve_sqlite_type(None), "String");
        assert_eq!(resolve_sqlite_type(Some("")), "String");
    }

    #[test]
    fn case_insensitive() {
        assert_eq!(resolve_sqlite_type(Some("integer")), "i64");
        assert_eq!(resolve_sqlite_type(Some("text")), "String");
        assert_eq!(resolve_sqlite_type(Some("Real")), "f64");
        assert_eq!(resolve_sqlite_type(Some("boolean")), "bool");
    }

    // --- is_sqlite_param_compatible ---

    #[test]
    fn param_compat_scalars() {
        assert!(is_sqlite_param_compatible("bool"));
        assert!(is_sqlite_param_compatible("i8"));
        assert!(is_sqlite_param_compatible("i16"));
        assert!(is_sqlite_param_compatible("i32"));
        assert!(is_sqlite_param_compatible("i64"));
        assert!(is_sqlite_param_compatible("f32"));
        assert!(is_sqlite_param_compatible("f64"));
        assert!(is_sqlite_param_compatible("&str"));
        assert!(is_sqlite_param_compatible("String"));
        assert!(is_sqlite_param_compatible("&[u8]"));
        assert!(is_sqlite_param_compatible("Vec<u8>"));
    }

    #[test]
    fn param_compat_rejects_unknown() {
        assert!(!is_sqlite_param_compatible("u32"));
        assert!(!is_sqlite_param_compatible("u64"));
        assert!(!is_sqlite_param_compatible("SomeEnum"));
    }

    // --- is_sqlite_option_param_compatible ---

    #[test]
    fn option_param_compat() {
        assert!(is_sqlite_option_param_compatible("Option<i64>"));
        assert!(is_sqlite_option_param_compatible("Option<String>"));
        assert!(is_sqlite_option_param_compatible("Option<&str>"));
        assert!(is_sqlite_option_param_compatible("i32"));
        assert!(!is_sqlite_option_param_compatible("Option<u32>"));
    }

    // --- Feature-gated types ---

    #[test]
    fn datetime_types() {
        #[cfg(feature = "time")]
        {
            assert_eq!(
                resolve_sqlite_type(Some("DATETIME")),
                "::time::PrimitiveDateTime"
            );
            assert_eq!(
                resolve_sqlite_type(Some("TIMESTAMP")),
                "::time::PrimitiveDateTime"
            );
            assert_eq!(resolve_sqlite_type(Some("DATE")), "::time::Date");
            assert_eq!(resolve_sqlite_type(Some("TIME")), "::time::Time");
        }
        #[cfg(all(feature = "chrono", not(feature = "time")))]
        {
            assert_eq!(
                resolve_sqlite_type(Some("DATETIME")),
                "::chrono::NaiveDateTime"
            );
            assert_eq!(
                resolve_sqlite_type(Some("TIMESTAMP")),
                "::chrono::NaiveDateTime"
            );
            assert_eq!(resolve_sqlite_type(Some("DATE")), "::chrono::NaiveDate");
            assert_eq!(resolve_sqlite_type(Some("TIME")), "::chrono::NaiveTime");
        }
        #[cfg(not(any(feature = "time", feature = "chrono")))]
        {
            assert_eq!(resolve_sqlite_type(Some("DATETIME")), "String");
            assert_eq!(resolve_sqlite_type(Some("TIMESTAMP")), "String");
            assert_eq!(resolve_sqlite_type(Some("DATE")), "String");
            assert_eq!(resolve_sqlite_type(Some("TIME")), "String");
        }
    }

    #[test]
    fn uuid_type() {
        #[cfg(feature = "uuid")]
        assert_eq!(resolve_sqlite_type(Some("UUID")), "::uuid::Uuid");
        #[cfg(not(feature = "uuid"))]
        assert_eq!(resolve_sqlite_type(Some("UUID")), "String");
    }

    #[test]
    fn decimal_type() {
        #[cfg(feature = "decimal")]
        assert_eq!(
            resolve_sqlite_type(Some("DECIMAL")),
            "::rust_decimal::Decimal"
        );
        #[cfg(not(feature = "decimal"))]
        assert_eq!(resolve_sqlite_type(Some("DECIMAL")), "String");
    }
}
