//! PostgreSQL OID to Rust type mapping.
//!
//! This table is the single source of truth for how PostgreSQL types map to Rust
//! types in bsql. Both the proc macro (at compile time) and the runtime
//! (for error messages) reference this mapping.

/// A single entry in the PG-to-Rust type map.
#[derive(Debug, Clone, Copy)]
pub struct TypeMapping {
    /// The PostgreSQL OID for this type.
    pub pg_oid: u32,
    /// The PostgreSQL type name (e.g. `"int4"`, `"text"`).
    pub pg_name: &'static str,
    /// The Rust type string used in generated code (e.g. `"i32"`, `"String"`).
    pub rust_type: &'static str,
    /// Whether this is an array type.
    pub is_array: bool,
}

/// Base type mappings. No feature-gated types.
///
/// OIDs sourced from `pg_type.dat` in the PostgreSQL source tree.
/// <https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat>
pub const BASE_TYPE_MAP: &[TypeMapping] = &[
    // ── Scalar types ──────────────────────────────────────────────
    TypeMapping {
        pg_oid: 16,
        pg_name: "bool",
        rust_type: "bool",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 21,
        pg_name: "int2",
        rust_type: "i16",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 23,
        pg_name: "int4",
        rust_type: "i32",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 20,
        pg_name: "int8",
        rust_type: "i64",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 700,
        pg_name: "float4",
        rust_type: "f32",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 701,
        pg_name: "float8",
        rust_type: "f64",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 25,
        pg_name: "text",
        rust_type: "String",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 1043,
        pg_name: "varchar",
        rust_type: "String",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 1042,
        pg_name: "bpchar",
        rust_type: "String",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 17,
        pg_name: "bytea",
        rust_type: "Vec<u8>",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 26,
        pg_name: "oid",
        rust_type: "u32",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 2278,
        pg_name: "void",
        rust_type: "()",
        is_array: false,
    },
    // ── JSON types ────────────────────────────────────────────────
    TypeMapping {
        pg_oid: 114,
        pg_name: "json",
        rust_type: "String",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 3802,
        pg_name: "jsonb",
        rust_type: "String",
        is_array: false,
    },
    // NOTE: timestamp (1114), timestamptz (1184), date (1082), time (1083)
    // are NOT in the base map. They are resolved by bsql-macros with proper
    // feature-gated types (time::OffsetDateTime, chrono::NaiveDateTime, etc).
    // Without a feature flag, the macros crate emits a helpful error suggesting
    // the user enable the `time` or `chrono` feature.
    // ── Interval type ─────────────────────────────────────────────
    TypeMapping {
        pg_oid: 1186,
        pg_name: "interval",
        rust_type: "String",
        is_array: false,
    },
    // ── Network address types ─────────────────────────────────────
    TypeMapping {
        pg_oid: 869,
        pg_name: "inet",
        rust_type: "String",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 650,
        pg_name: "cidr",
        rust_type: "String",
        is_array: false,
    },
    TypeMapping {
        pg_oid: 829,
        pg_name: "macaddr",
        rust_type: "String",
        is_array: false,
    },
    // ── Array types ───────────────────────────────────────────────
    TypeMapping {
        pg_oid: 1000,
        pg_name: "_bool",
        rust_type: "Vec<bool>",
        is_array: true,
    },
    TypeMapping {
        pg_oid: 1005,
        pg_name: "_int2",
        rust_type: "Vec<i16>",
        is_array: true,
    },
    TypeMapping {
        pg_oid: 1007,
        pg_name: "_int4",
        rust_type: "Vec<i32>",
        is_array: true,
    },
    TypeMapping {
        pg_oid: 1016,
        pg_name: "_int8",
        rust_type: "Vec<i64>",
        is_array: true,
    },
    TypeMapping {
        pg_oid: 1021,
        pg_name: "_float4",
        rust_type: "Vec<f32>",
        is_array: true,
    },
    TypeMapping {
        pg_oid: 1022,
        pg_name: "_float8",
        rust_type: "Vec<f64>",
        is_array: true,
    },
    TypeMapping {
        pg_oid: 1009,
        pg_name: "_text",
        rust_type: "Vec<String>",
        is_array: true,
    },
    TypeMapping {
        pg_oid: 1015,
        pg_name: "_varchar",
        rust_type: "Vec<String>",
        is_array: true,
    },
    TypeMapping {
        pg_oid: 1001,
        pg_name: "_bytea",
        rust_type: "Vec<Vec<u8>>",
        is_array: true,
    },
    TypeMapping {
        pg_oid: 199,
        pg_name: "_json",
        rust_type: "Vec<String>",
        is_array: true,
    },
    TypeMapping {
        pg_oid: 3807,
        pg_name: "_jsonb",
        rust_type: "Vec<String>",
        is_array: true,
    },
];

/// Look up the Rust type for a PostgreSQL OID.
///
/// Returns `None` for unrecognized OIDs. The caller should emit a compile error
/// suggesting the user enable the appropriate feature flag.
pub fn rust_type_for_oid(oid: u32) -> Option<&'static str> {
    BASE_TYPE_MAP
        .iter()
        .find(|m| m.pg_oid == oid)
        .map(|m| m.rust_type)
}

/// Look up the PostgreSQL type name for an OID (for error messages).
pub fn pg_name_for_oid(oid: u32) -> Option<&'static str> {
    BASE_TYPE_MAP
        .iter()
        .find(|m| m.pg_oid == oid)
        .map(|m| m.pg_name)
}

/// Check whether a user-declared Rust parameter type is compatible with a PG OID.
///
/// This is used by the proc macro to verify that `$id: i32` matches the column
/// type PostgreSQL expects. The check is intentionally strict — no implicit
/// widening (i32 does not match int8).
pub fn is_param_compatible(rust_type: &str, pg_oid: u32) -> bool {
    matches!(
        (rust_type, pg_oid),
        // Exact matches
        ("bool", 16)
        | ("i16", 21)
        | ("i32", 23)
        | ("i64", 20)
        | ("f32", 700)
        | ("f64", 701)
        // String params: &str and String both accepted for text-like columns
        | ("&str", 25) | ("&str", 1043) | ("&str", 1042)
        | ("String", 25) | ("String", 1043) | ("String", 1042)
        // Byte params: &[u8] and Vec<u8> both accepted for bytea
        | ("&[u8]", 17)
        | ("Vec<u8>", 17)
        // OID
        | ("u32", 26)
        // JSON/JSONB: accept &str and String (sent as text)
        | ("&str", 114) | ("String", 114)
        | ("&str", 3802) | ("String", 3802)
        // NOTE: timestamp/timestamptz/date/time param compatibility is
        // handled by bsql-macros with feature-gated types, not here.
        // Interval: accept &str and String
        | ("&str", 1186) | ("String", 1186)
        // Network types: accept &str and String
        | ("&str", 869) | ("String", 869)     // inet
        | ("&str", 650) | ("String", 650)     // cidr
        | ("&str", 829) | ("String", 829)     // macaddr
        // Array params
        | ("&[bool]", 1000) | ("Vec<bool>", 1000)
        | ("&[i16]", 1005)  | ("Vec<i16>", 1005)
        | ("&[i32]", 1007)  | ("Vec<i32>", 1007)
        | ("&[i64]", 1016)  | ("Vec<i64>", 1016)
        | ("&[f32]", 1021)  | ("Vec<f32>", 1021)
        | ("&[f64]", 1022)  | ("Vec<f64>", 1022)
        | ("&[&str]", 1009) | ("&[String]", 1009) | ("Vec<String>", 1009)
        | ("&[&str]", 1015) | ("&[String]", 1015) | ("Vec<String>", 1015)
        | ("&[&str]", 199)  | ("&[String]", 199)  | ("Vec<String>", 199)   // json[]
        | ("&[&str]", 3807) | ("&[String]", 3807) | ("Vec<String>", 3807)  // jsonb[]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn all_oids_are_unique() {
        let mut seen = HashSet::new();
        for m in BASE_TYPE_MAP {
            assert!(
                seen.insert(m.pg_oid),
                "duplicate OID {} ({})",
                m.pg_oid,
                m.pg_name
            );
        }
    }

    #[test]
    fn lookup_scalar_types() {
        assert_eq!(rust_type_for_oid(16), Some("bool"));
        assert_eq!(rust_type_for_oid(21), Some("i16"));
        assert_eq!(rust_type_for_oid(23), Some("i32"));
        assert_eq!(rust_type_for_oid(20), Some("i64"));
        assert_eq!(rust_type_for_oid(700), Some("f32"));
        assert_eq!(rust_type_for_oid(701), Some("f64"));
        assert_eq!(rust_type_for_oid(25), Some("String"));
        assert_eq!(rust_type_for_oid(1043), Some("String"));
        assert_eq!(rust_type_for_oid(17), Some("Vec<u8>"));
        assert_eq!(rust_type_for_oid(26), Some("u32"));
    }

    #[test]
    fn lookup_array_types() {
        assert_eq!(rust_type_for_oid(1007), Some("Vec<i32>"));
        assert_eq!(rust_type_for_oid(1009), Some("Vec<String>"));
        assert_eq!(rust_type_for_oid(1001), Some("Vec<Vec<u8>>"));
    }

    #[test]
    fn unknown_oid_returns_none() {
        assert_eq!(rust_type_for_oid(99999), None);
    }

    #[test]
    fn pg_name_lookup() {
        assert_eq!(pg_name_for_oid(23), Some("int4"));
        assert_eq!(pg_name_for_oid(25), Some("text"));
        assert_eq!(pg_name_for_oid(99999), None);
    }

    #[test]
    fn param_compatible_accepts_valid_pairs() {
        // Scalar exact matches
        assert!(is_param_compatible("bool", 16));
        assert!(is_param_compatible("i16", 21));
        assert!(is_param_compatible("i32", 23));
        assert!(is_param_compatible("i64", 20));
        assert!(is_param_compatible("f32", 700));
        assert!(is_param_compatible("f64", 701));
        assert!(is_param_compatible("u32", 26));
        // String-like: &str and String for text/varchar/bpchar
        assert!(is_param_compatible("&str", 25));
        assert!(is_param_compatible("&str", 1043));
        assert!(is_param_compatible("&str", 1042));
        assert!(is_param_compatible("String", 25));
        assert!(is_param_compatible("String", 1043));
        // Bytes
        assert!(is_param_compatible("&[u8]", 17));
        assert!(is_param_compatible("Vec<u8>", 17));
        // Arrays
        assert!(is_param_compatible("&[i32]", 1007));
        assert!(is_param_compatible("Vec<i32>", 1007));
        assert!(is_param_compatible("&[&str]", 1009));
        assert!(is_param_compatible("Vec<String>", 1009));
    }

    #[test]
    fn param_compatible_rejects_mismatches() {
        // No implicit widening/narrowing
        assert!(!is_param_compatible("i16", 23));
        assert!(!is_param_compatible("i32", 20));
        assert!(!is_param_compatible("i64", 23));
        assert!(!is_param_compatible("f32", 701));
        assert!(!is_param_compatible("f64", 700));
        // Cross-category
        assert!(!is_param_compatible("&str", 23));
        assert!(!is_param_compatible("bool", 25));
        assert!(!is_param_compatible("bool", 23));
        assert!(!is_param_compatible("i32", 16));
        assert!(!is_param_compatible("i32", 26));
        // Array mismatch
        assert!(!is_param_compatible("Vec<i32>", 1009));
    }

    #[test]
    fn param_compatible_unknown_oid() {
        assert!(!is_param_compatible("i32", 99999));
    }

    #[test]
    fn all_base_types_have_pg_names() {
        for m in BASE_TYPE_MAP {
            assert!(!m.pg_name.is_empty(), "OID {} has empty pg_name", m.pg_oid);
            assert!(
                !m.rust_type.is_empty(),
                "OID {} has empty rust_type",
                m.pg_oid
            );
        }
    }

    #[test]
    fn array_types_flagged_correctly() {
        for m in BASE_TYPE_MAP {
            if m.pg_name.starts_with('_') {
                assert!(m.is_array, "{} should be flagged as array", m.pg_name);
            } else {
                assert!(!m.is_array, "{} should not be flagged as array", m.pg_name);
            }
        }
    }

    #[test]
    fn void_type_maps_to_unit() {
        assert_eq!(rust_type_for_oid(2278), Some("()"));
    }

    // ── New type tests ──────────────────────────────────────────────

    #[test]
    fn lookup_json_types() {
        assert_eq!(rust_type_for_oid(114), Some("String"));
        assert_eq!(pg_name_for_oid(114), Some("json"));
        assert_eq!(rust_type_for_oid(3802), Some("String"));
        assert_eq!(pg_name_for_oid(3802), Some("jsonb"));
    }

    #[test]
    fn lookup_json_array_types() {
        assert_eq!(rust_type_for_oid(199), Some("Vec<String>"));
        assert_eq!(pg_name_for_oid(199), Some("_json"));
        assert_eq!(rust_type_for_oid(3807), Some("Vec<String>"));
        assert_eq!(pg_name_for_oid(3807), Some("_jsonb"));
    }

    #[test]
    fn datetime_types_not_in_base_map() {
        // timestamp/timestamptz/date/time are resolved by bsql-macros
        // with feature-gated types, not by the base type map.
        assert_eq!(rust_type_for_oid(1114), None); // timestamp
        assert_eq!(rust_type_for_oid(1184), None); // timestamptz
        assert_eq!(rust_type_for_oid(1082), None); // date
        assert_eq!(rust_type_for_oid(1083), None); // time
    }

    #[test]
    fn lookup_interval_type() {
        assert_eq!(rust_type_for_oid(1186), Some("String"));
        assert_eq!(pg_name_for_oid(1186), Some("interval"));
    }

    #[test]
    fn lookup_network_types() {
        assert_eq!(rust_type_for_oid(869), Some("String"));
        assert_eq!(pg_name_for_oid(869), Some("inet"));
        assert_eq!(rust_type_for_oid(650), Some("String"));
        assert_eq!(pg_name_for_oid(650), Some("cidr"));
        assert_eq!(rust_type_for_oid(829), Some("String"));
        assert_eq!(pg_name_for_oid(829), Some("macaddr"));
    }

    #[test]
    fn param_compatible_json() {
        assert!(is_param_compatible("&str", 114));
        assert!(is_param_compatible("String", 114));
        assert!(is_param_compatible("&str", 3802));
        assert!(is_param_compatible("String", 3802));
        assert!(!is_param_compatible("i32", 114));
    }

    #[test]
    fn param_compatible_datetime_not_in_base() {
        // Date/time param compatibility is handled by bsql-macros
        // with feature-gated types.
        assert!(!is_param_compatible("&str", 1114));
        assert!(!is_param_compatible("&str", 1184));
        assert!(!is_param_compatible("&str", 1082));
        assert!(!is_param_compatible("&str", 1083));
    }

    #[test]
    fn param_compatible_interval() {
        assert!(is_param_compatible("&str", 1186));
        assert!(is_param_compatible("String", 1186));
        assert!(!is_param_compatible("i64", 1186));
    }

    #[test]
    fn param_compatible_network() {
        assert!(is_param_compatible("&str", 869));
        assert!(is_param_compatible("String", 869));
        assert!(is_param_compatible("&str", 650));
        assert!(is_param_compatible("String", 650));
        assert!(is_param_compatible("&str", 829));
        assert!(is_param_compatible("String", 829));
        assert!(!is_param_compatible("i32", 869));
    }

    #[test]
    fn param_compatible_json_arrays() {
        assert!(is_param_compatible("&[&str]", 199));
        assert!(is_param_compatible("Vec<String>", 199));
        assert!(is_param_compatible("&[&str]", 3807));
        assert!(is_param_compatible("Vec<String>", 3807));
        assert!(!is_param_compatible("Vec<i32>", 199));
    }

    // --- Audit gap tests ---

    // #83: JSON OID 114 -> "String"
    #[test]
    fn json_oid_114_maps_to_string() {
        assert_eq!(rust_type_for_oid(114), Some("String"));
    }

    // #84: JSONB OID 3802 -> "String"
    #[test]
    fn jsonb_oid_3802_maps_to_string() {
        assert_eq!(rust_type_for_oid(3802), Some("String"));
    }

    // #85: INTERVAL OID 1186 -> "String"
    #[test]
    fn interval_oid_1186_maps_to_string() {
        assert_eq!(rust_type_for_oid(1186), Some("String"));
    }

    // #86: INET OID 869 -> "String"
    #[test]
    fn inet_oid_869_maps_to_string() {
        assert_eq!(rust_type_for_oid(869), Some("String"));
    }

    // #87: json/jsonb param compatible with &str and String
    #[test]
    fn json_jsonb_param_compatible_str() {
        assert!(is_param_compatible("&str", 114));
        assert!(is_param_compatible("String", 114));
        assert!(is_param_compatible("&str", 3802));
        assert!(is_param_compatible("String", 3802));
    }

    // #88: Array OIDs for json[], jsonb[], inet[], cidr[], macaddr[], interval[]
    #[test]
    fn array_oid_json() {
        assert_eq!(rust_type_for_oid(199), Some("Vec<String>"));
        assert_eq!(pg_name_for_oid(199), Some("_json"));
    }

    #[test]
    fn array_oid_jsonb() {
        assert_eq!(rust_type_for_oid(3807), Some("Vec<String>"));
        assert_eq!(pg_name_for_oid(3807), Some("_jsonb"));
    }

    // Verify cidr and macaddr OIDs
    #[test]
    fn cidr_oid_650_maps_to_string() {
        assert_eq!(rust_type_for_oid(650), Some("String"));
        assert_eq!(pg_name_for_oid(650), Some("cidr"));
    }

    #[test]
    fn macaddr_oid_829_maps_to_string() {
        assert_eq!(rust_type_for_oid(829), Some("String"));
        assert_eq!(pg_name_for_oid(829), Some("macaddr"));
    }

    // bpchar (char) maps to String
    #[test]
    fn bpchar_oid_1042_maps_to_string() {
        assert_eq!(rust_type_for_oid(1042), Some("String"));
    }

    // Param compatibility: cidr and macaddr
    #[test]
    fn param_compatible_cidr() {
        assert!(is_param_compatible("&str", 650));
        assert!(is_param_compatible("String", 650));
        assert!(!is_param_compatible("i32", 650));
    }

    #[test]
    fn param_compatible_macaddr() {
        assert!(is_param_compatible("&str", 829));
        assert!(is_param_compatible("String", 829));
        assert!(!is_param_compatible("i32", 829));
    }
}
