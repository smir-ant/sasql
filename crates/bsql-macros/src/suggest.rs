//! "Did you mean?" suggestions via Levenshtein distance.
//!
//! When a SQL validation error mentions an unknown table or column, this module
//! queries the schema for available names and suggests the closest match.
//!
//! This only runs on VALIDATION FAILURE — zero cost on success.

/// Levenshtein distance between two strings.
///
/// Standard dynamic programming algorithm, O(m*n) time, O(min(m,n)) space.
/// Uses a single row of the DP matrix to minimize allocation.
pub fn levenshtein(a: &str, b: &str) -> usize {
    let a_len = a.len();
    let b_len = b.len();

    // Ensure a is the shorter string for O(min(m,n)) space
    if a_len > b_len {
        return levenshtein(b, a);
    }

    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();

    // Previous row of distances
    let mut prev: Vec<usize> = (0..=a_len).collect();

    for j in 1..=b_len {
        let mut prev_diag = prev[0];
        prev[0] = j;

        for i in 1..=a_len {
            let old_diag = prev[i];
            let cost = if a_bytes[i - 1] == b_bytes[j - 1] {
                0
            } else {
                1
            };
            prev[i] = (prev_diag + cost)
                .min(prev[i] + 1) // deletion
                .min(prev[i - 1] + 1); // insertion
            prev_diag = old_diag;
        }
    }

    prev[a_len]
}

/// Find the closest match from candidates within a maximum distance.
///
/// Returns `None` if no candidate is within distance 3.
/// Ties are broken by first occurrence (stable).
pub fn did_you_mean<'a>(target: &str, candidates: &[&'a str]) -> Option<&'a str> {
    const MAX_DISTANCE: usize = 3;

    candidates
        .iter()
        .map(|c| (*c, levenshtein(target, c)))
        .filter(|(_, d)| *d <= MAX_DISTANCE && *d > 0)
        .min_by_key(|(_, d)| *d)
        .map(|(c, _)| c)
}

/// Query the database for available table names in the public schema.
///
/// Used to generate "did you mean?" suggestions when a table is not found.
pub fn fetch_table_names(
    rt: &tokio::runtime::Runtime,
    client: &tokio_postgres::Client,
) -> Vec<String> {
    let query = "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = 'public' ORDER BY table_name";
    match rt.block_on(client.query(query, &[])) {
        Ok(rows) => rows.iter().map(|r| r.get::<_, String>(0)).collect(),
        Err(_) => Vec::new(),
    }
}

/// Query the database for available column names in a given table.
///
/// Used to generate "did you mean?" suggestions when a column is not found.
pub fn fetch_column_names(
    rt: &tokio::runtime::Runtime,
    client: &tokio_postgres::Client,
    table_name: &str,
) -> Vec<String> {
    let query = "SELECT column_name FROM information_schema.columns \
                 WHERE table_name = $1 ORDER BY ordinal_position";
    match rt.block_on(client.query(query, &[&table_name])) {
        Ok(rows) => rows.iter().map(|r| r.get::<_, String>(0)).collect(),
        Err(_) => Vec::new(),
    }
}

/// Enhance a PostgreSQL error message with "did you mean?" suggestions.
///
/// Detects table-not-found (42P01) and column-not-found (42703) errors from
/// the error message text and queries the schema for alternatives.
pub fn enhance_error(
    error_msg: &str,
    rt: &tokio::runtime::Runtime,
    client: &tokio_postgres::Client,
) -> Option<String> {
    // Table not found: "relation \"xyz\" does not exist"
    if let Some(table) = extract_relation_name(error_msg) {
        let tables = fetch_table_names(rt, client);
        let table_refs: Vec<&str> = tables.iter().map(|s| s.as_str()).collect();
        if let Some(suggestion) = did_you_mean(&table, &table_refs) {
            return Some(format!(
                "\n  did you mean \"{suggestion}\"?\n  available tables: {}",
                format_list(&table_refs, 10)
            ));
        } else if !table_refs.is_empty() {
            return Some(format!(
                "\n  available tables: {}",
                format_list(&table_refs, 10)
            ));
        }
    }

    // Column not found: "column \"xyz\" does not exist"
    // or "column \"xyz\" of relation \"tbl\" does not exist"
    if let Some(column) = extract_column_name(error_msg) {
        // Try to extract the table name from the error for scoped lookup
        let table = extract_column_relation(error_msg);
        if let Some(table) = table {
            let columns = fetch_column_names(rt, client, &table);
            let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            if let Some(suggestion) = did_you_mean(&column, &col_refs) {
                return Some(format!(
                    "\n  did you mean \"{suggestion}\"?\n  available columns in \"{table}\": {}",
                    format_list(&col_refs, 12)
                ));
            } else if !col_refs.is_empty() {
                return Some(format!(
                    "\n  available columns in \"{table}\": {}",
                    format_list(&col_refs, 12)
                ));
            }
        }

        // No table in the error — try all public tables
        let tables = fetch_table_names(rt, client);
        for tbl in &tables {
            let columns = fetch_column_names(rt, client, tbl);
            let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            if let Some(suggestion) = did_you_mean(&column, &col_refs) {
                return Some(format!(
                    "\n  did you mean \"{suggestion}\"? (in table \"{tbl}\")"
                ));
            }
        }

        // No close match in any table — give a generic hint
        return Some("\n  check the column name and table alias".to_owned());
    }

    None
}

/// Extract a relation (table) name from "relation \"xyz\" does not exist".
fn extract_relation_name(msg: &str) -> Option<String> {
    let marker = "relation \"";
    let start = msg.find(marker)?;
    let rest = &msg[start + marker.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_owned())
}

/// Extract a column name from "column \"xyz\" does not exist"
/// or "column \"xyz\" of relation \"tbl\" does not exist".
fn extract_column_name(msg: &str) -> Option<String> {
    let marker = "column \"";
    let start = msg.find(marker)?;
    let rest = &msg[start + marker.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_owned())
}

/// Extract the relation name from "column \"xyz\" of relation \"tbl\" does not exist".
fn extract_column_relation(msg: &str) -> Option<String> {
    let marker = "of relation \"";
    let start = msg.find(marker)?;
    let rest = &msg[start + marker.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_owned())
}

/// Format a list of names for display, truncating if too many.
fn format_list(items: &[&str], max: usize) -> String {
    if items.len() <= max {
        items.join(", ")
    } else {
        let shown: Vec<&str> = items[..max].to_vec();
        format!("{}, ... ({} more)", shown.join(", "), items.len() - max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- levenshtein ---

    #[test]
    fn identical_strings() {
        assert_eq!(levenshtein("name", "name"), 0);
    }

    #[test]
    fn single_insertion() {
        assert_eq!(levenshtein("name", "names"), 1);
    }

    #[test]
    fn single_deletion() {
        assert_eq!(levenshtein("names", "name"), 1);
    }

    #[test]
    fn single_substitution() {
        assert_eq!(levenshtein("name", "nome"), 1);
    }

    #[test]
    fn transposition() {
        assert_eq!(levenshtein("naem", "name"), 2);
    }

    #[test]
    fn empty_strings() {
        assert_eq!(levenshtein("", ""), 0);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("", "abc"), 3);
    }

    #[test]
    fn completely_different() {
        assert_eq!(levenshtein("abc", "xyz"), 3);
    }

    #[test]
    fn case_sensitive() {
        assert_eq!(levenshtein("Name", "name"), 1);
    }

    // --- did_you_mean ---

    #[test]
    fn suggest_close_match() {
        assert_eq!(did_you_mean("naem", &["name", "id", "email"]), Some("name"));
    }

    #[test]
    fn suggest_typo_in_column() {
        assert_eq!(
            did_you_mean("frist_name", &["first_name", "last_name", "email"]),
            Some("first_name")
        );
    }

    #[test]
    fn no_suggestion_when_too_distant() {
        assert_eq!(did_you_mean("xyzzy", &["name", "id"]), None);
    }

    #[test]
    fn no_suggestion_for_empty_candidates() {
        assert_eq!(did_you_mean("name", &[]), None);
    }

    #[test]
    fn exact_match_not_suggested() {
        // Exact match has distance 0, filtered by d > 0
        assert_eq!(did_you_mean("name", &["name", "id"]), None);
    }

    #[test]
    fn picks_closest() {
        assert_eq!(
            did_you_mean("nme", &["name", "names", "nmea"]),
            Some("name") // distance 1 vs 2
        );
    }

    // --- extract helpers ---

    #[test]
    fn extract_relation_from_error() {
        let msg = r#"relation "tcikets" does not exist"#;
        assert_eq!(extract_relation_name(msg), Some("tcikets".into()));
    }

    #[test]
    fn extract_column_from_error() {
        let msg = r#"column "naem" does not exist"#;
        assert_eq!(extract_column_name(msg), Some("naem".into()));
    }

    #[test]
    fn extract_column_relation_from_error() {
        let msg = r#"column "naem" of relation "users" does not exist"#;
        assert_eq!(extract_column_name(msg), Some("naem".into()));
        assert_eq!(extract_column_relation(msg), Some("users".into()));
    }

    #[test]
    fn extract_no_relation() {
        assert_eq!(extract_relation_name("some other error"), None);
    }

    #[test]
    fn extract_no_column() {
        assert_eq!(extract_column_name("some other error"), None);
    }

    // --- format_list ---

    #[test]
    fn format_short_list() {
        assert_eq!(format_list(&["a", "b", "c"], 10), "a, b, c");
    }

    #[test]
    fn format_truncated_list() {
        let items: Vec<&str> = (0..15).map(|_| "x").collect();
        let result = format_list(&items, 10);
        assert!(result.contains("... (5 more)"));
    }
}
