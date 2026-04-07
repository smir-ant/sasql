//! Analyze PostgreSQL EXPLAIN output for performance anti-patterns.
//!
//! When `feature = "explain"` is enabled, the proc macro fetches EXPLAIN output
//! for each query and runs it through `analyze_plan` to detect common issues
//! like sequential scans on large tables and sorts without index backing.
//!
//! Warnings are emitted via `eprintln!` so they appear as compiler warnings
//! during `cargo build`.

/// A performance warning detected from EXPLAIN analysis.
pub struct ExplainWarning {
    pub message: String,
}

/// Read the Seq Scan row-count threshold from the `BSQL_EXPLAIN_THRESHOLD` env var.
///
/// Defaults to 1000 if the variable is unset or not a valid integer.
pub fn explain_threshold() -> u64 {
    std::env::var("BSQL_EXPLAIN_THRESHOLD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000)
}

/// Analyze a PostgreSQL EXPLAIN plan text for performance anti-patterns.
///
/// `threshold` is the minimum estimated row count to trigger Seq Scan warnings.
/// A Seq Scan warns only when `rows > threshold` (strictly greater than).
///
/// Returns a list of warnings (may be empty).
pub fn analyze_plan(plan_text: &str, threshold: u64) -> Vec<ExplainWarning> {
    let mut warnings = Vec::new();

    for line in plan_text.lines() {
        let trimmed = line.trim().trim_start_matches("-> ");

        // Detect Seq Scan with high row count
        if let Some(warning) = check_seq_scan(trimmed, threshold) {
            warnings.push(warning);
        }

        // Detect Sort without index
        if let Some(warning) = check_sort_without_index(trimmed, plan_text) {
            warnings.push(warning);
        }
    }

    warnings
}

/// Check a single EXPLAIN line for a Seq Scan with an estimated row count
/// exceeding `threshold`.
///
/// EXPLAIN FORMAT TEXT line format:
/// ```text
/// Seq Scan on tablename  (cost=0.00..35.50 rows=2550 width=36)
/// ```
fn check_seq_scan(line: &str, threshold: u64) -> Option<ExplainWarning> {
    let table = parse_table_name(line)?;
    let rows = parse_rows_estimate(line)?;
    if rows > threshold {
        Some(ExplainWarning {
            message: format!(
                "Seq Scan on `{}` with estimated {} rows (threshold: {}). \
                 Consider adding an index.",
                table, rows, threshold
            ),
        })
    } else {
        None
    }
}

/// Check a single EXPLAIN line for a Sort node that is not backed by an index.
///
/// Heuristic: if the plan contains a `Sort` node but no `Index Scan` or
/// `Index Only Scan` node anywhere, the sort is likely materializing in memory
/// or on disk rather than leveraging an index.
fn check_sort_without_index(line: &str, full_plan: &str) -> Option<ExplainWarning> {
    // Only trigger on lines that start with "Sort" followed by whitespace and cost
    if !line.starts_with("Sort") {
        return None;
    }
    // Make sure it's actually a Sort node, not "Sort Key:" or "Sort Method:"
    let after_sort = &line[4..];
    if !after_sort.starts_with(' ') && !after_sort.starts_with('\t') {
        return None;
    }
    // Must contain a cost estimate to be a plan node
    if !after_sort.contains("(cost=") {
        return None;
    }

    // Check if there is ANY index scan in the full plan
    let has_index_scan = full_plan.contains("Index Scan") || full_plan.contains("Index Only Scan");

    if !has_index_scan {
        Some(ExplainWarning {
            message: "Sort without index backing detected. \
                      Consider adding an index on the sort columns."
                .to_owned(),
        })
    } else {
        None
    }
}

/// Parse the estimated row count from an EXPLAIN line.
///
/// Looks for `rows=N` in the cost estimate parenthetical.
/// Returns `None` if the pattern is not found or the number cannot be parsed.
fn parse_rows_estimate(line: &str) -> Option<u64> {
    let rows_start = line.find("rows=")?;
    let after_rows = &line[rows_start + 5..];
    let end = after_rows
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after_rows.len());
    if end == 0 {
        return None;
    }
    after_rows[..end].parse().ok()
}

/// Parse the table name from a Seq Scan EXPLAIN line.
///
/// Expected format: `Seq Scan on tablename  (cost=...)`
/// or with alias:   `Seq Scan on tablename alias  (cost=...)`
/// or schema-qualified: `Seq Scan on public.tablename  (cost=...)`
///
/// Returns `None` if the line is not a Seq Scan line.
fn parse_table_name(line: &str) -> Option<&str> {
    let prefix = "Seq Scan on ";
    let start = line.find(prefix)?;
    let after = &line[start + prefix.len()..];
    let end = after
        .find(|c: char| c == ' ' || c == '(')
        .unwrap_or(after.len());
    if end == 0 {
        return None;
    }
    Some(&after[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Seq Scan detection ---

    #[test]
    fn seq_scan_above_threshold_warns() {
        let plan = "Seq Scan on users  (cost=0.00..35.50 rows=5000 width=36)";
        let warnings = analyze_plan(plan, 1000);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("Seq Scan"));
        assert!(warnings[0].message.contains("users"));
    }

    #[test]
    fn seq_scan_below_threshold_no_warning() {
        let plan = "Seq Scan on users  (cost=0.00..1.10 rows=10 width=36)";
        let warnings = analyze_plan(plan, 1000);
        assert!(warnings.is_empty());
    }

    #[test]
    fn seq_scan_at_threshold_no_warning() {
        let plan = "Seq Scan on users  (cost=0.00..35.50 rows=1000 width=36)";
        let warnings = analyze_plan(plan, 1000);
        assert!(warnings.is_empty()); // > not >=
    }

    #[test]
    fn index_scan_no_warning() {
        let plan = "Index Scan using users_pkey on users  (cost=0.00..8.27 rows=1 width=36)";
        let warnings = analyze_plan(plan, 1000);
        assert!(warnings.is_empty());
    }

    #[test]
    fn index_only_scan_no_warning() {
        let plan =
            "Index Only Scan using idx_users_email on users  (cost=0.00..1.05 rows=1 width=36)";
        let warnings = analyze_plan(plan, 1000);
        assert!(warnings.is_empty());
    }

    #[test]
    fn nested_plan_seq_scan_detected() {
        let plan = "\
Nested Loop  (cost=0.00..500.00 rows=1000 width=72)
  ->  Index Scan using orders_pkey on orders  (cost=0.00..8.27 rows=1 width=36)
  ->  Seq Scan on order_items  (cost=0.00..50.00 rows=5000 width=36)
        Filter: (order_id = orders.id)";
        let warnings = analyze_plan(plan, 1000);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("order_items"));
    }

    #[test]
    fn multiple_seq_scans_multiple_warnings() {
        let plan = "\
Hash Join  (cost=100.00..500.00 rows=1000 width=72)
  ->  Seq Scan on users  (cost=0.00..35.50 rows=2550 width=36)
  ->  Seq Scan on orders  (cost=0.00..50.00 rows=3000 width=36)";
        let warnings = analyze_plan(plan, 1000);
        assert_eq!(warnings.len(), 2);
    }

    #[test]
    fn threshold_zero_warns_on_any_seq_scan() {
        let plan = "Seq Scan on tiny_table  (cost=0.00..1.01 rows=1 width=4)";
        let warnings = analyze_plan(plan, 0);
        assert_eq!(warnings.len(), 1);
    }

    #[test]
    fn threshold_max_never_warns() {
        let plan = "Seq Scan on huge  (cost=0.00..99999.00 rows=9999999 width=36)";
        let warnings = analyze_plan(plan, u64::MAX);
        assert!(warnings.is_empty());
    }

    #[test]
    fn empty_plan_no_warnings() {
        let warnings = analyze_plan("", 1000);
        assert!(warnings.is_empty());
    }

    #[test]
    fn malformed_plan_no_crash() {
        let warnings = analyze_plan("this is not a plan at all {{{", 1000);
        assert!(warnings.is_empty());
    }

    #[test]
    fn plan_with_cte() {
        let plan = "\
CTE Scan on cte  (cost=0.00..20.00 rows=1000 width=36)
  CTE cte
    ->  Seq Scan on large_table  (cost=0.00..500.00 rows=50000 width=36)";
        let warnings = analyze_plan(plan, 1000);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("large_table"));
    }

    #[test]
    fn plan_with_subquery() {
        let plan = "\
Seq Scan on products  (cost=0.00..100.00 rows=5000 width=36)
  Filter: (price > (SubPlan 1))
  SubPlan 1
    ->  Aggregate  (cost=10.00..10.01 rows=1 width=8)
          ->  Seq Scan on prices  (cost=0.00..8.00 rows=800 width=8)";
        let warnings = analyze_plan(plan, 500);
        assert_eq!(warnings.len(), 2); // products (5000 > 500) and prices (800 > 500)
    }

    #[test]
    fn rows_estimate_parsing() {
        assert_eq!(
            parse_rows_estimate("(cost=0.00..1.00 rows=42 width=4)"),
            Some(42)
        );
        assert_eq!(
            parse_rows_estimate("(cost=0.00..1.00 rows=0 width=4)"),
            Some(0)
        );
        assert_eq!(parse_rows_estimate("no rows here"), None);
        assert_eq!(parse_rows_estimate("rows="), None); // empty after rows=
    }

    #[test]
    fn table_name_parsing() {
        assert_eq!(parse_table_name("Seq Scan on users  (cost="), Some("users"));
        assert_eq!(
            parse_table_name("Seq Scan on my_schema.users  (cost="),
            Some("my_schema.users")
        );
        assert_eq!(parse_table_name("Index Scan on users"), None); // Not a Seq Scan
    }

    #[test]
    fn schema_qualified_table_name() {
        let plan = "Seq Scan on public.users  (cost=0.00..35.50 rows=5000 width=36)";
        let warnings = analyze_plan(plan, 1000);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("public.users"));
    }

    #[test]
    fn aliased_table() {
        let plan = "Seq Scan on users u  (cost=0.00..35.50 rows=5000 width=36)";
        let warnings = analyze_plan(plan, 1000);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("users"));
    }

    #[test]
    fn explain_threshold_default_when_unset() {
        // When BSQL_EXPLAIN_THRESHOLD is not set, should return 1000.
        // We cannot set/remove env vars in Rust 2024 edition without unsafe
        // (and this crate forbids unsafe_code), so we only test the default
        // and the parse-from-string logic indirectly via analyze_plan thresholds.
        //
        // The env var integration is implicitly tested: if BSQL_EXPLAIN_THRESHOLD
        // is not set in the test environment, explain_threshold() returns 1000.
        if std::env::var("BSQL_EXPLAIN_THRESHOLD").is_err() {
            assert_eq!(explain_threshold(), 1000);
        }
    }

    #[test]
    fn threshold_boundaries_exercised() {
        // Exercise the threshold parameter directly through analyze_plan
        // to cover the same logic explain_threshold feeds into.
        let plan = "Seq Scan on t  (cost=0.00..10.00 rows=500 width=4)";
        assert!(analyze_plan(plan, 499).len() == 1); // 500 > 499
        assert!(analyze_plan(plan, 500).is_empty()); // 500 is not > 500
        assert!(analyze_plan(plan, 501).is_empty()); // 500 is not > 501
    }

    // --- Sort detection ---

    #[test]
    fn sort_in_plan_warns() {
        let plan = "\
Sort  (cost=100.00..110.00 rows=5000 width=36)
  Sort Key: created_at
  ->  Seq Scan on events  (cost=0.00..80.00 rows=5000 width=36)";
        let warnings = analyze_plan(plan, 1000);
        // Should warn about Seq Scan (5000 rows) and Sort (no index scan in plan)
        assert!(warnings.len() >= 2);
        let has_sort_warning = warnings
            .iter()
            .any(|w| w.message.contains("Sort without index"));
        assert!(has_sort_warning, "should detect sort without index");
    }

    #[test]
    fn sort_with_index_scan_no_sort_warning() {
        let plan = "\
Sort  (cost=10.00..11.00 rows=100 width=36)
  Sort Key: name
  ->  Index Scan using idx_users_name on users  (cost=0.00..8.27 rows=100 width=36)";
        let warnings = analyze_plan(plan, 1000);
        // No seq scan warning (100 < 1000), no sort warning (Index Scan exists)
        assert!(warnings.is_empty());
    }

    #[test]
    fn sort_key_line_not_mistaken_for_sort_node() {
        // "Sort Key:" should NOT trigger a sort warning
        let plan = "\
Index Scan using idx on users  (cost=0.00..8.27 rows=1 width=36)
  Sort Key: name";
        let warnings = analyze_plan(plan, 1000);
        assert!(warnings.is_empty());
    }

    #[test]
    fn sort_method_line_not_mistaken_for_sort_node() {
        // "Sort Method: quicksort" should NOT trigger
        let plan = "\
Sort  (cost=100.00..110.00 rows=100 width=36)
  Sort Key: name
  Sort Method: quicksort  Memory: 25kB
  ->  Index Scan using idx on users  (cost=0.00..50.00 rows=100 width=36)";
        let warnings = analyze_plan(plan, 1000);
        // Sort node exists but Index Scan also exists -> no sort warning
        // Seq scan not present -> no seq scan warning
        assert!(warnings.is_empty());
    }

    // --- Edge cases ---

    #[test]
    fn plan_only_whitespace() {
        let warnings = analyze_plan("   \n  \n   ", 1000);
        assert!(warnings.is_empty());
    }

    #[test]
    fn plan_with_arrow_prefix() {
        // Lines prefixed with "-> " after trimming should still be detected
        let plan = "  ->  Seq Scan on big_table  (cost=0.00..999.00 rows=50000 width=36)";
        let warnings = analyze_plan(plan, 1000);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("big_table"));
    }

    #[test]
    fn rows_zero_below_any_positive_threshold() {
        let plan = "Seq Scan on empty_table  (cost=0.00..0.00 rows=0 width=0)";
        let warnings = analyze_plan(plan, 1);
        assert!(warnings.is_empty()); // 0 is not > 1
    }

    #[test]
    fn rows_one_at_threshold_zero() {
        let plan = "Seq Scan on t  (cost=0.00..1.00 rows=1 width=4)";
        let warnings = analyze_plan(plan, 0);
        assert_eq!(warnings.len(), 1); // 1 > 0
    }
}
