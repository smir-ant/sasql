mod cache;
mod migrate;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 4 || args[1] != "migrate" || args[2] != "--check" {
        eprintln!(
            "Usage: bsql migrate --check <migration.sql> [--database-url URL] [--cache-dir DIR]"
        );
        std::process::exit(2);
    }

    let migration_path = &args[3];

    // Parse optional flags.
    let database_url = parse_flag(&args, "--database-url")
        .or_else(|| std::env::var("BSQL_DATABASE_URL").ok())
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| {
            eprintln!("error: no database URL. Set BSQL_DATABASE_URL or use --database-url");
            std::process::exit(2);
        });

    let cache_dir = parse_flag(&args, "--cache-dir")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            find_cache_dir().unwrap_or_else(|| {
                eprintln!("error: .bsql/queries/ directory not found");
                std::process::exit(2);
            })
        });

    // Read the migration file.
    let migration_sql = std::fs::read_to_string(migration_path).unwrap_or_else(|e| {
        eprintln!("error: cannot read {}: {}", migration_path, e);
        std::process::exit(2);
    });

    // Read the offline cache.
    let queries = cache::read_cache_dir(&cache_dir).unwrap_or_else(|e| {
        eprintln!("error: cannot read cache: {}", e);
        std::process::exit(2);
    });

    if queries.is_empty() {
        println!(
            "No cached queries found in {}. Nothing to check.",
            cache_dir.display()
        );
        std::process::exit(0);
    }

    println!(
        "Checking {} cached queries against migration...",
        queries.len()
    );

    // Run the migration check.
    let result = migrate::check_migration(&database_url, &migration_sql, &queries)
        .unwrap_or_else(|e| {
            eprintln!("error: {}", e);
            std::process::exit(1);
        });

    // Report results.
    if result.failed.is_empty() {
        println!("All {} queries passed.", result.total_queries);
        std::process::exit(0);
    } else {
        println!(
            "\n{} of {} queries FAILED:\n",
            result.failed.len(),
            result.total_queries
        );
        for f in &result.failed {
            println!("  FAIL [{:016x}]: {}", f.sql_hash, f.sql);
            println!("  Error: {}", f.error);
            println!();
        }
        std::process::exit(1);
    }
}

/// Parse a `--flag value` pair from the argument list.
pub fn parse_flag(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|w| w[0] == flag)
        .map(|w| w[1].clone())
}

/// Walk up from the current working directory looking for `.bsql/queries/`.
pub fn find_cache_dir() -> Option<std::path::PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        let candidate = dir.join(".bsql").join("queries");
        if candidate.is_dir() {
            return Some(candidate);
        }
        if !dir.pop() {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_flag_present() {
        let args = vec![
            "bsql".into(),
            "--database-url".into(),
            "postgres://localhost/test".into(),
        ];
        assert_eq!(
            parse_flag(&args, "--database-url"),
            Some("postgres://localhost/test".into())
        );
    }

    #[test]
    fn parse_flag_absent() {
        let args = vec!["bsql".into(), "migrate".into()];
        assert_eq!(parse_flag(&args, "--database-url"), None);
    }

    #[test]
    fn parse_flag_at_end_without_value() {
        // --database-url is the last arg with no value following
        let args = vec!["bsql".into(), "--database-url".into()];
        // windows(2) yields ["bsql","--database-url"] which doesn't match
        assert_eq!(parse_flag(&args, "--database-url"), None);
    }

    #[test]
    fn parse_flag_multiple_flags() {
        let args = vec![
            "bsql".into(),
            "migrate".into(),
            "--check".into(),
            "file.sql".into(),
            "--database-url".into(),
            "postgres://host/db".into(),
            "--cache-dir".into(),
            "/tmp/cache".into(),
        ];
        assert_eq!(
            parse_flag(&args, "--database-url"),
            Some("postgres://host/db".into())
        );
        assert_eq!(
            parse_flag(&args, "--cache-dir"),
            Some("/tmp/cache".into())
        );
    }

    #[test]
    fn parse_flag_empty_args() {
        let args: Vec<String> = vec![];
        assert_eq!(parse_flag(&args, "--database-url"), None);
    }

    #[test]
    fn parse_flag_single_arg() {
        let args = vec!["bsql".into()];
        assert_eq!(parse_flag(&args, "--database-url"), None);
    }

    #[test]
    fn find_cache_dir_walks_up() {
        // If .bsql/queries/ exists somewhere above CWD, find_cache_dir returns it.
        if let Some(dir) = find_cache_dir() {
            assert!(dir.exists());
            assert!(dir.is_dir());
        }
    }

    #[test]
    fn parse_flag_duplicate_takes_first() {
        let args = vec![
            "bsql".into(),
            "--database-url".into(),
            "first".into(),
            "--database-url".into(),
            "second".into(),
        ];
        assert_eq!(parse_flag(&args, "--database-url"), Some("first".into()));
    }
}
