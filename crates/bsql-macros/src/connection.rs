//! Shared compile-time database connection for proc macro validation.
//!
//! Uses `LazyLock` to maintain a single connection across all `query!`
//! invocations within one `cargo build`. The first invocation pays ~5ms
//! for the connection. Subsequent invocations reuse it at ~0 cost.
//!
//! # Backend detection
//!
//! The URL scheme in `BSQL_DATABASE_URL` / `DATABASE_URL` determines the backend:
//! - `postgres://` or `postgresql://` -> PostgreSQL (async via tokio)
//! - `sqlite:` -> SQLite (synchronous, no tokio needed)
//!   - `sqlite:///absolute/path` -> absolute path
//!   - `sqlite:./relative/path` or `sqlite:relative/path` -> relative to CARGO_MANIFEST_DIR
//!   - `sqlite::memory:` -> in-memory (for tests)
//!
//! # TLS support (PostgreSQL only)
//!
//! When the `tls` feature is enabled on `bsql`, the compile-time connection
//! uses `rustls` for encrypted connections. The connection URL can include
//! `?sslmode=require` to enforce TLS:
//!
//! ```text
//! BSQL_DATABASE_URL=postgres://user:pass@host/db?sslmode=require
//! ```
//!
//! Without the `tls` feature, connections use `NoTls` and `sslmode=require`
//! will fail with a connection error.

use std::sync::LazyLock;
use tokio::runtime::Runtime;

use bsql_driver_postgres::Connection;

/// Detected backend from the database URL scheme.
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Postgres,
    Sqlite,
}

/// Detect the backend from the database URL.
///
/// Returns `None` if no URL is set (offline mode handles this separately).
#[cfg(feature = "sqlite")]
pub fn detect_backend_from_url(url: &str) -> Result<Backend, String> {
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        return Ok(Backend::Postgres);
    }

    if url.starts_with("sqlite:") {
        return Ok(Backend::Sqlite);
    }

    Err(format!(
        "bsql: unrecognized database URL scheme. Expected 'postgres://', \
         'postgresql://', or 'sqlite:'. Got: {url}"
    ))
}

/// Detect the backend from environment variables.
///
/// Returns `None` if no database URL is set.
#[cfg(feature = "sqlite")]
pub fn detect_backend() -> Result<Option<Backend>, String> {
    let url = match std::env::var("BSQL_DATABASE_URL").or_else(|_| std::env::var("DATABASE_URL")) {
        Ok(url) => url,
        Err(_) => return Ok(None), // no URL -> offline mode
    };
    detect_backend_from_url(&url).map(Some)
}

// ---------------------------------------------------------------------------
// PostgreSQL connection
// ---------------------------------------------------------------------------

/// The shared PG connection state, initialized once per compilation.
struct MacroConnection {
    runtime: Runtime,
    conn: std::sync::Mutex<Connection>,
}

/// The global shared PG connection. Initialized on first access.
static MACRO_CONN: LazyLock<Result<MacroConnection, String>> = LazyLock::new(|| {
    let database_url = std::env::var("BSQL_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map_err(|_| {
            "bsql: BSQL_DATABASE_URL or DATABASE_URL must be set for compile-time \
             SQL validation. Set one of these environment variables to a PostgreSQL \
             connection URL (e.g. postgres://user:pass@localhost/mydb)."
                .to_string()
        })?;

    let rt = Runtime::new().map_err(|e| format!("bsql: failed to create tokio runtime: {e}"))?;

    // Parse URL and configure TLS via our driver's Config
    let mut config = bsql_driver_postgres::Config::from_url(&database_url)
        .map_err(|e| format!("bsql: invalid DATABASE_URL: {e}"))?;

    // TLS: when the `tls` feature is not enabled, force SslMode::Disable
    // so our driver does not attempt a TLS upgrade.
    #[cfg(not(feature = "tls"))]
    {
        config.ssl = bsql_driver_postgres::SslMode::Disable;
    }

    // Compile-time queries should time out, not hang the build.
    config.statement_timeout_secs = 30;

    let conn = rt.block_on(Connection::connect(&config)).map_err(|e| {
        format!(
            "bsql: failed to connect to PostgreSQL at compile time: {e}. \
                 Check that BSQL_DATABASE_URL or DATABASE_URL is set correctly \
                 and the database is running."
        )
    })?;

    Ok(MacroConnection {
        runtime: rt,
        conn: std::sync::Mutex::new(conn),
    })
});

/// Run an async operation on the shared compile-time PG connection.
///
/// Returns a `syn::Error` if the connection is not available.
pub fn with_connection<F, T>(f: F) -> Result<T, syn::Error>
where
    F: FnOnce(&Runtime, &mut Connection) -> Result<T, String>,
{
    let mc = MACRO_CONN
        .as_ref()
        .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?;
    let mut conn = mc.conn.lock().unwrap_or_else(|e| e.into_inner());
    f(&mc.runtime, &mut conn).map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))
}

// ---------------------------------------------------------------------------
// SQLite connection
// ---------------------------------------------------------------------------

/// Resolve a SQLite URL (`sqlite:...`) to a filesystem path.
///
/// - `sqlite:///absolute/path` -> `/absolute/path`
/// - `sqlite:./relative/path` -> `{CARGO_MANIFEST_DIR}/./relative/path`
/// - `sqlite:relative/path` -> `{CARGO_MANIFEST_DIR}/relative/path`
/// - `sqlite::memory:` -> `:memory:`
#[cfg(feature = "sqlite")]
pub fn resolve_sqlite_path(url: &str) -> Result<String, String> {
    let rest = url
        .strip_prefix("sqlite:")
        .ok_or_else(|| format!("bsql: not a sqlite URL: {url}"))?;

    if rest == ":memory:" {
        return Ok(":memory:".to_owned());
    }

    if let Some(path) = rest.strip_prefix("//") {
        // sqlite:///absolute/path -> /absolute/path
        return Ok(path.to_owned());
    }

    // Relative path: resolve from CARGO_MANIFEST_DIR
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").map_err(|_| {
        "bsql: CARGO_MANIFEST_DIR not set (required for relative sqlite paths)".to_owned()
    })?;
    let full_path = std::path::Path::new(&manifest_dir).join(rest);
    Ok(full_path.to_string_lossy().into_owned())
}

/// Run a synchronous operation on the compile-time SQLite connection.
///
/// Opens the connection on first use and caches it in a thread-local.
/// `SqliteConnection` is `!Send + !Sync` (raw FFI handle), so it cannot
/// live in a `static`. Thread-local storage is correct here because proc
/// macro expansion is single-threaded per compilation unit.
///
/// Returns a `syn::Error` if the connection cannot be opened.
#[cfg(feature = "sqlite")]
pub fn with_sqlite_connection<F, T>(f: F) -> Result<T, syn::Error>
where
    F: FnOnce(&mut bsql_driver_sqlite::conn::SqliteConnection) -> Result<T, String>,
{
    use std::cell::RefCell;

    thread_local! {
        static SQLITE_CONN: RefCell<Option<Result<bsql_driver_sqlite::conn::SqliteConnection, String>>> = const { RefCell::new(None) };
    }

    SQLITE_CONN.with(|cell| {
        let mut borrow = cell.borrow_mut();

        // Initialize on first access
        if borrow.is_none() {
            let result = (|| -> Result<bsql_driver_sqlite::conn::SqliteConnection, String> {
                let database_url = std::env::var("BSQL_DATABASE_URL")
                    .or_else(|_| std::env::var("DATABASE_URL"))
                    .map_err(|_| {
                        "bsql: BSQL_DATABASE_URL or DATABASE_URL must be set for compile-time \
                         SQL validation. Set one of these environment variables to a SQLite \
                         connection URL (e.g. sqlite:./mydb.db or sqlite::memory:)."
                            .to_string()
                    })?;

                let path = resolve_sqlite_path(&database_url)?;

                bsql_driver_sqlite::conn::SqliteConnection::open(&path).map_err(|e| {
                    format!(
                        "bsql: failed to open SQLite database at compile time: {e}. \
                         Path: {path}"
                    )
                })
            })();
            *borrow = Some(result);
        }

        let conn = borrow
            .as_mut()
            .unwrap()
            .as_mut()
            .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg.clone()))?;

        f(conn).map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))
    })
}
