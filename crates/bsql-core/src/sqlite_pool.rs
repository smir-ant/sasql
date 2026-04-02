//! SQLite connection pool — async wrapper over `bsql_driver_sqlite::pool::SqlitePool`.
//!
//! The driver pool uses dedicated OS threads and crossbeam channels. This
//! wrapper provides an async-compatible API by spawning blocking tasks on
//! tokio's blocking thread pool.

use std::sync::Arc;

use crate::error::{BsqlError, BsqlResult};

/// A SQLite connection pool.
///
/// Wraps `bsql_driver_sqlite::pool::SqlitePool` with bsql error types
/// and an async-compatible API.
///
/// The driver pool is `Send + Sync` (asserted in bsql-driver-sqlite)
/// because it communicates with its threads via crossbeam channels and
/// atomic flags only.
pub struct SqlitePool {
    inner: Arc<bsql_driver_sqlite::pool::SqlitePool>,
}

/// Builder for configuring a SQLite connection pool.
pub struct SqlitePoolBuilder {
    path: Option<String>,
    reader_count: usize,
}

impl SqlitePoolBuilder {
    /// Set the database file path.
    pub fn path(mut self, path: &str) -> Self {
        self.path = Some(path.to_owned());
        self
    }

    /// Set the number of reader threads. Default: 4.
    pub fn reader_count(mut self, count: usize) -> Self {
        self.reader_count = count;
        self
    }

    /// Build and open the pool.
    pub fn build(self) -> BsqlResult<SqlitePool> {
        let path = self.path.ok_or_else(|| {
            BsqlError::Connect(crate::error::ConnectError {
                message: "SQLite pool builder requires a path".into(),
                source: None,
            })
        })?;

        let inner = bsql_driver_sqlite::pool::SqlitePool::builder()
            .path(&path)
            .reader_count(self.reader_count)
            .build()
            .map_err(BsqlError::from_sqlite)?;

        Ok(SqlitePool {
            inner: Arc::new(inner),
        })
    }
}

impl SqlitePool {
    /// Open a SQLite pool with default settings (4 reader threads).
    pub fn connect(path: &str) -> BsqlResult<Self> {
        let inner =
            bsql_driver_sqlite::pool::SqlitePool::connect(path).map_err(BsqlError::from_sqlite)?;
        Ok(SqlitePool {
            inner: Arc::new(inner),
        })
    }

    /// Create a pool builder for custom configuration.
    pub fn builder() -> SqlitePoolBuilder {
        SqlitePoolBuilder {
            path: None,
            reader_count: 4,
        }
    }

    /// Execute a read-only query via the async wrapper.
    ///
    /// Routes to a reader thread in the pool. Returns the `QueryResult`
    /// and its associated `Arena`.
    pub async fn query_readonly(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<(bsql_driver_sqlite::conn::QueryResult, bsql_arena::Arena)> {
        let pool = Arc::clone(&self.inner);
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            pool.query_readonly(&sql, sql_hash, params)
                .map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })?
    }

    /// Execute a read-write query via the async wrapper.
    pub async fn query_readwrite(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<(bsql_driver_sqlite::conn::QueryResult, bsql_arena::Arena)> {
        let pool = Arc::clone(&self.inner);
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            pool.query_readwrite(&sql, sql_hash, params)
                .map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })?
    }

    /// Execute a write statement (INSERT/UPDATE/DELETE), return affected row count.
    pub async fn execute_sql(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<u64> {
        let pool = Arc::clone(&self.inner);
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            pool.execute(&sql, sql_hash, params)
                .map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })?
    }

    /// Execute a simple SQL statement on the writer (PRAGMA, DDL).
    pub async fn simple_exec(&self, sql: &str) -> BsqlResult<()> {
        let pool = Arc::clone(&self.inner);
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || pool.simple_exec(&sql).map_err(BsqlError::from_sqlite))
            .await
            .map_err(|e| {
                BsqlError::Query(crate::error::QueryError {
                    message: format!("SQLite task panicked: {e}").into(),
                    pg_code: None,
                    source: None,
                })
            })?
    }

    /// Pre-prepare statements on all threads (warmup).
    pub fn warmup(&self, sqls: &[&str]) {
        self.inner.warmup(sqls);
    }

    /// Number of reader threads.
    pub fn reader_count(&self) -> usize {
        self.inner.reader_count()
    }

    /// Whether the pool has been closed.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Close the pool.
    pub fn close(&self) {
        self.inner.close();
    }
}

impl Clone for SqlitePool {
    fn clone(&self) -> Self {
        SqlitePool {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Debug for SqlitePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlitePool")
            .field("reader_count", &self.inner.reader_count())
            .field("closed", &self.inner.is_closed())
            .finish()
    }
}
