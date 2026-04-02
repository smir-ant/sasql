//! Connection pool — thin wrapper over `bsql_driver_postgres::Pool`.
//!
//! Delegates all connection management, fail-fast semantics, and LIFO ordering
//! to the driver. This layer adds only the bsql error type conversions.

use std::time::Duration;

use bsql_driver_postgres::arena::acquire_arena;
use bsql_driver_postgres::codec::Encode;
use tokio::sync::Mutex;

use crate::error::{BsqlError, BsqlResult};
use crate::stream::QueryStream;
use crate::transaction::Transaction;

/// A PostgreSQL connection pool.
///
/// Wraps `bsql_driver_postgres::Pool` with bsql error types and the `Executor` trait.
pub struct Pool {
    pub(crate) inner: bsql_driver_postgres::Pool,
}

/// Builder for configuring a connection pool.
pub struct PoolBuilder {
    url: Option<String>,
    max_size: usize,
    max_lifetime: Option<Option<Duration>>,
    acquire_timeout: Option<Option<Duration>>,
    min_idle: Option<usize>,
}

impl PoolBuilder {
    /// Configure the pool from a PostgreSQL connection URL.
    ///
    /// Format: `postgres://user:password@host:port/dbname`
    pub fn url(mut self, url: &str) -> Self {
        self.url = Some(url.into());
        self
    }

    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// Set the maximum lifetime of a connection. Connections older than this
    /// are discarded when returned to the pool. Default: 30 minutes.
    ///
    /// Pass `None` for unlimited lifetime.
    pub fn max_lifetime(mut self, d: Option<Duration>) -> Self {
        self.max_lifetime = Some(d);
        self
    }

    /// Set the maximum time to wait for a connection when the pool is
    /// exhausted. Default: 5 seconds.
    ///
    /// Pass `None` for fail-fast behavior (no waiting, immediate error).
    pub fn acquire_timeout(mut self, d: Option<Duration>) -> Self {
        self.acquire_timeout = Some(d);
        self
    }

    /// Set the minimum number of idle connections to maintain. Default: 0.
    ///
    /// When greater than 0, a background task creates connections as needed
    /// to maintain this idle floor.
    pub fn min_idle(mut self, n: usize) -> Self {
        self.min_idle = Some(n);
        self
    }

    pub async fn build(self) -> BsqlResult<Pool> {
        let url = self.url.ok_or_else(|| {
            BsqlError::from(bsql_driver_postgres::DriverError::Pool(
                "pool builder requires a URL".into(),
            ))
        })?;
        let mut builder = bsql_driver_postgres::Pool::builder()
            .url(&url)
            .max_size(self.max_size);

        if let Some(lt) = self.max_lifetime {
            builder = builder.max_lifetime(lt);
        }
        if let Some(at) = self.acquire_timeout {
            builder = builder.acquire_timeout(at);
        }
        if let Some(mi) = self.min_idle {
            builder = builder.min_idle(mi);
        }

        let inner = builder.build().await.map_err(BsqlError::from)?;
        Ok(Pool { inner })
    }
}

impl Pool {
    /// Connect to PostgreSQL using a connection URL.
    ///
    /// Format: `postgres://user:password@host:port/dbname`
    pub async fn connect(url: &str) -> BsqlResult<Self> {
        let inner = bsql_driver_postgres::Pool::connect(url)
            .await
            .map_err(BsqlError::from)?;
        Ok(Pool { inner })
    }

    /// Create a pool builder for fine-grained configuration.
    pub fn builder() -> PoolBuilder {
        PoolBuilder {
            url: None,
            max_size: 10,
            max_lifetime: None,
            acquire_timeout: None,
            min_idle: None,
        }
    }

    /// Acquire a connection from the pool.
    ///
    /// **Fail-fast**: returns `BsqlError::Pool` immediately if no connections
    /// are available (unless `acquire_timeout` is configured).
    pub async fn acquire(&self) -> BsqlResult<PoolConnection> {
        let guard = self.inner.acquire().await.map_err(BsqlError::from)?;
        Ok(PoolConnection {
            inner: Mutex::new(guard),
        })
    }

    /// Begin a new transaction.
    ///
    /// Acquires a connection and sends BEGIN immediately.
    pub async fn begin(&self) -> BsqlResult<Transaction> {
        let tx = self.inner.begin().await.map_err(BsqlError::from)?;
        Ok(Transaction::from_driver(tx))
    }

    /// Execute a query and return a stream of rows.
    ///
    /// Acquires a connection from the pool and returns a [`QueryStream`]
    /// that holds the connection alive until the stream is consumed or dropped.
    ///
    /// Uses true PG-level streaming via `Execute(max_rows=64)`. Only 64 rows
    /// are in memory at a time. The stream fetches additional chunks on demand
    /// via the `PortalSuspended` / re-`Execute` protocol.
    pub async fn query_stream(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> BsqlResult<QueryStream> {
        let mut guard = self.inner.acquire().await.map_err(BsqlError::from)?;
        let mut arena = acquire_arena();

        // chunk_size=64 rows per Execute call
        const CHUNK_SIZE: i32 = 64;

        let (columns, _) = guard
            .query_streaming_start(sql, sql_hash, params, CHUNK_SIZE)
            .await
            .map_err(BsqlError::from)?;

        let num_cols = columns.len();
        let mut all_col_offsets: Vec<(usize, i32)> =
            Vec::with_capacity(num_cols * CHUNK_SIZE as usize);

        let more = guard
            .streaming_next_chunk(&mut arena, &mut all_col_offsets)
            .await
            .map_err(BsqlError::from)?;

        let first_result = bsql_driver_postgres::QueryResult::from_parts(
            all_col_offsets,
            num_cols,
            columns.clone(),
            0,
        );

        Ok(QueryStream::new(guard, arena, first_result, columns, !more))
    }

    /// Set the SQL statements to pre-PREPARE on new connections.
    ///
    /// Each SQL string is PREPAREd on new connections before they are returned
    /// from `acquire()`. This eliminates first-use Parse overhead for hot queries.
    ///
    /// Warmup errors are silently ignored — a bad warmup SQL does not prevent
    /// the connection from being usable.
    pub fn set_warmup_sqls(&self, sqls: &[&str]) {
        self.inner.set_warmup_sqls(sqls);
    }

    /// Pool status metrics: idle, active, open, and max_size.
    ///
    /// Returns detailed pool utilization metrics from the driver.
    pub fn status(&self) -> PoolStatus {
        let driver_status = self.inner.status();
        PoolStatus {
            idle: driver_status.idle,
            active: driver_status.active,
            open: driver_status.open,
            max_size: driver_status.max_size,
        }
    }

    /// Gracefully close the pool.
    ///
    /// No new connections can be acquired after this call. All idle connections
    /// are closed immediately. Active connections are closed when returned to
    /// the pool.
    pub async fn close(&self) {
        self.inner.close().await;
    }

    /// Whether the pool has been closed.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        Pool {
            inner: self.inner.clone(),
        }
    }
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("status", &self.status())
            .finish()
    }
}

/// A connection borrowed from the pool.
///
/// Uses `tokio::sync::Mutex` for interior mutability because the driver's
/// `Connection` requires `&mut self` for queries, but the `Executor` trait
/// takes `&self`. The mutex is uncontended in practice — a single connection
/// is used by one task at a time, never shared between concurrent tasks.
/// `tokio::sync::Mutex` is needed (over `RefCell`) because the future holding
/// the guard must be `Send` for tokio task migration between worker threads.
///
/// Returned to the pool when dropped.
pub struct PoolConnection {
    pub(crate) inner: Mutex<bsql_driver_postgres::PoolGuard>,
}

/// Snapshot of pool utilization.
#[derive(Debug, Clone, Copy)]
pub struct PoolStatus {
    /// Number of idle connections in the pool.
    pub idle: usize,
    /// Number of connections currently in use.
    pub active: usize,
    /// Total open connections (idle + active).
    pub open: usize,
    /// Maximum pool size.
    pub max_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let b = Pool::builder();
        assert_eq!(b.max_size, 10);
        assert!(b.max_lifetime.is_none());
        assert!(b.acquire_timeout.is_none());
        assert!(b.min_idle.is_none());
    }

    #[test]
    fn builder_max_lifetime() {
        let b = Pool::builder().max_lifetime(Some(Duration::from_secs(60)));
        assert_eq!(b.max_lifetime, Some(Some(Duration::from_secs(60))));
    }

    #[test]
    fn builder_max_lifetime_none_disables() {
        let b = Pool::builder().max_lifetime(None);
        assert_eq!(b.max_lifetime, Some(None));
    }

    #[test]
    fn builder_acquire_timeout() {
        let b = Pool::builder().acquire_timeout(Some(Duration::from_secs(3)));
        assert_eq!(b.acquire_timeout, Some(Some(Duration::from_secs(3))));
    }

    #[test]
    fn builder_acquire_timeout_none_disables() {
        let b = Pool::builder().acquire_timeout(None);
        assert_eq!(b.acquire_timeout, Some(None));
    }

    #[test]
    fn builder_min_idle() {
        let b = Pool::builder().min_idle(5);
        assert_eq!(b.min_idle, Some(5));
    }
}
