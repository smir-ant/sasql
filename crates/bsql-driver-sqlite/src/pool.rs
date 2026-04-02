//! Connection pool — dedicated threads + crossbeam channels, WAL reader/writer split.
//!
//! SQLite connections are single-threaded (opened with `SQLITE_OPEN_NOMUTEX`).
//! The pool runs each connection on a dedicated OS thread, communicating via
//! crossbeam channels. No tokio dependency — async wrapping happens in bsql-core.
//!
//! # Architecture
//!
//! - **Writer thread**: one dedicated thread with a read-write connection.
//!   All INSERT/UPDATE/DELETE/DDL goes here.
//! - **Reader threads**: N dedicated threads with read-only connections.
//!   SELECT queries are round-robin distributed across readers.
//! - **Channel transport**: commands are sent as `Command` enums with pre-serialized
//!   parameters (`ParamValue`). Replies come back via `crossbeam_channel::bounded(1)`.
//!
//! # Fail-fast
//!
//! Per CREDO #17, `busy_timeout = 0` on all connections. If the writer is busy,
//! SQLite returns SQLITE_BUSY immediately. The pool does not queue or retry.
//!
//! # Safety
//!
//! This module contains **zero** `unsafe` code. The pool communicates with its
//! dedicated threads exclusively via crossbeam channels and atomic flags. No raw
//! SQLite pointers ever leave the dedicated threads.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use bsql_arena::{Arena, acquire_arena};
use crossbeam_channel::{Receiver, Sender, bounded};
use smallvec::SmallVec;

use crate::SqliteError;
use crate::codec::SqliteEncode;
use crate::conn::{QueryResult, SqliteConnection, hash_sql};
use crate::ffi::StmtHandle;

// --- ParamValue ---

/// Pre-serialized parameter for channel transport.
///
/// Avoids `Box<dyn SqliteEncode>` per parameter. Typical queries (<=8 params)
/// fit in `SmallVec<[ParamValue; 8]>` with zero heap allocation for the array.
#[derive(Debug, Clone)]
pub enum ParamValue {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Bool(bool),
}

impl SqliteEncode for ParamValue {
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        match self {
            ParamValue::Null => stmt.bind_null(idx),
            ParamValue::Int(v) => stmt.bind_int64(idx, *v),
            ParamValue::Real(v) => stmt.bind_double(idx, *v),
            ParamValue::Text(v) => stmt.bind_text(idx, v.as_str()),
            ParamValue::Blob(v) => stmt.bind_blob(idx, v.as_slice()),
            ParamValue::Bool(v) => stmt.bind_int64(idx, if *v { 1 } else { 0 }),
        }
    }
}

// --- Command ---

/// A command sent to a dedicated SQLite thread.
enum Command {
    Query {
        sql: String,
        sql_hash: u64,
        params: SmallVec<[ParamValue; 8]>,
        reply: Sender<Result<(QueryResult, Arena), SqliteError>>,
    },
    Execute {
        sql: String,
        sql_hash: u64,
        params: SmallVec<[ParamValue; 8]>,
        reply: Sender<Result<u64, SqliteError>>,
    },
    SimpleExec {
        sql: String,
        reply: Sender<Result<(), SqliteError>>,
    },
    PrepareOnly {
        sql: String,
        sql_hash: u64,
    },
    Shutdown,
}

// --- DedicatedThread ---

/// A dedicated OS thread running a single SQLite connection.
struct DedicatedThread {
    cmd_tx: Sender<Command>,
    handle: Option<thread::JoinHandle<()>>,
}

impl DedicatedThread {
    /// Spawn a dedicated thread with a writer (read-write) connection.
    fn spawn_writer(path: &str) -> Result<Self, SqliteError> {
        let path = path.to_owned();
        let (cmd_tx, cmd_rx) = bounded::<Command>(256);

        // Open the connection on the dedicated thread to ensure thread affinity.
        let (init_tx, init_rx) = bounded::<Result<(), SqliteError>>(1);

        let handle = thread::Builder::new()
            .name("bsql-sqlite-writer".into())
            .spawn(move || {
                let conn = match SqliteConnection::open(&path) {
                    Ok(c) => {
                        let _ = init_tx.send(Ok(()));
                        c
                    }
                    Err(e) => {
                        let _ = init_tx.send(Err(e));
                        return;
                    }
                };
                Self::run_loop(conn, cmd_rx);
            })
            .map_err(|e| SqliteError::Internal(format!("failed to spawn writer thread: {e}")))?;

        // Wait for the connection to be opened (or fail).
        init_rx
            .recv()
            .map_err(|_| SqliteError::Internal("writer thread exited during init".into()))??;

        Ok(Self {
            cmd_tx,
            handle: Some(handle),
        })
    }

    /// Spawn a dedicated thread with a reader (read-only) connection.
    fn spawn_reader(path: &str, idx: usize) -> Result<Self, SqliteError> {
        let path = path.to_owned();
        let (cmd_tx, cmd_rx) = bounded::<Command>(256);
        let (init_tx, init_rx) = bounded::<Result<(), SqliteError>>(1);

        let handle = thread::Builder::new()
            .name(format!("bsql-sqlite-reader-{idx}"))
            .spawn(move || {
                let conn = match SqliteConnection::open_readonly(&path) {
                    Ok(c) => {
                        let _ = init_tx.send(Ok(()));
                        c
                    }
                    Err(e) => {
                        let _ = init_tx.send(Err(e));
                        return;
                    }
                };
                Self::run_loop(conn, cmd_rx);
            })
            .map_err(|e| {
                SqliteError::Internal(format!("failed to spawn reader thread {idx}: {e}"))
            })?;

        init_rx.recv().map_err(|_| {
            SqliteError::Internal(format!("reader thread {idx} exited during init"))
        })??;

        Ok(Self {
            cmd_tx,
            handle: Some(handle),
        })
    }

    /// Command processing loop. Runs until Shutdown or channel disconnect.
    fn run_loop(mut conn: SqliteConnection, cmd_rx: Receiver<Command>) {
        while let Ok(cmd) = cmd_rx.recv() {
            match cmd {
                Command::Query {
                    sql,
                    sql_hash,
                    params,
                    reply,
                } => {
                    let mut arena = acquire_arena();
                    let param_refs: SmallVec<[&dyn SqliteEncode; 8]> =
                        params.iter().map(|p| p as &dyn SqliteEncode).collect();
                    let result = conn.query(&sql, sql_hash, &param_refs, &mut arena);
                    let _ = reply.send(result.map(|r| (r, arena)));
                }
                Command::Execute {
                    sql,
                    sql_hash,
                    params,
                    reply,
                } => {
                    let param_refs: SmallVec<[&dyn SqliteEncode; 8]> =
                        params.iter().map(|p| p as &dyn SqliteEncode).collect();
                    let result = conn.execute(&sql, sql_hash, &param_refs);
                    let _ = reply.send(result);
                }
                Command::SimpleExec { sql, reply } => {
                    let result = conn.exec(&sql);
                    let _ = reply.send(result);
                }
                Command::PrepareOnly { sql, sql_hash } => {
                    let _ = conn.prepare_only(&sql, sql_hash);
                }
                Command::Shutdown => break,
            }
        }
        // conn is dropped here — finalizes all statements and closes the database.
    }

    /// Send a query command and wait for the reply.
    fn query(
        &self,
        sql: &str,
        sql_hash: u64,
        params: SmallVec<[ParamValue; 8]>,
    ) -> Result<(QueryResult, Arena), SqliteError> {
        let (reply_tx, reply_rx) = bounded(1);
        self.cmd_tx
            .send(Command::Query {
                sql: sql.to_owned(),
                sql_hash,
                params,
                reply: reply_tx,
            })
            .map_err(|_| SqliteError::Pool("pool thread disconnected".into()))?;
        reply_rx
            .recv()
            .map_err(|_| SqliteError::Pool("pool thread disconnected".into()))?
    }

    /// Send an execute command and wait for the reply.
    fn execute(
        &self,
        sql: &str,
        sql_hash: u64,
        params: SmallVec<[ParamValue; 8]>,
    ) -> Result<u64, SqliteError> {
        let (reply_tx, reply_rx) = bounded(1);
        self.cmd_tx
            .send(Command::Execute {
                sql: sql.to_owned(),
                sql_hash,
                params,
                reply: reply_tx,
            })
            .map_err(|_| SqliteError::Pool("pool thread disconnected".into()))?;
        reply_rx
            .recv()
            .map_err(|_| SqliteError::Pool("pool thread disconnected".into()))?
    }

    /// Send a simple exec command and wait for the reply.
    fn simple_exec(&self, sql: &str) -> Result<(), SqliteError> {
        let (reply_tx, reply_rx) = bounded(1);
        self.cmd_tx
            .send(Command::SimpleExec {
                sql: sql.to_owned(),
                reply: reply_tx,
            })
            .map_err(|_| SqliteError::Pool("pool thread disconnected".into()))?;
        reply_rx
            .recv()
            .map_err(|_| SqliteError::Pool("pool thread disconnected".into()))?
    }

    /// Send a shutdown command and wait for the thread to exit.
    fn shutdown(&mut self) {
        let _ = self.cmd_tx.send(Command::Shutdown);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

// --- SqlitePool ---

/// Connection pool with dedicated threads — one writer + N readers.
///
/// Read queries are round-robin distributed across reader threads.
/// Write queries (INSERT/UPDATE/DELETE/DDL) go to the single writer thread.
///
/// # Thread safety
///
/// `SqlitePool` communicates with its dedicated threads exclusively via
/// crossbeam channels (`Sender<Command>`, which is `Send + Sync`) and atomic
/// flags (`Arc<AtomicBool>`, `AtomicUsize`). It does not hold any raw sqlite3
/// pointers — those live only on the dedicated threads.
///
/// # Example
///
/// ```no_run
/// use bsql_driver_sqlite::pool::SqlitePool;
///
/// let pool = SqlitePool::connect("/tmp/test.db").unwrap();
/// // Read queries go to reader threads
/// // Write queries go to the writer thread
/// pool.close();
/// ```
pub struct SqlitePool {
    readers: Vec<DedicatedThread>,
    writer: DedicatedThread,
    closed: Arc<AtomicBool>,
    reader_idx: AtomicUsize,
}

// SqlitePool auto-derives Send+Sync because all its fields are Send+Sync:
// - `DedicatedThread` contains `Sender<Command>` (Send+Sync) and
//   `Option<JoinHandle<()>>` (Send, not Sync — but the pool only accesses
//   JoinHandle from &mut self in Drop, so Sync is derived from Sender).
// - `Arc<AtomicBool>` (Send+Sync)
// - `AtomicUsize` (Send+Sync)
//
// No raw pointers, no SQLite handles — those live only on dedicated threads.

impl SqlitePool {
    /// Open a pool with default settings (4 reader threads).
    pub fn connect(path: &str) -> Result<Self, SqliteError> {
        SqlitePoolBuilder::new().path(path).build()
    }

    /// Create a pool builder for custom configuration.
    pub fn builder() -> SqlitePoolBuilder {
        SqlitePoolBuilder::new()
    }

    /// Route a read query to a reader thread (round-robin).
    pub fn query_readonly(
        &self,
        sql: &str,
        sql_hash: u64,
        params: SmallVec<[ParamValue; 8]>,
    ) -> Result<(QueryResult, Arena), SqliteError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteError::Pool("pool is closed".into()));
        }
        let idx = self.reader_idx.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        self.readers[idx].query(sql, sql_hash, params)
    }

    /// Route a write query to the writer thread.
    pub fn query_readwrite(
        &self,
        sql: &str,
        sql_hash: u64,
        params: SmallVec<[ParamValue; 8]>,
    ) -> Result<(QueryResult, Arena), SqliteError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteError::Pool("pool is closed".into()));
        }
        self.writer.query(sql, sql_hash, params)
    }

    /// Execute a write statement (INSERT/UPDATE/DELETE), return affected row count.
    pub fn execute(
        &self,
        sql: &str,
        sql_hash: u64,
        params: SmallVec<[ParamValue; 8]>,
    ) -> Result<u64, SqliteError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteError::Pool("pool is closed".into()));
        }
        self.writer.execute(sql, sql_hash, params)
    }

    /// Execute a simple SQL statement on the writer (PRAGMA, DDL).
    pub fn simple_exec(&self, sql: &str) -> Result<(), SqliteError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteError::Pool("pool is closed".into()));
        }
        self.writer.simple_exec(sql)
    }

    /// Pre-prepare statements on all threads (warmup).
    pub fn warmup(&self, sqls: &[&str]) {
        for sql in sqls {
            let sql_hash = hash_sql(sql);
            // Warmup on writer
            let _ = self.writer.cmd_tx.send(Command::PrepareOnly {
                sql: (*sql).to_owned(),
                sql_hash,
            });
            // Warmup on all readers
            for reader in &self.readers {
                let _ = reader.cmd_tx.send(Command::PrepareOnly {
                    sql: (*sql).to_owned(),
                    sql_hash,
                });
            }
        }
    }

    /// Number of reader threads.
    pub fn reader_count(&self) -> usize {
        self.readers.len()
    }

    /// Whether the pool has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Close the pool. Shuts down all threads.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

impl Drop for SqlitePool {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.writer.shutdown();
        for reader in &mut self.readers {
            reader.shutdown();
        }
    }
}

// --- SqlitePoolBuilder ---

/// Builder for configuring a SQLite connection pool.
pub struct SqlitePoolBuilder {
    path: Option<String>,
    reader_count: usize,
}

impl SqlitePoolBuilder {
    fn new() -> Self {
        Self {
            path: None,
            reader_count: 4,
        }
    }

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

    /// Build and open the pool. Spawns dedicated threads and opens connections.
    pub fn build(self) -> Result<SqlitePool, SqliteError> {
        let path = self
            .path
            .ok_or_else(|| SqliteError::Pool("pool builder requires a path".into()))?;

        let reader_count = if self.reader_count == 0 {
            1 // need at least one reader
        } else {
            self.reader_count
        };

        // Spawn writer first (creates the database if needed, sets WAL mode)
        let writer = DedicatedThread::spawn_writer(&path)?;

        // Spawn readers
        let mut readers = Vec::with_capacity(reader_count);
        for i in 0..reader_count {
            match DedicatedThread::spawn_reader(&path, i) {
                Ok(reader) => readers.push(reader),
                Err(e) => {
                    // Clean up already-spawned threads on failure
                    for mut r in readers {
                        r.shutdown();
                    }
                    // writer is dropped here — thread will exit when channel disconnects
                    return Err(e);
                }
            }
        }

        Ok(SqlitePool {
            readers,
            writer,
            closed: Arc::new(AtomicBool::new(false)),
            reader_idx: AtomicUsize::new(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db_path() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir();
        format!("{}/bsql_test_pool_{}.db", dir.display(), id)
    }

    #[test]
    fn pool_connect_and_close() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        assert!(!pool.is_closed());
        assert_eq!(pool.reader_count(), 4);
        pool.close();
        assert!(pool.is_closed());
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_builder_custom_readers() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(2)
            .build()
            .unwrap();
        assert_eq!(pool.reader_count(), 2);
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_builder_requires_path() {
        let result = SqlitePoolBuilder::new().build();
        assert!(result.is_err());
        match result {
            Err(SqliteError::Pool(msg)) => assert!(msg.contains("path")),
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn pool_builder_zero_readers_becomes_one() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(0)
            .build()
            .unwrap();
        assert_eq!(pool.reader_count(), 1);
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_simple_exec() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_execute_and_query() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();

        // Insert via writer
        let sql_ins = "INSERT INTO t VALUES (?1, ?2)";
        let hash_ins = hash_sql(sql_ins);
        let params: SmallVec<[ParamValue; 8]> =
            smallvec::smallvec![ParamValue::Int(1), ParamValue::Text("alice".into())];
        let affected = pool.execute(sql_ins, hash_ins, params).unwrap();
        assert_eq!(affected, 1);

        // Second insert
        let params2: SmallVec<[ParamValue; 8]> =
            smallvec::smallvec![ParamValue::Int(2), ParamValue::Text("bob".into())];
        pool.execute(sql_ins, hash_ins, params2).unwrap();

        // Read via reader
        let sql_sel = "SELECT id, name FROM t ORDER BY id";
        let hash_sel = hash_sql(sql_sel);
        let (result, arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_str(0, 1, &arena), Some("alice"));
        assert_eq!(result.get_i64(1, 0, &arena), Some(2));
        assert_eq!(result.get_str(1, 1, &arena), Some("bob"));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_query_readwrite() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (42)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let (result, arena) = pool.query_readwrite(sql, hash, SmallVec::new()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_i64(0, 0, &arena), Some(42));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_closed_rejects_queries() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.close();

        let result = pool.query_readonly("SELECT 1", 0, SmallVec::new());
        assert!(result.is_err());
        match result {
            Err(SqliteError::Pool(msg)) => assert!(msg.contains("closed")),
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }

        let result = pool.query_readwrite("SELECT 1", 0, SmallVec::new());
        assert!(result.is_err());

        let result = pool.execute("SELECT 1", 0, SmallVec::new());
        assert!(result.is_err());

        let result = pool.simple_exec("SELECT 1");
        assert!(result.is_err());

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_round_robin_readers() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(2)
            .build()
            .unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);

        // Issue multiple reads — they should round-robin across 2 readers
        for _ in 0..4 {
            let (result, arena) = pool.query_readonly(sql, hash, SmallVec::new()).unwrap();
            assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        }

        // reader_idx should have advanced
        let idx = pool.reader_idx.load(Ordering::Relaxed);
        assert_eq!(idx, 4);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_warmup() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        // Warmup should not fail
        pool.warmup(&["SELECT id FROM t WHERE id = ?1"]);

        // Use the warmed-up statement
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();
        let sql = "SELECT id FROM t WHERE id = ?1";
        let hash = hash_sql(sql);
        let (result, arena) = pool
            .query_readonly(sql, hash, smallvec::smallvec![ParamValue::Int(1)])
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_null_params() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();

        let sql = "INSERT INTO t VALUES (?1, ?2)";
        let hash = hash_sql(sql);
        let params: SmallVec<[ParamValue; 8]> =
            smallvec::smallvec![ParamValue::Int(1), ParamValue::Null];
        pool.execute(sql, hash, params).unwrap();

        let sql_sel = "SELECT id, name FROM t";
        let hash_sel = hash_sql(sql_sel);
        let (result, arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert!(result.is_null(0, 1));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_param_value_all_types() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec(
            "CREATE TABLE t (a INTEGER, b REAL, c TEXT, d BLOB, e INTEGER, f INTEGER)",
        )
        .unwrap();

        let sql = "INSERT INTO t VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
        let hash = hash_sql(sql);
        let params: SmallVec<[ParamValue; 8]> = smallvec::smallvec![
            ParamValue::Int(99),
            ParamValue::Real(1.5),
            ParamValue::Text("test".into()),
            ParamValue::Blob(vec![0xAB, 0xCD]),
            ParamValue::Bool(true),
            ParamValue::Null,
        ];
        pool.execute(sql, hash, params).unwrap();

        let sql_sel = "SELECT a, b, c, d, e, f FROM t";
        let hash_sel = hash_sql(sql_sel);
        let (result, arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();

        assert_eq!(result.get_i64(0, 0, &arena), Some(99));
        assert!((result.get_f64(0, 1, &arena).unwrap() - 1.5).abs() < f64::EPSILON);
        assert_eq!(result.get_str(0, 2, &arena), Some("test"));
        assert_eq!(result.get_bytes(0, 3, &arena), Some(&[0xAB, 0xCD][..]));
        assert_eq!(result.get_bool(0, 4, &arena), Some(true));
        assert!(result.is_null(0, 5));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }
}
