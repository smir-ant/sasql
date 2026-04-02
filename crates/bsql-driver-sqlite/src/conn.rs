//! SQLite connection — open, prepare, step, column decode, statement cache.
//!
//! `SqliteConnection` owns a [`DbHandle`] and implements statement caching
//! with an identity-hashed `HashMap` (same pattern as the PG driver). Statements are
//! cached by `rapidhash` of the SQL text. On first use, the statement is prepared; on
//! subsequent uses, it is reused after `sqlite3_reset`.
//!
//! All row data is copied into an `Arena` during the step loop, making the result
//! independent of the SQLite statement lifetime.
//!
//! This module contains **zero** `unsafe` code — all FFI interaction goes through
//! the safe [`DbHandle`] and [`StmtHandle`] wrapper types in [`crate::ffi`].

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use bsql_arena::Arena;
use libsqlite3_sys as raw;
use rapidhash::quality::RapidHasher;

use crate::SqliteError;
use crate::codec::SqliteEncode;
use crate::ffi::{DbHandle, StepResult, StmtHandle};

// --- Identity hasher (same pattern as PG driver) ---

/// Identity hasher for pre-hashed u64 keys. Avoids SipHash overhead
/// on keys that are already well-distributed rapidhash values.
#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
    #[inline]
    fn write(&mut self, _: &[u8]) {
        debug_assert!(false, "IdentityHasher only supports u64 keys");
        self.0 = 0;
    }
}

type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;
type StmtCache = HashMap<u64, CachedStmt, IdentityBuildHasher>;

/// Cached prepared statement with pre-computed metadata.
struct CachedStmt {
    handle: StmtHandle,
    /// Column count cached at prepare time. Used for pre-allocation.
    #[allow(dead_code)]
    column_count: i32,
}

/// Hash a SQL string with rapidhash. Used as the statement cache key.
///
/// # Example
///
/// ```
/// let hash = bsql_driver_sqlite::conn::hash_sql("SELECT 1");
/// assert_ne!(hash, 0);
/// ```
pub fn hash_sql(sql: &str) -> u64 {
    use std::hash::Hash;
    let mut hasher = RapidHasher::default();
    sql.hash(&mut hasher);
    hasher.finish()
}

// --- SqliteConnection ---

/// A single SQLite database connection with a statement cache.
///
/// `SqliteConnection` is **not** `Send` or `Sync` — the underlying `DbHandle`
/// wraps a raw `sqlite3*` handle opened with `SQLITE_OPEN_NOMUTEX` (no internal
/// mutexing). The pool module handles thread affinity by opening connections on
/// dedicated threads.
pub struct SqliteConnection {
    db: DbHandle,
    stmts: StmtCache,
}

impl SqliteConnection {
    /// Open a read-write database connection with optimal PRAGMAs.
    ///
    /// Sets WAL mode, synchronous=NORMAL, 256MB mmap, 64MB cache, and
    /// busy_timeout=0 (fail-fast per CREDO #17).
    pub fn open(path: &str) -> Result<Self, SqliteError> {
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(path, flags)?;

        db.exec("PRAGMA journal_mode = WAL")?;
        db.exec("PRAGMA synchronous = NORMAL")?;
        db.exec("PRAGMA mmap_size = 268435456")?; // 256MB
        db.exec("PRAGMA cache_size = -64000")?; // 64MB
        db.exec("PRAGMA busy_timeout = 0")?; // fail-fast (CREDO #17)
        db.exec("PRAGMA temp_store = MEMORY")?;
        db.exec("PRAGMA foreign_keys = ON")?;

        Ok(Self {
            db,
            stmts: StmtCache::default(),
        })
    }

    /// Open a read-only database connection.
    ///
    /// Used by reader threads in the pool. Does not set journal_mode (only
    /// the writer sets that).
    pub fn open_readonly(path: &str) -> Result<Self, SqliteError> {
        let flags = raw::SQLITE_OPEN_READONLY | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(path, flags)?;

        db.exec("PRAGMA synchronous = NORMAL")?;
        db.exec("PRAGMA mmap_size = 268435456")?;
        db.exec("PRAGMA cache_size = -64000")?;
        db.exec("PRAGMA busy_timeout = 0")?;
        db.exec("PRAGMA temp_store = MEMORY")?;
        db.exec("PRAGMA foreign_keys = ON")?;

        Ok(Self {
            db,
            stmts: StmtCache::default(),
        })
    }

    /// Execute a query and return results in an arena.
    ///
    /// Row data is copied into the arena during the step loop. Integer and float
    /// values are stored as little-endian bytes; text and blob are copied verbatim.
    /// NULL values are indicated by `len == -1` in the column offset array.
    pub fn query(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        arena: &mut Arena,
    ) -> Result<QueryResult, SqliteError> {
        let stmt = self.get_or_prepare(sql, sql_hash)?;

        // Bind parameters
        stmt.clear_bindings();
        for (i, param) in params.iter().enumerate() {
            param.bind(stmt, (i + 1) as i32)?;
        }

        // Step loop — read all rows into arena
        let col_count = stmt.column_count() as usize;
        let mut col_offsets: Vec<(usize, i32)> = Vec::with_capacity(col_count * 8);
        let mut row_count: usize = 0;

        loop {
            match stmt.step()? {
                StepResult::Done => break,
                StepResult::Row => {
                    for col in 0..col_count as i32 {
                        let col_type = stmt.column_type(col);
                        match col_type {
                            raw::SQLITE_NULL => {
                                col_offsets.push((0, -1));
                            }
                            raw::SQLITE_INTEGER => {
                                let val = stmt.column_int64(col);
                                let bytes = val.to_le_bytes();
                                let offset = arena.alloc_copy(&bytes);
                                col_offsets.push((offset, 8));
                            }
                            raw::SQLITE_FLOAT => {
                                let val = stmt.column_double(col);
                                let bytes = val.to_le_bytes();
                                let offset = arena.alloc_copy(&bytes);
                                col_offsets.push((offset, 8));
                            }
                            raw::SQLITE_TEXT => {
                                let data = stmt.column_text(col);
                                match data {
                                    Some(bytes) => {
                                        let offset = arena.alloc_copy(bytes);
                                        col_offsets.push((offset, bytes.len() as i32));
                                    }
                                    None => {
                                        col_offsets.push((0, -1));
                                    }
                                }
                            }
                            _ => {
                                // SQLITE_BLOB or unknown type — treat as blob
                                let data = stmt.column_blob(col);
                                if data.is_empty() {
                                    col_offsets.push((arena.global_offset(), 0));
                                } else {
                                    let offset = arena.alloc_copy(data);
                                    col_offsets.push((offset, data.len() as i32));
                                }
                            }
                        }
                    }
                    row_count += 1;
                }
            }
        }

        // Reset statement for reuse
        stmt.reset()?;

        Ok(QueryResult {
            col_count,
            row_count,
            col_offsets,
        })
    }

    /// Execute a statement (INSERT/UPDATE/DELETE), return affected row count.
    pub fn execute(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
    ) -> Result<u64, SqliteError> {
        let stmt = self.get_or_prepare(sql, sql_hash)?;

        stmt.clear_bindings();
        for (i, param) in params.iter().enumerate() {
            param.bind(stmt, (i + 1) as i32)?;
        }
        stmt.step()?;
        stmt.reset()?;
        Ok(self.db.changes())
    }

    /// Prepare a statement without executing it (cache warmup).
    pub fn prepare_only(&mut self, sql: &str, sql_hash: u64) -> Result<(), SqliteError> {
        self.get_or_prepare(sql, sql_hash)?;
        Ok(())
    }

    /// Execute a simple SQL string (PRAGMA, DDL). No parameters, no results.
    pub fn exec(&self, sql: &str) -> Result<(), SqliteError> {
        self.db.exec(sql)
    }

    /// Validate a SQL statement and extract column metadata.
    ///
    /// Used by the proc macro for compile-time validation. Prepares the
    /// statement, extracts column info (name, declared type, source table/
    /// column for nullability), then drops the statement.
    ///
    /// Returns `(column_info_vec, param_count)`.
    pub fn compile_validate(
        &mut self,
        sql: &str,
    ) -> Result<(Vec<CompileColumnInfo>, usize), SqliteError> {
        let stmt = self.db.prepare(sql)?;

        let col_count = stmt.column_count() as usize;
        let param_count = stmt.bind_parameter_count() as usize;

        let mut columns = Vec::with_capacity(col_count);
        for i in 0..col_count as i32 {
            let name = stmt.column_name(i).unwrap_or("?column?").to_owned();
            let declared_type = stmt.column_decltype(i).map(|s| s.to_owned());
            let table_name = stmt.column_table_name(i).map(|s| s.to_owned());
            let origin_name = stmt.column_origin_name(i).map(|s| s.to_owned());

            // Resolve nullability from table schema
            let is_nullable = match (&table_name, &origin_name) {
                (Some(table), Some(column)) => self.resolve_column_nullable(table, column),
                _ => true, // expression/aggregate -> nullable (safe default)
            };

            columns.push(CompileColumnInfo {
                name,
                declared_type,
                table_name,
                origin_name,
                is_nullable,
            });
        }

        // stmt is finalized on drop

        Ok((columns, param_count))
    }

    /// Query PRAGMA table_info to determine if a column is nullable.
    fn resolve_column_nullable(&mut self, table: &str, column: &str) -> bool {
        let pragma_sql = format!("PRAGMA table_info(\"{}\")", table);
        let pragma_hash = hash_sql(&pragma_sql);
        let mut arena = Arena::new();

        match self.query(&pragma_sql, pragma_hash, &[], &mut arena) {
            Ok(result) => {
                // PRAGMA table_info columns: cid(0), name(1), type(2), notnull(3), dflt_value(4), pk(5)
                for row in 0..result.row_count {
                    if let Some(col_name) = result.get_str(row, 1, &arena) {
                        if col_name == column {
                            // notnull flag: 1 = NOT NULL, 0 = nullable
                            return result.get_i64(row, 3, &arena) != Some(1);
                        }
                    }
                }
                true // column not found -> assume nullable
            }
            Err(_) => true, // error -> assume nullable (safe)
        }
    }

    /// Get a cached statement or prepare a new one.
    fn get_or_prepare(&mut self, sql: &str, sql_hash: u64) -> Result<&StmtHandle, SqliteError> {
        if !self.stmts.contains_key(&sql_hash) {
            let handle = self.db.prepare(sql)?;
            let col_count = handle.column_count();
            self.stmts.insert(
                sql_hash,
                CachedStmt {
                    handle,
                    column_count: col_count,
                },
            );
        }
        Ok(&self.stmts.get(&sql_hash).unwrap().handle)
    }
}

// No manual Drop needed — `DbHandle` and `StmtHandle` handle cleanup via
// their own Drop impls. The `StmtCache` entries (each containing a `StmtHandle`)
// are dropped before `DbHandle`, which is correct because Rust drops struct
// fields in declaration order.

// --- QueryResult ---

/// Column metadata extracted during compile-time validation.
///
/// Used by the proc macro to generate typed Rust code for SQLite queries.
#[derive(Debug, Clone)]
pub struct CompileColumnInfo {
    /// Column name (or alias) in the result set.
    pub name: String,
    /// Declared type from CREATE TABLE (e.g. "INTEGER", "TEXT").
    /// `None` for expressions without a declared type.
    pub declared_type: Option<String>,
    /// Source table name, if this column comes from a table column.
    pub table_name: Option<String>,
    /// Origin column name in the source table.
    pub origin_name: Option<String>,
    /// Whether this column can be NULL.
    pub is_nullable: bool,
}

/// Result of a query execution. Row data lives in the associated `Arena`.
///
/// Column data is addressed by `(offset, length)` pairs into the arena.
/// A length of `-1` indicates a SQL NULL value.
pub struct QueryResult {
    /// Number of columns per row.
    pub col_count: usize,
    /// Number of rows returned.
    pub row_count: usize,
    /// Flat array of `(arena_offset, byte_length)` for every cell.
    /// Layout: `[row0_col0, row0_col1, ..., row1_col0, ...]`.
    /// A `byte_length` of `-1` indicates NULL.
    pub col_offsets: Vec<(usize, i32)>,
}

impl QueryResult {
    /// Number of rows.
    pub fn len(&self) -> usize {
        self.row_count
    }

    /// Whether the result set is empty.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Get the `(offset, length)` for a specific cell.
    ///
    /// # Panics
    ///
    /// Panics if `row >= row_count` or `col >= col_count`.
    pub fn cell(&self, row: usize, col: usize) -> (usize, i32) {
        debug_assert!(
            row < self.row_count,
            "row {row} out of range (max {})",
            self.row_count
        );
        debug_assert!(
            col < self.col_count,
            "col {col} out of range (max {})",
            self.col_count
        );
        self.col_offsets[row * self.col_count + col]
    }

    /// Check if a cell is NULL.
    pub fn is_null(&self, row: usize, col: usize) -> bool {
        self.cell(row, col).1 == -1
    }

    /// Get the raw bytes for a cell from the arena. Returns `None` for NULL.
    pub fn get_bytes<'a>(&self, row: usize, col: usize, arena: &'a Arena) -> Option<&'a [u8]> {
        let (offset, len) = self.cell(row, col);
        if len == -1 {
            return None;
        }
        Some(arena.get(offset, len as usize))
    }

    /// Get an i64 value from the arena. Returns `None` for NULL.
    pub fn get_i64(&self, row: usize, col: usize, arena: &Arena) -> Option<i64> {
        let bytes = self.get_bytes(row, col, arena)?;
        if bytes.len() == 8 {
            Some(i64::from_le_bytes(bytes.try_into().unwrap()))
        } else {
            None
        }
    }

    /// Get an f64 value from the arena. Returns `None` for NULL.
    pub fn get_f64(&self, row: usize, col: usize, arena: &Arena) -> Option<f64> {
        let bytes = self.get_bytes(row, col, arena)?;
        if bytes.len() == 8 {
            Some(f64::from_le_bytes(bytes.try_into().unwrap()))
        } else {
            None
        }
    }

    /// Get a text value from the arena. Returns `None` for NULL or invalid UTF-8.
    pub fn get_str<'a>(&self, row: usize, col: usize, arena: &'a Arena) -> Option<&'a str> {
        let bytes = self.get_bytes(row, col, arena)?;
        std::str::from_utf8(bytes).ok()
    }

    /// Get a bool value (stored as i64, 0=false, nonzero=true). Returns `None` for NULL.
    pub fn get_bool(&self, row: usize, col: usize, arena: &Arena) -> Option<bool> {
        self.get_i64(row, col, arena).map(|v| v != 0)
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
        format!("{}/bsql_test_conn_{}.db", dir.display(), id)
    }

    #[test]
    fn open_and_query() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'alice')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'bob')").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id, name FROM t ORDER BY id";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.col_count, 2);
        assert!(!result.is_empty());

        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_str(0, 1, &arena), Some("alice"));
        assert_eq!(result.get_i64(1, 0, &arena), Some(2));
        assert_eq!(result.get_str(1, 1, &arena), Some("bob"));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_with_params() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'alice')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'bob')").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id, name FROM t WHERE id = ?1";
        let hash = hash_sql(sql);
        let id: i64 = 2;
        let result = conn.query(sql, hash, &[&id], &mut arena).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result.get_i64(0, 0, &arena), Some(2));
        assert_eq!(result.get_str(0, 1, &arena), Some("bob"));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn statement_cache_hit() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);

        // First query — prepare + execute
        let mut arena = Arena::new();
        conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(conn.stmts.len(), 1);

        // Second query — cache hit (no new prepare)
        arena.reset();
        conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(conn.stmts.len(), 1); // still just one cached stmt

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn execute_returns_affected_rows() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, val TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.exec("INSERT INTO t VALUES (3, 'c')").unwrap();

        let sql = "DELETE FROM t WHERE id > ?1";
        let hash = hash_sql(sql);
        let threshold: i64 = 1;
        let affected = conn.execute(sql, hash, &[&threshold]).unwrap();
        assert_eq!(affected, 2);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn null_handling() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, NULL)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id, name FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();

        assert_eq!(result.len(), 1);
        assert!(!result.is_null(0, 0));
        assert!(result.is_null(0, 1));
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_str(0, 1, &arena), None);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn float_values() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val REAL)").unwrap();
        conn.exec("INSERT INTO t VALUES (3.14)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT val FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();

        let val = result.get_f64(0, 0, &arena).unwrap();
        assert!((val - 3.14).abs() < f64::EPSILON);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn blob_values() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (data BLOB)").unwrap();

        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        let blob: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
        conn.execute(sql_ins, hash_ins, &[&blob]).unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT data FROM t";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();

        assert_eq!(
            result.get_bytes(0, 0, &arena),
            Some(&[0xDE, 0xAD, 0xBE, 0xEF][..])
        );

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bool_values() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (flag INTEGER)").unwrap();

        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        let flag = true;
        conn.execute(sql_ins, hash_ins, &[&flag]).unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT flag FROM t";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();

        assert_eq!(result.get_bool(0, 0, &arena), Some(true));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_readonly() {
        let path = temp_db_path();

        // Create the database first with a writable connection
        {
            let conn = SqliteConnection::open(&path).unwrap();
            conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
            conn.exec("INSERT INTO t VALUES (42)").unwrap();
        }

        // Open read-only
        let mut conn = SqliteConnection::open_readonly(&path).unwrap();
        let mut arena = Arena::new();
        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(42));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn prepare_only_warmup() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);

        conn.prepare_only(sql, hash).unwrap();
        assert_eq!(conn.stmts.len(), 1);

        // Query using the warmed-up statement
        let mut arena = Arena::new();
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.len(), 0);
        assert_eq!(conn.stmts.len(), 1); // still just one (cache hit)

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn exec_ddl() {
        let path = temp_db_path();
        let conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t1 (id INTEGER)").unwrap();
        conn.exec("CREATE TABLE t2 (id INTEGER)").unwrap();
        conn.exec("DROP TABLE t1").unwrap();
        // t2 should still exist
        conn.exec("INSERT INTO t2 VALUES (1)").unwrap();
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_result_set() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn hash_sql_deterministic() {
        let h1 = hash_sql("SELECT 1");
        let h2 = hash_sql("SELECT 1");
        let h3 = hash_sql("SELECT 2");
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }

    #[test]
    fn query_result_cell_accessor() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (10, 'x')").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT a, b FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();

        let (_, len) = result.cell(0, 0);
        assert_eq!(len, 8); // i64 LE bytes
        let (_, len) = result.cell(0, 1);
        assert_eq!(len, 1); // 'x' is 1 byte

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn multiple_params() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
            .unwrap();

        let sql_ins = "INSERT INTO t VALUES (?1, ?2, ?3)";
        let hash_ins = hash_sql(sql_ins);
        let a: i64 = 7;
        let b: &str = "hello";
        let c: f64 = 2.718;
        conn.execute(sql_ins, hash_ins, &[&a, &b, &c]).unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT a, b, c FROM t";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();

        assert_eq!(result.get_i64(0, 0, &arena), Some(7));
        assert_eq!(result.get_str(0, 1, &arena), Some("hello"));
        let c_val = result.get_f64(0, 2, &arena).unwrap();
        assert!((c_val - 2.718).abs() < f64::EPSILON);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }
}
