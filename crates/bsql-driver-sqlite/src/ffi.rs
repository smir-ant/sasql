//! Safe wrappers over the `libsqlite3-sys` C API.
//!
//! This module is the **only** place in the crate that contains `unsafe` code.
//! It exposes two safe wrapper types — [`DbHandle`] and [`StmtHandle`] — that
//! encapsulate the raw `*mut sqlite3` and `*mut sqlite3_stmt` pointers. All
//! other modules in the crate use these safe types exclusively.

use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::ptr;

use libsqlite3_sys as raw;

use crate::SqliteError;

/// Return the SQLITE_TRANSIENT destructor sentinel.
///
/// In C this is `((sqlite3_destructor_type)-1)` — a sentinel function pointer
/// that tells SQLite to make its own copy of the data immediately. We cannot
/// define this as a `const` in Rust because the compiler rejects a non-null
/// function pointer with value -1 as UB in const context. Instead we produce
/// it at runtime via transmute, which is sound because SQLite never calls
/// this "pointer" as a function — it only checks if the value equals -1.
#[inline]
fn sqlite_transient() -> raw::sqlite3_destructor_type {
    // SAFETY: SQLITE_TRANSIENT is the well-known sentinel value -1 cast to a
    // function pointer. SQLite checks this value with pointer comparison and
    // never dereferences or calls it. This is the standard way to express it
    // from Rust when the crate does not re-export the macro.
    Some(unsafe { std::mem::transmute::<isize, unsafe extern "C" fn(*mut std::ffi::c_void)>(-1) })
}

/// Get the last error message from a database connection.
///
/// # Safety
///
/// `db` must be a valid (possibly errored) database handle, or null.
unsafe fn error_message(db: *mut raw::sqlite3) -> String {
    if db.is_null() {
        return "unknown error (null db handle)".into();
    }
    let ptr = unsafe { raw::sqlite3_errmsg(db) };
    if ptr.is_null() {
        return "unknown error".into();
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}

// ---------------------------------------------------------------------------
// StepResult
// ---------------------------------------------------------------------------

/// Result of stepping a prepared statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepResult {
    /// A new row of data is ready for reading.
    Row,
    /// The statement has finished executing.
    Done,
}

// ---------------------------------------------------------------------------
// DbHandle
// ---------------------------------------------------------------------------

/// Safe wrapper around a SQLite database handle.
///
/// Owns the handle and closes it on drop. NOT Send/Sync — SQLite connections
/// opened with `SQLITE_OPEN_NOMUTEX` must be used from a single thread.
pub struct DbHandle {
    ptr: *mut raw::sqlite3,
    /// Prevent Send+Sync auto-derivation.
    _marker: PhantomData<*mut ()>,
}

impl DbHandle {
    /// Open a database.
    pub fn open(path: &str, flags: i32) -> Result<Self, SqliteError> {
        let c_path =
            CString::new(path).map_err(|_| SqliteError::Internal("null byte in path".into()))?;
        let mut db: *mut raw::sqlite3 = ptr::null_mut();
        // SAFETY: We pass valid CString pointer, a mutable pointer to receive the
        // handle, valid flags, and null VFS name. If open fails we close the handle.
        let rc = unsafe { raw::sqlite3_open_v2(c_path.as_ptr(), &mut db, flags, ptr::null()) };
        if rc != raw::SQLITE_OK {
            let msg = unsafe { error_message(db) };
            if !db.is_null() {
                // SAFETY: db was allocated by sqlite3_open_v2, close releases it.
                unsafe {
                    raw::sqlite3_close(db);
                }
            }
            return Err(SqliteError::Sqlite {
                code: rc,
                message: msg,
            });
        }
        Ok(Self {
            ptr: db,
            _marker: PhantomData,
        })
    }

    /// Prepare a SQL statement.
    pub fn prepare(&self, sql: &str) -> Result<StmtHandle, SqliteError> {
        let mut stmt: *mut raw::sqlite3_stmt = ptr::null_mut();
        // SAFETY: self.ptr is a valid open database handle. We pass the SQL bytes
        // with their length (no NUL terminator required for _v2).
        let rc = unsafe {
            raw::sqlite3_prepare_v2(
                self.ptr,
                sql.as_ptr().cast::<i8>(),
                sql.len() as i32,
                &mut stmt,
                ptr::null_mut(),
            )
        };
        if rc != raw::SQLITE_OK {
            let msg = unsafe { error_message(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: msg,
            });
        }
        Ok(StmtHandle {
            ptr: stmt,
            _marker: PhantomData,
        })
    }

    /// Execute a simple SQL string (PRAGMA, DDL). No parameters, no results.
    pub fn exec(&self, sql: &str) -> Result<(), SqliteError> {
        let c_sql =
            CString::new(sql).map_err(|_| SqliteError::Internal("null byte in SQL".into()))?;
        // SAFETY: self.ptr is a valid open database handle. We pass a NUL-terminated
        // C string, no callback, no callback context, and no error string output.
        let rc = unsafe {
            raw::sqlite3_exec(
                self.ptr,
                c_sql.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if rc != raw::SQLITE_OK {
            let msg = unsafe { error_message(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: msg,
            });
        }
        Ok(())
    }

    /// Get number of changes from last INSERT/UPDATE/DELETE.
    pub fn changes(&self) -> u64 {
        // SAFETY: self.ptr is a valid open database handle.
        (unsafe { raw::sqlite3_changes(self.ptr) }) as u64
    }

    /// Get last error message.
    pub fn error_message(&self) -> String {
        // SAFETY: self.ptr is a valid open database handle.
        unsafe { error_message(self.ptr) }
    }
}

impl Drop for DbHandle {
    fn drop(&mut self) {
        // SAFETY: self.ptr is a valid database handle. All statements must have
        // been finalized before this point (ensured by the caller / StmtHandle Drop).
        unsafe {
            raw::sqlite3_close(self.ptr);
        }
    }
}

// ---------------------------------------------------------------------------
// StmtHandle
// ---------------------------------------------------------------------------

/// Safe wrapper around a SQLite prepared statement.
///
/// The statement is finalized on drop. NOT Send/Sync.
pub struct StmtHandle {
    ptr: *mut raw::sqlite3_stmt,
    /// Prevent Send+Sync auto-derivation.
    _marker: PhantomData<*mut ()>,
}

impl StmtHandle {
    // --- Binding (safe methods) ---

    /// Bind an i64 parameter at 1-based index.
    pub fn bind_int64(&self, idx: i32, val: i64) -> Result<(), SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let rc = unsafe { raw::sqlite3_bind_int64(self.ptr, idx, val) };
        if rc != raw::SQLITE_OK {
            let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: unsafe { error_message(db) },
            });
        }
        Ok(())
    }

    /// Bind a f64 parameter at 1-based index.
    pub fn bind_double(&self, idx: i32, val: f64) -> Result<(), SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let rc = unsafe { raw::sqlite3_bind_double(self.ptr, idx, val) };
        if rc != raw::SQLITE_OK {
            let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: unsafe { error_message(db) },
            });
        }
        Ok(())
    }

    /// Bind a text parameter at 1-based index.
    ///
    /// Uses `SQLITE_TRANSIENT` — SQLite copies the data immediately.
    pub fn bind_text(&self, idx: i32, val: &str) -> Result<(), SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle. SQLITE_TRANSIENT
        // tells SQLite to copy the data, so the Rust reference need not outlive this call.
        let rc = unsafe {
            raw::sqlite3_bind_text(
                self.ptr,
                idx,
                val.as_ptr().cast::<i8>(),
                val.len() as i32,
                sqlite_transient(),
            )
        };
        if rc != raw::SQLITE_OK {
            let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: unsafe { error_message(db) },
            });
        }
        Ok(())
    }

    /// Bind a blob parameter at 1-based index.
    ///
    /// Uses `SQLITE_TRANSIENT` — SQLite copies the data immediately.
    pub fn bind_blob(&self, idx: i32, val: &[u8]) -> Result<(), SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle. SQLITE_TRANSIENT
        // tells SQLite to copy the data.
        let rc = unsafe {
            raw::sqlite3_bind_blob(
                self.ptr,
                idx,
                val.as_ptr().cast::<std::ffi::c_void>(),
                val.len() as i32,
                sqlite_transient(),
            )
        };
        if rc != raw::SQLITE_OK {
            let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: unsafe { error_message(db) },
            });
        }
        Ok(())
    }

    /// Bind NULL at 1-based index.
    pub fn bind_null(&self, idx: i32) -> Result<(), SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let rc = unsafe { raw::sqlite3_bind_null(self.ptr, idx) };
        if rc != raw::SQLITE_OK {
            let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: unsafe { error_message(db) },
            });
        }
        Ok(())
    }

    /// Clear all bindings on this statement.
    pub fn clear_bindings(&self) {
        // SAFETY: self.ptr is a valid prepared statement handle.
        // sqlite3_clear_bindings always returns SQLITE_OK.
        unsafe {
            raw::sqlite3_clear_bindings(self.ptr);
        }
    }

    // --- Stepping ---

    /// Step the statement. Returns `StepResult::Row` or `StepResult::Done`.
    pub fn step(&self) -> Result<StepResult, SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let rc = unsafe { raw::sqlite3_step(self.ptr) };
        match rc {
            raw::SQLITE_ROW => Ok(StepResult::Row),
            raw::SQLITE_DONE => Ok(StepResult::Done),
            _ => {
                let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
                let msg = unsafe { error_message(db) };
                Err(SqliteError::Sqlite {
                    code: rc,
                    message: msg,
                })
            }
        }
    }

    /// Reset the statement for reuse (clears the step state, keeps bindings).
    pub fn reset(&self) -> Result<(), SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let rc = unsafe { raw::sqlite3_reset(self.ptr) };
        if rc != raw::SQLITE_OK {
            let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: unsafe { error_message(db) },
            });
        }
        Ok(())
    }

    // --- Column reading ---

    /// Get the number of columns in the result set.
    pub fn column_count(&self) -> i32 {
        // SAFETY: self.ptr is a valid prepared statement handle.
        unsafe { raw::sqlite3_column_count(self.ptr) }
    }

    /// Get the storage type of a column (SQLITE_INTEGER, SQLITE_FLOAT,
    /// SQLITE_TEXT, SQLITE_BLOB, or SQLITE_NULL).
    pub fn column_type(&self, idx: i32) -> i32 {
        // SAFETY: self.ptr is a valid statement that has been stepped to SQLITE_ROW.
        unsafe { raw::sqlite3_column_type(self.ptr, idx) }
    }

    /// Get a column value as i64.
    pub fn column_int64(&self, idx: i32) -> i64 {
        // SAFETY: self.ptr is a valid statement that has been stepped to SQLITE_ROW.
        unsafe { raw::sqlite3_column_int64(self.ptr, idx) }
    }

    /// Get a column value as f64.
    pub fn column_double(&self, idx: i32) -> f64 {
        // SAFETY: self.ptr is a valid statement that has been stepped to SQLITE_ROW.
        unsafe { raw::sqlite3_column_double(self.ptr, idx) }
    }

    /// Get a column value as text (UTF-8 bytes).
    ///
    /// Returns `None` if the column is NULL. The returned slice is valid until
    /// the next `step`, `reset`, or the statement is dropped.
    pub fn column_text(&self, idx: i32) -> Option<&[u8]> {
        // SAFETY: self.ptr is a valid statement that has been stepped to SQLITE_ROW.
        // The returned pointer is valid until the next step/reset/finalize.
        let ptr = unsafe { raw::sqlite3_column_text(self.ptr, idx) };
        if ptr.is_null() {
            return None;
        }
        let len = unsafe { raw::sqlite3_column_bytes(self.ptr, idx) } as usize;
        Some(unsafe { std::slice::from_raw_parts(ptr, len) })
    }

    /// Get a column value as blob (raw bytes).
    ///
    /// Returns an empty slice if the column is NULL or has zero length.
    /// The returned slice is valid until the next `step`, `reset`, or the
    /// statement is dropped.
    pub fn column_blob(&self, idx: i32) -> &[u8] {
        // SAFETY: self.ptr is a valid statement that has been stepped to SQLITE_ROW.
        let len = unsafe { raw::sqlite3_column_bytes(self.ptr, idx) } as usize;
        if len == 0 {
            return &[];
        }
        let ptr = unsafe { raw::sqlite3_column_blob(self.ptr, idx) };
        if ptr.is_null() {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) }
    }

    /// Get the byte length of a column value.
    pub fn column_bytes(&self, idx: i32) -> i32 {
        // SAFETY: self.ptr is a valid statement that has been stepped to SQLITE_ROW.
        unsafe { raw::sqlite3_column_bytes(self.ptr, idx) }
    }

    /// Get the column name as a UTF-8 string.
    pub fn column_name(&self, idx: i32) -> Option<&str> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let ptr = unsafe { raw::sqlite3_column_name(self.ptr, idx) };
        if ptr.is_null() {
            return None;
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().ok()
    }

    /// Get the column declared type (from CREATE TABLE schema).
    pub fn column_decltype(&self, idx: i32) -> Option<&str> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let ptr = unsafe { raw::sqlite3_column_decltype(self.ptr, idx) };
        if ptr.is_null() {
            return None;
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().ok()
    }

    /// Get the table name for a column in the result set.
    ///
    /// Returns `None` if the column is an expression (not backed by a table)
    /// or if the index is out of range.
    pub fn column_table_name(&self, idx: i32) -> Option<&str> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let ptr = unsafe { raw::sqlite3_column_table_name(self.ptr, idx) };
        if ptr.is_null() {
            return None;
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().ok()
    }

    /// Get the origin column name (the actual column name in CREATE TABLE) for
    /// a column in the result set.
    ///
    /// Returns `None` if the column is an expression (not backed by a table column)
    /// or if the index is out of range.
    pub fn column_origin_name(&self, idx: i32) -> Option<&str> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let ptr = unsafe { raw::sqlite3_column_origin_name(self.ptr, idx) };
        if ptr.is_null() {
            return None;
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().ok()
    }

    /// Get the number of parameters in this prepared statement.
    pub fn bind_parameter_count(&self) -> i32 {
        // SAFETY: self.ptr is a valid prepared statement handle.
        unsafe { raw::sqlite3_bind_parameter_count(self.ptr) }
    }
}

impl Drop for StmtHandle {
    fn drop(&mut self) {
        // SAFETY: self.ptr is a valid statement handle. After finalize, the
        // pointer is invalid — but we never use it again (we are in drop).
        // We intentionally ignore the return code; sqlite3_finalize returns the
        // error from the most recent step, which we already handled.
        unsafe {
            raw::sqlite3_finalize(self.ptr);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db_path() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir();
        format!("{}/bsql_test_ffi_{}.db", dir.display(), id)
    }

    #[test]
    fn open_and_close() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).expect("open failed");
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_invalid_path_with_null() {
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE;
        let result = DbHandle::open("path\0with_null", flags);
        assert!(result.is_err());
        match result {
            Err(SqliteError::Internal(msg)) => assert!(msg.contains("null")),
            Err(e) => panic!("expected Internal error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn prepare_and_finalize() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).unwrap();
        db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        let _stmt = db
            .prepare("INSERT INTO t (id, name) VALUES (?1, ?2)")
            .unwrap();
        // stmt finalized on drop
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn prepare_invalid_sql() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).unwrap();
        let result = db.prepare("NOT VALID SQL STATEMENT");
        assert!(result.is_err());
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_step_and_read_columns() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).unwrap();
        db.exec("CREATE TABLE t (id INTEGER, name TEXT, score REAL, data BLOB)")
            .unwrap();

        // Insert
        let ins = db.prepare("INSERT INTO t VALUES (?1, ?2, ?3, ?4)").unwrap();
        ins.bind_int64(1, 42).unwrap();
        ins.bind_text(2, "hello").unwrap();
        ins.bind_double(3, 3.14).unwrap();
        ins.bind_blob(4, &[0xDE, 0xAD]).unwrap();
        let rc = ins.step().unwrap();
        assert_eq!(rc, StepResult::Done);
        drop(ins);

        // Query
        let sel = db.prepare("SELECT id, name, score, data FROM t").unwrap();
        assert_eq!(sel.column_count(), 4);

        let rc = sel.step().unwrap();
        assert_eq!(rc, StepResult::Row);

        // Column types
        assert_eq!(sel.column_type(0), raw::SQLITE_INTEGER);
        assert_eq!(sel.column_type(1), raw::SQLITE_TEXT);
        assert_eq!(sel.column_type(2), raw::SQLITE_FLOAT);
        assert_eq!(sel.column_type(3), raw::SQLITE_BLOB);

        // Values
        assert_eq!(sel.column_int64(0), 42);
        let text = sel.column_text(1).unwrap();
        assert_eq!(text, b"hello");
        assert!((sel.column_double(2) - 3.14).abs() < f64::EPSILON);
        let blob = sel.column_blob(3);
        assert_eq!(blob, &[0xDE, 0xAD]);

        // Column names
        assert_eq!(sel.column_name(0), Some("id"));
        assert_eq!(sel.column_name(1), Some("name"));

        // No more rows
        let rc = sel.step().unwrap();
        assert_eq!(rc, StepResult::Done);

        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_null_and_read_null() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).unwrap();
        db.exec("CREATE TABLE t (val TEXT)").unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_null(1).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_type(0), raw::SQLITE_NULL);
        assert!(sel.column_text(0).is_none());
        drop(sel);

        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reset_and_reuse_statement() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();

        // First use
        ins.bind_int64(1, 1).unwrap();
        ins.step().unwrap();
        ins.reset().unwrap();
        ins.clear_bindings();

        // Second use
        ins.bind_int64(1, 2).unwrap();
        ins.step().unwrap();
        ins.reset().unwrap();

        drop(ins);

        // Verify both rows
        let sel = db.prepare("SELECT COUNT(*) FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 2);
        drop(sel);

        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn exec_pragma() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).unwrap();
        db.exec("PRAGMA journal_mode = WAL").unwrap();
        db.exec("PRAGMA synchronous = NORMAL").unwrap();
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn exec_invalid_sql() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).unwrap();
        let result = db.exec("NOT VALID SQL");
        assert!(result.is_err());
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn exec_null_in_sql() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).unwrap();
        let result = db.exec("SELECT\01");
        assert!(result.is_err());
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn changes_count() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();
        db.exec("INSERT INTO t VALUES (1)").unwrap();
        db.exec("INSERT INTO t VALUES (2)").unwrap();
        db.exec("INSERT INTO t VALUES (3)").unwrap();
        db.exec("DELETE FROM t WHERE id > 1").unwrap();
        assert_eq!(db.changes(), 2);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn column_decltype_returns_type() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).unwrap();
        db.exec("CREATE TABLE t (id INTEGER, name TEXT, score REAL)")
            .unwrap();
        let stmt = db.prepare("SELECT id, name, score FROM t").unwrap();
        assert_eq!(stmt.column_decltype(0), Some("INTEGER"));
        assert_eq!(stmt.column_decltype(1), Some("TEXT"));
        assert_eq!(stmt.column_decltype(2), Some("REAL"));
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn column_blob_null_returns_empty() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).unwrap();
        db.exec("CREATE TABLE t (data BLOB)").unwrap();
        db.exec("INSERT INTO t VALUES (NULL)").unwrap();
        let sel = db.prepare("SELECT data FROM t").unwrap();
        sel.step().unwrap();
        let blob = sel.column_blob(0);
        assert!(blob.is_empty());
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn column_bytes_len() {
        let path = temp_db_path();
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(&path, flags).unwrap();
        db.exec("CREATE TABLE t (name TEXT)").unwrap();
        db.exec("INSERT INTO t VALUES ('abcdef')").unwrap();
        let sel = db.prepare("SELECT name FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_bytes(0), 6);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }
}
