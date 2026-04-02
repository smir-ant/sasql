//! SQLite encode/decode — binding Rust values to prepared statements and
//! decoding arena bytes back to Rust types.
//!
//! # Encoding
//!
//! The `SqliteEncode` trait binds a Rust value to a SQLite prepared statement
//! parameter at a 1-based index. Implementations call safe methods on
//! [`StmtHandle`](crate::ffi::StmtHandle).
//!
//! # Decoding
//!
//! Decode helpers operate on raw byte slices from the arena. The step loop
//! in `conn.rs` stores values in a canonical format:
//! - `INTEGER`: 8 bytes, little-endian i64
//! - `REAL`: 8 bytes, little-endian f64
//! - `TEXT`: raw UTF-8 bytes
//! - `BLOB`: raw bytes
//! - `NULL`: nothing (indicated by length == -1 in the offset array)

use crate::SqliteError;
use crate::ffi::StmtHandle;

// --- SqliteEncode trait ---

/// Bind a Rust value to a SQLite prepared statement parameter.
///
/// Implementations call safe methods on [`StmtHandle`].
///
/// # Example
///
/// ```
/// use bsql_driver_sqlite::codec::SqliteEncode;
///
/// fn bind_example(val: &dyn SqliteEncode) {
///     // In practice, called by conn.rs with a real stmt handle.
///     let _ = val;
/// }
/// ```
pub trait SqliteEncode {
    /// Bind this value to the prepared statement at 1-based parameter index `idx`.
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError>;
}

// --- Encode implementations ---

impl SqliteEncode for i64 {
    #[inline]
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        stmt.bind_int64(idx, *self)
    }
}

impl SqliteEncode for i32 {
    #[inline]
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        stmt.bind_int64(idx, i64::from(*self))
    }
}

impl SqliteEncode for i16 {
    #[inline]
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        stmt.bind_int64(idx, i64::from(*self))
    }
}

impl SqliteEncode for i8 {
    #[inline]
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        stmt.bind_int64(idx, i64::from(*self))
    }
}

impl SqliteEncode for f64 {
    #[inline]
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        stmt.bind_double(idx, *self)
    }
}

impl SqliteEncode for f32 {
    #[inline]
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        stmt.bind_double(idx, f64::from(*self))
    }
}

impl SqliteEncode for bool {
    #[inline]
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        stmt.bind_int64(idx, if *self { 1 } else { 0 })
    }
}

impl SqliteEncode for &str {
    #[inline]
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        stmt.bind_text(idx, self)
    }
}

impl SqliteEncode for String {
    #[inline]
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        stmt.bind_text(idx, self.as_str())
    }
}

impl SqliteEncode for &[u8] {
    #[inline]
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        stmt.bind_blob(idx, self)
    }
}

impl SqliteEncode for Vec<u8> {
    #[inline]
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        stmt.bind_blob(idx, self.as_slice())
    }
}

impl<T: SqliteEncode> SqliteEncode for Option<T> {
    #[inline]
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        match self {
            Some(val) => val.bind(stmt, idx),
            None => stmt.bind_null(idx),
        }
    }
}

// --- Decode helpers ---
//
// Used by generated code and QueryResult accessors to decode arena bytes
// back to Rust types. All functions return None if the data is malformed.

/// Decode a little-endian i64 from 8 arena bytes.
#[inline]
pub fn decode_i64(data: &[u8]) -> Option<i64> {
    data.try_into().ok().map(i64::from_le_bytes)
}

/// Decode a little-endian f64 from 8 arena bytes.
#[inline]
pub fn decode_f64(data: &[u8]) -> Option<f64> {
    data.try_into().ok().map(f64::from_le_bytes)
}

/// Decode a boolean from arena bytes (stored as i64: 0 = false, nonzero = true).
#[inline]
pub fn decode_bool(data: &[u8]) -> Option<bool> {
    decode_i64(data).map(|v| v != 0)
}

/// Decode a UTF-8 string from arena bytes.
#[inline]
pub fn decode_str(data: &[u8]) -> Option<&str> {
    std::str::from_utf8(data).ok()
}

/// Decode an i32 from arena bytes (stored as i64, truncated).
#[inline]
pub fn decode_i32(data: &[u8]) -> Option<i32> {
    decode_i64(data).map(|v| v as i32)
}

/// Decode an i16 from arena bytes (stored as i64, truncated).
#[inline]
pub fn decode_i16(data: &[u8]) -> Option<i16> {
    decode_i64(data).map(|v| v as i16)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Decode tests ---

    #[test]
    fn decode_i64_valid() {
        let bytes = 42i64.to_le_bytes();
        assert_eq!(decode_i64(&bytes), Some(42));
    }

    #[test]
    fn decode_i64_negative() {
        let bytes = (-1i64).to_le_bytes();
        assert_eq!(decode_i64(&bytes), Some(-1));
    }

    #[test]
    fn decode_i64_wrong_length() {
        assert_eq!(decode_i64(&[1, 2, 3]), None);
        assert_eq!(decode_i64(&[]), None);
    }

    #[test]
    fn decode_f64_valid() {
        let bytes = 3.14f64.to_le_bytes();
        let val = decode_f64(&bytes).unwrap();
        assert!((val - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn decode_f64_wrong_length() {
        assert_eq!(decode_f64(&[1, 2, 3]), None);
    }

    #[test]
    fn decode_bool_true() {
        let bytes = 1i64.to_le_bytes();
        assert_eq!(decode_bool(&bytes), Some(true));
    }

    #[test]
    fn decode_bool_false() {
        let bytes = 0i64.to_le_bytes();
        assert_eq!(decode_bool(&bytes), Some(false));
    }

    #[test]
    fn decode_bool_nonzero_is_true() {
        let bytes = 99i64.to_le_bytes();
        assert_eq!(decode_bool(&bytes), Some(true));
    }

    #[test]
    fn decode_bool_wrong_length() {
        assert_eq!(decode_bool(&[1]), None);
    }

    #[test]
    fn decode_str_valid() {
        assert_eq!(decode_str(b"hello"), Some("hello"));
    }

    #[test]
    fn decode_str_empty() {
        assert_eq!(decode_str(b""), Some(""));
    }

    #[test]
    fn decode_str_invalid_utf8() {
        assert_eq!(decode_str(&[0xFF, 0xFE]), None);
    }

    #[test]
    fn decode_i32_valid() {
        let bytes = 42i64.to_le_bytes();
        assert_eq!(decode_i32(&bytes), Some(42));
    }

    #[test]
    fn decode_i16_valid() {
        let bytes = 123i64.to_le_bytes();
        assert_eq!(decode_i16(&bytes), Some(123));
    }

    // --- Encode integration tests (require a real database) ---

    fn temp_db_path() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir();
        format!("{}/bsql_test_codec_{}.db", dir.display(), id)
    }

    #[test]
    fn encode_i64_roundtrip() {
        let path = temp_db_path();
        let mut conn = crate::conn::SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::conn::hash_sql(sql);
        let val: i64 = i64::MAX;
        conn.execute(sql, hash, &[&val]).unwrap();

        let mut arena = bsql_arena::Arena::new();
        let sel = "SELECT val FROM t";
        let sel_hash = crate::conn::hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(i64::MAX));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encode_i32_roundtrip() {
        let path = temp_db_path();
        let mut conn = crate::conn::SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::conn::hash_sql(sql);
        let val: i32 = 42;
        conn.execute(sql, hash, &[&val]).unwrap();

        let mut arena = bsql_arena::Arena::new();
        let sel = "SELECT val FROM t";
        let sel_hash = crate::conn::hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(42));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encode_i16_roundtrip() {
        let path = temp_db_path();
        let mut conn = crate::conn::SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::conn::hash_sql(sql);
        let val: i16 = -100;
        conn.execute(sql, hash, &[&val]).unwrap();

        let mut arena = bsql_arena::Arena::new();
        let sel = "SELECT val FROM t";
        let sel_hash = crate::conn::hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(-100));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encode_i8_roundtrip() {
        let path = temp_db_path();
        let mut conn = crate::conn::SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::conn::hash_sql(sql);
        let val: i8 = -5;
        conn.execute(sql, hash, &[&val]).unwrap();

        let mut arena = bsql_arena::Arena::new();
        let sel = "SELECT val FROM t";
        let sel_hash = crate::conn::hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(-5));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encode_f64_roundtrip() {
        let path = temp_db_path();
        let mut conn = crate::conn::SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val REAL)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::conn::hash_sql(sql);
        let val: f64 = std::f64::consts::PI;
        conn.execute(sql, hash, &[&val]).unwrap();

        let mut arena = bsql_arena::Arena::new();
        let sel = "SELECT val FROM t";
        let sel_hash = crate::conn::hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        let decoded = result.get_f64(0, 0, &arena).unwrap();
        assert!((decoded - std::f64::consts::PI).abs() < f64::EPSILON);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encode_f32_roundtrip() {
        let path = temp_db_path();
        let mut conn = crate::conn::SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val REAL)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::conn::hash_sql(sql);
        let val: f32 = 1.5;
        conn.execute(sql, hash, &[&val]).unwrap();

        let mut arena = bsql_arena::Arena::new();
        let sel = "SELECT val FROM t";
        let sel_hash = crate::conn::hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        let decoded = result.get_f64(0, 0, &arena).unwrap();
        assert!((decoded - 1.5).abs() < f64::EPSILON);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encode_bool_roundtrip() {
        let path = temp_db_path();
        let mut conn = crate::conn::SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (flag INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::conn::hash_sql(sql);

        let t = true;
        conn.execute(sql, hash, &[&t]).unwrap();
        let f = false;
        conn.execute(sql, hash, &[&f]).unwrap();

        let mut arena = bsql_arena::Arena::new();
        let sel = "SELECT flag FROM t ORDER BY flag";
        let sel_hash = crate::conn::hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_bool(0, 0, &arena), Some(false));
        assert_eq!(result.get_bool(1, 0, &arena), Some(true));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encode_string_roundtrip() {
        let path = temp_db_path();
        let mut conn = crate::conn::SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (name TEXT)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::conn::hash_sql(sql);
        let val = String::from("hello world");
        conn.execute(sql, hash, &[&val]).unwrap();

        let mut arena = bsql_arena::Arena::new();
        let sel = "SELECT name FROM t";
        let sel_hash = crate::conn::hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_str(0, 0, &arena), Some("hello world"));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encode_vec_u8_roundtrip() {
        let path = temp_db_path();
        let mut conn = crate::conn::SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (data BLOB)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::conn::hash_sql(sql);
        let val: Vec<u8> = vec![0xCA, 0xFE, 0xBA, 0xBE];
        conn.execute(sql, hash, &[&val]).unwrap();

        let mut arena = bsql_arena::Arena::new();
        let sel = "SELECT data FROM t";
        let sel_hash = crate::conn::hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert_eq!(
            result.get_bytes(0, 0, &arena),
            Some(&[0xCA, 0xFE, 0xBA, 0xBE][..])
        );

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encode_option_some() {
        let path = temp_db_path();
        let mut conn = crate::conn::SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::conn::hash_sql(sql);
        let val: Option<i64> = Some(99);
        conn.execute(sql, hash, &[&val]).unwrap();

        let mut arena = bsql_arena::Arena::new();
        let sel = "SELECT val FROM t";
        let sel_hash = crate::conn::hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(99));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encode_option_none() {
        let path = temp_db_path();
        let mut conn = crate::conn::SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::conn::hash_sql(sql);
        let val: Option<i64> = None;
        conn.execute(sql, hash, &[&val]).unwrap();

        let mut arena = bsql_arena::Arena::new();
        let sel = "SELECT val FROM t";
        let sel_hash = crate::conn::hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert!(result.is_null(0, 0));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }
}
