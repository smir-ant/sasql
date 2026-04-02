//! Error types for bsql.
//!
//! [`BsqlError`] is the single error type returned by all bsql operations.
//! It has four variants matching the four failure modes of a database operation:
//! pool, query execution, data decoding, and initial connection.

use std::borrow::Cow;
use std::fmt;

/// The error type for all bsql operations.
///
/// # Variants
///
/// - [`Pool`](BsqlError::Pool) — connection pool exhausted or misconfigured.
/// - [`Query`](BsqlError::Query) — PostgreSQL rejected the query at runtime
///   (triggers, RLS policies, constraint violations).
/// - [`Decode`](BsqlError::Decode) — a column value could not be converted to
///   the expected Rust type.
/// - [`Connect`](BsqlError::Connect) — initial connection to PostgreSQL failed.
#[derive(Debug)]
pub enum BsqlError {
    Pool(PoolError),
    Query(QueryError),
    Decode(DecodeError),
    Connect(ConnectError),
}

/// Connection pool failure.
#[derive(Debug)]
pub struct PoolError {
    pub message: Cow<'static, str>,
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Query execution failure. Contains the PostgreSQL error code when available.
#[derive(Debug)]
pub struct QueryError {
    pub message: Cow<'static, str>,
    /// The five-character SQLSTATE code (e.g. `"23505"` for unique violation).
    pub pg_code: Option<Box<str>>,
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Row/column decoding failure.
#[derive(Debug)]
pub struct DecodeError {
    pub column: Cow<'static, str>,
    pub expected: &'static str,
    pub actual: Cow<'static, str>,
    /// Optional underlying error that caused the decode failure.
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Initial connection failure.
#[derive(Debug)]
pub struct ConnectError {
    pub message: Cow<'static, str>,
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Convenience alias used throughout bsql.
pub type BsqlResult<T> = Result<T, BsqlError>;

// --- Display ---

impl fmt::Display for BsqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pool(e) => write!(f, "pool error: {e}"),
            Self::Query(e) => write!(f, "query error: {e}"),
            Self::Decode(e) => write!(f, "decode error: {e}"),
            Self::Connect(e) => write!(f, "connect error: {e}"),
        }
    }
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.pg_code {
            Some(code) => write!(f, "[{}] {}", &**code, self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "column \"{}\": expected {}, got {}",
            self.column, self.expected, self.actual
        )
    }
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for BsqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Pool(e) => e.source(),
            Self::Query(e) => e.source(),
            Self::Decode(e) => e.source(),
            Self::Connect(e) => e.source(),
        }
    }
}

impl std::error::Error for PoolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        boxed_source(&self.source)
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        boxed_source(&self.source)
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        boxed_source(&self.source)
    }
}

impl std::error::Error for ConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        boxed_source(&self.source)
    }
}

fn boxed_source(
    src: &Option<Box<dyn std::error::Error + Send + Sync>>,
) -> Option<&(dyn std::error::Error + 'static)> {
    src.as_ref()
        .map(|e| &**e as &(dyn std::error::Error + 'static))
}

// --- Query helpers ---

impl BsqlError {
    /// Whether this error is a PostgreSQL query cancellation / statement timeout
    /// (SQLSTATE 57014).
    pub fn is_timeout(&self) -> bool {
        matches!(self, BsqlError::Query(q) if q.pg_code.as_deref() == Some("57014"))
    }

    /// Whether this error is a serialization failure (SQLSTATE 40001).
    ///
    /// When using `SERIALIZABLE` isolation, PostgreSQL may abort a transaction
    /// with this code. The correct response is to retry the entire transaction.
    pub fn is_serialization_failure(&self) -> bool {
        matches!(self, BsqlError::Query(q) if q.pg_code.as_deref() == Some("40001"))
    }

    /// Convert a `DriverError` that occurred during query execution.
    ///
    /// Unlike the blanket `From<DriverError>` impl (which maps `Io` to `Connect`),
    /// this maps `Io` errors to `Query` — because a network failure mid-query is
    /// a query error, not a connection error.
    pub fn from_driver_query(e: bsql_driver::DriverError) -> Self {
        match e {
            bsql_driver::DriverError::Io(io_err) => BsqlError::Query(QueryError {
                message: Cow::Owned(format!("I/O error during query: {io_err}")),
                pg_code: None,
                source: Some(Box::new(io_err)),
            }),
            other => BsqlError::from(other),
        }
    }
}

// --- From conversions ---

impl From<bsql_driver::DriverError> for BsqlError {
    fn from(e: bsql_driver::DriverError) -> Self {
        match e {
            bsql_driver::DriverError::Io(io_err) => BsqlError::Connect(ConnectError {
                message: Cow::Owned(io_err.to_string()),
                source: Some(Box::new(io_err)),
            }),
            bsql_driver::DriverError::Auth(msg) => BsqlError::Connect(ConnectError {
                message: Cow::Owned(msg),
                source: None,
            }),
            bsql_driver::DriverError::Protocol(msg) => BsqlError::Query(QueryError {
                message: Cow::Owned(msg),
                pg_code: None,
                source: None,
            }),
            bsql_driver::DriverError::Server {
                code,
                message,
                detail,
                hint,
            } => {
                let msg = if detail.is_some() || hint.is_some() {
                    let mut s = String::from(&*message);
                    if let Some(d) = &detail {
                        s.push_str("\n  detail: ");
                        s.push_str(d);
                    }
                    if let Some(h) = &hint {
                        s.push_str("\n  hint: ");
                        s.push_str(h);
                    }
                    Cow::Owned(s)
                } else {
                    Cow::Owned(String::from(message))
                };
                BsqlError::Query(QueryError {
                    message: msg,
                    pg_code: Some(code),
                    source: None,
                })
            }
            bsql_driver::DriverError::Pool(msg) => BsqlError::Pool(PoolError {
                message: Cow::Owned(msg),
                source: None,
            }),
        }
    }
}

// --- Constructor helpers ---

impl PoolError {
    pub fn exhausted() -> BsqlError {
        BsqlError::Pool(PoolError {
            message: Cow::Borrowed("pool exhausted: all connections in use"),
            source: None,
        })
    }
}

impl ConnectError {
    pub fn create(msg: impl Into<String>) -> BsqlError {
        BsqlError::Connect(ConnectError {
            message: Cow::Owned(msg.into()),
            source: None,
        })
    }

    pub fn with_source(
        msg: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> BsqlError {
        BsqlError::Connect(ConnectError {
            message: Cow::Owned(msg.into()),
            source: Some(Box::new(source)),
        })
    }
}

impl QueryError {
    pub fn row_count(expected: &str, actual: u64) -> BsqlError {
        BsqlError::Query(QueryError {
            message: Cow::Owned(format!("expected {expected}, got {actual} rows")),
            pg_code: None,
            source: None,
        })
    }
}

impl DecodeError {
    /// Create a decode error with an underlying cause.
    pub fn with_source(
        column: impl Into<Cow<'static, str>>,
        expected: &'static str,
        actual: impl Into<Cow<'static, str>>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> BsqlError {
        BsqlError::Decode(DecodeError {
            column: column.into(),
            expected,
            actual: actual.into(),
            source: Some(Box::new(source)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn pool_error_display() {
        let e = PoolError::exhausted();
        assert_eq!(
            e.to_string(),
            "pool error: pool exhausted: all connections in use"
        );
    }

    #[test]
    fn query_error_with_code_display() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("duplicate key"),
            pg_code: Some(Box::from("23505")),
            source: None,
        });
        assert_eq!(e.to_string(), "query error: [23505] duplicate key");
    }

    #[test]
    fn query_error_without_code_display() {
        let e = QueryError::row_count("exactly 1 row", 0);
        assert_eq!(
            e.to_string(),
            "query error: expected exactly 1 row, got 0 rows"
        );
    }

    #[test]
    fn decode_error_display() {
        let e = BsqlError::Decode(DecodeError {
            column: Cow::Borrowed("age"),
            expected: "i32",
            actual: Cow::Borrowed("text"),
            source: None,
        });
        assert_eq!(
            e.to_string(),
            "decode error: column \"age\": expected i32, got text"
        );
    }

    #[test]
    fn connect_error_display() {
        let e = ConnectError::create("connection refused");
        assert_eq!(e.to_string(), "connect error: connection refused");
    }

    #[test]
    fn pool_exhausted_uses_borrowed_cow() {
        let e = PoolError::exhausted();
        match e {
            BsqlError::Pool(ref pe) => {
                assert!(
                    matches!(pe.message, Cow::Borrowed(_)),
                    "exhausted() should use Cow::Borrowed for zero-alloc"
                );
            }
            _ => panic!("expected Pool variant"),
        }
    }

    #[test]
    fn connect_error_uses_owned_cow() {
        let e = ConnectError::create("dynamic message");
        match e {
            BsqlError::Connect(ref ce) => {
                assert!(
                    matches!(ce.message, Cow::Owned(_)),
                    "create() with dynamic msg should use Cow::Owned"
                );
            }
            _ => panic!("expected Connect variant"),
        }
    }

    #[test]
    fn query_row_count_uses_owned_cow() {
        let e = QueryError::row_count("exactly 1 row", 5);
        match e {
            BsqlError::Query(ref qe) => {
                assert!(
                    matches!(qe.message, Cow::Owned(_)),
                    "row_count() with formatted msg should use Cow::Owned"
                );
            }
            _ => panic!("expected Query variant"),
        }
    }

    #[test]
    fn pool_error_source_chain() {
        let e = PoolError::exhausted();
        // exhausted() has no source
        assert!(e.source().is_none());
    }

    #[test]
    fn connect_error_with_source_chain() {
        let inner = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let e = ConnectError::with_source("connection failed", inner);
        assert!(e.source().is_some());
    }

    #[test]
    fn server_error_preserves_detail_and_hint() {
        let driver_err = bsql_driver::DriverError::Server {
            code: "23505".into(),
            message: "duplicate key".into(),
            detail: Some("Key (login)=(alice) already exists.".into()),
            hint: Some("Use ON CONFLICT to handle duplicates.".into()),
        };
        let e = BsqlError::from(driver_err);
        let display = e.to_string();
        assert!(
            display.contains("duplicate key"),
            "missing message: {display}"
        );
        assert!(
            display.contains("detail: Key (login)=(alice) already exists."),
            "missing detail: {display}"
        );
        assert!(
            display.contains("hint: Use ON CONFLICT to handle duplicates."),
            "missing hint: {display}"
        );
        // pg_code should be preserved
        match &e {
            BsqlError::Query(qe) => assert_eq!(qe.pg_code.as_deref(), Some("23505")),
            other => panic!("expected Query, got: {other:?}"),
        }
    }

    #[test]
    fn server_error_without_detail_hint() {
        let driver_err = bsql_driver::DriverError::Server {
            code: "42P01".into(),
            message: "relation does not exist".into(),
            detail: None,
            hint: None,
        };
        let e = BsqlError::from(driver_err);
        let display = e.to_string();
        assert!(
            display.contains("relation does not exist"),
            "missing message: {display}"
        );
        assert!(
            !display.contains("detail:"),
            "should not contain detail: {display}"
        );
        assert!(
            !display.contains("hint:"),
            "should not contain hint: {display}"
        );
    }

    #[test]
    fn decode_error_has_no_source() {
        let e = BsqlError::Decode(DecodeError {
            column: Cow::Borrowed("col"),
            expected: "i32",
            actual: Cow::Borrowed("text"),
            source: None,
        });
        assert!(e.source().is_none());
    }

    #[test]
    fn decode_error_with_source_chain() {
        let inner = std::io::Error::new(std::io::ErrorKind::InvalidData, "bad utf-8");
        let e = DecodeError::with_source("name", "String", "invalid bytes", inner);
        assert!(e.source().is_some());
        match &e {
            BsqlError::Decode(d) => {
                assert_eq!(d.column, "name");
                assert_eq!(d.expected, "String");
            }
            other => panic!("expected Decode, got: {other:?}"),
        }
    }

    #[test]
    fn is_timeout_true_for_57014() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("canceling statement due to statement timeout"),
            pg_code: Some(Box::from("57014")),
            source: None,
        });
        assert!(e.is_timeout());
    }

    #[test]
    fn is_timeout_false_for_other_codes() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("unique violation"),
            pg_code: Some(Box::from("23505")),
            source: None,
        });
        assert!(!e.is_timeout());
    }

    #[test]
    fn is_timeout_false_for_non_query() {
        let e = PoolError::exhausted();
        assert!(!e.is_timeout());
    }

    #[test]
    fn is_serialization_failure_true_for_40001() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("could not serialize access"),
            pg_code: Some(Box::from("40001")),
            source: None,
        });
        assert!(e.is_serialization_failure());
    }

    #[test]
    fn is_serialization_failure_false_for_other_codes() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("timeout"),
            pg_code: Some(Box::from("57014")),
            source: None,
        });
        assert!(!e.is_serialization_failure());
    }

    #[test]
    fn from_driver_query_maps_io_to_query() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broke");
        let e = BsqlError::from_driver_query(bsql_driver::DriverError::Io(io_err));
        match &e {
            BsqlError::Query(q) => {
                assert!(q.message.contains("I/O error during query"));
                assert!(q.source.is_some());
            }
            other => panic!("expected Query, got: {other:?}"),
        }
    }

    #[test]
    fn from_driver_query_non_io_delegates_to_from() {
        let e = BsqlError::from_driver_query(bsql_driver::DriverError::Pool("test".into()));
        assert!(matches!(e, BsqlError::Pool(_)));
    }
}
