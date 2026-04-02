//! PostgreSQL connection — startup, authentication, statement cache, query execution.
//!
//! `Connection` owns a TCP (or TLS) stream and implements the extended query protocol
//! with pipelining. Statements are cached by rapidhash of the SQL text. On first use,
//! Parse+Describe+Bind+Execute+Sync are pipelined in one TCP write. On subsequent uses,
//! only Bind+Execute+Sync are sent.

use std::collections::HashMap;
use std::sync::Arc;

use rapidhash::quality::RapidHasher;
use std::hash::Hasher;

use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::DriverError;
use crate::arena::Arena;
use crate::auth;
use crate::codec::Encode;
use crate::proto::{self, BackendMessage};

#[cfg(feature = "tls")]
use crate::tls;

// --- Stream abstraction ---

/// The underlying stream type — either plain TCP or TLS.
enum Stream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl Stream {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.write_all(buf).await,
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.write_all(buf).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush().await,
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.flush().await,
        }
    }
}

/// Wrapper to implement AsyncRead for Stream.
struct StreamReader<'a>(&'a mut Stream);

impl AsyncRead for StreamReader<'_> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self.0 {
            Stream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

// --- Config ---

/// Connection configuration parsed from a URL.
///
/// Format: `postgres://user:password@host:port/database`
#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub ssl: SslMode,
}

/// SSL/TLS connection mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    /// Never use TLS.
    Disable,
    /// Try TLS, fall back to plain if server says 'N'.
    Prefer,
    /// Require TLS, fail if server says 'N'.
    Require,
}

impl Config {
    /// Parse a PostgreSQL connection URL.
    ///
    /// Format: `postgres://user:password@host:port/database?sslmode=prefer`
    pub fn from_url(url: &str) -> Result<Self, DriverError> {
        let url = url
            .strip_prefix("postgres://")
            .or_else(|| url.strip_prefix("postgresql://"))
            .ok_or_else(|| DriverError::Protocol("URL must start with postgres://".into()))?;

        // Split user:password@host:port/database
        let (userinfo, rest) = url
            .split_once('@')
            .ok_or_else(|| DriverError::Protocol("missing @ in connection URL".into()))?;

        let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));

        // Split host:port/database?params
        let (hostport, rest) = rest.split_once('/').unwrap_or((rest, ""));
        let (database, params) = rest.split_once('?').unwrap_or((rest, ""));

        let (host, port) = if let Some((h, p)) = hostport.split_once(':') {
            let port = p
                .parse::<u16>()
                .map_err(|_| DriverError::Protocol(format!("invalid port: {p}")))?;
            (h.to_owned(), port)
        } else {
            (hostport.to_owned(), 5432)
        };

        let mut ssl = SslMode::Prefer;
        for param in params.split('&') {
            if let Some(val) = param.strip_prefix("sslmode=") {
                ssl = match val {
                    "disable" => SslMode::Disable,
                    "prefer" => SslMode::Prefer,
                    "require" => SslMode::Require,
                    _ => SslMode::Prefer,
                };
            }
        }

        Ok(Config {
            host,
            port,
            user: url_decode(user),
            password: url_decode(password),
            database: if database.is_empty() {
                url_decode(user)
            } else {
                url_decode(database)
            },
            ssl,
        })
    }
}

/// Minimal percent-decoding for connection URL components.
fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.as_bytes().iter();
    while let Some(&b) = chars.next() {
        if b == b'%' {
            let hi = chars.next().copied().unwrap_or(0);
            let lo = chars.next().copied().unwrap_or(0);
            let val = hex_val(hi) * 16 + hex_val(lo);
            result.push(val as char);
        } else {
            result.push(b as char);
        }
    }
    result
}

fn hex_val(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

/// Owned action from a startup message, avoiding borrow conflicts with `self.read_buf`.
enum StartupAction {
    AuthOk,
    AuthCleartext,
    AuthMd5([u8; 4]),
    AuthSasl(Vec<u8>),
    ParameterStatus(Box<str>, Box<str>),
    BackendKeyData(i32, i32),
    ReadyForQuery(u8),
    Error(String),
    Notice,
}

// --- Statement cache ---

/// Cached information about a prepared statement.
struct StmtInfo {
    /// Statement name: `"s_{hash:016x}"`
    name: Box<str>,
    /// Column metadata from RowDescription.
    columns: Arc<[ColumnDesc]>,
}

/// Description of a result column.
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    /// Column name from the query.
    pub name: Box<str>,
    /// PostgreSQL type OID.
    pub type_oid: u32,
    /// Type size in bytes (-1 for variable-length).
    pub type_size: i16,
}

// --- Connection ---

/// A PostgreSQL connection with statement cache and inline message processing.
///
/// Connections are not `Send` — they must be used on one task at a time. The pool
/// handles concurrent access by lending connections to individual tasks.
pub struct Connection {
    stream: Stream,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    stmts: HashMap<u64, StmtInfo>,
    params: HashMap<Box<str>, Box<str>>,
    pid: i32,
    secret: i32,
    tx_status: u8,
}

impl Connection {
    /// Connect to PostgreSQL and complete the startup/auth handshake.
    pub async fn connect(config: &Config) -> Result<Self, DriverError> {
        let addr = format!("{}:{}", config.host, config.port);
        let tcp = TcpStream::connect(&addr).await.map_err(DriverError::Io)?;

        // Set TCP_NODELAY to avoid Nagle delay on pipelined messages
        tcp.set_nodelay(true).map_err(DriverError::Io)?;

        let stream = match config.ssl {
            SslMode::Disable => Stream::Plain(tcp),
            #[cfg(feature = "tls")]
            SslMode::Prefer | SslMode::Require => {
                match tls::try_upgrade(tcp, &config.host, config.ssl == SslMode::Require).await {
                    Ok(tls_stream) => Stream::Tls(tls_stream),
                    Err(e) if config.ssl == SslMode::Require => return Err(e),
                    Err(_) => {
                        // Prefer mode: TLS failed, reconnect plain
                        let tcp = TcpStream::connect(&addr).await.map_err(DriverError::Io)?;
                        tcp.set_nodelay(true).map_err(DriverError::Io)?;
                        Stream::Plain(tcp)
                    }
                }
            }
            #[cfg(not(feature = "tls"))]
            SslMode::Require => {
                return Err(DriverError::Protocol(
                    "TLS required but bsql-driver compiled without 'tls' feature".into(),
                ));
            }
            #[cfg(not(feature = "tls"))]
            SslMode::Prefer => Stream::Plain(tcp),
        };

        let mut conn = Self {
            stream,
            read_buf: Vec::with_capacity(8192),
            write_buf: Vec::with_capacity(4096),
            stmts: HashMap::new(),
            params: HashMap::new(),
            pid: 0,
            secret: 0,
            tx_status: b'I',
        };

        conn.startup(config).await?;
        Ok(conn)
    }

    /// Perform the startup handshake: StartupMessage -> auth -> parameter status -> ReadyForQuery.
    ///
    /// Uses a two-phase read approach: first read the message type + copy needed
    /// data out of the borrow, then act on it. This avoids holding a borrow on
    /// `self.read_buf` while calling other `&mut self` methods.
    async fn startup(&mut self, config: &Config) -> Result<(), DriverError> {
        // Send StartupMessage
        self.write_buf.clear();
        proto::write_startup(&mut self.write_buf, &config.user, &config.database);
        self.flush_write().await?;

        // Process auth and startup messages
        loop {
            let action = self.read_startup_action().await?;
            match action {
                StartupAction::AuthOk => {}
                StartupAction::AuthCleartext => {
                    self.write_buf.clear();
                    let mut pw = config.password.as_bytes().to_vec();
                    pw.push(0);
                    proto::write_password(&mut self.write_buf, &pw);
                    self.flush_write().await?;
                }
                StartupAction::AuthMd5(salt) => {
                    self.write_buf.clear();
                    let hash = auth::md5_password(&config.user, &config.password, &salt);
                    proto::write_password(&mut self.write_buf, &hash);
                    self.flush_write().await?;
                }
                StartupAction::AuthSasl(mechanisms_data) => {
                    self.handle_scram(config, &mechanisms_data).await?;
                }
                StartupAction::ParameterStatus(name, value) => {
                    self.params.insert(name, value);
                }
                StartupAction::BackendKeyData(pid, secret) => {
                    self.pid = pid;
                    self.secret = secret;
                }
                StartupAction::ReadyForQuery(status) => {
                    self.tx_status = status;
                    return Ok(());
                }
                StartupAction::Error(msg) => {
                    return Err(DriverError::Auth(msg));
                }
                StartupAction::Notice => {}
            }
        }
    }

    /// Read one startup message, parse it, copy needed data, and return an owned action.
    ///
    /// This method reads the raw message into `self.read_buf`, parses it, extracts
    /// all needed data into owned types, and drops the borrow before returning.
    async fn read_startup_action(&mut self) -> Result<StartupAction, DriverError> {
        let (msg_type, _) =
            proto::read_message(&mut StreamReader(&mut self.stream), &mut self.read_buf).await?;
        self.read_startup_message_from_type(msg_type)
    }

    fn read_startup_message_from_type(&self, msg_type: u8) -> Result<StartupAction, DriverError> {
        let payload = &self.read_buf;
        let msg = proto::parse_backend_message(msg_type, payload)?;
        match msg {
            BackendMessage::AuthOk => Ok(StartupAction::AuthOk),
            BackendMessage::AuthCleartext => Ok(StartupAction::AuthCleartext),
            BackendMessage::AuthMd5 { salt } => Ok(StartupAction::AuthMd5(salt)),
            BackendMessage::AuthSasl { mechanisms } => {
                Ok(StartupAction::AuthSasl(mechanisms.to_vec()))
            }
            BackendMessage::ParameterStatus { name, value } => {
                Ok(StartupAction::ParameterStatus(name.into(), value.into()))
            }
            BackendMessage::BackendKeyData { pid, secret } => {
                Ok(StartupAction::BackendKeyData(pid, secret))
            }
            BackendMessage::ReadyForQuery { status } => Ok(StartupAction::ReadyForQuery(status)),
            BackendMessage::ErrorResponse { data } => {
                let fields = proto::parse_error_response(data);
                Ok(StartupAction::Error(fields.to_string()))
            }
            BackendMessage::NoticeResponse { .. } => Ok(StartupAction::Notice),
            other => Err(DriverError::Protocol(format!(
                "unexpected message during startup: {other:?}"
            ))),
        }
    }

    /// Handle SCRAM-SHA-256 authentication exchange.
    async fn handle_scram(
        &mut self,
        config: &Config,
        mechanisms_data: &[u8],
    ) -> Result<(), DriverError> {
        let mechs = auth::parse_sasl_mechanisms(mechanisms_data);
        if !mechs.contains(&"SCRAM-SHA-256") {
            return Err(DriverError::Auth(format!(
                "server requires unsupported SASL mechanism(s): {mechs:?}"
            )));
        }

        let mut scram = auth::ScramClient::new(&config.user, &config.password);

        // Send SASLInitialResponse
        let client_first = scram.client_first_message();
        self.write_buf.clear();
        proto::write_sasl_initial(&mut self.write_buf, "SCRAM-SHA-256", &client_first);
        self.flush_write().await?;

        // Read SASLContinue — read message, extract data, drop borrow
        let (msg_type, _) =
            proto::read_message(&mut StreamReader(&mut self.stream), &mut self.read_buf).await?;
        let server_first = {
            let msg = proto::parse_backend_message(msg_type, &self.read_buf)?;
            match msg {
                BackendMessage::AuthSaslContinue { data } => data.to_vec(),
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    return Err(DriverError::Auth(fields.to_string()));
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "expected AuthSaslContinue, got: {other:?}"
                    )));
                }
            }
        };

        scram.process_server_first(&server_first)?;

        // Send SASLResponse (client-final)
        let client_final = scram.client_final_message()?;
        self.write_buf.clear();
        proto::write_sasl_response(&mut self.write_buf, &client_final);
        self.flush_write().await?;

        // Read SASLFinal — read message, extract data, drop borrow
        let (msg_type, _) =
            proto::read_message(&mut StreamReader(&mut self.stream), &mut self.read_buf).await?;
        {
            let msg = proto::parse_backend_message(msg_type, &self.read_buf)?;
            match msg {
                BackendMessage::AuthSaslFinal { data } => {
                    // Copy server final data to verify after the borrow ends
                    let data_owned = data.to_vec();
                    scram.verify_server_final(&data_owned)?;
                }
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    return Err(DriverError::Auth(fields.to_string()));
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "expected AuthSaslFinal, got: {other:?}"
                    )));
                }
            }
        }

        // AuthOk should follow
        let (msg_type, _) =
            proto::read_message(&mut StreamReader(&mut self.stream), &mut self.read_buf).await?;
        let msg = proto::parse_backend_message(msg_type, &self.read_buf)?;
        match msg {
            BackendMessage::AuthOk => Ok(()),
            BackendMessage::ErrorResponse { data } => {
                let fields = proto::parse_error_response(data);
                Err(DriverError::Auth(fields.to_string()))
            }
            other => Err(DriverError::Protocol(format!(
                "expected AuthOk after SCRAM, got: {other:?}"
            ))),
        }
    }

    // --- Query execution ---

    /// Execute a prepared query and return rows in arena-allocated storage.
    ///
    /// If the statement is not yet cached, Parse+Describe+Bind+Execute+Sync are
    /// pipelined in a single TCP write. On cache hit, only Bind+Execute+Sync are sent.
    pub async fn query(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn Encode],
        arena: &mut Arena,
    ) -> Result<QueryResult, DriverError> {
        let cached = self.stmts.contains_key(&sql_hash);
        let stmt_name = if cached {
            self.stmts[&sql_hash].name.clone()
        } else {
            format!("s_{sql_hash:016x}").into_boxed_str()
        };

        // Build the pipelined message
        self.write_buf.clear();

        if !cached {
            // Parse: prepare the statement
            let param_oids: Vec<u32> = params.iter().map(|p| p.type_oid()).collect();
            proto::write_parse(&mut self.write_buf, &stmt_name, sql, &param_oids);
            // Describe: get column metadata
            proto::write_describe(&mut self.write_buf, b'S', &stmt_name);
        }

        // Bind: bind parameters (binary format)
        let encoded_params = encode_params(params);
        let param_refs: Vec<Option<&[u8]>> =
            encoded_params.iter().map(|p| Some(p.as_slice())).collect();
        proto::write_bind(&mut self.write_buf, "", &stmt_name, &param_refs);

        // Execute: run the portal
        proto::write_execute(&mut self.write_buf, "", 0);

        // Sync: end pipeline
        proto::write_sync(&mut self.write_buf);

        self.flush_write().await?;

        // Read responses
        let columns = if !cached {
            // ParseComplete
            self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))
                .await?;

            // RowDescription or NoData
            let msg = self.read_one_message().await?;
            let columns: Arc<[ColumnDesc]> = match msg {
                BackendMessage::RowDescription { data } => {
                    let col_infos = proto::parse_row_description(data)?;
                    col_infos
                        .into_iter()
                        .map(|c| ColumnDesc {
                            name: c.name,
                            type_oid: c.type_oid,
                            type_size: c.type_size,
                        })
                        .collect::<Vec<_>>()
                        .into()
                }
                BackendMessage::ParameterDescription { .. } => {
                    // ParameterDescription comes before RowDescription for Describe Statement
                    let msg = self.read_one_message().await?;
                    match msg {
                        BackendMessage::RowDescription { data } => {
                            let col_infos = proto::parse_row_description(data)?;
                            col_infos
                                .into_iter()
                                .map(|c| ColumnDesc {
                                    name: c.name,
                                    type_oid: c.type_oid,
                                    type_size: c.type_size,
                                })
                                .collect::<Vec<_>>()
                                .into()
                        }
                        BackendMessage::NoData => Arc::from(Vec::new()),
                        BackendMessage::ErrorResponse { data } => {
                            let fields = proto::parse_error_response(data);
                            // Drain to ReadyForQuery
                            self.drain_to_ready().await?;
                            return Err(DriverError::Server {
                                code: fields.code,
                                message: fields.message,
                                detail: fields.detail,
                                hint: fields.hint,
                            });
                        }
                        other => {
                            return Err(DriverError::Protocol(format!(
                                "expected RowDescription or NoData, got: {other:?}"
                            )));
                        }
                    }
                }
                BackendMessage::NoData => Arc::from(Vec::new()),
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(DriverError::Server {
                        code: fields.code,
                        message: fields.message,
                        detail: fields.detail,
                        hint: fields.hint,
                    });
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "expected RowDescription/NoData after Parse, got: {other:?}"
                    )));
                }
            };

            // Cache the statement
            self.stmts.insert(
                sql_hash,
                StmtInfo {
                    name: stmt_name,
                    columns: columns.clone(),
                },
            );
            columns
        } else {
            self.stmts[&sql_hash].columns.clone()
        };

        // BindComplete
        self.expect_message(|m| matches!(m, BackendMessage::BindComplete))
            .await?;

        // Read DataRow messages and CommandComplete
        let mut row_offsets: Vec<RowData> = Vec::new();
        let mut affected_rows: u64 = 0;

        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::DataRow { data } => {
                    let row = parse_data_row(data, arena)?;
                    row_offsets.push(row);
                }
                BackendMessage::CommandComplete { tag } => {
                    affected_rows = proto::parse_command_tag(tag);
                    break;
                }
                BackendMessage::EmptyQuery => {
                    break;
                }
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(DriverError::Server {
                        code: fields.code,
                        message: fields.message,
                        detail: fields.detail,
                        hint: fields.hint,
                    });
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "unexpected message during query: {other:?}"
                    )));
                }
            }
        }

        // ReadyForQuery
        self.expect_ready().await?;

        Ok(QueryResult {
            row_offsets,
            columns,
            affected_rows,
        })
    }

    /// Execute a query without result rows (INSERT/UPDATE/DELETE).
    ///
    /// Returns the number of affected rows.
    pub async fn execute(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn Encode],
    ) -> Result<u64, DriverError> {
        let mut arena = Arena::new();
        let result = self.query(sql, sql_hash, params, &mut arena).await?;
        Ok(result.affected_rows)
    }

    /// Simple query protocol — for non-prepared SQL (BEGIN, COMMIT, SET, etc.).
    ///
    /// Does not use the extended query protocol. Cannot have parameters.
    pub async fn simple_query(&mut self, sql: &str) -> Result<(), DriverError> {
        self.write_buf.clear();
        proto::write_simple_query(&mut self.write_buf, sql);
        self.flush_write().await?;

        // Read until ReadyForQuery
        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::ReadyForQuery { status } => {
                    self.tx_status = status;
                    return Ok(());
                }
                BackendMessage::CommandComplete { .. } => {}
                BackendMessage::RowDescription { .. } => {}
                BackendMessage::DataRow { .. } => {}
                BackendMessage::EmptyQuery => {}
                BackendMessage::NoticeResponse { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    // Continue reading until ReadyForQuery, then return error
                    loop {
                        let msg = self.read_one_message().await?;
                        if matches!(msg, BackendMessage::ReadyForQuery { .. }) {
                            break;
                        }
                    }
                    return Err(DriverError::Server {
                        code: fields.code,
                        message: fields.message,
                        detail: fields.detail,
                        hint: fields.hint,
                    });
                }
                _ => {}
            }
        }
    }

    /// Send Terminate and close the connection.
    pub async fn close(mut self) -> Result<(), DriverError> {
        self.write_buf.clear();
        proto::write_terminate(&mut self.write_buf);
        // Best-effort flush — ignore errors since we're closing
        let _ = self.flush_write().await;
        Ok(())
    }

    /// Whether the connection is in an idle transaction state.
    pub fn is_idle(&self) -> bool {
        self.tx_status == b'I'
    }

    /// Whether the connection is in a transaction.
    pub fn is_in_transaction(&self) -> bool {
        self.tx_status == b'T'
    }

    /// Whether the connection is in a failed transaction.
    pub fn is_in_failed_transaction(&self) -> bool {
        self.tx_status == b'E'
    }

    /// Get a server parameter value (set during startup or via SET).
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.params.get(name).map(|s| &**s)
    }

    /// Backend process ID (for cancel requests).
    pub fn pid(&self) -> i32 {
        self.pid
    }

    // --- Internal helpers ---

    /// Read one backend message. The returned message borrows from `self.read_buf`.
    ///
    /// We need to use an index-based approach because the message borrows from
    /// `read_buf`, and we can't return a reference to it while `self` is borrowed.
    async fn read_one_message(&mut self) -> Result<BackendMessage<'_>, DriverError> {
        let (msg_type, _payload_len) =
            proto::read_message(&mut StreamReader(&mut self.stream), &mut self.read_buf).await?;
        proto::parse_backend_message(msg_type, &self.read_buf)
    }

    /// Read messages until we find one matching `pred`, erroring on ErrorResponse.
    ///
    /// On error, drains to ReadyForQuery so the connection remains usable.
    async fn expect_message(
        &mut self,
        pred: impl Fn(&BackendMessage<'_>) -> bool,
    ) -> Result<(), DriverError> {
        loop {
            let msg = self.read_one_message().await?;
            if pred(&msg) {
                return Ok(());
            }
            match msg {
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(DriverError::Server {
                        code: fields.code,
                        message: fields.message,
                        detail: fields.detail,
                        hint: fields.hint,
                    });
                }
                BackendMessage::NoticeResponse { .. } | BackendMessage::ParameterStatus { .. } => {
                    // These can arrive at any time — skip them
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "unexpected message while waiting for expected type: {other:?}"
                    )));
                }
            }
        }
    }

    /// Read until ReadyForQuery.
    async fn expect_ready(&mut self) -> Result<(), DriverError> {
        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::ReadyForQuery { status } => {
                    self.tx_status = status;
                    return Ok(());
                }
                BackendMessage::NoticeResponse { .. } | BackendMessage::ParameterStatus { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    // Continue draining until ReadyForQuery
                    loop {
                        let msg2 = self.read_one_message().await?;
                        if let BackendMessage::ReadyForQuery { status } = msg2 {
                            self.tx_status = status;
                            break;
                        }
                    }
                    return Err(DriverError::Server {
                        code: fields.code,
                        message: fields.message,
                        detail: fields.detail,
                        hint: fields.hint,
                    });
                }
                _ => {}
            }
        }
    }

    /// Drain messages until ReadyForQuery (used after an error).
    async fn drain_to_ready(&mut self) -> Result<(), DriverError> {
        loop {
            let msg = self.read_one_message().await?;
            if let BackendMessage::ReadyForQuery { status } = msg {
                self.tx_status = status;
                return Ok(());
            }
        }
    }

    /// Flush the write buffer to the stream.
    async fn flush_write(&mut self) -> Result<(), DriverError> {
        self.stream
            .write_all(&self.write_buf)
            .await
            .map_err(DriverError::Io)?;
        self.stream.flush().await.map_err(DriverError::Io)?;
        Ok(())
    }
}

// --- QueryResult ---

/// Per-row data: column offsets and lengths stored in the arena.
pub(crate) struct RowData {
    /// (arena_offset, length) per column. length = -1 means NULL.
    col_offsets: Vec<(u32, i32)>,
}

/// Result of a query execution. Owns the row offset metadata.
///
/// Row data is stored in the caller-provided `Arena`. The `QueryResult` holds only
/// the offset/length pairs that describe where each column's data lives in the arena.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), bsql_driver::DriverError> {
/// # let mut conn: bsql_driver::Connection = todo!();
/// # let mut arena = bsql_driver::Arena::new();
/// let result = conn.query("SELECT 1 as n", 0, &[], &mut arena).await?;
/// for i in 0..result.len() {
///     let row = result.row(i, &arena);
///     // Access columns by index
/// }
/// # Ok(())
/// # }
/// ```
pub struct QueryResult {
    row_offsets: Vec<RowData>,
    columns: Arc<[ColumnDesc]>,
    affected_rows: u64,
}

impl QueryResult {
    /// Number of rows in the result.
    pub fn len(&self) -> usize {
        self.row_offsets.len()
    }

    /// Whether the result set is empty.
    pub fn is_empty(&self) -> bool {
        self.row_offsets.is_empty()
    }

    /// Number of affected rows (for INSERT/UPDATE/DELETE).
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    /// Column descriptors.
    pub fn columns(&self) -> &[ColumnDesc] {
        &self.columns
    }

    /// Get a row by index. The returned `Row` borrows from the arena.
    pub fn row<'a>(&'a self, idx: usize, arena: &'a Arena) -> Row<'a> {
        Row {
            arena,
            data: &self.row_offsets[idx],
            columns: &self.columns,
        }
    }

    /// Iterate over rows.
    pub fn rows<'a>(&'a self, arena: &'a Arena) -> impl Iterator<Item = Row<'a>> {
        self.row_offsets.iter().map(move |data| Row {
            arena,
            data,
            columns: &self.columns,
        })
    }
}

// --- Row ---

/// A view into a single result row, borrowing data from the arena.
///
/// Column values are accessed by index. NULL values return `None`.
pub struct Row<'a> {
    arena: &'a Arena,
    data: &'a RowData,
    columns: &'a [ColumnDesc],
}

impl<'a> Row<'a> {
    /// Get the raw bytes for a column, or `None` if NULL.
    pub fn get_raw(&self, idx: usize) -> Option<&'a [u8]> {
        let (offset, len) = self.data.col_offsets[idx];
        if len < 0 {
            None
        } else {
            Some(self.arena.get(offset as usize, len as usize))
        }
    }

    /// Whether a column is NULL.
    pub fn is_null(&self, idx: usize) -> bool {
        self.data.col_offsets[idx].1 < 0
    }

    /// Number of columns.
    pub fn column_count(&self) -> usize {
        self.data.col_offsets.len()
    }

    /// Get a boolean column value.
    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        self.get_raw(idx)
            .map(|data| crate::codec::decode_bool(data).expect("invalid bool data"))
    }

    /// Get an i16 column value.
    pub fn get_i16(&self, idx: usize) -> Option<i16> {
        self.get_raw(idx)
            .map(|data| crate::codec::decode_i16(data).expect("invalid i16 data"))
    }

    /// Get an i32 column value.
    pub fn get_i32(&self, idx: usize) -> Option<i32> {
        self.get_raw(idx)
            .map(|data| crate::codec::decode_i32(data).expect("invalid i32 data"))
    }

    /// Get an i64 column value.
    pub fn get_i64(&self, idx: usize) -> Option<i64> {
        self.get_raw(idx)
            .map(|data| crate::codec::decode_i64(data).expect("invalid i64 data"))
    }

    /// Get an f32 column value.
    pub fn get_f32(&self, idx: usize) -> Option<f32> {
        self.get_raw(idx)
            .map(|data| crate::codec::decode_f32(data).expect("invalid f32 data"))
    }

    /// Get an f64 column value.
    pub fn get_f64(&self, idx: usize) -> Option<f64> {
        self.get_raw(idx)
            .map(|data| crate::codec::decode_f64(data).expect("invalid f64 data"))
    }

    /// Get a string column value.
    pub fn get_str(&self, idx: usize) -> Option<&'a str> {
        self.get_raw(idx)
            .map(|data| crate::codec::decode_str(data).expect("invalid UTF-8 in text column"))
    }

    /// Get a byte slice column value.
    pub fn get_bytes(&self, idx: usize) -> Option<&'a [u8]> {
        self.get_raw(idx)
    }

    /// Get the column name by index.
    pub fn column_name(&self, idx: usize) -> &str {
        &self.columns[idx].name
    }

    /// Get the column type OID by index.
    pub fn column_type_oid(&self, idx: usize) -> u32 {
        self.columns[idx].type_oid
    }
}

// --- DataRow parsing ---

/// Parse a DataRow message, copying column data into the arena.
///
/// DataRow format: `[num_columns: i16] ([col_len: i32] [col_data: col_len bytes])...`
/// `col_len = -1` indicates NULL.
fn parse_data_row(data: &[u8], arena: &mut Arena) -> Result<RowData, DriverError> {
    if data.len() < 2 {
        return Err(DriverError::Protocol("DataRow too short".into()));
    }

    let num_cols = i16::from_be_bytes([data[0], data[1]]) as usize;
    let mut col_offsets = Vec::with_capacity(num_cols);
    let mut pos = 2;

    for _ in 0..num_cols {
        if pos + 4 > data.len() {
            return Err(DriverError::Protocol("DataRow truncated".into()));
        }

        let col_len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;

        if col_len < 0 {
            // NULL
            col_offsets.push((0, -1));
        } else {
            let len = col_len as usize;
            if pos + len > data.len() {
                return Err(DriverError::Protocol(
                    "DataRow column data truncated".into(),
                ));
            }

            let offset = arena.alloc_copy(&data[pos..pos + len]);
            col_offsets.push((offset as u32, col_len));
            pos += len;
        }
    }

    Ok(RowData { col_offsets })
}

/// Encode parameters into individual byte vectors.
fn encode_params(params: &[&dyn Encode]) -> Vec<Vec<u8>> {
    params
        .iter()
        .map(|p| {
            let mut buf = Vec::new();
            p.encode_binary(&mut buf);
            buf
        })
        .collect()
}

/// Compute a rapidhash of a SQL string.
pub fn hash_sql(sql: &str) -> u64 {
    let mut hasher = RapidHasher::default();
    hasher.write(sql.as_bytes());
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_parse_full_url() {
        let cfg = Config::from_url("postgres://user:pass@localhost:5432/mydb").unwrap();
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.password, "pass");
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.database, "mydb");
    }

    #[test]
    fn config_parse_default_port() {
        let cfg = Config::from_url("postgres://user:pass@localhost/mydb").unwrap();
        assert_eq!(cfg.port, 5432);
    }

    #[test]
    fn config_parse_no_password() {
        let cfg = Config::from_url("postgres://user@localhost/mydb").unwrap();
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.password, "");
    }

    #[test]
    fn config_parse_empty_database() {
        let cfg = Config::from_url("postgres://user:pass@localhost").unwrap();
        // database defaults to user
        assert_eq!(cfg.database, "user");
    }

    #[test]
    fn config_parse_sslmode() {
        let cfg = Config::from_url("postgres://user:pass@localhost/db?sslmode=require").unwrap();
        assert_eq!(cfg.ssl, SslMode::Require);
    }

    #[test]
    fn config_parse_percent_encoding() {
        let cfg = Config::from_url("postgres://user%40domain:p%40ss@localhost/db").unwrap();
        assert_eq!(cfg.user, "user@domain");
        assert_eq!(cfg.password, "p@ss");
    }

    #[test]
    fn config_rejects_bad_scheme() {
        let result = Config::from_url("mysql://user:pass@localhost/db");
        assert!(result.is_err());
    }

    #[test]
    fn hash_sql_deterministic() {
        let h1 = hash_sql("SELECT 1");
        let h2 = hash_sql("SELECT 1");
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_sql_different_queries() {
        let h1 = hash_sql("SELECT 1");
        let h2 = hash_sql("SELECT 2");
        assert_ne!(h1, h2);
    }

    #[test]
    fn data_row_parsing() {
        let mut arena = Arena::new();

        // Build a DataRow with 2 columns: i32(42) and NULL
        let mut data = Vec::new();
        data.extend_from_slice(&2i16.to_be_bytes()); // 2 columns

        // Column 1: i32 = 42
        data.extend_from_slice(&4i32.to_be_bytes()); // length = 4
        data.extend_from_slice(&42i32.to_be_bytes()); // value

        // Column 2: NULL
        data.extend_from_slice(&(-1i32).to_be_bytes()); // length = -1

        let row = parse_data_row(&data, &mut arena).unwrap();
        assert_eq!(row.col_offsets.len(), 2);

        // First column should have length 4
        assert_eq!(row.col_offsets[0].1, 4);

        // Second column should be NULL
        assert_eq!(row.col_offsets[1].1, -1);
    }

    #[test]
    fn data_row_empty() {
        let mut arena = Arena::new();
        let data = 0i16.to_be_bytes();
        let row = parse_data_row(&data, &mut arena).unwrap();
        assert_eq!(row.col_offsets.len(), 0);
    }

    #[test]
    fn query_result_empty() {
        let result = QueryResult {
            row_offsets: vec![],
            columns: Arc::from(Vec::new()),
            affected_rows: 0,
        };
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn url_decode_works() {
        assert_eq!(url_decode("hello%20world"), "hello world");
        assert_eq!(url_decode("no%20escape"), "no escape");
        assert_eq!(url_decode("plain"), "plain");
        assert_eq!(url_decode("a%40b"), "a@b");
    }
}
