//! LISTEN/NOTIFY support via a dedicated PostgreSQL connection.
//!
//! [`Listener`] opens a standalone connection (not from the pool) and
//! subscribes to named channels. Notifications arrive asynchronously and
//! are read via [`recv()`](Listener::recv).
//!
//! # Design
//!
//! The listener uses a dedicated connection because LISTEN requires a
//! persistent session -- the subscription is tied to the backend process.
//! Pooled connections cycle between callers, so LISTEN on a pooled
//! connection would silently lose the subscription on return-to-pool.
//!
//! A background task owns the `Connection` exclusively. It uses
//! `tokio::select!` to multiplex between:
//!
//! 1. **Commands** from the caller (listen/unlisten/notify) delivered via
//!    an unbounded mpsc channel.
//! 2. **Notifications** from PostgreSQL read via `Connection::wait_for_notification()`,
//!    forwarded to the caller through a bounded mpsc channel.
//!
//! This avoids splitting `&mut Connection` between two tasks.

use tokio::sync::mpsc;

use crate::error::{BsqlError, BsqlResult, ConnectError};

/// Buffer capacity for the notification channel.
const NOTIFICATION_BUFFER_SIZE: usize = 1024;

/// A notification received from PostgreSQL via LISTEN/NOTIFY.
#[derive(Debug, Clone)]
pub struct Notification {
    channel: String,
    payload: String,
}

impl Notification {
    /// The channel name this notification was raised on.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// The payload string attached to the notification (may be empty).
    pub fn payload(&self) -> &str {
        &self.payload
    }
}

/// Commands sent from the `Listener` API to the background task.
enum Command {
    Listen(
        String,
        tokio::sync::oneshot::Sender<Result<(), bsql_driver::DriverError>>,
    ),
    Unlisten(
        String,
        tokio::sync::oneshot::Sender<Result<(), bsql_driver::DriverError>>,
    ),
    Notify(
        String,
        tokio::sync::oneshot::Sender<Result<(), bsql_driver::DriverError>>,
    ),
}

/// A dedicated LISTEN/NOTIFY connection to PostgreSQL.
///
/// Created via [`Listener::connect`]. This is NOT a pooled connection --
/// it opens a fresh TCP connection that persists for the listener's lifetime.
///
/// # Example
///
/// ```rust,ignore
/// use bsql::Listener;
///
/// let mut listener = Listener::connect("postgres://user:pass@localhost/mydb").await?;
/// listener.listen("order_updates").await?;
///
/// loop {
///     let notif = listener.recv().await?;
///     println!("{}: {}", notif.channel(), notif.payload());
/// }
/// ```
pub struct Listener {
    cmd_tx: mpsc::UnboundedSender<Command>,
    rx: mpsc::Receiver<Notification>,
    _task_handle: tokio::task::JoinHandle<()>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        self._task_handle.abort();
    }
}

impl std::fmt::Debug for Listener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Listener")
            .field("active", &!self._task_handle.is_finished())
            .finish()
    }
}

impl Listener {
    /// Connect to PostgreSQL and start the background notification reader.
    ///
    /// Opens a dedicated connection (not from any pool).
    pub async fn connect(url: &str) -> BsqlResult<Self> {
        let config = bsql_driver::Config::from_url(url)
            .map_err(|e| ConnectError::create(format!("listener connect failed: {e}")))?;

        // Disable statement_timeout on the listener connection -- it only runs
        // LISTEN/UNLISTEN/NOTIFY, and the notification wait is unbounded by design.
        let mut listener_config = config;
        listener_config.statement_timeout_secs = 0;

        let conn = bsql_driver::Connection::connect(&listener_config)
            .await
            .map_err(|e| ConnectError::create(format!("listener connect failed: {e}")))?;

        let (notif_tx, rx) = mpsc::channel(NOTIFICATION_BUFFER_SIZE);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn(drive_listener(conn, cmd_rx, notif_tx));

        Ok(Listener {
            cmd_tx,
            rx,
            _task_handle: handle,
        })
    }

    /// Subscribe to a named notification channel.
    ///
    /// The channel name is properly quoted as a PostgreSQL identifier to
    /// prevent SQL injection.
    pub async fn listen(&self, channel: &str) -> BsqlResult<()> {
        if channel.is_empty() {
            return Err(ConnectError::create(
                "LISTEN channel name must not be empty",
            ));
        }
        let quoted = quote_ident(channel)?;
        let sql = format!("LISTEN {quoted}");
        self.send_command_listen(sql).await
    }

    /// Unsubscribe from a named notification channel.
    pub async fn unlisten(&self, channel: &str) -> BsqlResult<()> {
        if channel.is_empty() {
            return Err(ConnectError::create(
                "UNLISTEN channel name must not be empty",
            ));
        }
        let quoted = quote_ident(channel)?;
        let sql = format!("UNLISTEN {quoted}");
        self.send_command_unlisten(sql).await
    }

    /// Unsubscribe from all channels.
    pub async fn unlisten_all(&self) -> BsqlResult<()> {
        self.send_command_unlisten("UNLISTEN *".to_owned()).await
    }

    /// Receive the next notification.
    ///
    /// Blocks until a notification arrives, or returns an error if the
    /// connection has been closed.
    pub async fn recv(&mut self) -> BsqlResult<Notification> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| ConnectError::create("listener connection closed"))
    }

    /// Send a NOTIFY on a channel with a payload.
    pub async fn notify(&self, channel: &str, payload: &str) -> BsqlResult<()> {
        if channel.is_empty() {
            return Err(ConnectError::create(
                "NOTIFY channel name must not be empty",
            ));
        }
        if payload.contains('\0') {
            return Err(ConnectError::create(
                "NOTIFY payload must not contain null bytes",
            ));
        }
        let quoted_channel = quote_ident(channel)?;
        let escaped_payload = payload.replace('\'', "''");
        let sql = format!("NOTIFY {quoted_channel}, '{escaped_payload}'");

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Command::Notify(sql, resp_tx))
            .map_err(|_| ConnectError::create("listener background task exited"))?;
        resp_rx
            .await
            .map_err(|_| ConnectError::create("listener background task exited"))?
            .map_err(BsqlError::from)
    }

    /// Send a LISTEN command to the background task and wait for the response.
    async fn send_command_listen(&self, sql: String) -> BsqlResult<()> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Command::Listen(sql, resp_tx))
            .map_err(|_| ConnectError::create("listener background task exited"))?;
        resp_rx
            .await
            .map_err(|_| ConnectError::create("listener background task exited"))?
            .map_err(BsqlError::from)
    }

    /// Send an UNLISTEN command to the background task and wait for the response.
    async fn send_command_unlisten(&self, sql: String) -> BsqlResult<()> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Command::Unlisten(sql, resp_tx))
            .map_err(|_| ConnectError::create("listener background task exited"))?;
        resp_rx
            .await
            .map_err(|_| ConnectError::create("listener background task exited"))?
            .map_err(BsqlError::from)
    }
}

/// Quote a PostgreSQL identifier: wrap in double quotes, double any internal quotes.
fn quote_ident(name: &str) -> BsqlResult<String> {
    if name.contains('\0') {
        return Err(ConnectError::create(
            "identifier must not contain null bytes",
        ));
    }
    let mut quoted = String::with_capacity(name.len() + 2);
    quoted.push('"');
    for c in name.chars() {
        if c == '"' {
            quoted.push('"');
        }
        quoted.push(c);
    }
    quoted.push('"');
    Ok(quoted)
}

/// Background task that owns the Connection and multiplexes between
/// reading commands from the API and waiting for PostgreSQL notifications.
async fn drive_listener(
    mut conn: bsql_driver::Connection,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    notif_tx: mpsc::Sender<Notification>,
) {
    loop {
        tokio::select! {
            // Branch 1: Commands from the Listener API
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(Command::Listen(sql, resp)) => {
                        let result = conn.simple_query(&sql).await;
                        let _ = resp.send(result);
                    }
                    Some(Command::Unlisten(sql, resp)) => {
                        let result = conn.simple_query(&sql).await;
                        let _ = resp.send(result);
                    }
                    Some(Command::Notify(sql, resp)) => {
                        let result = conn.simple_query(&sql).await;
                        let _ = resp.send(result);
                    }
                    None => break, // Listener dropped, all senders gone
                }
            }
            // Branch 2: Notifications from PostgreSQL
            notif = conn.wait_for_notification() => {
                match notif {
                    Ok((channel, payload)) => {
                        // try_send: drop notification if buffer is full rather than
                        // blocking the reader (which would prevent processing commands)
                        let _ = notif_tx.try_send(Notification { channel, payload });
                    }
                    Err(_) => break, // Connection error, exit task
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_simple() {
        assert_eq!(quote_ident("my_channel").unwrap(), "\"my_channel\"");
    }

    #[test]
    fn quote_ident_with_double_quotes() {
        assert_eq!(quote_ident("my\"channel").unwrap(), "\"my\"\"channel\"");
    }

    #[test]
    fn quote_ident_empty() {
        assert_eq!(quote_ident("").unwrap(), "\"\"");
    }

    #[test]
    fn quote_ident_with_spaces() {
        assert_eq!(quote_ident("my channel").unwrap(), "\"my channel\"");
    }

    #[test]
    fn quote_ident_with_semicolon() {
        assert_eq!(
            quote_ident("foo; DROP TABLE users").unwrap(),
            "\"foo; DROP TABLE users\""
        );
    }

    #[test]
    fn quote_ident_multiple_quotes() {
        assert_eq!(quote_ident("a\"b\"c").unwrap(), "\"a\"\"b\"\"c\"");
    }

    #[test]
    fn quote_ident_rejects_null_bytes() {
        let result = quote_ident("chan\0nel");
        assert!(result.is_err());
    }
}
