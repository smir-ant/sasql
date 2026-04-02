//! Optional TLS upgrade via rustls.
//!
//! Sends SSLRequest to PostgreSQL, reads the single-byte response ('S' = upgrade,
//! 'N' = no TLS), and upgrades the TCP stream to TLS if accepted.

use std::sync::{Arc, LazyLock};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::DriverError;
use crate::proto;

/// Cached TLS client config. Built once, reused for all connections.
static TLS_CONFIG: LazyLock<Arc<rustls::ClientConfig>> = LazyLock::new(|| {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    )
});

/// Attempt TLS upgrade on a TCP connection.
///
/// 1. Send SSLRequest (8 bytes).
/// 2. Read server response: 'S' (accept) or 'N' (reject).
/// 3. If 'S', perform TLS handshake with rustls.
///
/// If `required` is true and server responds 'N', returns an error.
/// If `required` is false and server responds 'N', returns an error that the
/// caller should handle by falling back to plain TCP (reconnecting).
pub async fn try_upgrade(
    mut tcp: TcpStream,
    host: &str,
    required: bool,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, DriverError> {
    // Send SSLRequest
    let mut buf = Vec::with_capacity(8);
    proto::write_ssl_request(&mut buf);
    tcp.write_all(&buf).await.map_err(DriverError::Io)?;
    tcp.flush().await.map_err(DriverError::Io)?;

    // Read response byte
    let mut response = [0u8; 1];
    tcp.read_exact(&mut response)
        .await
        .map_err(DriverError::Io)?;

    match response[0] {
        b'S' => {
            // Server accepts TLS — perform handshake
            let connector = tokio_rustls::TlsConnector::from(TLS_CONFIG.clone());

            let server_name =
                rustls::pki_types::ServerName::try_from(host.to_owned()).map_err(|e| {
                    DriverError::Protocol(format!("invalid TLS server name '{host}': {e}"))
                })?;

            let tls_stream = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| DriverError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

            Ok(tls_stream)
        }
        b'N' => {
            if required {
                Err(DriverError::Protocol(
                    "server does not support TLS (sslmode=require)".into(),
                ))
            } else {
                Err(DriverError::Protocol(
                    "server declined TLS (sslmode=prefer, falling back)".into(),
                ))
            }
        }
        other => Err(DriverError::Protocol(format!(
            "unexpected SSL response byte: 0x{other:02x}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_config_cached() {
        // Verify the LazyLock TLS config is accessible and reusable
        let c1 = TLS_CONFIG.clone();
        let c2 = TLS_CONFIG.clone();
        assert!(Arc::ptr_eq(&c1, &c2));
    }
}
