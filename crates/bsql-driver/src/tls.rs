//! Optional TLS upgrade via rustls.
//!
//! Sends SSLRequest to PostgreSQL, reads the single-byte response ('S' = upgrade,
//! 'N' = no TLS), and upgrades the TCP stream to TLS if accepted.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::DriverError;
use crate::proto;

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
            let tls_config = build_tls_config()?;
            let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));

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

/// Build a rustls ClientConfig with WebPKI root certificates.
fn build_tls_config() -> Result<rustls::ClientConfig, DriverError> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_config_builds() {
        let config = build_tls_config();
        assert!(config.is_ok());
    }
}
