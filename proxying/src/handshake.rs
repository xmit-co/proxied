use std::fmt;
use std::net::SocketAddr;

use redis::AsyncCommands;
use shared::{PROTOCOL_PREFIX, decode_session_bindings};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::session::SessionBinding;

#[derive(Debug)]
pub(crate) enum HandshakeError {
    InvalidPrefix,
    Io(std::io::Error),
    SessionLookup(String),
}

impl fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandshakeError::InvalidPrefix => write!(f, "invalid handshake prefix"),
            HandshakeError::Io(err) => write!(f, "{err}"),
            HandshakeError::SessionLookup(reason) => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for HandshakeError {}

impl From<std::io::Error> for HandshakeError {
    fn from(err: std::io::Error) -> Self {
        HandshakeError::Io(err)
    }
}

pub(crate) async fn read_handshake(stream: &mut TcpStream) -> Result<String, HandshakeError> {
    let mut prefix = [0u8; 3];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(HandshakeError::Io)?;
    if prefix != PROTOCOL_PREFIX {
        return Err(HandshakeError::InvalidPrefix);
    }

    let mut len_buf = [0u8; 1];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(HandshakeError::Io)?;
    let session_len = len_buf[0] as usize;
    if session_len == 0 {
        return Err(HandshakeError::SessionLookup("Invalid session".to_string()));
    }

    let mut id_buf = vec![0u8; session_len];
    stream
        .read_exact(&mut id_buf)
        .await
        .map_err(HandshakeError::Io)?;
    let session_id = String::from_utf8(id_buf)
        .map_err(|_| HandshakeError::SessionLookup("Invalid session".to_string()))?;
    Ok(session_id)
}

pub(crate) async fn load_session_bindings(
    session_id: &str,
    redis: &redis::Client,
) -> Result<Vec<SessionBinding>, String> {
    let key = format!("s:{}", session_id);
    let mut connection = redis
        .get_multiplexed_async_connection()
        .await
        .map_err(|err| format!("Redis error: {err}"))?;
    let payload: Option<Vec<u8>> = connection
        .get(key)
        .await
        .map_err(|err: redis::RedisError| format!("Redis error: {err}"))?;

    let Some(bytes) = payload else {
        return Err("Invalid session".to_string());
    };

    let configs = decode_session_bindings(&bytes)
        .map_err(|err| format!("Invalid session definition: {err}"))?;

    let bindings = configs
        .into_iter()
        .map(|config| SessionBinding {
            listen: SocketAddr::from((config.proxying_ip, config.proxying_port)),
            target: SocketAddr::from((config.target_ip, config.target_port)),
        })
        .collect();

    Ok(bindings)
}

pub(crate) async fn send_ok(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    stream.write_all(b"OK").await
}

pub(crate) async fn send_ko(stream: &mut TcpStream, message: &str) -> Result<(), std::io::Error> {
    stream.write_all(b"KO").await?;
    stream.write_all(message.as_bytes()).await
}
