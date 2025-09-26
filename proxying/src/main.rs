mod config;
mod handshake;
mod session;

use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

use crate::config::{Config, init_tracing};
use crate::handshake::{HandshakeError, load_session_bindings, read_handshake, send_ko};
use crate::session::{SessionBinding, run_session};
use shared::DynError;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    init_tracing();

    let config = Config::from_env();
    info!(port = config.bind_port, redis = %config.redis_url, "proxying starting");

    let redis_client = redis::Client::open(config.redis_url.as_str())?;

    let listen_addr = SocketAddr::from(([0, 0, 0, 0], config.bind_port));
    let listener = TcpListener::bind(listen_addr).await?;
    info!(%listen_addr, "listening for proxied clients");

    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                error!(%err, "failed to accept proxied connection");
                continue;
            }
        };

        let config_clone = config.clone();
        let redis_clone = redis_client.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_client(stream, addr, config_clone, redis_clone).await {
                error!(%err, "session handling failed");
            }
        });
    }
}

async fn handle_client(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    config: Config,
    redis: redis::Client,
) -> Result<(), DynError> {
    stream.set_nodelay(true).ok();
    info!(%peer_addr, "incoming proxied connection");

    let session_id = match read_handshake(&mut stream).await {
        Ok(id) => id,
        Err(HandshakeError::InvalidPrefix) => {
            error!(%peer_addr, "invalid handshake prefix from client");
            return Ok(());
        }
        Err(HandshakeError::Io(err)) => {
            return Err(Box::new(err));
        }
        Err(HandshakeError::SessionLookup(reason)) => {
            send_ko(&mut stream, &reason).await.ok();
            return Ok(());
        }
    };

    let bindings = match load_session_bindings(&session_id, &redis).await {
        Ok(bindings) if bindings.is_empty() => {
            let message = "Invalid session".to_string();
            send_ko(&mut stream, &message).await.ok();
            warn!(%session_id, %peer_addr, "session has no bindings");
            return Ok(());
        }
        Ok(bindings) => bindings,
        Err(message) => {
            send_ko(&mut stream, &message).await.ok();
            warn!(%session_id, %peer_addr, error = %message, "session preparation failed");
            return Ok(());
        }
    };

    let (binding_tx, binding_rx) = watch::channel(bindings);

    let session_id_clone = session_id.clone();
    let redis_clone = redis.clone();
    let updates = tokio::spawn(async move {
        if let Err(err) = watch_session_bindings(redis_clone, session_id_clone, binding_tx).await {
            warn!(%err, "binding watcher terminated early");
        }
    });

    let result = run_session(
        stream,
        session_id,
        binding_rx,
        redis,
        config.udp_timeout,
        peer_addr,
    )
    .await;

    updates.abort();
    if let Err(err) = updates.await
        && !err.is_cancelled()
    {
        warn!(%err, "binding watcher task failed");
    }

    result
}

async fn watch_session_bindings(
    redis: redis::Client,
    session_id: String,
    bindings_tx: watch::Sender<Vec<SessionBinding>>,
) -> Result<(), DynError> {
    let channel = format!("s:{}", session_id);
    let mut pubsub = redis.get_async_pubsub().await?;
    pubsub.subscribe(channel).await?;
    let mut stream = pubsub.into_on_message();

    while let Some(_) = stream.next().await {
        match load_session_bindings(&session_id, &redis).await {
            Ok(updated) => {
                if bindings_tx.send(updated).is_err() {
                    break;
                }
            }
            Err(message) => {
                warn!(%session_id, error = %message, "failed to reload session bindings");
            }
        }
    }

    Ok(())
}
