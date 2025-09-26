use std::env;
use std::time::Duration;

use tracing_subscriber::EnvFilter;

#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) redis_url: String,
    pub(crate) bind_port: u16,
    pub(crate) udp_timeout: Duration,
}

impl Config {
    pub(crate) fn from_env() -> Self {
        let redis_url =
            env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let bind_port = env::var("PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(8080);
        let udp_timeout_ms = env::var("UDP_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(60_000);

        Self {
            redis_url,
            bind_port,
            udp_timeout: Duration::from_millis(udp_timeout_ms),
        }
    }
}

pub(crate) fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .is_err()
    {
        // Another subscriber was already set; continue with existing configuration.
    }
}
