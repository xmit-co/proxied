use std::collections::HashMap;
use std::env;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use postcard::from_bytes;
use shared::{
    ConnectionInfo, DynError, FRAME_CLOSE, FRAME_DATA, FRAME_EXIT, FRAME_OPEN_IPV4,
    FRAME_OPEN_IPV6, FRAME_OPEN_UDP, PROTOCOL_PREFIX, close_frame, data_frame,
    is_connection_closed,
};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket, tcp::OwnedReadHalf, tcp::OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;

#[derive(Copy, Clone)]
enum LogLevel {
    Info,
    Warn,
    Error,
}

fn log_with_level(level: LogLevel, args: fmt::Arguments<'_>) {
    let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S %:z");
    match level {
        LogLevel::Info => println!("[{}] {}", timestamp, args),
        LogLevel::Warn => println!("[{}] WARNING: {}", timestamp, args),
        LogLevel::Error => println!("[{}] ERROR: {}", timestamp, args),
    }
}

macro_rules! user_info {
    ($($arg:tt)*) => {{
        log_with_level(LogLevel::Info, format_args!($($arg)*));
    }};
}

macro_rules! user_warn {
    ($($arg:tt)*) => {{
        log_with_level(LogLevel::Warn, format_args!($($arg)*));
    }};
}

macro_rules! user_error {
    ($($arg:tt)*) => {{
        log_with_level(LogLevel::Error, format_args!($($arg)*));
    }};
}

const DEFAULT_API_BASE: &str = "https://proxied.eu";
const DEFAULT_CONTROLLER_ORIGIN: &str = "proxied.eu";
const DEFAULT_CONTROL_PORT: u16 = 32_123;
const STARTUP_FILENAME: &str = ".proxied";

#[derive(Clone, Debug)]
struct Config {
    api_base: String,
    controller_origin: String,
    control_port: u16,
    startup_file: PathBuf,
}

impl Config {
    fn from_env() -> Result<Self, DynError> {
        let api_base = env::var("API").unwrap_or_else(|_| DEFAULT_API_BASE.to_string());
        let controller_origin =
            env::var("CONTROLLER").unwrap_or_else(|_| DEFAULT_CONTROLLER_ORIGIN.to_string());
        let control_port = env::var("PORT")
            .ok()
            .and_then(|port| port.parse::<u16>().ok())
            .unwrap_or(DEFAULT_CONTROL_PORT);

        let home = resolve_home_dir()?;
        let startup_file = home.join(STARTUP_FILENAME);

        Ok(Self {
            api_base,
            controller_origin,
            control_port,
            startup_file,
        })
    }

    fn proxy_endpoint(&self, info: &ConnectionInfo) -> SocketAddr {
        SocketAddr::from((info.target_ip, info.target_port))
    }
}

fn resolve_home_dir() -> Result<PathBuf, DynError> {
    if let Some(home) = env::var_os("HOME") {
        return Ok(PathBuf::from(home));
    }

    #[cfg(windows)]
    {
        if let (Some(drive), Some(path)) = (env::var_os("HOMEDRIVE"), env::var_os("HOMEPATH")) {
            let mut buf = PathBuf::from(drive);
            buf.push(path);
            return Ok(buf);
        }
    }

    Err("Unable to determine home directory".into())
}

struct AppState {
    config: Config,
    startup_id: String,
}

impl AppState {
    fn new(config: Config, startup_id: String) -> Arc<Self> {
        Arc::new(Self { config, startup_id })
    }
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let config = Config::from_env()?;
    user_info!(
        "Starting. Controller origin: {}. Control port: {}. Config file: {}.",
        config.controller_origin,
        config.control_port,
        config.startup_file.display()
    );

    let mut startup_id = read_startup_id(&config.startup_file).await?;
    if startup_id.is_none() {
        user_info!("No saved ID found. Requesting one...");
        let fetched = fetch_startup_id(&config.api_base).await?;
        fs::write(&config.startup_file, fetched.as_bytes()).await?;
        user_info!("Stored freshly issued ID");
        startup_id = Some(fetched);
    }

    let startup_id = startup_id.expect("ID should be present");

    let shared_state = AppState::new(config.clone(), startup_id.clone());

    let proxy_config = config.clone();
    let proxy_startup_id = startup_id.clone();

    let proxy_task = tokio::spawn(async move {
        if let Err(err) = run_session_loop(proxy_config, proxy_startup_id).await {
            user_error!(
                "Background connection stopped working. We'll retry automatically after a short backoff. Details: {}",
                err
            );
        }
    });
    let http_server = run_http_server(shared_state.clone(), config.control_port).await;

    if let Err(err) = http_server {
        user_error!(
            "The control API stopped unexpectedly. Please restart proxied or check for conflicting ports. Details: {}",
            err
        );
    }

    proxy_task.abort();
    Ok(())
}

async fn run_http_server(state: Arc<AppState>, port: u16) -> Result<(), DynError> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = TcpListener::bind(addr).await?;
    user_info!("Control API listening on http://127.0.0.1:{}", port);

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(err) => {
                user_warn!(
                    "Couldn't accept a control connection just now. We'll keep listening. Details: {}",
                    err
                );
                continue;
            }
        };

        stream.set_nodelay(true).ok();
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_control_connection(stream, state.clone()).await {
                if let ControlError::Io(io_err) = &err {
                    if !is_connection_closed(io_err) {
                        user_warn!(
                            "A control connection ended unexpectedly. The service remains available. Details: {}",
                            io_err
                        );
                    }
                } else {
                    user_warn!(
                        "A control connection hit a protocol error. The service remains available. Details: {}",
                        err
                    );
                }
            }
        });
    }
}

const MAX_REQUEST_HEADER_SIZE: usize = 8 * 1024;

#[derive(Debug)]
enum ControlError {
    Io(std::io::Error),
    ConnectionClosed,
    Malformed(&'static str),
}

impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControlError::Io(err) => write!(f, "io error: {}", err),
            ControlError::ConnectionClosed => write!(f, "connection closed"),
            ControlError::Malformed(msg) => write!(f, "malformed request: {}", msg),
        }
    }
}

impl std::error::Error for ControlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ControlError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ControlError {
    fn from(err: std::io::Error) -> Self {
        ControlError::Io(err)
    }
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
}

#[derive(Debug)]
struct SimpleResponse {
    status: u16,
    reason: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl SimpleResponse {
    fn into_bytes(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer
            .extend_from_slice(format!("HTTP/1.1 {} {}\r\n", self.status, self.reason).as_bytes());
        for (name, value) in self.headers {
            buffer.extend_from_slice(name.as_bytes());
            buffer.extend_from_slice(b": ");
            buffer.extend_from_slice(value.as_bytes());
            buffer.extend_from_slice(b"\r\n");
        }
        buffer.extend_from_slice(b"\r\n");
        buffer.extend_from_slice(&self.body);
        buffer
    }
}

async fn handle_control_connection(
    mut stream: TcpStream,
    state: Arc<AppState>,
) -> Result<(), ControlError> {
    let request = match read_http_request(&mut stream).await {
        Ok(req) => req,
        Err(ControlError::ConnectionClosed) => return Ok(()),
        Err(err) => {
            let origin = state.config.controller_origin.clone();
            let status = 400;
            let response = simple_empty_response(status, &origin);
            let _ = stream.write_all(&response.into_bytes()).await;
            return Err(err);
        }
    };

    let response = handle_http_request(state.clone(), &request).await;
    stream.write_all(&response.into_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn handle_http_request(state: Arc<AppState>, request: &HttpRequest) -> SimpleResponse {
    let origin = state.config.controller_origin.clone();
    match request.method.as_str() {
        "OPTIONS" => simple_empty_response(204, &origin),
        "GET" if request.path == "/" => simple_body_response(
            200,
            state.startup_id.clone().into_bytes(),
            Some("text/plain; charset=utf-8"),
            &origin,
        ),
        _ => simple_empty_response(404, &origin),
    }
}

fn simple_empty_response(status: u16, origin: &str) -> SimpleResponse {
    simple_body_response(status, Vec::new(), None, origin)
}

fn simple_body_response(
    status: u16,
    body: Vec<u8>,
    content_type: Option<&str>,
    origin: &str,
) -> SimpleResponse {
    let reason = reason_phrase(status);
    let mut headers = cors_headers(origin);
    headers.push(("Connection".into(), "close".into()));
    headers.push(("Content-Length".into(), body.len().to_string()));
    if let Some(ct) = content_type {
        headers.push(("Content-Type".into(), ct.to_string()));
    }

    SimpleResponse {
        status,
        reason,
        headers,
        body,
    }
}

fn cors_headers(origin: &str) -> Vec<(String, String)> {
    let sanitized = sanitize_header_value(origin).unwrap_or_else(|| "*".to_string());
    vec![
        ("Access-Control-Allow-Origin".into(), sanitized),
        ("Access-Control-Allow-Methods".into(), "OPTIONS, GET".into()),
        ("Access-Control-Allow-Headers".into(), "content-type".into()),
    ]
}

fn sanitize_header_value(input: &str) -> Option<String> {
    if input.is_empty() {
        return None;
    }
    if input
        .bytes()
        .all(|b| (0x20..=0x7e).contains(&b) && b != b'\r' && b != b'\n')
    {
        Some(input.to_string())
    } else {
        None
    }
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        _ => "Unknown",
    }
}

async fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest, ControlError> {
    let mut buffer = Vec::new();
    let mut temp = [0u8; 1024];

    loop {
        if buffer.len() > MAX_REQUEST_HEADER_SIZE {
            return Err(ControlError::Malformed("headers too large"));
        }

        let bytes_read = stream.read(&mut temp).await?;
        if bytes_read == 0 {
            if buffer.is_empty() {
                return Err(ControlError::ConnectionClosed);
            }
            return Err(ControlError::Malformed("unexpected eof"));
        }
        buffer.extend_from_slice(&temp[..bytes_read]);

        if let Some(pos) = find_header_end(&buffer) {
            let header_bytes = &buffer[..pos];
            let header_text = std::str::from_utf8(header_bytes)
                .map_err(|_| ControlError::Malformed("headers not utf-8"))?;
            let mut lines = header_text.split("\r\n");
            let request_line = lines
                .next()
                .ok_or(ControlError::Malformed("missing request line"))?;
            let mut parts = request_line.split_whitespace();
            let method = parts
                .next()
                .ok_or(ControlError::Malformed("missing method"))?
                .to_ascii_uppercase();
            let path = parts
                .next()
                .ok_or(ControlError::Malformed("missing path"))?
                .to_string();
            let version = parts
                .next()
                .ok_or(ControlError::Malformed("missing version"))?;
            if version != "HTTP/1.1" {
                return Err(ControlError::Malformed("unsupported http version"));
            }

            return Ok(HttpRequest { method, path });
        }
    }
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

async fn read_startup_id(path: &Path) -> Result<Option<String>, DynError> {
    match fs::read(path).await {
        Ok(bytes) => {
            if bytes.is_empty() {
                Ok(None)
            } else {
                let content = String::from_utf8(bytes)?;
                if content.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(content))
                }
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(Box::new(err)),
    }
}

async fn run_session_loop(config: Config, startup_id: String) -> Result<(), DynError> {
    let mut delay = Duration::from_secs(1);
    loop {
        user_info!("Connecting to the service...");
        match run_single_session(&config, &startup_id).await {
            Ok(()) => {
                user_info!("The server closed the tunnel. We will retry automatically shortly.");
                delay = Duration::from_secs(1);
            }
            Err(err) => {
                if let Some(exit) = err.downcast_ref::<RemoteExit>() {
                    user_info!(
                        "Remote service requested shutdown with exit code {}. Exiting now...",
                        exit.code
                    );
                    std::process::exit(exit.code as i32);
                }

                if let Some(rejected) = err.downcast_ref::<SessionRejected>() {
                    user_info!(
                        "The server refused this account code: {}. Check that your account details are correct.",
                        rejected
                    );
                    return Ok(());
                }

                let message = err.to_string();
                user_info!(
                    "Connection failed: {}. We will retry automatically.",
                    message
                );
                user_warn!(
                    "Session ID hit a connection problem. We'll try again shortly. Details: {}",
                    message
                );
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }

        user_info!(
            "Waiting {} seconds before the next attempt...",
            delay.as_secs()
        );
        sleep(delay).await;
    }
}

async fn run_single_session(config: &Config, startup_id: &str) -> Result<(), DynError> {
    user_info!("Asking for connection details...");
    let info = fetch_connection_info(&config.api_base, startup_id).await?;
    let target = config.proxy_endpoint(&info);
    user_info!(
        "Connecting to {} via session {}...",
        target,
        info.session_id
    );

    let mut stream = TcpStream::connect(target).await?;
    stream.set_nodelay(true).ok();

    perform_handshake(&mut stream, &info.session_id).await?;
    user_info!("Tunnel established. Traffic will start flowing now.");

    run_proxy_session(stream).await
}

#[derive(Debug)]
struct SessionRejected {
    reason: String,
}

impl fmt::Display for SessionRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.reason)
    }
}

impl std::error::Error for SessionRejected {}

#[derive(Debug)]
struct RemoteExit {
    code: u16,
}

impl fmt::Display for RemoteExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "remote requested exit with code {}", self.code)
    }
}

impl std::error::Error for RemoteExit {}

async fn fetch_connection_info(
    api_base: &str,
    startup_id: &str,
) -> Result<ConnectionInfo, DynError> {
    let url = format!("{}/session/{}", api_base.trim_end_matches('/'), startup_id);
    let response = tokio::task::spawn_blocking(move || {
        minreq::get(url)
            .with_header("User-Agent", "proxied/0")
            .send()
    })
    .await
    .map_err(|err| Box::new(err) as DynError)?;
    let response = response.map_err(|err| Box::new(err) as DynError)?;

    let status = response.status_code;
    let body = response.into_bytes();

    if !(200..300).contains(&status) {
        let message = {
            let raw = String::from_utf8_lossy(&body);
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                "(empty response)".to_string()
            } else {
                trimmed.to_owned()
            }
        };
        user_error!(
            "Session lookup failed with HTTP {}. Server message: {}",
            status,
            message
        );
        return Err(format!("session lookup failed with status {}", status).into());
    }

    let info: ConnectionInfo = from_bytes(&body)?;
    Ok(info)
}

async fn fetch_startup_id(api_base: &str) -> Result<String, DynError> {
    let url = format!("{}/startup", api_base.trim_end_matches('/'));
    let response = tokio::task::spawn_blocking(move || minreq::get(url).send())
        .await
        .map_err(|err| Box::new(err) as DynError)?
        .map_err(|err| Box::new(err) as DynError)?;

    if response.status_code != 200 {
        let body = response
            .as_str()
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        let message = if body.is_empty() {
            "(empty response)".to_string()
        } else {
            body
        };
        return Err(format!(
            "startup acquisition failed with status {} and message {}",
            response.status_code, message
        )
        .into());
    }

    let text = response
        .as_str()
        .map_err(|err| Box::new(err) as DynError)?
        .trim()
        .to_string();

    if text.is_empty() {
        return Err("startup endpoint returned an empty identifier".into());
    }

    Ok(text)
}

async fn perform_handshake(stream: &mut TcpStream, session_id: &str) -> Result<(), DynError> {
    if session_id.len() > u8::MAX as usize {
        return Err("session id too long".into());
    }

    stream.write_all(&PROTOCOL_PREFIX).await?;
    stream.write_all(&[session_id.len() as u8]).await?;
    stream.write_all(session_id.as_bytes()).await?;

    let mut response = [0u8; 2];
    stream.read_exact(&mut response).await?;

    match &response {
        b"OK" => Ok(()),
        b"KO" => {
            let mut error = Vec::new();
            stream.read_to_end(&mut error).await?;
            let reason = String::from_utf8_lossy(&error).to_string();
            let message = if reason.trim().is_empty() {
                "Server rejected the session request".to_string()
            } else {
                format!("Server rejected the session: {}", reason)
            };
            Err(SessionRejected { reason: message }.into())
        }
        _ => Err(format!("unexpected handshake response: {:?}", response).into()),
    }
}

#[derive(Debug)]
enum RemoteFrame {
    OpenIpv4 {
        connection_id: u16,
        address: Ipv4Addr,
        port: u16,
    },
    OpenIpv6 {
        connection_id: u16,
        address: Ipv6Addr,
        port: u16,
    },
    OpenUdp {
        connection_id: u16,
        address: Ipv4Addr,
        port: u16,
    },
    Close {
        connection_id: u16,
    },
    Data {
        connection_id: u16,
        payload: Vec<u8>,
    },
    Exit {
        code: u16,
    },
}

#[derive(Debug)]
enum LocalEvent {
    Closed(u16),
}

enum ConnectionKind {
    Tcp { writer: OwnedWriteHalf },
    Udp { socket: Arc<UdpSocket> },
}

struct ConnectionEntry {
    kind: ConnectionKind,
    reader_task: JoinHandle<()>,
}

async fn run_proxy_session(stream: TcpStream) -> Result<(), DynError> {
    let (mut reader, writer) = stream.into_split();
    let (frame_tx, mut frame_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let writer_task = tokio::spawn(async move {
        let mut writer = writer;
        while let Some(frame) = frame_rx.recv().await {
            if writer.write_all(&frame).await.is_err() {
                break;
            }
        }
        let _ = writer.shutdown().await;
    });

    let (local_event_tx, mut local_event_rx) = mpsc::unbounded_channel::<LocalEvent>();
    let mut connections: HashMap<u16, ConnectionEntry> = HashMap::new();
    let mut exit_request: Option<u16> = None;

    'session: loop {
        tokio::select! {
            frame = read_remote_frame(&mut reader) => {
                match frame {
                    Ok(RemoteFrame::Exit { code }) => {
                        exit_request = Some(code);
                        break 'session;
                    }
                    Ok(other) => {
                        if let Err(err) = handle_remote_frame(other, &mut connections, &frame_tx, &local_event_tx).await {
                            user_warn!(
                                "We hit a problem while forwarding data through the tunnel. The connection will recover if possible. Details: {}",
                                err
                            );
                        }
                    }
                    Err(err) => {
                        if !is_connection_closed(&err) {
                            user_warn!(
                                "Lost the connection to the remote service unexpectedly. We'll close active streams. Details: {}",
                                err
                            );
                        }
                        break;
                    }
                }
            }
            Some(event) = local_event_rx.recv() => {
                match event {
                    LocalEvent::Closed(connection_id) => {
                        user_info!("Local application closed connection #{}.", connection_id);
                        close_connection(&mut connections, connection_id).await;
                    }
                }
            }
        }
    }

    drop(frame_tx);
    if let Err(err) = writer_task.await {
        if err.is_panic() {
            user_error!(
                "The remote writer task crashed. Active connections will close. Details: {}",
                err
            );
        } else if !err.is_cancelled() {
            user_warn!(
                "The remote writer task ended with an error. We're closing the tunnel. Details: {}",
                err
            );
        }
    }

    for entry in connections.into_values() {
        close_connection_entry(entry).await;
    }

    if let Some(code) = exit_request {
        return Err(RemoteExit { code }.into());
    }

    Ok(())
}

async fn read_remote_frame(reader: &mut OwnedReadHalf) -> Result<RemoteFrame, std::io::Error> {
    let mut header = [0u8; 3];
    reader.read_exact(&mut header).await?;
    let message_type = header[0];

    match message_type {
        FRAME_OPEN_IPV4 => {
            let connection_id = u16::from_be_bytes([header[1], header[2]]);
            let mut payload = [0u8; 6];
            reader.read_exact(&mut payload).await?;
            let address = Ipv4Addr::new(payload[0], payload[1], payload[2], payload[3]);
            let port = u16::from_be_bytes([payload[4], payload[5]]);
            Ok(RemoteFrame::OpenIpv4 {
                connection_id,
                address,
                port,
            })
        }
        FRAME_OPEN_IPV6 => {
            let connection_id = u16::from_be_bytes([header[1], header[2]]);
            let mut payload = [0u8; 18];
            reader.read_exact(&mut payload).await?;
            let mut ip_bytes = [0u8; 16];
            ip_bytes.copy_from_slice(&payload[..16]);
            let address = Ipv6Addr::from(ip_bytes);
            let port = u16::from_be_bytes([payload[16], payload[17]]);
            Ok(RemoteFrame::OpenIpv6 {
                connection_id,
                address,
                port,
            })
        }
        FRAME_OPEN_UDP => {
            let connection_id = u16::from_be_bytes([header[1], header[2]]);
            let mut payload = [0u8; 6];
            reader.read_exact(&mut payload).await?;
            let address = Ipv4Addr::new(payload[0], payload[1], payload[2], payload[3]);
            let port = u16::from_be_bytes([payload[4], payload[5]]);
            Ok(RemoteFrame::OpenUdp {
                connection_id,
                address,
                port,
            })
        }
        FRAME_CLOSE => {
            let connection_id = u16::from_be_bytes([header[1], header[2]]);
            Ok(RemoteFrame::Close { connection_id })
        }
        FRAME_DATA => {
            let connection_id = u16::from_be_bytes([header[1], header[2]]);
            let mut len_buf = [0u8; 2];
            reader.read_exact(&mut len_buf).await?;
            let len = u16::from_be_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            reader.read_exact(&mut payload).await?;
            Ok(RemoteFrame::Data {
                connection_id,
                payload,
            })
        }
        FRAME_EXIT => {
            let code = u16::from_be_bytes([header[1], header[2]]);
            Ok(RemoteFrame::Exit { code })
        }
        other => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown frame type {:02X}", other),
        )),
    }
}

async fn handle_remote_frame(
    frame: RemoteFrame,
    connections: &mut HashMap<u16, ConnectionEntry>,
    frame_tx: &mpsc::UnboundedSender<Vec<u8>>,
    local_event_tx: &mpsc::UnboundedSender<LocalEvent>,
) -> Result<(), DynError> {
    match frame {
        RemoteFrame::OpenIpv4 {
            connection_id,
            address,
            port,
        } => {
            let addr = SocketAddr::from((address, port));
            open_local_tcp_connection(connection_id, addr, connections, frame_tx, local_event_tx)
                .await;
        }
        RemoteFrame::OpenIpv6 {
            connection_id,
            address,
            port,
        } => {
            let addr = SocketAddr::from((address, port));
            open_local_tcp_connection(connection_id, addr, connections, frame_tx, local_event_tx)
                .await;
        }
        RemoteFrame::OpenUdp {
            connection_id,
            address,
            port,
        } => {
            let addr = SocketAddr::from((address, port));
            open_local_udp_socket(connection_id, addr, connections, frame_tx, local_event_tx).await;
        }
        RemoteFrame::Close { connection_id } => {
            user_info!("Remote server closed connection #{}.", connection_id);
            close_connection(connections, connection_id).await;
        }
        RemoteFrame::Data {
            connection_id,
            payload,
        } => {
            if let Some(entry) = connections.get_mut(&connection_id) {
                match &mut entry.kind {
                    ConnectionKind::Tcp { writer } => {
                        if writer.write_all(&payload).await.is_err() {
                            let _ = frame_tx.send(close_frame(connection_id));
                            close_connection(connections, connection_id).await;
                        }
                    }
                    ConnectionKind::Udp { socket } => {
                        if let Err(err) = socket.send(&payload).await {
                            user_warn!(
                                "Sending UDP data for connection #{} failed. The remote tunnel will close and retry. Details: {}",
                                connection_id,
                                err
                            );
                            let _ = frame_tx.send(close_frame(connection_id));
                            close_connection(connections, connection_id).await;
                        }
                    }
                }
            } else {
                user_warn!(
                    "Received data for unknown connection #{}. Requesting closure.",
                    connection_id
                );
                let _ = frame_tx.send(close_frame(connection_id));
            }
        }
        RemoteFrame::Exit { .. } => {}
    }
    Ok(())
}

async fn open_local_tcp_connection(
    connection_id: u16,
    addr: SocketAddr,
    connections: &mut HashMap<u16, ConnectionEntry>,
    frame_tx: &mpsc::UnboundedSender<Vec<u8>>,
    local_event_tx: &mpsc::UnboundedSender<LocalEvent>,
) {
    close_connection(connections, connection_id).await;

    match TcpStream::connect(addr).await {
        Ok(stream) => {
            stream.set_nodelay(true).ok();
            let (reader, writer) = stream.into_split();
            user_info!(
                "Opened local connection #{}, destination {}",
                connection_id,
                addr
            );
            let reader_task = tokio::spawn(read_local_stream(
                connection_id,
                reader,
                frame_tx.clone(),
                local_event_tx.clone(),
            ));
            connections.insert(
                connection_id,
                ConnectionEntry {
                    kind: ConnectionKind::Tcp { writer },
                    reader_task,
                },
            );
        }
        Err(err) => {
            user_warn!(
                "We couldn't open local connection #{} to {}. The remote side will be notified so it can retry. Details: {}",
                connection_id,
                addr,
                err
            );
            let _ = frame_tx.send(close_frame(connection_id));
        }
    }
}

async fn open_local_udp_socket(
    connection_id: u16,
    addr: SocketAddr,
    connections: &mut HashMap<u16, ConnectionEntry>,
    frame_tx: &mpsc::UnboundedSender<Vec<u8>>,
    local_event_tx: &mpsc::UnboundedSender<LocalEvent>,
) {
    close_connection(connections, connection_id).await;

    let bind_addr = unspecified_socket_for(&addr);

    match UdpSocket::bind(bind_addr).await {
        Ok(socket) => {
            if let Err(err) = socket.connect(addr).await {
                user_warn!(
                    "We couldn't connect UDP socket #{} to {}. The remote side will be notified so it can retry. Details: {}",
                    connection_id,
                    addr,
                    err
                );
                let _ = frame_tx.send(close_frame(connection_id));
                return;
            }

            let socket = Arc::new(socket);
            user_info!(
                "Opened local UDP socket #{}, destination {}",
                connection_id,
                addr
            );

            let reader_task = tokio::spawn(read_local_udp(
                connection_id,
                socket.clone(),
                frame_tx.clone(),
                local_event_tx.clone(),
            ));

            connections.insert(
                connection_id,
                ConnectionEntry {
                    kind: ConnectionKind::Udp { socket },
                    reader_task,
                },
            );
        }
        Err(err) => {
            user_warn!(
                "We couldn't bind UDP socket #{} locally. The remote side will be notified so it can retry. Details: {}",
                connection_id,
                err
            );
            let _ = frame_tx.send(close_frame(connection_id));
        }
    }
}

async fn close_connection_entry(entry: ConnectionEntry) {
    let ConnectionEntry { kind, reader_task } = entry;
    reader_task.abort();
    if let Err(err) = reader_task.await {
        if err.is_panic() {
            user_error!(
                "The connection handler crashed while processing local traffic. Active connection data may be lost. Details: {}",
                err
            );
        } else if !err.is_cancelled() {
            user_warn!(
                "The connection handler ended with an error while cleaning up. We'll dispose of the connection. Details: {}",
                err
            );
        }
    }

    match kind {
        ConnectionKind::Tcp { mut writer } => {
            if let Err(err) = writer.shutdown().await
                && !is_connection_closed(&err)
            {
                user_warn!(
                    "We couldn't cleanly shut down the local socket. It has been force closed. Details: {}",
                    err
                );
            }
        }
        ConnectionKind::Udp { .. } => {
            // Dropping the socket is sufficient.
        }
    }
}

async fn close_connection(connections: &mut HashMap<u16, ConnectionEntry>, connection_id: u16) {
    if let Some(entry) = connections.remove(&connection_id) {
        close_connection_entry(entry).await;
    }
}

fn unspecified_socket_for(addr: &SocketAddr) -> SocketAddr {
    match addr {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    }
}

async fn read_local_stream(
    connection_id: u16,
    mut reader: OwnedReadHalf,
    frame_tx: mpsc::UnboundedSender<Vec<u8>>,
    local_event_tx: mpsc::UnboundedSender<LocalEvent>,
) {
    let mut buffer = vec![0u8; 65_535];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => {
                let _ = frame_tx.send(close_frame(connection_id));
                break;
            }
            Ok(read) => {
                let payload = &buffer[..read];
                if let Err(err) = frame_tx.send(data_frame(connection_id, payload)) {
                    user_warn!(
                        "We couldn't forward some data for connection #{}. The remote tunnel will close and retry. Details: {}",
                        connection_id,
                        err
                    );
                    break;
                }
            }
            Err(err) => {
                user_warn!(
                    "Reading from connection #{} failed unexpectedly. We'll close it and await a new attempt. Details: {}",
                    connection_id,
                    err
                );
                let _ = frame_tx.send(close_frame(connection_id));
                break;
            }
        }
    }

    let _ = local_event_tx.send(LocalEvent::Closed(connection_id));
}

async fn read_local_udp(
    connection_id: u16,
    socket: Arc<UdpSocket>,
    frame_tx: mpsc::UnboundedSender<Vec<u8>>,
    local_event_tx: mpsc::UnboundedSender<LocalEvent>,
) {
    let mut buffer = vec![0u8; 65_535];
    loop {
        match socket.recv(&mut buffer).await {
            Ok(read) => {
                let payload = &buffer[..read];
                if let Err(err) = frame_tx.send(data_frame(connection_id, payload)) {
                    user_warn!(
                        "We couldn't forward some UDP data for connection #{}. The remote tunnel will close and retry. Details: {}",
                        connection_id,
                        err
                    );
                    break;
                }
            }
            Err(err) => {
                user_warn!(
                    "Reading from UDP socket #{} failed unexpectedly. We'll close it and await a new attempt. Details: {}",
                    connection_id,
                    err
                );
                let _ = frame_tx.send(close_frame(connection_id));
                break;
            }
        }
    }

    let _ = local_event_tx.send(LocalEvent::Closed(connection_id));
}
