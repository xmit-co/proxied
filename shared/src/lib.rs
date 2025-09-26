use serde::{Deserialize, Serialize};
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Bytes that prefix proxied handshake payloads.
pub const PROTOCOL_PREFIX: [u8; 3] = *b"PRX";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionBindingConfig {
    pub proxying_ip: IpAddr,
    pub proxying_port: u16,
    pub target_ip: IpAddr,
    pub target_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSessionInfo {
    #[serde(default)]
    pub bindings: Vec<SessionBindingConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub session_id: String,
    pub target_ip: IpAddr,
    pub target_port: u16,
}

/// Download location advertised for the lightweight "tun" helper.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TunManifest {
    /// URL to download the OS-specific build of the proxy helper.
    pub download_url: String,
}

/// Decode the bindings component of a stored session payload.
pub fn decode_session_bindings(bytes: &[u8]) -> Result<Vec<SessionBindingConfig>, postcard::Error> {
    decode_session_info(bytes).map(|info| info.bindings)
}

/// Decode the canonical session payload stored in Redis.
pub fn decode_session_info(bytes: &[u8]) -> Result<StoredSessionInfo, postcard::Error> {
    postcard::from_bytes::<StoredSessionInfo>(bytes)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ProxyHost {
    pub ip: IpAddr,
    pub control_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PortRange {
    pub start: u16,
    pub end: u16,
}

impl PortRange {
    pub fn contains(&self, port: u16) -> bool {
        port >= self.start && port <= self.end
    }

    pub fn len(&self) -> usize {
        (self.end - self.start) as usize + 1
    }

    pub fn is_empty(&self) -> bool {
        self.start > self.end
    }
}

pub fn parse_proxy_host_spec(input: &str) -> Result<ProxyHost, String> {
    if input.is_empty() {
        return Err("missing host".to_string());
    }

    let (ip_str, port_str) = if input.starts_with('[') {
        let closing = input
            .find(']')
            .ok_or_else(|| "unterminated IPv6 address".to_string())?;
        let ip = &input[1..closing];
        let remainder = input
            .get(closing + 1..)
            .ok_or_else(|| "missing IPv6 suffix".to_string())?;
        let port = remainder
            .strip_prefix(':')
            .ok_or_else(|| "expected ':' after IPv6 address".to_string())?;
        (ip, port)
    } else {
        let idx = input
            .rfind(':')
            .ok_or_else(|| "expected ':' before port".to_string())?;
        (&input[..idx], &input[idx + 1..])
    };

    if ip_str.is_empty() {
        return Err("missing proxying IP".to_string());
    }
    if port_str.is_empty() {
        return Err("missing proxying port".to_string());
    }

    let ip = ip_str
        .parse::<IpAddr>()
        .map_err(|err| format!("invalid IP: {err}"))?;
    let port = port_str
        .parse::<u16>()
        .map_err(|err| format!("invalid port: {err}"))?;

    Ok(ProxyHost {
        ip,
        control_port: port,
    })
}

pub fn format_proxy_host(host: &ProxyHost) -> String {
    match host.ip {
        IpAddr::V4(_) => format!("{}:{}", host.ip, host.control_port),
        IpAddr::V6(_) => format!("[{}]:{}", host.ip, host.control_port),
    }
}

pub fn offer_key(host: &ProxyHost) -> String {
    match host.ip {
        IpAddr::V4(_) => format!("o:{}:{}", host.ip, host.control_port),
        IpAddr::V6(_) => format!("o:[{}]:{}", host.ip, host.control_port),
    }
}

pub fn parse_offer_key(key: &str) -> Result<ProxyHost, String> {
    let rest = key
        .strip_prefix("o:")
        .ok_or_else(|| "missing prefix".to_string())?;
    if rest.is_empty() {
        return Err("missing offer body".to_string());
    }

    let (ip_str, port_str) = if rest.starts_with('[') {
        let end = rest
            .find(']')
            .ok_or_else(|| "unterminated IPv6".to_string())?;
        let ip = &rest[1..end];
        let remainder = rest
            .get(end + 1..)
            .ok_or_else(|| "missing IPv6 suffix".to_string())?;
        let port_part = remainder
            .strip_prefix(':')
            .ok_or_else(|| "missing ':' after IPv6".to_string())?;
        (ip, port_part)
    } else {
        let idx = rest
            .rfind(':')
            .ok_or_else(|| "missing ':' before port".to_string())?;
        (&rest[..idx], &rest[idx + 1..])
    };

    let ip = ip_str
        .parse::<IpAddr>()
        .map_err(|err| format!("invalid IP: {err}"))?;
    let port = port_str
        .parse::<u16>()
        .map_err(|err| format!("invalid port: {err}"))?;

    Ok(ProxyHost {
        ip,
        control_port: port,
    })
}

pub fn decode_offer_ranges(bytes: &[u8]) -> Result<Vec<PortRange>, String> {
    let ranges = postcard::from_bytes::<Vec<PortRange>>(bytes)
        .map_err(|err| format!("unable to decode offer: {err}"))?;

    if ranges.is_empty() {
        return Err("no ranges provided".to_string());
    }

    if ranges.iter().any(|range| range.start > range.end) {
        return Err("invalid range ordering".to_string());
    }

    Ok(ranges)
}

pub const FRAME_OPEN_IPV4: u8 = 0x34;
pub const FRAME_OPEN_IPV6: u8 = 0x36;
pub const FRAME_OPEN_UDP: u8 = 0x55;
pub const FRAME_CLOSE: u8 = 0x5A;
pub const FRAME_DATA: u8 = 0x44;
pub const FRAME_EXIT: u8 = 0x58;

pub fn open_ipv4_frame(connection_id: u16, address: Ipv4Addr, port: u16) -> Vec<u8> {
    let mut frame = Vec::with_capacity(9);
    frame.push(FRAME_OPEN_IPV4);
    frame.extend_from_slice(&connection_id.to_be_bytes());
    frame.extend_from_slice(&address.octets());
    frame.extend_from_slice(&port.to_be_bytes());
    frame
}

pub fn open_ipv6_frame(connection_id: u16, address: Ipv6Addr, port: u16) -> Vec<u8> {
    let mut frame = Vec::with_capacity(21);
    frame.push(FRAME_OPEN_IPV6);
    frame.extend_from_slice(&connection_id.to_be_bytes());
    frame.extend_from_slice(&address.octets());
    frame.extend_from_slice(&port.to_be_bytes());
    frame
}

pub fn open_udp_frame(connection_id: u16, address: Ipv4Addr, port: u16) -> Vec<u8> {
    let mut frame = Vec::with_capacity(9);
    frame.push(FRAME_OPEN_UDP);
    frame.extend_from_slice(&connection_id.to_be_bytes());
    frame.extend_from_slice(&address.octets());
    frame.extend_from_slice(&port.to_be_bytes());
    frame
}

pub fn close_frame(connection_id: u16) -> Vec<u8> {
    let mut frame = Vec::with_capacity(3);
    frame.push(FRAME_CLOSE);
    frame.extend_from_slice(&connection_id.to_be_bytes());
    frame
}

pub fn data_frame(connection_id: u16, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(FRAME_DATA);
    frame.extend_from_slice(&connection_id.to_be_bytes());
    frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

pub fn exit_frame(code: u16) -> Vec<u8> {
    let mut frame = Vec::with_capacity(3);
    frame.push(FRAME_EXIT);
    frame.extend_from_slice(&code.to_be_bytes());
    frame
}

pub fn is_connection_closed(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
    )
}

pub type DynError = Box<dyn std::error::Error + Send + Sync>;
