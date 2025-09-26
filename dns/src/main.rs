use std::collections::{BTreeMap, HashSet};
use std::env;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use redis::AsyncCommands;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tracing::{debug, info, warn};
use uuid::Uuid;

use hickory_proto::op::{Message, MessageType, ResponseCode};
use hickory_proto::rr::domain::Name;
use hickory_proto::rr::rdata::SRV;
use hickory_proto::rr::{RData, Record, RecordType};
use shared::DynError;

const DEFAULT_TTL: u32 = 30;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    init_tracing();

    let config = Config::from_env()?;
    let redis = redis::Client::open(config.redis_url.as_str())?;

    let state = Arc::new(DnsState::new(
        config.domain_ascii.clone(),
        config.root_ipv4.clone(),
        config.root_ipv6.clone(),
        redis,
    ));

    info!(domain = %state.zone_without_dot, bind = %config.bind_addr, "dns server starting");

    tokio::try_join!(
        run_udp(state.clone(), config.bind_addr),
        run_tcp(state, config.bind_addr)
    )?;

    Ok(())
}

fn init_tracing() {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);
}

#[derive(Debug)]
struct Config {
    domain_ascii: String,
    redis_url: String,
    bind_addr: SocketAddr,
    root_ipv4: Vec<Ipv4Addr>,
    root_ipv6: Vec<Ipv6Addr>,
}

impl Config {
    fn from_env() -> Result<Self, ConfigError> {
        let domain = env::var("DOMAIN").map_err(|_| ConfigError::missing("DOMAIN"))?;
        let redis_url =
            env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

        let bind_str = env::var("ADDR").unwrap_or_else(|_| "127.0.0.1:5353".to_string());
        let bind_addr = bind_str
            .parse()
            .map_err(|err| ConfigError::invalid("ADDR", err))?;

        let root_addrs_raw = env::var("ROOT_ADDRS").unwrap_or_else(|_| "127.0.0.1".to_string());
        let mut root_ipv4 = Vec::new();
        let mut root_ipv6 = Vec::new();
        for part in root_addrs_raw.split(',') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }

            let addr: IpAddr = trimmed
                .parse()
                .map_err(|err| ConfigError::invalid("ROOT_ADDRS", err))?;
            match addr {
                IpAddr::V4(v4) => {
                    if !root_ipv4.contains(&v4) {
                        root_ipv4.push(v4);
                    }
                }
                IpAddr::V6(v6) => {
                    if !root_ipv6.contains(&v6) {
                        root_ipv6.push(v6);
                    }
                }
            }
        }

        if root_ipv4.is_empty() && root_ipv6.is_empty() {
            return Err(ConfigError::invalid(
                "ROOT_ADDRS",
                "must contain at least one valid IPv4 or IPv6 address",
            ));
        }

        let domain_ascii = normalize_domain(&domain)?;
        Name::from_ascii(&domain_ascii).map_err(|err| ConfigError::invalid("DOMAIN", err))?;

        Ok(Self {
            domain_ascii,
            redis_url,
            bind_addr,
            root_ipv4,
            root_ipv6,
        })
    }
}

fn normalize_domain(raw: &str) -> Result<String, ConfigError> {
    let trimmed = raw.trim().trim_end_matches('.');
    if trimmed.is_empty() {
        return Err(ConfigError::invalid("DOMAIN", "cannot be empty"));
    }

    let lower = trimmed.to_ascii_lowercase();
    Ok(format!("{lower}."))
}

#[derive(Debug)]
struct ConfigError {
    message: String,
}

impl ConfigError {
    fn missing(var: &str) -> Self {
        Self {
            message: format!("missing environment variable {var}"),
        }
    }

    fn invalid<T: std::fmt::Display>(var: &str, err: T) -> Self {
        Self {
            message: format!("invalid value for {var}: {err}"),
        }
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ConfigError {}

#[derive(Clone)]
struct DnsState {
    zone_ascii: String,
    zone_without_dot: String,
    root_ipv4: Vec<Ipv4Addr>,
    root_ipv6: Vec<Ipv6Addr>,
    redis: redis::Client,
}

impl DnsState {
    fn new(
        zone_ascii: String,
        root_ipv4: Vec<Ipv4Addr>,
        root_ipv6: Vec<Ipv6Addr>,
        redis: redis::Client,
    ) -> Self {
        let zone_without_dot = zone_ascii.trim_end_matches('.').to_string();
        Self {
            zone_ascii,
            zone_without_dot,
            root_ipv4,
            root_ipv6,
            redis,
        }
    }

    fn relative_labels(&self, name: &Name) -> Option<Vec<String>> {
        let ascii = name.to_ascii();
        let lowered = ascii.to_ascii_lowercase();

        if lowered == self.zone_ascii {
            return Some(Vec::new());
        }

        if !lowered.ends_with(&self.zone_ascii) {
            return None;
        }

        let mut prefix = &lowered[..lowered.len() - self.zone_ascii.len()];
        if prefix.ends_with('.') {
            prefix = &prefix[..prefix.len() - 1];
        }

        if prefix.is_empty() {
            return Some(Vec::new());
        }

        let labels = prefix
            .split('.')
            .filter(|label| !label.is_empty())
            .map(|label| label.to_string())
            .collect();
        Some(labels)
    }

    async fn lookup_domain(&self, ident: &str) -> Result<Option<DomainEntry>, LookupError> {
        let mut connection = self
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(LookupError::Redis)?;

        let key = format!("d:{ident}");
        let payload: Option<Vec<u8>> = connection.get(&key).await.map_err(LookupError::Redis)?;

        let Some(bytes) = payload else {
            return Ok(None);
        };

        let stored = postcard::from_bytes::<StoredDomain>(&bytes).map_err(LookupError::Decode)?;
        Ok(Some(DomainEntry::from(stored)))
    }

    fn host_name(&self, label: &str) -> Result<Name, LookupError> {
        if label.is_empty() {
            return Err(LookupError::InvalidHost);
        }

        let fqdn = if self.zone_without_dot.is_empty() {
            format!("{label}.")
        } else {
            format!("{label}.{}.", self.zone_without_dot)
        };

        Name::from_ascii(&fqdn).map_err(LookupError::Name)
    }
}

#[derive(Debug, Deserialize)]
struct StoredDomain {
    user_id: Uuid,
    services: BTreeMap<String, StoredService>,
}

#[derive(Debug, Deserialize)]
struct StoredService {
    #[serde(default)]
    name: Option<String>,
    target_ip: IpAddr,
    target_port: u16,
}

#[derive(Debug)]
struct DomainEntry {
    _user_id: Uuid,
    services: Vec<ServiceEntry>,
}

#[derive(Debug)]
struct ServiceEntry {
    name: String,
    target_ip: IpAddr,
    target_port: u16,
}

impl From<StoredDomain> for DomainEntry {
    fn from(value: StoredDomain) -> Self {
        let services = value
            .services
            .into_iter()
            .map(|(key, service)| ServiceEntry::from_parts(key, service))
            .collect();

        Self {
            _user_id: value.user_id,
            services,
        }
    }
}

impl ServiceEntry {
    fn from_parts(key: String, value: StoredService) -> Self {
        let StoredService {
            name,
            target_ip,
            target_port,
        } = value;

        let name = name.unwrap_or(key);

        Self {
            name,
            target_ip,
            target_port,
        }
    }
}

impl DomainEntry {
    fn service(&self, name: &str) -> Option<&ServiceEntry> {
        self.services
            .iter()
            .find(|service| service.name.eq_ignore_ascii_case(name))
    }

    fn ipv4_addresses(&self) -> Vec<Ipv4Addr> {
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for service in &self.services {
            if let IpAddr::V4(addr) = service.target_ip
                && seen.insert(addr)
            {
                result.push(addr);
            }
        }
        result
    }

    fn ipv6_addresses(&self) -> Vec<Ipv6Addr> {
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for service in &self.services {
            if let IpAddr::V6(addr) = service.target_ip
                && seen.insert(addr)
            {
                result.push(addr);
            }
        }
        result
    }
}

#[derive(Debug)]
enum LookupError {
    Redis(redis::RedisError),
    Decode(postcard::Error),
    Name(hickory_proto::error::ProtoError),
    InvalidHost,
}

impl std::fmt::Display for LookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LookupError::Redis(err) => write!(f, "redis error: {err}"),
            LookupError::Decode(err) => write!(f, "decode error: {err}"),
            LookupError::Name(err) => write!(f, "invalid host name: {err}"),
            LookupError::InvalidHost => write!(f, "invalid empty host label"),
        }
    }
}

impl std::error::Error for LookupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LookupError::Redis(err) => Some(err),
            LookupError::Decode(err) => Some(err),
            LookupError::Name(err) => Some(err),
            LookupError::InvalidHost => None,
        }
    }
}

async fn run_udp(state: Arc<DnsState>, bind_addr: SocketAddr) -> Result<(), DynError> {
    let socket = UdpSocket::bind(bind_addr).await?;
    info!(protocol = "udp", %bind_addr, "listening");

    let mut buffer = [0u8; 512];
    loop {
        let (len, peer) = socket.recv_from(&mut buffer).await?;
        let request = &buffer[..len];
        match handle_dns_query(state.clone(), request).await {
            Ok(response) => {
                if let Err(err) = socket.send_to(&response, peer).await {
                    warn!(%err, peer = %peer, "failed to send udp response");
                }
            }
            Err(err) => {
                debug!(%err, peer = %peer, "failed to process udp query");
            }
        }
    }
}

async fn run_tcp(state: Arc<DnsState>, bind_addr: SocketAddr) -> Result<(), DynError> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!(protocol = "tcp", %bind_addr, "listening");

    loop {
        let (stream, peer) = listener.accept().await?;
        let state_clone = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_tcp_client(state_clone, stream).await {
                debug!(%err, peer = %peer, "tcp client handling failed");
            }
        });
    }
}

async fn handle_tcp_client(state: Arc<DnsState>, mut stream: TcpStream) -> Result<(), DynError> {
    loop {
        let mut len_buf = [0u8; 2];
        if let Err(err) = stream.read_exact(&mut len_buf).await {
            if err.kind() == std::io::ErrorKind::UnexpectedEof
                || err.kind() == std::io::ErrorKind::ConnectionReset
                || err.kind() == std::io::ErrorKind::ConnectionAborted
            {
                return Ok(());
            }
            return Err(Box::new(err));
        }

        let length = u16::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; length];
        stream.read_exact(&mut payload).await?;

        match handle_dns_query(state.clone(), &payload).await {
            Ok(response) => {
                let response_len = response.len() as u16;
                stream.write_all(&response_len.to_be_bytes()).await?;
                stream.write_all(&response).await?;
            }
            Err(err) => {
                debug!(%err, "failed to process tcp query");
                // Send SERVFAIL response if we could decode the header
                if let Ok(message) = build_servfail_from_raw(&payload) {
                    let data = message.to_vec()?;
                    let len = data.len() as u16;
                    if stream.write_all(&len.to_be_bytes()).await.is_ok() {
                        let _ = stream.write_all(&data).await;
                    }
                }
                return Ok(());
            }
        }
    }
}

async fn handle_dns_query(
    state: Arc<DnsState>,
    request: &[u8],
) -> Result<Vec<u8>, hickory_proto::error::ProtoError> {
    let message = Message::from_vec(request)?;
    let response = build_response(state, message).await;
    response.to_vec()
}

fn build_servfail_from_raw(request: &[u8]) -> Result<Message, hickory_proto::error::ProtoError> {
    let message = Message::from_vec(request)?;
    let mut response = Message::new();
    response.set_id(message.id());
    response.set_message_type(MessageType::Response);
    response.set_op_code(message.op_code());
    response.set_response_code(ResponseCode::ServFail);
    response.set_authoritative(true);
    response.set_recursion_desired(message.recursion_desired());
    response.set_recursion_available(false);
    for query in message.queries().iter().cloned() {
        response.add_query(query);
    }
    Ok(response)
}

async fn build_response(state: Arc<DnsState>, request: Message) -> Message {
    let mut response = Message::new();
    response.set_id(request.id());
    response.set_message_type(MessageType::Response);
    response.set_op_code(request.op_code());
    response.set_authoritative(true);
    response.set_recursion_desired(request.recursion_desired());
    response.set_recursion_available(false);

    let queries: Vec<_> = request.queries().to_vec();
    for query in &queries {
        response.add_query(query.clone());
    }

    if queries.is_empty() {
        response.set_response_code(ResponseCode::FormErr);
        return response;
    }

    let mut response_code = ResponseCode::NoError;

    for query in queries {
        match answer_question(&state, &query).await {
            Ok(QuestionResponse::Answer {
                answers,
                additionals,
            }) => {
                for record in answers {
                    response.add_answer(record);
                }
                for record in additionals {
                    response.add_additional(record);
                }
            }
            Ok(QuestionResponse::NoData) => {
                // Leave response empty but keep NOERROR.
            }
            Ok(QuestionResponse::NameError) => {
                response_code = ResponseCode::NXDomain;
                break;
            }
            Err(QuestionError::ServFail) => {
                response_code = ResponseCode::ServFail;
                break;
            }
        }
    }

    response.set_response_code(response_code);
    response
}

enum QuestionResponse {
    Answer {
        answers: Vec<Record>,
        additionals: Vec<Record>,
    },
    NoData,
    NameError,
}

enum QuestionError {
    ServFail,
}

async fn answer_question(
    state: &DnsState,
    query: &hickory_proto::op::Query,
) -> Result<QuestionResponse, QuestionError> {
    let Some(labels) = state.relative_labels(query.name()) else {
        return Ok(QuestionResponse::NameError);
    };

    match labels.len() {
        0 => answer_zone_root(state, query),
        1 => answer_identifier(state, &labels[0], query).await,
        3 => answer_service(state, &labels, query).await,
        _ => Ok(QuestionResponse::NameError),
    }
}

fn answer_zone_root(
    state: &DnsState,
    query: &hickory_proto::op::Query,
) -> Result<QuestionResponse, QuestionError> {
    let name = query.name().clone();

    match query.query_type() {
        RecordType::A => {
            let answers = build_a_records_from_addrs(&name, &state.root_ipv4);
            if answers.is_empty() {
                Ok(QuestionResponse::NoData)
            } else {
                Ok(QuestionResponse::Answer {
                    answers,
                    additionals: Vec::new(),
                })
            }
        }
        RecordType::AAAA => {
            let answers = build_aaaa_records_from_addrs(&name, &state.root_ipv6);
            if answers.is_empty() {
                Ok(QuestionResponse::NoData)
            } else {
                Ok(QuestionResponse::Answer {
                    answers,
                    additionals: Vec::new(),
                })
            }
        }
        RecordType::ANY => {
            let mut answers = build_a_records_from_addrs(&name, &state.root_ipv4);
            answers.extend(build_aaaa_records_from_addrs(&name, &state.root_ipv6));

            if answers.is_empty() {
                Ok(QuestionResponse::NoData)
            } else {
                Ok(QuestionResponse::Answer {
                    answers,
                    additionals: Vec::new(),
                })
            }
        }
        _ => Ok(QuestionResponse::NoData),
    }
}

async fn answer_identifier(
    state: &DnsState,
    label: &str,
    query: &hickory_proto::op::Query,
) -> Result<QuestionResponse, QuestionError> {
    let domain = match state.lookup_domain(label).await {
        Ok(Some(domain)) => domain,
        Ok(None) => return Ok(QuestionResponse::NameError),
        Err(err) => {
            warn!(host = label, %err, "failed to fetch domain data");
            return Err(QuestionError::ServFail);
        }
    };

    let name = query.name().clone();

    match query.query_type() {
        RecordType::A => {
            let answers = build_a_records(&name, &domain);
            if answers.is_empty() {
                Ok(QuestionResponse::NoData)
            } else {
                Ok(QuestionResponse::Answer {
                    answers,
                    additionals: Vec::new(),
                })
            }
        }
        RecordType::AAAA => {
            let answers = build_aaaa_records(&name, &domain);
            if answers.is_empty() {
                Ok(QuestionResponse::NoData)
            } else {
                Ok(QuestionResponse::Answer {
                    answers,
                    additionals: Vec::new(),
                })
            }
        }
        RecordType::ANY => {
            let mut answers = build_a_records(&name, &domain);
            answers.extend(build_aaaa_records(&name, &domain));

            if answers.is_empty() {
                Ok(QuestionResponse::NoData)
            } else {
                Ok(QuestionResponse::Answer {
                    answers,
                    additionals: Vec::new(),
                })
            }
        }
        _ => Ok(QuestionResponse::NoData),
    }
}

async fn answer_service(
    state: &DnsState,
    labels: &[String],
    query: &hickory_proto::op::Query,
) -> Result<QuestionResponse, QuestionError> {
    if labels.len() != 3 {
        return Ok(QuestionResponse::NameError);
    }

    let service = labels[0].as_str();
    let protocol = labels[1].as_str();
    let ident = labels[2].as_str();

    let service_name = format!("{service}.{protocol}");

    if query.query_type() != RecordType::SRV && query.query_type() != RecordType::ANY {
        return Ok(QuestionResponse::NoData);
    }

    let domain = match state.lookup_domain(ident).await {
        Ok(Some(domain)) => domain,
        Ok(None) => return Ok(QuestionResponse::NameError),
        Err(err) => {
            warn!(host = ident, %err, "failed to fetch domain data");
            return Err(QuestionError::ServFail);
        }
    };

    let service_entry = match domain.service(&service_name) {
        Some(service) => service,
        None => return Ok(QuestionResponse::NameError),
    };

    let host_name = match state.host_name(ident) {
        Ok(name) => name,
        Err(err) => {
            warn!(host = ident, %err, "failed to build host name");
            return Err(QuestionError::ServFail);
        }
    };

    let answers = vec![Record::from_rdata(
        query.name().clone(),
        DEFAULT_TTL,
        RData::SRV(SRV::new(0, 0, service_entry.target_port, host_name.clone())),
    )];

    let mut additionals = build_a_records(&host_name, &domain);
    additionals.extend(build_aaaa_records(&host_name, &domain));

    Ok(QuestionResponse::Answer {
        answers,
        additionals,
    })
}

fn build_a_records(name: &Name, domain: &DomainEntry) -> Vec<Record> {
    domain
        .ipv4_addresses()
        .into_iter()
        .map(|addr| Record::from_rdata(name.clone(), DEFAULT_TTL, RData::A(addr.into())))
        .collect()
}

fn build_aaaa_records(name: &Name, domain: &DomainEntry) -> Vec<Record> {
    domain
        .ipv6_addresses()
        .into_iter()
        .map(|addr| Record::from_rdata(name.clone(), DEFAULT_TTL, RData::AAAA(addr.into())))
        .collect()
}

fn build_a_records_from_addrs(name: &Name, addrs: &[Ipv4Addr]) -> Vec<Record> {
    addrs
        .iter()
        .copied()
        .map(|addr| Record::from_rdata(name.clone(), DEFAULT_TTL, RData::A(addr.into())))
        .collect()
}

fn build_aaaa_records_from_addrs(name: &Name, addrs: &[Ipv6Addr]) -> Vec<Record> {
    addrs
        .iter()
        .copied()
        .map(|addr| Record::from_rdata(name.clone(), DEFAULT_TTL, RData::AAAA(addr.into())))
        .collect()
}
