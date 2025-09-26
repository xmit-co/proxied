use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use shared::{
    DynError, PortRange, ProxyHost, SessionBindingConfig, StoredSessionInfo, decode_offer_ranges,
    decode_session_info, format_proxy_host, offer_key, parse_offer_key, parse_proxy_host_spec,
};
use std::env;
use std::fmt::{self, Write as FmtWrite};
use std::net::IpAddr;
use std::process;
use std::str::FromStr;

struct Cli {
    redis_url: String,
    command: Command,
}

enum Command {
    /// Create or replace a session definition.
    Add {
        /// Session identifier.
        session_id: String,
        /// Binding definitions formatted as proxyingIP:proxyingPort:targetIP:targetPort.
        bindings: Vec<String>,
    },
    /// Remove an existing session.
    Remove {
        /// Session identifier.
        session_id: String,
    },
    /// List all stored sessions.
    List,
    /// Show a single session definition.
    Show {
        /// Session identifier.
        session_id: String,
    },
    /// Create or replace an offer definition.
    Offer {
        /// Host specification formatted as proxyingIP:proxyingPort.
        host_spec: String,
        /// Port ranges formatted as start-end or single ports.
        range_specs: Vec<String>,
    },
    /// Remove an existing offer definition.
    Rescind {
        /// Host specification formatted as proxyingIP:proxyingPort.
        host_spec: String,
    },
    /// List all stored offers.
    Offers,
}

impl Cli {
    fn parse() -> Result<Self, CliParseError> {
        let args: Vec<String> = env::args().skip(1).collect();
        let mut redis_url =
            env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

        let mut idx = 0;
        while idx < args.len() {
            let current = args[idx].as_str();
            if current == "--" {
                idx += 1;
                break;
            }

            if current == "--help" || current == "-h" {
                print_usage();
                process::exit(0);
            }

            if current == "--version" || current == "-V" {
                print_version();
                process::exit(0);
            }

            if current == "--redis-url" {
                idx += 1;
                if idx >= args.len() {
                    return Err(CliParseError::new("Missing value for --redis-url"));
                }
                redis_url = args[idx].clone();
                idx += 1;
                continue;
            }

            if let Some(value) = current.strip_prefix("--redis-url=") {
                if value.is_empty() {
                    return Err(CliParseError::new("Missing value for --redis-url"));
                }
                redis_url = value.to_string();
                idx += 1;
                continue;
            }

            if current.starts_with('-') {
                return Err(CliParseError::new(format!("Unknown option: {}", current)));
            }

            break;
        }

        let positionals = args[idx..].to_vec();
        let Some(command_name) = positionals.first() else {
            return Err(CliParseError::new("Missing command"));
        };

        let command = match command_name.as_str() {
            "add" => {
                if positionals.len() < 3 {
                    return Err(CliParseError::new(
                        "Usage: admin add <session-id> <binding> [binding ...]",
                    ));
                }
                let session_id = positionals[1].clone();
                let bindings = positionals[2..].to_vec();
                Command::Add {
                    session_id,
                    bindings,
                }
            }
            "remove" => {
                if positionals.len() != 2 {
                    return Err(CliParseError::new("Usage: admin remove <session-id>"));
                }
                Command::Remove {
                    session_id: positionals[1].clone(),
                }
            }
            "list" => {
                if positionals.len() != 1 {
                    return Err(CliParseError::new("Usage: admin list"));
                }
                Command::List
            }
            "show" => {
                if positionals.len() != 2 {
                    return Err(CliParseError::new("Usage: admin show <session-id>"));
                }
                Command::Show {
                    session_id: positionals[1].clone(),
                }
            }
            "offer" => {
                if positionals.len() < 3 {
                    return Err(CliParseError::new(
                        "Usage: admin offer <proxy-host> <range> [range ...]",
                    ));
                }
                Command::Offer {
                    host_spec: positionals[1].clone(),
                    range_specs: positionals[2..].to_vec(),
                }
            }
            "rescind" => {
                if positionals.len() != 2 {
                    return Err(CliParseError::new("Usage: admin rescind <proxy-host>"));
                }
                Command::Rescind {
                    host_spec: positionals[1].clone(),
                }
            }
            "offers" => {
                if positionals.len() != 1 {
                    return Err(CliParseError::new("Usage: admin offers"));
                }
                Command::Offers
            }
            _ => {
                return Err(CliParseError::new(format!(
                    "Unknown command: {}",
                    command_name
                )));
            }
        };

        Ok(Self { redis_url, command })
    }
}

#[derive(Debug)]
struct CliParseError {
    message: String,
}

impl CliParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CliParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}\n{}", self.message, usage())
    }
}

impl std::error::Error for CliParseError {}

fn usage() -> &'static str {
    concat!(
        "Usage:\n",
        "  admin [--redis-url <url>] <command> [arguments]\n",
        "\n",
        "Commands:\n",
        "  add <session-id> <binding> [binding ...]\n",
        "  remove <session-id>\n",
        "  list\n",
        "  show <session-id>\n",
        "  offer <proxy-host> <range> [range ...]\n",
        "  rescind <proxy-host>\n",
        "  offers\n",
    )
}

fn print_usage() {
    println!("{}", usage());
}

fn print_version() {
    println!("admin {}", env!("CARGO_PKG_VERSION"));
}

#[derive(Debug, Clone)]
struct SessionListEntry {
    session_id: String,
    info: StoredSessionInfo,
    last_activity: Option<u64>,
}

#[derive(Debug)]
struct BindingParseError {
    input: String,
    message: String,
}

impl BindingParseError {
    fn new(input: &str, message: impl Into<String>) -> Self {
        Self {
            input: input.to_string(),
            message: message.into(),
        }
    }
}

impl fmt::Display for BindingParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.input, self.message)
    }
}

impl std::error::Error for BindingParseError {}

#[derive(Debug)]
struct RangeParseError {
    input: String,
    message: String,
}

impl RangeParseError {
    fn new(input: &str, message: impl Into<String>) -> Self {
        Self {
            input: input.to_string(),
            message: message.into(),
        }
    }
}

impl fmt::Display for RangeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.input, self.message)
    }
}

impl std::error::Error for RangeParseError {}

#[derive(Debug)]
struct CommandError {
    message: String,
}

impl CommandError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for CommandError {}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let cli = Cli::parse().map_err(|err| Box::new(err) as DynError)?;
    let client = redis::Client::open(cli.redis_url.as_str())?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    match cli.command {
        Command::Add {
            session_id,
            bindings,
        } => add_session(&mut connection, session_id, bindings).await?,
        Command::Remove { session_id } => remove_session(&mut connection, session_id).await?,
        Command::List => list_sessions(&mut connection).await?,
        Command::Show { session_id } => show_session(&mut connection, session_id).await?,
        Command::Offer {
            host_spec,
            range_specs,
        } => offer_definition(&mut connection, host_spec, range_specs).await?,
        Command::Rescind { host_spec } => rescind_offer(&mut connection, host_spec).await?,
        Command::Offers => list_offers(&mut connection).await?,
    }

    Ok(())
}

async fn add_session(
    connection: &mut MultiplexedConnection,
    session_id: String,
    binding_specs: Vec<String>,
) -> Result<(), DynError> {
    let mut bindings = Vec::with_capacity(binding_specs.len());
    for spec in binding_specs {
        let binding = parse_binding(&spec).map_err(|err| Box::new(err) as DynError)?;
        bindings.push(binding);
    }

    let info = StoredSessionInfo { bindings };
    let payload = postcard::to_allocvec(&info)?;
    let key = session_key(session_id.as_str());
    connection.set::<_, _, ()>(key, payload).await?;

    println!(
        "Stored session {} with {} binding(s).",
        session_id,
        info.bindings.len()
    );

    Ok(())
}

async fn remove_session(
    connection: &mut MultiplexedConnection,
    session_id: String,
) -> Result<(), DynError> {
    let key = session_key(session_id.as_str());
    let removed: i64 = connection.del(key).await?;
    if removed == 0 {
        println!("Session {} not found.", session_id);
    } else {
        println!("Removed session {}.", session_id);
    }
    Ok(())
}

async fn list_sessions(connection: &mut MultiplexedConnection) -> Result<(), DynError> {
    let keys: Vec<String> = connection.keys("s:*").await?;

    let mut entries: Vec<SessionListEntry> = Vec::new();
    let mut session_ids: Vec<String> = Vec::new();
    for key in keys {
        let session_id = key.strip_prefix("s:").unwrap_or(&key).to_string();
        let payload: Option<Vec<u8>> = connection.get(&key).await?;
        let Some(bytes) = payload else {
            continue;
        };

        match decode_session_info(&bytes) {
            Ok(info) => {
                session_ids.push(session_id.clone());
                entries.push(SessionListEntry {
                    session_id,
                    info,
                    last_activity: None,
                });
            }
            Err(err) => {
                eprintln!("Failed to decode session {}: {}", session_id, err);
            }
        }
    }

    if !session_ids.is_empty() {
        let last_keys: Vec<String> = session_ids.iter().map(|id| format!("l:{}", id)).collect();
        let values = connection
            .mget::<_, Vec<Option<u64>>>(&last_keys)
            .await
            .unwrap_or_else(|_| vec![None; session_ids.len()]);
        for (entry, value) in entries.iter_mut().zip(values.into_iter()) {
            entry.last_activity = value;
        }
    }

    entries.sort_by(|a, b| a.session_id.cmp(&b.session_id));

    print_session_list_yaml(&entries);

    Ok(())
}

async fn show_session(
    connection: &mut MultiplexedConnection,
    session_id: String,
) -> Result<(), DynError> {
    let key = session_key(session_id.as_str());
    let payload: Option<Vec<u8>> = connection.get(&key).await?;
    let Some(bytes) = payload else {
        print_not_found_yaml(&session_id);
        return Ok(());
    };

    let info = decode_session_info(&bytes)?;
    print_single_session_yaml(&session_id, &info);

    Ok(())
}

fn session_key(id: &str) -> String {
    format!("s:{}", id)
}

fn print_session_list_yaml(entries: &[SessionListEntry]) {
    if entries.is_empty() {
        println!("sessions: []");
        return;
    }

    println!("sessions:");
    for entry in entries {
        println!("  - id: {}", yaml_string(&entry.session_id));
        match entry.last_activity {
            Some(value) => println!("    last_activity: {}", value),
            None => println!("    last_activity: null"),
        }
        print_bindings_yaml("    ", &entry.info.bindings);
    }
}

fn print_single_session_yaml(session_id: &str, info: &StoredSessionInfo) {
    println!("session:");
    println!("  id: {}", yaml_string(session_id));
    print_bindings_yaml("  ", &info.bindings);
}

fn print_bindings_yaml(indent: &str, bindings: &[SessionBindingConfig]) {
    if bindings.is_empty() {
        println!("{indent}bindings: []");
        return;
    }

    println!("{indent}bindings:");
    let nested_indent = format!("{indent}  ");
    for binding in bindings {
        println!(
            "{nested_indent}- proxying_ip: {}",
            yaml_string(&binding.proxying_ip.to_string())
        );
        println!(
            "{}  proxying_port: {}",
            nested_indent, binding.proxying_port
        );
        println!(
            "{}  target_ip: {}",
            nested_indent,
            yaml_string(&binding.target_ip.to_string())
        );
        println!("{}  target_port: {}", nested_indent, binding.target_port);
    }
}

fn print_not_found_yaml(session_id: &str) {
    println!("status: {}", yaml_string("not_found"));
    println!("session_id: {}", yaml_string(session_id));
    println!("session: null");
}

fn yaml_string(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() + 2);
    encoded.push('\"');
    for ch in value.chars() {
        match ch {
            '\\' => encoded.push_str("\\\\"),
            '\"' => encoded.push_str("\\\""),
            '\n' => encoded.push_str("\\n"),
            '\r' => encoded.push_str("\\r"),
            '\t' => encoded.push_str("\\t"),
            c if c.is_control() => {
                let code = c as u32;
                if code <= 0xFFFF {
                    let _ = FmtWrite::write_fmt(&mut encoded, format_args!("\\u{:04X}", code));
                } else {
                    let _ = FmtWrite::write_fmt(&mut encoded, format_args!("\\U{:08X}", code));
                }
            }
            c => encoded.push(c),
        }
    }
    encoded.push('\"');
    encoded
}

fn parse_binding(input: &str) -> Result<SessionBindingConfig, BindingParseError> {
    let (proxying_ip_raw, rest) =
        take_ip(input).map_err(|msg| BindingParseError::new(input, msg))?;
    let (proxying_port, rest) =
        take_port(rest).map_err(|msg| BindingParseError::new(input, msg))?;
    let (target_ip_raw, rest) = take_ip(rest).map_err(|msg| BindingParseError::new(input, msg))?;
    let target_port = parse_final_port(rest).map_err(|msg| BindingParseError::new(input, msg))?;

    let proxying_ip = IpAddr::from_str(proxying_ip_raw)
        .map_err(|err| BindingParseError::new(input, format!("invalid proxying IP: {err}")))?;
    let target_ip = IpAddr::from_str(target_ip_raw)
        .map_err(|err| BindingParseError::new(input, format!("invalid target IP: {err}")))?;

    Ok(SessionBindingConfig {
        proxying_ip,
        proxying_port,
        target_ip,
        target_port,
    })
}

fn take_ip(input: &str) -> Result<(&str, &str), &'static str> {
    if input.is_empty() {
        return Err("missing IP address");
    }

    if let Some(rest) = input.strip_prefix('[') {
        let end = rest.find(']').ok_or("unterminated IPv6 address")?;
        let ip = &rest[..end];
        let remainder = rest.get(end + 1..).unwrap_or("");
        let remainder = remainder
            .strip_prefix(':')
            .ok_or("expected ':' after IPv6 address")?;
        Ok((ip, remainder))
    } else {
        let idx = input.find(':').ok_or("expected ':' after IP address")?;
        let ip = &input[..idx];
        let remainder = &input[idx + 1..];
        if ip.is_empty() {
            return Err("missing IP address");
        }
        Ok((ip, remainder))
    }
}

fn take_port(input: &str) -> Result<(u16, &str), &'static str> {
    let idx = input.find(':').ok_or("expected ':' after proxying port")?;
    let port_str = &input[..idx];
    if port_str.is_empty() {
        return Err("missing proxying port");
    }
    let port = port_str
        .parse::<u16>()
        .map_err(|_| "invalid proxying port")?;
    Ok((port, &input[idx + 1..]))
}

fn parse_final_port(input: &str) -> Result<u16, &'static str> {
    if input.is_empty() {
        return Err("missing target port");
    }
    if input.contains(':') {
        return Err("unexpected ':' after target port");
    }
    input.parse::<u16>().map_err(|_| "invalid target port")
}

async fn offer_definition(
    connection: &mut MultiplexedConnection,
    host_spec: String,
    range_specs: Vec<String>,
) -> Result<(), DynError> {
    if range_specs.is_empty() {
        return Err(Box::new(CommandError::new(
            "At least one port range is required",
        )));
    }

    let host = parse_proxy_host_spec(&host_spec).map_err(|err| {
        Box::new(CommandError::new(format!("{}: {}", host_spec, err))) as DynError
    })?;

    let mut ranges = Vec::with_capacity(range_specs.len());
    for spec in range_specs {
        let range = parse_port_range(&spec).map_err(|err| Box::new(err) as DynError)?;
        ranges.push(range);
    }

    ranges.sort_by(|a, b| a.start.cmp(&b.start).then_with(|| a.end.cmp(&b.end)));

    let payload = postcard::to_allocvec(&ranges)?;
    let key = offer_key(&host);
    connection.set::<_, _, ()>(key, payload).await?;

    println!(
        "Stored offer for {} with {} range(s).",
        format_proxy_host(&host),
        ranges.len()
    );

    Ok(())
}

async fn rescind_offer(
    connection: &mut MultiplexedConnection,
    host_spec: String,
) -> Result<(), DynError> {
    let host = parse_proxy_host_spec(&host_spec).map_err(|err| {
        Box::new(CommandError::new(format!("{}: {}", host_spec, err))) as DynError
    })?;
    let key = offer_key(&host);

    let removed: i64 = connection.del(key).await?;
    if removed == 0 {
        println!("Offer {} not found.", format_proxy_host(&host));
    } else {
        println!("Removed offer {}.", format_proxy_host(&host));
    }

    Ok(())
}

async fn list_offers(connection: &mut MultiplexedConnection) -> Result<(), DynError> {
    let keys: Vec<String> = connection.keys("o:*").await?;
    let mut entries = Vec::new();

    for key in keys {
        let payload: Option<Vec<u8>> = connection.get(&key).await?;
        let Some(bytes) = payload else { continue };

        let host = match parse_offer_key(&key) {
            Ok(host) => host,
            Err(err) => {
                eprintln!("Failed to parse offer key {}: {}", key, err);
                continue;
            }
        };

        match decode_offer_ranges(&bytes) {
            Ok(ranges) => entries.push((host, ranges)),
            Err(err) => {
                eprintln!("Failed to decode offer {}: {}", key, err);
            }
        }
    }

    entries.sort_by(|a, b| {
        let left = (a.0.ip.to_string(), a.0.control_port);
        let right = (b.0.ip.to_string(), b.0.control_port);
        left.cmp(&right)
    });

    print_offers_yaml(&entries);

    Ok(())
}

fn parse_port_range(input: &str) -> Result<PortRange, RangeParseError> {
    if input.is_empty() {
        return Err(RangeParseError::new(input, "missing port range"));
    }

    if let Some(idx) = input.find('-') {
        let start_str = &input[..idx];
        let end_str = &input[idx + 1..];

        if start_str.is_empty() {
            return Err(RangeParseError::new(input, "missing start port"));
        }
        if end_str.is_empty() {
            return Err(RangeParseError::new(input, "missing end port"));
        }

        let start = start_str
            .parse::<u16>()
            .map_err(|_| RangeParseError::new(input, "invalid start port"))?;
        let end = end_str
            .parse::<u16>()
            .map_err(|_| RangeParseError::new(input, "invalid end port"))?;

        if start > end {
            return Err(RangeParseError::new(
                input,
                "start port must be <= end port",
            ));
        }

        Ok(PortRange { start, end })
    } else {
        let port = input
            .parse::<u16>()
            .map_err(|_| RangeParseError::new(input, "invalid port"))?;
        Ok(PortRange {
            start: port,
            end: port,
        })
    }
}

fn print_offers_yaml(entries: &[(ProxyHost, Vec<PortRange>)]) {
    if entries.is_empty() {
        println!("offers: []");
        return;
    }

    println!("offers:");
    for (host, ranges) in entries {
        println!("  - proxying_ip: {}", yaml_string(&host.ip.to_string()));
        println!("    proxying_port: {}", host.control_port);
        if ranges.is_empty() {
            println!("    ranges: []");
            continue;
        }
        println!("    ranges:");
        for range in ranges {
            println!("      - start: {}", range.start);
            println!("        end: {}", range.end);
        }
    }
}
