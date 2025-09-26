# proxied.eu

Just enough code to get a private beta of https://www.proxied.eu going.

🛜 Share your Minecraft or Factorio instance on the Internet. No hosting, your own computer!
🔒 Your privacy is protected: players cannot detect your IP.
🚅 No complicated configuration required. Just a tiny app configured here.
😁 Capped free trial, then 3€/month or 30€/year.

## `proxied` overview

- On startup, `proxied`:
  - Finds the address of `proxying` in `$API`, defaulting to https://proxied.eu.
  - Finds its control port in `$PORT`, defaulting to 32123.
  - Starts a tiny HTTP server on `http://127.0.0.1:$PORT`, with CORS for `$CONTROLLER` defaulting to `proxied.eu`. `GET /` returns the startup ID when it is set, or 404.
  - Reads `~/.proxied` containing a startup ID in plain text. If it exists, it starts using it. If not, it discovers a new one from `$API/startup` and persists it in `~/.proxied`.

### Reverse proxy lifecycle

- Query `$API/session/$startupID` for a session ID postcard, target IP and port (`ConnectionInfo` postcard).
- Establishes a connection to the target over TCP.

### Connection establishment

- `proxied` to `proxying`: `PRX`, session ID length over 1 byte, session ID.
- `proxying` to `proxied`: either:
  - `OK`, then the session starts.
  - `KO`, an error message, then a disconnect.

## Session protocol

Each side streams self-delimiting frames over the TCP socket. Frames are at least 3 bytes. The first byte is the type.

### `0x34`: new TCPv4 connection

`proxying` tells `proxied` to open a new TCPv4 socket to the target IP and port identified as the following 2 bytes, represented over the next 4+2 bytes.

### `0x36`: new TCPv6 connection

`proxying` tells `proxied` to open a new TCPv6 socket to the target IP and port identified as the following 2 bytes, represented over the next 18+2 bytes.

### `0x55`: new UDP socket

`proxying` tells `proxied` that the connection identified by the following 2 bytes should be associated with a UDP socket exchanging with the target IP and port represented over the next 4+2 bytes.

### `0x5A`: connection closed

Either side tells the other side that the socket identified by the following 2 bytes should be closed.

### `0x44`: data

Either side tells the other side that it has data to send. The data is prefixed by the connectionn ID as the following 2 bytes then its length over 2 bytes.

### `0x58`: exit

`proxying` tells `proxied` to exit. An exit code is sent over the next 2 bytes.

## `proxying` overview

- On startup, establishes a connection pool to Redis on `$REDIS_URL`, defaulting to `redis://127.0.0.1:6379`.
- Awaits connections on `$PORT`, defaulting to 8080.
- Upon connection, after receiving the session ID, it subscribes to `s:$sessionID` and looks it up as a key in Redis (`SessionInfo` postcard).
- If the session doesn't exist, it sends `KOInvalid session` to the client and closes the connection. Otherwise, it sends `OK` to the client and starts the session.
- From the start of the session, every minute, it updates `l:$sessionID` in Redis with the current timestamp.
- Whenever the subscription receives a message, it reloads the session info from Redis.
- It binds and forwards data from clients to the target as required from the latest session info.

## Admin commands
- `admin add $sessionID $proxyingIP:$proxyingPort:$targetIP:$targetPort …`
- `admin remove $sessionID`
- `admin list` (includes last activity)
- `admin show $sessionID`
- `admin offer $proxyingIP:$proxyingPort tcp:$startPort-$endPort udp:$startPort-$endPort …`
- `admin rescind $proxyingIP:$proxyingPort`
- `admin offers`
- `admin publish $os(windows,macos) $url`

`admin offer` expects each range argument to be prefixed with either `tcp:` or `udp:` (for example `tcp:1024-2047` or `udp:9000`). You may repeat protocols to define multiple disjoint ranges per host.

## Control plane

- Exposed over HTTP on `$PORT`, defaulting to 443.
- Keeps the whole mapping in memory.
- Loads it from Redis on startup by scanning `s:*` and `o:*` keys.
- `o:*` keys are shaped like `o:$proxyingIP:$proxyingPort` and hold protocol-specific port range pools that can be assigned to sessions.
- Internally, maintains separate bitsets of TCP and UDP IP:ports in use.

## Redis keyspace

All services share a single Redis instance. Keys and payloads are binary postcard blobs unless noted otherwise.

- `s:<session_id>` — session definitions. The canonical payload is a `StoredSessionInfo` struct encoded with postcard:

  ```rust
  struct StoredSessionInfo {
      bindings: Vec<SessionBindingConfig>,
  }

  struct SessionBindingConfig {
      proxying_ip: IpAddr,
      proxying_port: u16,
      target_ip: IpAddr,
      target_port: u16,
  }
  ```

- `l:<session_id>` — unsigned 64-bit UNIX timestamp (seconds) tracking the last activity of a running session. `proxying` refreshes this every 60 seconds while a tunnel is up. Keys are not automatically removed if the corresponding session is deleted.

- `o:<proxying_ip>:<proxying_port>` — offer definitions for a proxy host, stored as a postcard-encoded `OfferPortRanges` value with protocol-specific, inclusive port ranges:

  ```rust
  struct OfferPortRanges {
      tcp: Vec<PortRange>,
      udp: Vec<PortRange>,
  }

  struct PortRange {
      start: u16,
      end: u16,
  }
  ```

  IPv6 proxy addresses use square brackets in the key (for example `o:[2001:db8::1]:32123`). These entries are maintained via `admin offer`/`admin rescind`, and the control plane loads them at startup to determine available capacity. Only the `OfferPortRanges` encoding is accepted; at least one of `tcp` or `udp` must contain ranges.

- `d:<identifier>` — DNS view of a domain, in the form:

  ```rust
  struct Domain {
      user_id: UUID,
      services: BTreeMap<String, Service>, // eg _minecraft._tcp, _factorio._udp
  }

  struct Service {
      name: String,
      target_ip: IpAddr,
      target_port: u16,
  }
  ```
