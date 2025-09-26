use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use redis::AsyncCommands;
use shared::{
    FRAME_CLOSE, FRAME_DATA, close_frame, data_frame, is_connection_closed, open_ipv4_frame,
    open_ipv6_frame, open_udp_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{
    TcpListener, TcpStream, UdpSocket,
    tcp::{OwnedReadHalf, OwnedWriteHalf},
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::{interval, sleep};
use tracing::{error, info, warn};

use crate::DynError;
use crate::handshake::{send_ko, send_ok};

#[derive(Debug, Clone)]
pub(crate) struct SessionBinding {
    pub(crate) listen: SocketAddr,
    pub(crate) target: SocketAddr,
}

#[derive(Debug)]
struct BindingRuntime {
    listen_addr: SocketAddr,
    current_target: SocketAddr,
    target_tx: watch::Sender<SocketAddr>,
    tcp_handle: JoinHandle<()>,
    udp_handle: JoinHandle<()>,
}

#[derive(Debug)]
struct AcceptedConnection {
    stream: TcpStream,
    peer: SocketAddr,
    listen_addr: SocketAddr,
    target: SocketAddr,
}

pub(crate) async fn run_session(
    mut stream: TcpStream,
    session_id: String,
    mut bindings_rx: watch::Receiver<Vec<SessionBinding>>,
    redis: redis::Client,
    udp_timeout: Duration,
    peer_addr: SocketAddr,
) -> Result<(), DynError> {
    let (frame_tx, frame_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (local_event_tx, mut local_event_rx) = mpsc::unbounded_channel::<LocalEvent>();
    let (accepted_tx, mut accepted_rx) = mpsc::unbounded_channel::<AcceptedConnection>();

    let mut state = SessionState::new(session_id.clone(), udp_timeout);
    let mut runtimes: HashMap<SocketAddr, BindingRuntime> = HashMap::new();

    let initial_bindings = bindings_rx.borrow().clone();
    if let Err(message) = apply_binding_update(
        &session_id,
        &mut runtimes,
        &initial_bindings,
        &accepted_tx,
        &local_event_tx,
        &mut state,
        &frame_tx,
    )
    .await
    {
        warn!(%session_id, error = %message, "failed to prepare initial bindings");
        send_ko(&mut stream, &message).await.ok();
        return Ok(());
    }

    send_ok(&mut stream).await?;
    info!(%session_id, %peer_addr, "session established");

    let (mut reader, writer) = stream.into_split();
    let writer_task = tokio::spawn(async move {
        let mut writer = writer;
        let mut frame_rx = frame_rx;
        while let Some(frame) = frame_rx.recv().await {
            if let Err(err) = writer.write_all(&frame).await {
                warn!(%err, "failed to send frame to proxied");
                break;
            }
        }
        let _ = writer.shutdown().await;
    });

    let (activity_stop_tx, activity_stop_rx) = oneshot::channel();
    let activity_session_id = session_id.clone();
    let activity_redis = redis;
    let activity_handle = tokio::spawn(async move {
        session_activity_task(activity_session_id, activity_redis, activity_stop_rx).await;
    });

    let mut udp_cleanup_interval = interval(Duration::from_secs(1));
    let mut session_error: Option<DynError> = None;

    loop {
        tokio::select! {
            frame = read_remote_frame(&mut reader) => {
                match frame {
                    Ok(remote_frame) => {
                        state.handle_remote_frame(remote_frame, &frame_tx).await;
                    }
                    Err(err) => {
                        if is_connection_closed(&err) {
                            info!(session_id = %session_id, "proxied tunnel closed");
                            break;
                        } else {
                            session_error = Some(Box::new(err));
                            break;
                        }
                    }
                }
            }
            Some(event) = local_event_rx.recv() => {
                state.handle_local_event(event, &frame_tx).await;
            }
            Some(accepted) = accepted_rx.recv() => {
                state.accept_new_client(accepted, &frame_tx, &local_event_tx).await;
            }
            binding_changed = bindings_rx.changed() => {
                match binding_changed {
                    Ok(()) => {
                        let updated = bindings_rx.borrow().clone();
                        if let Err(message) = apply_binding_update(
                            &session_id,
                            &mut runtimes,
                            &updated,
                            &accepted_tx,
                            &local_event_tx,
                            &mut state,
                            &frame_tx,
                        ).await {
                            warn!(%session_id, error = %message, "failed to apply binding update");
                            session_error = Some(message.into());
                            break;
                        }
                    }
                    Err(_) => {
                        warn!(%session_id, "binding updates stream closed");
                        break;
                    }
                }
            }
            _ = udp_cleanup_interval.tick() => {
                state.cleanup_expired_udp_connections(&frame_tx).await;
            }
        }
    }

    drop(frame_tx);
    if let Err(err) = writer_task.await {
        handle_writer_task_error(err);
    }

    for entry in state.into_connections() {
        close_connection_entry(entry).await;
    }

    for runtime in runtimes.into_values() {
        runtime.shutdown().await;
    }

    let _ = activity_stop_tx.send(());
    if let Err(err) = activity_handle.await {
        log_task_error(err);
    }

    if let Some(err) = session_error {
        Err(err)
    } else {
        Ok(())
    }
}

impl BindingRuntime {
    async fn new(
        binding: &SessionBinding,
        session_id: &str,
        accepted_tx: &mpsc::UnboundedSender<AcceptedConnection>,
        local_event_tx: &mpsc::UnboundedSender<LocalEvent>,
    ) -> Result<Self, String> {
        let listen_addr = binding.listen;
        let target = binding.target;

        let listener = TcpListener::bind(listen_addr)
            .await
            .map_err(|err| format!("Could not bind to {listen_addr}: {err}"))?;
        let effective_listen = listener.local_addr().unwrap_or(listen_addr);
        info!(%session_id, listen = %effective_listen, target = %target, "binding ready");

        let udp_socket = UdpSocket::bind(listen_addr)
            .await
            .map_err(|err| format!("Could not bind UDP to {listen_addr}: {err}"))?;
        let effective_udp_listen = udp_socket.local_addr().unwrap_or(listen_addr);
        info!(%session_id, listen = %effective_udp_listen, target = %target, "udp binding ready");

        let (target_tx, _) = watch::channel(target);
        let tcp_target_rx = target_tx.subscribe();
        let udp_target_rx = target_tx.subscribe();

        let tcp_session_id = session_id.to_string();
        let tcp_tx = accepted_tx.clone();
        let tcp_handle = tokio::spawn(async move {
            if let Err(err) =
                accept_loop(listener, listen_addr, tcp_target_rx, tcp_tx, tcp_session_id).await
            {
                warn!(%err, listen = %listen_addr, "listener task ended with error");
            }
        });

        let socket = Arc::new(udp_socket);
        let udp_session_id = session_id.to_string();
        let udp_tx = local_event_tx.clone();
        let udp_handle = tokio::spawn(async move {
            udp_receive_loop(socket, listen_addr, udp_target_rx, udp_tx, udp_session_id).await;
        });

        Ok(Self {
            listen_addr,
            current_target: target,
            target_tx,
            tcp_handle,
            udp_handle,
        })
    }

    fn update_target(&mut self, new_target: SocketAddr) -> Result<(), String> {
        if self.current_target == new_target {
            return Ok(());
        }

        self.current_target = new_target;
        self.target_tx
            .send(new_target)
            .map_err(|_| "binding target watcher closed".to_string())
    }

    async fn shutdown(self) {
        self.tcp_handle.abort();
        if let Err(err) = self.tcp_handle.await {
            log_task_error(err);
        }

        self.udp_handle.abort();
        if let Err(err) = self.udp_handle.await {
            log_task_error(err);
        }
    }
}

async fn apply_binding_update(
    session_id: &str,
    runtimes: &mut HashMap<SocketAddr, BindingRuntime>,
    new_bindings: &[SessionBinding],
    accepted_tx: &mpsc::UnboundedSender<AcceptedConnection>,
    local_event_tx: &mpsc::UnboundedSender<LocalEvent>,
    state: &mut SessionState,
    frame_tx: &mpsc::UnboundedSender<Vec<u8>>,
) -> Result<(), String> {
    let mut desired = HashMap::new();
    for binding in new_bindings {
        desired.insert(binding.listen, binding.target);
    }

    let existing_keys: Vec<SocketAddr> = runtimes.keys().copied().collect();
    for listen in existing_keys {
        if !desired.contains_key(&listen) {
            if let Some(runtime) = runtimes.remove(&listen) {
                info!(%session_id, listen = %listen, "removing binding");
                runtime.shutdown().await;
                state.close_connections_for_listen(listen, frame_tx).await;
            }
        }
    }

    let mut additions = Vec::new();
    for binding in new_bindings {
        if let Some(runtime) = runtimes.get_mut(&binding.listen) {
            if runtime.current_target != binding.target {
                runtime
                    .update_target(binding.target)
                    .map_err(|err| format!("failed to update binding {}: {err}", binding.listen))?;
                info!(%session_id, listen = %binding.listen, target = %binding.target, "binding target updated");
                state
                    .close_connections_for_listen(binding.listen, frame_tx)
                    .await;
            }
        } else {
            additions.push(binding.clone());
        }
    }

    if additions.is_empty() {
        return Ok(());
    }

    let mut created = Vec::with_capacity(additions.len());
    for binding in additions {
        match BindingRuntime::new(&binding, session_id, accepted_tx, local_event_tx).await {
            Ok(runtime) => created.push(runtime),
            Err(err) => {
                for runtime in created {
                    runtime.shutdown().await;
                }
                return Err(err);
            }
        }
    }

    for runtime in created {
        runtimes.insert(runtime.listen_addr, runtime);
    }

    Ok(())
}

struct SessionState {
    session_id: String,
    udp_timeout: Duration,
    connections: HashMap<u16, ConnectionEntry>,
    udp_connections: HashMap<UdpAssociationKey, u16>,
    udp_expirations: BinaryHeap<Reverse<UdpExpiration>>,
    allocator: ConnectionIdAllocator,
}

impl SessionState {
    fn new(session_id: String, udp_timeout: Duration) -> Self {
        Self {
            session_id,
            udp_timeout,
            connections: HashMap::new(),
            udp_connections: HashMap::new(),
            udp_expirations: BinaryHeap::new(),
            allocator: ConnectionIdAllocator::new(),
        }
    }

    fn into_connections(self) -> impl Iterator<Item = ConnectionEntry> {
        self.connections.into_values()
    }

    async fn accept_new_client(
        &mut self,
        accepted: AcceptedConnection,
        frame_tx: &mpsc::UnboundedSender<Vec<u8>>,
        local_event_tx: &mpsc::UnboundedSender<LocalEvent>,
    ) {
        let Some(connection_id) = self.allocator.allocate() else {
            warn!(
                session_id = %self.session_id,
                peer = %accepted.peer,
                "no available connection IDs; rejecting client"
            );
            return;
        };

        let AcceptedConnection {
            stream,
            peer,
            listen_addr,
            target,
        } = accepted;

        info!(
            connection_id = connection_id,
            %peer,
            %listen_addr,
            %target,
            "accepted client connection"
        );

        let (reader, writer) = stream.into_split();
        let reader_task = tokio::spawn(read_client_stream(
            connection_id,
            reader,
            frame_tx.clone(),
            local_event_tx.clone(),
        ));
        self.connections.insert(
            connection_id,
            ConnectionEntry::Tcp {
                writer,
                reader_task,
                listen_addr,
            },
        );

        let frame = match target.ip() {
            IpAddr::V4(addr) => open_ipv4_frame(connection_id, addr, target.port()),
            IpAddr::V6(addr) => open_ipv6_frame(connection_id, addr, target.port()),
        };

        if frame_tx.send(frame).is_err() {
            warn!(
                connection_id = connection_id,
                "failed to notify proxied about new connection"
            );
            if let Some(entry) = self.release_connection(connection_id) {
                close_connection_entry(entry).await;
            }
        }
    }

    async fn handle_local_event(
        &mut self,
        event: LocalEvent,
        frame_tx: &mpsc::UnboundedSender<Vec<u8>>,
    ) {
        match event {
            LocalEvent::Closed(connection_id) => {
                if let Some(entry) = self.release_connection(connection_id) {
                    info!(connection_id = connection_id, "client connection finished");
                    close_connection_entry(entry).await;
                }
            }
            LocalEvent::UdpDatagram {
                listen_addr,
                target,
                peer,
                payload,
                socket,
            } => {
                self.handle_udp_datagram(listen_addr, target, peer, payload, socket, frame_tx)
                    .await;
            }
        }
    }

    async fn handle_remote_frame(
        &mut self,
        frame: RemoteFrame,
        frame_tx: &mpsc::UnboundedSender<Vec<u8>>,
    ) {
        match frame {
            RemoteFrame::Close { connection_id } => {
                info!(
                    connection_id = connection_id,
                    "proxied requested remote close"
                );
                if let Some(entry) = self.release_connection(connection_id) {
                    close_connection_entry(entry).await;
                }
            }
            RemoteFrame::Data {
                connection_id,
                payload,
            } => {
                if let Some(entry) = self.connections.get_mut(&connection_id) {
                    match entry {
                        ConnectionEntry::Tcp { writer, .. } => {
                            if let Err(err) = writer.write_all(&payload).await {
                                warn!(connection_id = connection_id, %err, "failed to write to client connection");
                                let _ = frame_tx.send(close_frame(connection_id));
                                if let Some(entry) = self.release_connection(connection_id) {
                                    close_connection_entry(entry).await;
                                }
                            }
                        }
                        ConnectionEntry::Udp {
                            socket,
                            peer,
                            deadline,
                            ..
                        } => match socket.send_to(&payload, *peer).await {
                            Ok(_) => {
                                let now = Instant::now();
                                let new_deadline = compute_udp_deadline(now, self.udp_timeout);
                                *deadline = new_deadline;
                                self.register_udp_deadline(connection_id, new_deadline);
                            }
                            Err(err) => {
                                warn!(connection_id = connection_id, %err, "failed to write UDP datagram to client");
                                let _ = frame_tx.send(close_frame(connection_id));
                                if let Some(entry) = self.release_connection(connection_id) {
                                    close_connection_entry(entry).await;
                                }
                            }
                        },
                    }
                } else {
                    let _ = frame_tx.send(close_frame(connection_id));
                }
            }
        }
    }

    async fn handle_udp_datagram(
        &mut self,
        listen_addr: SocketAddr,
        target: SocketAddr,
        peer: SocketAddr,
        payload: Vec<u8>,
        socket: Arc<UdpSocket>,
        frame_tx: &mpsc::UnboundedSender<Vec<u8>>,
    ) {
        let key = UdpAssociationKey {
            listen_addr,
            peer_addr: peer,
        };

        let now = Instant::now();

        let connection_id = if let Some(&connection_id) = self.udp_connections.get(&key) {
            if let Some(ConnectionEntry::Udp { deadline, .. }) =
                self.connections.get_mut(&connection_id)
            {
                let new_deadline = compute_udp_deadline(now, self.udp_timeout);
                *deadline = new_deadline;
                self.register_udp_deadline(connection_id, new_deadline);
            }
            connection_id
        } else {
            let IpAddr::V4(target_ip) = target.ip() else {
                warn!(
                    session_id = %self.session_id,
                    %listen_addr,
                    %target,
                    "dropping UDP datagram for unsupported target IP"
                );
                return;
            };

            let Some(connection_id) = self.allocator.allocate() else {
                warn!(
                    session_id = %self.session_id,
                    %peer,
                    "no available connection IDs; dropping UDP datagram"
                );
                return;
            };

            info!(
                connection_id = connection_id,
                %peer,
                %listen_addr,
                %target,
                "accepted UDP association"
            );

            let deadline = compute_udp_deadline(now, self.udp_timeout);
            self.udp_connections.insert(key, connection_id);
            self.connections.insert(
                connection_id,
                ConnectionEntry::Udp {
                    socket: socket.clone(),
                    listen_addr,
                    peer,
                    deadline,
                },
            );

            let frame = open_udp_frame(connection_id, target_ip, target.port());
            if frame_tx.send(frame).is_err() {
                warn!(
                    connection_id = connection_id,
                    "failed to notify proxied about UDP association"
                );
                if let Some(entry) = self.release_connection(connection_id) {
                    close_connection_entry(entry).await;
                }
                return;
            }

            self.register_udp_deadline(connection_id, deadline);
            connection_id
        };

        if frame_tx.send(data_frame(connection_id, &payload)).is_err() {
            warn!(
                connection_id = connection_id,
                "failed to forward UDP payload to proxied"
            );
            if let Some(entry) = self.release_connection(connection_id) {
                close_connection_entry(entry).await;
            }
            return;
        }

        if let Some(ConnectionEntry::Udp { deadline, .. }) =
            self.connections.get_mut(&connection_id)
        {
            let new_deadline = compute_udp_deadline(now, self.udp_timeout);
            *deadline = new_deadline;
            self.register_udp_deadline(connection_id, new_deadline);
        }
    }

    async fn cleanup_expired_udp_connections(&mut self, frame_tx: &mpsc::UnboundedSender<Vec<u8>>) {
        if self.udp_timeout.is_zero() {
            return;
        }

        let now = Instant::now();
        while let Some(Reverse(candidate)) = self.udp_expirations.peek() {
            if candidate.deadline > now {
                break;
            }

            let Reverse(expiration) = self.udp_expirations.pop().unwrap();
            let should_close = matches!(
                self.connections.get(&expiration.connection_id),
                Some(ConnectionEntry::Udp { deadline, .. }) if *deadline <= now
            );

            if !should_close {
                continue;
            }

            info!(
                session_id = %self.session_id,
                connection_id = expiration.connection_id,
                "udp association expired"
            );

            if let Some(entry) = self.release_connection(expiration.connection_id) {
                if frame_tx
                    .send(close_frame(expiration.connection_id))
                    .is_err()
                {
                    warn!(
                        connection_id = expiration.connection_id,
                        "failed to notify proxied about UDP timeout"
                    );
                }
                close_connection_entry(entry).await;
            }
        }
    }

    fn release_connection(&mut self, connection_id: u16) -> Option<ConnectionEntry> {
        let entry = self.connections.remove(&connection_id);
        if let Some(entry_ref) = entry.as_ref()
            && let ConnectionEntry::Udp {
                listen_addr, peer, ..
            } = entry_ref
        {
            self.udp_connections.remove(&UdpAssociationKey {
                listen_addr: *listen_addr,
                peer_addr: *peer,
            });
        }
        if entry.is_some() {
            self.allocator.release(connection_id);
        }
        entry
    }

    fn register_udp_deadline(&mut self, connection_id: u16, deadline: Instant) {
        if self.udp_timeout.is_zero() {
            return;
        }

        self.udp_expirations.push(Reverse(UdpExpiration {
            deadline,
            connection_id,
        }));
    }

    async fn close_connections_for_listen(
        &mut self,
        listen_addr: SocketAddr,
        frame_tx: &mpsc::UnboundedSender<Vec<u8>>,
    ) {
        let mut to_close = Vec::new();
        for (&connection_id, entry) in &self.connections {
            match entry {
                ConnectionEntry::Tcp {
                    listen_addr: entry_listen,
                    ..
                } if *entry_listen == listen_addr => {
                    to_close.push(connection_id);
                }
                ConnectionEntry::Udp {
                    listen_addr: entry_listen,
                    ..
                } if *entry_listen == listen_addr => {
                    to_close.push(connection_id);
                }
                _ => {}
            }
        }

        for connection_id in to_close {
            if frame_tx.send(close_frame(connection_id)).is_err() {
                warn!(
                    session_id = %self.session_id,
                    connection_id = connection_id,
                    "failed to notify proxied about closing stale connection"
                );
            }

            if let Some(entry) = self.release_connection(connection_id) {
                close_connection_entry(entry).await;
            }
        }
    }
}

#[derive(Debug)]
enum RemoteFrame {
    Close {
        connection_id: u16,
    },
    Data {
        connection_id: u16,
        payload: Vec<u8>,
    },
}

#[derive(Debug)]
enum LocalEvent {
    Closed(u16),
    UdpDatagram {
        listen_addr: SocketAddr,
        target: SocketAddr,
        peer: SocketAddr,
        payload: Vec<u8>,
        socket: Arc<UdpSocket>,
    },
}

enum ConnectionEntry {
    Tcp {
        writer: OwnedWriteHalf,
        reader_task: JoinHandle<()>,
        listen_addr: SocketAddr,
    },
    Udp {
        socket: Arc<UdpSocket>,
        listen_addr: SocketAddr,
        peer: SocketAddr,
        deadline: Instant,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct UdpAssociationKey {
    listen_addr: SocketAddr,
    peer_addr: SocketAddr,
}

#[derive(Debug, Clone, Copy)]
struct UdpExpiration {
    deadline: Instant,
    connection_id: u16,
}

impl PartialEq for UdpExpiration {
    fn eq(&self, other: &Self) -> bool {
        self.connection_id == other.connection_id && self.deadline == other.deadline
    }
}

impl Eq for UdpExpiration {}

impl PartialOrd for UdpExpiration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UdpExpiration {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.connection_id.cmp(&other.connection_id))
    }
}

struct ConnectionIdAllocator {
    bitset: [u64; Self::WORDS],
    occupied: usize,
    next_hint: usize,
}

impl ConnectionIdAllocator {
    const CAPACITY: usize = u16::MAX as usize + 1;
    const WORDS: usize = Self::CAPACITY / 64;

    fn new() -> Self {
        Self {
            bitset: [0; Self::WORDS],
            occupied: 0,
            next_hint: 0,
        }
    }

    fn allocate(&mut self) -> Option<u16> {
        if self.occupied == Self::CAPACITY {
            return None;
        }

        let mut word_index = self.next_hint / 64;
        for _ in 0..Self::WORDS {
            let word = self.bitset[word_index];
            if word != u64::MAX {
                let free_bits = !word;
                let bit_index = free_bits.trailing_zeros() as usize;
                let id = word_index * 64 + bit_index;
                debug_assert!(id < Self::CAPACITY);

                self.bitset[word_index] |= 1u64 << bit_index;
                self.occupied += 1;
                self.next_hint = (id + 1) % Self::CAPACITY;
                return Some(id as u16);
            }
            word_index = (word_index + 1) % Self::WORDS;
        }

        None
    }

    fn release(&mut self, id: u16) {
        let index = id as usize;
        let word_index = index / 64;
        let bit_index = index % 64;
        let mask = 1u64 << bit_index;

        if self.bitset[word_index] & mask != 0 {
            self.bitset[word_index] &= !mask;
            self.occupied -= 1;
            self.next_hint = index;
        } else {
            debug_assert!(false, "double free of connection id: {id}");
        }
    }
}

fn compute_udp_deadline(now: Instant, timeout: Duration) -> Instant {
    if timeout.is_zero() {
        now
    } else {
        now.checked_add(timeout).unwrap_or(now)
    }
}

async fn read_remote_frame(reader: &mut OwnedReadHalf) -> Result<RemoteFrame, std::io::Error> {
    let mut header = [0u8; 3];
    reader.read_exact(&mut header).await?;
    let frame_type = header[0];
    let connection_id = u16::from_be_bytes([header[1], header[2]]);

    match frame_type {
        FRAME_CLOSE => Ok(RemoteFrame::Close { connection_id }),
        FRAME_DATA => {
            let mut length_buf = [0u8; 2];
            reader.read_exact(&mut length_buf).await?;
            let length = u16::from_be_bytes(length_buf) as usize;
            let mut payload = vec![0u8; length];
            reader.read_exact(&mut payload).await?;
            Ok(RemoteFrame::Data {
                connection_id,
                payload,
            })
        }
        other => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Unknown frame type {:02X}", other),
        )),
    }
}

async fn accept_loop(
    listener: TcpListener,
    listen_addr: SocketAddr,
    target_rx: watch::Receiver<SocketAddr>,
    tx: mpsc::UnboundedSender<AcceptedConnection>,
    session_id: String,
) -> Result<(), DynError> {
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let target = *target_rx.borrow();
                let connection = AcceptedConnection {
                    stream,
                    peer,
                    listen_addr,
                    target,
                };
                if tx.send(connection).is_err() {
                    break;
                }
            }
            Err(err) => {
                warn!(%listen_addr, %session_id, %err, "listener accept failed");
                sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Ok(())
}

async fn read_client_stream(
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
                    warn!(connection_id = connection_id, %err, "failed to forward data from client");
                    break;
                }
            }
            Err(err) => {
                warn!(connection_id = connection_id, %err, "error reading client connection");
                let _ = frame_tx.send(close_frame(connection_id));
                break;
            }
        }
    }

    let _ = local_event_tx.send(LocalEvent::Closed(connection_id));
}

async fn udp_receive_loop(
    socket: Arc<UdpSocket>,
    listen_addr: SocketAddr,
    target_rx: watch::Receiver<SocketAddr>,
    local_event_tx: mpsc::UnboundedSender<LocalEvent>,
    session_id: String,
) {
    let mut buffer = vec![0u8; 65_535];
    loop {
        match socket.recv_from(&mut buffer).await {
            Ok((read, peer)) => {
                let target = *target_rx.borrow();
                let payload = buffer[..read].to_vec();
                let event = LocalEvent::UdpDatagram {
                    listen_addr,
                    target,
                    peer,
                    payload,
                    socket: socket.clone(),
                };
                if local_event_tx.send(event).is_err() {
                    break;
                }
            }
            Err(err) => {
                warn!(
                    %session_id,
                    %listen_addr,
                    %err,
                    "udp receive loop terminated with error"
                );
                break;
            }
        }
    }
}

async fn session_activity_task(
    session_id: String,
    redis: redis::Client,
    mut shutdown: oneshot::Receiver<()>,
) {
    let key = format!("l:{}", session_id);
    let mut connection = match redis.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(err) => {
            warn!(%session_id, %err, "failed to open redis connection for activity tracking");
            return;
        }
    };

    let mut interval = tokio::time::interval(Duration::from_secs(60));
    if let Err(err) = write_last_activity(&mut connection, &key).await {
        warn!(%session_id, %err, "failed to record initial last activity");
    }
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            _ = interval.tick() => {
                if let Err(err) = write_last_activity(&mut connection, &key).await {
                    warn!(%session_id, %err, "failed to record periodic last activity");
                }
            }
        }
    }
}

async fn write_last_activity(
    connection: &mut redis::aio::MultiplexedConnection,
    key: &str,
) -> Result<(), redis::RedisError> {
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    connection.set::<_, _, ()>(key, timestamp).await
}

async fn close_connection_entry(entry: ConnectionEntry) {
    match entry {
        ConnectionEntry::Tcp {
            mut writer,
            reader_task,
            ..
        } => {
            reader_task.abort();
            if let Err(err) = reader_task.await {
                log_task_error(err);
            }

            if let Err(err) = writer.shutdown().await
                && !is_connection_closed(&err)
            {
                warn!(%err, "failed to shutdown client connection");
            }
        }
        ConnectionEntry::Udp { .. } => {
            // Dropping the socket is enough for UDP associations.
        }
    }
}

fn log_task_error(err: JoinError) {
    if err.is_panic() {
        error!(%err, "connection task panicked");
    } else if !err.is_cancelled() {
        warn!(%err, "connection task ended with error");
    }
}

fn handle_writer_task_error(err: JoinError) {
    if err.is_panic() {
        error!(%err, "tunnel writer task panicked");
    } else if !err.is_cancelled() {
        warn!(%err, "tunnel writer task ended with error");
    }
}
