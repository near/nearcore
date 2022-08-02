use anyhow::Context;
use bytes::buf::{Buf, BufMut};
use bytes::BytesMut;
use near_crypto::{KeyType, SecretKey};
use near_network::types::{
    Encoding, Handshake, ParsePeerMessageError, PeerMessage, RoutingTableUpdate,
};
use near_network_primitives::time::Utc;
use near_network_primitives::types::{
    AccountOrPeerIdOrHash, PartialEdgeInfo, PeerChainInfoV2, Ping, RawRoutedMessage,
    RoutedMessageBody,
};
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, BlockHeight};
use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// TODO: also log number of bytes/other messages (like Blocks) received?
#[derive(Debug, Default)]
pub struct PingStats {
    pub pings_sent: usize,
    pub pongs_received: usize,
    // TODO: these latency stats could be separated into time to first byte
    // + time to last byte, etc.
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub average_latency: Duration,
}

impl PingStats {
    fn pong_received(&mut self, latency: Duration) {
        self.pongs_received += 1;

        if self.min_latency == Duration::ZERO || self.min_latency > latency {
            self.min_latency = latency;
        }
        if self.max_latency < latency {
            self.max_latency = latency;
        }
        let n = self.pongs_received as u32;
        self.average_latency = ((n - 1) * self.average_latency + latency) / n;
    }
}

struct Peer {
    stream: TcpStream,
    peer_id: PeerId,
    buf: BytesMut,
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let addr = match self.stream.peer_addr() {
            Ok(a) => format!("{:?}", a),
            Err(e) => format!("Err({:?})", e),
        };
        write!(f, "{:?}@{:?}", &self.peer_id, addr)
    }
}

impl Peer {
    async fn connect(addr: SocketAddr, peer_id: PeerId) -> anyhow::Result<Self> {
        let stream =
            tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr)).await??;
        Ok(Self { stream, peer_id, buf: BytesMut::with_capacity(1024) })
    }

    async fn write_message(&mut self, msg: &PeerMessage) -> io::Result<()> {
        let mut msg = msg.serialize(Encoding::Proto);
        let mut buf = (msg.len() as u32).to_le_bytes().to_vec();
        buf.append(&mut msg);
        self.stream.write_all(&buf).await
    }

    // panics if there's not at least `len` + 4 bytes available
    fn extract_msg(&mut self, len: usize) -> Result<PeerMessage, ParsePeerMessageError> {
        self.buf.advance(4);
        let msg = PeerMessage::deserialize(Encoding::Proto, &self.buf[..len]);
        self.buf.advance(len);
        msg
    }

    async fn do_read(&mut self) -> io::Result<()> {
        let n = tokio::time::timeout(Duration::from_secs(5), self.stream.read_buf(&mut self.buf))
            .await??;
        tracing::trace!(target: "ping", "Read {} bytes from {:?}", n, self.stream.peer_addr());
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "no more bytes available, but expected to receive a message",
            ));
        }
        Ok(())
    }

    // read at least 4 bytes, but probably the whole message in most cases
    async fn read_msg_length(&mut self) -> io::Result<(usize, Instant)> {
        let mut first_byte_time = None;
        while self.buf.remaining() < 4 {
            self.do_read().await?;
            if first_byte_time.is_none() {
                first_byte_time = Some(Instant::now());
            }
        }
        let len = u32::from_le_bytes(self.buf[..4].try_into().unwrap());
        // If first_byte_time is None, there were already 4 bytes from last time,
        // and they must have come before a partial frame.
        // So the Instant::now() is not quite correct, since the time was really in the past,
        // but this is prob not so important
        Ok((len as usize, first_byte_time.unwrap_or(Instant::now())))
    }

    // Append any messages currently buffered in the stream
    // returns whether we read `stop` = true
    fn read_remaining_messages(
        &mut self,
        messages: &mut Vec<PeerMessage>,
        stop: &AtomicBool,
    ) -> anyhow::Result<bool> {
        // drain anything still available to be read without blocking
        loop {
            match self.stream.try_read_buf(&mut self.buf) {
                Ok(n) => {
                    tracing::trace!(target: "ping", "Read {} bytes from {:?} non-blocking", n, self.stream.peer_addr())
                }
                Err(e) => match e.kind() {
                    io::ErrorKind::WouldBlock => {
                        break;
                    }
                    _ => return Err(e.into()),
                },
            };
            if stop.load(Ordering::Relaxed) {
                return Ok(true);
            }
        }
        loop {
            if self.buf.remaining() < 4 {
                if self.buf.has_remaining() {
                    tracing::debug!(
                        target: "ping", "There's a partial frame left over in the buffer with nothing left to read yet. \
                        {} bytes out of the 4-byte length prefix received", self.buf.remaining()
                    );
                }
                break;
            }
            let len = u32::from_le_bytes(self.buf[..4].try_into().unwrap()) as usize;
            if self.buf.remaining() < len + 4 {
                tracing::debug!(
                    target: "ping", "There's a partial frame left over in the buffer. \
                    {} bytes out of the {} byte message received", self.buf.remaining() - 4, len
                );
                break;
            }
            messages.push(
                self.extract_msg(len)
                    .with_context(|| format!("error parsing message of length {}", len))?,
            );
        }
        Ok(false)
    }

    // Reads from the socket until there is at least one full PeerMessage available.
    // After that point, continues to read any bytes available to be read without blocking
    // and appends any extra messages to the returned Vec. Usually there is only one in there

    // The Instant returned is the time we first read any bytes

    // The bool returned is true if we should stop now
    async fn recv_messages(
        &mut self,
        stop: &AtomicBool,
    ) -> anyhow::Result<(Vec<PeerMessage>, Instant, bool)> {
        if stop.load(Ordering::Relaxed) {
            return Ok((Vec::new(), Instant::now(), true));
        }
        let mut messages = Vec::new();
        let (msg_length, first_byte_time) = self.read_msg_length().await?;

        while self.buf.remaining() < msg_length + 4 {
            if stop.load(Ordering::Relaxed) {
                return Ok((messages, first_byte_time, true));
            }
            // TODO: measure time to last byte here
            self.do_read().await?;
        }

        messages.push(
            self.extract_msg(msg_length)
                .with_context(|| format!("error parsing message of length {}", msg_length))?,
        );

        let stopped = self.read_remaining_messages(&mut messages, stop)?;

        // make sure we can probably read the next message in one syscall next time
        let max_len_after_next_read = self.buf.chunk_mut().len() + self.buf.remaining();
        if max_len_after_next_read < 512 {
            self.buf.reserve(512 - max_len_after_next_read);
        }
        Ok((messages, first_byte_time, stopped))
    }

    async fn send_ping(
        &mut self,
        app_info: &mut AppInfo,
        target: &PeerId,
        nonce: u64,
        ttl: u8,
    ) -> anyhow::Result<()> {
        let body = RoutedMessageBody::Ping(Ping { nonce, source: app_info.my_peer_id.clone() });
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target.clone()), body }
            .sign(app_info.my_peer_id.clone(), &app_info.secret_key, ttl, Some(Utc::now_utc()));

        self.write_message(&PeerMessage::Routed(msg)).await?;
        app_info.ping_sent(target, nonce);
        Ok(())
    }

    async fn do_handshake(
        &mut self,
        app_info: &mut AppInfo,
        sigint_received: &AtomicBool,
    ) -> anyhow::Result<bool> {
        let handshake = PeerMessage::Handshake(Handshake::new(
            near_primitives::version::PROTOCOL_VERSION,
            app_info.my_peer_id.clone(),
            self.peer_id.clone(),
            // we have to set this even if we have no intention of listening since otherwise
            // the peer will drop our connection
            Some(24567),
            app_info.chain_info.clone(),
            PartialEdgeInfo::new(&app_info.my_peer_id, &self.peer_id, 1, &app_info.secret_key),
        ));

        self.write_message(&handshake).await?;

        let start = Instant::now();

        let (messages, first_byte_time, stop) = self.recv_messages(&sigint_received).await?;

        if let Some(first) = messages.first() {
            // TODO: maybe check the handshake for sanity
            if !matches!(first, PeerMessage::Handshake(_)) {
                return Err(anyhow::anyhow!(
                    "First message received from {:?} is not a handshake: {:?}",
                    self,
                    &first
                ));
            }
            tracing::info!(target: "ping", "handshake latency: {:?}", first_byte_time - start);
        }
        for msg in messages.iter().skip(1) {
            tracing::warn!(
                target: "ping", "Received unexpected message from {:?} right after receiving the handshake: {:?}",
                self,
                msg,
            );
            if let PeerMessage::SyncRoutingTable(r) = &msg {
                app_info.add_announce_accounts(r);
            }
        }
        Ok(stop)
    }
}

#[derive(Debug)]
struct PendingPing {
    nonce: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct PingTarget {
    peer_id: PeerId,
    last_pinged: Option<Instant>,
}

impl PartialOrd for PingTarget {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PingTarget {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match &self.last_pinged {
            Some(my_last_pinged) => match &other.last_pinged {
                Some(their_last_pinged) => my_last_pinged
                    .cmp(their_last_pinged)
                    .then_with(|| self.peer_id.cmp(&other.peer_id)),
                None => cmp::Ordering::Greater,
            },
            None => match &other.last_pinged {
                Some(_) => cmp::Ordering::Less,
                None => self.peer_id.cmp(&other.peer_id),
            },
        }
    }
}

#[derive(Debug)]
struct PingState {
    stats: PingStats,
    last_pinged: Option<Instant>,
    account_id: Option<AccountId>,
}

struct AppInfo {
    chain_info: PeerChainInfoV2,
    secret_key: SecretKey,
    my_peer_id: PeerId,
    stats: HashMap<PeerId, PingState>,
    // we will ping targets in round robin fashion. So this keeps a set of
    // targets ordered by when we last pinged them.
    requests: BTreeMap<PingTarget, Option<PendingPing>>,
    // how many requests in flight do we have?
    num_pings_in_flight: usize,
}

impl AppInfo {
    fn new(chain_id: &str, genesis_hash: CryptoHash, head_height: BlockHeight) -> Self {
        let secret_key = SecretKey::from_random(KeyType::ED25519);
        let my_peer_id = PeerId::new(secret_key.public_key());

        Self {
            chain_info: PeerChainInfoV2 {
                genesis_id: GenesisId { chain_id: chain_id.to_string(), hash: genesis_hash },
                height: head_height,
                tracked_shards: vec![0],
                archival: false,
            },
            secret_key,
            my_peer_id,
            stats: HashMap::new(),
            requests: BTreeMap::new(),
            num_pings_in_flight: 0,
        }
    }

    // Must not be called if self.num_pings_in_flight >= self.requests.len()
    fn pick_next_target(&self) -> PeerId {
        for (target, pending_request) in self.requests.iter() {
            if pending_request.is_none() {
                return target.peer_id.clone();
            }
        }
        panic!(
            "next_ping_target called with a pending request in flight for every peer. This is a bug."
        );
    }

    fn ping_sent(&mut self, peer_id: &PeerId, nonce: u64) {
        let timestamp = Instant::now();

        match self.stats.entry(peer_id.clone()) {
            Entry::Occupied(mut e) => {
                let state = e.get_mut();
                let pending_ping = self
                    .requests
                    .remove(&PingTarget {
                        peer_id: peer_id.clone(),
                        last_pinged: state.last_pinged,
                    })
                    .unwrap();
                assert!(pending_ping.is_none());

                if let Some(account_id) = state.account_id.as_ref() {
                    println!("send ping --------------> {}", account_id);
                } else {
                    println!("send ping --------------> {}", &peer_id);
                }
                state.stats.pings_sent += 1;
                state.last_pinged = Some(timestamp);
                self.requests.insert(
                    PingTarget { peer_id: peer_id.clone(), last_pinged: Some(timestamp) },
                    Some(PendingPing { nonce }),
                );
                self.num_pings_in_flight += 1;
            }
            Entry::Vacant(_) => {
                panic!("sent ping to {:?}, but not present in stats HashMap", peer_id)
            }
        };
    }

    fn pong_received(&mut self, peer_id: &PeerId, nonce: u64, received_at: Instant) {
        match self.stats.get_mut(peer_id) {
            Some(state) => {
                let pending_ping = self
                    .requests
                    .get_mut(&PingTarget {
                        peer_id: peer_id.clone(),
                        last_pinged: state.last_pinged,
                    })
                    .unwrap();
                match pending_ping {
                    Some(p) => {
                        if p.nonce == nonce {
                            let latency = received_at - state.last_pinged.unwrap();
                            state.stats.pong_received(latency);
                            self.num_pings_in_flight -= 1;
                            *pending_ping = None;
                            if let Some(account_id) = state.account_id.as_ref() {
                                println!(
                                    "recv pong <-------------- {} latency: {:?}",
                                    account_id, latency
                                );
                            } else {
                                println!(
                                    "recv pong <-------------- {} latency: {:?}",
                                    &peer_id, latency
                                );
                            }
                        } else {
                            // should we update the stats still?
                            tracing::warn!(
                                target: "ping", "Received Pong with nonce {} when {} was expected",
                                nonce,
                                p.nonce,
                            );
                        }
                    }
                    None => {
                        tracing::warn!(target: "ping", "received pong from {:?}, but don't remember sending a ping", peer_id)
                    }
                };
            }
            None => {
                tracing::warn!(target: "ping", "received pong from {:?}, but don't know of this peer", peer_id)
            }
        };
    }

    fn add_peer(&mut self, peer_id: &PeerId, account_id: Option<&AccountId>) {
        match self.stats.entry(peer_id.clone()) {
            Entry::Occupied(mut e) => {
                if let Some(account_id) = account_id {
                    let state = e.get_mut();
                    if let Some(old) = state.account_id.as_ref() {
                        if old != account_id {
                            tracing::warn!(
                                target: "ping", "Received Announce Account mapping {:?} to {:?}, but already \
                                knew of account id {:?}. Keeping old value",
                                peer_id, account_id, old
                            );
                        }
                    } else {
                        state.account_id = Some(account_id.clone());
                    }
                }
            }
            Entry::Vacant(e) => {
                e.insert(PingState {
                    account_id: account_id.cloned(),
                    last_pinged: None,
                    stats: PingStats::default(),
                });
                self.requests
                    .insert(PingTarget { peer_id: peer_id.clone(), last_pinged: None }, None);
            }
        }
    }

    fn add_announce_accounts(&mut self, r: &RoutingTableUpdate) {
        for a in r.accounts.iter() {
            self.add_peer(&a.peer_id, Some(&a.account_id));
        }
    }
}

async fn next_ping_target(sleep: Pin<&mut tokio::time::Sleep>, app_info: &AppInfo) -> PeerId {
    sleep.await;

    app_info.pick_next_target()
}

fn handle_message(app_info: &mut AppInfo, msg: &PeerMessage, received_at: Instant) {
    tracing::debug!(target: "ping", "received PeerMessage::{}", msg);
    match &msg {
        PeerMessage::Routed(msg) => {
            match &msg.body {
                RoutedMessageBody::Pong(p) => {
                    app_info.pong_received(&p.source, p.nonce, received_at);
                }
                _ => {}
            };
        }
        PeerMessage::SyncRoutingTable(r) => {
            app_info.add_announce_accounts(r);
        }
        _ => {}
    }
}

#[derive(Debug)]
pub struct PeerIdentifier {
    pub account_id: Option<AccountId>,
    pub peer_id: PeerId,
}

impl std::fmt::Display for PeerIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match &self.account_id {
            Some(a) => a.fmt(f),
            None => self.peer_id.fmt(f),
        }
    }
}

fn collect_stats(app_info: AppInfo) -> Vec<(PeerIdentifier, PingStats)> {
    let mut ret = Vec::new();
    for (peer_id, state) in app_info.stats {
        let PingState { stats, account_id, .. } = state;
        ret.push((PeerIdentifier { peer_id, account_id }, stats));
    }
    ret
}

pub async fn ping_via_node(
    chain_id: &str,
    genesis_hash: CryptoHash,
    head_height: BlockHeight,
    peer_id: PeerId,
    peer_addr: SocketAddr,
    ttl: u8,
    ping_frequency_millis: u64,
) -> Vec<(PeerIdentifier, PingStats)> {
    let mut app_info = AppInfo::new(chain_id, genesis_hash, head_height);

    app_info.add_peer(&peer_id, None);

    let connect_start = Instant::now();
    let mut peer = match Peer::connect(peer_addr, peer_id).await {
        Ok(p) => {
            tracing::info!(
                target: "ping", "Connection to {:?}@{:?} established. latency: {:?}",
                &p.peer_id, &peer_addr, connect_start.elapsed(),
            );
            p
        }
        Err(e) => {
            tracing::error!(target: "ping", "Error connecting to {:?}: {}", peer_addr, e);
            return vec![];
        }
    };

    let sigint_received = AtomicBool::new(false);

    tokio::select! {
        res = peer.do_handshake(&mut app_info, &sigint_received) => {
            match res {
                Ok(true) => return vec![],
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(target: "ping", "{:?}", e);
                    return vec![];
                }
            }
        }
        _ = tokio::signal::ctrl_c() => {
            sigint_received.store(true, Ordering::Relaxed);
        }
    }

    let mut nonce = 1;
    let next_ping = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(next_ping);

    loop {
        tokio::select! {
            target = next_ping_target(next_ping.as_mut(), &app_info), if app_info.num_pings_in_flight < app_info.requests.len() => {
                if let Err(e) = peer.send_ping(&mut app_info, &target, nonce, ttl).await {
                    tracing::error!(target: "ping", "Failed sending ping to {:?}: {:?}", &target, e);
                    break;
                }
                nonce += 1;
                next_ping.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(ping_frequency_millis));
            }
            res = peer.recv_messages(&sigint_received) => {
                let (messages, first_byte_time, stop) = match res {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::error!(target: "ping", "Failed receiving messages: {}", e);
                        break;
                    }
                };
                for msg in messages {
                    handle_message(&mut app_info, &msg, first_byte_time);
                }
                if stop {
                    break;
                }
            }
            _ = tokio::signal::ctrl_c() => {
                sigint_received.store(true, Ordering::Relaxed);
            }
        }
    }
    collect_stats(app_info)
}
