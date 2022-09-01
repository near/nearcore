use anyhow::Context;
use bytes::buf::{Buf, BufMut};
use bytes::BytesMut;
use near_crypto::{KeyType, SecretKey};
use near_network::types::{
    Encoding, Handshake, HandshakeFailureReason, ParsePeerMessageError, PeerMessage,
    RoutingTableUpdate,
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
use near_primitives_core::types::ProtocolVersion;
use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub mod csv;

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
    async fn connect(addr: SocketAddr, peer_id: PeerId) -> io::Result<Self> {
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

    // returns a vec of message lengths currently ready
    // bool returned says whether we read `stop` = true
    fn read_remaining_messages(
        &mut self,
        first_msg_length: usize,
        stop: &AtomicBool,
    ) -> anyhow::Result<(Vec<(usize, Instant)>, bool)> {
        // drain anything still available to be read without blocking
        let mut stopped = false;
        let mut pos = first_msg_length + 4;
        let mut lengths = Vec::new();
        let mut pending_msg_length = None;

        loop {
            match self.stream.try_read_buf(&mut self.buf) {
                Ok(n) => {
                    let timestamp = Instant::now();
                    tracing::trace!(target: "ping", "Read {} bytes from {:?} non-blocking", n, self.stream.peer_addr());
                    loop {
                        if pending_msg_length.is_none() && self.buf.remaining() - pos >= 4 {
                            let len =
                                u32::from_le_bytes(self.buf[pos..pos + 4].try_into().unwrap());
                            pending_msg_length = Some(len as usize);
                        }
                        if let Some(l) = pending_msg_length {
                            if self.buf.remaining() - pos >= l + 4 {
                                lengths.push((l, timestamp));
                                pos += l + 4;
                                pending_msg_length = None;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
                Err(e) => match e.kind() {
                    io::ErrorKind::WouldBlock => {
                        break;
                    }
                    _ => return Err(e.into()),
                },
            };
            if stop.load(Ordering::Relaxed) {
                stopped = true;
                break;
            }
        }

        Ok((lengths, stopped))
    }

    // Reads from the socket until there is at least one full PeerMessage available.
    // After that point, continues to read any bytes available to be read without blocking
    // and appends any extra messages to the returned Vec. Usually there is only one in there

    // The bool returned is true if we should stop now
    async fn recv_messages(
        &mut self,
        stop: &AtomicBool,
    ) -> anyhow::Result<(Vec<(PeerMessage, Instant)>, bool)> {
        if stop.load(Ordering::Relaxed) {
            return Ok((Vec::new(), true));
        }
        let mut messages = Vec::new();
        let (msg_length, first_byte_time) = self.read_msg_length().await?;

        while self.buf.remaining() < msg_length + 4 {
            if stop.load(Ordering::Relaxed) {
                return Ok((messages, true));
            }
            // TODO: measure time to last byte here
            self.do_read().await?;
        }

        let (more_messages, stopped) = self.read_remaining_messages(msg_length, stop)?;

        let msg = self
            .extract_msg(msg_length)
            .with_context(|| format!("error parsing message of length {}", msg_length))?;
        tracing::debug!(target: "ping", "received PeerMessage::{} len: {}", msg, msg_length);
        messages.push((msg, first_byte_time));

        for (len, timestamp) in more_messages {
            let msg = self
                .extract_msg(len)
                .with_context(|| format!("error parsing message of length {}", len))?;
            tracing::debug!(target: "ping", "received PeerMessage::{} len: {}", msg, len);
            messages.push((msg, timestamp));
        }

        // make sure we can probably read the next message in one syscall next time
        let max_len_after_next_read = self.buf.chunk_mut().len() + self.buf.remaining();
        if max_len_after_next_read < 512 {
            self.buf.reserve(512 - max_len_after_next_read);
        }
        Ok((messages, stopped))
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

        self.write_message(&PeerMessage::Routed(Box::new(msg))).await?;
        app_info.ping_sent(target, nonce);
        Ok(())
    }

    async fn do_handshake(
        &mut self,
        app_info: &mut AppInfo,
        sigint_received: &AtomicBool,
    ) -> anyhow::Result<bool> {
        let handshake = PeerMessage::Handshake(Handshake::new(
            app_info.protocol_version,
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

        let (messages, stop) = self.recv_messages(&sigint_received).await?;

        if let Some((first, timestamp)) = messages.first() {
            match &first {
                // TODO: maybe check the handshake for sanity
                PeerMessage::Handshake(_) => {
                    tracing::info!(target: "ping", "handshake latency: {:?}", *timestamp - start);
                },
                PeerMessage::HandshakeFailure(_peer_info, reason) => {
                    match &reason {
                        HandshakeFailureReason::ProtocolVersionMismatch { version, oldest_supported_version } => return Err(anyhow::anyhow!(
                            "Received Handshake Failure: {:?}. Try running again with --protocol-version between {} and {}",
                            reason, oldest_supported_version, version
                        )),
                        HandshakeFailureReason::GenesisMismatch(_) => return Err(anyhow::anyhow!(
                            "Received Handshake Failure: {:?}. Try running again with --chain-id and --genesis-hash set to these values.",
                            reason,
                        )),
                        HandshakeFailureReason::InvalidTarget => return Err(anyhow::anyhow!(
                            "Received Handshake Failure: {:?}. Is the public key given with --peer correct?",
                            reason,
                        )),
                    }
                }
                _ => return Err(anyhow::anyhow!(
                    "First message received from {:?} is not a handshake: {:?}",
                    self,
                    &first
                )),
            };
        }
        for (msg, _timestamp) in messages.iter().skip(1) {
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

type Nonce = u64;

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

#[derive(Clone, Debug, Eq, PartialEq)]
struct PingTimeout {
    peer_id: PeerId,
    nonce: u64,
    timeout: Instant,
}

impl PartialOrd for PingTimeout {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PingTimeout {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.timeout.cmp(&other.timeout).then_with(|| {
            self.nonce.cmp(&other.nonce).then_with(|| self.peer_id.cmp(&other.peer_id))
        })
    }
}

fn peer_str(peer_id: &PeerId, account_id: Option<&AccountId>) -> String {
    account_id.map_or_else(|| format!("{}", peer_id), |a| format!("{}", a))
}

const MAX_PINGS_IN_FLIGHT: usize = 10;
const PING_TIMEOUT: Duration = Duration::from_secs(100);

#[derive(Debug)]
struct PingState {
    stats: PingStats,
    last_pinged: Option<Instant>,
    account_id: Option<AccountId>,
}

struct PingTimes {
    sent_at: Instant,
    timeout: Instant,
}

struct AppInfo {
    chain_info: PeerChainInfoV2,
    protocol_version: ProtocolVersion,
    secret_key: SecretKey,
    my_peer_id: PeerId,
    stats: HashMap<PeerId, PingState>,
    // we will ping targets in round robin fashion. So this keeps a set of
    // targets ordered by when we last pinged them.
    requests: BTreeMap<PingTarget, HashMap<Nonce, PingTimes>>,
    timeouts: BTreeSet<PingTimeout>,
    account_filter: Option<HashSet<AccountId>>,
}

impl AppInfo {
    fn new(
        chain_id: &str,
        genesis_hash: CryptoHash,
        head_height: BlockHeight,
        protocol_version: ProtocolVersion,
        account_filter: Option<HashSet<AccountId>>,
    ) -> Self {
        let secret_key = SecretKey::from_random(KeyType::ED25519);
        let my_peer_id = PeerId::new(secret_key.public_key());

        Self {
            chain_info: PeerChainInfoV2 {
                genesis_id: GenesisId { chain_id: chain_id.to_string(), hash: genesis_hash },
                height: head_height,
                tracked_shards: vec![0],
                archival: false,
            },
            protocol_version,
            secret_key,
            my_peer_id,
            stats: HashMap::new(),
            requests: BTreeMap::new(),
            timeouts: BTreeSet::new(),
            account_filter,
        }
    }

    fn pick_next_target(&self) -> Option<PeerId> {
        for (target, pending_pings) in self.requests.iter() {
            if pending_pings.len() < MAX_PINGS_IN_FLIGHT {
                return Some(target.peer_id.clone());
            }
        }
        None
    }

    fn ping_sent(&mut self, peer_id: &PeerId, nonce: u64) {
        let timestamp = Instant::now();
        let timeout = timestamp + PING_TIMEOUT;

        match self.stats.entry(peer_id.clone()) {
            Entry::Occupied(mut e) => {
                let state = e.get_mut();
                let mut pending_pings = self
                    .requests
                    .remove(&PingTarget {
                        peer_id: peer_id.clone(),
                        last_pinged: state.last_pinged,
                    })
                    .unwrap();

                println!(
                    "send ping --------------> {}",
                    peer_str(&peer_id, state.account_id.as_ref())
                );

                state.stats.pings_sent += 1;
                state.last_pinged = Some(timestamp);

                match pending_pings.entry(nonce) {
                    Entry::Occupied(_) => {
                        tracing::warn!(
                            target: "ping", "Internal error! Sent two pings with nonce {} to {}. \
                            Latency stats will probably be wrong.", nonce, &peer_id
                        );
                    }
                    Entry::Vacant(e) => {
                        e.insert(PingTimes { sent_at: timestamp, timeout });
                    }
                };
                self.requests.insert(
                    PingTarget { peer_id: peer_id.clone(), last_pinged: Some(timestamp) },
                    pending_pings,
                );
                self.timeouts.insert(PingTimeout { peer_id: peer_id.clone(), nonce, timeout })
            }
            Entry::Vacant(_) => {
                panic!("sent ping to {:?}, but not present in stats HashMap", peer_id)
            }
        };
    }

    fn pong_received(
        &mut self,
        peer_id: &PeerId,
        nonce: u64,
        received_at: Instant,
    ) -> Option<(Duration, Option<&AccountId>)> {
        match self.stats.get_mut(peer_id) {
            Some(state) => {
                let pending_pings = self
                    .requests
                    .get_mut(&PingTarget {
                        peer_id: peer_id.clone(),
                        last_pinged: state.last_pinged,
                    })
                    .unwrap();

                match pending_pings.remove(&nonce) {
                    Some(times) => {
                        let latency = received_at - times.sent_at;
                        state.stats.pong_received(latency);
                        assert!(self.timeouts.remove(&PingTimeout {
                            peer_id: peer_id.clone(),
                            nonce,
                            timeout: times.timeout
                        }));

                        println!(
                            "recv pong <-------------- {} latency: {:?}",
                            peer_str(&peer_id, state.account_id.as_ref()),
                            latency
                        );
                        Some((latency, state.account_id.as_ref()))
                    }
                    None => {
                        tracing::warn!(
                            target: "ping",
                            "received pong with nonce {} from {}, after we probably treated it as timed out previously",
                            nonce, peer_str(&peer_id, state.account_id.as_ref())
                        );
                        None
                    }
                }
            }
            None => {
                tracing::warn!(target: "ping", "received pong from {:?}, but don't know of this peer", peer_id);
                None
            }
        }
    }

    fn pop_timeout(&mut self, t: &PingTimeout) {
        assert!(self.timeouts.remove(&t));
        let state = self.stats.get(&t.peer_id).unwrap();

        let pending_pings = self
            .requests
            .get_mut(&PingTarget {
                peer_id: t.peer_id.clone(),
                last_pinged: state.last_pinged.clone(),
            })
            .unwrap();
        assert!(pending_pings.remove(&t.nonce).is_some());
        println!(
            "{} timeout after {:?} ---------",
            peer_str(&t.peer_id, state.account_id.as_ref()),
            PING_TIMEOUT
        );
    }

    fn add_peer(&mut self, peer_id: &PeerId, account_id: Option<&AccountId>) {
        if let Some(filter) = self.account_filter.as_ref() {
            if let Some(account_id) = account_id {
                if !filter.contains(account_id) {
                    tracing::debug!(target: "ping", "skipping AnnounceAccount for {}", account_id);
                    return;
                }
            }
        }
        match self.stats.entry(peer_id.clone()) {
            Entry::Occupied(mut e) => {
                if let Some(account_id) = account_id {
                    let state = e.get_mut();
                    if let Some(old) = state.account_id.as_ref() {
                        if old != account_id {
                            // TODO: we should just keep track of all accounts that map
                            // to this peer id, since it's valid for there to be more than one.
                            // We only use the accounts in the account filter and when displaying
                            // ping targets, so theres no reason we cant keep track of all of them
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
                self.requests.insert(
                    PingTarget { peer_id: peer_id.clone(), last_pinged: None },
                    HashMap::new(),
                );
            }
        }
    }

    fn add_announce_accounts(&mut self, r: &RoutingTableUpdate) {
        for a in r.accounts.iter() {
            self.add_peer(&a.peer_id, Some(&a.account_id));
        }
    }

    fn peer_id_to_account_id(&self, peer_id: &PeerId) -> Option<&AccountId> {
        self.stats.get(peer_id).and_then(|s| s.account_id.as_ref())
    }
}

fn handle_message(
    app_info: &mut AppInfo,
    msg: &PeerMessage,
    received_at: Instant,
    latencies_csv: Option<&mut crate::csv::LatenciesCsv>,
) -> anyhow::Result<()> {
    match &msg {
        PeerMessage::Routed(msg) => {
            match &msg.body {
                RoutedMessageBody::Pong(p) => {
                    if let Some((latency, account_id)) =
                        app_info.pong_received(&p.source, p.nonce, received_at)
                    {
                        if let Some(csv) = latencies_csv {
                            csv.write(&p.source, account_id, latency)
                                .context("Failed writing to CSV file")?;
                        }
                    }
                }
                _ => {}
            };
        }
        PeerMessage::SyncRoutingTable(r) => {
            app_info.add_announce_accounts(r);
        }
        _ => {}
    };
    Ok(())
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

fn prepare_timeout(sleep: Pin<&mut tokio::time::Sleep>, app_info: &AppInfo) -> Option<PingTimeout> {
    if let Some(t) = app_info.timeouts.iter().next() {
        sleep.reset(tokio::time::Instant::from_std(t.timeout));
        Some(t.clone())
    } else {
        None
    }
}

pub async fn ping_via_node(
    chain_id: &str,
    genesis_hash: CryptoHash,
    head_height: BlockHeight,
    protocol_version: ProtocolVersion,
    peer_id: PeerId,
    peer_addr: SocketAddr,
    ttl: u8,
    ping_frequency_millis: u64,
    account_filter: Option<HashSet<AccountId>>,
    mut latencies_csv: Option<crate::csv::LatenciesCsv>,
) -> Vec<(PeerIdentifier, PingStats)> {
    let mut app_info =
        AppInfo::new(chain_id, genesis_hash, head_height, protocol_version, account_filter);

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
                    tracing::error!(target: "ping", "{:#}", e);
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
    let next_timeout = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(next_timeout);

    loop {
        let target = app_info.pick_next_target();
        let pending_timeout = prepare_timeout(next_timeout.as_mut(), &app_info);

        tokio::select! {
            _ = &mut next_ping, if target.is_some() => {
                let target = target.unwrap();
                if let Err(e) = peer.send_ping(&mut app_info, &target, nonce, ttl).await {
                    tracing::error!(target: "ping", "Failed sending ping to {:?}: {:#}", &target, e);
                    break;
                }
                nonce += 1;
                next_ping.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(ping_frequency_millis));
            }
            res = peer.recv_messages(&sigint_received) => {
                let (messages, stop) = match res {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::error!(target: "ping", "Failed receiving messages: {:#}", e);
                        break;
                    }
                };
                for (msg, first_byte_time) in messages {
                    if let Err(e) = handle_message(&mut app_info, &msg, first_byte_time, latencies_csv.as_mut()) {
                        tracing::error!(target: "ping", "{:#}", e);
                        break;
                    }
                }
                if stop {
                    break;
                }
            }
            _ = &mut next_timeout, if pending_timeout.is_some() => {
                let t = pending_timeout.unwrap();
                app_info.pop_timeout(&t);
                if let Some(csv) = latencies_csv.as_mut() {
                    if let Err(e) =
                    csv.write_timeout(&t.peer_id, app_info.peer_id_to_account_id(&t.peer_id)) {
                        tracing::error!("Failed writing to CSV file: {}", e);
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                sigint_received.store(true, Ordering::Relaxed);
            }
        }
    }
    collect_stats(app_info)
}
