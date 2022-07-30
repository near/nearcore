use anyhow::Context;
use bytes::buf::{Buf, BufMut};
use bytes::BytesMut;
use near_chain::types::ChainGenesis;
use near_chain::Chain;
use near_crypto::{KeyType, PublicKey, SecretKey};
use near_network::types::{Encoding, Handshake, ParsePeerMessageError, PeerMessage};
use near_network_primitives::time::Utc;
use near_network_primitives::types::{
    AccountOrPeerIdOrHash, PartialEdgeInfo, PeerChainInfoV2, Ping, RawRoutedMessage,
    RoutedMessageBody,
};
use near_primitives::block::GenesisId;
use near_primitives::network::PeerId;
use nearcore::NightshadeRuntime;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
    pub handshake_latency: Option<Duration>,
    pub connect_latency: Option<Duration>,
    // If we're able to connect at least, then any error we encounter later
    // will be set here, so we can still report the stats up to that point
    pub error: Option<anyhow::Error>,
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
    async fn connect(addr: SocketAddr, public_key: PublicKey) -> anyhow::Result<Self> {
        let stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr))
            .await
            .with_context(|| format!("Timed out connecting to {:?}", &addr))?
            .with_context(|| format!("Failed to connect to {:?}", &addr))?;
        Ok(Self { stream, peer_id: PeerId::new(public_key), buf: BytesMut::with_capacity(1024) })
    }

    async fn write_message(&mut self, msg: &PeerMessage) -> io::Result<()> {
        let mut msg = msg.serialize(Encoding::Proto);
        let mut buf = (msg.len() as u32).to_le_bytes().to_vec();
        buf.append(&mut msg);
        self.stream.write_all(&buf).await
    }

    // panics if there's not at least `len` bytes available
    fn extract_msg(&mut self, len: usize) -> Result<PeerMessage, ParsePeerMessageError> {
        let msg = PeerMessage::deserialize(Encoding::Proto, &self.buf[..len])?;
        self.buf.advance(len);
        tracing::debug!(target: "ping", "received PeerMessage::{} from {:?}", &msg, self);
        Ok(msg)
    }

    async fn do_read(&mut self) -> io::Result<()> {
        let n = self.stream.read_buf(&mut self.buf).await?;
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
    async fn read_msg_length(&mut self) -> io::Result<usize> {
        while self.buf.remaining() < 4 {
            self.do_read().await?;
        }
        let len = u32::from_le_bytes(self.buf[..4].try_into().unwrap());
        self.buf.advance(4);
        Ok(len as usize)
    }

    // Append any messages currently buffered in the stream
    async fn read_remaining_messages(
        &mut self,
        messages: &mut Vec<PeerMessage>,
    ) -> anyhow::Result<()> {
        // drain anything still available to be read without blocking
        loop {
            if let Err(e) = self.stream.try_read_buf(&mut self.buf) {
                match e.kind() {
                    io::ErrorKind::WouldBlock => {
                        break;
                    }
                    _ => return Err(e.into()),
                }
            }
        }
        loop {
            if self.buf.remaining() < 4 {
                if self.buf.has_remaining() {
                    tracing::warn!(
                        target: "ping", "There's a partial frame left over in the buffer with nothing left to read yet. \
                        {} bytes out of the 4-byte length prefix received", self.buf.remaining()
                    );
                }
                break;
            }
            let len = u32::from_le_bytes(self.buf[..4].try_into().unwrap()) as usize;
            if self.buf.remaining() < len + 4 {
                tracing::warn!(
                    target: "ping", "There's a partial frame left over in the buffer. \
                    {} bytes out of the {} byte message received", self.buf.remaining() - 4, len
                );
                break;
            }
            self.buf.advance(4);
            messages.push(self.extract_msg(len)?);
        }
        Ok(())
    }

    // Reads from the socket until there is at least one full PeerMessage available.
    // After that point, continues to read any bytes available to be read without blocking
    // and appends any extra messages to the returned Vec. Usually there is only one in there

    // The Instant returned is the time we first read any bytes
    async fn recv_messages(&mut self) -> anyhow::Result<(Vec<PeerMessage>, Instant)> {
        let msg_length;
        let mut messages = Vec::new();

        msg_length = self.read_msg_length().await?;
        let first_byte_time = Instant::now();

        while self.buf.remaining() < msg_length {
            // TODO: measure time to last byte here
            self.do_read().await?;
        }
        messages.push(self.extract_msg(msg_length)?);

        self.read_remaining_messages(&mut messages).await?;

        // make sure we can probably read the next message in one syscall next time
        let max_len_after_next_read = self.buf.chunk_mut().len() + self.buf.remaining();
        if max_len_after_next_read < 512 {
            self.buf.reserve(512 - max_len_after_next_read);
        }
        Ok((messages, first_byte_time))
    }

    async fn do_ping(
        &mut self,
        stats: &mut PingStats,
        app_info: &AppInfo,
        nonce: usize,
    ) -> anyhow::Result<()> {
        let nonce = nonce as u64;
        let body = RoutedMessageBody::Ping(Ping { nonce, source: app_info.my_peer_id.clone() });
        let msg =
            RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(self.peer_id.clone()), body }
                .sign(app_info.my_peer_id.clone(), &app_info.secret_key, 100, Some(Utc::now_utc()));

        self.write_message(&PeerMessage::Routed(Box::new(msg))).await?;
        let start = Instant::now();
        stats.pings_sent += 1;
        let mut pong_received = false;

        // We loop because maybe the peer sends us some other random message, and we're looking specifically
        // for the Pong(nonce) message
        while !pong_received {
            let (messages, first_byte_time) = self.recv_messages().await?;
            for msg in messages {
                match &msg {
                    PeerMessage::Routed(msg) => {
                        match &msg.body {
                            RoutedMessageBody::Pong(p) => {
                                stats.pongs_received += 1;
                                if pong_received {
                                    tracing::warn!(
                                        target: "ping", "Received more than one Pong in a row"
                                    );
                                    continue;
                                }
                                if p.nonce != nonce {
                                    tracing::warn!(
                                        target: "ping", "Received Pong with nonce {} when {} was expected",
                                        p.nonce,
                                        nonce,
                                    );
                                }
                                let latency = first_byte_time - start;
                                pong_received = true;
                                if stats.min_latency == Duration::ZERO
                                    || stats.min_latency > latency
                                {
                                    stats.min_latency = latency;
                                }
                                if stats.max_latency < latency {
                                    stats.max_latency = latency;
                                }
                                let n = stats.pongs_received as u32;
                                stats.average_latency =
                                    ((n - 1) * stats.average_latency + latency) / n;
                            }
                            _ => {}
                        };
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    async fn do_handshake(
        &mut self,
        stats: &mut PingStats,
        app_info: &AppInfo,
    ) -> anyhow::Result<()> {
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
        let (messages, first_byte_time) = self.recv_messages().await?;

        let first = messages.first().unwrap();
        // TODO: maybe check the handshake for sanity
        if !matches!(first, PeerMessage::Handshake(_)) {
            return Err(anyhow::anyhow!(
                "First message received from {:?} is not a handshake: {}",
                self,
                &first
            ));
        }
        stats.handshake_latency = Some(first_byte_time - start);
        for msg in messages.iter().skip(1) {
            tracing::warn!(
                target: "ping", "Received unexpected message from {:?} right after receiving the handshake: {:?}",
                self,
                msg,
            );
        }

        Ok(())
    }
}

struct AppInfo {
    chain_info: PeerChainInfoV2,
    secret_key: SecretKey,
    my_peer_id: PeerId,
    sigint_received: AtomicBool,
}

// try to connect to the given node, and ping it `num_pings` times, returning the associated latency stats
async fn ping_node(
    app_info: &AppInfo,
    peer_public_key: PublicKey,
    peer_addr: SocketAddr,
    num_pings: usize,
) -> PingStats {
    let mut stats = PingStats::default();
    if app_info.sigint_received.load(Ordering::Acquire) {
        return stats;
    }

    let connect_start = Instant::now();
    let mut peer = match Peer::connect(peer_addr, peer_public_key).await {
        Ok(p) => {
            tracing::info!(target: "ping", "Connection to {:?}@{:?} established", &p.peer_id, &peer_addr);
            stats.connect_latency = Some(connect_start.elapsed());
            p
        }
        Err(e) => {
            stats.error = Some(e);
            return stats;
        }
    };

    if app_info.sigint_received.load(Ordering::Acquire) {
        return stats;
    }
    if let Err(e) = peer.do_handshake(&mut stats, app_info).await {
        stats.error = Some(e);
        return stats;
    }

    for nonce in 0..num_pings {
        if app_info.sigint_received.load(Ordering::Acquire) {
            return stats;
        }
        if let Err(e) = peer.do_ping(&mut stats, app_info, nonce).await {
            stats.error = Some(e);
            return stats;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    stats
}

fn chain_info<P: AsRef<Path>>(home: P) -> anyhow::Result<PeerChainInfoV2> {
    let near_config = nearcore::config::load_config(
        home.as_ref(),
        near_chain_configs::GenesisValidationMode::UnsafeFast,
    )
    .with_context(|| format!("Failed to open config at {:?}", home.as_ref()))?;
    let store = near_store::Store::opener(home.as_ref(), &near_config.config.store).open();
    let runtime = NightshadeRuntime::from_config(home.as_ref(), store.clone(), &near_config);
    let chain = Chain::new_for_view_client(
        std::sync::Arc::new(runtime),
        &ChainGenesis::new(&near_config.genesis),
        near_chain::DoomslugThresholdMode::TwoThirds,
        !near_config.client_config.archive,
    )
    .context("Failed initializing chain")?;
    let height = chain.head().context("Failed fetching chain HEAD")?.height;

    Ok(PeerChainInfoV2 {
        genesis_id: GenesisId {
            chain_id: near_config.genesis.config.chain_id.clone(),
            hash: chain.genesis().hash().clone(),
        },
        height,
        tracked_shards: near_config.client_config.tracked_shards.clone(),
        archival: near_config.client_config.archive,
    })
}

pub async fn ping_nodes<P: AsRef<Path>>(
    home: P,
    peers: Vec<(PublicKey, SocketAddr)>,
    num_pings: usize,
) -> anyhow::Result<Vec<(SocketAddr, PingStats)>> {
    let chain_info = chain_info(home)?;
    // don't use the key in node_key.json so we dont have to worry about the nonce
    // to use in the handshake
    let secret_key = SecretKey::from_random(KeyType::ED25519);
    let my_peer_id = PeerId::new(secret_key.public_key());
    let app_info = Arc::new(AppInfo {
        chain_info,
        secret_key,
        my_peer_id,
        sigint_received: AtomicBool::new(false),
    });
    let mut tasks = Vec::new();
    for (public_key, addr) in peers {
        let app_info = app_info.clone();
        // TODO: maybe don't just spawn them all at once. It's fine for the first
        // version of the tool, but if we want the highest fidelity latency stats it would
        // make sense to only have one task per CPU at a time.
        tasks.push(tokio::spawn(async move {
            let stats = ping_node(app_info.as_ref(), public_key, addr, num_pings).await;
            (addr, stats)
        }));
    }

    let mut pings = futures::future::join_all(tasks);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                app_info.sigint_received.store(true, Ordering::Release);
            }
            results = &mut pings => {
                let mut ret = Vec::new();
                for r in results {
                    match r {
                        Ok(s) => ret.push(s),
                        Err(e) => {
                            tracing::error!(target: "ping", "ping task failed: {:?}", e);
                        }
                    }
                }
                return Ok(ret);
            }
        }
    }
}
