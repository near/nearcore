use crate::network_protocol::{
    Encoding, Handshake, HandshakeFailureReason, PartialEdgeInfo, PeerChainInfoV2, PeerIdOrHash,
    PeerMessage, Ping, Pong, RawRoutedMessage, RoutedMessageBody, RoutingTableUpdate,
};
use crate::tcp;
use crate::types::{
    PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, PeerInfo, StateResponseInfo,
};
use bytes::buf::{Buf, BufMut};
use bytes::BytesMut;
use near_async::time::{Duration, Instant, Utc};
use near_crypto::{KeyType, SecretKey};
use near_primitives::block::{Block, BlockHeader, GenesisId};
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use std::fmt;
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Represents a connection to a peer, and provides only minimal functionality.
/// Almost none of the usual NEAR network logic is implemented, and the user
/// will receive messages via the recv() function and send messages via
/// send_message() and send_routed_message()
pub struct Connection {
    secret_key: SecretKey,
    my_peer_id: PeerId,
    peer_id: PeerId,
    stream: PeerStream,
    // this is used to keep track of routed messages we've sent so that when we get a reply
    // that references one of our previously sent messages, we can determine that the message is for us
    route_cache: lru::LruCache<CryptoHash, ()>,
    // when a peer connects to us, it'll send two handshakes. One as a proto and one borsh-encoded.
    // If this field is true, it means we expect to receive a message that won't parse as a proto, and
    // will accept and drop one such message without giving an error.
    borsh_message_expected: bool,
}

// The types of messages it's possible to route to a target PeerId via the connected peer as a first hop
// These can be sent with Connection::send_routed_message(), and received in Message::Routed() from
// Connection::recv()
#[derive(Clone, strum::IntoStaticStr)]
pub enum RoutedMessage {
    Ping { nonce: u64 },
    Pong { nonce: u64, source: PeerId },
    StateRequestPart(ShardId, CryptoHash, u64),
    VersionedStateResponse(StateResponseInfo),
    PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg),
    PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg),
}

impl fmt::Display for RoutedMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(<&'static str>::from(self), f)
    }
}

impl fmt::Debug for RoutedMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ping { nonce } => write!(f, "Ping({})", nonce),
            Self::Pong { nonce, source } => write!(f, "Pong({}, {})", nonce, source),
            Self::StateRequestPart(shard_id, hash, part_id) => {
                write!(f, "StateRequestPart({}, {}, {})", shard_id, hash, part_id)
            }
            Self::VersionedStateResponse(r) => write!(
                f,
                "VersionedStateResponse(shard_id: {} sync_hash: {})",
                r.shard_id(),
                r.sync_hash()
            ),
            Self::PartialEncodedChunkRequest(r) => write!(f, "PartialEncodedChunkRequest({:?})", r),
            Self::PartialEncodedChunkResponse(r) => write!(
                f,
                "PartialEncodedChunkResponse(chunk_hash: {} parts_ords: {:?} num_receipts: {})",
                &r.chunk_hash.0,
                r.parts.iter().map(|p| p.part_ord).collect::<Vec<_>>(),
                r.receipts.len()
            ),
        }
    }
}

// The types of messages it's possible to send directly to the connected peer.
// These can be sent with Connection::send_message(), and received in Message::Direct() from
// Connection::recv()
#[derive(Clone, strum::IntoStaticStr)]
pub enum DirectMessage {
    AnnounceAccounts(Vec<AnnounceAccount>),
    BlockRequest(CryptoHash),
    Block(Block),
    BlockHeadersRequest(Vec<CryptoHash>),
    BlockHeaders(Vec<BlockHeader>),
}

impl fmt::Display for DirectMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(<&'static str>::from(self), f)
    }
}

impl fmt::Debug for DirectMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AnnounceAccounts(a) => write!(f, "AnnounceAccounts({:?})", a),
            Self::BlockRequest(r) => write!(f, "BlockRequest({})", r),
            Self::Block(b) => write!(f, "Block(#{} {})", b.header().height(), b.header().hash()),
            Self::BlockHeadersRequest(r) => write!(f, "BlockHeadersRequest({:?})", r),
            Self::BlockHeaders(h) => {
                write!(
                    f,
                    "BlockHeaders({:?})",
                    h.iter().map(|h| format!("#{} {}", h.height(), h.hash())).collect::<Vec<_>>()
                )
            }
        }
    }
}

/// The types of messages it's possible to send/receive from a `Connection`. Any PeerMessage
/// we receive that doesn't fit one of the types we've enumerated will just be logged and dropped.
#[derive(Clone, strum::IntoStaticStr)]
pub enum Message {
    Direct(DirectMessage),
    Routed(RoutedMessage),
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct(msg) => write!(f, "raw::{}", msg),
            Self::Routed(msg) => write!(f, "raw::Routed({})", msg),
        }
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct(msg) => write!(f, "raw::{:?}", msg),
            Self::Routed(msg) => write!(f, "raw::Routed({:?})", msg),
        }
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "raw::Connection({:?}@{} -> {:?}@{})",
            &self.my_peer_id,
            &self.stream.stream.local_addr,
            &self.peer_id,
            &self.stream.stream.peer_addr
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectError {
    #[error(transparent)]
    IO(std::io::Error),
    #[error("failed parsing protobuf of length {0}")]
    Parse(usize),
    #[error("handshake failed {0:?}")]
    HandshakeFailure(HandshakeFailureReason),
    #[error("received unexpected message before the handshake: {0:?}")]
    UnexpectedFirstMessage(PeerMessage),
    #[error(transparent)]
    TcpConnect(anyhow::Error),
}

impl From<RecvError> for ConnectError {
    fn from(err: RecvError) -> Self {
        match err {
            RecvError::IO(io) => Self::IO(io),
            RecvError::Parse(len) => Self::Parse(len),
        }
    }
}

fn new_handshake(
    secret_key: &SecretKey,
    my_peer_id: &PeerId,
    target_peer_id: &PeerId,
    listen_port: u16,
    nonce: u64,
    protocol_version: ProtocolVersion,
    chain_id: &str,
    genesis_hash: CryptoHash,
    head_height: BlockHeight,
    tracked_shards: Vec<ShardId>,
    archival: bool,
) -> PeerMessage {
    PeerMessage::Tier2Handshake(Handshake {
        protocol_version,
        oldest_supported_version: protocol_version - 2,
        sender_peer_id: my_peer_id.clone(),
        target_peer_id: target_peer_id.clone(),
        // we have to set this even if we have no intention of listening since otherwise
        // the peer will drop our connection
        sender_listen_port: Some(listen_port),
        sender_chain_info: PeerChainInfoV2 {
            genesis_id: GenesisId { chain_id: chain_id.to_string(), hash: genesis_hash },
            height: head_height,
            tracked_shards,
            archival,
        },
        partial_edge_info: PartialEdgeInfo::new(my_peer_id, target_peer_id, nonce, secret_key),
        owned_account: None,
    })
}

impl Connection {
    /// Connect to the NEAR node at `peer_id`@`addr`. The inputs are used to build out handshake,
    /// and this function will return a `Peer` when a handshake has been received successfully.
    pub async fn connect(
        addr: SocketAddr,
        peer_id: PeerId,
        my_protocol_version: Option<ProtocolVersion>,
        chain_id: &str,
        genesis_hash: CryptoHash,
        head_height: BlockHeight,
        tracked_shards: Vec<ShardId>,
        recv_timeout: Duration,
    ) -> Result<Self, ConnectError> {
        let secret_key = SecretKey::from_random(KeyType::ED25519);
        let my_peer_id = PeerId::new(secret_key.public_key());

        let start = Instant::now();
        let stream = tcp::Stream::connect(&PeerInfo::new(peer_id.clone(), addr), tcp::Tier::T2)
            .await
            .map_err(ConnectError::TcpConnect)?;
        tracing::info!(
            target: "network", "Connection to {}@{:?} established. latency: {}",
            &peer_id, &addr, start.elapsed(),
        );
        let mut peer = Self {
            stream: PeerStream::new(stream, recv_timeout),
            peer_id,
            secret_key,
            my_peer_id,
            route_cache: lru::LruCache::new(1_000_000),
            borsh_message_expected: false,
        };
        peer.do_handshake(
            my_protocol_version.unwrap_or(PROTOCOL_VERSION),
            chain_id,
            genesis_hash,
            head_height,
            tracked_shards,
        )
        .await?;

        Ok(peer)
    }

    async fn on_accept(
        stream: tcp::Stream,
        secret_key: SecretKey,
        chain_id: &str,
        genesis_hash: CryptoHash,
        head_height: BlockHeight,
        tracked_shards: Vec<ShardId>,
        archival: bool,
        recv_timeout: Duration,
    ) -> Result<Self, ConnectError> {
        let mut stream = PeerStream::new(stream, recv_timeout);
        let mut borsh_message_expected = true;
        let (message, _timestamp) = match stream.recv_message().await {
            Ok(m) => m,
            Err(RecvError::Parse(len)) => {
                tracing::debug!(target: "network", "dropping a non protobuf message of length {}. Probably an extra handshake.", len);
                borsh_message_expected = false;
                stream.recv_message().await?
            }
            Err(RecvError::IO(e)) => return Err(ConnectError::IO(e)),
        };

        let (peer_id, nonce) = match message {
            // TODO: maybe check the handshake for sanity
            PeerMessage::Tier2Handshake(h) => (h.sender_peer_id, h.partial_edge_info.nonce),
            PeerMessage::HandshakeFailure(_peer_info, reason) => {
                return Err(ConnectError::HandshakeFailure(reason))
            }
            _ => return Err(ConnectError::UnexpectedFirstMessage(message)),
        };

        let my_peer_id = PeerId::new(secret_key.public_key());
        let handshake = new_handshake(
            &secret_key,
            &my_peer_id,
            &peer_id,
            stream.stream.local_addr.port(),
            nonce,
            PROTOCOL_VERSION,
            chain_id,
            genesis_hash,
            head_height,
            tracked_shards,
            archival,
        );

        stream.write_message(&handshake).await.map_err(ConnectError::IO)?;

        Ok(Self {
            secret_key,
            my_peer_id,
            stream,
            peer_id,
            route_cache: lru::LruCache::new(1_000_000),
            borsh_message_expected,
        })
    }

    async fn do_handshake(
        &mut self,
        protocol_version: ProtocolVersion,
        chain_id: &str,
        genesis_hash: CryptoHash,
        head_height: BlockHeight,
        tracked_shards: Vec<ShardId>,
    ) -> Result<(), ConnectError> {
        let handshake = new_handshake(
            &self.secret_key,
            &self.my_peer_id,
            &self.peer_id,
            self.stream.stream.local_addr.port(),
            1,
            protocol_version,
            chain_id,
            genesis_hash,
            head_height,
            tracked_shards,
            false,
        );

        self.stream.write_message(&handshake).await.map_err(ConnectError::IO)?;

        let start = Instant::now();

        let (message, timestamp) = self.stream.recv_message().await?;

        match message {
            // TODO: maybe check the handshake for sanity
            PeerMessage::Tier2Handshake(_) => {
                tracing::info!(target: "network", "handshake latency: {}", timestamp - start);
            }
            PeerMessage::HandshakeFailure(_peer_info, reason) => {
                return Err(ConnectError::HandshakeFailure(reason))
            }
            _ => return Err(ConnectError::UnexpectedFirstMessage(message)),
        };

        Ok(())
    }

    // Try to send a PeerMessage corresponding to the given DirectMessage
    pub async fn send_message(&mut self, msg: DirectMessage) -> io::Result<()> {
        let peer_msg = match msg {
            DirectMessage::AnnounceAccounts(accounts) => {
                PeerMessage::SyncRoutingTable(RoutingTableUpdate { edges: Vec::new(), accounts })
            }
            DirectMessage::BlockRequest(h) => PeerMessage::BlockRequest(h),
            DirectMessage::Block(b) => PeerMessage::Block(b),
            DirectMessage::BlockHeadersRequest(h) => PeerMessage::BlockHeadersRequest(h),
            DirectMessage::BlockHeaders(h) => PeerMessage::BlockHeaders(h),
        };

        self.stream.write_message(&peer_msg).await
    }

    // Try to send a routed PeerMessage corresponding to the given RoutedMessage
    pub async fn send_routed_message(
        &mut self,
        msg: RoutedMessage,
        target: PeerId,
        ttl: u8,
    ) -> io::Result<()> {
        let body = match msg {
            RoutedMessage::Ping { nonce } => {
                RoutedMessageBody::Ping(Ping { nonce, source: self.my_peer_id.clone() })
            }
            RoutedMessage::Pong { nonce, source } => {
                RoutedMessageBody::Pong(Pong { nonce, source })
            }
            RoutedMessage::StateRequestPart(shard_id, block_hash, part_id) => {
                RoutedMessageBody::StateRequestPart(shard_id, block_hash, part_id)
            }
            RoutedMessage::VersionedStateResponse(response) => {
                RoutedMessageBody::VersionedStateResponse(response)
            }
            RoutedMessage::PartialEncodedChunkRequest(request) => {
                RoutedMessageBody::PartialEncodedChunkRequest(request)
            }
            RoutedMessage::PartialEncodedChunkResponse(response) => {
                RoutedMessageBody::PartialEncodedChunkResponse(response)
            }
        };
        let msg = RawRoutedMessage { target: PeerIdOrHash::PeerId(target), body }.sign(
            &self.secret_key,
            ttl,
            Some(Utc::now_utc()),
        );
        self.route_cache.put(msg.hash(), ());
        self.stream.write_message(&PeerMessage::Routed(Box::new(msg))).await
    }

    fn target_is_for_me(&mut self, target: &PeerIdOrHash) -> bool {
        match target {
            PeerIdOrHash::PeerId(peer_id) => peer_id == &self.my_peer_id,
            PeerIdOrHash::Hash(hash) => self.route_cache.pop(hash).is_some(),
        }
    }

    fn recv_routed_msg(
        &mut self,
        msg: &crate::network_protocol::RoutedMessage,
    ) -> Option<RoutedMessage> {
        if !self.target_is_for_me(&msg.target) {
            tracing::debug!(
                target: "network", "{:?} dropping routed message {} for {:?}",
                &self, <&'static str>::from(&msg.body), &msg.target
            );
            return None;
        }
        match &msg.body {
            RoutedMessageBody::Ping(p) => Some(RoutedMessage::Ping { nonce: p.nonce }),
            RoutedMessageBody::Pong(p) => {
                Some(RoutedMessage::Pong { nonce: p.nonce, source: p.source.clone() })
            }
            RoutedMessageBody::VersionedStateResponse(state_response_info) => {
                Some(RoutedMessage::VersionedStateResponse(state_response_info.clone()))
            }
            RoutedMessageBody::StateRequestPart(shard_id, hash, part_id) => {
                Some(RoutedMessage::StateRequestPart(*shard_id, *hash, *part_id))
            }
            RoutedMessageBody::PartialEncodedChunkRequest(request) => {
                Some(RoutedMessage::PartialEncodedChunkRequest(request.clone()))
            }
            RoutedMessageBody::PartialEncodedChunkResponse(response) => {
                Some(RoutedMessage::PartialEncodedChunkResponse(response.clone()))
            }
            _ => None,
        }
    }

    /// Reads from the socket until we receive some message that we care to pass to the caller
    /// (that is, represented in `DirectMessage` or `RoutedMessage`).
    pub async fn recv(&mut self) -> io::Result<(Message, Instant)> {
        loop {
            let (msg, timestamp) = match self.stream.recv_message().await {
                Ok(m) => m,
                Err(RecvError::Parse(len)) => {
                    if self.borsh_message_expected {
                        tracing::debug!(target: "network", "{:?} dropping a non protobuf message. Probably an extra handshake.", &self);
                        self.borsh_message_expected = false;
                        continue;
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("error parsing protobuf of length {}", len),
                        ));
                    }
                }
                Err(RecvError::IO(e)) => return Err(e),
            };
            match msg {
                PeerMessage::Routed(r) => {
                    if let Some(msg) = self.recv_routed_msg(&r) {
                        return Ok((Message::Routed(msg), timestamp));
                    }
                }
                PeerMessage::SyncRoutingTable(r) => {
                    return Ok((
                        Message::Direct(DirectMessage::AnnounceAccounts(r.accounts)),
                        timestamp,
                    ));
                }
                PeerMessage::BlockRequest(hash) => {
                    return Ok((Message::Direct(DirectMessage::BlockRequest(hash)), timestamp));
                }
                PeerMessage::Block(b) => {
                    return Ok((Message::Direct(DirectMessage::Block(b)), timestamp));
                }
                PeerMessage::BlockHeadersRequest(hashes) => {
                    return Ok((
                        Message::Direct(DirectMessage::BlockHeadersRequest(hashes)),
                        timestamp,
                    ));
                }
                PeerMessage::BlockHeaders(headers) => {
                    return Ok((Message::Direct(DirectMessage::BlockHeaders(headers)), timestamp));
                }
                _ => {}
            }
        }
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }
}

#[derive(thiserror::Error, Debug)]
enum RecvError {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("error parsing protobuf of length {0}")]
    Parse(usize),
}

struct PeerStream {
    stream: tcp::Stream,
    buf: BytesMut,
    recv_timeout: Duration,
}

impl std::fmt::Debug for PeerStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "raw::PeerStream({} -> {})", &self.stream.local_addr, &self.stream.peer_addr)
    }
}

impl PeerStream {
    fn new(stream: tcp::Stream, recv_timeout: Duration) -> Self {
        Self { stream, buf: BytesMut::with_capacity(1024), recv_timeout }
    }

    async fn write_message(&mut self, msg: &PeerMessage) -> io::Result<()> {
        let mut msg = msg.serialize(Encoding::Proto);
        let mut buf = (msg.len() as u32).to_le_bytes().to_vec();
        buf.append(&mut msg);
        self.stream.stream.write_all(&buf).await
    }

    async fn do_read(&mut self) -> io::Result<()> {
        let n = tokio::time::timeout(
            self.recv_timeout.try_into().unwrap(),
            self.stream.stream.read_buf(&mut self.buf),
        )
        .await??;
        tracing::trace!(target: "network", "Read {} bytes from {:?}", n, self.stream.peer_addr);
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

    // Reads from the socket until there is at least one full PeerMessage available.
    async fn recv_message(&mut self) -> Result<(PeerMessage, Instant), RecvError> {
        let (msg_length, first_byte_time) = self.read_msg_length().await?;

        while self.buf.remaining() < msg_length + 4 {
            self.do_read().await?;
        }

        self.buf.advance(4);
        let msg = PeerMessage::deserialize(Encoding::Proto, &self.buf[..msg_length]);
        self.buf.advance(msg_length);

        // make sure we can probably read the next message in one syscall next time
        let max_len_after_next_read = self.buf.chunk_mut().len() + self.buf.remaining();
        if max_len_after_next_read < 512 {
            self.buf.reserve(512 - max_len_after_next_read);
        }
        msg.map(|m| {
            tracing::debug!(target: "network", "{:?} received PeerMessage::{} len: {}", &self, &m, msg_length);
            (m, first_byte_time)
        })
        .map_err(|_| RecvError::Parse(msg_length))
    }
}

pub struct Listener {
    listener: tcp::Listener,
    secret_key: SecretKey,
    chain_id: String,
    genesis_hash: CryptoHash,
    head_height: BlockHeight,
    tracked_shards: Vec<ShardId>,
    archival: bool,
    recv_timeout: Duration,
}

impl Listener {
    pub async fn bind(
        addr: tcp::ListenerAddr,
        secret_key: SecretKey,
        chain_id: &str,
        genesis_hash: CryptoHash,
        head_height: BlockHeight,
        tracked_shards: Vec<ShardId>,
        archival: bool,
        recv_timeout: Duration,
    ) -> io::Result<Self> {
        Ok(Self {
            listener: addr.listener()?,
            secret_key,
            chain_id: chain_id.to_string(),
            genesis_hash,
            head_height,
            tracked_shards,
            archival,
            recv_timeout,
        })
    }

    pub async fn accept(&mut self) -> Result<Connection, ConnectError> {
        let stream = self.listener.accept().await.map_err(ConnectError::IO)?;
        Connection::on_accept(
            stream,
            self.secret_key.clone(),
            &self.chain_id,
            self.genesis_hash,
            self.head_height,
            self.tracked_shards.clone(),
            self.archival,
            self.recv_timeout,
        )
        .await
    }
}
