use actix::dev::{MessageResponse, ResponseChannel};
use actix::{Actor, Message};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::DateTime;
#[cfg(feature = "deepsize_feature")]
use deepsize::DeepSizeOf;
use near_crypto::{KeyType, PublicKey, SecretKey, Signature};
use near_primitives::block::{Approval, Block, BlockHeader, GenesisId};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::combine_hash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::{
    ChunkHash, PartialEncodedChunk, PartialEncodedChunkPart, PartialEncodedChunkV1,
    PartialEncodedChunkWithArcReceipts, ReceiptProof, ShardChunkHeader,
};
use near_primitives::syncing::{
    EpochSyncFinalizationResponse, EpochSyncResponse, ShardStateSyncResponse,
    ShardStateSyncResponseV1,
};
use near_primitives::time::{Clock, Utc};
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, SignedTransaction};
use near_primitives::types::{AccountId, BlockHeight, BlockReference, EpochId, ShardId};
use near_primitives::utils::{from_timestamp, to_timestamp};
use near_primitives::views::{FinalExecutionOutcomeView, QueryRequest, QueryResponse};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{Debug, Error, Formatter};
use std::hash::Hash;
use std::net::{AddrParseError, IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;
use strum::AsStaticStr;
use tokio::net::TcpStream;
use tracing::{error, warn};

/// Number of hops a message is allowed to travel before being dropped.
/// This is used to avoid infinite loop because of inconsistent view of the network
/// by different nodes.
pub const ROUTED_MESSAGE_TTL: u8 = 100;
/// On every message from peer don't update `last_time_received_message`
/// but wait some "small" timeout between updates to avoid a lot of messages between
/// Peer and PeerManager.
pub const UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE: Duration = Duration::from_secs(60);

/// Peer information.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: PeerId,
    pub addr: Option<SocketAddr>,
    pub account_id: Option<AccountId>,
}

#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for PeerInfo {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.id.deep_size_of_children(context) + self.account_id.deep_size_of_children(context)
    }
}

impl PeerInfo {
    /// Creates random peer info.
    pub fn new(id: PeerId, addr: SocketAddr) -> Self {
        PeerInfo { id, addr: Some(addr), account_id: None }
    }

    pub fn random() -> Self {
        PeerInfo { id: PeerId::random(), addr: None, account_id: None }
    }

    pub fn addr_port(&self) -> Option<u16> {
        self.addr.map(|addr| addr.port())
    }
}

// Note, `Display` automatically implements `ToString` which must be reciprocal to `FromStr`.
impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id)?;
        if let Some(addr) = &self.addr {
            write!(f, "@{}", addr)?;
        }
        if let Some(account_id) = &self.account_id {
            write!(f, "@{}", account_id)?;
        }
        Ok(())
    }
}

impl FromStr for PeerInfo {
    type Err = Box<dyn std::error::Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let chunks: Vec<&str> = s.split('@').collect();
        let addr;
        let account_id;
        if chunks.len() == 1 {
            addr = None;
            account_id = None;
        } else if chunks.len() == 2 {
            if let Ok(x) = chunks[1].parse::<SocketAddr>() {
                addr = Some(x);
                account_id = None;
            } else {
                addr = None;
                account_id = Some(chunks[1].parse().unwrap());
            }
        } else if chunks.len() == 3 {
            addr = Some(chunks[1].parse::<SocketAddr>()?);
            account_id = Some(chunks[2].parse().unwrap());
        } else {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid PeerInfo format: {:?}", chunks),
            )));
        }
        Ok(PeerInfo { id: PeerId::new(chunks[0].parse()?), addr, account_id })
    }
}

impl TryFrom<&str> for PeerInfo {
    type Error = Box<dyn std::error::Error>;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::from_str(s)
    }
}

/// Peer chain information.
/// TODO: Remove in next version
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct PeerChainInfo {
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Last known chain height of the peer.
    pub height: BlockHeight,
    /// Shards that the peer is tracking.
    pub tracked_shards: Vec<ShardId>,
}

/// Peer chain information.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct PeerChainInfoV2 {
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Last known chain height of the peer.
    pub height: BlockHeight,
    /// Shards that the peer is tracking.
    pub tracked_shards: Vec<ShardId>,
    /// Denote if a node is running in archival mode or not.
    pub archival: bool,
}

impl From<PeerChainInfo> for PeerChainInfoV2 {
    fn from(peer_chain_info: PeerChainInfo) -> Self {
        Self {
            genesis_id: peer_chain_info.genesis_id,
            height: peer_chain_info.height,
            tracked_shards: peer_chain_info.tracked_shards,
            archival: false,
        }
    }
}

/// Peer type.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize)]
pub enum PeerType {
    /// Inbound session
    Inbound,
    /// Outbound session
    Outbound,
}

/// Peer status.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PeerStatus {
    /// Waiting for handshake.
    Connecting,
    /// Ready to go.
    Ready,
    /// Banned, should shutdown this peer.
    Banned(ReasonForBan),
}

/// Account route description
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct AnnounceAccountRoute {
    pub peer_id: PeerId,
    pub hash: CryptoHash,
    pub signature: Signature,
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct Ping {
    pub nonce: u64,
    pub source: PeerId,
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct Pong {
    pub nonce: u64,
    pub source: PeerId,
}

// TODO(#1313): Use Box
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    strum::AsStaticStr,
    strum::EnumVariantNames,
)]
#[allow(clippy::large_enum_variant)]
pub enum RoutedMessageBody {
    BlockApproval(Approval),
    ForwardTx(SignedTransaction),

    TxStatusRequest(AccountId, CryptoHash),
    TxStatusResponse(FinalExecutionOutcomeView),
    QueryRequest {
        query_id: String,
        block_reference: BlockReference,
        request: QueryRequest,
    },
    QueryResponse {
        query_id: String,
        response: Result<QueryResponse, String>,
    },
    ReceiptOutcomeRequest(CryptoHash),
    /// Not used, but needed to preserve backward compatibility.
    Unused,
    StateRequestHeader(ShardId, CryptoHash),
    StateRequestPart(ShardId, CryptoHash, u64),
    StateResponse(StateResponseInfoV1),
    PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg),
    PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg),
    PartialEncodedChunk(PartialEncodedChunkV1),
    /// Ping/Pong used for testing networking and routing.
    Ping(Ping),
    Pong(Pong),
    VersionedPartialEncodedChunk(PartialEncodedChunk),
    VersionedStateResponse(StateResponseInfo),
    PartialEncodedChunkForward(PartialEncodedChunkForwardMsg),
}

impl From<PartialEncodedChunkWithArcReceipts> for RoutedMessageBody {
    fn from(pec: PartialEncodedChunkWithArcReceipts) -> Self {
        if let ShardChunkHeader::V1(legacy_header) = pec.header {
            Self::PartialEncodedChunk(PartialEncodedChunkV1 {
                header: legacy_header,
                parts: pec.parts,
                receipts: pec.receipts.into_iter().map(|r| ReceiptProof::clone(&r)).collect(),
            })
        } else {
            Self::VersionedPartialEncodedChunk(pec.into())
        }
    }
}

impl Debug for RoutedMessageBody {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            RoutedMessageBody::BlockApproval(approval) => write!(
                f,
                "Approval({}, {}, {:?})",
                approval.target_height, approval.account_id, approval.inner
            ),
            RoutedMessageBody::ForwardTx(tx) => write!(f, "tx {}", tx.get_hash()),
            RoutedMessageBody::TxStatusRequest(account_id, hash) => {
                write!(f, "TxStatusRequest({}, {})", account_id, hash)
            }
            RoutedMessageBody::TxStatusResponse(response) => {
                write!(f, "TxStatusResponse({})", response.transaction.hash)
            }
            RoutedMessageBody::QueryRequest { .. } => write!(f, "QueryRequest"),
            RoutedMessageBody::QueryResponse { .. } => write!(f, "QueryResponse"),
            RoutedMessageBody::ReceiptOutcomeRequest(hash) => write!(f, "ReceiptRequest({})", hash),
            RoutedMessageBody::StateRequestHeader(shard_id, sync_hash) => {
                write!(f, "StateRequestHeader({}, {})", shard_id, sync_hash)
            }
            RoutedMessageBody::StateRequestPart(shard_id, sync_hash, part_id) => {
                write!(f, "StateRequestPart({}, {}, {})", shard_id, sync_hash, part_id)
            }
            RoutedMessageBody::StateResponse(response) => {
                write!(f, "StateResponse({}, {})", response.shard_id, response.sync_hash)
            }
            RoutedMessageBody::PartialEncodedChunkRequest(request) => {
                write!(f, "PartialChunkRequest({:?}, {:?})", request.chunk_hash, request.part_ords)
            }
            RoutedMessageBody::PartialEncodedChunkResponse(response) => write!(
                f,
                "PartialChunkResponse({:?}, {:?})",
                response.chunk_hash,
                response.parts.iter().map(|p| p.part_ord).collect::<Vec<_>>()
            ),
            RoutedMessageBody::PartialEncodedChunk(chunk) => {
                write!(f, "PartialChunk({:?})", chunk.header.hash)
            }
            RoutedMessageBody::VersionedPartialEncodedChunk(_) => {
                write!(f, "VersionedPartialChunk(?)")
            }
            RoutedMessageBody::VersionedStateResponse(response) => write!(
                f,
                "VersionedStateResponse({}, {})",
                response.shard_id(),
                response.sync_hash()
            ),
            RoutedMessageBody::PartialEncodedChunkForward(forward) => write!(
                f,
                "PartialChunkForward({:?}, {:?})",
                forward.chunk_hash,
                forward.parts.iter().map(|p| p.part_ord).collect::<Vec<_>>(),
            ),
            RoutedMessageBody::Ping(_) => write!(f, "Ping"),
            RoutedMessageBody::Pong(_) => write!(f, "Pong"),
            RoutedMessageBody::Unused => write!(f, "Unused"),
        }
    }
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, Hash)]
pub enum PeerIdOrHash {
    PeerId(PeerId),
    Hash(CryptoHash),
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Hash)]
// Defines the destination for a network request.
// The request should be sent either to the `account_id` as a routed message, or directly to
// any peer that tracks the shard.
// If `prefer_peer` is `true`, should be sent to the peer, unless no peer tracks the shard, in which
// case fall back to sending to the account.
// Otherwise, send to the account, unless we do not know the route, in which case send to the peer.
pub struct AccountIdOrPeerTrackingShard {
    pub shard_id: ShardId,
    pub only_archival: bool,
    pub account_id: Option<AccountId>,
    pub prefer_peer: bool,
}

impl AccountIdOrPeerTrackingShard {
    pub fn from_account(shard_id: ShardId, account_id: AccountId) -> Self {
        Self { shard_id, only_archival: false, account_id: Some(account_id), prefer_peer: false }
    }
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Hash)]
pub enum AccountOrPeerIdOrHash {
    AccountId(AccountId),
    PeerId(PeerId),
    Hash(CryptoHash),
}

impl AccountOrPeerIdOrHash {
    fn peer_id_or_hash(&self) -> Option<PeerIdOrHash> {
        match self {
            AccountOrPeerIdOrHash::AccountId(_) => None,
            AccountOrPeerIdOrHash::PeerId(peer_id) => Some(PeerIdOrHash::PeerId(peer_id.clone())),
            AccountOrPeerIdOrHash::Hash(hash) => Some(PeerIdOrHash::Hash(hash.clone())),
        }
    }
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct RawRoutedMessage {
    pub target: AccountOrPeerIdOrHash,
    pub body: RoutedMessageBody,
}

impl RawRoutedMessage {
    /// Add signature to the message.
    /// Panics if the target is an AccountId instead of a PeerId.
    pub fn sign(
        self,
        author: PeerId,
        secret_key: &SecretKey,
        routed_message_ttl: u8,
    ) -> RoutedMessage {
        let target = self.target.peer_id_or_hash().unwrap();
        let hash = RoutedMessage::build_hash(&target, &author, &self.body);
        let signature = secret_key.sign(hash.as_ref());
        RoutedMessage { target, author, signature, ttl: routed_message_ttl, body: self.body }
    }
}

#[derive(BorshSerialize, PartialEq, Eq, Clone, Debug)]
pub struct RoutedMessageNoSignature<'a> {
    target: &'a PeerIdOrHash,
    author: &'a PeerId,
    body: &'a RoutedMessageBody,
}

/// RoutedMessage represent a package that will travel the network towards a specific peer id.
/// It contains the peer_id and signature from the original sender. Every intermediate peer in the
/// route must verify that this signature is valid otherwise previous sender of this package should
/// be banned. If the final receiver of this package finds that the body is invalid the original
/// sender of the package should be banned instead.
/// If target is hash, it is a message that should be routed back using the same path used to route
/// the request in first place. It is the hash of the request message.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct RoutedMessage {
    /// Peer id which is directed this message.
    /// If `target` is hash, this a message should be routed back.
    pub target: PeerIdOrHash,
    /// Original sender of this message
    pub author: PeerId,
    /// Signature from the author of the message. If this signature is invalid we should ban
    /// last sender of this message. If the message is invalid we should ben author of the message.
    pub signature: Signature,
    /// Time to live for this message. After passing through some hop this number should be
    /// decreased by 1. If this number is 0, drop this message.
    pub ttl: u8,
    /// Message
    pub body: RoutedMessageBody,
}

impl RoutedMessage {
    pub fn build_hash(
        target: &PeerIdOrHash,
        source: &PeerId,
        body: &RoutedMessageBody,
    ) -> CryptoHash {
        CryptoHash::hash_borsh(&RoutedMessageNoSignature { target, author: source, body })
    }

    pub fn hash(&self) -> CryptoHash {
        RoutedMessage::build_hash(&self.target, &self.author, &self.body)
    }

    pub fn verify(&self) -> bool {
        self.signature.verify(self.hash().as_ref(), &self.author.public_key())
    }

    pub fn expect_response(&self) -> bool {
        match self.body {
            RoutedMessageBody::Ping(_)
            | RoutedMessageBody::TxStatusRequest(_, _)
            | RoutedMessageBody::StateRequestHeader(_, _)
            | RoutedMessageBody::StateRequestPart(_, _, _)
            | RoutedMessageBody::PartialEncodedChunkRequest(_)
            | RoutedMessageBody::QueryRequest { .. }
            | RoutedMessageBody::ReceiptOutcomeRequest(_) => true,
            _ => false,
        }
    }

    /// Return true if ttl is positive after decreasing ttl by one, false otherwise.
    pub fn decrease_ttl(&mut self) -> bool {
        self.ttl = self.ttl.saturating_sub(1);
        self.ttl > 0
    }
}

/// Routed Message wrapped with previous sender of the message.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Clone, Debug)]
pub struct RoutedMessageFrom {
    /// Routed messages.
    pub msg: RoutedMessage,
    /// Previous hop in the route. Used for messages that needs routing back.
    pub from: PeerId,
}

impl Message for RoutedMessageFrom {
    type Result = bool;
}

#[derive(Debug, Clone)]
pub enum BlockedPorts {
    All,
    Some(HashSet<u16>),
}

/// Configuration for the peer-to-peer manager.
#[derive(Clone)]
pub struct NetworkConfig {
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
    pub account_id: Option<AccountId>,
    pub addr: Option<SocketAddr>,
    pub boot_nodes: Vec<PeerInfo>,
    pub handshake_timeout: Duration,
    pub reconnect_delay: Duration,
    pub bootstrap_peers_period: Duration,
    /// Maximum number of active peers. Hard limit.
    pub max_num_peers: u32,
    /// Minimum outbound connections a peer should have to avoid eclipse attacks.
    pub minimum_outbound_peers: u32,
    /// Lower bound of the ideal number of connections.
    pub ideal_connections_lo: u32,
    /// Upper bound of the ideal number of connections.
    pub ideal_connections_hi: u32,
    /// Peers which last message is was within this period of time are considered active recent peers.
    pub peer_recent_time_window: Duration,
    /// Number of peers to keep while removing a connection.
    /// Used to avoid disconnecting from peers we have been connected since long time.
    pub safe_set_size: u32,
    /// Lower bound of the number of connections to archival peers to keep
    /// if we are an archival node.
    pub archival_peer_connections_lower_bound: u32,
    /// Duration of the ban for misbehaving peers.
    pub ban_window: Duration,
    /// Remove expired peers.
    pub peer_expiration_duration: Duration,
    /// Maximum number of peer addresses we should ever send on PeersRequest.
    pub max_send_peers: u32,
    /// Duration for checking on stats from the peers.
    pub peer_stats_period: Duration,
    /// Time to persist Accounts Id in the router without removing them.
    pub ttl_account_id_router: Duration,
    /// Number of hops a message is allowed to travel before being dropped.
    /// This is used to avoid infinite loop because of inconsistent view of the network
    /// by different nodes.
    pub routed_message_ttl: u8,
    /// Maximum number of routes that we should keep track for each Account id in the Routing Table.
    pub max_routes_to_store: usize,
    /// Height horizon for highest height peers
    /// For example if one peer is 1 height away from max height peer,
    /// we still want to use the rest to query for state/headers/blocks.
    pub highest_peer_horizon: u64,
    /// Period between pushing network info to client
    pub push_info_period: Duration,
    /// Peers on blacklist by IP:Port.
    /// Nodes will not accept or try to establish connection to such peers.
    pub blacklist: HashMap<IpAddr, BlockedPorts>,
    /// Flag to disable outbound connections. When this flag is active, nodes will not try to
    /// establish connection with other nodes, but will accept incoming connection if other requirements
    /// are satisfied.
    /// This flag should be ALWAYS FALSE. Only set to true for testing purposes.
    pub outbound_disabled: bool,
    /// Not clear old data, set `true` for archive nodes.
    pub archive: bool,
}

impl NetworkConfig {
    /// Returns network config with given seed used for peer id.
    pub fn from_seed(seed: &str, port: u16) -> Self {
        let secret_key = SecretKey::from_seed(KeyType::ED25519, seed);
        let public_key = secret_key.public_key();
        NetworkConfig {
            public_key,
            secret_key,
            account_id: Some(seed.parse().unwrap()),
            addr: Some(format!("0.0.0.0:{}", port).parse().unwrap()),
            boot_nodes: vec![],
            handshake_timeout: Duration::from_secs(60),
            reconnect_delay: Duration::from_secs(60),
            bootstrap_peers_period: Duration::from_millis(100),
            max_num_peers: 10,
            minimum_outbound_peers: 5,
            ideal_connections_lo: 30,
            ideal_connections_hi: 35,
            peer_recent_time_window: Duration::from_secs(600),
            safe_set_size: 20,
            archival_peer_connections_lower_bound: 10,
            ban_window: Duration::from_secs(1),
            peer_expiration_duration: Duration::from_secs(60 * 60),
            max_send_peers: 512,
            peer_stats_period: Duration::from_secs(5),
            ttl_account_id_router: Duration::from_secs(60 * 60),
            routed_message_ttl: ROUTED_MESSAGE_TTL,
            max_routes_to_store: 1,
            highest_peer_horizon: 5,
            push_info_period: Duration::from_millis(100),
            blacklist: HashMap::new(),
            outbound_disabled: false,
            archive: false,
        }
    }

    pub fn verify(&self) {
        if self.ideal_connections_lo + 1 >= self.ideal_connections_hi {
            error!(target: "network",
            "Invalid ideal_connections values. lo({}) > hi({}).",
            self.ideal_connections_lo, self.ideal_connections_hi);
        }

        if self.ideal_connections_hi >= self.max_num_peers {
            error!(target: "network",
                "max_num_peers({}) is below ideal_connections_hi({}) which may lead to connection saturation and declining new connections.",
                self.max_num_peers, self.ideal_connections_hi
            );
        }

        if self.outbound_disabled {
            warn!(target: "network", "Outbound connections are disabled.");
        }

        if self.safe_set_size <= self.minimum_outbound_peers {
            error!(target: "network",
                "safe_set_size({}) must be larger than minimum_outbound_peers({}).",
                self.safe_set_size,
                self.minimum_outbound_peers
            );
        }

        if UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE * 2 > self.peer_recent_time_window {
            error!(
                target: "network",
                "Very short peer_recent_time_window({}). it should be at least twice update_interval_last_time_received_message({}).",
                self.peer_recent_time_window.as_secs(), UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE.as_secs()
            );
        }
    }
}

/// Used to match a socket addr by IP:Port or only by IP
#[derive(Clone, Debug)]
pub enum PatternAddr {
    Ip(IpAddr),
    IpPort(SocketAddr),
}

impl PatternAddr {
    pub fn contains(&self, addr: &SocketAddr) -> bool {
        match self {
            PatternAddr::Ip(pattern) => &addr.ip() == pattern,
            PatternAddr::IpPort(pattern) => addr == pattern,
        }
    }
}

impl FromStr for PatternAddr {
    type Err = AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(pattern) = s.parse::<IpAddr>() {
            return Ok(PatternAddr::Ip(pattern));
        }

        s.parse::<SocketAddr>().map(PatternAddr::IpPort)
    }
}

/// Status of the known peers.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Eq, PartialEq, Debug, Clone)]
pub enum KnownPeerStatus {
    Unknown,
    NotConnected,
    Connected,
    Banned(ReasonForBan, u64),
}

impl KnownPeerStatus {
    pub fn is_banned(&self) -> bool {
        match self {
            KnownPeerStatus::Banned(_, _) => true,
            _ => false,
        }
    }
}

/// Information node stores about known peers.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct KnownPeerState {
    pub peer_info: PeerInfo,
    pub status: KnownPeerStatus,
    pub first_seen: u64,
    pub last_seen: u64,
}

impl KnownPeerState {
    pub fn new(peer_info: PeerInfo) -> Self {
        KnownPeerState {
            peer_info,
            status: KnownPeerStatus::Unknown,
            first_seen: to_timestamp(Clock::utc()),
            last_seen: to_timestamp(Clock::utc()),
        }
    }

    pub fn first_seen(&self) -> DateTime<Utc> {
        from_timestamp(self.first_seen)
    }

    pub fn last_seen(&self) -> DateTime<Utc> {
        from_timestamp(self.last_seen)
    }
}

impl TryFrom<Vec<u8>> for KnownPeerState {
    type Error = Box<dyn std::error::Error>;

    fn try_from(bytes: Vec<u8>) -> Result<KnownPeerState, Self::Error> {
        KnownPeerState::try_from_slice(&bytes).map_err(|err| err.into())
    }
}

/// Actor message that holds the TCP stream from an inbound TCP connection
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct InboundTcpConnect {
    /// Tcp stream of the inbound connections
    pub stream: TcpStream,
}

#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for InboundTcpConnect {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        0
    }
}

impl InboundTcpConnect {
    /// Method to create a new InboundTcpConnect message from a TCP stream
    pub fn new(stream: TcpStream) -> InboundTcpConnect {
        InboundTcpConnect { stream }
    }
}

/// Actor message to request the creation of an outbound TCP connection to a peer.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct OutboundTcpConnect {
    /// Peer information of the outbound connection
    pub peer_info: PeerInfo,
}

/// Unregister message from Peer to PeerManager.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Message)]
#[rtype(result = "()")]
pub struct Unregister {
    pub peer_id: PeerId,
    pub peer_type: PeerType,
    pub remove_from_peer_store: bool,
}

pub struct PeerList {
    pub peers: Vec<PeerInfo>,
}

/// Requesting peers from peer manager to communicate to a peer.
pub struct PeersRequest {}

impl Message for PeersRequest {
    type Result = PeerList;
}

/// Received new peers from another peer.
#[derive(Message)]
#[rtype(result = "()")]
pub struct PeersResponse {
    pub peers: Vec<PeerInfo>,
}

impl<A, M> MessageResponse<A, M> for PeerList
where
    A: Actor,
    M: Message<Result = PeerList>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

/// Ban reason.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq, Copy)]
pub enum ReasonForBan {
    None = 0,
    BadBlock = 1,
    BadBlockHeader = 2,
    HeightFraud = 3,
    BadHandshake = 4,
    BadBlockApproval = 5,
    Abusive = 6,
    InvalidSignature = 7,
    InvalidPeerId = 8,
    InvalidHash = 9,
    InvalidEdge = 10,
    EpochSyncNoResponse = 11,
    EpochSyncInvalidResponse = 12,
    EpochSyncInvalidFinalizationResponse = 13,
}

/// Banning signal sent from Peer instance to PeerManager
/// just before Peer instance is stopped.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Ban {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

/// Messages from PeerManager to Peer
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum PeerManagerRequest {
    BanPeer(ReasonForBan),
    UnregisterPeer,
}

/// Messages from Peer to PeerManager
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum PeerRequest {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KnownProducer {
    pub account_id: AccountId,
    pub addr: Option<SocketAddr>,
    pub peer_id: PeerId,
}

#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for KnownProducer {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.account_id.deep_size_of_children(context) + self.peer_id.deep_size_of_children(context)
    }
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateResponseInfoV1 {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub state_response: ShardStateSyncResponseV1,
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateResponseInfoV2 {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub state_response: ShardStateSyncResponse,
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub enum StateResponseInfo {
    V1(StateResponseInfoV1),
    V2(StateResponseInfoV2),
}

impl StateResponseInfo {
    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::V1(info) => info.shard_id,
            Self::V2(info) => info.shard_id,
        }
    }

    pub fn sync_hash(&self) -> CryptoHash {
        match self {
            Self::V1(info) => info.sync_hash,
            Self::V2(info) => info.sync_hash,
        }
    }

    pub fn take_state_response(self) -> ShardStateSyncResponse {
        match self {
            Self::V1(info) => ShardStateSyncResponse::V1(info.state_response),
            Self::V2(info) => info.state_response,
        }
    }
}

#[cfg(feature = "test_features")]
#[derive(Debug)]
pub enum NetworkAdversarialMessage {
    AdvProduceBlocks(u64, bool),
    AdvSwitchToHeight(u64),
    AdvDisableHeaderSync,
    AdvDisableDoomslug,
    AdvGetSavedBlocks,
    AdvCheckStorageConsistency,
    AdvSetSyncInfo(u64),
}

#[cfg(feature = "sandbox")]
#[derive(Debug)]
pub enum NetworkSandboxMessage {
    SandboxPatchState(Vec<near_primitives::state_record::StateRecord>),
    SandboxPatchStateStatus,
}

#[derive(AsStaticStr)]
pub enum NetworkViewClientMessages {
    #[cfg(feature = "test_features")]
    Adversarial(NetworkAdversarialMessage),

    /// Transaction status query
    TxStatus { tx_hash: CryptoHash, signer_account_id: AccountId },
    /// Transaction status response
    TxStatusResponse(Box<FinalExecutionOutcomeView>),
    /// Request for receipt outcome
    ReceiptOutcomeRequest(CryptoHash),
    /// Receipt outcome response
    ReceiptOutcomeResponse(Box<ExecutionOutcomeWithIdAndProof>),
    /// Request a block.
    BlockRequest(CryptoHash),
    /// Request headers.
    BlockHeadersRequest(Vec<CryptoHash>),
    /// State request header.
    StateRequestHeader { shard_id: ShardId, sync_hash: CryptoHash },
    /// State request part.
    StateRequestPart { shard_id: ShardId, sync_hash: CryptoHash, part_id: u64 },
    /// A request for a light client info during Epoch Sync
    EpochSyncRequest { epoch_id: EpochId },
    /// A request for headers and proofs during Epoch Sync
    EpochSyncFinalizationRequest { epoch_id: EpochId },
    /// Get Chain information from Client.
    GetChainInfo,
    /// Account announcements that needs to be validated before being processed.
    /// They are paired with last epoch id known to this announcement, in order to accept only
    /// newer announcements.
    AnnounceAccount(Vec<(AnnounceAccount, Option<EpochId>)>),
}

#[derive(Debug)]
pub enum NetworkViewClientResponses {
    /// Transaction execution outcome
    TxStatus(Box<FinalExecutionOutcomeView>),
    /// Response to general queries
    QueryResponse { query_id: String, response: Result<QueryResponse, String> },
    /// Receipt outcome response
    ReceiptOutcomeResponse(Box<ExecutionOutcomeWithIdAndProof>),
    /// Block response.
    Block(Box<Block>),
    /// Headers response.
    BlockHeaders(Vec<BlockHeader>),
    /// Chain information.
    ChainInfo {
        genesis_id: GenesisId,
        height: BlockHeight,
        tracked_shards: Vec<ShardId>,
        archival: bool,
    },
    /// Response to state request.
    StateResponse(Box<StateResponseInfo>),
    /// Valid announce accounts.
    AnnounceAccount(Vec<AnnounceAccount>),
    /// A response to a request for a light client block during Epoch Sync
    EpochSyncResponse(EpochSyncResponse),
    /// A response to a request for headers and proofs during Epoch Sync
    EpochSyncFinalizationResponse(EpochSyncFinalizationResponse),
    /// Ban peer for malicious behavior.
    Ban { ban_reason: ReasonForBan },
    /// Response not needed
    NoResponse,
}

impl<A, M> MessageResponse<A, M> for NetworkViewClientResponses
where
    A: Actor,
    M: Message<Result = NetworkViewClientResponses>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for NetworkViewClientMessages {
    type Result = NetworkViewClientResponses;
}

/// Peer stats query.
pub struct QueryPeerStats {}

/// Peer stats result
#[derive(Debug)]
pub struct PeerStatsResult {
    /// Chain info.
    pub chain_info: PeerChainInfoV2,
    /// Number of bytes we've received from the peer.
    pub received_bytes_per_sec: u64,
    /// Number of bytes we've sent to the peer.
    pub sent_bytes_per_sec: u64,
    /// Returns if this peer is abusive and should be banned.
    pub is_abusive: bool,
    /// Counts of incoming/outgoing messages from given peer.
    pub message_counts: (u64, u64),
}

impl<A, M> MessageResponse<A, M> for PeerStatsResult
where
    A: Actor,
    M: Message<Result = PeerStatsResult>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for QueryPeerStats {
    type Result = PeerStatsResult;
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct PartialEncodedChunkRequestMsg {
    pub chunk_hash: ChunkHash,
    pub part_ords: Vec<u64>,
    pub tracking_shards: HashSet<ShardId>,
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct PartialEncodedChunkResponseMsg {
    pub chunk_hash: ChunkHash,
    pub parts: Vec<PartialEncodedChunkPart>,
    pub receipts: Vec<ReceiptProof>,
}

/// Message for chunk part owners to forward their parts to validators tracking that shard.
/// This reduces the number of requests a node tracking a shard needs to send to obtain enough
/// parts to reconstruct the message (in the best case no such requests are needed).
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct PartialEncodedChunkForwardMsg {
    pub chunk_hash: ChunkHash,
    pub inner_header_hash: CryptoHash,
    pub merkle_root: CryptoHash,
    pub signature: Signature,
    pub prev_block_hash: CryptoHash,
    pub height_created: BlockHeight,
    pub shard_id: ShardId,
    pub parts: Vec<PartialEncodedChunkPart>,
}

impl PartialEncodedChunkForwardMsg {
    pub fn from_header_and_parts(
        header: &ShardChunkHeader,
        parts: Vec<PartialEncodedChunkPart>,
    ) -> Self {
        Self {
            chunk_hash: header.chunk_hash(),
            inner_header_hash: header.inner_header_hash(),
            merkle_root: header.encoded_merkle_root(),
            signature: header.signature().clone(),
            prev_block_hash: header.prev_block_hash(),
            height_created: header.height_created(),
            shard_id: header.shard_id(),
            parts,
        }
    }

    pub fn is_valid_hash(&self) -> bool {
        let correct_hash = combine_hash(self.inner_header_hash, self.merkle_root);
        ChunkHash(correct_hash) == self.chunk_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: this has it's counterpart in `near_network::types::tests`
    const ALLOWED_SIZE: usize = 1 << 20;
    const NOTIFY_SIZE: usize = 1024;

    macro_rules! assert_size {
        ($type:ident) => {
            let struct_size = std::mem::size_of::<$type>();
            if struct_size >= NOTIFY_SIZE {
                println!("The size of {} is {}", stringify!($type), struct_size);
            }
            assert!(struct_size <= ALLOWED_SIZE);
        };
    }

    #[test]
    fn test_enum_size() {
        assert_size!(PeerType);
        assert_size!(PeerStatus);
        assert_size!(RoutedMessageBody);
        assert_size!(PeerIdOrHash);
        assert_size!(KnownPeerStatus);
        assert_size!(ReasonForBan);
        assert_size!(PeerManagerRequest);
    }

    #[test]
    fn test_struct_size() {
        assert_size!(PeerInfo);
        assert_size!(PeerChainInfoV2);
        assert_size!(AnnounceAccountRoute);
        assert_size!(AnnounceAccount);
        assert_size!(Ping);
        assert_size!(Pong);
        assert_size!(RawRoutedMessage);
        assert_size!(RoutedMessageNoSignature);
        assert_size!(RoutedMessage);
        assert_size!(RoutedMessageFrom);
        assert_size!(NetworkConfig);
        assert_size!(KnownPeerState);
        assert_size!(InboundTcpConnect);
        assert_size!(OutboundTcpConnect);
        assert_size!(Unregister);
        assert_size!(PeerList);
        assert_size!(PeersRequest);
        assert_size!(PeersResponse);
        assert_size!(Ban);
        assert_size!(StateResponseInfoV1);
        assert_size!(QueryPeerStats);
        assert_size!(PartialEncodedChunkRequestMsg);
    }

    #[test]
    fn routed_message_body_compatibility_smoke_test() {
        #[track_caller]
        fn check(msg: RoutedMessageBody, expected: &[u8]) {
            let actual = msg.try_to_vec().unwrap();
            assert_eq!(actual.as_slice(), expected);
        }

        check(
            RoutedMessageBody::TxStatusRequest("test_x".parse().unwrap(), CryptoHash([42; 32])),
            &[
                2, 6, 0, 0, 0, 116, 101, 115, 116, 95, 120, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                42,
            ],
        );

        check(
            RoutedMessageBody::VersionedStateResponse(StateResponseInfo::V1(StateResponseInfoV1 {
                shard_id: 62,
                sync_hash: CryptoHash([92; 32]),
                state_response: ShardStateSyncResponseV1 { header: None, part: None },
            })),
            &[
                17, 0, 62, 0, 0, 0, 0, 0, 0, 0, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92,
                92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 0, 0,
            ],
        );
    }
}
