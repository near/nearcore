use std::collections::{HashMap, HashSet};
use std::convert::{From, Into, TryFrom, TryInto};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use actix::dev::{MessageResponse, ResponseChannel};
use actix::{Actor, Addr, Message};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use tokio::net::TcpStream;

use near_chain::types::ShardStateSyncResponse;
use near_chain::{Block, BlockApproval, BlockHeader, Weight};
use near_crypto::{PublicKey, SecretKey, Signature};
use near_primitives::block::GenesisId;
use near_primitives::challenge::Challenge;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::{hash, CryptoHash};
pub use near_primitives::sharding::ChunkPartMsg;
use near_primitives::sharding::{ChunkHash, ChunkOnePart};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockIndex, EpochId, Range, ShardId};
use near_primitives::utils::{from_timestamp, to_timestamp};
use near_primitives::views::FinalExecutionOutcomeView;

use crate::peer::Peer;
use crate::routing::{Edge, EdgeInfo, RoutingTableInfo};

/// Current latest version of the protocol
pub const PROTOCOL_VERSION: u32 = 4;

/// Peer id is the public key.
#[derive(BorshSerialize, BorshDeserialize, Clone, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PeerId(PublicKey);

impl PeerId {
    pub fn new(key: PublicKey) -> Self {
        Self(key)
    }

    pub fn public_key(&self) -> PublicKey {
        self.0.clone()
    }
}

impl From<PeerId> for Vec<u8> {
    fn from(peer_id: PeerId) -> Vec<u8> {
        peer_id.0.try_to_vec().unwrap()
    }
}

impl From<PublicKey> for PeerId {
    fn from(public_key: PublicKey) -> PeerId {
        PeerId(public_key)
    }
}

impl TryFrom<Vec<u8>> for PeerId {
    type Error = Box<dyn std::error::Error>;

    fn try_from(bytes: Vec<u8>) -> Result<PeerId, Self::Error> {
        Ok(PeerId(PublicKey::try_from_slice(&bytes)?))
    }
}

impl Hash for PeerId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.0.try_to_vec().unwrap());
    }
}

impl PartialEq for PeerId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Peer information.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: PeerId,
    pub addr: Option<SocketAddr>,
    pub account_id: Option<AccountId>,
}

impl PeerInfo {
    pub fn addr_port(&self) -> Option<u16> {
        self.addr.map(|addr| addr.port())
    }
}

impl PeerInfo {
    pub fn new(id: PeerId, addr: SocketAddr) -> Self {
        PeerInfo { id, addr: Some(addr), account_id: None }
    }
}

// Note, `Display` automatically implements `ToString` which must be reciprocal to `FromStr`.
impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
                account_id = Some(chunks[1].to_string());
            }
        } else if chunks.len() == 3 {
            addr = Some(chunks[1].parse::<SocketAddr>()?);
            account_id = Some(chunks[2].to_string());
        } else {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid PeerInfo format: {:?}", chunks),
            )));
        }
        Ok(PeerInfo { id: PeerId(chunks[0].try_into()?), addr, account_id })
    }
}

impl TryFrom<&str> for PeerInfo {
    type Error = Box<dyn std::error::Error>;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::from_str(s)
    }
}

/// Peer chain information.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct PeerChainInfo {
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Last known chain height of the peer.
    pub height: BlockIndex,
    /// Last known chain weight of the peer.
    pub total_weight: Weight,
}

/// Peer type.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PeerType {
    /// Inbound session
    Inbound,
    /// Outbound session
    Outbound,
}

/// Peer status.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PeerStatus {
    /// Waiting for handshake.
    Connecting,
    /// Ready to go.
    Ready,
    /// Banned, should shutdown this peer.
    Banned(ReasonForBan),
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct Handshake {
    /// Protocol version.
    pub version: u32,
    /// Sender's peer id.
    pub peer_id: PeerId,
    /// Sender's listening addr.
    pub listen_port: Option<u16>,
    /// Peer's chain information.
    pub chain_info: PeerChainInfo,
    /// Info for new edge.
    pub edge_info: EdgeInfo,
}

impl Handshake {
    pub fn new(
        peer_id: PeerId,
        listen_port: Option<u16>,
        chain_info: PeerChainInfo,
        edge_info: EdgeInfo,
    ) -> Self {
        Handshake { version: PROTOCOL_VERSION, peer_id, listen_port, chain_info, edge_info }
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct AnnounceAccountRouteHeader {
    pub account_id: AccountId,
    pub peer_id: PeerId,
    pub epoch_id: EpochId,
}

/// Account route description
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct AnnounceAccountRoute {
    pub peer_id: PeerId,
    pub hash: CryptoHash,
    pub signature: Signature,
}

/// Account announcement information
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct AnnounceAccount {
    /// AccountId to be announced.
    pub account_id: AccountId,
    /// PeerId from the owner of the account.
    pub peer_id: PeerId,
    /// This announcement is only valid for this `epoch`.
    pub epoch_id: EpochId,
    /// Signature using AccountId associated secret key.
    pub signature: Signature,
}

impl AnnounceAccount {
    pub fn build_header_hash(
        account_id: &AccountId,
        peer_id: &PeerId,
        epoch_id: &EpochId,
    ) -> CryptoHash {
        let header = AnnounceAccountRouteHeader {
            account_id: account_id.clone(),
            peer_id: peer_id.clone(),
            epoch_id: epoch_id.clone(),
        };
        hash(&header.try_to_vec().unwrap())
    }

    pub fn hash(&self) -> CryptoHash {
        AnnounceAccount::build_header_hash(&self.account_id, &self.peer_id, &self.epoch_id)
    }
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub enum HandshakeFailureReason {
    ProtocolVersionMismatch(u32),
    GenesisMismatch(GenesisId),
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct Ping {
    pub nonce: usize,
    pub source: PeerId,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct Pong {
    pub nonce: usize,
    pub source: PeerId,
}

// TODO(#1313): Use Box
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum RoutedMessageBody {
    BlockApproval(AccountId, CryptoHash, Signature),
    ForwardTx(SignedTransaction),
    TxStatusRequest(AccountId, CryptoHash),
    TxStatusResponse(FinalExecutionOutcomeView),
    StateRequest(ShardId, CryptoHash, bool, Vec<Range>),
    ChunkPartRequest(ChunkPartRequestMsg),
    ChunkOnePartRequest(ChunkOnePartRequestMsg),
    ChunkOnePart(ChunkOnePart),
    /// Ping/Pong used for testing networking and routing.
    Ping(Ping),
    Pong(Pong),
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub enum PeerIdOrHash {
    PeerId(PeerId),
    Hash(CryptoHash),
}

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

#[derive(Message)]
pub struct RawRoutedMessage {
    pub target: AccountOrPeerIdOrHash,
    pub body: RoutedMessageBody,
}

impl RawRoutedMessage {
    /// Add signature to the message.
    /// Panics if the target is an AccountId instead of a PeerId.
    pub fn sign(self, author: PeerId, secret_key: &SecretKey) -> RoutedMessage {
        let target = self.target.peer_id_or_hash().unwrap();
        let hash = RoutedMessage::build_hash(target.clone(), author.clone(), self.body.clone());
        let signature = secret_key.sign(hash.as_ref());
        RoutedMessage { target, author, signature, body: self.body }
    }
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct RoutedMessageNoSignature {
    target: PeerIdOrHash,
    author: PeerId,
    body: RoutedMessageBody,
}

// TODO(MarX, #1367): Add TTL for routed message to avoid infinite loops
/// RoutedMessage represent a package that will travel the network towards a specific peer id.
/// It contains the peer_id and signature from the original sender. Every intermediate peer in the
/// route must verify that this signature is valid otherwise previous sender of this package should
/// be banned. If the final receiver of this package finds that the body is invalid the original
/// sender of the package should be banned instead.
/// If target is hash, it is a message that should be routed back using the same path used to route
/// the request in first place. It is the hash of the request message.
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
    /// Message
    pub body: RoutedMessageBody,
}

impl RoutedMessage {
    pub fn build_hash(target: PeerIdOrHash, source: PeerId, body: RoutedMessageBody) -> CryptoHash {
        hash(
            &RoutedMessageNoSignature { target, author: source, body }
                .try_to_vec()
                .expect("Failed to serialize"),
        )
    }

    pub fn hash(&self) -> CryptoHash {
        RoutedMessage::build_hash(self.target.clone(), self.author.clone(), self.body.clone())
    }

    pub fn verify(&self) -> bool {
        self.signature.verify(self.hash().as_ref(), &self.author.public_key())
    }

    pub fn expect_response(&self) -> bool {
        match self.body {
            RoutedMessageBody::Ping(_) => true,
            RoutedMessageBody::TxStatusRequest(_, _) => true,
            _ => false,
        }
    }
}

/// Routed Message wrapped with previous sender of the message.
pub struct RoutedMessageFrom {
    /// Routed messages.
    pub msg: RoutedMessage,
    /// Previous hop in the route. Used for messages that needs routing back.
    pub from: PeerId,
}

impl Message for RoutedMessageFrom {
    type Result = bool;
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct SyncData {
    pub edges: Vec<Edge>,
    pub accounts: Vec<AnnounceAccount>,
}

impl SyncData {
    pub fn edge(edge: Edge) -> Self {
        Self { edges: vec![edge], accounts: Vec::new() }
    }

    pub fn account(account: AnnounceAccount) -> Self {
        Self { edges: Vec::new(), accounts: vec![account] }
    }

    pub fn is_empty(&self) -> bool {
        self.edges.is_empty() && self.accounts.is_empty()
    }
}

// TODO(MarX, #1312): We have duplicated types of messages for now while routing between non-validators
//  is necessary. Some message are routed and others are directed between peers.
// TODO(MarX, #1312): Separate PeerMessages in client messages and network messages. I expect that most of
//  the client messages are routed.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub enum PeerMessage {
    Handshake(Handshake),
    HandshakeFailure(PeerInfo, HandshakeFailureReason),
    /// When a failed nonce is used by some peer, this message is sent back as evidence.
    LastEdge(Edge),
    /// Contains accounts and edge information.
    Sync(SyncData),
    RequestUpdateNonce(EdgeInfo),
    ResponseUpdateNonce(Edge),

    PeersRequest,
    PeersResponse(Vec<PeerInfo>),

    BlockHeadersRequest(Vec<CryptoHash>),
    BlockHeaders(Vec<BlockHeader>),
    BlockHeaderAnnounce(BlockHeader),

    BlockRequest(CryptoHash),
    Block(Block),

    Transaction(SignedTransaction),

    StateRequest(ShardId, CryptoHash, bool, Vec<Range>),
    StateResponse(StateResponseInfo),
    Routed(RoutedMessage),

    ChunkPartRequest(ChunkPartRequestMsg),
    ChunkOnePartRequest(ChunkOnePartRequestMsg),
    ChunkPart(ChunkPartMsg),
    ChunkOnePart(ChunkOnePart),

    /// Gracefully disconnect from other peer.
    Disconnect,

    Challenge(Challenge),
}

impl fmt::Display for PeerMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PeerMessage::Handshake(_) => f.write_str("Handshake"),
            PeerMessage::HandshakeFailure(_, _) => f.write_str("HandshakeFailure"),
            PeerMessage::Sync(_) => f.write_str("Sync"),
            PeerMessage::RequestUpdateNonce(_) => f.write_str("RequestUpdateNonce"),
            PeerMessage::ResponseUpdateNonce(_) => f.write_str("ResponseUpdateNonce"),
            PeerMessage::LastEdge(_) => f.write_str("LastEdge"),
            PeerMessage::PeersRequest => f.write_str("PeersRequest"),
            PeerMessage::PeersResponse(_) => f.write_str("PeersResponse"),
            PeerMessage::BlockHeadersRequest(_) => f.write_str("BlockHeaderRequest"),
            PeerMessage::BlockHeaders(_) => f.write_str("BlockHeaders"),
            PeerMessage::BlockHeaderAnnounce(_) => f.write_str("BlockHeaderAnnounce"),
            PeerMessage::BlockRequest(_) => f.write_str("BlockRequest"),
            PeerMessage::Block(_) => f.write_str("Block"),
            PeerMessage::Transaction(_) => f.write_str("Transaction"),
            PeerMessage::StateRequest(_, _, _, _) => f.write_str("StateRequest"),
            PeerMessage::StateResponse(_) => f.write_str("StateResponse"),
            PeerMessage::Routed(routed_message) => match routed_message.body {
                RoutedMessageBody::BlockApproval(_, _, _) => f.write_str("BlockApproval"),
                RoutedMessageBody::ForwardTx(_) => f.write_str("ForwardTx"),
                RoutedMessageBody::TxStatusRequest(_, _) => f.write_str("Transaction status query"),
                RoutedMessageBody::TxStatusResponse(_) => {
                    f.write_str("Transaction status response")
                }
                RoutedMessageBody::StateRequest(_, _, _, _) => f.write_str("StateResponse"),
                RoutedMessageBody::ChunkPartRequest(_) => f.write_str("ChunkPartRequest"),
                RoutedMessageBody::ChunkOnePartRequest(_) => f.write_str("ChunkOnePartRequest"),
                RoutedMessageBody::ChunkOnePart(_) => f.write_str("ChunkOnePart"),
                RoutedMessageBody::Ping(_) => f.write_str("Ping"),
                RoutedMessageBody::Pong(_) => f.write_str("Pong"),
            },
            PeerMessage::ChunkPartRequest(_) => f.write_str("ChunkPartRequest"),
            PeerMessage::ChunkOnePartRequest(_) => f.write_str("ChunkOnePartRequest"),
            PeerMessage::ChunkPart(_) => f.write_str("ChunkPart"),
            PeerMessage::ChunkOnePart(_) => f.write_str("ChunkOnePart"),
            PeerMessage::Disconnect => f.write_str("Disconnect"),
            PeerMessage::Challenge(_) => f.write_str("Challenge"),
        }
    }
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
    pub peer_max_count: u32,
    /// Duration of the ban for misbehaving peers.
    pub ban_window: Duration,
    /// Remove expired peers.
    pub peer_expiration_duration: Duration,
    /// Maximum number of peer addresses we should ever send.
    pub max_send_peers: u32,
    /// Duration for checking on stats from the peers.
    pub peer_stats_period: Duration,
    /// Time to persist Accounts Id in the router without removing them.
    pub ttl_account_id_router: Duration,
    /// Maximum number of routes that we should keep track for each Account id in the Routing Table.
    pub max_routes_to_store: usize,
}

/// Status of the known peers.
#[derive(BorshSerialize, BorshDeserialize, Eq, PartialEq, Debug)]
pub enum KnownPeerStatus {
    Unknown,
    NotConnected,
    Connected,
    Banned(ReasonForBan, u64),
}

/// Information node stores about known peers.
#[derive(BorshSerialize, BorshDeserialize, Debug)]
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
            first_seen: to_timestamp(Utc::now()),
            last_seen: to_timestamp(Utc::now()),
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
#[derive(Message)]
pub struct InboundTcpConnect {
    /// Tcp stream of the inbound connections
    pub stream: TcpStream,
}

impl InboundTcpConnect {
    /// Method to create a new InboundTcpConnect message from a TCP stream
    pub fn new(stream: TcpStream) -> InboundTcpConnect {
        InboundTcpConnect { stream }
    }
}

/// Actor message to request the creation of an outbound TCP connection to a peer.
#[derive(Message)]
pub struct OutboundTcpConnect {
    /// Peer information of the outbound connection
    pub peer_info: PeerInfo,
}

#[derive(Message, Clone, Debug)]
pub struct SendMessage {
    pub message: PeerMessage,
}

/// Actor message to consolidate potential new peer.
/// Returns if connection should be kept or dropped.
pub struct Consolidate {
    pub actor: Addr<Peer>,
    pub peer_info: PeerInfo,
    pub peer_type: PeerType,
    pub chain_info: PeerChainInfo,
    // Edge information from this node.
    // If this is None it implies we are outbound connection, so we need to create our
    // EdgeInfo part and send it to the other peer.
    pub this_edge_info: Option<EdgeInfo>,
    // Edge information from other node.
    pub other_edge_info: EdgeInfo,
}

impl Message for Consolidate {
    type Result = ConsolidateResponse;
}

#[derive(MessageResponse, Debug)]
pub enum ConsolidateResponse {
    Accept(Option<EdgeInfo>),
    InvalidNonce(Edge),
    Reject,
}

/// Unregister message from Peer to PeerManager.
#[derive(Message)]
pub struct Unregister {
    pub peer_id: PeerId,
}

pub struct PeerList {
    pub peers: Vec<PeerInfo>,
}

/// TODO(MarX): Wrap the following type of messages in this category:
///     - PeersRequest
///     - PeersResponse
///     - Unregister
///     - Ban
///     - Consolidate (Maybe not)
///  check that this messages are only used from peer -> peer manager.
/// Message from peer to peer manager
pub enum PeerRequest {
    UpdateEdge((PeerId, u64)),
}

impl Message for PeerRequest {
    type Result = PeerResponse;
}

#[derive(MessageResponse)]
pub enum PeerResponse {
    UpdatedEdge(EdgeInfo),
}

/// Requesting peers from peer manager to communicate to a peer.
pub struct PeersRequest {}

impl Message for PeersRequest {
    type Result = PeerList;
}

/// Received new peers from another peer.
#[derive(Message)]
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
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Copy)]
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
}

#[derive(Message)]
pub struct Ban {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

// TODO(#1313): Use Box
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkRequests {
    /// Fetch information from the network.
    FetchInfo,
    /// Sends block, either when block was just produced or when requested.
    Block {
        block: Block,
    },
    /// Sends block header announcement, with possibly attaching approval for this block if
    /// participating in this epoch.
    BlockHeaderAnnounce {
        header: BlockHeader,
        approval: Option<BlockApproval>,
    },
    /// Request block with given hash from given peer.
    BlockRequest {
        hash: CryptoHash,
        peer_id: PeerId,
    },
    /// Request given block headers.
    BlockHeadersRequest {
        hashes: Vec<CryptoHash>,
        peer_id: PeerId,
    },
    /// Request state for given shard at given state root.
    StateRequest {
        shard_id: ShardId,
        hash: CryptoHash,
        need_header: bool,
        parts_ranges: Vec<Range>,
        account_id: AccountId,
    },
    /// Ban given peer.
    BanPeer {
        peer_id: PeerId,
        ban_reason: ReasonForBan,
    },
    /// Announce account
    AnnounceAccount(AnnounceAccount),

    /// Request chunk part
    ChunkPartRequest {
        account_id: AccountId,
        part_request: ChunkPartRequestMsg,
    },
    /// Request chunk part and receipts
    ChunkOnePartRequest {
        account_id: AccountId,
        one_part_request: ChunkOnePartRequestMsg,
    },
    /// Response to a peer with chunk part and receipts.
    ChunkOnePartResponse {
        peer_id: PeerId,
        header_and_part: ChunkOnePart,
    },
    /// A chunk header and one part for another validator.
    ChunkOnePartMessage {
        account_id: AccountId,
        header_and_part: ChunkOnePart,
    },
    /// A chunk part
    ChunkPart {
        peer_id: PeerId,
        part: ChunkPartMsg,
    },

    /// Valid transaction but since we are not validators we send this transaction to current validators.
    ForwardTx(AccountId, SignedTransaction),
    /// Query transaction status
    TxStatus(AccountId, AccountId, CryptoHash),

    /// The following types of requests are used to trigger actions in the Peer Manager for testing.
    /// Fetch current routing table.
    FetchRoutingTable,
    /// Data to sync routing table from active peer.
    Sync {
        peer_id: PeerId,
        sync_data: SyncData,
    },

    RequestUpdateNonce(PeerId, EdgeInfo),
    ResponseUpdateNonce(Edge),

    /// Start ping to `PeerId` with `nonce`.
    PingTo(usize, PeerId),
    /// Fetch all received ping and pong so far.
    FetchPingPongInfo,

    /// A challenge to invalidate a block.
    Challenge(Challenge),
}

/// Messages from PeerManager to Peer
#[derive(Message)]
pub enum PeerManagerRequest {
    BanPeer(ReasonForBan),
    UnregisterPeer,
}

/// Combines peer address info and chain information.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FullPeerInfo {
    pub peer_info: PeerInfo,
    pub chain_info: PeerChainInfo,
    pub edge_info: EdgeInfo,
}

#[derive(Debug)]
pub struct NetworkInfo {
    pub active_peers: Vec<FullPeerInfo>,
    pub num_active_peers: usize,
    pub peer_max_count: u32,
    pub most_weight_peers: Vec<FullPeerInfo>,
    pub sent_bytes_per_sec: u64,
    pub received_bytes_per_sec: u64,
    /// Accounts of known block and chunk producers from routing table.
    pub known_producers: Vec<AccountId>,
}

#[derive(Debug)]
pub enum NetworkResponses {
    NoResponse,
    Info(NetworkInfo),
    RoutingTableInfo(RoutingTableInfo),
    PingPongInfo { pings: HashMap<usize, Ping>, pongs: HashMap<usize, Pong> },
    BanPeer(ReasonForBan),
    EdgeUpdate(Edge),
}

impl<A, M> MessageResponse<A, M> for NetworkResponses
where
    A: Actor,
    M: Message<Result = NetworkResponses>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for NetworkRequests {
    type Result = NetworkResponses;
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateResponseInfo {
    pub shard_id: ShardId,
    pub hash: CryptoHash,
    pub shard_state: ShardStateSyncResponse,
}

#[derive(Debug)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub enum NetworkClientMessages {
    /// Received transaction.
    Transaction(SignedTransaction),
    TxStatus {
        tx_hash: CryptoHash,
        signer_account_id: AccountId,
    },
    TxStatusResponse(FinalExecutionOutcomeView),
    /// Received block header.
    BlockHeader(BlockHeader, PeerId),
    /// Received block, possibly requested.
    Block(Block, PeerId, bool),
    /// Received list of headers for syncing.
    BlockHeaders(Vec<BlockHeader>, PeerId),
    /// Get Chain information from Client.
    GetChainInfo,
    /// Block approval.
    BlockApproval(AccountId, CryptoHash, Signature, PeerId),
    /// Request headers.
    BlockHeadersRequest(Vec<CryptoHash>),
    /// Request a block.
    BlockRequest(CryptoHash),
    /// State request.
    StateRequest(ShardId, CryptoHash, bool, Vec<Range>),
    /// State response.
    StateResponse(StateResponseInfo),
    /// Account announcements that needs to be validated before being processed.
    AnnounceAccount(Vec<AnnounceAccount>),

    /// Request chunk part.
    ChunkPartRequest(ChunkPartRequestMsg, PeerId),
    /// Request chunk part.
    ChunkOnePartRequest(ChunkOnePartRequestMsg, PeerId),
    /// A chunk part.
    ChunkPart(ChunkPartMsg),
    /// A chunk header and one part.
    ChunkOnePart(ChunkOnePart),

    /// A challenge to invalidate the block.
    Challenge(Challenge),
}

// TODO(#1313): Use Box
#[derive(Eq, PartialEq, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkClientResponses {
    /// No response.
    NoResponse,
    /// Valid transaction inserted into mempool as response to Transaction.
    ValidTx,
    /// Invalid transaction inserted into mempool as response to Transaction.
    InvalidTx(InvalidTxError),
    /// The request is routed to other shards
    RequestRouted,
    /// Ban peer for malicious behaviour.
    Ban { ban_reason: ReasonForBan },
    /// Chain information.
    ChainInfo { genesis_id: GenesisId, height: BlockIndex, total_weight: Weight },
    /// Block response.
    Block(Block),
    /// Headers response.
    BlockHeaders(Vec<BlockHeader>),
    /// Response to state request.
    StateResponse(StateResponseInfo),
    /// Valid announce accounts.
    AnnounceAccount(Vec<AnnounceAccount>),
    /// Transaction execution outcome
    TxStatus(FinalExecutionOutcomeView),
}

impl<A, M> MessageResponse<A, M> for NetworkClientResponses
where
    A: Actor,
    M: Message<Result = NetworkClientResponses>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for NetworkClientMessages {
    type Result = NetworkClientResponses;
}

/// Peer stats query.
pub struct QueryPeerStats {}

/// Peer stats result
#[derive(Debug)]
pub struct PeerStatsResult {
    /// Chain info.
    pub chain_info: PeerChainInfo,
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

#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct ChunkPartRequestMsg {
    pub shard_id: ShardId,
    pub chunk_hash: ChunkHash,
    pub height: BlockIndex,
    pub part_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct ChunkOnePartRequestMsg {
    pub shard_id: ShardId,
    pub chunk_hash: ChunkHash,
    pub height: BlockIndex,
    pub part_id: u64,
    pub tracking_shards: HashSet<ShardId>,
}

#[derive(Message)]
pub struct StopSignal {}
