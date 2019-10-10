use std::collections::HashSet;
use std::convert::{From, TryInto};
use std::convert::{Into, TryFrom};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::Duration;

use actix::dev::{MessageResponse, ResponseChannel};
use actix::{Actor, Addr, Message};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use tokio::net::TcpStream;

use near_chain::types::ShardStateSyncResponse;
use near_chain::{Block, BlockApproval, BlockHeader, Weight};
use near_crypto::{BlsSignature, PublicKey, ReadablePublicKey, SecretKey, Signature};
use near_primitives::hash::{hash, CryptoHash};
pub use near_primitives::sharding::ChunkPartMsg;
use near_primitives::sharding::{ChunkHash, ChunkOnePart};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockIndex, EpochId, Range, ShardId};
use near_primitives::utils::{from_timestamp, to_timestamp};
use serde_derive::{Deserialize, Serialize};

use crate::peer::Peer;
use near_primitives::errors::InvalidTxError;

/// Current latest version of the protocol
pub const PROTOCOL_VERSION: u32 = 4;

/// Peer id is the public key.
#[derive(BorshSerialize, BorshDeserialize, Clone, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PeerId(PublicKey);

impl PeerId {
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

impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(acc) = self.account_id.as_ref() {
            write!(f, "({}, {:?}, {})", self.id, self.addr, acc)
        } else {
            write!(f, "({}, {:?})", self.id, self.addr)
        }
    }
}

impl TryFrom<&str> for PeerInfo {
    type Error = Box<dyn std::error::Error>;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let chunks: Vec<_> = s.split('@').collect();
        if chunks.len() != 2 {
            return Err(format!("Invalid peer info format, got {}, must be id@ip_addr", s).into());
        }
        Ok(PeerInfo {
            id: PeerId(ReadablePublicKey::new(chunks[0]).try_into()?),
            addr: Some(
                chunks[1].parse().map_err(|err| {
                    format!("Invalid ip address format for {}: {}", chunks[1], err)
                })?,
            ),
            account_id: None,
        })
    }
}

/// Peer chain information.
#[derive(BorshSerialize, BorshDeserialize, Copy, Clone, Debug, Eq, PartialEq, Default)]
pub struct PeerChainInfo {
    /// Genesis hash.
    pub genesis: CryptoHash,
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
}

impl Handshake {
    pub fn new(peer_id: PeerId, listen_port: Option<u16>, chain_info: PeerChainInfo) -> Self {
        Handshake { version: PROTOCOL_VERSION, peer_id, listen_port, chain_info }
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct AnnounceAccountRouteHeader {
    pub account_id: AccountId,
    pub peer_id: PeerId,
    pub epoch_id: EpochId,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub enum AccountOrPeerSignature {
    PeerSignature(Signature),
    AccountSignature(BlsSignature),
}

impl AccountOrPeerSignature {
    pub fn peer_signature(&self) -> Option<&Signature> {
        match self {
            AccountOrPeerSignature::PeerSignature(signature) => Some(signature),
            AccountOrPeerSignature::AccountSignature(_) => None,
        }
    }

    pub fn account_signature(&self) -> Option<&BlsSignature> {
        match self {
            AccountOrPeerSignature::PeerSignature(_) => None,
            AccountOrPeerSignature::AccountSignature(signature) => Some(signature),
        }
    }

    pub fn verify_peer(&self, data: &[u8], public_key: &PublicKey) -> bool {
        match self {
            AccountOrPeerSignature::PeerSignature(signature) => signature.verify(data, public_key),
            AccountOrPeerSignature::AccountSignature(_) => false,
        }
    }
}

/// Account route description
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct AnnounceAccountRoute {
    pub peer_id: PeerId,
    pub hash: CryptoHash,
    pub signature: AccountOrPeerSignature,
}

/// Account announcement information
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct AnnounceAccount {
    /// AccountId to be announced
    pub account_id: AccountId,
    /// This announcement is only valid for this `epoch`
    pub epoch_id: EpochId,
    /// Complete route description to account id
    /// First element of the route (header) contains:
    ///     peer_id owner of the account_id
    ///     hash of the announcement
    ///     signature with account id secret key
    /// Subsequent elements of the route contain:
    ///     peer_id of intermediates hop in the route
    ///     hash built using previous hash and peer_id
    ///     signature with peer id secret key
    pub route: Vec<AnnounceAccountRoute>,
}

impl AnnounceAccount {
    pub fn new(
        account_id: AccountId,
        epoch_id: EpochId,
        peer_id: PeerId,
        hash: CryptoHash,
        signature: AccountOrPeerSignature,
    ) -> Self {
        let route = vec![AnnounceAccountRoute { peer_id, hash, signature }];
        Self { account_id, epoch_id, route }
    }

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

    pub fn header_hash(&self) -> CryptoHash {
        AnnounceAccount::build_header_hash(
            &self.account_id,
            &self.route.first().unwrap().peer_id,
            &self.epoch_id,
        )
    }

    pub fn header(&self) -> &AnnounceAccountRoute {
        self.route.first().unwrap()
    }

    /// Peer Id sending this announcement.
    pub fn peer_id(&self) -> PeerId {
        self.route.last().unwrap().peer_id.clone()
    }

    /// Peer Id of the originator of this announcement.
    pub fn original_peer_id(&self) -> PeerId {
        self.route.first().unwrap().peer_id.clone()
    }

    pub fn num_hops(&self) -> usize {
        self.route.len() - 1
    }

    pub fn extend(&mut self, peer_id: PeerId, secret_key: &SecretKey) {
        let last_hash = self.route.last().unwrap().hash;
        let new_hash =
            hash([last_hash.as_ref(), peer_id.try_to_vec().unwrap().as_ref()].concat().as_slice());
        let signature = secret_key.sign(new_hash.as_ref());
        self.route.push(AnnounceAccountRoute {
            peer_id,
            hash: new_hash,
            signature: AccountOrPeerSignature::PeerSignature(signature),
        })
    }
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub enum HandshakeFailureReason {
    ProtocolVersionMismatch(u32),
    GenesisMismatch(CryptoHash),
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub enum RoutedMessageBody {
    BlockApproval(AccountId, CryptoHash, BlsSignature),
    ForwardTx(SignedTransaction),
    StateRequest(ShardId, CryptoHash, bool, Vec<Range>),
    ChunkPartRequest(ChunkPartRequestMsg),
    ChunkOnePartRequest(ChunkOnePartRequestMsg),
    ChunkOnePart(ChunkOnePart),
}

#[derive(Message)]
pub struct RawRoutedMessage {
    pub account_id: AccountId,
    pub body: RoutedMessageBody,
}

impl RawRoutedMessage {
    pub fn sign(self, author: PeerId, secret_key: &SecretKey) -> RoutedMessage {
        let hash = RoutedMessage::build_hash(&self.account_id, &author, &self.body);
        let signature = secret_key.sign(hash.as_ref());
        RoutedMessage {
            account_id: self.account_id,
            author,
            signature: AccountOrPeerSignature::PeerSignature(signature),
            body: self.body,
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct RoutedMessageMsg {
    account_id: AccountId,
    author: PeerId,
    body: RoutedMessageBody,
}

/// RoutedMessage represent a package that will travel the network towards a specific account id.
/// It contains the peer_id and signature from the original sender. Every intermediate peer in the
/// route must verify that this signature is valid otherwise previous sender of this package should
/// be banned. If the final receiver of this package finds that the body is invalid the original
/// sender of the package should be banned instead. (Notice that this peer might not be connected
/// to us)
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct RoutedMessage {
    /// Account id which is directed this message
    pub account_id: AccountId,
    /// Original sender of this message
    pub author: PeerId,
    /// Signature from the author of the message. If this signature is invalid we should ban
    /// last sender of this message. If the message is invalid we should ben author of the message.
    pub signature: AccountOrPeerSignature,
    /// Message
    pub body: RoutedMessageBody,
}

impl RoutedMessage {
    pub fn build_hash(
        account_id: &AccountId,
        author: &PeerId,
        body: &RoutedMessageBody,
    ) -> CryptoHash {
        hash(
            &RoutedMessageMsg {
                account_id: account_id.clone(),
                author: author.clone(),
                body: body.clone(),
            }
            .try_to_vec()
            .expect("Failed to serialize"),
        )
    }

    fn hash(&self) -> CryptoHash {
        RoutedMessage::build_hash(&self.account_id, &self.author, &self.body)
    }

    pub fn verify(&self) -> bool {
        self.signature.verify_peer(self.hash().as_ref(), &self.author.public_key())
    }
}

impl Message for RoutedMessage {
    type Result = bool;
}

// TODO(MarX): We have duplicated types of messages for now while routing between non-validators
//  is necessary. Some message are routed and others are directed between peers.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub enum PeerMessage {
    Handshake(Handshake),
    HandshakeFailure(PeerInfo, HandshakeFailureReason),

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
    AnnounceAccount(AnnounceAccount),
    Routed(RoutedMessage),

    ChunkPartRequest(ChunkPartRequestMsg),
    ChunkOnePartRequest(ChunkOnePartRequestMsg),
    ChunkPart(ChunkPartMsg),
    ChunkOnePart(ChunkOnePart),
}

impl fmt::Display for PeerMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PeerMessage::Handshake(_) => f.write_str("Handshake"),
            PeerMessage::HandshakeFailure(_, _) => f.write_str("HandshakeFailure"),
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
            PeerMessage::AnnounceAccount(_) => f.write_str("AnnounceAccount"),
            PeerMessage::Routed(routed_message) => match routed_message.body {
                RoutedMessageBody::BlockApproval(_, _, _) => f.write_str("BlockApproval"),
                RoutedMessageBody::ForwardTx(_) => f.write_str("ForwardTx"),
                RoutedMessageBody::StateRequest(_, _, _, _) => f.write_str("StateResponse"),
                RoutedMessageBody::ChunkPartRequest(_) => f.write_str("ChunkPartRequest"),
                RoutedMessageBody::ChunkOnePartRequest(_) => f.write_str("ChunkOnePartRequest"),
                RoutedMessageBody::ChunkOnePart(_) => f.write_str("ChunkOnePart"),
            },
            PeerMessage::ChunkPartRequest(_) => f.write_str("ChunkPartRequest"),
            PeerMessage::ChunkOnePartRequest(_) => f.write_str("ChunkOnePartRequest"),
            PeerMessage::ChunkPart(_) => f.write_str("ChunkPart"),
            PeerMessage::ChunkOnePart(_) => f.write_str("ChunkOnePart"),
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
}

impl Message for Consolidate {
    type Result = bool;
}

/// Unregister message from Peer to PeerManager.
#[derive(Message)]
pub struct Unregister {
    pub peer_id: PeerId,
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
}

#[derive(Message)]
pub struct Ban {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

#[derive(Debug, Clone, PartialEq)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub enum NetworkRequests {
    /// Fetch information from the network.
    FetchInfo,
    /// Sends block, either when block was just produced or when requested.
    Block { block: Block },
    /// Sends block header announcement, with possibly attaching approval for this block if
    /// participating in this epoch.
    BlockHeaderAnnounce { header: BlockHeader, approval: Option<BlockApproval> },
    /// Request block with given hash from given peer.
    BlockRequest { hash: CryptoHash, peer_id: PeerId },
    /// Request given block headers.
    BlockHeadersRequest { hashes: Vec<CryptoHash>, peer_id: PeerId },
    /// Request state for given shard at given state root.
    StateRequest {
        shard_id: ShardId,
        hash: CryptoHash,
        need_header: bool,
        parts_ranges: Vec<Range>,
        account_id: AccountId,
    },
    /// Ban given peer.
    BanPeer { peer_id: PeerId, ban_reason: ReasonForBan },
    /// Announce account
    AnnounceAccount(AnnounceAccount),

    /// Request chunk part
    ChunkPartRequest { account_id: AccountId, part_request: ChunkPartRequestMsg },
    /// Request chunk part and receipts
    ChunkOnePartRequest { account_id: AccountId, one_part_request: ChunkOnePartRequestMsg },
    /// Response to a peer with chunk part and receipts.
    ChunkOnePartResponse { peer_id: PeerId, header_and_part: ChunkOnePart },
    /// A chunk header and one part for another validator.
    ChunkOnePartMessage { account_id: AccountId, header_and_part: ChunkOnePart },
    /// A chunk part
    ChunkPart { peer_id: PeerId, part: ChunkPartMsg },
}

/// Combines peer address info and chain information.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FullPeerInfo {
    pub peer_info: PeerInfo,
    pub chain_info: PeerChainInfo,
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
    /// Received block header.
    BlockHeader(BlockHeader, PeerId),
    /// Received block, possibly requested.
    Block(Block, PeerId, bool),
    /// Received list of headers for syncing.
    BlockHeaders(Vec<BlockHeader>, PeerId),
    /// Get Chain information from Client.
    GetChainInfo,
    /// Block approval.
    BlockApproval(AccountId, CryptoHash, BlsSignature, PeerId),
    /// Request headers.
    BlockHeadersRequest(Vec<CryptoHash>),
    /// Request a block.
    BlockRequest(CryptoHash),
    /// State request.
    StateRequest(ShardId, CryptoHash, bool, Vec<Range>),
    /// State response.
    StateResponse(StateResponseInfo),
    /// Account announcement that needs to be validated before being processed
    AnnounceAccount(AnnounceAccount),

    /// Request chunk part
    ChunkPartRequest(ChunkPartRequestMsg, PeerId),
    /// Request chunk part
    ChunkOnePartRequest(ChunkOnePartRequestMsg, PeerId),
    /// A chunk part
    ChunkPart(ChunkPartMsg),
    /// A chunk header and one part
    ChunkOnePart(ChunkOnePart),
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
    /// Valid transaction but since we are not validators we send this transaction to current validators.
    ForwardTx(AccountId, SignedTransaction),
    /// Ban peer for malicious behaviour.
    Ban { ban_reason: ReasonForBan },
    /// Chain information.
    ChainInfo { genesis: CryptoHash, height: BlockIndex, total_weight: Weight },
    /// Block response.
    Block(Block),
    /// Headers response.
    BlockHeaders(Vec<BlockHeader>),
    /// Response to state request.
    StateResponse(StateResponseInfo),
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
