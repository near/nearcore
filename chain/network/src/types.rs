use std::collections::{HashMap, HashSet};
use std::convert::{Into, TryFrom, TryInto};
use std::fmt;
use std::net::{AddrParseError, IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use actix::dev::{MessageResponse, ResponseChannel};
use actix::{Actor, Addr, MailboxError, Message, Recipient};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use futures::{future::BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use strum::AsStaticStr;
use tokio::net::TcpStream;
use tracing::{error, warn};

use near_chain::{Block, BlockHeader};
use near_crypto::{PublicKey, SecretKey, Signature};
use near_primitives::block::{Approval, ApprovalMessage, GenesisId};
use near_primitives::challenge::Challenge;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::{
    ChunkHash, PartialEncodedChunk, PartialEncodedChunkPart, PartialEncodedChunkV1,
    PartialEncodedChunkWithArcReceipts, ReceiptProof, ShardChunkHeader,
};
use near_primitives::syncing::{
    EpochSyncFinalizationResponse, EpochSyncResponse, ShardStateSyncResponse,
    ShardStateSyncResponseV1,
};
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, SignedTransaction};
use near_primitives::types::{AccountId, BlockHeight, BlockReference, EpochId, ShardId};
use near_primitives::utils::{from_timestamp, to_timestamp};
use near_primitives::version::{
    ProtocolVersion, OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION, PROTOCOL_VERSION,
};
use near_primitives::views::{FinalExecutionOutcomeView, QueryRequest, QueryResponse};

use crate::peer::Peer;
#[cfg(feature = "metric_recorder")]
use crate::recorder::MetricRecorder;
use crate::routing::{Edge, EdgeInfo, RoutingTableInfo};
use std::fmt::{Debug, Error, Formatter};
use std::io;

use conqueue::QueueSender;
use near_primitives::merkle::combine_hash;

const ERROR_UNEXPECTED_LENGTH_OF_INPUT: &str = "Unexpected length of input";
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
        Ok(PeerInfo { id: PeerId(chunks[0].parse()?), addr, account_id })
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
#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct PeerChainInfo {
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Last known chain height of the peer.
    pub height: BlockHeight,
    /// Shards that the peer is tracking.
    pub tracked_shards: Vec<ShardId>,
}

/// Peer chain information.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, Eq, PartialEq, Default)]
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

#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
pub enum HandshakeFailureReason {
    ProtocolVersionMismatch { version: u32, oldest_supported_version: u32 },
    GenesisMismatch(GenesisId),
    InvalidTarget,
}

impl fmt::Display for HandshakeFailureReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "HandshakeFailureReason")
    }
}

impl std::error::Error for HandshakeFailureReason {}

#[derive(BorshSerialize, Serialize, PartialEq, Eq, Clone, Debug)]
pub struct Handshake {
    pub version: u32,
    /// Oldest supported protocol version.
    pub oldest_supported_version: u32,
    /// Sender's peer id.
    pub peer_id: PeerId,
    /// Receiver's peer id.
    pub target_peer_id: PeerId,
    /// Sender's listening addr.
    pub listen_port: Option<u16>,
    /// Peer's chain information.
    pub chain_info: PeerChainInfoV2,
    /// Info for new edge.
    pub edge_info: EdgeInfo,
}

/// Struct describing the layout for Handshake.
/// It is used to automatically derive BorshDeserialize.
/// Struct describing the layout for Handshake.
/// It is used to automatically derive BorshDeserialize.
#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
pub struct HandshakeAutoDes {
    /// Protocol version.
    pub version: u32,
    /// Oldest supported protocol version.
    pub oldest_supported_version: u32,
    /// Sender's peer id.
    pub peer_id: PeerId,
    /// Receiver's peer id.
    pub target_peer_id: PeerId,
    /// Sender's listening addr.
    pub listen_port: Option<u16>,
    /// Peer's chain information.
    pub chain_info: PeerChainInfoV2,
    /// Info for new edge.
    pub edge_info: EdgeInfo,
}

impl Handshake {
    pub fn new(
        version: ProtocolVersion,
        peer_id: PeerId,
        target_peer_id: PeerId,
        listen_port: Option<u16>,
        chain_info: PeerChainInfoV2,
        edge_info: EdgeInfo,
    ) -> Self {
        Handshake {
            version,
            oldest_supported_version: OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION,
            peer_id,
            target_peer_id,
            listen_port,
            chain_info,
            edge_info,
        }
    }
}

// Use custom deserializer for HandshakeV2. Try to read version of the other peer from the header.
// If the version is supported then fallback to standard deserializer.
impl BorshDeserialize for Handshake {
    fn deserialize(buf: &mut &[u8]) -> std::io::Result<Self> {
        // Detect the current and oldest supported version from the header
        if buf.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                ERROR_UNEXPECTED_LENGTH_OF_INPUT,
            ));
        }

        let version = u32::from_le_bytes(buf[..4].try_into().unwrap());

        if OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION <= version && version <= PROTOCOL_VERSION {
            // If we support this version, then try to deserialize with custom deserializer
            HandshakeAutoDes::deserialize(buf).map(Into::into)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                HandshakeFailureReason::ProtocolVersionMismatch {
                    version,
                    oldest_supported_version: version,
                },
            ))
        }
    }
}

impl From<HandshakeAutoDes> for Handshake {
    fn from(handshake: HandshakeAutoDes) -> Self {
        Self {
            version: handshake.version,
            oldest_supported_version: handshake.oldest_supported_version,
            peer_id: handshake.peer_id,
            target_peer_id: handshake.target_peer_id,
            listen_port: handshake.listen_port,
            chain_info: handshake.chain_info,
            edge_info: handshake.edge_info,
        }
    }
}

// TODO: Remove Handshake V2 in next iteration
#[derive(BorshSerialize, Serialize, PartialEq, Eq, Clone, Debug)]
pub struct HandshakeV2 {
    pub version: u32,
    pub oldest_supported_version: u32,
    pub peer_id: PeerId,
    pub target_peer_id: PeerId,
    pub listen_port: Option<u16>,
    pub chain_info: PeerChainInfo,
    pub edge_info: EdgeInfo,
}

impl HandshakeV2 {
    pub fn new(
        version: ProtocolVersion,
        peer_id: PeerId,
        target_peer_id: PeerId,
        listen_port: Option<u16>,
        chain_info: PeerChainInfo,
        edge_info: EdgeInfo,
    ) -> Self {
        Self {
            version,
            oldest_supported_version: OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION,
            peer_id,
            target_peer_id,
            listen_port,
            chain_info,
            edge_info,
        }
    }
}

/// Struct describing the layout for HandshakeV2.
/// It is used to automatically derive BorshDeserialize.
#[derive(BorshDeserialize)]
pub struct HandshakeV2AutoDes {
    pub version: u32,
    pub oldest_supported_version: u32,
    pub peer_id: PeerId,
    pub target_peer_id: PeerId,
    pub listen_port: Option<u16>,
    pub chain_info: PeerChainInfo,
    pub edge_info: EdgeInfo,
}

// Use custom deserializer for HandshakeV2. Try to read version of the other peer from the header.
// If the version is supported then fallback to standard deserializer.
impl BorshDeserialize for HandshakeV2 {
    fn deserialize(buf: &mut &[u8]) -> std::io::Result<Self> {
        // Detect the current and oldest supported version from the header
        if buf.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                ERROR_UNEXPECTED_LENGTH_OF_INPUT,
            ));
        }

        let version = u32::from_le_bytes(buf[..4].try_into().unwrap());
        let oldest_supported_version = u32::from_le_bytes(buf[4..8].try_into().unwrap());

        if OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION <= version && version <= PROTOCOL_VERSION {
            // If we support this version, then try to deserialize with custom deserializer
            match version {
                _ => HandshakeV2AutoDes::deserialize(buf).map(Into::into),
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                HandshakeFailureReason::ProtocolVersionMismatch {
                    version,
                    oldest_supported_version,
                },
            ))
        }
    }
}

impl From<HandshakeV2AutoDes> for HandshakeV2 {
    fn from(handshake: HandshakeV2AutoDes) -> Self {
        Self {
            version: handshake.version,
            oldest_supported_version: handshake.oldest_supported_version,
            peer_id: handshake.peer_id,
            target_peer_id: handshake.target_peer_id,
            listen_port: handshake.listen_port,
            chain_info: handshake.chain_info,
            edge_info: handshake.edge_info,
        }
    }
}

impl From<HandshakeV2> for Handshake {
    fn from(handshake: HandshakeV2) -> Self {
        Self {
            version: handshake.version,
            oldest_supported_version: handshake.oldest_supported_version,
            peer_id: handshake.peer_id,
            target_peer_id: handshake.target_peer_id,
            listen_port: handshake.listen_port,
            chain_info: handshake.chain_info.into(),
            edge_info: handshake.edge_info,
        }
    }
}

/// Account route description
#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
pub struct AnnounceAccountRoute {
    pub peer_id: PeerId,
    pub hash: CryptoHash,
    pub signature: Signature,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
pub struct Ping {
    pub nonce: u64,
    pub source: PeerId,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
pub struct Pong {
    pub nonce: u64,
    pub source: PeerId,
}

// TODO(#1313): Use Box
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
    ReceiptOutComeResponse(ExecutionOutcomeWithIdAndProof),
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
            RoutedMessageBody::ReceiptOutComeResponse(response) => {
                write!(f, "ReceiptResponse({})", response.outcome_with_id.id)
            }
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
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
pub enum PeerIdOrHash {
    PeerId(PeerId),
    Hash(CryptoHash),
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize, Hash)]
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
        hash(
            &RoutedMessageNoSignature { target, author: source, body }
                .try_to_vec()
                .expect("Failed to serialize"),
        )
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
pub struct RoutedMessageFrom {
    /// Routed messages.
    pub msg: RoutedMessage,
    /// Previous hop in the route. Used for messages that needs routing back.
    pub from: PeerId,
}

impl Message for RoutedMessageFrom {
    type Result = bool;
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
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

/// Warning, position of each message type in this enum defines the protocol due to serialization.
/// DO NOT MOVE, REORDER, DELETE items from the list. Only add new items to the end.
/// If need to remove old items - replace with `None`.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    strum::AsStaticStr,
    strum::EnumVariantNames,
)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub enum PeerMessage {
    Handshake(Handshake),
    HandshakeFailure(PeerInfo, HandshakeFailureReason),
    /// When a failed nonce is used by some peer, this message is sent back as evidence.
    LastEdge(Edge),
    /// Contains accounts and edge information.
    RoutingTableSync(SyncData),
    RequestUpdateNonce(EdgeInfo),
    ResponseUpdateNonce(Edge),

    PeersRequest,
    PeersResponse(Vec<PeerInfo>),

    BlockHeadersRequest(Vec<CryptoHash>),
    BlockHeaders(Vec<BlockHeader>),

    BlockRequest(CryptoHash),
    Block(Block),

    Transaction(SignedTransaction),
    Routed(RoutedMessage),

    /// Gracefully disconnect from other peer.
    Disconnect,
    Challenge(Challenge),
    HandshakeV2(HandshakeV2),

    EpochSyncRequest(EpochId),
    EpochSyncResponse(EpochSyncResponse),
    EpochSyncFinalizationRequest(EpochId),
    EpochSyncFinalizationResponse(EpochSyncFinalizationResponse),
}

impl fmt::Display for PeerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(format!("{}", self.msg_variant()).as_ref())
    }
}

impl PeerMessage {
    pub fn msg_variant(&self) -> &str {
        match self {
            PeerMessage::Routed(routed_message) => {
                strum::AsStaticRef::as_static(&routed_message.body)
            }
            _ => strum::AsStaticRef::as_static(self),
        }
    }

    pub fn is_client_message(&self) -> bool {
        match self {
            PeerMessage::Block(_)
            | PeerMessage::BlockHeaders(_)
            | PeerMessage::Transaction(_)
            | PeerMessage::Challenge(_)
            | PeerMessage::EpochSyncResponse(_)
            | PeerMessage::EpochSyncFinalizationResponse(_) => true,
            PeerMessage::Routed(r) => match r.body {
                RoutedMessageBody::BlockApproval(_)
                | RoutedMessageBody::ForwardTx(_)
                | RoutedMessageBody::PartialEncodedChunk(_)
                | RoutedMessageBody::PartialEncodedChunkRequest(_)
                | RoutedMessageBody::PartialEncodedChunkResponse(_)
                | RoutedMessageBody::StateResponse(_)
                | RoutedMessageBody::VersionedPartialEncodedChunk(_)
                | RoutedMessageBody::VersionedStateResponse(_) => true,
                RoutedMessageBody::PartialEncodedChunkForward(_) => true,
                _ => false,
            },
            _ => false,
        }
    }

    pub fn is_view_client_message(&self) -> bool {
        match self {
            PeerMessage::Routed(r) => match r.body {
                RoutedMessageBody::QueryRequest { .. }
                | RoutedMessageBody::QueryResponse { .. }
                | RoutedMessageBody::TxStatusRequest(_, _)
                | RoutedMessageBody::TxStatusResponse(_)
                | RoutedMessageBody::ReceiptOutcomeRequest(_)
                | RoutedMessageBody::ReceiptOutComeResponse(_)
                | RoutedMessageBody::StateRequestHeader(_, _)
                | RoutedMessageBody::StateRequestPart(_, _, _) => true,
                _ => false,
            },
            PeerMessage::BlockHeadersRequest(_) => true,
            PeerMessage::BlockRequest(_) => true,
            PeerMessage::EpochSyncRequest(_) => true,
            PeerMessage::EpochSyncFinalizationRequest(_) => true,
            _ => false,
        }
    }
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
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone)]
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
#[rtype(result = "()")]
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
#[rtype(result = "()")]
pub struct OutboundTcpConnect {
    /// Peer information of the outbound connection
    pub peer_info: PeerInfo,
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct SendMessage {
    pub message: PeerMessage,
}

/// Actor message to consolidate potential new peer.
/// Returns if connection should be kept or dropped.
pub struct Consolidate {
    pub actor: Addr<Peer>,
    pub peer_info: PeerInfo,
    pub peer_type: PeerType,
    pub chain_info: PeerChainInfoV2,
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
    InvalidNonce(Box<Edge>),
    Reject,
}

/// Unregister message from Peer to PeerManager.
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

/// Message from peer to peer manager
#[derive(strum::AsRefStr)]
pub enum PeerRequest {
    UpdateEdge((PeerId, u64)),
    RouteBack(Box<RoutedMessageBody>, CryptoHash),
    UpdatePeerInfo(PeerInfo),
    ReceivedMessage(PeerId, Instant),
}

impl Message for PeerRequest {
    type Result = PeerResponse;
}

#[derive(MessageResponse)]
pub enum PeerResponse {
    NoResponse,
    UpdatedEdge(EdgeInfo),
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
#[derive(Message)]
#[rtype(result = "()")]
pub struct Ban {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

// TODO(#1313): Use Box
#[derive(Debug, Clone, PartialEq, strum::AsRefStr)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkRequests {
    /// Sends block, either when block was just produced or when requested.
    Block {
        block: Block,
    },
    /// Sends approval.
    Approval {
        approval_message: ApprovalMessage,
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
    /// Request state header for given shard at given state root.
    StateRequestHeader {
        shard_id: ShardId,
        sync_hash: CryptoHash,
        target: AccountOrPeerIdOrHash,
    },
    /// Request state part for given shard at given state root.
    StateRequestPart {
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: u64,
        target: AccountOrPeerIdOrHash,
    },
    /// Response to state request.
    StateResponse {
        route_back: CryptoHash,
        response: StateResponseInfo,
    },
    EpochSyncRequest {
        peer_id: PeerId,
        epoch_id: EpochId,
    },
    EpochSyncFinalizationRequest {
        peer_id: PeerId,
        epoch_id: EpochId,
    },
    /// Ban given peer.
    BanPeer {
        peer_id: PeerId,
        ban_reason: ReasonForBan,
    },
    /// Announce account
    AnnounceAccount(AnnounceAccount),

    /// Request chunk parts and/or receipts
    PartialEncodedChunkRequest {
        target: AccountIdOrPeerTrackingShard,
        request: PartialEncodedChunkRequestMsg,
    },
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunkResponse {
        route_back: CryptoHash,
        response: PartialEncodedChunkResponseMsg,
    },
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunkMessage {
        account_id: AccountId,
        partial_encoded_chunk: PartialEncodedChunkWithArcReceipts,
    },
    /// Forwarding a chunk part to a validator tracking the shard
    PartialEncodedChunkForward {
        account_id: AccountId,
        forward: PartialEncodedChunkForwardMsg,
    },

    /// Valid transaction but since we are not validators we send this transaction to current validators.
    ForwardTx(AccountId, SignedTransaction),
    /// Query transaction status
    TxStatus(AccountId, AccountId, CryptoHash),
    /// General query
    Query {
        query_id: String,
        account_id: AccountId,
        block_reference: BlockReference,
        request: QueryRequest,
    },
    /// Request for receipt execution outcome
    ReceiptOutComeRequest(AccountId, CryptoHash),

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
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum PeerManagerRequest {
    BanPeer(ReasonForBan),
    UnregisterPeer,
}

pub struct EdgeList {
    pub edges: Vec<Edge>,
    pub edges_info_shared: Arc<Mutex<HashMap<(PeerId, PeerId), u64>>>,
    pub sender: QueueSender<Edge>,
}

impl Message for EdgeList {
    type Result = bool;
}

/// Combines peer address info, chain and edge information.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FullPeerInfo {
    pub peer_info: PeerInfo,
    pub chain_info: PeerChainInfoV2,
    pub edge_info: EdgeInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KnownProducer {
    pub account_id: AccountId,
    pub addr: Option<SocketAddr>,
    pub peer_id: PeerId,
}

#[derive(Debug)]
pub struct NetworkInfo {
    pub active_peers: Vec<FullPeerInfo>,
    pub num_active_peers: usize,
    pub peer_max_count: u32,
    pub highest_height_peers: Vec<FullPeerInfo>,
    pub sent_bytes_per_sec: u64,
    pub received_bytes_per_sec: u64,
    /// Accounts of known block and chunk producers from routing table.
    pub known_producers: Vec<KnownProducer>,
    #[cfg(feature = "metric_recorder")]
    pub metric_recorder: MetricRecorder,
    pub peer_counter: usize,
}

impl<A, M> MessageResponse<A, M> for NetworkInfo
where
    A: Actor,
    M: Message<Result = NetworkInfo>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

#[derive(Debug)]
pub enum NetworkResponses {
    NoResponse,
    RoutingTableInfo(RoutingTableInfo),
    PingPongInfo { pings: HashMap<usize, Ping>, pongs: HashMap<usize, Pong> },
    BanPeer(ReasonForBan),
    EdgeUpdate(Box<Edge>),
    RouteNotFound,
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

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct StateResponseInfoV1 {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub state_response: ShardStateSyncResponseV1,
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct StateResponseInfoV2 {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub state_response: ShardStateSyncResponse,
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
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

#[cfg(feature = "adversarial")]
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

#[derive(Debug, strum::AsRefStr, AsStaticStr)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub enum NetworkClientMessages {
    #[cfg(feature = "adversarial")]
    Adversarial(NetworkAdversarialMessage),

    /// Received transaction.
    Transaction {
        transaction: SignedTransaction,
        /// Whether the transaction is forwarded from other nodes.
        is_forwarded: bool,
        /// Whether the transaction needs to be submitted.
        check_only: bool,
    },
    /// Received block, possibly requested.
    Block(Block, PeerId, bool),
    /// Received list of headers for syncing.
    BlockHeaders(Vec<BlockHeader>, PeerId),
    /// Block approval.
    BlockApproval(Approval, PeerId),
    /// State response.
    StateResponse(StateResponseInfo),
    /// Epoch Sync response for light client block request
    EpochSyncResponse(PeerId, EpochSyncResponse),
    /// Epoch Sync response for finalization request
    EpochSyncFinalizationResponse(PeerId, EpochSyncFinalizationResponse),

    /// Request chunk parts and/or receipts.
    PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg, CryptoHash),
    /// Response to a request for  chunk parts and/or receipts.
    PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg),
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunk(PartialEncodedChunk),
    /// Forwarding parts to those tracking the shard (so they don't need to send requests)
    PartialEncodedChunkForward(PartialEncodedChunkForwardMsg),

    /// A challenge to invalidate the block.
    Challenge(Challenge),

    NetworkInfo(NetworkInfo),
}

// TODO(#1313): Use Box
#[derive(Eq, PartialEq, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkClientResponses {
    /// Adv controls.
    #[cfg(feature = "adversarial")]
    AdvResult(u64),

    /// No response.
    NoResponse,
    /// Valid transaction inserted into mempool as response to Transaction.
    ValidTx,
    /// Invalid transaction inserted into mempool as response to Transaction.
    InvalidTx(InvalidTxError),
    /// The request is routed to other shards
    RequestRouted,
    /// The node being queried does not track the shard needed and therefore cannot provide userful
    /// response.
    DoesNotTrackShard,
    /// Ban peer for malicious behavior.
    Ban { ban_reason: ReasonForBan },
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

#[derive(AsStaticStr)]
pub enum NetworkViewClientMessages {
    #[cfg(feature = "adversarial")]
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

#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct PartialEncodedChunkRequestMsg {
    pub chunk_hash: ChunkHash,
    pub part_ords: Vec<u64>,
    pub tracking_shards: HashSet<ShardId>,
}

#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct PartialEncodedChunkResponseMsg {
    pub chunk_hash: ChunkHash,
    pub parts: Vec<PartialEncodedChunkPart>,
    pub receipts: Vec<ReceiptProof>,
}

/// Message for chunk part owners to forward their parts to validators tracking that shard.
/// This reduces the number of requests a node tracking a shard needs to send to obtain enough
/// parts to reconstruct the message (in the best case no such requests are needed).
#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize, Serialize)]
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

/// Adapter to break dependency of sub-components on the network requests.
/// For tests use MockNetworkAdapter that accumulates the requests to network.
pub trait NetworkAdapter: Sync + Send {
    fn send(
        &self,
        msg: NetworkRequests,
    ) -> BoxFuture<'static, Result<NetworkResponses, MailboxError>>;

    fn do_send(&self, msg: NetworkRequests);
}

pub struct NetworkRecipient {
    network_recipient: RwLock<Option<Recipient<NetworkRequests>>>,
}

unsafe impl Sync for NetworkRecipient {}

impl NetworkRecipient {
    pub fn new() -> Self {
        Self { network_recipient: RwLock::new(None) }
    }

    pub fn set_recipient(&self, network_recipient: Recipient<NetworkRequests>) {
        *self.network_recipient.write().unwrap() = Some(network_recipient);
    }
}

impl NetworkAdapter for NetworkRecipient {
    fn send(
        &self,
        msg: NetworkRequests,
    ) -> BoxFuture<'static, Result<NetworkResponses, MailboxError>> {
        self.network_recipient
            .read()
            .unwrap()
            .as_ref()
            .expect("Recipient must be set")
            .send(msg)
            .boxed()
    }

    fn do_send(&self, msg: NetworkRequests) {
        let _ = self
            .network_recipient
            .read()
            .unwrap()
            .as_ref()
            .expect("Recipient must be set")
            .do_send(msg);
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;

    const ALLOWED_SIZE: usize = 1 << 20;
    const NOTIFY_SIZE: usize = 1024;

    macro_rules! assert_size {
        ($type:ident) => {
            let struct_size = size_of::<$type>();
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
        assert_size!(HandshakeFailureReason);
        assert_size!(RoutedMessageBody);
        assert_size!(PeerIdOrHash);
        assert_size!(KnownPeerStatus);
        assert_size!(ConsolidateResponse);
        assert_size!(PeerRequest);
        assert_size!(PeerResponse);
        assert_size!(ReasonForBan);
        assert_size!(NetworkRequests);
        assert_size!(PeerManagerRequest);
        assert_size!(NetworkResponses);
        assert_size!(NetworkClientMessages);
        assert_size!(NetworkClientResponses);
    }

    #[test]
    fn test_struct_size() {
        assert_size!(PeerInfo);
        assert_size!(PeerChainInfoV2);
        assert_size!(Handshake);
        assert_size!(AnnounceAccountRoute);
        assert_size!(AnnounceAccount);
        assert_size!(Ping);
        assert_size!(Pong);
        assert_size!(RawRoutedMessage);
        assert_size!(RoutedMessageNoSignature);
        assert_size!(RoutedMessage);
        assert_size!(RoutedMessageFrom);
        assert_size!(SyncData);
        assert_size!(NetworkConfig);
        assert_size!(KnownPeerState);
        assert_size!(InboundTcpConnect);
        assert_size!(OutboundTcpConnect);
        assert_size!(SendMessage);
        assert_size!(Consolidate);
        assert_size!(Unregister);
        assert_size!(PeerList);
        assert_size!(PeersRequest);
        assert_size!(PeersResponse);
        assert_size!(Ban);
        assert_size!(FullPeerInfo);
        assert_size!(NetworkInfo);
        assert_size!(StateResponseInfoV1);
        assert_size!(QueryPeerStats);
        assert_size!(PartialEncodedChunkRequestMsg);
    }
}
