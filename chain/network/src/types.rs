use std::collections::{HashMap, HashSet};
use std::convert::{Into, TryFrom, TryInto};
use std::fmt;
use std::net::{AddrParseError, IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::RwLock;
use std::time::Duration;

use actix::dev::{MessageResponse, ResponseChannel};
use actix::{Actor, Addr, MailboxError, Message, Recipient};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use futures::{future::BoxFuture, FutureExt};
use serde_derive::{Deserialize, Serialize};
use tokio::net::TcpStream;

use near_chain::types::ShardStateSyncResponse;
use near_chain::{Block, BlockHeader};
use near_chain_configs::PROTOCOL_VERSION;
use near_crypto::{PublicKey, SecretKey, Signature};
use near_metrics;
use near_primitives::block::{Approval, ApprovalMessage, BlockScore, GenesisId, ScoreAndHeight};
use near_primitives::challenge::Challenge;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::{ChunkHash, PartialEncodedChunk};
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, SignedTransaction};
use near_primitives::types::{AccountId, BlockHeight, BlockIdOrFinality, EpochId, ShardId};
use near_primitives::utils::{from_timestamp, to_timestamp};
use near_primitives::views::{FinalExecutionOutcomeView, QueryRequest, QueryResponse};

use crate::metrics;
use crate::peer::Peer;
use crate::routing::{Edge, EdgeInfo, RoutingTableInfo};

/// Number of hops a message is allowed to travel before being dropped.
/// This is used to avoid infinite loop because of inconsistent view of the network
/// by different nodes.
pub const ROUTED_MESSAGE_TTL: u8 = 100;

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
#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct PeerChainInfo {
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Last known chain height of the peer.
    pub height: BlockHeight,
    /// Last known chain score of the peer.
    pub score: BlockScore,
    /// Shards that the peer is tracking
    pub tracked_shards: Vec<ShardId>,
}

impl PeerChainInfo {
    pub fn score_and_height(&self) -> ScoreAndHeight {
        ScoreAndHeight { score: self.score, height: self.height }
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

/// Account route description
#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
pub struct AnnounceAccountRoute {
    pub peer_id: PeerId,
    pub hash: CryptoHash,
    pub signature: Signature,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
pub enum HandshakeFailureReason {
    ProtocolVersionMismatch(u32),
    GenesisMismatch(GenesisId),
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
#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum RoutedMessageBody {
    BlockApproval(Approval),
    ForwardTx(SignedTransaction),

    TxStatusRequest(AccountId, CryptoHash),
    TxStatusResponse(FinalExecutionOutcomeView),
    QueryRequest {
        query_id: String,
        block_id_or_finality: BlockIdOrFinality,
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
    StateResponse(StateResponseInfo),
    PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg),
    PartialEncodedChunk(PartialEncodedChunk),
    /// Ping/Pong used for testing networking and routing.
    Ping(Ping),
    Pong(Pong),
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
pub enum PeerIdOrHash {
    PeerId(PeerId),
    Hash(CryptoHash),
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
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
        let hash = RoutedMessage::build_hash(target.clone(), author.clone(), self.body.clone());
        let signature = secret_key.sign(hash.as_ref());
        RoutedMessage { target, author, signature, ttl: routed_message_ttl, body: self.body }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
pub struct RoutedMessageNoSignature {
    target: PeerIdOrHash,
    author: PeerId,
    body: RoutedMessageBody,
}

/// RoutedMessage represent a package that will travel the network towards a specific peer id.
/// It contains the peer_id and signature from the original sender. Every intermediate peer in the
/// route must verify that this signature is valid otherwise previous sender of this package should
/// be banned. If the final receiver of this package finds that the body is invalid the original
/// sender of the package should be banned instead.
/// If target is hash, it is a message that should be routed back using the same path used to route
/// the request in first place. It is the hash of the request message.
#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
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

#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
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

    BlockRequest(CryptoHash),
    Block(Block),

    Transaction(SignedTransaction),
    Routed(RoutedMessage),

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
            PeerMessage::BlockRequest(_) => f.write_str("BlockRequest"),
            PeerMessage::Block(_) => f.write_str("Block"),
            PeerMessage::Transaction(_) => f.write_str("Transaction"),
            PeerMessage::Routed(routed_message) => match routed_message.body {
                RoutedMessageBody::BlockApproval(_) => f.write_str("BlockApproval"),
                RoutedMessageBody::ForwardTx(_) => f.write_str("ForwardTx"),
                RoutedMessageBody::TxStatusRequest(_, _) => f.write_str("Transaction status query"),
                RoutedMessageBody::TxStatusResponse(_) => {
                    f.write_str("Transaction status response")
                }
                RoutedMessageBody::QueryRequest { .. } => f.write_str("Query request"),
                RoutedMessageBody::QueryResponse { .. } => f.write_str("Query response"),
                RoutedMessageBody::StateRequestHeader(_, _) => f.write_str("StateResponseHeader"),
                RoutedMessageBody::StateRequestPart(_, _, _) => f.write_str("StateResponsePart"),
                RoutedMessageBody::StateResponse(_) => f.write_str("StateResponse"),
                RoutedMessageBody::ReceiptOutcomeRequest(_) => {
                    f.write_str("Receipt outcome request")
                }
                RoutedMessageBody::ReceiptOutComeResponse(_) => {
                    f.write_str("Receipt outcome response")
                }
                RoutedMessageBody::PartialEncodedChunkRequest(_) => {
                    f.write_str("PartialEncodedChunkRequest")
                }
                RoutedMessageBody::PartialEncodedChunk(_) => f.write_str("PartialEncodedChunk"),
                RoutedMessageBody::Ping(_) => f.write_str("Ping"),
                RoutedMessageBody::Pong(_) => f.write_str("Pong"),
            },
            PeerMessage::Disconnect => f.write_str("Disconnect"),
            PeerMessage::Challenge(_) => f.write_str("Challenge"),
        }
    }
}

impl PeerMessage {
    pub fn record(&self, size: usize) {
        match self {
            PeerMessage::Handshake(_) => {
                near_metrics::inc_counter(&metrics::HANDSHAKE_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(&metrics::HANDSHAKE_RECEIVED_BYTES, size as i64);
            }
            PeerMessage::HandshakeFailure(_, _) => {
                near_metrics::inc_counter(&metrics::HANDSHAKE_FAILURE_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(
                    &metrics::HANDSHAKE_FAILURE_RECEIVED_BYTES,
                    size as i64,
                );
            }
            PeerMessage::Sync(_) => {
                near_metrics::inc_counter(&metrics::SYNC_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(&metrics::SYNC_RECEIVED_BYTES, size as i64);
            }
            PeerMessage::RequestUpdateNonce(_) => {
                near_metrics::inc_counter(&metrics::REQUEST_UPDATE_NONCE_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(
                    &metrics::REQUEST_UPDATE_NONCE_RECEIVED_BYTES,
                    size as i64,
                );
            }
            PeerMessage::ResponseUpdateNonce(_) => {
                near_metrics::inc_counter(&metrics::RESPONSE_UPDATE_NONCE_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(
                    &metrics::RESPONSE_UPDATE_NONCE_RECEIVED_BYTES,
                    size as i64,
                );
            }
            PeerMessage::LastEdge(_) => {
                near_metrics::inc_counter(&metrics::LAST_EDGE_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(&metrics::LAST_EDGE_RECEIVED_BYTES, size as i64);
            }
            PeerMessage::PeersRequest => {
                near_metrics::inc_counter(&metrics::PEERS_REQUEST_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(&metrics::PEERS_REQUEST_RECEIVED_BYTES, size as i64);
            }
            PeerMessage::PeersResponse(_) => {
                near_metrics::inc_counter(&metrics::PEERS_RESPONSE_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(&metrics::PEERS_RESPONSE_RECEIVED_BYTES, size as i64);
            }
            PeerMessage::BlockHeadersRequest(_) => {
                near_metrics::inc_counter(&metrics::BLOCK_HEADERS_REQUEST_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(
                    &metrics::BLOCK_HEADERS_REQUEST_RECEIVED_BYTES,
                    size as i64,
                );
            }
            PeerMessage::BlockHeaders(_) => {
                near_metrics::inc_counter(&metrics::BLOCK_HEADERS_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(&metrics::BLOCK_HEADERS_RECEIVED_BYTES, size as i64);
            }
            PeerMessage::BlockRequest(_) => {
                near_metrics::inc_counter(&metrics::BLOCK_REQUEST_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(&metrics::BLOCK_REQUEST_RECEIVED_BYTES, size as i64);
            }
            PeerMessage::Block(_) => {
                near_metrics::inc_counter(&metrics::BLOCK_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(&metrics::BLOCK_RECEIVED_BYTES, size as i64);
            }
            PeerMessage::Transaction(_) => {
                near_metrics::inc_counter(&metrics::TRANSACTION_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(&metrics::TRANSACTION_RECEIVED_BYTES, size as i64);
            }
            PeerMessage::Routed(routed_message) => match routed_message.body {
                RoutedMessageBody::BlockApproval(_) => {
                    near_metrics::inc_counter(&metrics::ROUTED_BLOCK_APPROVAL_RECEIVED_TOTAL);
                    near_metrics::inc_counter_by(
                        &metrics::ROUTED_BLOCK_APPROVAL_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::ForwardTx(_) => {
                    near_metrics::inc_counter(&metrics::ROUTED_FORWARD_TX_RECEIVED_TOTAL);
                    near_metrics::inc_counter_by(
                        &metrics::ROUTED_FORWARD_TX_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::TxStatusRequest(_, _) => {
                    near_metrics::inc_counter(&metrics::ROUTED_TX_STATUS_REQUEST_RECEIVED_TOTAL);
                    near_metrics::inc_counter_by(
                        &metrics::ROUTED_TX_STATUS_REQUEST_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::TxStatusResponse(_) => {
                    near_metrics::inc_counter(&metrics::ROUTED_TX_STATUS_RESPONSE_RECEIVED_TOTAL);
                    near_metrics::inc_counter_by(
                        &metrics::ROUTED_TX_STATUS_RESPONSE_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::QueryRequest { .. } => {
                    near_metrics::inc_counter(&metrics::ROUTED_QUERY_REQUEST_RECEIVED_TOTAL);
                    near_metrics::inc_counter_by(
                        &metrics::ROUTED_QUERY_REQUEST_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::QueryResponse { .. } => {
                    near_metrics::inc_counter(&metrics::ROUTED_QUERY_RESPONSE_RECEIVED_TOTAL);
                    near_metrics::inc_counter_by(
                        &metrics::ROUTED_QUERY_RESPONSE_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::ReceiptOutcomeRequest(_) => {
                    near_metrics::inc_counter(
                        &metrics::ROUTED_RECEIPT_OUTCOME_REQUEST_RECEIVED_TOTAL,
                    );
                    near_metrics::inc_counter_by(
                        &metrics::ROUTED_RECEIPT_OUTCOME_REQUEST_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::ReceiptOutComeResponse(_) => {
                    near_metrics::inc_counter(
                        &metrics::ROUTED_RECEIPT_OUTCOME_RESPONSE_RECEIVED_TOTAL,
                    );
                    near_metrics::inc_counter_by(
                        &metrics::ROUTED_RECEIPT_OUTCOME_RESPONSE_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::StateRequestHeader(_, _) => {
                    near_metrics::inc_counter(&metrics::ROUTED_STATE_REQUEST_HEADER_RECEIVED_TOTAL);
                    near_metrics::inc_counter_by(
                        &metrics::ROUTED_STATE_REQUEST_HEADER_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::StateRequestPart(_, _, _) => {
                    near_metrics::inc_counter(&metrics::ROUTED_STATE_REQUEST_PART_RECEIVED_TOTAL);
                    near_metrics::inc_counter_by(
                        &metrics::ROUTED_STATE_REQUEST_PART_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::StateResponse(_) => {
                    near_metrics::inc_counter(&metrics::STATE_RESPONSE_RECEIVED_TOTAL);
                    near_metrics::inc_counter_by(
                        &metrics::STATE_RESPONSE_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::PartialEncodedChunkRequest(_) => {
                    near_metrics::inc_counter(
                        &metrics::ROUTED_PARTIAL_CHUNK_REQUEST_RECEIVED_TOTAL,
                    );
                    near_metrics::inc_counter_by(
                        &metrics::ROUTED_PARTIAL_CHUNK_REQUEST_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::PartialEncodedChunk(_) => {
                    near_metrics::inc_counter(&metrics::ROUTED_PARTIAL_CHUNK_RECEIVED_TOTAL);
                    near_metrics::inc_counter_by(
                        &metrics::ROUTED_PARTIAL_CHUNK_RECEIVED_BYTES,
                        size as i64,
                    );
                }
                RoutedMessageBody::Ping(_) => {
                    near_metrics::inc_counter(&metrics::ROUTED_PING_RECEIVED_TOTAL);
                    near_metrics::inc_counter_by(&metrics::ROUTED_PING_RECEIVED_BYTES, size as i64);
                }
                RoutedMessageBody::Pong(_) => {
                    near_metrics::inc_counter(&metrics::ROUTED_PONG_RECEIVED_TOTAL);
                    near_metrics::inc_counter_by(&metrics::ROUTED_PONG_RECEIVED_BYTES, size as i64);
                }
            },
            PeerMessage::Disconnect => {
                near_metrics::inc_counter(&metrics::DISCONNECT_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(&metrics::DISCONNECT_RECEIVED_BYTES, size as i64);
            }
            PeerMessage::Challenge(_) => {
                near_metrics::inc_counter(&metrics::CHALLENGE_RECEIVED_TOTAL);
                near_metrics::inc_counter_by(&metrics::CHALLENGE_RECEIVED_BYTES, size as i64);
            }
        }
    }

    pub fn is_client_message(&self) -> bool {
        match self {
            PeerMessage::Block(_)
            | PeerMessage::BlockHeaders(_)
            | PeerMessage::Transaction(_)
            | PeerMessage::Challenge(_) => true,
            PeerMessage::Routed(r) => match r.body {
                RoutedMessageBody::BlockApproval(_)
                | RoutedMessageBody::ForwardTx(_)
                | RoutedMessageBody::PartialEncodedChunk(_)
                | RoutedMessageBody::PartialEncodedChunkRequest(_)
                | RoutedMessageBody::StateResponse(_) => true,
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
    pub max_peer: u32,
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
#[derive(BorshSerialize, BorshDeserialize, Serialize, Eq, PartialEq, Debug)]
pub enum KnownPeerStatus {
    Unknown,
    NotConnected,
    Connected,
    Banned(ReasonForBan, u64),
}

/// Information node stores about known peers.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug)]
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
#[rtype(result = "()")]
pub struct Unregister {
    pub peer_id: PeerId,
}

pub struct PeerList {
    pub peers: Vec<PeerInfo>,
}

/// Message from peer to peer manager
pub enum PeerRequest {
    UpdateEdge((PeerId, u64)),
    RouteBack(RoutedMessageBody, CryptoHash),
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
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Ban {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

// TODO(#1313): Use Box
#[derive(Debug, Clone, PartialEq)]
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
    /// Ban given peer.
    BanPeer {
        peer_id: PeerId,
        ban_reason: ReasonForBan,
    },
    /// Announce account
    AnnounceAccount(AnnounceAccount),

    /// Request chunk parts and/or receipts
    PartialEncodedChunkRequest {
        account_id: AccountId,
        request: PartialEncodedChunkRequestMsg,
    },
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunkResponse {
        route_back: CryptoHash,
        partial_encoded_chunk: PartialEncodedChunk,
    },
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunkMessage {
        account_id: AccountId,
        partial_encoded_chunk: PartialEncodedChunk,
    },

    /// Valid transaction but since we are not validators we send this transaction to current validators.
    ForwardTx(AccountId, SignedTransaction),
    /// Query transaction status
    TxStatus(AccountId, AccountId, CryptoHash),
    /// General query
    Query {
        query_id: String,
        account_id: AccountId,
        block_id_or_finality: BlockIdOrFinality,
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
#[derive(Message)]
#[rtype(result = "()")]
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
    EdgeUpdate(Edge),
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
pub struct StateResponseInfo {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub state_response: ShardStateSyncResponse,
}

#[cfg(feature = "adversarial")]
#[derive(Debug)]
pub enum NetworkAdversarialMessage {
    AdvProduceBlocks(u64, bool),
    AdvDisableHeaderSync,
    AdvDisableDoomslug,
    AdvGetSavedBlocks,
    AdvSetSyncInfo(u64, u64),
}

#[derive(Debug)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub enum NetworkClientMessages {
    #[cfg(feature = "adversarial")]
    Adversarial(NetworkAdversarialMessage),

    /// Received transaction.
    Transaction(SignedTransaction),
    /// Received block, possibly requested.
    Block(Block, PeerId, bool),
    /// Received list of headers for syncing.
    BlockHeaders(Vec<BlockHeader>, PeerId),
    /// Block approval.
    BlockApproval(Approval, PeerId),
    /// State response.
    StateResponse(StateResponseInfo),

    /// Request chunk parts and/or receipts.
    PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg, CryptoHash),
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunk(PartialEncodedChunk),

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
    AdvU64(u64),

    /// No response.
    NoResponse,
    /// Valid transaction inserted into mempool as response to Transaction.
    ValidTx,
    /// Invalid transaction inserted into mempool as response to Transaction.
    InvalidTx(InvalidTxError),
    /// The request is routed to other shards
    RequestRouted,
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

pub enum NetworkViewClientMessages {
    #[cfg(feature = "adversarial")]
    Adversarial(NetworkAdversarialMessage),

    /// Transaction status query
    TxStatus { tx_hash: CryptoHash, signer_account_id: AccountId },
    /// Transaction status response
    TxStatusResponse(FinalExecutionOutcomeView),
    /// General query
    Query { query_id: String, block_id_or_finality: BlockIdOrFinality, request: QueryRequest },
    /// Query response
    QueryResponse { query_id: String, response: Result<QueryResponse, String> },
    /// Request for receipt outcome
    ReceiptOutcomeRequest(CryptoHash),
    /// Receipt outcome response
    ReceiptOutcomeResponse(ExecutionOutcomeWithIdAndProof),
    /// Request a block.
    BlockRequest(CryptoHash),
    /// Request headers.
    BlockHeadersRequest(Vec<CryptoHash>),
    /// State request header.
    StateRequestHeader { shard_id: ShardId, sync_hash: CryptoHash },
    /// State request part.
    StateRequestPart { shard_id: ShardId, sync_hash: CryptoHash, part_id: u64 },
    /// Get Chain information from Client.
    GetChainInfo,
    /// Account announcements that needs to be validated before being processed.
    /// They are paired with last epoch id known to this announcement, in order to accept only
    /// newer announcements.
    AnnounceAccount(Vec<(AnnounceAccount, Option<EpochId>)>),
}

pub enum NetworkViewClientResponses {
    /// Transaction execution outcome
    TxStatus(FinalExecutionOutcomeView),
    /// Response to general queries
    QueryResponse { query_id: String, response: Result<QueryResponse, String> },
    /// Receipt outcome response
    ReceiptOutcomeResponse(ExecutionOutcomeWithIdAndProof),
    /// Block response.
    Block(Block),
    /// Headers response.
    BlockHeaders(Vec<BlockHeader>),
    /// Chain information.
    ChainInfo {
        genesis_id: GenesisId,
        height: BlockHeight,
        score: BlockScore,
        tracked_shards: Vec<ShardId>,
    },
    /// Response to state request.
    StateResponse(StateResponseInfo),
    /// Valid announce accounts.
    AnnounceAccount(Vec<AnnounceAccount>),
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

#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct PartialEncodedChunkRequestMsg {
    pub chunk_hash: ChunkHash,
    pub part_ords: Vec<u64>,
    pub tracking_shards: HashSet<ShardId>,
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
        assert_size!(PeerChainInfo);
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
        assert_size!(StateResponseInfo);
        assert_size!(QueryPeerStats);
        assert_size!(PartialEncodedChunkRequestMsg);
    }
}
