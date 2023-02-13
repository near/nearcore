//! Contains types that belong to the `network protocol.
//!
//! WARNING WARNING WARNING
//! WARNING WARNING WARNING
//! We need to maintain backwards compatibility, all changes to this file needs to be reviews.
use crate::network_protocol::edge::{Edge, PartialEdgeInfo};
use crate::network_protocol::{PeerChainInfoV2, PeerInfo, RoutedMessage};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::block::{Block, BlockHeader, GenesisId};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::transaction::SignedTransaction;
use std::fmt;
use std::fmt::Formatter;

#[derive(BorshSerialize, PartialEq, Eq, Clone, Debug)]
/// Structure representing handshake between peers.
/// This replaces deprecated handshake `HandshakeV2`.
pub struct Handshake {
    /// Current protocol version.
    pub(crate) protocol_version: u32,
    /// Oldest supported protocol version.
    pub(crate) oldest_supported_version: u32,
    /// Sender's peer id.
    pub(crate) sender_peer_id: PeerId,
    /// Receiver's peer id.
    pub(crate) target_peer_id: PeerId,
    /// Sender's listening addr.
    pub(crate) sender_listen_port: Option<u16>,
    /// Peer's chain information.
    pub(crate) sender_chain_info: PeerChainInfoV2,
    /// Represents new `edge`. Contains only `none` and `Signature` from the sender.
    pub(crate) partial_edge_info: PartialEdgeInfo,
}

/// Struct describing the layout for Handshake.
/// It is used to automatically derive BorshDeserialize.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
struct HandshakeAutoDes {
    /// Protocol version.
    protocol_version: u32,
    /// Oldest supported protocol version.
    oldest_supported_version: u32,
    /// Sender's peer id.
    sender_peer_id: PeerId,
    /// Receiver's peer id.
    target_peer_id: PeerId,
    /// Sender's listening addr.
    sender_listen_port: Option<u16>,
    /// Peer's chain information.
    sender_chain_info: PeerChainInfoV2,
    /// Info for new edge.
    partial_edge_info: PartialEdgeInfo,
}

// Use custom deserializer for HandshakeV2. Try to read version of the other peer from the header.
// If the version is supported then fallback to standard deserializer.
impl BorshDeserialize for Handshake {
    fn deserialize_reader<R: std::io::Read>(rd: &mut R) -> std::io::Result<Self> {
        HandshakeAutoDes::deserialize_reader(rd).map(Into::into)
    }
}

impl From<HandshakeAutoDes> for Handshake {
    fn from(handshake: HandshakeAutoDes) -> Self {
        Self {
            protocol_version: handshake.protocol_version,
            oldest_supported_version: handshake.oldest_supported_version,
            sender_peer_id: handshake.sender_peer_id,
            target_peer_id: handshake.target_peer_id,
            sender_listen_port: handshake.sender_listen_port,
            sender_chain_info: handshake.sender_chain_info,
            partial_edge_info: handshake.partial_edge_info,
        }
    }
}

#[derive(Default, BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub(super) struct RoutingTableUpdate {
    pub edges: Vec<Edge>,
    pub accounts: Vec<AnnounceAccount>,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub enum HandshakeFailureReason {
    ProtocolVersionMismatch { version: u32, oldest_supported_version: u32 },
    GenesisMismatch(GenesisId),
    InvalidTarget,
}
const _: () = assert!(
    std::mem::size_of::<HandshakeFailureReason>() <= 64,
    "HandshakeFailureReason > 64 bytes"
);

impl fmt::Display for HandshakeFailureReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "HandshakeFailureReason")
    }
}

impl std::error::Error for HandshakeFailureReason {}

/// Warning, position of each message type in this enum defines the protocol due to serialization.
/// DO NOT MOVE, REORDER, DELETE items from the list. Only add new items to the end.
/// If need to remove old items - replace with `None`.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, strum::AsRefStr)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub(super) enum PeerMessage {
    Handshake(Handshake),
    HandshakeFailure(PeerInfo, HandshakeFailureReason),
    /// When a failed nonce is used by some peer, this message is sent back as evidence.
    LastEdge(Edge),
    /// Contains accounts and edge information.
    SyncRoutingTable(RoutingTableUpdate),
    RequestUpdateNonce(PartialEdgeInfo),
    _ResponseUpdateNonce,

    PeersRequest,
    PeersResponse(Vec<PeerInfo>),

    BlockHeadersRequest(Vec<CryptoHash>),
    BlockHeaders(Vec<BlockHeader>),

    BlockRequest(CryptoHash),
    Block(Block),

    Transaction(SignedTransaction),
    Routed(Box<RoutedMessage>),

    /// Gracefully disconnect from other peer.
    Disconnect,
    Challenge(Challenge),

    _HandshakeV2,
    _EpochSyncRequest,
    _EpochSyncResponse,
    _EpochSyncFinalizationRequest,
    _EpochSyncFinalizationResponse,
    _RoutingTableSyncV2,
}
#[cfg(target_arch = "x86_64")] // Non-x86_64 doesn't match this requirement yet but it's not bad as it's not production-ready
const _: () = assert!(std::mem::size_of::<PeerMessage>() <= 1144, "PeerMessage > 1144 bytes");
