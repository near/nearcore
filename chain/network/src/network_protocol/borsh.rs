/// Contains types that belong to the `network protocol.
///
/// WARNING WARNING WARNING
/// WARNING WARNING WARNING
/// We need to maintain backwards compatibility, all changes to this file needs to be reviews.
use borsh::{BorshDeserialize, BorshSerialize};
use near_network_primitives::types::{
    Edge, PartialEdgeInfo, PeerChainInfoV2, PeerInfo, RoutedMessage,
};
use near_primitives::block::{Block, BlockHeader, GenesisId};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::syncing::{EpochSyncFinalizationResponse, EpochSyncResponse};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::EpochId;
use near_primitives::version::{PEER_MIN_ALLOWED_PROTOCOL_VERSION, PROTOCOL_VERSION};
use std::fmt::Formatter;
use std::{fmt, io};

const ERROR_UNEXPECTED_LENGTH_OF_INPUT: &str = "Unexpected length of input";

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
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
    fn deserialize(buf: &mut &[u8]) -> std::io::Result<Self> {
        // Detect the current and oldest supported version from the header
        if buf.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                ERROR_UNEXPECTED_LENGTH_OF_INPUT,
            ));
        }

        let version = u32::from_le_bytes(buf[..4].try_into().unwrap());

        if PEER_MIN_ALLOWED_PROTOCOL_VERSION <= version && version <= PROTOCOL_VERSION {
            // If we support this version, then try to deserialize with custom deserializer
            <HandshakeAutoDes as BorshDeserialize>::deserialize(buf).map(Into::into)
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

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct RoutingTableUpdate {
    pub(crate) edges: Vec<Edge>,
    pub(crate) accounts: Vec<AnnounceAccount>,
}

impl RoutingTableUpdate {
    pub(crate) fn from_edges(edges: Vec<Edge>) -> Self {
        Self { edges, accounts: Vec::new() }
    }

    pub fn from_accounts(accounts: Vec<AnnounceAccount>) -> Self {
        Self { edges: Vec::new(), accounts }
    }

    pub(crate) fn new(edges: Vec<Edge>, accounts: Vec<AnnounceAccount>) -> Self {
        Self { edges, accounts }
    }
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
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
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, strum::AsRefStr)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub enum PeerMessage {
    Handshake(Handshake),
    HandshakeFailure(PeerInfo, HandshakeFailureReason),
    /// When a failed nonce is used by some peer, this message is sent back as evidence.
    LastEdge(Edge),
    /// Contains accounts and edge information.
    SyncRoutingTable(RoutingTableUpdate),
    RequestUpdateNonce(PartialEdgeInfo),
    ResponseUpdateNonce(Edge),

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
    EpochSyncRequest(EpochId),
    EpochSyncResponse(Box<EpochSyncResponse>),
    EpochSyncFinalizationRequest(EpochId),
    EpochSyncFinalizationResponse(Box<EpochSyncFinalizationResponse>),

    RoutingTableSyncV2(RoutingSyncV2),
}
#[cfg(target_arch = "x86_64")] // Non-x86_64 doesn't match this requirement yet but it's not bad as it's not production-ready
const _: () = assert!(std::mem::size_of::<PeerMessage>() <= 1144, "PeerMessage > 1144 bytes");

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub enum RoutingSyncV2 {
    Version2(RoutingVersion2),
}
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
const _: () = assert!(std::mem::size_of::<RoutingSyncV2>() <= 80, "RoutingSyncV2 > 80 bytes");

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct PartialSync {
    pub(crate) ibf_level: crate::routing::ibf_peer_set::ValidIBFLevel,
    pub(crate) ibf: Vec<crate::routing::ibf::IbfBox>,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub enum RoutingState {
    PartialSync(PartialSync),
    RequestAllEdges,
    Done,
    RequestMissingEdges(Vec<u64>),
    InitializeIbf,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct RoutingVersion2 {
    pub(crate) known_edges: u64,
    pub(crate) seed: u64,
    pub(crate) edges: Vec<Edge>,
    pub(crate) routing_state: RoutingState,
}

impl fmt::Display for PeerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.msg_variant(), f)
    }
}

impl PeerMessage {
    pub(crate) fn msg_variant(&self) -> &str {
        if let PeerMessage::Routed(routed_message) = self {
            routed_message.body.as_ref()
        } else {
            self.as_ref()
        }
    }

    pub(crate) fn is_client_message(&self) -> bool {
        match self {
            PeerMessage::Block(_)
            | PeerMessage::BlockHeaders(_)
            | PeerMessage::Challenge(_)
            | PeerMessage::EpochSyncFinalizationResponse(_)
            | PeerMessage::EpochSyncResponse(_)
            | PeerMessage::Transaction(_) => true,
            PeerMessage::Routed(r) => matches!(
                r.body,
                RoutedMessageBody::BlockApproval(_)
                    | RoutedMessageBody::ForwardTx(_)
                    | RoutedMessageBody::PartialEncodedChunk(_)
                    | RoutedMessageBody::PartialEncodedChunkForward(_)
                    | RoutedMessageBody::PartialEncodedChunkRequest(_)
                    | RoutedMessageBody::PartialEncodedChunkResponse(_)
                    | RoutedMessageBody::StateResponse(_)
                    | RoutedMessageBody::VersionedPartialEncodedChunk(_)
                    | RoutedMessageBody::VersionedStateResponse(_)
            ),
            _ => false,
        }
    }

    pub(crate) fn is_view_client_message(&self) -> bool {
        match self {
            PeerMessage::BlockHeadersRequest(_)
            | PeerMessage::BlockRequest(_)
            | PeerMessage::EpochSyncFinalizationRequest(_)
            | PeerMessage::EpochSyncRequest(_) => true,
            PeerMessage::Routed(r) => matches!(
                r.body,
                RoutedMessageBody::QueryRequest { .. }
                    | RoutedMessageBody::QueryResponse { .. }
                    | RoutedMessageBody::ReceiptOutcomeRequest(_)
                    | RoutedMessageBody::StateRequestHeader(_, _)
                    | RoutedMessageBody::StateRequestPart(_, _, _)
                    | RoutedMessageBody::TxStatusRequest(_, _)
                    | RoutedMessageBody::TxStatusResponse(_)
            ),
            _ => false,
        }
    }
}
