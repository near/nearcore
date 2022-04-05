/// Contains types that belong to the `network protocol.
mod borsh;
mod borsh_conv;
mod proto {
    include!(concat!(env!("OUT_DIR"), "/network.rs"));
}
mod proto_conv;

pub use self::borsh::{
    PartialSync, RoutingState, RoutingSyncV2, RoutingTableUpdate, RoutingVersion2,
};

use ::borsh::{BorshDeserialize as _, BorshSerialize as _};
use ::prost::Message as _;
use near_network_primitives::types::{
    Edge, PartialEdgeInfo, PeerChainInfoV2, PeerInfo, RoutedMessage, RoutedMessageBody,
};
use near_primitives::block::{Block, BlockHeader, GenesisId};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::syncing::{EpochSyncFinalizationResponse, EpochSyncResponse};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{EpochId, ProtocolVersion};
use near_primitives::version::PEER_MIN_ALLOWED_PROTOCOL_VERSION;
use std::fmt;

#[derive(PartialEq, Eq, Clone, Debug)]
/// Structure representing handshake between peers.
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

impl Handshake {
    pub(crate) fn new(
        version: ProtocolVersion,
        peer_id: PeerId,
        target_peer_id: PeerId,
        listen_port: Option<u16>,
        chain_info: PeerChainInfoV2,
        partial_edge_info: PartialEdgeInfo,
    ) -> Self {
        Handshake {
            protocol_version: version,
            oldest_supported_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION,
            sender_peer_id: peer_id,
            target_peer_id,
            sender_listen_port: listen_port,
            sender_chain_info: chain_info,
            partial_edge_info,
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum HandshakeFailureReason {
    ProtocolVersionMismatch { version: u32, oldest_supported_version: u32 },
    GenesisMismatch(GenesisId),
    InvalidTarget,
}

#[derive(PartialEq, Eq, Clone, Debug, strum::AsStaticStr, strum::EnumVariantNames)]
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
    EpochSyncRequest(EpochId),
    EpochSyncResponse(Box<EpochSyncResponse>),
    EpochSyncFinalizationRequest(EpochId),
    EpochSyncFinalizationResponse(Box<EpochSyncFinalizationResponse>),

    RoutingTableSyncV2(RoutingSyncV2),
}

impl fmt::Display for PeerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.msg_variant(), f)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Encoding {
    Borsh,
    Proto,
}

impl PeerMessage {
    pub(crate) fn serialize(&self, enc: Encoding) -> anyhow::Result<Vec<u8>> {
        Ok(match enc {
            Encoding::Borsh => borsh::PeerMessage::try_from(self)?.try_to_vec()?,
            Encoding::Proto => proto::PeerMessage::try_from(self)?.encode_to_vec(),
        })
    }

    pub(crate) fn deserialize(enc: Encoding, data: &[u8]) -> anyhow::Result<PeerMessage> {
        Ok(match enc {
            Encoding::Borsh => (&borsh::PeerMessage::try_from_slice(data)?).try_into()?,
            Encoding::Proto => (&proto::PeerMessage::decode(data)?).try_into()?,
        })
    }

    pub(crate) fn msg_variant(&self) -> &str {
        match self {
            PeerMessage::Routed(routed_message) => {
                strum::AsStaticRef::as_static(&routed_message.body)
            }
            _ => strum::AsStaticRef::as_static(self),
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
