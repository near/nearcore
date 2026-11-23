//! Kinds of network messages.
//!
//! A kind identifies the type of a `PeerMessage` (and of the body of a routed message) without
//! carrying its contents. It is sent in the header of a borsh-encoded message, so that the
//! receiver can decide how to handle the message (e.g. enforce the size limit for its type)
//! before decoding the body.
use super::{
    MAX_HUGE_MESSAGE_SIZE, MAX_LARGE_MESSAGE_SIZE, MAX_MEDIUM_MESSAGE_SIZE, MAX_SMALL_MESSAGE_SIZE,
    PeerMessage, T1MessageBody, T2MessageBody, TieredMessageBody,
};
use crate::config::DecodeMemoryTier;
use borsh::{BorshDeserialize, BorshSerialize};

/// Kind of a `PeerMessage`.
#[derive(BorshSerialize, BorshDeserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum PeerMessageKind {
    Tier1Handshake = 0,
    Tier2Handshake = 1,
    Tier3Handshake = 2,
    HandshakeFailure = 3,
    LastEdge = 4,
    SyncRoutingTable = 5,
    RequestUpdateNonce = 6,
    SyncAccountsData = 7,
    PeersRequest = 8,
    PeersResponse = 9,
    BlockHeadersRequest = 10,
    BlockHeaders = 11,
    BlockRequest = 12,
    Block = 13,
    OptimisticBlock = 14,
    Transaction = 15,
    Routed(TieredMessageKind) = 16,
    Disconnect = 17,
    Challenge = 18,
    SyncSnapshotHosts = 19,
    StateRequestHeader = 20,
    StateRequestPart = 21,
    StateRequestAck = 22,
    VersionedStateResponse = 23,
    EpochSyncRequest = 24,
    EpochSyncResponse = 25,
}

/// Kind of a `TieredMessageBody`.
#[derive(BorshSerialize, BorshDeserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum TieredMessageKind {
    T1(T1MessageKind) = 0,
    T2(T2MessageKind) = 1,
}

/// Kind of a `T1MessageBody`. Discriminants match the ones of `T1MessageBody`.
#[derive(BorshSerialize, BorshDeserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum T1MessageKind {
    BlockApproval = 0,
    VersionedPartialEncodedChunk = 1,
    PartialEncodedChunkForward = 2,
    PartialEncodedStateWitness = 3,
    PartialEncodedStateWitnessForward = 4,
    VersionedChunkEndorsement = 5,
    ChunkContractAccesses = 6,
    ContractCodeRequest = 7,
    ContractCodeResponse = 8,
    SpicePartialData = 9,
    SpiceChunkEndorsement = 10,
    SpiceDataRequest = 11,
    SpiceChunkContractAccesses = 12,
    SpiceContractCodeRequest = 13,
    SpiceContractCodeResponse = 14,
    VersionedPartialEncodedStateWitness = 15,
    VersionedPartialEncodedStateWitnessForward = 16,
}

/// Kind of a `T2MessageBody`. Discriminants match the ones of `T2MessageBody`.
#[derive(BorshSerialize, BorshDeserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum T2MessageKind {
    ForwardTx = 0,
    TxStatusRequest = 1,
    TxStatusResponse = 2,
    PartialEncodedChunkRequest = 3,
    PartialEncodedChunkResponse = 4,
    Ping = 5,
    Pong = 6,
    ChunkStateWitnessAck = 7,
    StatePartRequest = 8,
    PartialEncodedContractDeploys = 9,
    StateHeaderRequest = 10,
    StateRequestAck = 11,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(strum::EnumIter))]
pub(crate) enum MessageSizeClass {
    Small,
    Medium,
    Large,
    Huge,
}

impl MessageSizeClass {
    pub(crate) fn max_size(self) -> usize {
        match self {
            MessageSizeClass::Small => MAX_SMALL_MESSAGE_SIZE,
            MessageSizeClass::Medium => MAX_MEDIUM_MESSAGE_SIZE,
            MessageSizeClass::Large => MAX_LARGE_MESSAGE_SIZE,
            MessageSizeClass::Huge => MAX_HUGE_MESSAGE_SIZE,
        }
    }

    pub(crate) fn decode_memory_tier(self) -> DecodeMemoryTier {
        match self {
            MessageSizeClass::Small => DecodeMemoryTier::Low,
            MessageSizeClass::Medium => DecodeMemoryTier::Medium,
            MessageSizeClass::Large | MessageSizeClass::Huge => DecodeMemoryTier::High,
        }
    }
}

impl PeerMessageKind {
    /// Maximum allowed size (in bytes) of the encoded form of a message of this kind.
    pub(crate) fn max_size(&self) -> usize {
        self.size_class().max_size()
    }

    // Unused until message bodies are decoded with a memory limit.
    #[allow(dead_code)]
    pub(crate) fn decode_memory_tier(&self) -> DecodeMemoryTier {
        self.size_class().decode_memory_tier()
    }

    pub(crate) fn size_class(&self) -> MessageSizeClass {
        use T1MessageKind::*;
        use T2MessageKind::*;
        use TieredMessageKind::{T1, T2};

        match self {
            PeerMessageKind::BlockRequest
            | PeerMessageKind::BlockHeadersRequest
            | PeerMessageKind::StateRequestHeader
            | PeerMessageKind::StateRequestPart
            | PeerMessageKind::StateRequestAck
            | PeerMessageKind::RequestUpdateNonce
            | PeerMessageKind::Tier1Handshake
            | PeerMessageKind::Tier2Handshake
            | PeerMessageKind::Tier3Handshake
            | PeerMessageKind::Disconnect
            | PeerMessageKind::HandshakeFailure
            | PeerMessageKind::LastEdge
            | PeerMessageKind::PeersRequest
            | PeerMessageKind::Challenge // challenges are disabled
            | PeerMessageKind::EpochSyncRequest => MessageSizeClass::Small,

            PeerMessageKind::PeersResponse
            | PeerMessageKind::Transaction
            | PeerMessageKind::SyncRoutingTable
            | PeerMessageKind::SyncAccountsData
            | PeerMessageKind::SyncSnapshotHosts
            | PeerMessageKind::BlockHeaders
            | PeerMessageKind::Block
            | PeerMessageKind::OptimisticBlock => MessageSizeClass::Medium,

            PeerMessageKind::VersionedStateResponse => MessageSizeClass::Large,
            PeerMessageKind::EpochSyncResponse => MessageSizeClass::Huge,
            PeerMessageKind::Routed(kind) => match kind {
                T1(kind) => match kind {
                    BlockApproval | VersionedChunkEndorsement | SpiceChunkEndorsement => {
                        MessageSizeClass::Small
                    }

                    PartialEncodedChunkForward
                    | ChunkContractAccesses
                    | ContractCodeRequest
                    | SpiceDataRequest
                    | SpiceChunkContractAccesses
                    | SpiceContractCodeRequest
                    | VersionedPartialEncodedChunk => MessageSizeClass::Medium,

                    PartialEncodedStateWitness
                    | PartialEncodedStateWitnessForward
                    | VersionedPartialEncodedStateWitness
                    | VersionedPartialEncodedStateWitnessForward
                    | ContractCodeResponse
                    | SpicePartialData
                    | SpiceContractCodeResponse => MessageSizeClass::Large,
                },
                T2(kind) => match kind {
                    TxStatusRequest
                    | Ping
                    | Pong
                    | ChunkStateWitnessAck
                    | StatePartRequest
                    | StateHeaderRequest
                    | StateRequestAck => MessageSizeClass::Small,

                    ForwardTx
                    | PartialEncodedChunkRequest
                    | TxStatusResponse
                    | PartialEncodedChunkResponse => MessageSizeClass::Medium,

                    PartialEncodedContractDeploys => MessageSizeClass::Large,
                },
            },
        }
    }
}

impl PeerMessage {
    pub(crate) fn kind(&self) -> PeerMessageKind {
        match self {
            PeerMessage::Tier1Handshake(_) => PeerMessageKind::Tier1Handshake,
            PeerMessage::Tier2Handshake(_) => PeerMessageKind::Tier2Handshake,
            PeerMessage::Tier3Handshake(_) => PeerMessageKind::Tier3Handshake,
            PeerMessage::HandshakeFailure(_, _) => PeerMessageKind::HandshakeFailure,
            PeerMessage::LastEdge(_) => PeerMessageKind::LastEdge,
            PeerMessage::SyncRoutingTable(_) => PeerMessageKind::SyncRoutingTable,
            PeerMessage::RequestUpdateNonce(_) => PeerMessageKind::RequestUpdateNonce,
            PeerMessage::SyncAccountsData(_) => PeerMessageKind::SyncAccountsData,
            PeerMessage::PeersRequest(_) => PeerMessageKind::PeersRequest,
            PeerMessage::PeersResponse(_) => PeerMessageKind::PeersResponse,
            PeerMessage::BlockHeadersRequest(_) => PeerMessageKind::BlockHeadersRequest,
            PeerMessage::BlockHeaders(_) => PeerMessageKind::BlockHeaders,
            PeerMessage::BlockRequest(_) => PeerMessageKind::BlockRequest,
            PeerMessage::Block(_) => PeerMessageKind::Block,
            PeerMessage::OptimisticBlock(_) => PeerMessageKind::OptimisticBlock,
            PeerMessage::Transaction(_) => PeerMessageKind::Transaction,
            PeerMessage::Routed(msg) => PeerMessageKind::Routed(msg.body().kind()),
            PeerMessage::Disconnect(_) => PeerMessageKind::Disconnect,
            PeerMessage::Challenge(_) => PeerMessageKind::Challenge,
            PeerMessage::SyncSnapshotHosts(_) => PeerMessageKind::SyncSnapshotHosts,
            PeerMessage::StateRequestHeader(_, _) => PeerMessageKind::StateRequestHeader,
            PeerMessage::StateRequestPart(_, _, _) => PeerMessageKind::StateRequestPart,
            PeerMessage::StateRequestAck(_) => PeerMessageKind::StateRequestAck,
            PeerMessage::VersionedStateResponse(_) => PeerMessageKind::VersionedStateResponse,
            PeerMessage::EpochSyncRequest => PeerMessageKind::EpochSyncRequest,
            PeerMessage::EpochSyncResponse(_) => PeerMessageKind::EpochSyncResponse,
        }
    }
}

impl TieredMessageBody {
    pub fn kind(&self) -> TieredMessageKind {
        match self {
            TieredMessageBody::T1(body) => TieredMessageKind::T1(body.kind()),
            TieredMessageBody::T2(body) => TieredMessageKind::T2(body.kind()),
        }
    }
}

impl T1MessageBody {
    pub fn kind(&self) -> T1MessageKind {
        match self {
            T1MessageBody::BlockApproval(_) => T1MessageKind::BlockApproval,
            T1MessageBody::VersionedPartialEncodedChunk(_) => {
                T1MessageKind::VersionedPartialEncodedChunk
            }
            T1MessageBody::PartialEncodedChunkForward(_) => {
                T1MessageKind::PartialEncodedChunkForward
            }
            T1MessageBody::PartialEncodedStateWitness(_) => {
                T1MessageKind::PartialEncodedStateWitness
            }
            T1MessageBody::PartialEncodedStateWitnessForward(_) => {
                T1MessageKind::PartialEncodedStateWitnessForward
            }
            T1MessageBody::VersionedChunkEndorsement(_) => T1MessageKind::VersionedChunkEndorsement,
            T1MessageBody::ChunkContractAccesses(_) => T1MessageKind::ChunkContractAccesses,
            T1MessageBody::ContractCodeRequest(_) => T1MessageKind::ContractCodeRequest,
            T1MessageBody::ContractCodeResponse(_) => T1MessageKind::ContractCodeResponse,
            T1MessageBody::SpicePartialData(_) => T1MessageKind::SpicePartialData,
            T1MessageBody::SpiceChunkEndorsement(_) => T1MessageKind::SpiceChunkEndorsement,
            T1MessageBody::SpiceDataRequest(_) => T1MessageKind::SpiceDataRequest,
            T1MessageBody::SpiceChunkContractAccesses(_) => {
                T1MessageKind::SpiceChunkContractAccesses
            }
            T1MessageBody::SpiceContractCodeRequest(_) => T1MessageKind::SpiceContractCodeRequest,
            T1MessageBody::SpiceContractCodeResponse(_) => T1MessageKind::SpiceContractCodeResponse,
            T1MessageBody::VersionedPartialEncodedStateWitness(_) => {
                T1MessageKind::VersionedPartialEncodedStateWitness
            }
            T1MessageBody::VersionedPartialEncodedStateWitnessForward(_) => {
                T1MessageKind::VersionedPartialEncodedStateWitnessForward
            }
        }
    }
}

impl T2MessageBody {
    pub fn kind(&self) -> T2MessageKind {
        match self {
            T2MessageBody::ForwardTx(_) => T2MessageKind::ForwardTx,
            T2MessageBody::TxStatusRequest(_, _) => T2MessageKind::TxStatusRequest,
            T2MessageBody::TxStatusResponse(_) => T2MessageKind::TxStatusResponse,
            T2MessageBody::PartialEncodedChunkRequest(_) => {
                T2MessageKind::PartialEncodedChunkRequest
            }
            T2MessageBody::PartialEncodedChunkResponse(_) => {
                T2MessageKind::PartialEncodedChunkResponse
            }
            T2MessageBody::Ping(_) => T2MessageKind::Ping,
            T2MessageBody::Pong(_) => T2MessageKind::Pong,
            T2MessageBody::ChunkStateWitnessAck(_) => T2MessageKind::ChunkStateWitnessAck,
            T2MessageBody::StatePartRequest(_) => T2MessageKind::StatePartRequest,
            T2MessageBody::PartialEncodedContractDeploys(_) => {
                T2MessageKind::PartialEncodedContractDeploys
            }
            T2MessageBody::StateHeaderRequest(_) => T2MessageKind::StateHeaderRequest,
            T2MessageBody::StateRequestAck(_) => T2MessageKind::StateRequestAck,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MessageSizeClass, PeerMessageKind, T1MessageKind, T2MessageKind, TieredMessageKind,
    };
    use crate::config::{DecodeMemoryLimits, DecodeMemoryTier};
    use crate::network_protocol::{
        MAX_HUGE_MESSAGE_SIZE, MAX_LARGE_MESSAGE_SIZE, MAX_MEDIUM_MESSAGE_SIZE,
        MAX_SMALL_MESSAGE_SIZE,
    };
    use strum::IntoEnumIterator;

    #[test]
    fn size_class_max_size() {
        assert_eq!(MessageSizeClass::Small.max_size(), MAX_SMALL_MESSAGE_SIZE);
        assert_eq!(MessageSizeClass::Medium.max_size(), MAX_MEDIUM_MESSAGE_SIZE);
        assert_eq!(MessageSizeClass::Large.max_size(), MAX_LARGE_MESSAGE_SIZE);
        assert_eq!(MessageSizeClass::Huge.max_size(), MAX_HUGE_MESSAGE_SIZE);
    }

    #[test]
    fn size_class_decode_memory_tier() {
        assert_eq!(MessageSizeClass::Small.decode_memory_tier(), DecodeMemoryTier::Low);
        assert_eq!(MessageSizeClass::Medium.decode_memory_tier(), DecodeMemoryTier::Medium);
        assert_eq!(MessageSizeClass::Large.decode_memory_tier(), DecodeMemoryTier::High);
        assert_eq!(MessageSizeClass::Huge.decode_memory_tier(), DecodeMemoryTier::High);
    }

    #[test]
    fn kind_size_class_and_decode_memory_tier() {
        use MessageSizeClass::{Huge, Large, Medium, Small};
        let cases = [
            (PeerMessageKind::Routed(TieredMessageKind::T2(T2MessageKind::Ping)), Small),
            (PeerMessageKind::Tier2Handshake, Small),
            (PeerMessageKind::Routed(TieredMessageKind::T1(T1MessageKind::BlockApproval)), Small),
            (PeerMessageKind::Block, Medium),
            (
                PeerMessageKind::Routed(TieredMessageKind::T1(
                    T1MessageKind::VersionedPartialEncodedChunk,
                )),
                Medium,
            ),
            (PeerMessageKind::Routed(TieredMessageKind::T2(T2MessageKind::ForwardTx)), Medium),
            (PeerMessageKind::VersionedStateResponse, Large),
            (
                PeerMessageKind::Routed(TieredMessageKind::T1(
                    T1MessageKind::PartialEncodedStateWitness,
                )),
                Large,
            ),
            (
                PeerMessageKind::Routed(TieredMessageKind::T2(
                    T2MessageKind::PartialEncodedContractDeploys,
                )),
                Large,
            ),
            (PeerMessageKind::EpochSyncResponse, Huge),
        ];
        for (kind, class) in cases {
            assert_eq!(kind.size_class(), class, "{kind:?}");
            assert_eq!(kind.max_size(), class.max_size(), "{kind:?}");
            assert_eq!(kind.decode_memory_tier(), class.decode_memory_tier(), "{kind:?}");
        }
    }

    #[test]
    fn default_decode_memory_limits_cover_max_size() {
        let limits = DecodeMemoryLimits::default();
        for class in MessageSizeClass::iter() {
            let limit = limits.limit_bytes(class.decode_memory_tier());
            assert!(limit >= class.max_size(), "{class:?}: limit {limit} < {}", class.max_size());
        }
    }

    #[test]
    fn default_decode_memory_limits_are_8x_max_size() {
        let limits = DecodeMemoryLimits::default();
        assert_eq!(limits.low_bytes, 8 * MAX_SMALL_MESSAGE_SIZE);
        assert_eq!(limits.medium_bytes, 8 * MAX_MEDIUM_MESSAGE_SIZE);
        assert_eq!(limits.high_bytes, 8 * MAX_LARGE_MESSAGE_SIZE);
    }
}
