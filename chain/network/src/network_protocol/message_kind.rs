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

impl PeerMessageKind {
    /// Maximum allowed size (in bytes) of the encoded form of a message of this kind.
    pub(crate) fn max_size(&self) -> usize {
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
            | PeerMessageKind::EpochSyncRequest => MAX_SMALL_MESSAGE_SIZE,

            PeerMessageKind::PeersResponse
            | PeerMessageKind::Transaction
            | PeerMessageKind::SyncRoutingTable
            | PeerMessageKind::SyncAccountsData
            | PeerMessageKind::SyncSnapshotHosts
            | PeerMessageKind::BlockHeaders
            | PeerMessageKind::Block
            | PeerMessageKind::OptimisticBlock => MAX_MEDIUM_MESSAGE_SIZE,

            PeerMessageKind::VersionedStateResponse => MAX_LARGE_MESSAGE_SIZE,
            PeerMessageKind::EpochSyncResponse => MAX_HUGE_MESSAGE_SIZE,
            PeerMessageKind::Routed(kind) => match kind {
                T1(kind) => match kind {
                    BlockApproval | VersionedChunkEndorsement | SpiceChunkEndorsement => {
                        MAX_SMALL_MESSAGE_SIZE
                    }

                    PartialEncodedChunkForward
                    | ChunkContractAccesses
                    | ContractCodeRequest
                    | SpiceDataRequest
                    | SpiceChunkContractAccesses
                    | SpiceContractCodeRequest
                    | VersionedPartialEncodedChunk => MAX_MEDIUM_MESSAGE_SIZE,

                    PartialEncodedStateWitness
                    | PartialEncodedStateWitnessForward
                    | VersionedPartialEncodedStateWitness
                    | VersionedPartialEncodedStateWitnessForward
                    | ContractCodeResponse
                    | SpicePartialData
                    | SpiceContractCodeResponse => MAX_LARGE_MESSAGE_SIZE,
                },
                T2(kind) => match kind {
                    TxStatusRequest
                    | Ping
                    | Pong
                    | ChunkStateWitnessAck
                    | StatePartRequest
                    | StateHeaderRequest
                    | StateRequestAck => MAX_SMALL_MESSAGE_SIZE,

                    ForwardTx
                    | PartialEncodedChunkRequest
                    | TxStatusResponse
                    | PartialEncodedChunkResponse => MAX_MEDIUM_MESSAGE_SIZE,

                    PartialEncodedContractDeploys => MAX_LARGE_MESSAGE_SIZE,
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
