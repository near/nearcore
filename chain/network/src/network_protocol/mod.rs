/// Contains types that belong to the `network protocol.
mod borsh;
mod borsh_conv;
mod proto_conv;

#[cfg(test)]
pub(crate) mod testonly;
#[cfg(test)]
mod tests;

mod _proto {
    include!(concat!(env!("OUT_DIR"), "/proto/mod.rs"));
}

pub use _proto::network as proto;

use ::borsh::{BorshDeserialize as _, BorshSerialize as _};
use near_crypto::PublicKey;
use near_network_primitives::time;
use near_network_primitives::types::{
    Edge, PartialEdgeInfo, PeerChainInfoV2, PeerInfo, RoutedMessageBody, RoutedMessageV2,
};
use near_primitives::block::{Block, BlockHeader, GenesisId};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::syncing::{EpochSyncFinalizationResponse, EpochSyncResponse};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId, ProtocolVersion};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PEER_MIN_ALLOWED_PROTOCOL_VERSION;
use protobuf::Message as _;
use std::fmt;
use thiserror::Error;

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub struct PeerAddr {
    pub addr: std::net::SocketAddr,
    pub peer_id: Option<PeerId>,
}

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub struct AccountData {
    pub peers: Vec<PeerAddr>,
    pub account_id: AccountId,
    pub epoch_id: EpochId,
    pub timestamp: time::Utc,
}

// Limit on the size of the serialized AccountData message.
// It is important to have such a constraint on the serialized proto,
// because it may contain many unknown fields (which are dropped during parsing).
pub const MAX_ACCOUNT_DATA_SIZE_BYTES: usize = 10000; // 10kB

impl AccountData {
    /// Serializes AccountData to proto and signs it using `signer`.
    /// Panics if AccountData.account_id doesn't match signer.validator_id(),
    /// as this would likely be a bug.
    /// Returns an error if the serialized data is too large to be broadcasted.
    /// TODO(gprusak): consider separating serialization from signing (so introducing an
    /// intermediate SerializedAccountData type) so that sign() then could fail only
    /// due to account_id mismatch. Then instead of panicking we could return an error
    /// and the caller (who constructs the arguments) would do an unwrap(). This would
    /// consistute a cleaner never-panicking interface.
    pub fn sign(self, signer: &dyn ValidatorSigner) -> anyhow::Result<SignedAccountData> {
        assert_eq!(
            &self.account_id,
            signer.validator_id(),
            "AccountData.account_id doesn't match the signer's account_id"
        );
        let payload = proto::AccountKeyPayload::from(&self).write_to_bytes().unwrap();
        if payload.len() > MAX_ACCOUNT_DATA_SIZE_BYTES {
            anyhow::bail!(
                "payload size = {}, max is {}",
                payload.len(),
                MAX_ACCOUNT_DATA_SIZE_BYTES
            );
        }
        let signature = signer.sign_account_key_payload(&payload);
        Ok(SignedAccountData {
            account_data: self,
            payload: AccountKeySignedPayload { payload, signature },
        })
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub struct AccountKeySignedPayload {
    payload: Vec<u8>,
    signature: near_crypto::Signature,
}

impl AccountKeySignedPayload {
    pub fn len(&self) -> usize {
        self.payload.len()
    }
    pub fn verify(&self, key: &PublicKey) -> bool {
        self.signature.verify(&self.payload, key)
    }
}

// TODO(gprusak): if we expect this to be large, perhaps we should
// pass around an Arc, rather than pass it by value.
#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub struct SignedAccountData {
    account_data: AccountData,
    // Serialized and signed AccountData.
    payload: AccountKeySignedPayload,
}

impl std::ops::Deref for SignedAccountData {
    type Target = AccountData;
    fn deref(&self) -> &Self::Target {
        &self.account_data
    }
}

impl SignedAccountData {
    pub fn payload(&self) -> &AccountKeySignedPayload {
        &self.payload
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Default)]
pub struct RoutingTableUpdate {
    pub edges: Vec<Edge>,
    pub accounts: Vec<AnnounceAccount>,
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
/// Structure representing handshake between peers.
#[derive(PartialEq, Eq, Clone, Debug)]
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

#[derive(PartialEq, Eq, Clone, Debug, strum::IntoStaticStr)]
pub enum HandshakeFailureReason {
    ProtocolVersionMismatch { version: u32, oldest_supported_version: u32 },
    GenesisMismatch(GenesisId),
    InvalidTarget,
}

/// See SyncAccountsData in network_protocol/network.proto.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct SyncAccountsData {
    pub accounts_data: Vec<SignedAccountData>,
    pub requesting_full_sync: bool,
    pub incremental: bool,
}

#[derive(PartialEq, Eq, Clone, Debug, strum::IntoStaticStr, strum::EnumVariantNames)]
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

    SyncAccountsData(SyncAccountsData),

    PeersRequest,
    PeersResponse(Vec<PeerInfo>),

    BlockHeadersRequest(Vec<CryptoHash>),
    BlockHeaders(Vec<BlockHeader>),

    BlockRequest(CryptoHash),
    Block(Block),

    Transaction(SignedTransaction),
    Routed(Box<RoutedMessageV2>),

    /// Gracefully disconnect from other peer.
    Disconnect,
    Challenge(Challenge),
    EpochSyncRequest(EpochId),
    EpochSyncResponse(Box<EpochSyncResponse>),
    EpochSyncFinalizationRequest(EpochId),
    EpochSyncFinalizationResponse(Box<EpochSyncFinalizationResponse>),
}

impl fmt::Display for PeerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.msg_variant(), f)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash, strum::IntoStaticStr)]
pub enum Encoding {
    Borsh,
    Proto,
}

#[derive(Error, Debug)]
pub enum ParsePeerMessageError {
    #[error("BorshDecode")]
    BorshDecode(std::io::Error),
    #[error("BorshConv")]
    BorshConv(borsh_conv::ParsePeerMessageError),
    #[error("ProtoDecode")]
    ProtoDecode(protobuf::Error),
    #[error("ProtoConv")]
    ProtoConv(proto_conv::ParsePeerMessageError),
}

impl PeerMessage {
    pub(crate) fn serialize(&self, enc: Encoding) -> Vec<u8> {
        match enc {
            Encoding::Borsh => borsh::PeerMessage::from(self).try_to_vec().unwrap(),
            Encoding::Proto => proto::PeerMessage::from(self).write_to_bytes().unwrap(),
        }
    }

    pub(crate) fn deserialize(
        enc: Encoding,
        data: &[u8],
    ) -> Result<PeerMessage, ParsePeerMessageError> {
        Ok(match enc {
            Encoding::Borsh => (&borsh::PeerMessage::try_from_slice(data)
                .map_err(ParsePeerMessageError::BorshDecode)?)
                .try_into()
                .map_err(ParsePeerMessageError::BorshConv)?,
            Encoding::Proto => (&proto::PeerMessage::parse_from_bytes(data)
                .map_err(ParsePeerMessageError::ProtoDecode)?)
                .try_into()
                .map_err(ParsePeerMessageError::ProtoConv)?,
        })
    }

    pub(crate) fn msg_variant(&self) -> &'static str {
        match self {
            PeerMessage::Routed(routed_msg) => routed_msg.msg.body_variant(),
            _ => self.into(),
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
                r.msg.body,
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
                r.msg.body,
                RoutedMessageBody::ReceiptOutcomeRequest(_)
                    | RoutedMessageBody::StateRequestHeader(_, _)
                    | RoutedMessageBody::StateRequestPart(_, _, _)
                    | RoutedMessageBody::TxStatusRequest(_, _)
                    | RoutedMessageBody::TxStatusResponse(_)
            ),
            _ => false,
        }
    }
}
