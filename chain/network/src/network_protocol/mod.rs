/// Contains types that belong to the `network protocol.
#[path = "borsh.rs"]
mod borsh_;
mod borsh_conv;
mod edge;
mod peer;
mod proto_conv;
mod state_sync;
use borsh::BorshDeserialize;
use borsh::BorshSerialize;
pub use edge::*;
use near_primitives::genesis::GenesisId;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::contract_distribution::ChunkContractAccesses;
use near_primitives::stateless_validation::contract_distribution::ContractCodeRequest;
use near_primitives::stateless_validation::contract_distribution::ContractCodeResponse;
use near_primitives::stateless_validation::contract_distribution::PartialEncodedContractDeploys;
use near_primitives::stateless_validation::partial_witness::PartialEncodedStateWitness;
use near_primitives::stateless_validation::state_witness::ChunkStateWitnessAck;
pub use peer::*;
pub use state_sync::*;

#[cfg(test)]
pub(crate) mod testonly;
#[cfg(test)]
mod tests;

mod _proto {
    // TODO: protobuf codegen includes `#![allow(box_pointers)]` which Clippy
    // doesn’t like.  Allow renamed_and_removed_lints to silence that warning.
    // Remove this once protobuf codegen is updated.
    #![allow(renamed_and_removed_lints)]
    include!(concat!(env!("OUT_DIR"), "/proto/mod.rs"));
}

pub use _proto::network as proto;

use crate::network_protocol::proto_conv::trace_context::{
    extract_span_context, inject_trace_context,
};
use near_async::time;
use near_crypto::PublicKey;
use near_crypto::Signature;
use near_o11y::OpenTelemetrySpanExt;
use near_primitives::block::{Approval, Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::epoch_sync::CompressedEpochSyncProof;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::combine_hash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::optimistic_block::OptimisticBlock;
use near_primitives::sharding::{
    ChunkHash, PartialEncodedChunk, PartialEncodedChunkPart, ReceiptProof, ShardChunkHeader,
};
use near_primitives::state_sync::{ShardStateSyncResponse, ShardStateSyncResponseV1};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::views::FinalExecutionOutcomeView;
use near_schema_checker_lib::ProtocolSchema;
use protobuf::Message as _;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Debug;
use std::io::Read;
use std::io::Write;
use std::sync::Arc;
use tracing::Span;

/// Send important messages three times.
/// We send these messages multiple times to reduce the chance that they are lost
const IMPORTANT_MESSAGE_RESENT_COUNT: usize = 3;

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub struct PeerAddr {
    pub addr: std::net::SocketAddr,
    pub peer_id: PeerId,
}

impl serde::Serialize for PeerAddr {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{}@{}", self.peer_id, self.addr))
    }
}

impl<'a> serde::Deserialize<'a> for PeerAddr {
    fn deserialize<D: serde::Deserializer<'a>>(d: D) -> Result<Self, D::Error> {
        <String as serde::Deserialize>::deserialize(d)?.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParsePeerAddrError {
    #[error("expected <PeerId>@<IP>:<port>, got \'{0}\'")]
    Format(String),
    #[error("PeerId: {0}")]
    PeerId(#[source] near_crypto::ParseKeyError),
    #[error("SocketAddr: {0}")]
    SocketAddr(#[source] std::net::AddrParseError),
}

impl std::str::FromStr for PeerAddr {
    type Err = ParsePeerAddrError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = s.split('@').collect();
        if parts.len() != 2 {
            return Err(Self::Err::Format(s.to_string()));
        }
        Ok(PeerAddr {
            peer_id: PeerId::new(parts[0].parse().map_err(Self::Err::PeerId)?),
            addr: parts[1].parse().map_err(Self::Err::SocketAddr)?,
        })
    }
}

/// AccountData is a piece of global state that a validator
/// signs and broadcasts to the network. It is essentially
/// the data that a validator wants to share with the network.
/// All the nodes in the network are collecting the account data
/// broadcasted by the validators.
/// Since the number of the validators is bounded and their
/// identity is known (and the maximal size of allowed AccountData is bounded)
/// the global state that is distributed in the form of AccountData is bounded
/// as well.
#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct AccountData {
    /// ID of the node that handles the account key (aka validator key).
    pub peer_id: PeerId,
    /// Proxy nodes that are directly connected to the validator node
    /// (this list may include the validator node itself).
    /// TIER1 nodes should connect to one of the proxies to sent TIER1
    /// messages to the validator.
    pub proxies: Vec<PeerAddr>,
}

/// Wrapper of the AccountData which adds metadata to it.
/// It allows to decide which AccountData is newer (authoritative)
/// and discard the older versions.
#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct VersionedAccountData {
    /// The wrapped account data.
    pub data: AccountData,
    /// Account key of the validator signing this AccountData.
    pub account_key: PublicKey,
    /// Version of the AccountData. Each network node stores only
    /// the newest version of the data per validator. The newest AccountData
    /// is the one with the lexicographically biggest (version,timestamp) tuple:
    /// * version is a manually incremented version counter. In case a validator
    ///   (after a restart/crash/state loss) learns from the network that it has
    ///   already published AccountData with some version, it can immediately
    ///   override it by signing and broadcasting AccountData with a higher version.
    /// * timestamp is a version tie breaker, introduced only to minimize
    ///   the risk of version collision (see accounts_data/mod.rs).
    pub version: u64,
    /// UTC timestamp of when the AccountData has been signed.
    pub timestamp: time::Utc,
}

/// Limit on the size of the serialized AccountData message.
/// It is important to have such a constraint on the serialized proto,
/// because it may contain many unknown fields (which are dropped during parsing).
pub const MAX_ACCOUNT_DATA_SIZE_BYTES: usize = 10000; // 10kB

/// Limit on the number of shard ids in a single [`SnapshotHostInfo`](state_sync::SnapshotHostInfo) message.
/// The number of shards has to be limited, otherwise a malicious attack could fill the snapshot host cache
/// with millions of shards.
/// The assumption is that no single host is going to track state for more than 512 shards. Keeping state for
/// a shard requires significant resources, so a single peer shouldn't be able to handle too many of them.
/// If this assumption changes in the future, this limit will have to be revisited.
///
/// Warning: adjusting this constant directly will break upgradeability. A new versioned-node would not interop
/// correctly with an old-versioned node; it could send an excessively large message to an old node.
/// If we ever want to change it we will need to introduce separate send and receive limits,
/// increase the receive limit in one release then increase the send limit in the next.
pub const MAX_SHARDS_PER_SNAPSHOT_HOST_INFO: usize = 512;

impl VersionedAccountData {
    /// Serializes AccountData to proto and signs it using `signer`.
    /// Panics if AccountData.account_id doesn't match signer.validator_id(),
    /// as this would likely be a bug.
    /// Returns an error if the serialized data is too large to be broadcasted.
    /// TODO(gprusak): consider separating serialization from signing (so introducing an
    /// intermediate SerializedAccountData type) so that sign() then could fail only
    /// due to account_id mismatch. Then instead of panicking we could return an error
    /// and the caller (who constructs the arguments) would do an unwrap(). This would
    /// constitute a cleaner never-panicking interface.
    pub fn sign(self, signer: &ValidatorSigner) -> anyhow::Result<SignedAccountData> {
        assert_eq!(
            self.account_key,
            signer.public_key(),
            "AccountData.account_key doesn't match the signer's account_key"
        );
        let payload = proto::AccountKeyPayload::from(&self).write_to_bytes().unwrap();
        if payload.len() > MAX_ACCOUNT_DATA_SIZE_BYTES {
            anyhow::bail!(
                "payload size = {}, max is {}",
                payload.len(),
                MAX_ACCOUNT_DATA_SIZE_BYTES
            );
        }
        let signature = signer.sign_bytes(&payload);
        Ok(SignedAccountData {
            account_data: self,
            payload: AccountKeySignedPayload { payload, signature },
        })
    }
}

impl std::ops::Deref for VersionedAccountData {
    type Target = AccountData;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct AccountKeySignedPayload {
    payload: Vec<u8>,
    signature: near_crypto::Signature,
}

impl AccountKeySignedPayload {
    pub fn len(&self) -> usize {
        self.payload.len()
    }
    pub fn signature(&self) -> &near_crypto::Signature {
        &self.signature
    }
    pub fn verify(&self, key: &PublicKey) -> Result<(), ()> {
        match self.signature.verify(&self.payload, key) {
            true => Ok(()),
            false => Err(()),
        }
    }
}

// TODO(gprusak): this is effectively immutable, and we always pass it around
// in an Arc, so the Arc can be moved inside (except that constructing malformed
// SignedAccountData for tests may get a little tricky).
#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct SignedAccountData {
    account_data: VersionedAccountData,
    // Serialized and signed AccountData.
    payload: AccountKeySignedPayload,
}

impl std::ops::Deref for SignedAccountData {
    type Target = VersionedAccountData;
    fn deref(&self) -> &Self::Target {
        &self.account_data
    }
}

impl SignedAccountData {
    pub fn payload(&self) -> &AccountKeySignedPayload {
        &self.payload
    }
}

/// Proof that a given peer owns the account key.
/// Included in every handshake sent by a validator node.
#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct OwnedAccount {
    pub(crate) account_key: PublicKey,
    pub(crate) peer_id: PeerId,
    pub(crate) timestamp: time::Utc,
}

impl OwnedAccount {
    /// Serializes OwnedAccount to proto and signs it using `signer`.
    /// Panics if OwnedAccount.account_key doesn't match signer.public_key(),
    /// as this would likely be a bug.
    pub fn sign(self, signer: &ValidatorSigner) -> SignedOwnedAccount {
        assert_eq!(
            self.account_key,
            signer.public_key(),
            "OwnedAccount.account_key doesn't match the signer's account_key"
        );
        let payload = proto::AccountKeyPayload::from(&self).write_to_bytes().unwrap();
        let signature = signer.sign_bytes(&payload);
        SignedOwnedAccount {
            owned_account: self,
            payload: AccountKeySignedPayload { payload, signature },
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct SignedOwnedAccount {
    owned_account: OwnedAccount,
    // Serialized and signed OwnedAccount.
    payload: AccountKeySignedPayload,
}

impl std::ops::Deref for SignedOwnedAccount {
    type Target = OwnedAccount;
    fn deref(&self) -> &Self::Target {
        &self.owned_account
    }
}

impl SignedOwnedAccount {
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

/// Denotes a network path to `destination` of length `distance`.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct AdvertisedPeerDistance {
    pub destination: PeerId,
    pub distance: u32,
}

/// Struct shared by a peer listing the distances it has to other peers
/// in the NEAR network.
///
/// It includes a collection of signed edges forming a spanning tree
/// which verifiably achieves the advertised routing distances.
///
/// The distances in the tree may be the same or better than the advertised
/// distances; see routing::graph_v2::tests::inconsistent_peers.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct DistanceVector {
    /// PeerId of the node sending the message.
    pub root: PeerId,
    /// List of distances the root has to other peers in the network.
    pub distances: Vec<AdvertisedPeerDistance>,
    /// Spanning tree of signed edges achieving the claimed distances (or better).
    pub edges: Vec<Edge>,
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
    /// Account owned by the sender.
    pub(crate) owned_account: Option<SignedOwnedAccount>,
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
    pub accounts_data: Vec<Arc<SignedAccountData>>,
    pub requesting_full_sync: bool,
    pub incremental: bool,
}

/// Message sent to request a PeersResponse
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct PeersRequest {
    /// Limits the number of peers to send back
    pub max_peers: Option<u32>,
    /// Limits the number of direct peers to send back
    pub max_direct_peers: Option<u32>,
}

/// Message sent as a response to PeersRequest
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct PeersResponse {
    /// Peers drawn from the PeerStore of the responding node,
    /// which includes peers learned transitively from other peers
    pub peers: Vec<PeerInfo>,
    /// Peers directly connected to the responding node
    pub direct_peers: Vec<PeerInfo>,
}

/// Message sent when gracefully disconnecting from the other peer.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Disconnect {
    /// Advises the other peer to remove the connection from storage
    /// Used when it is not expected that a reconnect attempt would succeed
    pub remove_from_connection_store: bool,
}

#[derive(PartialEq, Eq, Clone, Debug, strum::IntoStaticStr, strum::EnumVariantNames)]
#[allow(clippy::large_enum_variant)]
pub enum PeerMessage {
    Tier1Handshake(Handshake),
    Tier2Handshake(Handshake),
    Tier3Handshake(Handshake),
    HandshakeFailure(PeerInfo, HandshakeFailureReason),
    /// When a failed nonce is used by some peer, this message is sent back as evidence.
    LastEdge(Edge),
    /// Contains accounts and edge information.
    SyncRoutingTable(RoutingTableUpdate),
    DistanceVector(DistanceVector),
    RequestUpdateNonce(PartialEdgeInfo),

    SyncAccountsData(SyncAccountsData),

    PeersRequest(PeersRequest),
    PeersResponse(PeersResponse),

    BlockHeadersRequest(Vec<CryptoHash>),
    BlockHeaders(Vec<Arc<BlockHeader>>),

    BlockRequest(CryptoHash),
    Block(Arc<Block>),
    OptimisticBlock(OptimisticBlock),

    Transaction(SignedTransaction),
    Routed(Box<RoutedMessage>),

    /// Gracefully disconnect from other peer.
    Disconnect(Disconnect),
    Challenge(Box<Challenge>),

    SyncSnapshotHosts(SyncSnapshotHosts),
    StateRequestHeader(ShardId, CryptoHash),
    StateRequestPart(ShardId, CryptoHash, u64),
    VersionedStateResponse(StateResponseInfo),

    EpochSyncRequest,
    EpochSyncResponse(CompressedEpochSyncProof),
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

#[derive(thiserror::Error, Debug)]
pub enum ParsePeerMessageError {
    #[error("BorshDecode")]
    BorshDecode(#[source] std::io::Error),
    #[error("BorshConv")]
    BorshConv(#[source] borsh_conv::ParsePeerMessageError),
    #[error("ProtoDecode")]
    ProtoDecode(#[source] protobuf::Error),
    #[error("ProtoConv")]
    ProtoConv(#[source] proto_conv::ParsePeerMessageError),
}

impl PeerMessage {
    /// Serializes a message in the given encoding.
    /// If the encoding is `Proto`, then also attaches current Span's context to the message.
    pub(crate) fn serialize(&self, enc: Encoding) -> Vec<u8> {
        match enc {
            Encoding::Borsh => borsh::to_vec(&borsh_::PeerMessage::from(self)).unwrap(),
            Encoding::Proto => {
                let mut msg = proto::PeerMessage::from(self);
                let cx = Span::current().context();
                msg.trace_context = inject_trace_context(&cx);
                msg.write_to_bytes().unwrap()
            }
        }
    }

    pub(crate) fn deserialize(
        enc: Encoding,
        data: &[u8],
    ) -> Result<PeerMessage, ParsePeerMessageError> {
        let span = tracing::trace_span!(target: "network", "deserialize").entered();
        Ok(match enc {
            Encoding::Borsh => (&borsh_::PeerMessage::try_from_slice(data)
                .map_err(ParsePeerMessageError::BorshDecode)?)
                .try_into()
                .map_err(ParsePeerMessageError::BorshConv)?,
            Encoding::Proto => {
                let proto_msg: proto::PeerMessage = proto::PeerMessage::parse_from_bytes(data)
                    .map_err(ParsePeerMessageError::ProtoDecode)?;
                if let Ok(extracted_span_context) = extract_span_context(&proto_msg.trace_context) {
                    span.clone().or_current().add_link(extracted_span_context);
                }
                (&proto_msg).try_into().map_err(|err| ParsePeerMessageError::ProtoConv(err))?
            }
        })
    }

    pub(crate) fn msg_variant(&self) -> &'static str {
        match self {
            PeerMessage::Routed(routed_msg) => routed_msg.body_variant(),
            _ => self.into(),
        }
    }
}

/// `TieredMessageBody` is used to distinguish between T1 and T2 messages.
/// T1 messages are sent over T1 connections and they are critical for the progress of the network.
/// T2 messages are sent over T2 connections and they are routed over multiple hops.
#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, PartialEq, Eq, Clone, ProtocolSchema)]
pub enum TieredMessageBody {
    T1(Box<T1MessageBody>),
    T2(Box<T2MessageBody>),
}

impl fmt::Debug for TieredMessageBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TieredMessageBody::T1(body) => write!(f, "T1({:?})", body),
            TieredMessageBody::T2(body) => write!(f, "T2({:?})", body),
        }
    }
}

impl TieredMessageBody {
    pub fn is_t1(&self) -> bool {
        matches!(self, TieredMessageBody::T1(_))
    }

    pub fn variant(&self) -> &'static str {
        match self {
            TieredMessageBody::T1(body) => (&(**body)).into(),
            TieredMessageBody::T2(body) => (&(**body)).into(),
        }
    }

    pub fn message_resend_count(&self) -> usize {
        match self {
            TieredMessageBody::T1(body) => body.message_resend_count(),
            TieredMessageBody::T2(body) => body.message_resend_count(),
        }
    }

    // Return true if we allow the message sent to our own account_id to be redirected back to us.
    // The default behavior is to drop all messages sent to our own account_id.
    // This is helpful in managing scenarios like sending chunk_endorsement to block_producer, where
    // we may be the block_producer.
    pub fn allow_sending_to_self(&self) -> bool {
        match self {
            TieredMessageBody::T1(body) => body.allow_sending_to_self(),
            TieredMessageBody::T2(body) => body.allow_sending_to_self(),
        }
    }

    pub fn from_routed(routed: RoutedMessageBody) -> Self {
        match routed {
            RoutedMessageBody::BlockApproval(approval) => {
                T1MessageBody::BlockApproval(approval).into()
            }
            RoutedMessageBody::ForwardTx(signed_transaction) => {
                T2MessageBody::ForwardTx(signed_transaction).into()
            }
            RoutedMessageBody::TxStatusRequest(account_id, crypto_hash) => {
                T2MessageBody::TxStatusRequest(account_id, crypto_hash).into()
            }
            RoutedMessageBody::TxStatusResponse(final_execution_outcome_view) => {
                T2MessageBody::TxStatusResponse(Box::new(final_execution_outcome_view)).into()
            }
            RoutedMessageBody::PartialEncodedChunkRequest(partial_encoded_chunk_request_msg) => {
                T2MessageBody::PartialEncodedChunkRequest(partial_encoded_chunk_request_msg).into()
            }
            RoutedMessageBody::PartialEncodedChunkResponse(partial_encoded_chunk_response_msg) => {
                T2MessageBody::PartialEncodedChunkResponse(partial_encoded_chunk_response_msg)
                    .into()
            }
            RoutedMessageBody::Ping(ping) => T2MessageBody::Ping(ping).into(),
            RoutedMessageBody::Pong(pong) => T2MessageBody::Pong(pong).into(),
            RoutedMessageBody::VersionedPartialEncodedChunk(partial_encoded_chunk) => {
                T1MessageBody::VersionedPartialEncodedChunk(Box::new(partial_encoded_chunk)).into()
            }
            RoutedMessageBody::PartialEncodedChunkForward(partial_encoded_chunk_forward_msg) => {
                T1MessageBody::PartialEncodedChunkForward(partial_encoded_chunk_forward_msg).into()
            }
            RoutedMessageBody::ChunkStateWitnessAck(chunk_state_witness_ack) => {
                T2MessageBody::ChunkStateWitnessAck(chunk_state_witness_ack).into()
            }
            RoutedMessageBody::PartialEncodedStateWitness(partial_encoded_state_witness) => {
                T1MessageBody::PartialEncodedStateWitness(partial_encoded_state_witness).into()
            }
            RoutedMessageBody::PartialEncodedStateWitnessForward(partial_encoded_state_witness) => {
                T1MessageBody::PartialEncodedStateWitnessForward(partial_encoded_state_witness)
                    .into()
            }
            RoutedMessageBody::VersionedChunkEndorsement(chunk_endorsement) => {
                T1MessageBody::VersionedChunkEndorsement(chunk_endorsement).into()
            }
            RoutedMessageBody::StatePartRequest(state_part_request) => {
                T2MessageBody::StatePartRequest(state_part_request).into()
            }
            RoutedMessageBody::ChunkContractAccesses(chunk_contract_accesses) => {
                T1MessageBody::ChunkContractAccesses(chunk_contract_accesses).into()
            }
            RoutedMessageBody::ContractCodeRequest(contract_code_request) => {
                T1MessageBody::ContractCodeRequest(contract_code_request).into()
            }
            RoutedMessageBody::ContractCodeResponse(contract_code_response) => {
                T1MessageBody::ContractCodeResponse(contract_code_response).into()
            }
            RoutedMessageBody::PartialEncodedContractDeploys(partial_encoded_contract_deploys) => {
                T2MessageBody::PartialEncodedContractDeploys(partial_encoded_contract_deploys)
                    .into()
            }
            RoutedMessageBody::StateHeaderRequest(state_header_request) => {
                T2MessageBody::StateHeaderRequest(state_header_request).into()
            }
            RoutedMessageBody::_UnusedQueryRequest
            | RoutedMessageBody::_UnusedQueryResponse
            | RoutedMessageBody::_UnusedReceiptOutcomeRequest(_)
            | RoutedMessageBody::_UnusedReceiptOutcomeResponse
            | RoutedMessageBody::_UnusedStateRequestHeader
            | RoutedMessageBody::_UnusedStateRequestPart
            | RoutedMessageBody::_UnusedStateResponse
            | RoutedMessageBody::_UnusedPartialEncodedChunk
            | RoutedMessageBody::_UnusedVersionedStateResponse
            | RoutedMessageBody::_UnusedChunkStateWitness
            | RoutedMessageBody::_UnusedChunkEndorsement
            | RoutedMessageBody::_UnusedEpochSyncRequest
            | RoutedMessageBody::_UnusedEpochSyncResponse(_) => unreachable!(),
        }
    }
}

impl From<T1MessageBody> for TieredMessageBody {
    fn from(body: T1MessageBody) -> Self {
        TieredMessageBody::T1(Box::new(body))
    }
}

impl From<T2MessageBody> for TieredMessageBody {
    fn from(body: T2MessageBody) -> Self {
        TieredMessageBody::T2(Box::new(body))
    }
}

/// T1 messages are sent over T1 connections and they are critical for the progress of the network.
#[derive(
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    strum::IntoStaticStr,
    ProtocolSchema,
    Debug,
)]
pub enum T1MessageBody {
    BlockApproval(Approval),
    VersionedPartialEncodedChunk(Box<PartialEncodedChunk>),
    PartialEncodedChunkForward(PartialEncodedChunkForwardMsg),
    PartialEncodedStateWitness(PartialEncodedStateWitness),
    PartialEncodedStateWitnessForward(PartialEncodedStateWitness),
    VersionedChunkEndorsement(ChunkEndorsement),
    ChunkContractAccesses(ChunkContractAccesses),
    ContractCodeRequest(ContractCodeRequest),
    ContractCodeResponse(ContractCodeResponse),
}

impl T1MessageBody {
    pub fn message_resend_count(&self) -> usize {
        match self {
            T1MessageBody::BlockApproval(_) | T1MessageBody::VersionedPartialEncodedChunk(_) => {
                IMPORTANT_MESSAGE_RESENT_COUNT
            }
            _ => 1,
        }
    }

    pub fn allow_sending_to_self(&self) -> bool {
        match self {
            T1MessageBody::PartialEncodedStateWitness(_)
            | T1MessageBody::PartialEncodedStateWitnessForward(_)
            | T1MessageBody::VersionedChunkEndorsement(_) => true,
            _ => false,
        }
    }
}

// TODO(#1313): Use Box
/// T2 messages are sent over T2 connections and they are routed over multiple hops.
#[derive(
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    strum::IntoStaticStr,
    ProtocolSchema,
    Debug,
)]
pub enum T2MessageBody {
    ForwardTx(SignedTransaction),
    TxStatusRequest(AccountId, CryptoHash),
    TxStatusResponse(Box<FinalExecutionOutcomeView>),
    PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg),
    PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg),
    /// Ping/Pong used for testing networking and routing.
    Ping(Ping),
    Pong(Pong),
    ChunkStateWitnessAck(ChunkStateWitnessAck),
    StatePartRequest(StatePartRequest),
    PartialEncodedContractDeploys(PartialEncodedContractDeploys),
    StateHeaderRequest(StateHeaderRequest),
}

impl T2MessageBody {
    pub fn message_resend_count(&self) -> usize {
        1
    }

    pub fn allow_sending_to_self(&self) -> bool {
        false
    }
}

// TODO(#1313): Use Box
#[derive(
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    strum::IntoStaticStr,
    ProtocolSchema,
)]
pub enum RoutedMessageBody {
    BlockApproval(Approval),
    ForwardTx(SignedTransaction),
    TxStatusRequest(AccountId, CryptoHash),
    TxStatusResponse(FinalExecutionOutcomeView),
    /// Not used, but needed for borsh backward compatibility.
    _UnusedQueryRequest,
    _UnusedQueryResponse,
    _UnusedReceiptOutcomeRequest(CryptoHash),
    _UnusedReceiptOutcomeResponse,
    _UnusedStateRequestHeader,
    _UnusedStateRequestPart,
    _UnusedStateResponse,
    PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg),
    PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg),
    _UnusedPartialEncodedChunk,
    /// Ping/Pong used for testing networking and routing.
    Ping(Ping),
    Pong(Pong),
    VersionedPartialEncodedChunk(PartialEncodedChunk),
    _UnusedVersionedStateResponse,
    PartialEncodedChunkForward(PartialEncodedChunkForwardMsg),
    _UnusedChunkStateWitness,
    _UnusedChunkEndorsement,
    ChunkStateWitnessAck(ChunkStateWitnessAck),
    PartialEncodedStateWitness(PartialEncodedStateWitness),
    PartialEncodedStateWitnessForward(PartialEncodedStateWitness),
    VersionedChunkEndorsement(ChunkEndorsement),
    /// Not used, but needed for borsh backward compatibility.
    _UnusedEpochSyncRequest,
    _UnusedEpochSyncResponse(CompressedEpochSyncProof),
    StatePartRequest(StatePartRequest),
    ChunkContractAccesses(ChunkContractAccesses),
    ContractCodeRequest(ContractCodeRequest),
    ContractCodeResponse(ContractCodeResponse),
    PartialEncodedContractDeploys(PartialEncodedContractDeploys),
    StateHeaderRequest(StateHeaderRequest),
}

impl RoutedMessageBody {
    // Return the number of times this message should be sent.
    // In routing logics, we send important messages multiple times to minimize the risk that they are lost
    pub fn message_resend_count(&self) -> usize {
        match self {
            // These messages are important because they are critical for block and chunk production,
            // and lost messages cannot be requested again.
            RoutedMessageBody::BlockApproval(_)
            | RoutedMessageBody::VersionedPartialEncodedChunk(_) => IMPORTANT_MESSAGE_RESENT_COUNT,
            // Default value is sending just once.
            _ => 1,
        }
    }

    // Return true if we allow the message sent to our own account_id to be redirected back to us.
    // The default behavior is to drop all messages sent to our own account_id.
    // This is helpful in managing scenarios like sending chunk_endorsement to block_producer, where
    // we may be the block_producer.
    pub fn allow_sending_to_self(&self) -> bool {
        match self {
            RoutedMessageBody::PartialEncodedStateWitness(_)
            | RoutedMessageBody::PartialEncodedStateWitnessForward(_)
            | RoutedMessageBody::VersionedChunkEndorsement(_) => true,
            _ => false,
        }
    }

    pub fn is_t1(&self) -> bool {
        match self {
            RoutedMessageBody::BlockApproval(_)
            | RoutedMessageBody::VersionedPartialEncodedChunk(_)
            | RoutedMessageBody::PartialEncodedChunkForward(_)
            | RoutedMessageBody::PartialEncodedStateWitness(_)
            | RoutedMessageBody::PartialEncodedStateWitnessForward(_)
            | RoutedMessageBody::VersionedChunkEndorsement(_)
            | RoutedMessageBody::ChunkContractAccesses(_)
            | RoutedMessageBody::ContractCodeRequest(_)
            | RoutedMessageBody::ContractCodeResponse(_) => true,
            _ => false,
        }
    }
}

impl fmt::Debug for RoutedMessageBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
            RoutedMessageBody::_UnusedQueryRequest => write!(f, "QueryRequest"),
            RoutedMessageBody::_UnusedQueryResponse => write!(f, "QueryResponse"),
            RoutedMessageBody::_UnusedReceiptOutcomeRequest(_) => write!(f, "ReceiptRequest"),
            RoutedMessageBody::_UnusedReceiptOutcomeResponse => write!(f, "ReceiptResponse"),
            RoutedMessageBody::_UnusedStateRequestHeader => write!(f, "StateRequestHeader"),
            RoutedMessageBody::_UnusedStateRequestPart => write!(f, "StateRequestPart"),
            RoutedMessageBody::_UnusedStateResponse => write!(f, "StateResponse"),
            RoutedMessageBody::PartialEncodedChunkRequest(request) => {
                write!(f, "PartialChunkRequest({:?}, {:?})", request.chunk_hash, request.part_ords)
            }
            RoutedMessageBody::PartialEncodedChunkResponse(response) => write!(
                f,
                "PartialChunkResponse({:?}, {:?})",
                response.chunk_hash,
                response.parts.iter().map(|p| p.part_ord).collect::<Vec<_>>()
            ),
            RoutedMessageBody::_UnusedPartialEncodedChunk => write!(f, "PartialEncodedChunk"),
            RoutedMessageBody::VersionedPartialEncodedChunk(_) => {
                write!(f, "VersionedPartialEncodedChunk(?)")
            }
            RoutedMessageBody::PartialEncodedChunkForward(forward) => write!(
                f,
                "PartialChunkForward({:?}, {:?})",
                forward.chunk_hash,
                forward.parts.iter().map(|p| p.part_ord).collect::<Vec<_>>(),
            ),
            RoutedMessageBody::Ping(_) => write!(f, "Ping"),
            RoutedMessageBody::Pong(_) => write!(f, "Pong"),
            RoutedMessageBody::_UnusedVersionedStateResponse => write!(f, "VersionedStateResponse"),
            RoutedMessageBody::_UnusedChunkStateWitness => write!(f, "ChunkStateWitness"),
            RoutedMessageBody::_UnusedChunkEndorsement => write!(f, "ChunkEndorsement"),
            RoutedMessageBody::ChunkStateWitnessAck(ack, ..) => {
                f.debug_tuple("ChunkStateWitnessAck").field(&ack.chunk_hash).finish()
            }
            RoutedMessageBody::PartialEncodedStateWitness(_) => {
                write!(f, "PartialEncodedStateWitness")
            }
            RoutedMessageBody::PartialEncodedStateWitnessForward(_) => {
                write!(f, "PartialEncodedStateWitnessForward")
            }
            RoutedMessageBody::VersionedChunkEndorsement(_) => {
                write!(f, "VersionedChunkEndorsement")
            }
            RoutedMessageBody::_UnusedEpochSyncRequest => write!(f, "EpochSyncRequest"),
            RoutedMessageBody::_UnusedEpochSyncResponse(_) => {
                write!(f, "EpochSyncResponse")
            }
            RoutedMessageBody::StatePartRequest(request) => write!(
                f,
                "StatePartRequest(sync_hash={:?}, shard_id={:?}, part_id={:?})",
                request.sync_hash, request.shard_id, request.part_id,
            ),
            RoutedMessageBody::ChunkContractAccesses(accesses) => {
                write!(f, "ChunkContractAccesses(code_hashes={:?})", accesses.contracts())
            }
            RoutedMessageBody::ContractCodeRequest(request) => {
                write!(f, "ContractCodeRequest(code_hashes={:?})", request.contracts())
            }
            RoutedMessageBody::ContractCodeResponse(_) => write!(f, "ContractCodeResponse",),
            RoutedMessageBody::PartialEncodedContractDeploys(deploys) => {
                write!(f, "PartialEncodedContractDeploys(part={:?})", deploys.part())
            }
            RoutedMessageBody::StateHeaderRequest(request) => write!(
                f,
                "StateHeaderRequest(sync_hash={:?}, shard_id={:?})",
                request.sync_hash, request.shard_id,
            ),
        }
    }
}

impl From<TieredMessageBody> for RoutedMessageBody {
    fn from(tiered: TieredMessageBody) -> Self {
        match tiered {
            TieredMessageBody::T1(body) => match *body {
                T1MessageBody::BlockApproval(approval) => {
                    RoutedMessageBody::BlockApproval(approval)
                }
                T1MessageBody::VersionedPartialEncodedChunk(partial_encoded_chunk) => {
                    RoutedMessageBody::VersionedPartialEncodedChunk(*partial_encoded_chunk)
                }
                T1MessageBody::PartialEncodedChunkForward(partial_encoded_chunk_forward_msg) => {
                    RoutedMessageBody::PartialEncodedChunkForward(partial_encoded_chunk_forward_msg)
                }
                T1MessageBody::PartialEncodedStateWitness(partial_encoded_state_witness) => {
                    RoutedMessageBody::PartialEncodedStateWitness(partial_encoded_state_witness)
                }
                T1MessageBody::PartialEncodedStateWitnessForward(partial_encoded_state_witness) => {
                    RoutedMessageBody::PartialEncodedStateWitnessForward(
                        partial_encoded_state_witness,
                    )
                }
                T1MessageBody::VersionedChunkEndorsement(chunk_endorsement) => {
                    RoutedMessageBody::VersionedChunkEndorsement(chunk_endorsement)
                }
                T1MessageBody::ChunkContractAccesses(chunk_contract_accesses) => {
                    RoutedMessageBody::ChunkContractAccesses(chunk_contract_accesses)
                }
                T1MessageBody::ContractCodeRequest(contract_code_request) => {
                    RoutedMessageBody::ContractCodeRequest(contract_code_request)
                }
                T1MessageBody::ContractCodeResponse(contract_code_response) => {
                    RoutedMessageBody::ContractCodeResponse(contract_code_response)
                }
            },
            TieredMessageBody::T2(body) => match *body {
                T2MessageBody::ForwardTx(signed_transaction) => {
                    RoutedMessageBody::ForwardTx(signed_transaction)
                }
                T2MessageBody::TxStatusRequest(account_id, crypto_hash) => {
                    RoutedMessageBody::TxStatusRequest(account_id, crypto_hash)
                }
                T2MessageBody::TxStatusResponse(final_execution_outcome_view) => {
                    RoutedMessageBody::TxStatusResponse(*final_execution_outcome_view)
                }
                T2MessageBody::PartialEncodedChunkRequest(partial_encoded_chunk_request_msg) => {
                    RoutedMessageBody::PartialEncodedChunkRequest(partial_encoded_chunk_request_msg)
                }
                T2MessageBody::PartialEncodedChunkResponse(partial_encoded_chunk_response_msg) => {
                    RoutedMessageBody::PartialEncodedChunkResponse(
                        partial_encoded_chunk_response_msg,
                    )
                }
                T2MessageBody::Ping(ping) => RoutedMessageBody::Ping(ping),
                T2MessageBody::Pong(pong) => RoutedMessageBody::Pong(pong),
                T2MessageBody::ChunkStateWitnessAck(chunk_state_witness_ack) => {
                    RoutedMessageBody::ChunkStateWitnessAck(chunk_state_witness_ack)
                }
                T2MessageBody::StatePartRequest(state_part_request) => {
                    RoutedMessageBody::StatePartRequest(state_part_request)
                }
                T2MessageBody::PartialEncodedContractDeploys(partial_encoded_contract_deploys) => {
                    RoutedMessageBody::PartialEncodedContractDeploys(
                        partial_encoded_contract_deploys,
                    )
                }
                T2MessageBody::StateHeaderRequest(state_header_request) => {
                    RoutedMessageBody::StateHeaderRequest(state_header_request)
                }
            },
        }
    }
}

/// TODO(13709): Remove V1 support after forward compatible release.
#[derive(
    borsh::BorshSerialize, borsh::BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema,
)]
pub struct RoutedMessageV1 {
    /// Peer id which is directed this message.
    /// If `target` is hash, this a message should be routed back.
    pub target: PeerIdOrHash,
    /// Original sender of this message
    pub author: PeerId,
    /// Signature from the author of the message. If this signature is invalid we should ban
    /// last sender of this message. If the message is invalid we should ben author of the message.
    pub signature: Signature,
    /// Time to live for this message. After passing through a hop this number should be
    /// decreased by 1. If this number is 0, drop this message.
    pub ttl: u8,
    /// Message
    pub body: RoutedMessageBody,
}

#[derive(PartialEq, Eq, Clone, Debug, ProtocolSchema)]
pub struct RoutedMessageV2 {
    /// Message
    pub msg: RoutedMessageV1,
    /// The time the Routed message was created by `author`.
    pub created_at: Option<time::Utc>,
    /// Number of peers this routed message traveled through.
    /// Doesn't include the peers that are the source and the destination of the message.
    pub num_hops: u32,
}

impl BorshSerialize for RoutedMessageV2 {
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.msg.serialize(writer)?;
        self.created_at.map(|t| t.unix_timestamp()).serialize(writer)?;
        self.num_hops.serialize(writer)
    }
}

impl BorshDeserialize for RoutedMessageV2 {
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let msg = RoutedMessageV1::deserialize_reader(reader)?;
        let created_at = Option::<i64>::deserialize_reader(reader)?
            .map(|t| time::Utc::from_unix_timestamp(t).unwrap());
        let num_hops = u32::deserialize_reader(reader)?;
        Ok(Self { msg, created_at, num_hops })
    }
}

/// V3 message will remove the signature field for T1.
/// Contains the body as a `TieredMessageBody` instead of `RoutedMessageBody`.
/// All other fields are the same as in previous versions.
#[derive(BorshDeserialize, BorshSerialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
pub struct RoutedMessageV3 {
    /// Peer id to which this message is directed.
    /// If `target` is hash, this message should be routed back.
    pub target: PeerIdOrHash,
    /// Original sender of this message
    pub author: PeerId,
    /// Time to live for this message. After passing through a hop this number should be
    /// decreased by 1. If this number is 0, drop this message.
    pub ttl: u8,
    /// Message
    pub body: TieredMessageBody,
    /// Signature only for T2 messages.
    pub signature: Option<Signature>,
    /// The time the Routed message was created by `author`.
    pub created_at: Option<i64>,
    /// Number of peers this routed message traveled through.
    /// Doesn't include the peers that are the source and the destination of the message.
    pub num_hops: u32,
}

impl RoutedMessageV3 {
    pub fn hash_tiered(&self) -> CryptoHash {
        let routed = RoutedMessageBody::from(self.body.clone());
        CryptoHash::hash_borsh(RoutedMessageNoSignature {
            target: &self.target,
            author: &self.author,
            body: &routed,
        })
    }

    pub fn verify(&self) -> bool {
        if self.body.is_t1() {
            true
        } else {
            let Some(signature) = &self.signature else {
                return false;
            };
            signature.verify(self.hash_tiered().as_ref(), self.author.public_key())
        }
    }

    pub fn expect_response(&self) -> bool {
        if let TieredMessageBody::T2(body) = &self.body {
            matches!(
                **body,
                T2MessageBody::Ping(_)
                    | T2MessageBody::TxStatusRequest(_, _)
                    | T2MessageBody::PartialEncodedChunkRequest(_)
            )
        } else {
            false
        }
    }

    /// Return true if ttl is positive after decreasing ttl by one, false otherwise.
    pub fn decrease_ttl(&mut self) -> bool {
        self.ttl = self.ttl.saturating_sub(1);
        self.ttl > 0
    }
}

impl From<RoutedMessageV1> for RoutedMessageV3 {
    fn from(msg: RoutedMessageV1) -> Self {
        Self {
            target: msg.target,
            author: msg.author,
            ttl: msg.ttl,
            body: TieredMessageBody::from_routed(msg.body),
            signature: Some(msg.signature),
            created_at: None,
            num_hops: 0,
        }
    }
}

impl From<RoutedMessageV3> for RoutedMessage {
    fn from(msg: RoutedMessageV3) -> Self {
        RoutedMessage::V3(msg)
    }
}

/// RoutedMessage represent a package that will travel the network towards a specific peer id.
/// It contains the peer_id and signature from the original sender. Every intermediate peer in the
/// route must verify that this signature is valid otherwise previous sender of this package should
/// be banned. If the final receiver of this package finds that the body is invalid the original
/// sender of the package should be banned instead.
/// If target is hash, it is a message that should be routed back using the same path used to route
/// the request in first place. It is the hash of the request message.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
pub enum RoutedMessage {
    V1(RoutedMessageV1),
    V2(RoutedMessageV2),
    V3(RoutedMessageV3),
}

impl From<RoutedMessageV1> for RoutedMessage {
    fn from(msg: RoutedMessageV1) -> Self {
        RoutedMessage::V1(msg)
    }
}

impl From<RoutedMessageV2> for RoutedMessage {
    fn from(msg: RoutedMessageV2) -> Self {
        RoutedMessage::V2(msg)
    }
}

impl RoutedMessage {
    pub fn build_hash(
        target: &PeerIdOrHash,
        source: &PeerId,
        body: &RoutedMessageBody,
    ) -> CryptoHash {
        CryptoHash::hash_borsh(RoutedMessageNoSignature { target, author: source, body })
    }

    /// Get the V1 message from the current version. Used for serializations (only V1 is sent over the wire).
    /// TODO(13709): Remove V1 support after forward compatible release.
    pub fn msg_v1(self) -> RoutedMessageV1 {
        match self {
            RoutedMessage::V1(msg) => msg,
            RoutedMessage::V2(msg) => msg.msg,
            RoutedMessage::V3(msg) => RoutedMessageV1 {
                target: msg.target,
                author: msg.author,
                ttl: msg.ttl,
                body: msg.body.into(),
                signature: msg.signature.unwrap_or_else(|| {
                    tracing::error!(
                        target: "network",
                        "Signature is missing. This should not yet happen."
                    );
                    Signature::default()
                }),
            },
        }
    }

    pub fn target(&self) -> &PeerIdOrHash {
        match self {
            RoutedMessage::V1(msg) => &msg.target,
            RoutedMessage::V2(msg) => &msg.msg.target,
            RoutedMessage::V3(msg) => &msg.target,
        }
    }

    pub fn signature(&self) -> Option<&Signature> {
        match self {
            RoutedMessage::V1(msg) => Some(&msg.signature),
            RoutedMessage::V2(msg) => Some(&msg.msg.signature),
            RoutedMessage::V3(msg) => msg.signature.as_ref(),
        }
    }

    pub fn author(&self) -> &PeerId {
        match self {
            RoutedMessage::V1(msg) => &msg.author,
            RoutedMessage::V2(msg) => &msg.msg.author,
            RoutedMessage::V3(msg) => &msg.author,
        }
    }

    pub fn body(&self) -> &TieredMessageBody {
        // Old versions should be upgraded to V3.
        match self {
            RoutedMessage::V1(_) | RoutedMessage::V2(_) => unreachable!(),
            RoutedMessage::V3(msg) => &msg.body,
        }
    }

    pub fn body_owned(mut self) -> TieredMessageBody {
        self.upgrade_to_v3();
        let RoutedMessage::V3(msg) = self else { unreachable!() };
        msg.body
    }

    pub fn created_at(&self) -> Option<i64> {
        match self {
            RoutedMessage::V1(_) => None,
            RoutedMessage::V2(msg) => msg.created_at.map(|t| t.unix_timestamp()),
            RoutedMessage::V3(msg) => msg.created_at,
        }
    }

    pub fn num_hops(&self) -> u32 {
        match self {
            RoutedMessage::V1(_) => 0,
            RoutedMessage::V2(msg) => msg.num_hops,
            RoutedMessage::V3(msg) => msg.num_hops,
        }
    }

    pub fn num_hops_mut(&mut self) -> &mut u32 {
        self.upgrade_to_v3();
        let RoutedMessage::V3(msg) = self else { unreachable!() };
        &mut msg.num_hops
    }

    pub fn hash(&self) -> CryptoHash {
        match self {
            RoutedMessage::V1(msg) => msg.hash(),
            RoutedMessage::V2(msg) => msg.msg.hash(),
            RoutedMessage::V3(msg) => msg.hash_tiered(),
        }
    }

    pub fn verify(&self) -> bool {
        match self {
            RoutedMessage::V1(msg) => msg.verify(),
            RoutedMessage::V2(msg) => msg.msg.verify(),
            RoutedMessage::V3(msg) => msg.verify(),
        }
    }

    pub fn expect_response(&self) -> bool {
        match self {
            RoutedMessage::V1(msg) => msg.expect_response(),
            RoutedMessage::V2(msg) => msg.msg.expect_response(),
            RoutedMessage::V3(msg) => msg.expect_response(),
        }
    }

    /// Return true if ttl is positive after decreasing ttl by one, false otherwise.
    pub fn decrease_ttl(&mut self) -> bool {
        match self {
            RoutedMessage::V1(msg) => msg.decrease_ttl(),
            RoutedMessage::V2(msg) => msg.msg.decrease_ttl(),
            RoutedMessage::V3(msg) => msg.decrease_ttl(),
        }
    }

    pub fn body_variant(&self) -> &'static str {
        match self {
            RoutedMessage::V1(msg) => (&msg.body).into(),
            RoutedMessage::V2(msg) => (&msg.msg.body).into(),
            RoutedMessage::V3(msg) => msg.body.variant(),
        }
    }

    pub fn ttl(&self) -> u8 {
        match self {
            RoutedMessage::V1(msg) => msg.ttl,
            RoutedMessage::V2(msg) => msg.msg.ttl,
            RoutedMessage::V3(msg) => msg.ttl,
        }
    }

    fn upgrade_to_v3(&mut self) {
        if let RoutedMessage::V1(msg) = self {
            *self = RoutedMessage::V3(RoutedMessageV3 {
                target: msg.target.clone(),
                author: msg.author.clone(),
                ttl: msg.ttl,
                body: TieredMessageBody::from_routed(msg.body.clone()),
                signature: Some(msg.signature.clone()),
                created_at: None,
                num_hops: 0,
            });
        } else if let RoutedMessage::V2(msg) = self {
            *self = RoutedMessage::V3(RoutedMessageV3 {
                target: msg.msg.target.clone(),
                author: msg.msg.author.clone(),
                ttl: msg.msg.ttl,
                body: TieredMessageBody::from_routed(msg.msg.body.clone()),
                signature: Some(msg.msg.signature.clone()),
                created_at: msg.created_at.map(|t| t.unix_timestamp()),
                num_hops: msg.num_hops,
            });
        }
    }
}

#[derive(borsh::BorshSerialize, PartialEq, Eq, Clone, Debug)]
struct RoutedMessageNoSignature<'a> {
    target: &'a PeerIdOrHash,
    author: &'a PeerId,
    body: &'a RoutedMessageBody,
}

impl RoutedMessageV1 {
    pub fn hash(&self) -> CryptoHash {
        RoutedMessage::build_hash(&self.target, &self.author, &self.body)
    }

    pub fn verify(&self) -> bool {
        self.signature.verify(self.hash().as_ref(), self.author.public_key())
    }

    pub fn expect_response(&self) -> bool {
        matches!(
            self.body,
            RoutedMessageBody::Ping(_)
                | RoutedMessageBody::TxStatusRequest(_, _)
                | RoutedMessageBody::PartialEncodedChunkRequest(_)
        )
    }

    /// Return true if ttl is positive after decreasing ttl by one, false otherwise.
    pub fn decrease_ttl(&mut self) -> bool {
        self.ttl = self.ttl.saturating_sub(1);
        self.ttl > 0
    }
}

#[derive(
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    Hash,
    ProtocolSchema,
)]
pub enum PeerIdOrHash {
    PeerId(PeerId),
    Hash(CryptoHash),
}

/// Message for chunk part owners to forward their parts to validators tracking that shard.
/// This reduces the number of requests a node tracking a shard needs to send to obtain enough
/// parts to reconstruct the message (in the best case no such requests are needed).
#[derive(
    Clone, Debug, Eq, PartialEq, borsh::BorshSerialize, borsh::BorshDeserialize, ProtocolSchema,
)]
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

/// Test code that someone become part of our protocol?
#[derive(
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    Hash,
    ProtocolSchema,
)]
pub struct Ping {
    pub nonce: u64,
    pub source: PeerId,
}

/// Test code that someone become part of our protocol?
#[derive(
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    Hash,
    ProtocolSchema,
)]
pub struct Pong {
    pub nonce: u64,
    pub source: PeerId,
}

impl PartialEncodedChunkForwardMsg {
    pub fn from_header_and_parts(
        header: &ShardChunkHeader,
        parts: Vec<PartialEncodedChunkPart>,
    ) -> Self {
        Self {
            chunk_hash: header.chunk_hash().clone(),
            inner_header_hash: header.inner_header_hash(),
            merkle_root: *header.encoded_merkle_root(),
            signature: header.signature().clone(),
            prev_block_hash: *header.prev_block_hash(),
            height_created: header.height_created(),
            shard_id: header.shard_id(),
            parts,
        }
    }

    pub fn is_valid_hash(&self) -> bool {
        let correct_hash = combine_hash(&self.inner_header_hash, &self.merkle_root);
        ChunkHash(correct_hash) == self.chunk_hash
    }
}

#[derive(
    Clone, Debug, Eq, PartialEq, borsh::BorshSerialize, borsh::BorshDeserialize, ProtocolSchema,
)]
pub struct PartialEncodedChunkRequestMsg {
    pub chunk_hash: ChunkHash,
    pub part_ords: Vec<u64>,
    pub tracking_shards: HashSet<ShardId>,
}

#[derive(
    Clone, Debug, Eq, PartialEq, borsh::BorshSerialize, borsh::BorshDeserialize, ProtocolSchema,
)]
pub struct PartialEncodedChunkResponseMsg {
    pub chunk_hash: ChunkHash,
    pub parts: Vec<PartialEncodedChunkPart>,
    pub receipts: Vec<ReceiptProof>,
}

#[derive(
    PartialEq, Eq, Clone, Debug, borsh::BorshSerialize, borsh::BorshDeserialize, ProtocolSchema,
)]
pub struct StateResponseInfoV1 {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub state_response: ShardStateSyncResponseV1,
}

#[derive(
    PartialEq, Eq, Clone, Debug, borsh::BorshSerialize, borsh::BorshDeserialize, ProtocolSchema,
)]
pub struct StateResponseInfoV2 {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub state_response: ShardStateSyncResponse,
}

#[derive(
    PartialEq, Eq, Clone, Debug, borsh::BorshSerialize, borsh::BorshDeserialize, ProtocolSchema,
)]
pub enum StateResponseInfo {
    V1(Box<StateResponseInfoV1>),
    V2(Box<StateResponseInfoV2>),
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

pub(crate) struct RawRoutedMessage {
    pub target: PeerIdOrHash,
    pub body: TieredMessageBody,
}

impl RawRoutedMessage {
    /// Add signature to the message.
    /// Panics if the target is an AccountId instead of a PeerId.
    pub fn sign(
        self,
        node_key: &near_crypto::SecretKey,
        routed_message_ttl: u8,
        now: Option<time::Utc>,
    ) -> RoutedMessage {
        let author = PeerId::new(node_key.public_key());
        let body = RoutedMessageBody::from(self.body.clone());
        let hash = RoutedMessage::build_hash(&self.target, &author, &body);
        let signature = Some(node_key.sign(hash.as_ref()));
        RoutedMessage::V3(RoutedMessageV3 {
            target: self.target,
            author,
            signature,
            ttl: routed_message_ttl,
            body: self.body,
            created_at: now.map(|t| t.unix_timestamp()),
            num_hops: 0,
        })
    }
}
