/// Contains types that belong to the `network protocol.
#[path = "borsh.rs"]
mod borsh_;
mod borsh_conv;
mod edge;
mod peer;
mod proto_conv;
mod state_sync;
pub use edge::*;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsementV1;
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
use borsh::BorshDeserialize as _;
use near_async::time;
use near_crypto::PublicKey;
use near_crypto::Signature;
use near_o11y::OpenTelemetrySpanExt;
use near_primitives::block::{Approval, Block, BlockHeader, GenesisId};
use near_primitives::challenge::Challenge;
use near_primitives::epoch_sync::CompressedEpochSyncProof;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::combine_hash;
use near_primitives::network::{AnnounceAccount, PeerId};
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
    /// consistute a cleaner never-panicking interface.
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
        let signature = signer.sign_account_key_payload(&payload);
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
        let signature = signer.sign_account_key_payload(&payload);
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
    BlockHeaders(Vec<BlockHeader>),

    BlockRequest(CryptoHash),
    Block(Block),

    Transaction(SignedTransaction),
    Routed(Box<RoutedMessageV2>),

    /// Gracefully disconnect from other peer.
    Disconnect(Disconnect),
    Challenge(Challenge),

    SyncSnapshotHosts(SyncSnapshotHosts),
    StateRequestHeader(ShardId, CryptoHash),
    StateRequestPart(ShardId, CryptoHash, u64),
    VersionedStateResponse(StateResponseInfo),
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
    /// TODO(ChunkEndorsementV2): Deprecate once we move to VersionedChunkEndorsement
    ChunkEndorsement(ChunkEndorsementV1),
    ChunkStateWitnessAck(ChunkStateWitnessAck),
    PartialEncodedStateWitness(PartialEncodedStateWitness),
    PartialEncodedStateWitnessForward(PartialEncodedStateWitness),
    VersionedChunkEndorsement(ChunkEndorsement),
    EpochSyncRequest,
    EpochSyncResponse(CompressedEpochSyncProof),
    StatePartRequest(StatePartRequest),
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
            RoutedMessageBody::ChunkEndorsement(_)
            | RoutedMessageBody::PartialEncodedStateWitness(_)
            | RoutedMessageBody::PartialEncodedStateWitnessForward(_)
            | RoutedMessageBody::VersionedChunkEndorsement(_) => true,
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
            RoutedMessageBody::_UnusedPartialEncodedChunk => write!(f, "PartiaEncodedChunk"),
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
            RoutedMessageBody::ChunkEndorsement(_) => write!(f, "ChunkEndorsement"),
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
            RoutedMessageBody::EpochSyncRequest => write!(f, "EpochSyncRequest"),
            RoutedMessageBody::EpochSyncResponse(_) => {
                write!(f, "EpochSyncResponse")
            }
            RoutedMessageBody::StatePartRequest(_) => write!(f, "StatePartRequest"),
        }
    }
}

/// RoutedMessage represent a package that will travel the network towards a specific peer id.
/// It contains the peer_id and signature from the original sender. Every intermediate peer in the
/// route must verify that this signature is valid otherwise previous sender of this package should
/// be banned. If the final receiver of this package finds that the body is invalid the original
/// sender of the package should be banned instead.
/// If target is hash, it is a message that should be routed back using the same path used to route
/// the request in first place. It is the hash of the request message.
#[derive(
    borsh::BorshSerialize, borsh::BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema,
)]
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

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct RoutedMessageV2 {
    /// Message
    pub msg: RoutedMessage,
    /// The time the Routed message was created by `author`.
    pub created_at: Option<time::Utc>,
    /// Number of peers this routed message travelled through.
    /// Doesn't include the peers that are the source and the destination of the message.
    pub num_hops: u32,
}

impl std::ops::Deref for RoutedMessageV2 {
    type Target = RoutedMessage;

    fn deref(&self) -> &Self::Target {
        &self.msg
    }
}

impl std::ops::DerefMut for RoutedMessageV2 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.msg
    }
}

#[derive(borsh::BorshSerialize, PartialEq, Eq, Clone, Debug)]
struct RoutedMessageNoSignature<'a> {
    target: &'a PeerIdOrHash,
    author: &'a PeerId,
    body: &'a RoutedMessageBody,
}

impl RoutedMessage {
    pub fn build_hash(
        target: &PeerIdOrHash,
        source: &PeerId,
        body: &RoutedMessageBody,
    ) -> CryptoHash {
        CryptoHash::hash_borsh(RoutedMessageNoSignature { target, author: source, body })
    }

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
                | RoutedMessageBody::EpochSyncRequest
        )
    }

    /// Return true if ttl is positive after decreasing ttl by one, false otherwise.
    pub fn decrease_ttl(&mut self) -> bool {
        self.ttl = self.ttl.saturating_sub(1);
        self.ttl > 0
    }

    pub fn body_variant(&self) -> &'static str {
        (&self.body).into()
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
            chunk_hash: header.chunk_hash(),
            inner_header_hash: header.inner_header_hash(),
            merkle_root: header.encoded_merkle_root(),
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

pub(crate) struct RawRoutedMessage {
    pub target: PeerIdOrHash,
    pub body: RoutedMessageBody,
}

impl RawRoutedMessage {
    /// Add signature to the message.
    /// Panics if the target is an AccountId instead of a PeerId.
    pub fn sign(
        self,
        node_key: &near_crypto::SecretKey,
        routed_message_ttl: u8,
        now: Option<time::Utc>,
    ) -> RoutedMessageV2 {
        let author = PeerId::new(node_key.public_key());
        let hash = RoutedMessage::build_hash(&self.target, &author, &self.body);
        let signature = node_key.sign(hash.as_ref());
        RoutedMessageV2 {
            msg: RoutedMessage {
                target: self.target,
                author,
                signature,
                ttl: routed_message_ttl,
                body: self.body,
            },
            created_at: now,
            num_hops: 0,
        }
    }
}
