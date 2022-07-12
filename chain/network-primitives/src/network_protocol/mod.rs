use crate::time::Utc;
/// `network_protocol.rs` contains types which are part of network protocol.
/// We need to maintain backward compatibility in network protocol.
/// All changes to this file should be reviewed.
///
/// TODO: - document all types in this file
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::Signature;
use near_primitives::block::{Approval, GenesisId};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::combine_hash;
use near_primitives::network::PeerId;
use near_primitives::sharding::{
    ChunkHash, PartialEncodedChunk, PartialEncodedChunkPart, PartialEncodedChunkV1,
    PartialEncodedChunkWithArcReceipts, ReceiptProof, ShardChunkHeader,
};
use near_primitives::syncing::{ShardStateSyncResponse, ShardStateSyncResponseV1};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight, BlockReference, ShardId};
use near_primitives::views::{FinalExecutionOutcomeView, QueryRequest, QueryResponse};
use std::collections::HashSet;
use std::fmt;
use std::fmt::{Debug, Error, Formatter};
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::{Deref, DerefMut};
use std::str::FromStr;

pub(crate) mod edge;

/// Peer information.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq)]
pub struct PeerInfo {
    pub id: PeerId,
    pub addr: Option<SocketAddr>,
    pub account_id: Option<AccountId>,
}

#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for PeerInfo {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.id.deep_size_of_children(context) + self.account_id.deep_size_of_children(context)
    }
}

impl PeerInfo {
    /// Creates random peer info.
    pub fn new(id: PeerId, addr: SocketAddr) -> Self {
        PeerInfo { id, addr: Some(addr), account_id: None }
    }

    pub fn random() -> Self {
        PeerInfo { id: PeerId::random(), addr: None, account_id: None }
    }

    pub fn addr_port(&self) -> Option<u16> {
        self.addr.map(|addr| addr.port())
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
    /// Returns a PeerInfo from string
    ///
    /// Valid format examples:
    ///     ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV
    ///     ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@127.0.0.1:24567
    ///     ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@test.near
    ///     ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@127.0.0.1:24567@test.near
    ///
    /// Hostname can be used instead of IP address, if node trusts DNS server it connects to, for example:
    ///     ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@localhost:24567@test.near
    ///     ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@my.own.node.test:24567@test.near
    ///
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let chunks: Vec<&str> = s.split('@').collect();
        let addr;
        let account_id;

        let invalid_peer_error_factory = || -> Self::Err {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid PeerInfo format: {:?}", chunks),
            ))
        };

        if chunks.len() == 1 {
            addr = None;
            account_id = None;
        } else if chunks.len() == 2 {
            if let Ok(mut x) = chunks[1].to_socket_addrs() {
                addr = x.next();
                account_id = None;
            } else {
                addr = None;
                account_id = Some(chunks[1].parse().unwrap());
            }
        } else if chunks.len() == 3 {
            if let Ok(mut x) = chunks[1].to_socket_addrs() {
                addr = x.next();
                account_id = Some(chunks[2].parse().unwrap());
            } else {
                return Err(invalid_peer_error_factory());
            }
        } else {
            return Err(invalid_peer_error_factory());
        }
        Ok(PeerInfo { id: PeerId::new(chunks[0].parse()?), addr, account_id })
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
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, Default)]
/// Represents `peers` view about chain.
pub struct PeerChainInfo {
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Last known chain height of the peer.
    pub height: BlockHeight,
    /// Shards that the peer is tracking.
    pub tracked_shards: Vec<ShardId>,
}

/// Peer chain information.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, Default)]
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

/// Test code that someone become part of our protocol?
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, Hash)]
pub struct Ping {
    pub nonce: u64,
    pub source: PeerId,
}

/// Test code that someone become part of our protocol?
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, Hash)]
pub struct Pong {
    pub nonce: u64,
    pub source: PeerId,
}

// TODO(#1313): Use Box
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, strum::IntoStaticStr)]
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
    /// Not used, but needed to preserve backward compatibility.
    Unused,
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
            RoutedMessageBody::Unused => write!(f, "Unused"),
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
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
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

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct RoutedMessageV2 {
    /// Message
    pub msg: RoutedMessage,
    /// The time the Routed message was created by `author`.
    pub created_at: Option<Utc>,
}

#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for RoutedMessageV2 {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.msg.deep_size_of_children(context) + std::mem::size_of::<Option<Utc>>()
    }
}

impl Deref for RoutedMessageV2 {
    type Target = RoutedMessage;

    fn deref(&self) -> &Self::Target {
        &self.msg
    }
}

impl DerefMut for RoutedMessageV2 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.msg
    }
}

#[derive(BorshSerialize, PartialEq, Eq, Clone, Debug)]
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
        CryptoHash::hash_borsh(&RoutedMessageNoSignature { target, author: source, body })
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
                | RoutedMessageBody::StateRequestHeader(_, _)
                | RoutedMessageBody::StateRequestPart(_, _, _)
                | RoutedMessageBody::PartialEncodedChunkRequest(_)
                | RoutedMessageBody::QueryRequest { .. }
                | RoutedMessageBody::ReceiptOutcomeRequest(_)
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

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, Hash)]
pub enum PeerIdOrHash {
    PeerId(PeerId),
    Hash(CryptoHash),
}

/// Message for chunk part owners to forward their parts to validators tracking that shard.
/// This reduces the number of requests a node tracking a shard needs to send to obtain enough
/// parts to reconstruct the message (in the best case no such requests are needed).
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
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
            prev_block_hash: header.prev_block_hash().clone(),
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

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct PartialEncodedChunkRequestMsg {
    pub chunk_hash: ChunkHash,
    pub part_ords: Vec<u64>,
    pub tracking_shards: HashSet<ShardId>,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct PartialEncodedChunkResponseMsg {
    pub chunk_hash: ChunkHash,
    pub parts: Vec<PartialEncodedChunkPart>,
    pub receipts: Vec<ReceiptProof>,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateResponseInfoV1 {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub state_response: ShardStateSyncResponseV1,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateResponseInfoV2 {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub state_response: ShardStateSyncResponse,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
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

#[cfg(test)]
mod test {
    use std::net::IpAddr;
    use std::net::SocketAddr;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    /// TODO this test might require an improvement (probably by mocking the DNS resolution)
    fn test_from_str() {
        use crate::types::PeerInfo;
        use std::str::FromStr;

        let socket_v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1337);
        let socket_v6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 1337);

        let mut peer_test = PeerInfo::from_str(
            "ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@localhost:1337@account.near",
        )
        .unwrap();
        assert!(peer_test.addr.unwrap() == socket_v4 || peer_test.addr.unwrap() == socket_v6);

        peer_test = PeerInfo::from_str(
            "ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@localhost:1337",
        )
        .unwrap();
        assert!(peer_test.addr.unwrap() == socket_v4 || peer_test.addr.unwrap() == socket_v6);

        peer_test = PeerInfo::from_str(
            "ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@127.0.0.1:1337@account.near",
        )
        .unwrap();
        assert!(peer_test.addr.unwrap() == socket_v4 || peer_test.addr.unwrap() == socket_v6);

        peer_test = PeerInfo::from_str(
            "ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@127.0.0.1:1337",
        )
        .unwrap();
        assert!(peer_test.addr.unwrap() == socket_v4 || peer_test.addr.unwrap() == socket_v6);
    }
}
