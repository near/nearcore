use std::borrow::Borrow;
use std::convert::{TryFrom, TryInto};
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;

use serde_derive::{Deserialize, Serialize};

use crate::block_traits::{SignedBlock, SignedHeader};
use crate::consensus::Payload;
use crate::hash::{hash_struct, CryptoHash};
use crate::merkle::{Direction, MerklePath};
use crate::transaction::{ReceiptTransaction, SignedTransaction};
use crate::types::{AuthorityId, BlockIndex, MerkleHash, ShardId};
use crate::utils::proto_to_type;
use near_protos::chain as chain_proto;
use near_protos::network as network_proto;
use near_protos::types as types_proto;
use protobuf::{RepeatedField, SingularPtrField};
use crate::crypto::group_signature::GroupSignature;
use crate::types::PartialSignature;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardBlockHeader {
    pub parent_hash: CryptoHash,
    pub shard_id: ShardId,
    pub index: BlockIndex,
    pub merkle_root_state: MerkleHash,
    /// If there are no receipts generated in this block, the root is hash(0)
    pub receipt_merkle_root: MerkleHash,
}

impl TryFrom<chain_proto::ShardBlockHeader> for ShardBlockHeader {
    type Error = String;

    fn try_from(proto: chain_proto::ShardBlockHeader) -> Result<Self, Self::Error> {
        Ok(ShardBlockHeader {
            parent_hash: proto.parent_hash.try_into()?,
            shard_id: proto.shard_id,
            index: proto.block_index,
            merkle_root_state: proto.merkle_root_state.try_into()?,
            receipt_merkle_root: proto.receipt_merkle_root.try_into()?,
        })
    }
}

impl From<ShardBlockHeader> for chain_proto::ShardBlockHeader {
    fn from(header: ShardBlockHeader) -> Self {
        chain_proto::ShardBlockHeader {
            parent_hash: header.parent_hash.into(),
            shard_id: header.shard_id,
            block_index: header.index,
            merkle_root_state: header.merkle_root_state.into(),
            receipt_merkle_root: header.receipt_merkle_root.into(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedShardBlockHeader {
    pub body: ShardBlockHeader,
    pub hash: CryptoHash,
    pub signature: GroupSignature,
}

impl TryFrom<chain_proto::SignedShardBlockHeader> for SignedShardBlockHeader {
    type Error = String;

    fn try_from(proto: chain_proto::SignedShardBlockHeader) -> Result<Self, Self::Error> {
        let body = proto_to_type(proto.body)?;
        let signature = proto_to_type(proto.signature)?;
        let hash = proto.hash.try_into()?;
        Ok(SignedShardBlockHeader { body, hash, signature })
    }
}

impl From<SignedShardBlockHeader> for chain_proto::SignedShardBlockHeader {
    fn from(header: SignedShardBlockHeader) -> Self {
        chain_proto::SignedShardBlockHeader {
            body: SingularPtrField::some(header.body.into()),
            hash: header.hash.into(),
            signature: SingularPtrField::some(header.signature.into()),
            ..Default::default()
        }
    }
}

impl SignedShardBlockHeader {
    #[inline]
    pub fn shard_id(&self) -> ShardId {
        self.body.shard_id
    }

    #[inline]
    pub fn merkle_root_state(&self) -> MerkleHash {
        self.body.merkle_root_state
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardBlock {
    pub header: ShardBlockHeader,
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<ReceiptBlock>,
}

impl TryFrom<chain_proto::ShardBlock> for ShardBlock {
    type Error = String;

    fn try_from(proto: chain_proto::ShardBlock) -> Result<Self, Self::Error> {
        let transactions =
            proto.transactions.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        let receipts =
            proto.receipts.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        let header = proto_to_type(proto.header)?;
        Ok(ShardBlock { header, transactions, receipts })
    }
}

impl From<ShardBlock> for chain_proto::ShardBlock {
    fn from(block: ShardBlock) -> Self {
        chain_proto::ShardBlock {
            header: SingularPtrField::some(block.header.into()),
            transactions: block.transactions.into_iter().map(std::convert::Into::into).collect(),
            receipts: block.receipts.into_iter().map(std::convert::Into::into).collect(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedShardBlock {
    pub body: ShardBlock,
    pub hash: CryptoHash,
    pub signature: GroupSignature,
}

impl TryFrom<chain_proto::SignedShardBlock> for SignedShardBlock {
    type Error = String;

    fn try_from(proto: chain_proto::SignedShardBlock) -> Result<Self, Self::Error> {
        let body = proto_to_type(proto.body)?;
        let signature = proto_to_type(proto.signature)?;
        let hash = proto.hash.try_into()?;
        Ok(SignedShardBlock { body, hash, signature })
    }
}

impl From<SignedShardBlock> for chain_proto::SignedShardBlock {
    fn from(block: SignedShardBlock) -> Self {
        chain_proto::SignedShardBlock {
            body: SingularPtrField::some(block.body.into()),
            hash: block.hash.into(),
            signature: SingularPtrField::some(block.signature.into()),
            ..Default::default()
        }
    }
}

impl Borrow<CryptoHash> for SignedShardBlock {
    fn borrow(&self) -> &CryptoHash {
        &self.hash
    }
}

impl Hash for SignedShardBlock {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl PartialEq for SignedShardBlock {
    fn eq(&self, other: &SignedShardBlock) -> bool {
        self.hash == other.hash
    }
}

impl Eq for SignedShardBlock {}

#[derive(Debug, Clone, Eq, Serialize, Deserialize)]
pub struct ReceiptBlock {
    pub header: SignedShardBlockHeader,
    pub path: MerklePath,
    // receipts should not be empty
    pub receipts: Vec<ReceiptTransaction>,
    // hash is the hash of receipts. It is
    // sufficient to uniquely identify the
    // receipt block because of the uniqueness
    // of nonce in receipts
    hash: CryptoHash,
}

impl TryFrom<chain_proto::ReceiptBlock> for ReceiptBlock {
    type Error = String;

    fn try_from(proto: chain_proto::ReceiptBlock) -> Result<Self, Self::Error> {
        let path = proto
            .path
            .into_iter()
            .map(|node| {
                let direction = if node.direction { Direction::Left } else { Direction::Right };
                Ok::<_, String>((node.hash.try_into()?, direction))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let receipts =
            proto.receipts.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        let header = proto_to_type(proto.header)?;
        Ok(ReceiptBlock { header, path, receipts, hash: proto.hash.try_into()? })
    }
}

impl From<ReceiptBlock> for chain_proto::ReceiptBlock {
    fn from(receipt: ReceiptBlock) -> Self {
        let path = RepeatedField::from_iter(receipt.path.into_iter().map(|(hash, dir)| {
            types_proto::MerkleNode {
                hash: hash.into(),
                direction: dir == Direction::Left,
                ..Default::default()
            }
        }));
        chain_proto::ReceiptBlock {
            header: SingularPtrField::some(receipt.header.into()),
            path,
            receipts: RepeatedField::from_iter(
                receipt.receipts.into_iter().map(std::convert::Into::into),
            ),
            hash: receipt.hash.into(),
            ..Default::default()
        }
    }
}

impl PartialEq for ReceiptBlock {
    fn eq(&self, other: &ReceiptBlock) -> bool {
        self.hash == other.hash
    }
}

impl Hash for ReceiptBlock {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state)
    }
}

impl Borrow<CryptoHash> for ReceiptBlock {
    fn borrow(&self) -> &CryptoHash {
        &self.hash
    }
}

impl ReceiptBlock {
    pub fn new(
        header: SignedShardBlockHeader,
        path: MerklePath,
        receipts: Vec<ReceiptTransaction>,
    ) -> Self {
        let hash = hash_struct(&receipts);
        ReceiptBlock { header, path, receipts, hash }
    }

    pub fn get_hash(&self) -> CryptoHash {
        self.hash
    }
}

impl SignedHeader for SignedShardBlockHeader {
    #[inline]
    fn block_hash(&self) -> CryptoHash {
        self.hash
    }
    #[inline]
    fn index(&self) -> u64 {
        self.body.index
    }
    #[inline]
    fn parent_hash(&self) -> CryptoHash {
        self.body.parent_hash
    }
}

impl SignedShardBlock {
    pub fn new(
        shard_id: ShardId,
        index: u64,
        parent_hash: CryptoHash,
        merkle_root_state: MerkleHash,
        transactions: Vec<SignedTransaction>,
        receipts: Vec<ReceiptBlock>,
        receipt_merkle_root: MerkleHash,
    ) -> Self {
        let header = ShardBlockHeader {
            shard_id,
            index,
            parent_hash,
            merkle_root_state,
            receipt_merkle_root,
        };
        let hash = hash_struct(&header);
        SignedShardBlock {
            body: ShardBlock { header, transactions, receipts },
            hash,
            signature: GroupSignature::default(),
        }
    }

    pub fn genesis(merkle_root_state: MerkleHash) -> SignedShardBlock {
        SignedShardBlock::new(
            0,
            0,
            CryptoHash::default(),
            merkle_root_state,
            vec![],
            vec![],
            CryptoHash::default(),
        )
    }

    #[inline]
    pub fn merkle_root_state(&self) -> MerkleHash {
        self.body.header.merkle_root_state
    }

    #[inline]
    pub fn shard_id(&self) -> ShardId {
        self.body.header.shard_id
    }
}

impl SignedBlock for SignedShardBlock {
    type SignedHeader = SignedShardBlockHeader;

    fn header(&self) -> Self::SignedHeader {
        SignedShardBlockHeader {
            body: self.body.header.clone(),
            hash: self.hash,
            signature: self.signature.clone(),
        }
    }

    #[inline]
    fn index(&self) -> u64 {
        self.body.header.index
    }

    #[inline]
    fn block_hash(&self) -> CryptoHash {
        self.hash
    }

    fn add_signature(&mut self, signature: &PartialSignature, authority_id: usize) {
        self.signature.add_signature(signature, authority_id);
    }

    fn weight(&self) -> u128 {
        1
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ChainPayload {
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<ReceiptBlock>,
    hash: CryptoHash,
}

impl TryFrom<chain_proto::ChainPayload> for ChainPayload {
    type Error = String;

    fn try_from(proto: chain_proto::ChainPayload) -> Result<Self, Self::Error> {
        let transactions =
            proto.transactions.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        let receipts =
            proto.receipts.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        Ok(ChainPayload { transactions, receipts, hash: proto.hash.try_into()? })
    }
}

impl From<ChainPayload> for chain_proto::ChainPayload {
    fn from(payload: ChainPayload) -> Self {
        chain_proto::ChainPayload {
            transactions: RepeatedField::from_iter(
                payload.transactions.into_iter().map(std::convert::Into::into),
            ),
            receipts: RepeatedField::from_iter(
                payload.receipts.into_iter().map(std::convert::Into::into),
            ),
            hash: payload.hash.into(),
            ..Default::default()
        }
    }
}

impl ChainPayload {
    pub fn new(transactions: Vec<SignedTransaction>, receipts: Vec<ReceiptBlock>) -> Self {
        let hash = hash_struct(&(&transactions, &receipts));
        ChainPayload { transactions, receipts, hash }
    }

    pub fn get_hash(&self) -> CryptoHash {
        self.hash
    }
}

impl Hash for ChainPayload {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state)
    }
}

impl PartialEq for ChainPayload {
    fn eq(&self, other: &ChainPayload) -> bool {
        self.hash == other.hash
    }
}

impl Borrow<CryptoHash> for ChainPayload {
    fn borrow(&self) -> &CryptoHash {
        &self.hash
    }
}

impl Eq for ChainPayload {}

impl Payload for ChainPayload {
    fn verify(&self) -> Result<(), &'static str> {
        Ok(())
    }

    fn union_update(&mut self, mut other: Self) {
        self.transactions.extend(other.transactions.drain(..));
        self.receipts.extend(other.receipts.drain(..))
    }

    fn is_empty(&self) -> bool {
        self.transactions.is_empty() && self.receipts.is_empty()
    }

    fn new() -> Self {
        Self { transactions: vec![], receipts: vec![], hash: CryptoHash::default() }
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct ChainState {
    pub genesis_hash: CryptoHash,
    pub last_index: u64,
}

impl TryFrom<chain_proto::ChainState> for ChainState {
    type Error = String;

    fn try_from(proto: chain_proto::ChainState) -> Result<Self, Self::Error> {
        Ok(ChainState {
            genesis_hash: proto.genesis_hash.try_into()?,
            last_index: proto.last_index,
        })
    }
}

impl From<ChainState> for chain_proto::ChainState {
    fn from(chain_state: ChainState) -> chain_proto::ChainState {
        chain_proto::ChainState {
            genesis_hash: chain_state.genesis_hash.into(),
            last_index: chain_state.last_index,
            ..Default::default()
        }
    }
}

/// request missing parts of the payload snapshot which has hash snapshot_hash
#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct MissingPayloadRequest {
    pub transactions: Vec<CryptoHash>,
    pub receipts: Vec<CryptoHash>,
    pub snapshot_hash: CryptoHash,
}

impl TryFrom<network_proto::MissingPayloadRequest> for MissingPayloadRequest {
    type Error = String;

    fn try_from(proto: network_proto::MissingPayloadRequest) -> Result<Self, Self::Error> {
        let transactions =
            proto.transactions.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        let receipts =
            proto.receipts.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        Ok(MissingPayloadRequest {
            transactions,
            receipts,
            snapshot_hash: proto.snapshot_hash.try_into()?,
        })
    }
}

impl From<MissingPayloadRequest> for network_proto::MissingPayloadRequest {
    fn from(response: MissingPayloadRequest) -> Self {
        let transactions = RepeatedField::from_iter(
            response.transactions.into_iter().map(std::convert::Into::into),
        );
        let receipts =
            RepeatedField::from_iter(response.receipts.into_iter().map(std::convert::Into::into));
        network_proto::MissingPayloadRequest {
            transactions,
            receipts,
            snapshot_hash: response.snapshot_hash.into(),
            ..Default::default()
        }
    }
}

/// response to missing parts of the payload snapshot which has hash snapshot_hash
/// it is basically a ChainPayload except that snapshot_hash is not the hash of the payload
#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct MissingPayloadResponse {
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<ReceiptBlock>,
    pub snapshot_hash: CryptoHash,
}

impl TryFrom<network_proto::MissingPayloadResponse> for MissingPayloadResponse {
    type Error = String;

    fn try_from(proto: network_proto::MissingPayloadResponse) -> Result<Self, Self::Error> {
        let transactions =
            proto.transactions.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        let receipts =
            proto.receipts.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        Ok(MissingPayloadResponse {
            transactions,
            receipts,
            snapshot_hash: proto.snapshot_hash.try_into()?,
        })
    }
}

impl From<MissingPayloadResponse> for network_proto::MissingPayloadResponse {
    fn from(response: MissingPayloadResponse) -> Self {
        let transactions = RepeatedField::from_iter(
            response.transactions.into_iter().map(std::convert::Into::into),
        );
        let receipts =
            RepeatedField::from_iter(response.receipts.into_iter().map(std::convert::Into::into));
        network_proto::MissingPayloadResponse {
            transactions,
            receipts,
            snapshot_hash: response.snapshot_hash.into(),
            ..Default::default()
        }
    }
}

impl MissingPayloadResponse {
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty() && self.receipts.is_empty()
    }
}

pub enum PayloadRequest {
    General(AuthorityId, MissingPayloadRequest),
    BlockProposal(AuthorityId, CryptoHash),
}

pub enum PayloadResponse {
    General(AuthorityId, MissingPayloadResponse),
    BlockProposal(AuthorityId, Snapshot),
}

/// snapshot of payload. Stores only necessary information for retrieving
/// the actual payload.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub transactions: Vec<CryptoHash>,
    pub receipts: Vec<CryptoHash>,
    hash: CryptoHash,
}

impl TryFrom<network_proto::Snapshot> for Snapshot {
    type Error = String;

    fn try_from(proto: network_proto::Snapshot) -> Result<Self, Self::Error> {
        let transactions =
            proto.transactions.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        let receipts =
            proto.receipts.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        Ok(Snapshot { transactions, receipts, hash: proto.hash.try_into()? })
    }
}

impl From<Snapshot> for network_proto::Snapshot {
    fn from(snapshot: Snapshot) -> Self {
        let transactions = RepeatedField::from_iter(
            snapshot.transactions.into_iter().map(std::convert::Into::into),
        );
        let receipts =
            RepeatedField::from_iter(snapshot.receipts.into_iter().map(std::convert::Into::into));
        network_proto::Snapshot {
            transactions,
            receipts,
            hash: snapshot.hash.into(),
            ..Default::default()
        }
    }
}

impl PartialEq for Snapshot {
    fn eq(&self, other: &Snapshot) -> bool {
        self.hash == other.hash
    }
}

impl Eq for Snapshot {}

impl Hash for Snapshot {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state)
    }
}

impl Borrow<CryptoHash> for Snapshot {
    fn borrow(&self) -> &CryptoHash {
        &self.hash
    }
}

impl Snapshot {
    pub fn new(transactions: Vec<CryptoHash>, receipts: Vec<CryptoHash>) -> Self {
        let hash = hash_struct(&(&transactions, &receipts));
        Snapshot { transactions, receipts, hash }
    }

    pub fn get_hash(&self) -> CryptoHash {
        self.hash
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty() && self.receipts.is_empty()
    }

    pub fn clear(&mut self) {
        self.transactions.clear();
        self.receipts.clear();
        self.hash = CryptoHash::default();
    }
}
