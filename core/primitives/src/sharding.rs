use borsh::{BorshDeserialize, BorshSerialize};
use reed_solomon_erasure::galois_8::{Field, ReedSolomon};
use serde::Serialize;

use near_crypto::Signature;

use crate::hash::{hash, CryptoHash};
use crate::merkle::{combine_hash, merklize, MerklePath};
use crate::receipt::Receipt;
use crate::transaction::SignedTransaction;
use crate::types::validator_stake::{ValidatorStake, ValidatorStakeIter, ValidatorStakeV1};
use crate::types::{Balance, BlockHeight, Gas, MerkleHash, ShardId, StateRoot};
use crate::validator_signer::ValidatorSigner;
#[cfg(feature = "protocol_feature_block_header_v3")]
use crate::version::ProtocolFeature;
use crate::version::{ProtocolVersion, ProtocolVersionRange, SHARD_CHUNK_HEADER_UPGRADE_VERSION};
use reed_solomon_erasure::ReconstructShard;
use std::sync::Arc;

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Serialize,
    Hash,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Default,
)]
pub struct ChunkHash(pub CryptoHash);

impl AsRef<[u8]> for ChunkHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<ChunkHash> for Vec<u8> {
    fn from(chunk_hash: ChunkHash) -> Self {
        chunk_hash.0.into()
    }
}

impl From<CryptoHash> for ChunkHash {
    fn from(crypto_hash: CryptoHash) -> Self {
        Self(crypto_hash)
    }
}

#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ShardInfo(pub ShardId, pub ChunkHash);

/// Contains the information that is used to sync state for shards as epochs switch
#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct StateSyncInfo {
    /// The first block of the epoch for which syncing is happening
    pub epoch_tail_hash: CryptoHash,
    /// Shards to fetch state
    pub shards: Vec<ShardInfo>,
}

#[cfg(feature = "protocol_feature_block_header_v3")]
pub mod shard_chunk_header_inner;
#[cfg(feature = "protocol_feature_block_header_v3")]
pub use shard_chunk_header_inner::{
    ShardChunkHeaderInner, ShardChunkHeaderInnerV1, ShardChunkHeaderInnerV2,
};

#[cfg(not(feature = "protocol_feature_block_header_v3"))]
#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, PartialEq, Eq, Debug)]
pub struct ShardChunkHeaderInner {
    /// Previous block hash.
    pub prev_block_hash: CryptoHash,
    pub prev_state_root: StateRoot,
    /// Root of the outcomes from execution transactions and results.
    pub outcome_root: CryptoHash,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockHeight,
    /// Shard index.
    pub shard_id: ShardId,
    /// Gas used in this chunk.
    pub gas_used: Gas,
    /// Gas limit voted by validators.
    pub gas_limit: Gas,
    /// Total balance burnt in previous chunk
    pub balance_burnt: Balance,
    /// Outgoing receipts merkle root.
    pub outgoing_receipts_root: CryptoHash,
    /// Tx merkle root.
    pub tx_root: CryptoHash,
    /// Validator proposals.
    pub validator_proposals: Vec<ValidatorStake>,
}
#[cfg(not(feature = "protocol_feature_block_header_v3"))]
pub type ShardChunkHeaderInnerV1 = ShardChunkHeaderInner;
#[cfg(not(feature = "protocol_feature_block_header_v3"))]
impl ShardChunkHeaderInner {
    #[inline]
    pub fn prev_state_root(&self) -> &StateRoot {
        &self.prev_state_root
    }

    #[inline]
    pub fn prev_block_hash(&self) -> &CryptoHash {
        &self.prev_block_hash
    }

    #[inline]
    pub fn gas_limit(&self) -> Gas {
        self.gas_limit
    }

    #[inline]
    pub fn gas_used(&self) -> Gas {
        self.gas_used
    }

    #[inline]
    pub fn validator_proposals(&self) -> ValidatorStakeIter {
        ValidatorStakeIter::new(&self.validator_proposals)
    }

    #[inline]
    pub fn height_created(&self) -> BlockHeight {
        self.height_created
    }

    #[inline]
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    #[inline]
    pub fn outcome_root(&self) -> &CryptoHash {
        &self.outcome_root
    }

    #[inline]
    pub fn encoded_merkle_root(&self) -> &CryptoHash {
        &self.encoded_merkle_root
    }

    #[inline]
    pub fn encoded_length(&self) -> u64 {
        self.encoded_length
    }

    #[inline]
    pub fn balance_burnt(&self) -> Balance {
        self.balance_burnt
    }

    #[inline]
    pub fn outgoing_receipts_root(&self) -> &CryptoHash {
        &self.outgoing_receipts_root
    }

    #[inline]
    pub fn tx_root(&self) -> &CryptoHash {
        &self.tx_root
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, PartialEq, Eq, Debug)]
#[borsh_init(init)]
pub struct ShardChunkHeaderV1 {
    pub inner: ShardChunkHeaderInnerV1,

    pub height_included: BlockHeight,

    /// Signature of the chunk producer.
    pub signature: Signature,

    #[borsh_skip]
    pub hash: ChunkHash,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, PartialEq, Eq, Debug)]
#[borsh_init(init)]
pub struct ShardChunkHeaderV2 {
    pub inner: ShardChunkHeaderInnerV1,

    pub height_included: BlockHeight,

    /// Signature of the chunk producer.
    pub signature: Signature,

    #[borsh_skip]
    pub hash: ChunkHash,
}

impl ShardChunkHeaderV2 {
    pub fn init(&mut self) {
        self.hash = Self::compute_hash(&self.inner);
    }

    pub fn compute_hash(inner: &ShardChunkHeaderInnerV1) -> ChunkHash {
        let inner_bytes = inner.try_to_vec().expect("Failed to serialize");
        let inner_hash = hash(&inner_bytes);

        ChunkHash(combine_hash(inner_hash, inner.encoded_merkle_root))
    }

    pub fn new(
        prev_block_hash: CryptoHash,
        prev_state_root: StateRoot,
        outcome_root: CryptoHash,
        encoded_merkle_root: CryptoHash,
        encoded_length: u64,
        height: BlockHeight,
        shard_id: ShardId,
        gas_used: Gas,
        gas_limit: Gas,
        balance_burnt: Balance,
        outgoing_receipts_root: CryptoHash,
        tx_root: CryptoHash,
        validator_proposals: Vec<ValidatorStakeV1>,
        signer: &dyn ValidatorSigner,
    ) -> Self {
        let inner = ShardChunkHeaderInnerV1 {
            prev_block_hash,
            prev_state_root,
            outcome_root,
            encoded_merkle_root,
            encoded_length,
            height_created: height,
            shard_id,
            gas_used,
            gas_limit,
            balance_burnt,
            outgoing_receipts_root,
            tx_root,
            validator_proposals,
        };
        let hash = Self::compute_hash(&inner);
        let signature = signer.sign_chunk_hash(&hash);
        Self { inner, height_included: 0, signature, hash }
    }
}

// V2 -> V3: Use versioned ShardChunkHeaderInner structure
#[cfg(feature = "protocol_feature_block_header_v3")]
#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, PartialEq, Eq, Debug)]
#[borsh_init(init)]
pub struct ShardChunkHeaderV3 {
    pub inner: ShardChunkHeaderInner,

    pub height_included: BlockHeight,

    /// Signature of the chunk producer.
    pub signature: Signature,

    #[borsh_skip]
    pub hash: ChunkHash,
}

#[cfg(feature = "protocol_feature_block_header_v3")]
impl ShardChunkHeaderV3 {
    pub fn init(&mut self) {
        self.hash = Self::compute_hash(&self.inner);
    }

    pub fn compute_hash(inner: &ShardChunkHeaderInner) -> ChunkHash {
        let inner_bytes = inner.try_to_vec().expect("Failed to serialize");
        let inner_hash = hash(&inner_bytes);

        ChunkHash(combine_hash(inner_hash, *inner.encoded_merkle_root()))
    }

    pub fn new(
        prev_block_hash: CryptoHash,
        prev_state_root: StateRoot,
        outcome_root: CryptoHash,
        encoded_merkle_root: CryptoHash,
        encoded_length: u64,
        height: BlockHeight,
        shard_id: ShardId,
        gas_used: Gas,
        gas_limit: Gas,
        balance_burnt: Balance,
        outgoing_receipts_root: CryptoHash,
        tx_root: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        signer: &dyn ValidatorSigner,
    ) -> Self {
        let inner = ShardChunkHeaderInner::V2(ShardChunkHeaderInnerV2 {
            prev_block_hash,
            prev_state_root,
            outcome_root,
            encoded_merkle_root,
            encoded_length,
            height_created: height,
            shard_id,
            gas_used,
            gas_limit,
            balance_burnt,
            outgoing_receipts_root,
            tx_root,
            validator_proposals,
        });
        let hash = Self::compute_hash(&inner);
        let signature = signer.sign_chunk_hash(&hash);
        Self { inner, height_included: 0, signature, hash }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, PartialEq, Eq, Debug)]
pub enum ShardChunkHeader {
    V1(ShardChunkHeaderV1),
    V2(ShardChunkHeaderV2),
    #[cfg(feature = "protocol_feature_block_header_v3")]
    V3(ShardChunkHeaderV3),
}

impl ShardChunkHeader {
    #[cfg(not(feature = "protocol_feature_block_header_v3"))]
    #[inline]
    pub fn take_inner(self) -> ShardChunkHeaderInner {
        match self {
            Self::V1(header) => header.inner,
            Self::V2(header) => header.inner,
        }
    }

    #[cfg(feature = "protocol_feature_block_header_v3")]
    #[inline]
    pub fn take_inner(self) -> ShardChunkHeaderInner {
        match self {
            Self::V1(header) => ShardChunkHeaderInner::V1(header.inner),
            Self::V2(header) => ShardChunkHeaderInner::V1(header.inner),
            Self::V3(header) => header.inner,
        }
    }

    pub fn inner_header_hash(&self) -> CryptoHash {
        let inner_bytes = match self {
            Self::V1(header) => header.inner.try_to_vec(),
            Self::V2(header) => header.inner.try_to_vec(),
            #[cfg(feature = "protocol_feature_block_header_v3")]
            Self::V3(header) => header.inner.try_to_vec(),
        };
        hash(&inner_bytes.expect("Failed to serialize"))
    }

    #[inline]
    pub fn height_created(&self) -> BlockHeight {
        match self {
            Self::V1(header) => header.inner.height_created,
            Self::V2(header) => header.inner.height_created,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            Self::V3(header) => header.inner.height_created(),
        }
    }

    #[inline]
    pub fn signature(&self) -> &Signature {
        match self {
            Self::V1(header) => &header.signature,
            Self::V2(header) => &header.signature,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            Self::V3(header) => &header.signature,
        }
    }

    #[inline]
    pub fn height_included(&self) -> BlockHeight {
        match self {
            Self::V1(header) => header.height_included,
            Self::V2(header) => header.height_included,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            Self::V3(header) => header.height_included,
        }
    }

    #[inline]
    pub fn height_included_mut(&mut self) -> &mut BlockHeight {
        match self {
            Self::V1(header) => &mut header.height_included,
            Self::V2(header) => &mut header.height_included,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            Self::V3(header) => &mut header.height_included,
        }
    }

    #[inline]
    pub fn validator_proposals(&self) -> ValidatorStakeIter {
        match self {
            Self::V1(header) => ValidatorStakeIter::v1(&header.inner.validator_proposals),
            Self::V2(header) => ValidatorStakeIter::v1(&header.inner.validator_proposals),
            #[cfg(feature = "protocol_feature_block_header_v3")]
            Self::V3(header) => header.inner.validator_proposals(),
        }
    }

    #[inline]
    pub fn prev_state_root(&self) -> StateRoot {
        match self {
            Self::V1(header) => header.inner.prev_state_root,
            Self::V2(header) => header.inner.prev_state_root,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            Self::V3(header) => *header.inner.prev_state_root(),
        }
    }

    #[inline]
    pub fn prev_block_hash(&self) -> CryptoHash {
        match self {
            Self::V1(header) => header.inner.prev_block_hash,
            Self::V2(header) => header.inner.prev_block_hash,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            Self::V3(header) => *header.inner.prev_block_hash(),
        }
    }

    #[inline]
    pub fn encoded_merkle_root(&self) -> CryptoHash {
        match self {
            Self::V1(header) => header.inner.encoded_merkle_root,
            Self::V2(header) => header.inner.encoded_merkle_root,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            Self::V3(header) => *header.inner.encoded_merkle_root(),
        }
    }

    #[inline]
    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::V1(header) => header.inner.shard_id,
            Self::V2(header) => header.inner.shard_id,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            Self::V3(header) => header.inner.shard_id(),
        }
    }

    #[inline]
    pub fn encoded_length(&self) -> u64 {
        match self {
            Self::V1(header) => header.inner.encoded_length,
            Self::V2(header) => header.inner.encoded_length,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            Self::V3(header) => header.inner.encoded_length(),
        }
    }

    #[inline]
    pub fn gas_used(&self) -> Gas {
        match &self {
            ShardChunkHeader::V1(header) => header.inner.gas_used,
            ShardChunkHeader::V2(header) => header.inner.gas_used,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            ShardChunkHeader::V3(header) => header.inner.gas_used(),
        }
    }

    #[inline]
    pub fn gas_limit(&self) -> Gas {
        match &self {
            ShardChunkHeader::V1(header) => header.inner.gas_limit,
            ShardChunkHeader::V2(header) => header.inner.gas_limit,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            ShardChunkHeader::V3(header) => header.inner.gas_limit(),
        }
    }

    #[inline]
    pub fn balance_burnt(&self) -> Balance {
        match &self {
            ShardChunkHeader::V1(header) => header.inner.balance_burnt,
            ShardChunkHeader::V2(header) => header.inner.balance_burnt,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            ShardChunkHeader::V3(header) => header.inner.balance_burnt(),
        }
    }

    #[inline]
    pub fn outgoing_receipts_root(&self) -> CryptoHash {
        match &self {
            ShardChunkHeader::V1(header) => header.inner.outgoing_receipts_root,
            ShardChunkHeader::V2(header) => header.inner.outgoing_receipts_root,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            ShardChunkHeader::V3(header) => *header.inner.outgoing_receipts_root(),
        }
    }

    #[inline]
    pub fn outcome_root(&self) -> CryptoHash {
        match &self {
            ShardChunkHeader::V1(header) => header.inner.outcome_root,
            ShardChunkHeader::V2(header) => header.inner.outcome_root,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            ShardChunkHeader::V3(header) => *header.inner.outcome_root(),
        }
    }

    #[inline]
    pub fn tx_root(&self) -> CryptoHash {
        match &self {
            ShardChunkHeader::V1(header) => header.inner.tx_root,
            ShardChunkHeader::V2(header) => header.inner.tx_root,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            ShardChunkHeader::V3(header) => *header.inner.tx_root(),
        }
    }

    #[inline]
    pub fn chunk_hash(&self) -> ChunkHash {
        match &self {
            ShardChunkHeader::V1(header) => header.hash.clone(),
            ShardChunkHeader::V2(header) => header.hash.clone(),
            #[cfg(feature = "protocol_feature_block_header_v3")]
            ShardChunkHeader::V3(header) => header.hash.clone(),
        }
    }

    #[cfg(not(feature = "protocol_feature_block_header_v3"))]
    pub fn version_range(&self) -> ProtocolVersionRange {
        match &self {
            ShardChunkHeader::V1(_) => {
                ProtocolVersionRange::new(0, Some(SHARD_CHUNK_HEADER_UPGRADE_VERSION))
            }
            ShardChunkHeader::V2(_) => {
                ProtocolVersionRange::new(SHARD_CHUNK_HEADER_UPGRADE_VERSION, None)
            }
        }
    }

    #[cfg(feature = "protocol_feature_block_header_v3")]
    pub fn version_range(&self) -> ProtocolVersionRange {
        let block_header_v3_version = ProtocolFeature::BlockHeaderV3.protocol_version();
        match &self {
            ShardChunkHeader::V1(_) => {
                ProtocolVersionRange::new(0, Some(SHARD_CHUNK_HEADER_UPGRADE_VERSION))
            }
            ShardChunkHeader::V2(_) => ProtocolVersionRange::new(
                SHARD_CHUNK_HEADER_UPGRADE_VERSION,
                Some(block_header_v3_version),
            ),
            ShardChunkHeader::V3(_) => ProtocolVersionRange::new(block_header_v3_version, None),
        }
    }
}

#[derive(
    BorshSerialize, BorshDeserialize, Serialize, Hash, Eq, PartialEq, Clone, Debug, Default,
)]
pub struct ChunkHashHeight(pub ChunkHash, pub BlockHeight);

impl ShardChunkHeaderV1 {
    pub fn init(&mut self) {
        self.hash = Self::compute_hash(&self.inner);
    }

    pub fn chunk_hash(&self) -> ChunkHash {
        self.hash.clone()
    }

    pub fn compute_hash(inner: &ShardChunkHeaderInnerV1) -> ChunkHash {
        let inner_bytes = inner.try_to_vec().expect("Failed to serialize");
        let inner_hash = hash(&inner_bytes);

        ChunkHash(inner_hash)
    }

    pub fn new(
        prev_block_hash: CryptoHash,
        prev_state_root: StateRoot,
        outcome_root: CryptoHash,
        encoded_merkle_root: CryptoHash,
        encoded_length: u64,
        height: BlockHeight,
        shard_id: ShardId,
        gas_used: Gas,
        gas_limit: Gas,
        balance_burnt: Balance,
        outgoing_receipts_root: CryptoHash,
        tx_root: CryptoHash,
        validator_proposals: Vec<ValidatorStakeV1>,
        signer: &dyn ValidatorSigner,
    ) -> Self {
        let inner = ShardChunkHeaderInnerV1 {
            prev_block_hash,
            prev_state_root,
            outcome_root,
            encoded_merkle_root,
            encoded_length,
            height_created: height,
            shard_id,
            gas_used,
            gas_limit,
            balance_burnt,
            outgoing_receipts_root,
            tx_root,
            validator_proposals,
        };
        let hash = Self::compute_hash(&inner);
        let signature = signer.sign_chunk_hash(&hash);
        Self { inner, height_included: 0, signature, hash }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub enum PartialEncodedChunk {
    V1(PartialEncodedChunkV1),
    V2(PartialEncodedChunkV2),
}

impl PartialEncodedChunk {
    pub fn new(
        header: ShardChunkHeader,
        parts: Vec<PartialEncodedChunkPart>,
        receipts: Vec<ReceiptProof>,
    ) -> Self {
        match header {
            ShardChunkHeader::V1(header) => {
                Self::V1(PartialEncodedChunkV1 { header, parts, receipts })
            }
            header => Self::V2(PartialEncodedChunkV2 { header, parts, receipts }),
        }
    }

    pub fn cloned_header(&self) -> ShardChunkHeader {
        match self {
            Self::V1(chunk) => ShardChunkHeader::V1(chunk.header.clone()),
            Self::V2(chunk) => chunk.header.clone(),
        }
    }

    pub fn chunk_hash(&self) -> ChunkHash {
        match self {
            Self::V1(chunk) => chunk.header.hash.clone(),
            Self::V2(chunk) => chunk.header.chunk_hash(),
        }
    }

    pub fn height_included(&self) -> BlockHeight {
        match self {
            Self::V1(chunk) => chunk.header.height_included,
            Self::V2(chunk) => chunk.header.height_included(),
        }
    }

    #[inline]
    pub fn parts(&self) -> &Vec<PartialEncodedChunkPart> {
        match self {
            Self::V1(chunk) => &chunk.parts,
            Self::V2(chunk) => &chunk.parts,
        }
    }

    #[inline]
    pub fn receipts(&self) -> &Vec<ReceiptProof> {
        match self {
            Self::V1(chunk) => &chunk.receipts,
            Self::V2(chunk) => &chunk.receipts,
        }
    }

    #[inline]
    pub fn prev_block(&self) -> &CryptoHash {
        match &self {
            PartialEncodedChunk::V1(chunk) => &chunk.header.inner.prev_block_hash,
            PartialEncodedChunk::V2(chunk) => match &chunk.header {
                ShardChunkHeader::V1(header) => &header.inner.prev_block_hash,
                ShardChunkHeader::V2(header) => &header.inner.prev_block_hash,
                #[cfg(feature = "protocol_feature_block_header_v3")]
                ShardChunkHeader::V3(header) => header.inner.prev_block_hash(),
            },
        }
    }

    /// Returns the lowest ProtocolVersion where this version of the message is
    /// accepted, along with the highest (exclusive), if any.
    pub fn version_range(&self) -> ProtocolVersionRange {
        match &self {
            PartialEncodedChunk::V1(_) => {
                ProtocolVersionRange::new(0, Some(SHARD_CHUNK_HEADER_UPGRADE_VERSION))
            }
            PartialEncodedChunk::V2(_) => {
                ProtocolVersionRange::new(SHARD_CHUNK_HEADER_UPGRADE_VERSION, None)
            }
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct PartialEncodedChunkV2 {
    pub header: ShardChunkHeader,
    pub parts: Vec<PartialEncodedChunkPart>,
    pub receipts: Vec<ReceiptProof>,
}

impl From<PartialEncodedChunk> for PartialEncodedChunkV2 {
    fn from(pec: PartialEncodedChunk) -> Self {
        match pec {
            PartialEncodedChunk::V1(chunk) => PartialEncodedChunkV2 {
                header: ShardChunkHeader::V1(chunk.header),
                parts: chunk.parts,
                receipts: chunk.receipts,
            },
            PartialEncodedChunk::V2(chunk) => chunk,
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct PartialEncodedChunkV1 {
    pub header: ShardChunkHeaderV1,
    pub parts: Vec<PartialEncodedChunkPart>,
    pub receipts: Vec<ReceiptProof>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartialEncodedChunkWithArcReceipts {
    pub header: ShardChunkHeader,
    pub parts: Vec<PartialEncodedChunkPart>,
    pub receipts: Vec<Arc<ReceiptProof>>,
}

impl From<PartialEncodedChunkWithArcReceipts> for PartialEncodedChunk {
    fn from(pec: PartialEncodedChunkWithArcReceipts) -> Self {
        Self::V2(PartialEncodedChunkV2 {
            header: pec.header,
            parts: pec.parts,
            receipts: pec.receipts.into_iter().map(|r| ReceiptProof::clone(&r)).collect(),
        })
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct ShardProof {
    pub from_shard_id: ShardId,
    pub to_shard_id: ShardId,
    pub proof: MerklePath,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
/// For each Merkle proof there is a subset of receipts which may be proven.
pub struct ReceiptProof(pub Vec<Receipt>, pub ShardProof);

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct PartialEncodedChunkPart {
    pub part_ord: u64,
    pub part: Box<[u8]>,
    pub merkle_proof: MerklePath,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct ShardChunkV1 {
    pub chunk_hash: ChunkHash,
    pub header: ShardChunkHeaderV1,
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<Receipt>,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct ShardChunkV2 {
    pub chunk_hash: ChunkHash,
    pub header: ShardChunkHeader,
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<Receipt>,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub enum ShardChunk {
    V1(ShardChunkV1),
    V2(ShardChunkV2),
}

impl ShardChunk {
    pub fn with_header(chunk: ShardChunk, header: ShardChunkHeader) -> Option<ShardChunk> {
        match chunk {
            Self::V1(chunk) => match header {
                ShardChunkHeader::V1(header) => Some(ShardChunk::V1(ShardChunkV1 {
                    chunk_hash: header.chunk_hash(),
                    header,
                    transactions: chunk.transactions,
                    receipts: chunk.receipts,
                })),
                ShardChunkHeader::V2(_) => None,
                #[cfg(feature = "protocol_feature_block_header_v3")]
                ShardChunkHeader::V3(_) => None,
            },
            Self::V2(chunk) => Some(ShardChunk::V2(ShardChunkV2 {
                chunk_hash: header.chunk_hash(),
                header,
                transactions: chunk.transactions,
                receipts: chunk.receipts,
            })),
        }
    }

    pub fn set_height_included(&mut self, height: BlockHeight) {
        match self {
            Self::V1(chunk) => chunk.header.height_included = height,
            Self::V2(chunk) => *chunk.header.height_included_mut() = height,
        }
    }

    #[inline]
    pub fn height_included(&self) -> BlockHeight {
        match self {
            Self::V1(chunk) => chunk.header.height_included,
            Self::V2(chunk) => chunk.header.height_included(),
        }
    }

    #[inline]
    pub fn height_created(&self) -> BlockHeight {
        match self {
            Self::V1(chunk) => chunk.header.inner.height_created,
            Self::V2(chunk) => chunk.header.height_created(),
        }
    }

    #[inline]
    pub fn prev_state_root(&self) -> StateRoot {
        match self {
            Self::V1(chunk) => chunk.header.inner.prev_state_root,
            Self::V2(chunk) => chunk.header.prev_state_root(),
        }
    }

    #[inline]
    pub fn tx_root(&self) -> CryptoHash {
        match self {
            Self::V1(chunk) => chunk.header.inner.tx_root,
            Self::V2(chunk) => chunk.header.tx_root(),
        }
    }

    #[inline]
    pub fn outgoing_receipts_root(&self) -> CryptoHash {
        match self {
            Self::V1(chunk) => chunk.header.inner.outgoing_receipts_root,
            Self::V2(chunk) => chunk.header.outgoing_receipts_root(),
        }
    }

    #[inline]
    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::V1(chunk) => chunk.header.inner.shard_id,
            Self::V2(chunk) => chunk.header.shard_id(),
        }
    }

    #[inline]
    pub fn chunk_hash(&self) -> ChunkHash {
        match self {
            Self::V1(chunk) => chunk.chunk_hash.clone(),
            Self::V2(chunk) => chunk.chunk_hash.clone(),
        }
    }

    #[inline]
    pub fn receipts(&self) -> &Vec<Receipt> {
        match self {
            Self::V1(chunk) => &chunk.receipts,
            Self::V2(chunk) => &chunk.receipts,
        }
    }

    #[inline]
    pub fn transactions(&self) -> &Vec<SignedTransaction> {
        match self {
            Self::V1(chunk) => &chunk.transactions,
            Self::V2(chunk) => &chunk.transactions,
        }
    }

    #[inline]
    pub fn take_header(self) -> ShardChunkHeader {
        match self {
            Self::V1(chunk) => ShardChunkHeader::V1(chunk.header),
            Self::V2(chunk) => chunk.header,
        }
    }

    pub fn cloned_header(&self) -> ShardChunkHeader {
        match self {
            Self::V1(chunk) => ShardChunkHeader::V1(chunk.header.clone()),
            Self::V2(chunk) => chunk.header.clone(),
        }
    }
}

#[derive(Default, BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct EncodedShardChunkBody {
    pub parts: Vec<Option<Box<[u8]>>>,
}

impl EncodedShardChunkBody {
    pub fn num_fetched_parts(&self) -> usize {
        let mut fetched_parts: usize = 0;

        for part in self.parts.iter() {
            if part.is_some() {
                fetched_parts += 1;
            }
        }

        fetched_parts
    }

    /// Returns true if reconstruction was successful
    pub fn reconstruct(
        &mut self,
        rs: &mut ReedSolomonWrapper,
    ) -> Result<(), reed_solomon_erasure::Error> {
        rs.reconstruct(self.parts.as_mut_slice())
    }

    pub fn get_merkle_hash_and_paths(&self) -> (MerkleHash, Vec<MerklePath>) {
        merklize(&self.parts.iter().map(|x| x.as_ref().unwrap().clone()).collect::<Vec<_>>())
    }
}

#[derive(BorshSerialize, Serialize, Debug, Clone)]
pub struct ReceiptList<'a>(pub ShardId, pub &'a Vec<Receipt>);

#[derive(BorshSerialize, BorshDeserialize, Serialize)]
struct TransactionReceipt(Vec<SignedTransaction>, Vec<Receipt>);

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct EncodedShardChunkV1 {
    pub header: ShardChunkHeaderV1,
    pub content: EncodedShardChunkBody,
}

impl EncodedShardChunkV1 {
    pub fn chunk_hash(&self) -> ChunkHash {
        self.header.chunk_hash()
    }

    pub fn decode_chunk(&self, data_parts: usize) -> Result<ShardChunkV1, std::io::Error> {
        let transaction_receipts = EncodedShardChunk::create_transaction_receipts(
            &self.content.parts[0..data_parts],
            self.header.inner.encoded_length,
        )?;

        Ok(ShardChunkV1 {
            chunk_hash: self.header.chunk_hash(),
            header: self.header.clone(),
            transactions: transaction_receipts.0,
            receipts: transaction_receipts.1,
        })
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct EncodedShardChunkV2 {
    pub header: ShardChunkHeader,
    pub content: EncodedShardChunkBody,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum EncodedShardChunk {
    V1(EncodedShardChunkV1),
    V2(EncodedShardChunkV2),
}

impl EncodedShardChunk {
    pub fn cloned_header(&self) -> ShardChunkHeader {
        match self {
            Self::V1(chunk) => ShardChunkHeader::V1(chunk.header.clone()),
            Self::V2(chunk) => chunk.header.clone(),
        }
    }

    #[inline]
    pub fn content(&self) -> &EncodedShardChunkBody {
        match self {
            Self::V1(chunk) => &chunk.content,
            Self::V2(chunk) => &chunk.content,
        }
    }

    #[inline]
    pub fn content_mut(&mut self) -> &mut EncodedShardChunkBody {
        match self {
            Self::V1(chunk) => &mut chunk.content,
            Self::V2(chunk) => &mut chunk.content,
        }
    }

    #[inline]
    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::V1(chunk) => chunk.header.inner.shard_id,
            Self::V2(chunk) => chunk.header.shard_id(),
        }
    }

    #[inline]
    pub fn encoded_merkle_root(&self) -> CryptoHash {
        match self {
            Self::V1(chunk) => chunk.header.inner.encoded_merkle_root,
            Self::V2(chunk) => chunk.header.encoded_merkle_root(),
        }
    }

    #[inline]
    pub fn encoded_length(&self) -> u64 {
        match self {
            Self::V1(chunk) => chunk.header.inner.encoded_length,
            Self::V2(chunk) => chunk.header.encoded_length(),
        }
    }

    pub fn from_header(
        header: ShardChunkHeader,
        total_parts: usize,
        protocol_version: ProtocolVersion,
    ) -> Self {
        if protocol_version < SHARD_CHUNK_HEADER_UPGRADE_VERSION {
            if let ShardChunkHeader::V1(header) = header {
                let chunk = EncodedShardChunkV1 {
                    header,
                    content: EncodedShardChunkBody { parts: vec![None; total_parts] },
                };
                Self::V1(chunk)
            } else {
                panic!("Attempted to include ShardChunkHeader::V2 in old protocol version");
            }
        } else {
            let chunk = EncodedShardChunkV2 {
                header,
                content: EncodedShardChunkBody { parts: vec![None; total_parts] },
            };
            Self::V2(chunk)
        }
    }

    pub fn new(
        prev_block_hash: CryptoHash,
        prev_state_root: StateRoot,
        outcome_root: CryptoHash,
        height: BlockHeight,
        shard_id: ShardId,
        rs: &mut ReedSolomonWrapper,
        gas_used: Gas,
        gas_limit: Gas,
        balance_burnt: Balance,

        tx_root: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        transactions: Vec<SignedTransaction>,
        outgoing_receipts: &Vec<Receipt>,
        outgoing_receipts_root: CryptoHash,
        signer: &dyn ValidatorSigner,
        protocol_version: ProtocolVersion,
    ) -> Result<(Self, Vec<MerklePath>), std::io::Error> {
        let mut bytes = TransactionReceipt(transactions, outgoing_receipts.clone()).try_to_vec()?;

        let mut parts = vec![];
        let data_parts = rs.data_shard_count();
        let total_parts = rs.total_shard_count();
        let encoded_length = bytes.len();

        if bytes.len() % data_parts != 0 {
            bytes.extend((bytes.len() % data_parts..data_parts).map(|_| 0));
        }
        let shard_length = (encoded_length + data_parts - 1) / data_parts;
        assert_eq!(bytes.len(), shard_length * data_parts);

        for i in 0..data_parts {
            parts.push(Some(
                bytes[i * shard_length..(i + 1) * shard_length].to_vec().into_boxed_slice()
                    as Box<[u8]>,
            ));
        }
        for _ in data_parts..total_parts {
            parts.push(None);
        }

        let (new_chunk, merkle_paths) = Self::from_parts_and_metadata(
            prev_block_hash,
            prev_state_root,
            outcome_root,
            height,
            shard_id,
            gas_used,
            gas_limit,
            balance_burnt,
            outgoing_receipts_root,
            tx_root,
            validator_proposals,
            encoded_length as u64,
            parts,
            rs,
            signer,
            protocol_version,
        );
        Ok((new_chunk, merkle_paths))
    }

    pub fn from_parts_and_metadata(
        prev_block_hash: CryptoHash,
        prev_state_root: StateRoot,
        outcome_root: CryptoHash,
        height: BlockHeight,
        shard_id: ShardId,
        gas_used: Gas,
        gas_limit: Gas,
        balance_burnt: Balance,
        outgoing_receipts_root: CryptoHash,
        tx_root: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,

        encoded_length: u64,
        parts: Vec<Option<Box<[u8]>>>,

        rs: &mut ReedSolomonWrapper,

        signer: &dyn ValidatorSigner,
        protocol_version: ProtocolVersion,
    ) -> (Self, Vec<MerklePath>) {
        let mut content = EncodedShardChunkBody { parts };
        content.reconstruct(rs).unwrap();
        let (encoded_merkle_root, merkle_paths) = content.get_merkle_hash_and_paths();

        #[cfg(not(feature = "protocol_feature_block_header_v3"))]
        let block_header_v3_version = None;
        #[cfg(feature = "protocol_feature_block_header_v3")]
        let block_header_v3_version = Some(ProtocolFeature::BlockHeaderV3.protocol_version());

        if protocol_version < SHARD_CHUNK_HEADER_UPGRADE_VERSION {
            #[cfg(feature = "protocol_feature_block_header_v3")]
            let validator_proposals =
                validator_proposals.into_iter().map(|v| v.into_v1()).collect();
            let header = ShardChunkHeaderV1::new(
                prev_block_hash,
                prev_state_root,
                outcome_root,
                encoded_merkle_root,
                encoded_length,
                height,
                shard_id,
                gas_used,
                gas_limit,
                balance_burnt,
                outgoing_receipts_root,
                tx_root,
                validator_proposals,
                signer,
            );
            let chunk = EncodedShardChunkV1 { header, content };
            (Self::V1(chunk), merkle_paths)
        } else if block_header_v3_version.is_none()
            || protocol_version < block_header_v3_version.unwrap()
        {
            #[cfg(feature = "protocol_feature_block_header_v3")]
            let validator_proposals =
                validator_proposals.into_iter().map(|v| v.into_v1()).collect();
            let header = ShardChunkHeaderV2::new(
                prev_block_hash,
                prev_state_root,
                outcome_root,
                encoded_merkle_root,
                encoded_length,
                height,
                shard_id,
                gas_used,
                gas_limit,
                balance_burnt,
                outgoing_receipts_root,
                tx_root,
                validator_proposals,
                signer,
            );
            let chunk = EncodedShardChunkV2 { header: ShardChunkHeader::V2(header), content };
            (Self::V2(chunk), merkle_paths)
        } else {
            #[cfg(not(feature = "protocol_feature_block_header_v3"))]
            unreachable!();
            #[cfg(feature = "protocol_feature_block_header_v3")]
            {
                let header = ShardChunkHeaderV3::new(
                    prev_block_hash,
                    prev_state_root,
                    outcome_root,
                    encoded_merkle_root,
                    encoded_length,
                    height,
                    shard_id,
                    gas_used,
                    gas_limit,
                    balance_burnt,
                    outgoing_receipts_root,
                    tx_root,
                    validator_proposals,
                    signer,
                );
                let chunk = EncodedShardChunkV2 { header: ShardChunkHeader::V3(header), content };
                (Self::V2(chunk), merkle_paths)
            }
        }
    }

    pub fn chunk_hash(&self) -> ChunkHash {
        match self {
            Self::V1(chunk) => chunk.header.chunk_hash(),
            Self::V2(chunk) => chunk.header.chunk_hash(),
        }
    }

    fn part_ords_to_parts(
        &self,
        part_ords: Vec<u64>,
        merkle_paths: &[MerklePath],
    ) -> Vec<PartialEncodedChunkPart> {
        let parts = match self {
            Self::V1(chunk) => &chunk.content.parts,
            Self::V2(chunk) => &chunk.content.parts,
        };
        part_ords
            .into_iter()
            .map(|part_ord| PartialEncodedChunkPart {
                part_ord,
                part: parts[part_ord as usize].clone().unwrap(),
                merkle_proof: merkle_paths[part_ord as usize].clone(),
            })
            .collect()
    }

    pub fn create_partial_encoded_chunk(
        &self,
        part_ords: Vec<u64>,
        receipts: Vec<ReceiptProof>,
        merkle_paths: &[MerklePath],
    ) -> PartialEncodedChunk {
        let parts = self.part_ords_to_parts(part_ords, merkle_paths);
        match self {
            Self::V1(chunk) => {
                let chunk = PartialEncodedChunkV1 { header: chunk.header.clone(), parts, receipts };
                PartialEncodedChunk::V1(chunk)
            }
            Self::V2(chunk) => {
                let chunk = PartialEncodedChunkV2 { header: chunk.header.clone(), parts, receipts };
                PartialEncodedChunk::V2(chunk)
            }
        }
    }

    pub fn create_partial_encoded_chunk_with_arc_receipts(
        &self,
        part_ords: Vec<u64>,
        receipts: Vec<Arc<ReceiptProof>>,
        merkle_paths: &[MerklePath],
    ) -> PartialEncodedChunkWithArcReceipts {
        let parts = self.part_ords_to_parts(part_ords, merkle_paths);
        let header = match self {
            Self::V1(chunk) => ShardChunkHeader::V1(chunk.header.clone()),
            Self::V2(chunk) => chunk.header.clone(),
        };
        PartialEncodedChunkWithArcReceipts { header, parts, receipts }
    }

    fn create_transaction_receipts(
        parts: &[Option<Box<[u8]>>],
        encoded_length: u64,
    ) -> Result<TransactionReceipt, std::io::Error> {
        let encoded_data = parts
            .iter()
            .flat_map(|option| option.as_ref().expect("Missing shard").iter())
            .cloned()
            .take(encoded_length as usize)
            .collect::<Vec<u8>>();

        TransactionReceipt::try_from_slice(&encoded_data)
    }

    pub fn decode_chunk(&self, data_parts: usize) -> Result<ShardChunk, std::io::Error> {
        let parts = match self {
            Self::V1(chunk) => &chunk.content.parts[0..data_parts],
            Self::V2(chunk) => &chunk.content.parts[0..data_parts],
        };
        let encoded_length = match self {
            Self::V1(chunk) => chunk.header.inner.encoded_length,
            Self::V2(chunk) => chunk.header.encoded_length(),
        };

        let transaction_receipts = Self::create_transaction_receipts(parts, encoded_length)?;

        match self {
            Self::V1(chunk) => Ok(ShardChunk::V1(ShardChunkV1 {
                chunk_hash: chunk.header.chunk_hash(),
                header: chunk.header.clone(),
                transactions: transaction_receipts.0,
                receipts: transaction_receipts.1,
            })),

            Self::V2(chunk) => Ok(ShardChunk::V2(ShardChunkV2 {
                chunk_hash: chunk.header.chunk_hash(),
                header: chunk.header.clone(),
                transactions: transaction_receipts.0,
                receipts: transaction_receipts.1,
            })),
        }
    }
}

/// The ttl for a reed solomon instance to control memory usage. This number below corresponds to
/// roughly 60MB of memory usage.
const RS_TTL: u64 = 2 * 1024;

/// Wrapper around reed solomon which occasionally resets the underlying
/// reed solomon instead to work around the memory leak in reed solomon
/// implementation https://github.com/darrenldl/reed-solomon-erasure/issues/74.
pub struct ReedSolomonWrapper {
    rs: ReedSolomon,
    ttl: u64,
}

impl ReedSolomonWrapper {
    pub fn new(data_shards: usize, parity_shards: usize) -> Self {
        ReedSolomonWrapper {
            rs: ReedSolomon::new(data_shards, parity_shards).unwrap(),
            ttl: RS_TTL,
        }
    }

    pub fn reconstruct<T: ReconstructShard<Field>>(
        &mut self,
        slices: &mut [T],
    ) -> Result<(), reed_solomon_erasure::Error> {
        let res = self.rs.reconstruct(slices);
        self.ttl -= 1;
        if self.ttl == 0 {
            *self =
                ReedSolomonWrapper::new(self.rs.data_shard_count(), self.rs.parity_shard_count());
        }
        res
    }

    pub fn data_shard_count(&self) -> usize {
        self.rs.data_shard_count()
    }

    pub fn total_shard_count(&self) -> usize {
        self.rs.total_shard_count()
    }
}
