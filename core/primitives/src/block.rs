use std::sync::Arc;

use crate::static_clock::StaticClock;
use borsh::{BorshDeserialize, BorshSerialize};

use chrono::{DateTime, Utc};
use near_crypto::Signature;
use primitive_types::U256;

use crate::block::BlockValidityError::{
    InvalidChallengeRoot, InvalidChunkHeaderRoot, InvalidChunkMask, InvalidReceiptRoot,
    InvalidStateRoot, InvalidTransactionRoot,
};
pub use crate::block_header::*;
use crate::challenge::{Challenges, ChallengesResult};
use crate::checked_feature;
use crate::hash::{hash, CryptoHash};
use crate::merkle::{merklize, verify_path, MerklePath};
use crate::num_rational::Rational32;
use crate::sharding::{
    ChunkHashHeight, EncodedShardChunk, ReedSolomonWrapper, ShardChunk, ShardChunkHeader,
    ShardChunkHeaderV1,
};
use crate::types::{Balance, BlockHeight, EpochId, Gas, NumBlocks, NumShards, StateRoot};
use crate::utils::to_timestamp;
use crate::validator_signer::{EmptyValidatorSigner, ValidatorSigner};
use crate::version::{ProtocolVersion, SHARD_CHUNK_HEADER_UPGRADE_VERSION};
use std::ops::Index;

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct GenesisId {
    /// Chain Id
    pub chain_id: String,
    /// Hash of genesis block
    pub hash: CryptoHash,
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq)]
pub enum BlockValidityError {
    InvalidStateRoot,
    InvalidReceiptRoot,
    InvalidChunkHeaderRoot,
    InvalidTransactionRoot,
    InvalidChunkMask,
    InvalidChallengeRoot,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockV1 {
    pub header: BlockHeader,
    pub chunks: Vec<ShardChunkHeaderV1>,
    pub challenges: Challenges,

    // Data to confirm the correctness of randomness beacon output
    pub vrf_value: near_crypto::vrf::Value,
    pub vrf_proof: near_crypto::vrf::Proof,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockV2 {
    pub header: BlockHeader,
    pub chunks: Vec<ShardChunkHeader>,
    pub challenges: Challenges,

    // Data to confirm the correctness of randomness beacon output
    pub vrf_value: near_crypto::vrf::Value,
    pub vrf_proof: near_crypto::vrf::Proof,
}

/// V2 -> V3: added BlockBody
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockV3 {
    pub header: BlockHeader,
    pub body: BlockBody,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockBody {
    pub chunks: Vec<ShardChunkHeader>,
    pub challenges: Challenges,

    // Data to confirm the correctness of randomness beacon output
    pub vrf_value: near_crypto::vrf::Value,
    pub vrf_proof: near_crypto::vrf::Proof,
}

/// Versioned Block data structure.
/// For each next version, document what are the changes between versions.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub enum Block {
    BlockV1(Arc<BlockV1>),
    BlockV2(Arc<BlockV2>),
    BlockV3(Arc<BlockV3>),
}

pub fn genesis_chunks(
    state_roots: Vec<StateRoot>,
    num_shards: NumShards,
    initial_gas_limit: Gas,
    genesis_height: BlockHeight,
    genesis_protocol_version: ProtocolVersion,
) -> Vec<ShardChunk> {
    assert!(state_roots.len() == 1 || state_roots.len() == (num_shards as usize));
    let mut rs = ReedSolomonWrapper::new(1, 2);

    (0..num_shards)
        .map(|i| {
            let (encoded_chunk, _) = EncodedShardChunk::new(
                CryptoHash::default(),
                state_roots[i as usize % state_roots.len()],
                CryptoHash::default(),
                genesis_height,
                i,
                &mut rs,
                0,
                initial_gas_limit,
                0,
                CryptoHash::default(),
                vec![],
                vec![],
                &[],
                CryptoHash::default(),
                &EmptyValidatorSigner::default(),
                genesis_protocol_version,
            )
            .expect("Failed to decode genesis chunk");
            let mut chunk = encoded_chunk.decode_chunk(1).expect("Failed to decode genesis chunk");
            chunk.set_height_included(genesis_height);
            chunk
        })
        .collect()
}
impl Block {
    fn block_from_protocol_version(
        this_epoch_protocol_version: ProtocolVersion,
        next_epoch_protocol_version: ProtocolVersion,
        header: BlockHeader,
        body: BlockBody,
    ) -> Block {
        if next_epoch_protocol_version < SHARD_CHUNK_HEADER_UPGRADE_VERSION {
            let legacy_chunks = body
                .chunks
                .into_iter()
                .map(|chunk| match chunk {
                    ShardChunkHeader::V1(header) => header,
                    ShardChunkHeader::V2(_) => panic!(
                        "Attempted to include VersionedShardChunkHeaderV2 in old protocol version"
                    ),
                    ShardChunkHeader::V3(_) => panic!(
                        "Attempted to include VersionedShardChunkHeaderV3 in old protocol version"
                    ),
                })
                .collect();

            Block::BlockV1(Arc::new(BlockV1 {
                header,
                chunks: legacy_chunks,
                challenges: body.challenges,
                vrf_value: body.vrf_value,
                vrf_proof: body.vrf_proof,
            }))
        } else if checked_feature!(
            "protocol_feature_block_header_v4",
            BlockHeaderV4,
            this_epoch_protocol_version
        ) {
            Block::BlockV3(Arc::new(BlockV3 { header, body }))
        } else {
            Block::BlockV2(Arc::new(BlockV2 {
                header,
                chunks: body.chunks,
                challenges: body.challenges,
                vrf_value: body.vrf_value,
                vrf_proof: body.vrf_proof,
            }))
        }
    }

    /// Returns genesis block for given genesis date and state root.
    pub fn genesis(
        genesis_protocol_version: ProtocolVersion,
        chunks: Vec<ShardChunkHeader>,
        timestamp: DateTime<Utc>,
        height: BlockHeight,
        initial_gas_price: Balance,
        initial_total_supply: Balance,
        next_bp_hash: CryptoHash,
    ) -> Self {
        let challenges = vec![];
        for chunk in &chunks {
            assert_eq!(chunk.height_included(), height);
        }
        let vrf_value = near_crypto::vrf::Value([0; 32]);
        let vrf_proof = near_crypto::vrf::Proof([0; 64]);
        let body = BlockBody { chunks, challenges, vrf_value, vrf_proof };
        let header = BlockHeader::genesis(
            genesis_protocol_version,
            height,
            Block::compute_state_root(&body.chunks),
            #[cfg(feature = "protocol_feature_block_header_v4")]
            Block::compute_block_body_hash_impl(&body),
            Block::compute_chunk_receipts_root(&body.chunks),
            Block::compute_chunk_headers_root(&body.chunks).0,
            Block::compute_chunk_tx_root(&body.chunks),
            body.chunks.len() as u64,
            Block::compute_challenges_root(&body.challenges),
            timestamp,
            initial_gas_price,
            initial_total_supply,
            next_bp_hash,
        );

        Self::block_from_protocol_version(
            genesis_protocol_version,
            genesis_protocol_version,
            header,
            body,
        )
    }

    /// Produces new block from header of previous block, current state root and set of transactions.
    pub fn produce(
        this_epoch_protocol_version: ProtocolVersion,
        next_epoch_protocol_version: ProtocolVersion,
        prev: &BlockHeader,
        height: BlockHeight,
        block_ordinal: NumBlocks,
        chunks: Vec<ShardChunkHeader>,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        epoch_sync_data_hash: Option<CryptoHash>,
        approvals: Vec<Option<Signature>>,
        gas_price_adjustment_rate: Rational32,
        min_gas_price: Balance,
        max_gas_price: Balance,
        minted_amount: Option<Balance>,
        challenges_result: ChallengesResult,
        challenges: Challenges,
        signer: &dyn ValidatorSigner,
        next_bp_hash: CryptoHash,
        block_merkle_root: CryptoHash,
        timestamp_override: Option<DateTime<chrono::Utc>>,
    ) -> Self {
        // Collect aggregate of validators and gas usage/limits from chunks.
        let mut validator_proposals = vec![];
        let mut gas_used = 0;
        // This computation of chunk_mask relies on the fact that chunks are ordered by shard_id.
        let mut chunk_mask = vec![];
        let mut balance_burnt = 0;
        let mut gas_limit = 0;
        for chunk in chunks.iter() {
            if chunk.height_included() == height {
                validator_proposals.extend(chunk.validator_proposals());
                gas_used += chunk.gas_used();
                gas_limit += chunk.gas_limit();
                balance_burnt += chunk.balance_burnt();
                chunk_mask.push(true);
            } else {
                chunk_mask.push(false);
            }
        }
        let new_gas_price = Self::compute_new_gas_price(
            prev.gas_price(),
            gas_used,
            gas_limit,
            gas_price_adjustment_rate,
            min_gas_price,
            max_gas_price,
        );

        let new_total_supply = prev.total_supply() + minted_amount.unwrap_or(0) - balance_burnt;
        let now = to_timestamp(timestamp_override.unwrap_or_else(StaticClock::utc));
        let time = if now <= prev.raw_timestamp() { prev.raw_timestamp() + 1 } else { now };

        let (vrf_value, vrf_proof) = signer.compute_vrf_with_proof(prev.random_value().as_ref());
        let random_value = hash(vrf_value.0.as_ref());

        let last_ds_final_block =
            if height == prev.height() + 1 { prev.hash() } else { prev.last_ds_final_block() };

        let last_final_block =
            if height == prev.height() + 1 && prev.last_ds_final_block() == prev.prev_hash() {
                prev.prev_hash()
            } else {
                prev.last_final_block()
            };

        match prev {
            BlockHeader::BlockHeaderV1(_) => debug_assert_eq!(prev.block_ordinal(), 0),
            BlockHeader::BlockHeaderV2(_) => debug_assert_eq!(prev.block_ordinal(), 0),
            BlockHeader::BlockHeaderV3(_) => {
                debug_assert_eq!(prev.block_ordinal() + 1, block_ordinal)
            }
            BlockHeader::BlockHeaderV4(_) => {
                debug_assert_eq!(prev.block_ordinal() + 1, block_ordinal)
            }
        };

        let body = BlockBody { chunks, challenges, vrf_value, vrf_proof };
        let header = BlockHeader::new(
            this_epoch_protocol_version,
            next_epoch_protocol_version,
            height,
            *prev.hash(),
            #[cfg(feature = "protocol_feature_block_header_v4")]
            Block::compute_block_body_hash_impl(&body),
            Block::compute_state_root(&body.chunks),
            Block::compute_chunk_receipts_root(&body.chunks),
            Block::compute_chunk_headers_root(&body.chunks).0,
            Block::compute_chunk_tx_root(&body.chunks),
            Block::compute_outcome_root(&body.chunks),
            time,
            Block::compute_challenges_root(&body.challenges),
            random_value,
            validator_proposals,
            chunk_mask,
            block_ordinal,
            epoch_id,
            next_epoch_id,
            new_gas_price,
            new_total_supply,
            challenges_result,
            signer,
            *last_final_block,
            *last_ds_final_block,
            epoch_sync_data_hash,
            approvals,
            next_bp_hash,
            block_merkle_root,
            prev.height(),
        );

        Self::block_from_protocol_version(
            this_epoch_protocol_version,
            next_epoch_protocol_version,
            header,
            body,
        )
    }

    pub fn verify_total_supply(
        &self,
        prev_total_supply: Balance,
        minted_amount: Option<Balance>,
    ) -> bool {
        let mut balance_burnt = 0;

        for chunk in self.chunks().iter() {
            if chunk.height_included() == self.header().height() {
                balance_burnt += chunk.balance_burnt();
            }
        }

        let new_total_supply = prev_total_supply + minted_amount.unwrap_or(0) - balance_burnt;
        self.header().total_supply() == new_total_supply
    }

    pub fn verify_gas_price(
        &self,
        prev_gas_price: Balance,
        min_gas_price: Balance,
        max_gas_price: Balance,
        gas_price_adjustment_rate: Rational32,
    ) -> bool {
        let gas_used = Self::compute_gas_used(self.chunks().iter(), self.header().height());
        let gas_limit = Self::compute_gas_limit(self.chunks().iter(), self.header().height());
        let expected_price = Self::compute_new_gas_price(
            prev_gas_price,
            gas_used,
            gas_limit,
            gas_price_adjustment_rate,
            min_gas_price,
            max_gas_price,
        );
        self.header().gas_price() == expected_price
    }

    /// Computes the new gas price according to the formula:
    ///   gas_price = prev_gas_price * (1 + (gas_used/gas_limit - 1/2) * adjustment_rate)
    /// and clamped between min_gas_price and max_gas_price.
    pub fn compute_new_gas_price(
        prev_gas_price: Balance,
        gas_used: Gas,
        gas_limit: Gas,
        gas_price_adjustment_rate: Rational32,
        min_gas_price: Balance,
        max_gas_price: Balance,
    ) -> Balance {
        // If block was skipped, the price does not change.
        if gas_limit == 0 {
            return prev_gas_price;
        }

        let gas_used = u128::from(gas_used);
        let gas_limit = u128::from(gas_limit);
        let adjustment_rate_numer = *gas_price_adjustment_rate.numer() as u128;
        let adjustment_rate_denom = *gas_price_adjustment_rate.denom() as u128;

        // This number can never be negative as long as gas_used <= gas_limit and
        // adjustment_rate_numer <= adjustment_rate_denom.
        let numerator = 2 * adjustment_rate_denom * gas_limit
            + 2 * adjustment_rate_numer * gas_used
            - adjustment_rate_numer * gas_limit;
        let denominator = 2 * adjustment_rate_denom * gas_limit;
        let new_gas_price =
            U256::from(prev_gas_price) * U256::from(numerator) / U256::from(denominator);

        new_gas_price.clamp(U256::from(min_gas_price), U256::from(max_gas_price)).as_u128()
    }

    pub fn compute_state_root<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
    ) -> CryptoHash {
        merklize(
            &chunks.into_iter().map(|chunk| chunk.prev_state_root()).collect::<Vec<CryptoHash>>(),
        )
        .0
    }

    pub fn compute_block_body_hash_impl(body: &BlockBody) -> CryptoHash {
        CryptoHash::hash_borsh(body)
    }

    pub fn compute_chunk_receipts_root<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
    ) -> CryptoHash {
        merklize(
            &chunks
                .into_iter()
                .map(|chunk| chunk.outgoing_receipts_root())
                .collect::<Vec<CryptoHash>>(),
        )
        .0
    }

    pub fn compute_chunk_headers_root<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
    ) -> (CryptoHash, Vec<MerklePath>) {
        merklize(
            &chunks
                .into_iter()
                .map(|chunk| ChunkHashHeight(chunk.chunk_hash(), chunk.height_included()))
                .collect::<Vec<ChunkHashHeight>>(),
        )
    }

    pub fn compute_chunk_tx_root<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
    ) -> CryptoHash {
        merklize(&chunks.into_iter().map(|chunk| chunk.tx_root()).collect::<Vec<CryptoHash>>()).0
    }

    pub fn compute_outcome_root<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
    ) -> CryptoHash {
        merklize(&chunks.into_iter().map(|chunk| chunk.outcome_root()).collect::<Vec<CryptoHash>>())
            .0
    }

    pub fn compute_challenges_root(challenges: &Challenges) -> CryptoHash {
        merklize(&challenges.iter().map(|challenge| challenge.hash).collect::<Vec<CryptoHash>>()).0
    }

    pub fn compute_gas_used<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
        height: BlockHeight,
    ) -> Gas {
        chunks.into_iter().fold(0, |acc, chunk| {
            if chunk.height_included() == height {
                acc + chunk.gas_used()
            } else {
                acc
            }
        })
    }

    pub fn compute_gas_limit<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
        height: BlockHeight,
    ) -> Gas {
        chunks.into_iter().fold(0, |acc, chunk| {
            if chunk.height_included() == height {
                acc + chunk.gas_limit()
            } else {
                acc
            }
        })
    }

    pub fn validate_chunk_header_proof(
        chunk: &ShardChunkHeader,
        chunk_root: &CryptoHash,
        merkle_path: &MerklePath,
    ) -> bool {
        verify_path(
            *chunk_root,
            merkle_path,
            &ChunkHashHeight(chunk.chunk_hash(), chunk.height_included()),
        )
    }

    #[inline]
    pub fn header(&self) -> &BlockHeader {
        match self {
            Block::BlockV1(block) => &block.header,
            Block::BlockV2(block) => &block.header,
            Block::BlockV3(block) => &block.header,
        }
    }

    pub fn chunks(&self) -> ChunksCollection {
        match self {
            Block::BlockV1(block) => ChunksCollection::V1(
                block.chunks.iter().map(|h| ShardChunkHeader::V1(h.clone())).collect(),
            ),
            Block::BlockV2(block) => ChunksCollection::V2(&block.chunks),
            Block::BlockV3(block) => ChunksCollection::V2(&block.body.chunks),
        }
    }

    #[inline]
    pub fn challenges(&self) -> &Challenges {
        match self {
            Block::BlockV1(block) => &block.challenges,
            Block::BlockV2(block) => &block.challenges,
            Block::BlockV3(block) => &block.body.challenges,
        }
    }

    #[inline]
    pub fn vrf_value(&self) -> &near_crypto::vrf::Value {
        match self {
            Block::BlockV1(block) => &block.vrf_value,
            Block::BlockV2(block) => &block.vrf_value,
            Block::BlockV3(block) => &block.body.vrf_value,
        }
    }

    #[inline]
    pub fn vrf_proof(&self) -> &near_crypto::vrf::Proof {
        match self {
            Block::BlockV1(block) => &block.vrf_proof,
            Block::BlockV2(block) => &block.vrf_proof,
            Block::BlockV3(block) => &block.body.vrf_proof,
        }
    }

    pub fn hash(&self) -> &CryptoHash {
        self.header().hash()
    }

    pub fn compute_block_body_hash(&self) -> Option<CryptoHash> {
        match self {
            Block::BlockV1(_) => None,
            Block::BlockV2(_) => None,
            Block::BlockV3(block) => Some(Self::compute_block_body_hash_impl(&block.body)),
        }
    }

    /// Checks that block content matches block hash, with the possible exception of chunk signatures
    pub fn check_validity(&self) -> Result<(), BlockValidityError> {
        // Check that state root stored in the header matches the state root of the chunks
        let state_root = Block::compute_state_root(self.chunks().iter());
        if self.header().prev_state_root() != &state_root {
            return Err(InvalidStateRoot);
        }

        // Check that chunk receipts root stored in the header matches the state root of the chunks
        let chunk_receipts_root = Block::compute_chunk_receipts_root(self.chunks().iter());
        if self.header().chunk_receipts_root() != &chunk_receipts_root {
            return Err(InvalidReceiptRoot);
        }

        // Check that chunk headers root stored in the header matches the chunk headers root of the chunks
        let chunk_headers_root = Block::compute_chunk_headers_root(self.chunks().iter()).0;
        if self.header().chunk_headers_root() != &chunk_headers_root {
            return Err(InvalidChunkHeaderRoot);
        }

        // Check that chunk tx root stored in the header matches the tx root of the chunks
        let chunk_tx_root = Block::compute_chunk_tx_root(self.chunks().iter());
        if self.header().chunk_tx_root() != &chunk_tx_root {
            return Err(InvalidTransactionRoot);
        }

        let outcome_root = Block::compute_outcome_root(self.chunks().iter());
        if self.header().outcome_root() != &outcome_root {
            return Err(InvalidTransactionRoot);
        }

        // Check that chunk included root stored in the header matches the chunk included root of the chunks
        let chunk_mask: Vec<bool> = self
            .chunks()
            .iter()
            .map(|chunk| chunk.height_included() == self.header().height())
            .collect();
        if self.header().chunk_mask() != &chunk_mask[..] {
            return Err(InvalidChunkMask);
        }

        // Check that challenges root stored in the header matches the challenges root of the challenges
        let challenges_root = Block::compute_challenges_root(self.challenges());
        if self.header().challenges_root() != &challenges_root {
            return Err(InvalidChallengeRoot);
        }

        Ok(())
    }
}

pub enum ChunksCollection<'a> {
    V1(Vec<ShardChunkHeader>),
    V2(&'a [ShardChunkHeader]),
}

pub struct VersionedChunksIter<'a> {
    chunks: &'a [ShardChunkHeader],
    curr_index: usize,
    len: usize,
}

impl<'a> VersionedChunksIter<'a> {
    fn new(chunks: &'a [ShardChunkHeader]) -> Self {
        Self { chunks, curr_index: 0, len: chunks.len() }
    }
}

impl<'a> Iterator for VersionedChunksIter<'a> {
    type Item = &'a ShardChunkHeader;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_index < self.len {
            let item = &self.chunks[self.curr_index];
            self.curr_index += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for VersionedChunksIter<'a> {
    fn len(&self) -> usize {
        self.len - self.curr_index
    }
}

impl<'a> Index<usize> for ChunksCollection<'a> {
    type Output = ShardChunkHeader;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            ChunksCollection::V1(chunks) => &chunks[index],
            ChunksCollection::V2(chunks) => &chunks[index],
        }
    }
}

impl<'a> ChunksCollection<'a> {
    pub fn len(&self) -> usize {
        match self {
            ChunksCollection::V1(chunks) => chunks.len(),
            ChunksCollection::V2(chunks) => chunks.len(),
        }
    }

    pub fn iter(&'a self) -> VersionedChunksIter<'a> {
        match self {
            ChunksCollection::V1(chunks) => VersionedChunksIter::new(chunks),
            ChunksCollection::V2(chunks) => VersionedChunksIter::new(chunks),
        }
    }

    pub fn get(&self, index: usize) -> Option<&ShardChunkHeader> {
        match self {
            ChunksCollection::V1(chunks) => chunks.get(index),
            ChunksCollection::V2(chunks) => chunks.get(index),
        }
    }
}

/// The tip of a fork. A handle to the fork ancestry from its leaf in the
/// blockchain tree. References the max height and the latest and previous
/// blocks for convenience
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq)]
pub struct Tip {
    /// Height of the tip (max height of the fork)
    pub height: BlockHeight,
    /// Last block pushed to the fork
    pub last_block_hash: CryptoHash,
    /// Previous block
    pub prev_block_hash: CryptoHash,
    /// Current epoch id. Used for getting validator info.
    pub epoch_id: EpochId,
    /// Next epoch id.
    pub next_epoch_id: EpochId,
}

impl Tip {
    /// Creates a new tip based on provided header.
    pub fn from_header(header: &BlockHeader) -> Tip {
        Tip {
            height: header.height(),
            last_block_hash: *header.hash(),
            prev_block_hash: *header.prev_hash(),
            epoch_id: header.epoch_id().clone(),
            next_epoch_id: header.next_epoch_id().clone(),
        }
    }
}
