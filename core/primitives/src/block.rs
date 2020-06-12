use std::cmp::max;

use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use near_crypto::Signature;
use num_rational::Rational;
use primitive_types::U256;
use serde::Serialize;

pub use crate::block_header::*;
use crate::challenge::{Challenges, ChallengesResult};
use crate::hash::{hash, CryptoHash};
use crate::merkle::{merklize, verify_path, MerklePath};
use crate::sharding::{
    ChunkHashHeight, EncodedShardChunk, ReedSolomonWrapper, ShardChunk, ShardChunkHeader,
};
use crate::types::{Balance, BlockHeight, EpochId, Gas, NumShards, StateRoot};
use crate::utils::to_timestamp;
use crate::validator_signer::{EmptyValidatorSigner, ValidatorSigner};
use crate::version::ProtocolVersion;

#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct GenesisId {
    /// Chain Id
    pub chain_id: String,
    /// Hash of genesis block
    pub hash: CryptoHash,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockV1 {
    pub header: BlockHeader,
    pub chunks: Vec<ShardChunkHeader>,
    pub challenges: Challenges,

    // Data to confirm the correctness of randomness beacon output
    pub vrf_value: near_crypto::vrf::Value,
    pub vrf_proof: near_crypto::vrf::Proof,
}

/// Versioned Block data structure.
/// For each next version, document what are the changes between versions.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub enum Block {
    BlockV1(Box<BlockV1>),
}

pub fn genesis_chunks(
    state_roots: Vec<StateRoot>,
    num_shards: NumShards,
    initial_gas_limit: Gas,
    genesis_height: BlockHeight,
) -> Vec<ShardChunk> {
    assert!(state_roots.len() == 1 || state_roots.len() == (num_shards as usize));
    let mut rs = ReedSolomonWrapper::new(1, 2);

    (0..num_shards)
        .map(|i| {
            let (encoded_chunk, _) = EncodedShardChunk::new(
                CryptoHash::default(),
                state_roots[i as usize % state_roots.len()].clone(),
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
                &vec![],
                CryptoHash::default(),
                &EmptyValidatorSigner::default(),
            )
            .expect("Failed to decode genesis chunk");
            let mut chunk = encoded_chunk.decode_chunk(1).expect("Failed to decode genesis chunk");
            chunk.header.height_included = genesis_height;
            chunk
        })
        .collect()
}

impl Block {
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
        Block::BlockV1(Box::new(BlockV1 {
            header: BlockHeader::genesis(
                genesis_protocol_version,
                height,
                Block::compute_state_root(&chunks),
                Block::compute_chunk_receipts_root(&chunks),
                Block::compute_chunk_headers_root(&chunks).0,
                Block::compute_chunk_tx_root(&chunks),
                Block::compute_chunks_included(&chunks, 0),
                Block::compute_challenges_root(&challenges),
                timestamp,
                initial_gas_price,
                initial_total_supply,
                next_bp_hash,
            ),
            chunks,
            challenges,

            vrf_value: near_crypto::vrf::Value([0; 32]),
            vrf_proof: near_crypto::vrf::Proof([0; 64]),
        }))
    }

    /// Produces new block from header of previous block, current state root and set of transactions.
    pub fn produce(
        protocol_version: ProtocolVersion,
        prev: &BlockHeader,
        height: BlockHeight,
        chunks: Vec<ShardChunkHeader>,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        approvals: Vec<Option<Signature>>,
        gas_price_adjustment_rate: Rational,
        min_gas_price: Balance,
        max_gas_price: Balance,
        minted_amount: Option<Balance>,
        challenges_result: ChallengesResult,
        challenges: Challenges,
        signer: &dyn ValidatorSigner,
        next_bp_hash: CryptoHash,
        block_merkle_root: CryptoHash,
    ) -> Self {
        // Collect aggregate of validators and gas usage/limits from chunks.
        let mut validator_proposals = vec![];
        let mut gas_used = 0;
        // This computation of chunk_mask relies on the fact that chunks are ordered by shard_id.
        let mut chunk_mask = vec![];
        let mut balance_burnt = 0;
        let mut gas_limit = 0;
        for chunk in chunks.iter() {
            if chunk.height_included == height {
                validator_proposals.extend_from_slice(&chunk.inner.validator_proposals);
                gas_used += chunk.inner.gas_used;
                gas_limit += chunk.inner.gas_limit;
                balance_burnt += chunk.inner.balance_burnt;
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

        let now = to_timestamp(Utc::now());
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

        Block::BlockV1(Box::new(BlockV1 {
            header: BlockHeader::new(
                protocol_version,
                height,
                prev.hash().clone(),
                Block::compute_state_root(&chunks),
                Block::compute_chunk_receipts_root(&chunks),
                Block::compute_chunk_headers_root(&chunks).0,
                Block::compute_chunk_tx_root(&chunks),
                Block::compute_outcome_root(&chunks),
                time,
                Block::compute_chunks_included(&chunks, height),
                Block::compute_challenges_root(&challenges),
                random_value,
                validator_proposals,
                chunk_mask,
                epoch_id,
                next_epoch_id,
                new_gas_price,
                new_total_supply,
                challenges_result,
                signer,
                last_final_block.clone(),
                last_ds_final_block.clone(),
                approvals,
                next_bp_hash,
                block_merkle_root,
            ),
            chunks,
            challenges,

            vrf_value,
            vrf_proof,
        }))
    }

    pub fn verify_gas_price(
        &self,
        prev_gas_price: Balance,
        min_gas_price: Balance,
        max_gas_price: Balance,
        gas_price_adjustment_rate: Rational,
    ) -> bool {
        let gas_used = Self::compute_gas_used(self.chunks(), self.header().height());
        let gas_limit = Self::compute_gas_limit(self.chunks(), self.header().height());
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

    pub fn compute_new_gas_price(
        prev_gas_price: Balance,
        gas_used: Gas,
        gas_limit: Gas,
        gas_price_adjustment_rate: Rational,
        min_gas_price: Balance,
        max_gas_price: Balance,
    ) -> Balance {
        if gas_limit == 0 {
            prev_gas_price
        } else {
            let numerator = 2 * *gas_price_adjustment_rate.denom() as u128 * u128::from(gas_limit)
                - *gas_price_adjustment_rate.numer() as u128 * u128::from(gas_limit)
                + 2 * *gas_price_adjustment_rate.numer() as u128 * u128::from(gas_used);
            let denominator =
                2 * *gas_price_adjustment_rate.denom() as u128 * u128::from(gas_limit);
            let new_gas_price =
                U256::from(prev_gas_price) * U256::from(numerator) / U256::from(denominator);
            if new_gas_price > U256::from(max_gas_price) {
                max_gas_price
            } else {
                max(new_gas_price.as_u128(), min_gas_price)
            }
        }
    }

    pub fn compute_state_root(chunks: &[ShardChunkHeader]) -> CryptoHash {
        merklize(
            &chunks.iter().map(|chunk| chunk.inner.prev_state_root).collect::<Vec<CryptoHash>>(),
        )
        .0
    }

    pub fn compute_chunk_receipts_root(chunks: &[ShardChunkHeader]) -> CryptoHash {
        merklize(
            &chunks
                .iter()
                .map(|chunk| chunk.inner.outgoing_receipts_root)
                .collect::<Vec<CryptoHash>>(),
        )
        .0
    }

    pub fn compute_chunk_headers_root(
        chunks: &[ShardChunkHeader],
    ) -> (CryptoHash, Vec<MerklePath>) {
        merklize(
            &chunks
                .iter()
                .map(|chunk| ChunkHashHeight(chunk.hash.clone(), chunk.height_included))
                .collect::<Vec<ChunkHashHeight>>(),
        )
    }

    pub fn compute_chunk_tx_root(chunks: &[ShardChunkHeader]) -> CryptoHash {
        merklize(&chunks.iter().map(|chunk| chunk.inner.tx_root).collect::<Vec<CryptoHash>>()).0
    }

    pub fn compute_chunks_included(chunks: &[ShardChunkHeader], height: BlockHeight) -> u64 {
        chunks.iter().filter(|chunk| chunk.height_included == height).count() as u64
    }

    pub fn compute_outcome_root(chunks: &[ShardChunkHeader]) -> CryptoHash {
        merklize(&chunks.iter().map(|chunk| chunk.inner.outcome_root).collect::<Vec<CryptoHash>>())
            .0
    }

    pub fn compute_challenges_root(challenges: &Challenges) -> CryptoHash {
        merklize(&challenges.iter().map(|challenge| challenge.hash).collect::<Vec<CryptoHash>>()).0
    }

    pub fn compute_gas_used(chunks: &[ShardChunkHeader], height: BlockHeight) -> Gas {
        chunks.iter().fold(0, |acc, chunk| {
            if chunk.height_included == height {
                acc + chunk.inner.gas_used
            } else {
                acc
            }
        })
    }

    pub fn compute_gas_limit(chunks: &[ShardChunkHeader], height: BlockHeight) -> Gas {
        chunks.iter().fold(0, |acc, chunk| {
            if chunk.height_included == height {
                acc + chunk.inner.gas_limit
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
            &ChunkHashHeight(chunk.hash.clone(), chunk.height_included),
        )
    }

    pub fn header(&self) -> &BlockHeader {
        match self {
            Block::BlockV1(block) => &block.header,
        }
    }

    pub fn chunks(&self) -> &Vec<ShardChunkHeader> {
        match self {
            Block::BlockV1(block) => &block.chunks,
        }
    }

    pub fn challenges(&self) -> &Challenges {
        match self {
            Block::BlockV1(block) => &block.challenges,
        }
    }

    pub fn vrf_value(&self) -> &near_crypto::vrf::Value {
        match self {
            Block::BlockV1(block) => &block.vrf_value,
        }
    }

    pub fn vrf_proof(&self) -> &near_crypto::vrf::Proof {
        match self {
            Block::BlockV1(block) => &block.vrf_proof,
        }
    }

    pub fn hash(&self) -> &CryptoHash {
        self.header().hash()
    }

    pub fn check_validity(&self) -> bool {
        // Check that state root stored in the header matches the state root of the chunks
        let state_root = Block::compute_state_root(self.chunks());
        if self.header().prev_state_root() != &state_root {
            return false;
        }

        // Check that chunk receipts root stored in the header matches the state root of the chunks
        let chunk_receipts_root = Block::compute_chunk_receipts_root(self.chunks());
        if self.header().chunk_receipts_root() != &chunk_receipts_root {
            return false;
        }

        // Check that chunk headers root stored in the header matches the chunk headers root of the chunks
        let chunk_headers_root = Block::compute_chunk_headers_root(self.chunks()).0;
        if self.header().chunk_headers_root() != &chunk_headers_root {
            return false;
        }

        // Check that chunk tx root stored in the header matches the tx root of the chunks
        let chunk_tx_root = Block::compute_chunk_tx_root(self.chunks());
        if self.header().chunk_tx_root() != &chunk_tx_root {
            return false;
        }

        // Check that chunk included root stored in the header matches the chunk included root of the chunks
        let chunks_included_root =
            Block::compute_chunks_included(self.chunks(), self.header().height());
        if self.header().chunks_included() != chunks_included_root {
            return false;
        }

        // Check that challenges root stored in the header matches the challenges root of the challenges
        let challenges_root = Block::compute_challenges_root(&self.challenges());
        if self.header().challenges_root() != &challenges_root {
            return false;
        }

        true
    }
}

/// The tip of a fork. A handle to the fork ancestry from its leaf in the
/// blockchain tree. References the max height and the latest and previous
/// blocks for convenience
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq)]
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
            last_block_hash: header.hash().clone(),
            prev_block_hash: header.prev_hash().clone(),
            epoch_id: header.epoch_id().clone(),
            next_epoch_id: header.next_epoch_id().clone(),
        }
    }
}
