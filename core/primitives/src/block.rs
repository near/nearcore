use std::cmp::max;

use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use serde::Serialize;

use near_crypto::{KeyType, PublicKey, Signature};

use crate::challenge::{Challenges, ChallengesResult};
use crate::hash::{hash, CryptoHash};
use crate::merkle::{combine_hash, merklize, verify_path, MerklePath};
use crate::sharding::{
    ChunkHashHeight, EncodedShardChunk, ReedSolomonWrapper, ShardChunk, ShardChunkHeader,
};
use crate::types::{
    AccountId, Balance, BlockHeight, EpochId, Gas, MerkleHash, NumShards, StateRoot, ValidatorStake,
};
use crate::utils::{from_timestamp, to_timestamp};
use crate::validator_signer::{EmptyValidatorSigner, ValidatorSigner};
use num_rational::Rational;

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeaderInnerLite {
    /// Height of this block since the genesis block (height 0).
    pub height: BlockHeight,
    /// Epoch start hash of this block's epoch.
    /// Used for retrieving validator information
    pub epoch_id: EpochId,
    pub next_epoch_id: EpochId,
    /// Root hash of the state at the previous block.
    pub prev_state_root: MerkleHash,
    /// Root of the outcomes of transactions and receipts.
    pub outcome_root: MerkleHash,
    /// Timestamp at which the block was built (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    pub timestamp: u64,
    /// Hash of the next epoch block producers set
    pub next_bp_hash: CryptoHash,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeaderInnerRest {
    /// Root hash of the chunk receipts in the given block.
    pub chunk_receipts_root: MerkleHash,
    /// Root hash of the chunk headers in the given block.
    pub chunk_headers_root: MerkleHash,
    /// Root hash of the chunk transactions in the given block.
    pub chunk_tx_root: MerkleHash,
    /// Number of chunks included into the block.
    pub chunks_included: u64,
    /// Root hash of the challenges in the given block.
    pub challenges_root: MerkleHash,
    /// The output of the randomness beacon
    pub random_value: CryptoHash,
    /// Validator proposals.
    pub validator_proposals: Vec<ValidatorStake>,
    /// Mask for new chunks included in the block
    pub chunk_mask: Vec<bool>,
    /// Gas price. Same for all chunks
    pub gas_price: Balance,
    /// Sum of all validator reward across all chunks.
    pub validator_reward: Balance,
    /// Total supply of tokens in the system
    pub total_supply: Balance,
    /// List of challenges result from previous block.
    pub challenges_result: ChallengesResult,

    /// Last block that has full BFT finality
    pub last_final_block: CryptoHash,
    /// Last block that has doomslug finality
    pub last_ds_final_block: CryptoHash,

    /// All the approvals included in this block
    pub approvals: Vec<Option<Signature>>,
}

impl BlockHeaderInnerLite {
    pub fn new(
        height: BlockHeight,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        prev_state_root: MerkleHash,
        outcome_root: MerkleHash,
        timestamp: u64,
        next_bp_hash: CryptoHash,
    ) -> Self {
        Self {
            height,
            epoch_id,
            next_epoch_id,
            prev_state_root,
            outcome_root,
            timestamp,
            next_bp_hash,
        }
    }

    pub fn hash(&self) -> CryptoHash {
        hash(&self.try_to_vec().expect("Failed to serialize"))
    }
}

impl BlockHeaderInnerRest {
    pub fn new(
        chunk_receipts_root: MerkleHash,
        chunk_headers_root: MerkleHash,
        chunk_tx_root: MerkleHash,
        chunks_included: u64,
        challenges_root: MerkleHash,
        random_value: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        chunk_mask: Vec<bool>,
        gas_price: Balance,
        validator_reward: Balance,
        total_supply: Balance,
        challenges_result: ChallengesResult,
        last_final_block: CryptoHash,
        last_ds_final_block: CryptoHash,
        approvals: Vec<Option<Signature>>,
    ) -> Self {
        Self {
            chunk_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            chunks_included,
            challenges_root,
            random_value,
            validator_proposals,
            chunk_mask,
            gas_price,
            validator_reward,
            total_supply,
            challenges_result,
            last_final_block,
            last_ds_final_block,
            approvals,
        }
    }

    pub fn hash(&self) -> CryptoHash {
        hash(&self.try_to_vec().expect("Failed to serialize"))
    }
}

/// The part of the block approval that is different for endorsements and skips
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum ApprovalInner {
    Endorsement(CryptoHash),
    Skip(BlockHeight),
}

/// Block approval by other block producers with a signature
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Approval {
    pub inner: ApprovalInner,
    pub target_height: BlockHeight,
    pub signature: Signature,
    pub account_id: AccountId,
}

/// Block approval by other block producers.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ApprovalMessage {
    pub approval: Approval,
    pub target: AccountId,
}

impl ApprovalInner {
    pub fn new(
        parent_hash: &CryptoHash,
        parent_height: BlockHeight,
        target_height: BlockHeight,
    ) -> Self {
        if target_height == parent_height + 1 {
            ApprovalInner::Endorsement(parent_hash.clone())
        } else {
            ApprovalInner::Skip(parent_height)
        }
    }
}

impl Approval {
    pub fn new(
        parent_hash: CryptoHash,
        parent_height: BlockHeight,
        target_height: BlockHeight,
        signer: &dyn ValidatorSigner,
    ) -> Self {
        let inner = ApprovalInner::new(&parent_hash, parent_height, target_height);
        let signature = signer.sign_approval(&inner, target_height);
        Approval { inner, target_height, signature, account_id: signer.validator_id().clone() }
    }

    pub fn get_data_for_sig(inner: &ApprovalInner, target_height: BlockHeight) -> Vec<u8> {
        [inner.try_to_vec().unwrap().as_ref(), target_height.to_be_bytes().as_ref()].concat()
    }
}

impl ApprovalMessage {
    pub fn new(approval: Approval, target: AccountId) -> Self {
        ApprovalMessage { approval, target }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
#[borsh_init(init)]
pub struct BlockHeader {
    pub prev_hash: CryptoHash,

    /// Inner part of the block header that gets hashed, split into two parts, one that is sent
    ///    to light clients, and the rest
    pub inner_lite: BlockHeaderInnerLite,
    pub inner_rest: BlockHeaderInnerRest,

    /// Signature of the block producer.
    pub signature: Signature,

    /// Cached value of hash for this block.
    #[borsh_skip]
    pub hash: CryptoHash,
}

impl BlockHeader {
    pub fn compute_inner_hash(
        inner_lite: &BlockHeaderInnerLite,
        inner_rest: &BlockHeaderInnerRest,
    ) -> CryptoHash {
        let hash_lite = inner_lite.hash();
        let hash_rest = inner_rest.hash();
        combine_hash(hash_lite, hash_rest)
    }

    pub fn compute_hash(
        prev_hash: CryptoHash,
        inner_lite: &BlockHeaderInnerLite,
        inner_rest: &BlockHeaderInnerRest,
    ) -> CryptoHash {
        let hash_inner = BlockHeader::compute_inner_hash(inner_lite, inner_rest);

        return combine_hash(hash_inner, prev_hash);
    }

    pub fn init(&mut self) {
        self.hash = BlockHeader::compute_hash(self.prev_hash, &self.inner_lite, &self.inner_rest);
    }

    pub fn new(
        height: BlockHeight,
        prev_hash: CryptoHash,
        prev_state_root: MerkleHash,
        chunk_receipts_root: MerkleHash,
        chunk_headers_root: MerkleHash,
        chunk_tx_root: MerkleHash,
        outcome_root: MerkleHash,
        timestamp: u64,
        chunks_included: u64,
        challenges_root: MerkleHash,
        random_value: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        chunk_mask: Vec<bool>,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        gas_price: Balance,
        validator_reward: Balance,
        total_supply: Balance,
        challenges_result: ChallengesResult,
        signer: &dyn ValidatorSigner,
        last_final_block: CryptoHash,
        last_ds_final_block: CryptoHash,
        approvals: Vec<Option<Signature>>,
        next_bp_hash: CryptoHash,
    ) -> Self {
        let inner_lite = BlockHeaderInnerLite::new(
            height,
            epoch_id,
            next_epoch_id,
            prev_state_root,
            outcome_root,
            timestamp,
            next_bp_hash,
        );
        let inner_rest = BlockHeaderInnerRest::new(
            chunk_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            chunks_included,
            challenges_root,
            random_value,
            validator_proposals,
            chunk_mask,
            gas_price,
            validator_reward,
            total_supply,
            challenges_result,
            last_final_block,
            last_ds_final_block,
            approvals,
        );
        let (hash, signature) = signer.sign_block_header_parts(prev_hash, &inner_lite, &inner_rest);
        Self { prev_hash, inner_lite, inner_rest, signature, hash }
    }

    pub fn genesis(
        height: BlockHeight,
        state_root: MerkleHash,
        chunk_receipts_root: MerkleHash,
        chunk_headers_root: MerkleHash,
        chunk_tx_root: MerkleHash,
        chunks_included: u64,
        challenges_root: MerkleHash,
        timestamp: DateTime<Utc>,
        initial_gas_price: Balance,
        initial_total_supply: Balance,
        next_bp_hash: CryptoHash,
    ) -> Self {
        let inner_lite = BlockHeaderInnerLite::new(
            height,
            EpochId::default(),
            EpochId::default(),
            state_root,
            CryptoHash::default(),
            to_timestamp(timestamp),
            next_bp_hash,
        );
        let inner_rest = BlockHeaderInnerRest::new(
            chunk_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            chunks_included,
            challenges_root,
            CryptoHash::default(),
            vec![],
            vec![],
            initial_gas_price,
            0,
            initial_total_supply,
            vec![],
            CryptoHash::default(),
            CryptoHash::default(),
            vec![],
        );
        let hash = BlockHeader::compute_hash(CryptoHash::default(), &inner_lite, &inner_rest);
        Self {
            prev_hash: CryptoHash::default(),
            inner_lite,
            inner_rest,
            signature: Signature::empty(KeyType::ED25519),
            hash,
        }
    }

    pub fn hash(&self) -> CryptoHash {
        self.hash
    }

    /// Verifies that given public key produced the block.
    pub fn verify_block_producer(&self, public_key: &PublicKey) -> bool {
        self.signature.verify(self.hash.as_ref(), public_key)
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        from_timestamp(self.inner_lite.timestamp)
    }

    pub fn num_approvals(&self) -> u64 {
        self.inner_rest.approvals.iter().filter(|x| x.is_some()).count() as u64
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct Block {
    pub header: BlockHeader,
    pub chunks: Vec<ShardChunkHeader>,
    pub challenges: Challenges,

    // Data to confirm the correctness of randomness beacon output
    pub vrf_value: near_crypto::vrf::Value,
    pub vrf_proof: near_crypto::vrf::Proof,
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
        chunks: Vec<ShardChunkHeader>,
        timestamp: DateTime<Utc>,
        height: BlockHeight,
        initial_gas_price: Balance,
        initial_total_supply: Balance,
        next_bp_hash: CryptoHash,
    ) -> Self {
        let challenges = vec![];
        Block {
            header: BlockHeader::genesis(
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
        }
    }

    /// Produces new block from header of previous block, current state root and set of transactions.
    pub fn produce(
        prev: &BlockHeader,
        height: BlockHeight,
        chunks: Vec<ShardChunkHeader>,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        approvals: Vec<Option<Signature>>,
        gas_price_adjustment_rate: Rational,
        min_gas_price: Balance,
        inflation: Option<Balance>,
        challenges_result: ChallengesResult,
        challenges: Challenges,
        signer: &dyn ValidatorSigner,
        next_bp_hash: CryptoHash,
    ) -> Self {
        // Collect aggregate of validators and gas usage/limits from chunks.
        let mut validator_proposals = vec![];
        let mut gas_used = 0;
        // This computation of chunk_mask relies on the fact that chunks are ordered by shard_id.
        let mut chunk_mask = vec![];
        let mut validator_reward = 0;
        let mut balance_burnt = 0;
        let mut gas_limit = 0;
        for chunk in chunks.iter() {
            if chunk.height_included == height {
                validator_proposals.extend_from_slice(&chunk.inner.validator_proposals);
                gas_used += chunk.inner.gas_used;
                gas_limit += chunk.inner.gas_limit;
                validator_reward += chunk.inner.validator_reward;
                balance_burnt += chunk.inner.balance_burnt;
                chunk_mask.push(true);
            } else {
                chunk_mask.push(false);
            }
        }
        let new_gas_price = Self::compute_new_gas_price(
            prev.inner_rest.gas_price,
            gas_used,
            gas_limit,
            gas_price_adjustment_rate,
        );
        let new_gas_price = std::cmp::max(new_gas_price, min_gas_price);

        let new_total_supply =
            prev.inner_rest.total_supply + inflation.unwrap_or(0) - balance_burnt;

        let now = to_timestamp(Utc::now());
        let time =
            if now <= prev.inner_lite.timestamp { prev.inner_lite.timestamp + 1 } else { now };

        let (vrf_value, vrf_proof) =
            signer.compute_vrf_with_proof(prev.inner_rest.random_value.as_ref());
        let random_value = hash(vrf_value.0.as_ref());

        let last_ds_final_block = if height == prev.inner_lite.height + 1 {
            prev.hash()
        } else {
            prev.inner_rest.last_ds_final_block
        };

        let last_final_block = if height == prev.inner_lite.height + 1
            && prev.inner_rest.last_ds_final_block == prev.prev_hash
        {
            prev.prev_hash
        } else {
            prev.inner_rest.last_final_block
        };

        Block {
            header: BlockHeader::new(
                height,
                prev.hash(),
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
                validator_reward,
                new_total_supply,
                challenges_result,
                signer,
                last_final_block,
                last_ds_final_block,
                approvals,
                next_bp_hash,
            ),
            chunks,
            challenges,

            vrf_value,
            vrf_proof,
        }
    }

    pub fn verify_gas_price(
        &self,
        prev_gas_price: Balance,
        min_gas_price: Balance,
        gas_price_adjustment_rate: Rational,
    ) -> bool {
        let gas_used = Self::compute_gas_used(&self.chunks, self.header.inner_lite.height);
        let gas_limit = Self::compute_gas_limit(&self.chunks, self.header.inner_lite.height);
        let expected_price = Self::compute_new_gas_price(
            prev_gas_price,
            gas_used,
            gas_limit,
            gas_price_adjustment_rate,
        );
        self.header.inner_rest.gas_price == max(expected_price, min_gas_price)
    }

    pub fn compute_new_gas_price(
        prev_gas_price: Balance,
        gas_used: Gas,
        gas_limit: Gas,
        gas_price_adjustment_rate: Rational,
    ) -> Balance {
        if gas_limit == 0 {
            prev_gas_price
        } else {
            let numerator = 2 * *gas_price_adjustment_rate.denom() as u128 * u128::from(gas_limit)
                - *gas_price_adjustment_rate.numer() as u128 * u128::from(gas_limit)
                + 2 * *gas_price_adjustment_rate.numer() as u128 * u128::from(gas_used);
            let denominator =
                2 * *gas_price_adjustment_rate.denom() as u128 * u128::from(gas_limit);
            prev_gas_price * numerator / denominator
        }
    }

    pub fn compute_state_root(chunks: &Vec<ShardChunkHeader>) -> CryptoHash {
        merklize(
            &chunks.iter().map(|chunk| chunk.inner.prev_state_root).collect::<Vec<CryptoHash>>(),
        )
        .0
    }

    pub fn compute_chunk_receipts_root(chunks: &Vec<ShardChunkHeader>) -> CryptoHash {
        merklize(
            &chunks
                .iter()
                .map(|chunk| chunk.inner.outgoing_receipts_root)
                .collect::<Vec<CryptoHash>>(),
        )
        .0
    }

    pub fn compute_chunk_headers_root(
        chunks: &Vec<ShardChunkHeader>,
    ) -> (CryptoHash, Vec<MerklePath>) {
        merklize(
            &chunks
                .iter()
                .map(|chunk| ChunkHashHeight(chunk.hash.clone(), chunk.height_included))
                .collect::<Vec<ChunkHashHeight>>(),
        )
    }

    pub fn compute_chunk_tx_root(chunks: &Vec<ShardChunkHeader>) -> CryptoHash {
        merklize(&chunks.iter().map(|chunk| chunk.inner.tx_root).collect::<Vec<CryptoHash>>()).0
    }

    pub fn compute_chunks_included(chunks: &Vec<ShardChunkHeader>, height: BlockHeight) -> u64 {
        chunks.iter().filter(|chunk| chunk.height_included == height).count() as u64
    }

    pub fn compute_outcome_root(chunks: &Vec<ShardChunkHeader>) -> CryptoHash {
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

    pub fn hash(&self) -> CryptoHash {
        self.header.hash()
    }

    pub fn check_validity(&self) -> bool {
        // Check that state root stored in the header matches the state root of the chunks
        let state_root = Block::compute_state_root(&self.chunks);
        if self.header.inner_lite.prev_state_root != state_root {
            return false;
        }

        // Check that chunk receipts root stored in the header matches the state root of the chunks
        let chunk_receipts_root = Block::compute_chunk_receipts_root(&self.chunks);
        if self.header.inner_rest.chunk_receipts_root != chunk_receipts_root {
            return false;
        }

        // Check that chunk headers root stored in the header matches the chunk headers root of the chunks
        let chunk_headers_root = Block::compute_chunk_headers_root(&self.chunks).0;
        if self.header.inner_rest.chunk_headers_root != chunk_headers_root {
            return false;
        }

        // Check that chunk tx root stored in the header matches the tx root of the chunks
        let chunk_tx_root = Block::compute_chunk_tx_root(&self.chunks);
        if self.header.inner_rest.chunk_tx_root != chunk_tx_root {
            return false;
        }

        // Check that chunk included root stored in the header matches the chunk included root of the chunks
        let chunks_included_root =
            Block::compute_chunks_included(&self.chunks, self.header.inner_lite.height);
        if self.header.inner_rest.chunks_included != chunks_included_root {
            return false;
        }

        // Check that challenges root stored in the header matches the challenges root of the challenges
        let challenges_root = Block::compute_challenges_root(&self.challenges);
        if self.header.inner_rest.challenges_root != challenges_root {
            return false;
        }

        true
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct GenesisId {
    /// Chain Id
    pub chain_id: String,
    /// Hash of genesis block
    pub hash: CryptoHash,
}
