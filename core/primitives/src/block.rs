use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};

use near_crypto::{EmptySigner, KeyType, PublicKey, Signature, Signer};

use crate::challenge::{Challenges, ChallengesResult};
use crate::hash::{hash, CryptoHash};
use crate::merkle::{merklize, verify_path, MerklePath};
use crate::sharding::{ChunkHashHeight, EncodedShardChunk, ShardChunk, ShardChunkHeader};
use crate::types::{
    AccountId, Balance, BlockIndex, EpochId, Gas, MerkleHash, ShardId, StateRoot, ValidatorStake,
};
use crate::utils::{from_timestamp, to_timestamp};
use std::cmp::Ordering;

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeaderInner {
    /// Height of this block since the genesis block (height 0).
    pub height: BlockIndex,
    /// Epoch start hash of this block's epoch.
    /// Used for retrieving validator information
    pub epoch_id: EpochId,
    /// Hash of the block previous to this in the chain.
    pub prev_hash: CryptoHash,
    /// Root hash of the state at the previous block.
    pub prev_state_root: MerkleHash,
    /// Root hash of the chunk receipts in the given block.
    pub chunk_receipts_root: MerkleHash,
    /// Root hash of the chunk headers in the given block.
    pub chunk_headers_root: MerkleHash,
    /// Root hash of the chunk transactions in the given block.
    pub chunk_tx_root: MerkleHash,
    /// Root of the outcomes of transactions and receipts.
    pub outcome_root: MerkleHash,
    /// Number of chunks included into the block.
    pub chunks_included: u64,
    /// Timestamp at which the block was built.
    pub timestamp: u64,
    /// Total weight.
    pub total_weight: Weight,
    /// Score.
    pub score: Weight,
    /// Validator proposals.
    pub validator_proposals: Vec<ValidatorStake>,
    /// Mask for new chunks included in the block
    pub chunk_mask: Vec<bool>,
    /// Gas price. Same for all chunks
    pub gas_price: Balance,
    /// Sum of all storage rent paid across all chunks.
    pub rent_paid: Balance,
    /// Sum of all validator reward across all chunks.
    pub validator_reward: Balance,
    /// Total supply of tokens in the system
    pub total_supply: Balance,
    /// List of challenges result from previous block.
    pub challenges_result: ChallengesResult,

    /// Last block that has a quorum pre-vote on this chain
    pub last_quorum_pre_vote: CryptoHash,
    /// Last block that has a quorum pre-commit on this chain
    pub last_quorum_pre_commit: CryptoHash,

    /// All the approvals included in this block
    pub approvals: Vec<Approval>,
}

impl BlockHeaderInner {
    pub fn new(
        height: BlockIndex,
        epoch_id: EpochId,
        prev_hash: CryptoHash,
        prev_state_root: MerkleHash,
        chunk_receipts_root: MerkleHash,
        chunk_headers_root: MerkleHash,
        chunk_tx_root: MerkleHash,
        outcome_root: MerkleHash,
        timestamp: u64,
        chunks_included: u64,
        total_weight: Weight,
        score: Weight,
        validator_proposals: Vec<ValidatorStake>,
        chunk_mask: Vec<bool>,
        gas_price: Balance,
        rent_paid: Balance,
        validator_reward: Balance,
        total_supply: Balance,
        challenges_result: ChallengesResult,
        last_quorum_pre_vote: CryptoHash,
        last_quorum_pre_commit: CryptoHash,
        approvals: Vec<Approval>,
    ) -> Self {
        Self {
            height,
            epoch_id,
            prev_hash,
            prev_state_root,
            chunk_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            outcome_root,
            timestamp,
            chunks_included,
            total_weight,
            score,
            validator_proposals,
            chunk_mask,
            gas_price,
            rent_paid,
            validator_reward,
            total_supply,
            challenges_result,
            last_quorum_pre_vote,
            last_quorum_pre_commit,
            approvals,
        }
    }

    pub fn weight_and_score(&self) -> WeightAndScore {
        WeightAndScore { weight: self.total_weight, score: self.score }
    }
}

/// Block approval by other block producers.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct Approval {
    pub parent_hash: CryptoHash,
    pub reference_hash: CryptoHash,
    pub signature: Signature,
    pub account_id: AccountId,
}

/// Block approval by other block producers.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct ApprovalMessage {
    pub approval: Approval,
    pub target: AccountId,
}

impl Approval {
    pub fn new(
        parent_hash: CryptoHash,
        reference_hash: CryptoHash,
        signer: &dyn Signer,
        account_id: AccountId,
    ) -> Self {
        let signature =
            signer.sign(Approval::get_data_for_sig(&parent_hash, &reference_hash).as_ref());
        Approval { parent_hash, reference_hash, signature, account_id }
    }

    pub fn get_data_for_sig(parent_hash: &CryptoHash, reference_hash: &CryptoHash) -> Vec<u8> {
        let mut res = Vec::with_capacity(64);
        res.extend_from_slice(parent_hash.as_ref());
        res.extend_from_slice(reference_hash.as_ref());
        res
    }
}

impl ApprovalMessage {
    pub fn new(
        parent_hash: CryptoHash,
        reference_hash: CryptoHash,
        signer: &dyn Signer,
        account_id: AccountId,
        target: AccountId,
    ) -> Self {
        let approval = Approval::new(parent_hash, reference_hash, signer, account_id);
        ApprovalMessage { approval, target }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
#[borsh_init(init)]
pub struct BlockHeader {
    /// Inner part of the block header that gets hashed.
    pub inner: BlockHeaderInner,

    /// Signature of the block producer.
    pub signature: Signature,

    /// Cached value of hash for this block.
    #[borsh_skip]
    pub hash: CryptoHash,
}

impl BlockHeader {
    pub fn init(&mut self) {
        self.hash = hash(&self.inner.try_to_vec().expect("Failed to serialize"));
    }

    pub fn new(
        height: BlockIndex,
        prev_hash: CryptoHash,
        prev_state_root: MerkleHash,
        chunk_receipts_root: MerkleHash,
        chunk_headers_root: MerkleHash,
        chunk_tx_root: MerkleHash,
        outcome_root: MerkleHash,
        timestamp: u64,
        chunks_included: u64,
        total_weight: Weight,
        score: Weight,
        validator_proposals: Vec<ValidatorStake>,
        chunk_mask: Vec<bool>,
        epoch_id: EpochId,
        gas_price: Balance,
        rent_paid: Balance,
        validator_reward: Balance,
        total_supply: Balance,
        challenges_result: ChallengesResult,
        signer: &dyn Signer,
        last_quorum_pre_vote: CryptoHash,
        last_quorum_pre_commit: CryptoHash,
        approvals: Vec<Approval>,
    ) -> Self {
        let inner = BlockHeaderInner::new(
            height,
            epoch_id,
            prev_hash,
            prev_state_root,
            chunk_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            outcome_root,
            timestamp,
            chunks_included,
            total_weight,
            score,
            validator_proposals,
            chunk_mask,
            gas_price,
            rent_paid,
            validator_reward,
            total_supply,
            challenges_result,
            last_quorum_pre_vote,
            last_quorum_pre_commit,
            approvals,
        );
        let hash = hash(&inner.try_to_vec().expect("Failed to serialize"));
        Self { inner, signature: signer.sign(hash.as_ref()), hash }
    }

    pub fn genesis(
        state_root: MerkleHash,
        chunk_receipts_root: MerkleHash,
        chunk_headers_root: MerkleHash,
        chunk_tx_root: MerkleHash,
        chunks_included: u64,
        timestamp: DateTime<Utc>,
        initial_gas_price: Balance,
        initial_total_supply: Balance,
    ) -> Self {
        let inner = BlockHeaderInner::new(
            0,
            EpochId::default(),
            CryptoHash::default(),
            state_root,
            chunk_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            CryptoHash::default(),
            to_timestamp(timestamp),
            chunks_included,
            0.into(),
            0.into(),
            vec![],
            vec![],
            initial_gas_price,
            0,
            0,
            initial_total_supply,
            vec![],
            CryptoHash::default(),
            CryptoHash::default(),
            vec![],
        );
        let hash = hash(&inner.try_to_vec().expect("Failed to serialize"));
        Self { inner, signature: Signature::empty(KeyType::ED25519), hash }
    }

    pub fn hash(&self) -> CryptoHash {
        self.hash
    }

    /// Verifies that given public key produced the block.
    pub fn verify_block_producer(&self, public_key: &PublicKey) -> bool {
        self.signature.verify(self.hash.as_ref(), public_key)
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        from_timestamp(self.inner.timestamp)
    }

    pub fn num_approvals(&self) -> u64 {
        self.inner.approvals.len() as u64
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct Block {
    pub header: BlockHeader,
    pub chunks: Vec<ShardChunkHeader>,
    pub challenges: Challenges,
}

pub fn genesis_chunks(
    state_roots: Vec<StateRoot>,
    num_shards: ShardId,
    initial_gas_limit: Gas,
) -> Vec<ShardChunk> {
    assert!(state_roots.len() == 1 || state_roots.len() == (num_shards as usize));
    (0..num_shards)
        .map(|i| {
            let (encoded_chunk, _) = EncodedShardChunk::new(
                CryptoHash::default(),
                state_roots[i as usize % state_roots.len()].clone(),
                CryptoHash::default(),
                0,
                i,
                3,
                1,
                0,
                initial_gas_limit,
                0,
                0,
                0,
                CryptoHash::default(),
                vec![],
                &vec![],
                &vec![],
                CryptoHash::default(),
                &EmptySigner {},
            )
            .expect("Failed to decode genesis chunk");
            encoded_chunk.decode_chunk(1).expect("Failed to decode genesis chunk")
        })
        .collect()
}

impl Block {
    /// Returns genesis block for given genesis date and state root.
    pub fn genesis(
        chunks: Vec<ShardChunkHeader>,
        timestamp: DateTime<Utc>,
        initial_gas_price: Balance,
        initial_total_supply: Balance,
    ) -> Self {
        Block {
            header: BlockHeader::genesis(
                Block::compute_state_root(&chunks),
                Block::compute_chunk_receipts_root(&chunks),
                Block::compute_chunk_headers_root(&chunks).0,
                Block::compute_chunk_tx_root(&chunks),
                Block::compute_chunks_included(&chunks, 0),
                timestamp,
                initial_gas_price,
                initial_total_supply,
            ),
            chunks,
            challenges: vec![],
        }
    }

    /// Produces new block from header of previous block, current state root and set of transactions.
    pub fn produce(
        prev: &BlockHeader,
        height: BlockIndex,
        chunks: Vec<ShardChunkHeader>,
        epoch_id: EpochId,
        approvals: Vec<Approval>,
        gas_price_adjustment_rate: u8,
        inflation: Option<Balance>,
        challenges_result: ChallengesResult,
        challenges: Challenges,
        signer: &dyn Signer,
        score: Weight,
        last_quorum_pre_vote: CryptoHash,
        last_quorum_pre_commit: CryptoHash,
    ) -> Self {
        // Collect aggregate of validators and gas usage/limits from chunks.
        let mut validator_proposals = vec![];
        let mut gas_used = 0;
        // This computation of chunk_mask relies on the fact that chunks are ordered by shard_id.
        let mut chunk_mask = vec![];
        let mut storage_rent = 0;
        let mut validator_reward = 0;
        let mut balance_burnt = 0;
        let mut gas_limit = 0;
        for chunk in chunks.iter() {
            if chunk.height_included == height {
                validator_proposals.extend_from_slice(&chunk.inner.validator_proposals);
                gas_used += chunk.inner.gas_used;
                gas_limit += chunk.inner.gas_limit;
                storage_rent += chunk.inner.rent_paid;
                validator_reward += chunk.inner.validator_reward;
                balance_burnt += chunk.inner.balance_burnt;
                chunk_mask.push(true);
            } else {
                chunk_mask.push(false);
            }
        }
        let new_gas_price = Self::compute_new_gas_price(
            prev.inner.gas_price,
            gas_used,
            gas_limit,
            gas_price_adjustment_rate,
        );

        let new_total_supply = prev.inner.total_supply + inflation.unwrap_or(0) - balance_burnt;

        let num_approvals: u128 = approvals.len() as u128;
        let total_weight = prev.inner.total_weight.next(num_approvals);
        let now = to_timestamp(Utc::now());
        let time = if now <= prev.inner.timestamp { prev.inner.timestamp + 1 } else { now };

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
                total_weight,
                score,
                validator_proposals,
                chunk_mask,
                epoch_id,
                new_gas_price,
                storage_rent,
                validator_reward,
                new_total_supply,
                challenges_result,
                signer,
                last_quorum_pre_vote,
                last_quorum_pre_commit,
                approvals,
            ),
            chunks,
            challenges,
        }
    }

    pub fn verify_gas_price(&self, prev_gas_price: Balance, gas_price_adjustment_rate: u8) -> bool {
        let gas_used = Self::compute_gas_used(&self.chunks, self.header.inner.height);
        let gas_limit = Self::compute_gas_limit(&self.chunks, self.header.inner.height);
        let expected_price = Self::compute_new_gas_price(
            prev_gas_price,
            gas_used,
            gas_limit,
            gas_price_adjustment_rate,
        );
        expected_price == self.header.inner.gas_price
    }

    pub fn compute_new_gas_price(
        prev_gas_price: Balance,
        gas_used: Gas,
        gas_limit: Gas,
        gas_price_adjustment_rate: u8,
    ) -> Balance {
        if gas_limit == 0 {
            prev_gas_price
        } else {
            let numerator = 2 * 100 * u128::from(gas_limit)
                - u128::from(gas_price_adjustment_rate) * u128::from(gas_limit)
                + 2 * u128::from(gas_price_adjustment_rate) * u128::from(gas_used);
            let denominator = 2 * 100 * u128::from(gas_limit);
            prev_gas_price * numerator / denominator
        }
    }

    pub fn compute_state_root(chunks: &Vec<ShardChunkHeader>) -> CryptoHash {
        merklize(
            &chunks
                .iter()
                .map(|chunk| chunk.inner.prev_state_root.hash)
                .collect::<Vec<CryptoHash>>(),
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

    pub fn compute_chunks_included(chunks: &Vec<ShardChunkHeader>, height: BlockIndex) -> u64 {
        chunks.iter().filter(|chunk| chunk.height_included == height).count() as u64
    }

    pub fn compute_outcome_root(chunks: &Vec<ShardChunkHeader>) -> CryptoHash {
        merklize(&chunks.iter().map(|chunk| chunk.inner.outcome_root).collect::<Vec<CryptoHash>>())
            .0
    }

    pub fn compute_gas_used(chunks: &[ShardChunkHeader], block_height: BlockIndex) -> Gas {
        chunks.iter().fold(0, |acc, chunk| {
            if chunk.height_included == block_height {
                acc + chunk.inner.gas_used
            } else {
                acc
            }
        })
    }

    pub fn compute_gas_limit(chunks: &[ShardChunkHeader], block_height: BlockIndex) -> Gas {
        chunks.iter().fold(0, |acc, chunk| {
            if chunk.height_included == block_height {
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
        if self.header.inner.prev_state_root != state_root {
            return false;
        }

        // Check that chunk receipts root stored in the header matches the state root of the chunks
        let chunk_receipts_root = Block::compute_chunk_receipts_root(&self.chunks);
        if self.header.inner.chunk_receipts_root != chunk_receipts_root {
            return false;
        }

        // Check that chunk headers root stored in the header matches the chunk headers root of the chunks
        let chunk_headers_root = Block::compute_chunk_headers_root(&self.chunks).0;
        if self.header.inner.chunk_headers_root != chunk_headers_root {
            return false;
        }

        // Check that chunk tx root stored in the header matches the tx root of the chunks
        let chunk_tx_root = Block::compute_chunk_tx_root(&self.chunks);
        if self.header.inner.chunk_tx_root != chunk_tx_root {
            return false;
        }

        // Check that chunk included root stored in the header matches the chunk included root of the chunks
        let chunks_included_root =
            Block::compute_chunks_included(&self.chunks, self.header.inner.height);
        if self.header.inner.chunks_included != chunks_included_root {
            return false;
        }

        true
    }
}

/// The weight is defined as the number of unique validators approving this fork.
#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Default,
)]
pub struct Weight {
    num: u128,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WeightAndScore {
    pub weight: Weight,
    pub score: Weight,
}

impl Weight {
    pub fn to_num(self) -> u128 {
        self.num
    }

    pub fn next(self, num: u128) -> Self {
        Weight { num: self.num + num + 1 }
    }
}

impl From<u128> for Weight {
    fn from(num: u128) -> Self {
        Weight { num }
    }
}

impl std::fmt::Display for Weight {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.num)
    }
}

impl WeightAndScore {
    pub fn from_ints(weight: u128, score: u128) -> Self {
        Self { weight: weight.into(), score: score.into() }
    }

    /// Returns whether one chain is `threshold` weight ahead of the other, where "ahead" is losely
    /// defined as either having the score exceeding by the `threshold` (finality gadget is working
    /// fine, and the last reported final block is way ahead of the last known to us), or having the
    /// same score, but the weight exceeding by the `threshold` (finality gadget is down, and the
    /// canonical chain is has significantly higher weight)
    pub fn beyond_threshold(&self, other: &WeightAndScore, threshold: u128) -> bool {
        if self.score == other.score {
            self.weight.to_num() > other.weight.to_num() + threshold
        } else {
            self.score.to_num() > other.score.to_num() + threshold
        }
    }
}

impl PartialOrd for WeightAndScore {
    fn partial_cmp(&self, other: &WeightAndScore) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WeightAndScore {
    fn cmp(&self, other: &WeightAndScore) -> Ordering {
        match self.score.cmp(&other.score) {
            v @ Ordering::Less | v @ Ordering::Greater => v,
            Ordering::Equal => self.weight.cmp(&other.weight),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct GenesisId {
    /// Chain Id
    pub chain_id: String,
    /// Hash of genesis block
    pub hash: CryptoHash,
}
