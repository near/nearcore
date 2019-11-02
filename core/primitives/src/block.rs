use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};

use near_crypto::{EmptySigner, KeyType, PublicKey, Signature, Signer};

use crate::challenge::{Challenges, ChallengesResult};
use crate::hash::{hash, CryptoHash};
use crate::merkle::{merklize, verify_path, MerklePath};
use crate::sharding::{ChunkHashHeight, EncodedShardChunk, ShardChunk, ShardChunkHeader};
use crate::types::{
    Balance, BlockIndex, EpochId, Gas, MerkleHash, ShardId, StateRoot, ValidatorStake,
};
use crate::utils::{from_timestamp, to_timestamp};

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
    /// Number of chunks included into the block.
    pub chunks_included: u64,
    /// Timestamp at which the block was built.
    pub timestamp: u64,
    /// Approval mask, given current block producers.
    pub approval_mask: Vec<bool>,
    /// Approval signatures for previous block.
    pub approval_sigs: Vec<Signature>,
    /// Total weight.
    pub total_weight: Weight,
    /// Validator proposals.
    pub validator_proposals: Vec<ValidatorStake>,
    /// Mask for new chunks included in the block
    pub chunk_mask: Vec<bool>,
    /// Sum of gas used across all chunks.
    pub gas_used: Gas,
    /// Gas limit. Same for all chunks.
    pub gas_limit: Gas,
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
        timestamp: u64,
        chunks_included: u64,
        approval_mask: Vec<bool>,
        approval_sigs: Vec<Signature>,
        total_weight: Weight,
        validator_proposals: Vec<ValidatorStake>,
        chunk_mask: Vec<bool>,
        gas_used: Gas,
        gas_limit: Gas,
        gas_price: Balance,
        rent_paid: Balance,
        validator_reward: Balance,
        total_supply: Balance,
        challenges_result: ChallengesResult,
    ) -> Self {
        Self {
            height,
            epoch_id,
            prev_hash,
            prev_state_root,
            chunk_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            timestamp,
            chunks_included,
            approval_mask,
            approval_sigs,
            total_weight,
            validator_proposals,
            chunk_mask,
            gas_used,
            gas_limit,
            gas_price,
            rent_paid,
            validator_reward,
            total_supply,
            challenges_result,
        }
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
        timestamp: u64,
        chunks_included: u64,
        approval_mask: Vec<bool>,
        approval_sigs: Vec<Signature>,
        total_weight: Weight,
        validator_proposals: Vec<ValidatorStake>,
        chunk_mask: Vec<bool>,
        epoch_id: EpochId,
        gas_used: Gas,
        gas_limit: Gas,
        gas_price: Balance,
        rent_paid: Balance,
        validator_reward: Balance,
        total_supply: Balance,
        challenges_result: ChallengesResult,
        signer: &dyn Signer,
    ) -> Self {
        let inner = BlockHeaderInner::new(
            height,
            epoch_id,
            prev_hash,
            prev_state_root,
            chunk_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            timestamp,
            chunks_included,
            approval_mask,
            approval_sigs,
            total_weight,
            validator_proposals,
            chunk_mask,
            gas_used,
            gas_limit,
            gas_price,
            rent_paid,
            validator_reward,
            total_supply,
            challenges_result,
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
        initial_gas_limit: Gas,
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
            to_timestamp(timestamp),
            chunks_included,
            vec![],
            vec![],
            0.into(),
            vec![],
            vec![],
            0,
            initial_gas_limit,
            initial_gas_price,
            0,
            0,
            initial_total_supply,
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
        self.inner.approval_mask.iter().map(|x| if *x { 1u64 } else { 0u64 }).sum()
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
        initial_gas_limit: Gas,
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
                initial_gas_limit,
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
        mut approvals: HashMap<usize, Signature>,
        gas_price_adjustment_rate: u8,
        inflation: Option<Balance>,
        challenges_result: ChallengesResult,
        challenges: Challenges,
        signer: &dyn Signer,
    ) -> Self {
        let (approval_mask, approval_sigs) = if let Some(max_approver) = approvals.keys().max() {
            (
                (0..=*max_approver).map(|i| approvals.contains_key(&i)).collect(),
                (0..=*max_approver).filter_map(|i| approvals.remove(&i)).collect(),
            )
        } else {
            (vec![], vec![])
        };

        // Collect aggregate of validators and gas usage/limits from chunks.
        let mut validator_proposals = vec![];
        let mut gas_used = 0;
        let mut gas_limit = 0;
        // This computation of chunk_mask relies on the fact that chunks are ordered by shard_id.
        let mut chunk_mask = vec![];
        let mut storage_rent = 0;
        let mut validator_reward = 0;
        let mut balance_burnt = 0;
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

        let new_gas_price = if gas_limit > 0 {
            (2 * u128::from(gas_limit) + 2 * u128::from(gas_price_adjustment_rate)
                - u128::from(gas_limit) * u128::from(gas_price_adjustment_rate))
                * prev.inner.gas_price
                / (2 * u128::from(gas_limit) * 100)
        } else {
            // If there are no new chunks included in this block, use previous price.
            prev.inner.gas_price
        };
        let new_total_supply = prev.inner.total_supply + inflation.unwrap_or(0) - balance_burnt;

        let num_approvals: u128 =
            approval_mask.iter().map(|x| if *x { 1u128 } else { 0u128 }).sum();
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
                time,
                Block::compute_chunks_included(&chunks, height),
                approval_mask,
                approval_sigs,
                total_weight,
                validator_proposals,
                chunk_mask,
                epoch_id,
                gas_used,
                gas_limit,
                // TODO: calculate this correctly
                new_gas_price,
                storage_rent,
                validator_reward,
                new_total_supply,
                challenges_result,
                signer,
            ),
            chunks,
            challenges,
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

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct GenesisId {
    /// Chain Id
    pub chain_id: String,
    /// Hash of genesis block
    pub hash: CryptoHash,
}
