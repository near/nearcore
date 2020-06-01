use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use serde::Serialize;

use near_crypto::Signature;
use near_pool::types::PoolIterator;
pub use near_primitives::block::{Block, BlockHeader, Tip};
use near_primitives::challenge::{ChallengesResult, SlashedValidator};
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{merklize, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ReceiptProof, ShardChunk, ShardChunkHeader};
use near_primitives::transaction::{ExecutionOutcomeWithId, SignedTransaction};
use near_primitives::types::{
    AccountId, ApprovalStake, Balance, BlockHeight, EpochId, Gas, MerkleHash, ShardId, StateRoot,
    StateRootNode, ValidatorStake, ValidatorStats,
};
use near_primitives::views::{EpochValidatorInfo, QueryRequest, QueryResponse};
use near_store::{PartialStorage, ShardTries, Store, StoreUpdate, Trie, WrappedTrieChanges};

use crate::error::Error;

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ReceiptResponse(pub CryptoHash, pub Vec<Receipt>);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ReceiptProofResponse(pub CryptoHash, pub Vec<ReceiptProof>);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct RootProof(pub CryptoHash, pub MerklePath);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct StatePartKey(pub CryptoHash, pub ShardId, pub u64 /* PartId */);

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum BlockStatus {
    /// Block is the "next" block, updating the chain head.
    Next,
    /// Block does not update the chain head and is a fork.
    Fork,
    /// Block updates the chain head via a (potentially disruptive) "reorg".
    /// Previous block was not our previous chain head.
    Reorg(CryptoHash),
}

impl BlockStatus {
    pub fn is_new_head(&self) -> bool {
        match self {
            BlockStatus::Next => true,
            BlockStatus::Fork => false,
            BlockStatus::Reorg(_) => true,
        }
    }
}

/// Options for block origin.
#[derive(Eq, PartialEq, Clone, Debug)]
pub enum Provenance {
    /// No provenance.
    NONE,
    /// Adds block while in syncing mode.
    SYNC,
    /// Block we produced ourselves.
    PRODUCED,
}

/// Information about processed block.
#[derive(Debug, Clone)]
pub struct AcceptedBlock {
    pub hash: CryptoHash,
    pub status: BlockStatus,
    pub provenance: Provenance,
}

/// Map of shard to list of receipts to send to it.
pub type ReceiptResult = HashMap<ShardId, Vec<Receipt>>;

pub struct ApplyTransactionResult {
    pub trie_changes: WrappedTrieChanges,
    pub new_root: StateRoot,
    pub outcomes: Vec<ExecutionOutcomeWithId>,
    pub receipt_result: ReceiptResult,
    pub validator_proposals: Vec<ValidatorStake>,
    pub total_gas_burnt: Gas,
    pub total_balance_burnt: Balance,
    pub proof: Option<PartialStorage>,
}

impl ApplyTransactionResult {
    /// Returns root and paths for all the outcomes in the result.
    pub fn compute_outcomes_proof(
        outcomes: &[ExecutionOutcomeWithId],
    ) -> (MerkleHash, Vec<MerklePath>) {
        let mut result = vec![];
        for outcome_with_id in outcomes.iter() {
            result.push(outcome_with_id.to_hashes());
        }
        merklize(&result)
    }
}

/// Bridge between the chain and the runtime.
/// Main function is to update state given transactions.
/// Additionally handles validators.
pub trait RuntimeAdapter: Send + Sync {
    /// Initialize state to genesis state and returns StoreUpdate, state root and initial validators.
    /// StoreUpdate can be discarded if the chain past the genesis.
    fn genesis_state(&self) -> (Arc<Store>, StoreUpdate, Vec<StateRoot>);

    fn get_tries(&self) -> ShardTries;

    /// Returns trie.
    fn get_trie_for_shard(&self, shard_id: ShardId) -> Arc<Trie>;

    /// Verify block producer validity
    fn verify_block_signature(&self, header: &BlockHeader) -> Result<(), Error>;
    fn verify_block_vrf(
        &self,
        epoch_id: &EpochId,
        block_height: BlockHeight,
        prev_random_value: &CryptoHash,
        vrf_value: near_crypto::vrf::Value,
        vrf_proof: near_crypto::vrf::Proof,
    ) -> Result<(), Error>;

    /// Validates a given signed transaction on top of the given state root.
    /// Returns an option of `InvalidTxError`, it contains `Some(InvalidTxError)` if there is
    /// a validation error, or `None` in case the transaction succeeded.
    /// Throws an `Error` with `ErrorKind::StorageError` in case the runtime throws
    /// `RuntimeError::StorageError`.
    fn validate_tx(
        &self,
        gas_price: Balance,
        state_root: StateRoot,
        transaction: &SignedTransaction,
    ) -> Result<Option<InvalidTxError>, Error>;

    /// Returns an ordered list of valid transactions from the pool up the given limits.
    /// Pulls transactions from the given pool iterators one by one. Validates each transaction
    /// against the given `chain_validate` closure and runtime's transaction verifier.
    /// If the transaction is valid for both, it's added to the result and the temporary state
    /// update is preserved for validation of next transactions.
    /// Throws an `Error` with `ErrorKind::StorageError` in case the runtime throws
    /// `RuntimeError::StorageError`.
    fn prepare_transactions(
        &self,
        gas_price: Balance,
        gas_limit: Gas,
        shard_id: ShardId,
        state_root: StateRoot,
        max_number_of_transactions: usize,
        pool_iterator: &mut dyn PoolIterator,
        chain_validate: &mut dyn FnMut(&SignedTransaction) -> bool,
    ) -> Result<Vec<SignedTransaction>, Error>;

    /// Verify validator signature for the given epoch.
    /// Note: doesnt't account for slashed accounts within given epoch. USE WITH CAUTION.
    fn verify_validator_signature(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> Result<bool, Error>;

    /// Verify signature for validator or fisherman. Used for validating challenges.
    fn verify_validator_or_fisherman_signature(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> Result<bool, Error>;

    /// Verify header signature.
    fn verify_header_signature(&self, header: &BlockHeader) -> Result<bool, Error>;

    /// Verify chunk header signature.
    fn verify_chunk_header_signature(&self, header: &ShardChunkHeader) -> Result<bool, Error>;

    /// Verify aggregated bls signature
    fn verify_approval(
        &self,
        prev_block_hash: &CryptoHash,
        prev_block_height: BlockHeight,
        block_height: BlockHeight,
        approvals: &[Option<Signature>],
    ) -> Result<bool, Error>;

    /// Epoch block producers ordered by their order in the proposals.
    /// Returns error if height is outside of known boundaries.
    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, Error>;

    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<ApprovalStake>, Error>;

    /// Block producers for given height for the main block. Return error if outside of known boundaries.
    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, Error>;

    /// Chunk producer for given height for given shard. Return error if outside of known boundaries.
    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<AccountId, Error>;

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error>;

    fn get_fisherman_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error>;

    /// Number of missed blocks for given block producer.
    fn get_num_validator_blocks(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<ValidatorStats, Error>;

    /// Get current number of shards.
    fn num_shards(&self) -> ShardId;

    fn num_total_parts(&self) -> usize;

    fn num_data_parts(&self) -> usize;

    /// Account Id to Shard Id mapping, given current number of shards.
    fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId;

    /// Returns `account_id` that suppose to have the `part_id` of all chunks given previous block hash.
    fn get_part_owner(&self, parent_hash: &CryptoHash, part_id: u64) -> Result<AccountId, Error>;

    /// Whether the client cares about some shard right now.
    /// * If `account_id` is None, `is_me` is not checked and the
    /// result indicates whether the client is tracking the shard
    /// * If `account_id` is not None, it is supposed to be a validator
    /// account and `is_me` indicates whether we check what shards
    /// the client tracks.
    fn cares_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool;

    /// Whether the client cares about some shard in the next epoch.
    /// * If `account_id` is None, `is_me` is not checked and the
    /// result indicates whether the client will track the shard
    /// * If `account_id` is not None, it is supposed to be a validator
    /// account and `is_me` indicates whether we check what shards
    /// the client will track.
    fn will_care_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool;

    /// Returns true, if given hash is last block in it's epoch.
    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, Error>;

    /// Get epoch id given hash of previous block.
    fn get_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error>;

    /// Get next epoch id given hash of previous block.
    fn get_next_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash)
        -> Result<EpochId, Error>;

    /// Get epoch start for given block hash.
    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, Error>;

    /// Get the block height for which garbage collection should not go over
    fn get_gc_stop_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, Error>;

    /// Check if epoch exists.
    fn epoch_exists(&self, epoch_id: &EpochId) -> bool;

    /// Amount of tokens minted in given epoch.
    fn get_epoch_minted_amount(&self, epoch_id: &EpochId) -> Result<Balance, Error>;

    /// Add proposals for validators.
    fn add_validator_proposals(
        &self,
        parent_hash: CryptoHash,
        current_hash: CryptoHash,
        rng_seed: CryptoHash,
        height: BlockHeight,
        last_finalized_height: BlockHeight,
        proposals: Vec<ValidatorStake>,
        slashed_validators: Vec<SlashedValidator>,
        validator_mask: Vec<bool>,
        total_supply: Balance,
    ) -> Result<(), Error>;

    /// Apply transactions to given state root and return store update and new state root.
    /// Also returns transaction result for each transaction and new receipts.
    fn apply_transactions(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        height: BlockHeight,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        receipts: &[Receipt],
        transactions: &[SignedTransaction],
        last_validator_proposals: &[ValidatorStake],
        gas_price: Balance,
        gas_limit: Gas,
        challenges_result: &ChallengesResult,
    ) -> Result<ApplyTransactionResult, Error> {
        self.apply_transactions_with_optional_storage_proof(
            shard_id,
            state_root,
            height,
            block_timestamp,
            prev_block_hash,
            block_hash,
            receipts,
            transactions,
            last_validator_proposals,
            gas_price,
            gas_limit,
            challenges_result,
            false,
        )
    }

    fn apply_transactions_with_optional_storage_proof(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        height: BlockHeight,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        receipts: &[Receipt],
        transactions: &[SignedTransaction],
        last_validator_proposals: &[ValidatorStake],
        gas_price: Balance,
        gas_limit: Gas,
        challenges_result: &ChallengesResult,
        generate_storage_proof: bool,
    ) -> Result<ApplyTransactionResult, Error>;

    fn check_state_transition(
        &self,
        partial_storage: PartialStorage,
        shard_id: ShardId,
        state_root: &StateRoot,
        height: BlockHeight,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        receipts: &[Receipt],
        transactions: &[SignedTransaction],
        last_validator_proposals: &[ValidatorStake],
        gas_price: Balance,
        gas_limit: Gas,
        challenges_result: &ChallengesResult,
    ) -> Result<ApplyTransactionResult, Error>;

    /// Query runtime with given `path` and `data`.
    fn query(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        block_height: BlockHeight,
        block_timestamp: u64,
        block_hash: &CryptoHash,
        epoch_id: &EpochId,
        request: &QueryRequest,
    ) -> Result<QueryResponse, Box<dyn std::error::Error>>;

    fn get_validator_info(&self, block_hash: &CryptoHash) -> Result<EpochValidatorInfo, Error>;

    /// Get the part of the state from given state root.
    fn obtain_state_part(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        part_id: u64,
        num_parts: u64,
    ) -> Vec<u8>;

    /// Validate state part that expected to be given state root with provided data.
    /// Returns false if the resulting part doesn't match the expected one.
    fn validate_state_part(
        &self,
        state_root: &StateRoot,
        part_id: u64,
        num_parts: u64,
        data: &Vec<u8>,
    ) -> bool;

    /// Should be executed after accepting all the parts to set up a new state.
    fn confirm_state(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        parts: &Vec<Vec<u8>>,
    ) -> Result<(), Error>;

    /// Returns StateRootNode of a state.
    /// Panics if requested hash is not in storage.
    /// Never returns Error
    fn get_state_root_node(&self, shard_id: ShardId, state_root: &StateRoot) -> StateRootNode;

    /// Validate StateRootNode of a state.
    fn validate_state_root_node(
        &self,
        state_root_node: &StateRootNode,
        state_root: &StateRoot,
    ) -> bool;

    fn compare_epoch_id(
        &self,
        epoch_id: &EpochId,
        other_epoch_id: &EpochId,
    ) -> Result<Ordering, Error>;

    /// Build receipts hashes.
    fn build_receipts_hashes(&self, receipts: &[Receipt]) -> Vec<CryptoHash> {
        let mut receipts_hashes = vec![];
        for shard_id in 0..self.num_shards() {
            // importance to save the same order while filtering
            let shard_receipts: Vec<Receipt> = receipts
                .iter()
                .filter(|&receipt| self.account_id_to_shard_id(&receipt.receiver_id) == shard_id)
                .cloned()
                .collect();
            receipts_hashes
                .push(hash(&ReceiptList(shard_id, shard_receipts).try_to_vec().unwrap()));
        }
        receipts_hashes
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Default)]
pub struct ReceiptList(pub ShardId, pub Vec<Receipt>);

/// The last known / checked height and time when we have processed it.
/// Required to keep track of skipped blocks and not fallback to produce blocks at lower height.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Default)]
pub struct LatestKnown {
    pub height: BlockHeight,
    pub seen: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ShardStateSyncResponseHeader {
    pub chunk: ShardChunk,
    pub chunk_proof: MerklePath,
    pub prev_chunk_header: Option<ShardChunkHeader>,
    pub prev_chunk_proof: Option<MerklePath>,
    pub incoming_receipts_proofs: Vec<ReceiptProofResponse>,
    pub root_proofs: Vec<Vec<RootProof>>,
    pub state_root_node: StateRootNode,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ShardStateSyncResponse {
    pub header: Option<ShardStateSyncResponseHeader>,
    pub part: Option<(u64, Vec<u8>)>,
}

/// When running block sync response to know if the node needs to sync state,
/// or the hashes from the blocks that are needed.
pub enum BlockSyncResponse {
    /// State is needed before we start fetching recent blocks.
    StateNeeded,
    /// We are up to date with state, list of block hashes that need to be fetched.
    BlocksNeeded(Vec<CryptoHash>),
    /// We are up to date, nothing is required.
    None,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use near_crypto::KeyType;
    use near_primitives::block::{genesis_chunks, Approval};
    use near_primitives::merkle::verify_path;
    use near_primitives::transaction::{ExecutionOutcome, ExecutionStatus};
    use near_primitives::validator_signer::InMemoryValidatorSigner;

    use crate::Chain;

    use super::*;

    #[test]
    fn test_block_produce() {
        let num_shards = 32;
        let genesis_chunks = genesis_chunks(vec![StateRoot::default()], num_shards, 1_000_000, 0);
        let genesis = Block::genesis(
            genesis_chunks.into_iter().map(|chunk| chunk.header).collect(),
            Utc::now(),
            0,
            100,
            1_000_000_000,
            Chain::compute_bp_hash_inner(vec![]).unwrap(),
        );
        let signer = InMemoryValidatorSigner::from_seed("other", KeyType::ED25519, "other");
        let b1 = Block::empty(&genesis, &signer);
        assert!(b1.header.verify_block_producer(&signer.public_key()));
        let other_signer = InMemoryValidatorSigner::from_seed("other2", KeyType::ED25519, "other2");
        let approvals = vec![Some(Approval::new(b1.hash(), 1, 2, &other_signer).signature)];
        let b2 = Block::empty_with_approvals(
            &b1,
            2,
            b1.header.inner_lite.epoch_id.clone(),
            EpochId(genesis.hash()),
            approvals,
            &signer,
            genesis.header.inner_lite.next_bp_hash,
            CryptoHash::default(),
        );
        b2.header.verify_block_producer(&signer.public_key());
    }

    #[test]
    fn test_execution_outcome_merklization() {
        let outcome1 = ExecutionOutcomeWithId {
            id: Default::default(),
            outcome: ExecutionOutcome {
                status: ExecutionStatus::Unknown,
                logs: vec!["outcome1".to_string()],
                receipt_ids: vec![hash(&[1])],
                gas_burnt: 100,
            },
        };
        let outcome2 = ExecutionOutcomeWithId {
            id: Default::default(),
            outcome: ExecutionOutcome {
                status: ExecutionStatus::SuccessValue(vec![1]),
                logs: vec!["outcome2".to_string()],
                receipt_ids: vec![],
                gas_burnt: 0,
            },
        };
        let outcomes = vec![outcome1, outcome2];
        let (outcome_root, paths) = ApplyTransactionResult::compute_outcomes_proof(&outcomes);
        for (outcome_with_id, path) in outcomes.into_iter().zip(paths.into_iter()) {
            assert!(verify_path(outcome_root, &path, &outcome_with_id.to_hashes()));
        }
    }
}
