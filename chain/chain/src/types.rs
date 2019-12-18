use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use near_crypto::Signature;
use near_primitives::block::{Approval, WeightAndScore};
pub use near_primitives::block::{Block, BlockHeader, Weight};
use near_primitives::challenge::{ChallengesResult, SlashedValidator};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{merklize, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ReceiptProof, ShardChunk, ShardChunkHeader};
use near_primitives::transaction::{ExecutionOutcomeWithId, SignedTransaction};
use near_primitives::types::{
    AccountId, Balance, BlockIndex, EpochId, Gas, MerkleHash, ShardId, StateRoot, StateRootNode,
    ValidatorStake,
};
use near_primitives::views::{EpochValidatorInfo, QueryResponse};
use near_store::{PartialStorage, StoreUpdate, WrappedTrieChanges};

use crate::chain::WEIGHT_MULTIPLIER;
use crate::error::Error;
use near_pool::types::PoolIterator;
use near_primitives::errors::InvalidTxError;
use std::cmp::{max, Ordering};

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct ReceiptResponse(pub CryptoHash, pub Vec<Receipt>);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct ReceiptProofResponse(pub CryptoHash, pub Vec<ReceiptProof>);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct RootProof(pub CryptoHash, pub MerklePath);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateHeaderKey(pub ShardId, pub CryptoHash);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
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
    pub total_rent_paid: Balance,
    pub total_validator_reward: Balance,
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
/// Additionally handles validators and block weight computation.
pub trait RuntimeAdapter: Send + Sync {
    /// Initialize state to genesis state and returns StoreUpdate, state root and initial validators.
    /// StoreUpdate can be discarded if the chain past the genesis.
    fn genesis_state(&self) -> (StoreUpdate, Vec<StateRoot>);

    /// Verify block producer validity
    fn verify_block_signature(&self, header: &BlockHeader) -> Result<(), Error>;

    /// Validates a given signed transaction on top of the given state root.
    /// Returns an option of `InvalidTxError`, it contains `Some(InvalidTxError)` if there is
    /// a validation error, or `None` in case the transaction succeeded.
    /// Throws an `Error` with `ErrorKind::StorageError` in case the runtime throws
    /// `RuntimeError::StorageError`.
    fn validate_tx(
        &self,
        block_index: BlockIndex,
        block_timestamp: u64,
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
        block_index: BlockIndex,
        block_timestamp: u64,
        gas_price: Balance,
        gas_limit: Gas,
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
    fn verify_approval_signature(
        &self,
        epoch_id: &EpochId,
        prev_block_hash: &CryptoHash,
        approvals: &[Approval],
    ) -> Result<bool, Error>;

    /// Epoch block producers (ordered by their order in the proposals) for given shard.
    /// Returns error if height is outside of known boundaries.
    fn get_epoch_block_producers(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, Error>;

    /// Block producers for given height for the main block. Return error if outside of known boundaries.
    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockIndex,
    ) -> Result<AccountId, Error>;

    /// Chunk producer for given height for given shard. Return error if outside of known boundaries.
    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockIndex,
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
    fn get_num_missing_blocks(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<u64, Error>;

    /// Get current number of shards.
    fn num_shards(&self) -> ShardId;

    fn num_total_parts(&self, parent_hash: &CryptoHash) -> usize;

    fn num_data_parts(&self, parent_hash: &CryptoHash) -> usize;

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
    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockIndex, Error>;

    /// Get inflation for a certain epoch
    fn get_epoch_inflation(&self, epoch_id: &EpochId) -> Result<Balance, Error>;

    fn push_final_block_back_if_needed(
        &self,
        parent_hash: CryptoHash,
        last_final_hash: CryptoHash,
    ) -> Result<CryptoHash, Error>;

    /// Add proposals for validators.
    fn add_validator_proposals(
        &self,
        parent_hash: CryptoHash,
        current_hash: CryptoHash,
        block_index: BlockIndex,
        last_finalized_height: BlockIndex,
        proposals: Vec<ValidatorStake>,
        slashed_validators: Vec<SlashedValidator>,
        validator_mask: Vec<bool>,
        rent_paid: Balance,
        validator_reward: Balance,
        total_supply: Balance,
    ) -> Result<(), Error>;

    /// Apply transactions to given state root and return store update and new state root.
    /// Also returns transaction result for each transaction and new receipts.
    fn apply_transactions(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        block_index: BlockIndex,
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
            block_index,
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
        block_index: BlockIndex,
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
        block_index: BlockIndex,
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
        state_root: &StateRoot,
        height: BlockIndex,
        block_timestamp: u64,
        block_hash: &CryptoHash,
        path_parts: Vec<&str>,
        data: &[u8],
    ) -> Result<QueryResponse, Box<dyn std::error::Error>>;

    fn get_validator_info(&self, block_hash: &CryptoHash) -> Result<EpochValidatorInfo, Error>;

    /// Get the part of the state from given state root.
    fn obtain_state_part(&self, state_root: &StateRoot, part_id: u64, num_parts: u64) -> Vec<u8>;

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
    fn confirm_state(&self, state_root: &StateRoot, parts: &Vec<Vec<u8>>) -> Result<(), Error>;

    /// Returns StateRootNode of a state.
    /// Panics if requested hash is not in storage.
    /// Never returns Error
    fn get_state_root_node(&self, state_root: &StateRoot) -> StateRootNode;

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
    fn build_receipts_hashes(&self, receipts: &Vec<Receipt>) -> Vec<CryptoHash> {
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

impl dyn RuntimeAdapter {
    pub fn compute_block_weight_delta(
        &self,
        approvals: Vec<&String>,
        prev_epoch: &EpochId,
        prev_hash: &CryptoHash,
    ) -> Result<u128, Error> {
        let block_producers = self.get_epoch_block_producers(prev_epoch, prev_hash)?;
        let mut account_to_stake = HashMap::new();

        let mut approved_stake = 0;
        let mut total_stake = 0;

        for bp in block_producers {
            account_to_stake.insert(bp.0.account_id, bp.0.amount);
            total_stake += bp.0.amount;
        }

        for approval in approvals {
            approved_stake += *account_to_stake.get(approval).unwrap_or(&0u128);
            account_to_stake.insert(approval.clone(), 0);
        }

        debug_assert!(approved_stake <= total_stake);

        // Compute ret as
        //
        //    ret = WEIGHT_MULTIPLIER * approved_stake / total_stake
        //
        // carefully handling the overflow
        let ret = if approved_stake >= std::u128::MAX / WEIGHT_MULTIPLIER {
            approved_stake / (total_stake / WEIGHT_MULTIPLIER)
        } else {
            WEIGHT_MULTIPLIER * approved_stake / total_stake
        };
        Ok(max(ret, 1))
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct ReceiptList(pub ShardId, pub Vec<Receipt>);

/// The last known / checked height and time when we have processed it.
/// Required to keep track of skipped blocks and not fallback to produce blocks at lower height.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct LatestKnown {
    pub height: BlockIndex,
    pub seen: u64,
}

/// The tip of a fork. A handle to the fork ancestry from its leaf in the
/// blockchain tree. References the max height and the latest and previous
/// blocks for convenience and the total weight.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq)]
pub struct Tip {
    /// Height of the tip (max height of the fork)
    pub height: BlockIndex,
    /// Last block pushed to the fork
    pub last_block_hash: CryptoHash,
    /// Previous block
    pub prev_block_hash: CryptoHash,
    /// Total weight on that fork
    pub weight_and_score: WeightAndScore,
    /// The timestamp of the head block
    pub prev_timestamp: u64,
    /// Previous epoch id. Used for getting validator info.
    pub epoch_id: EpochId,
}

impl Tip {
    /// Creates a new tip based on provided header.
    pub fn from_header_and_prev_timestamp(header: &BlockHeader, prev_timestamp: u64) -> Tip {
        Tip {
            height: header.inner_lite.height,
            last_block_hash: header.hash(),
            prev_block_hash: header.prev_hash,
            weight_and_score: header.inner_rest.weight_and_score(),
            prev_timestamp,
            epoch_id: header.inner_lite.epoch_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ShardStateSyncResponseHeader {
    pub chunk: ShardChunk,
    pub chunk_proof: MerklePath,
    pub prev_chunk_header: Option<ShardChunkHeader>,
    pub prev_chunk_proof: Option<MerklePath>,
    pub incoming_receipts_proofs: Vec<ReceiptProofResponse>,
    pub root_proofs: Vec<Vec<RootProof>>,
    pub state_root_node: StateRootNode,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ShardStateSyncResponse {
    pub header: Option<ShardStateSyncResponseHeader>,
    pub part_ids: Vec<u64>,
    pub data: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Default)]
pub struct StateRequestParts {
    pub ids: Vec<u64>,
    pub num_parts: u64,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use near_crypto::{InMemorySigner, KeyType, Signer};
    use near_primitives::block::genesis_chunks;

    use super::*;
    use crate::Chain;
    use near_primitives::merkle::verify_path;
    use near_primitives::transaction::{ExecutionOutcome, ExecutionStatus};

    #[test]
    fn test_block_produce() {
        let num_shards = 32;
        let genesis_chunks = genesis_chunks(vec![StateRoot::default()], num_shards, 1_000_000);
        let genesis = Block::genesis(
            genesis_chunks.into_iter().map(|chunk| chunk.header).collect(),
            Utc::now(),
            100,
            1_000_000_000,
            Chain::compute_bp_hash_inner(&vec![]).unwrap(),
        );
        let signer = InMemorySigner::from_seed("other", KeyType::ED25519, "other");
        let b1 = Block::empty(&genesis, &signer);
        assert!(signer.verify(b1.hash().as_ref(), &b1.header.signature));
        assert_eq!(b1.header.inner_rest.total_weight.to_num(), 1);
        let other_signer = InMemorySigner::from_seed("other2", KeyType::ED25519, "other2");
        let approvals = vec![
            (Approval {
                parent_hash: b1.hash(),
                reference_hash: b1.hash(),
                account_id: "other2".to_string(),
                signature: other_signer
                    .sign(Approval::get_data_for_sig(&b1.hash(), &b1.hash()).as_ref()),
            }),
        ];
        let b2 = Block::empty_with_approvals(
            &b1,
            2,
            b1.header.inner_lite.epoch_id.clone(),
            EpochId(genesis.hash()),
            approvals,
            &signer,
            genesis.header.inner_lite.next_bp_hash,
            20,
            1,
        );
        assert!(signer.verify(b2.hash().as_ref(), &b2.header.signature));
        assert_eq!(b2.header.inner_rest.total_weight.to_num(), 21);
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
