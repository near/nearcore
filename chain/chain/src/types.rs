use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use near_crypto::Signature;
use near_primitives::block::Approval;
pub use near_primitives::block::{Block, BlockHeader, Weight};
use near_primitives::challenge::ChallengesResult;
use near_primitives::errors::RuntimeError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{merklize, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ReceiptProof, ShardChunk, ShardChunkHeader};
use near_primitives::transaction::{ExecutionOutcomeWithId, SignedTransaction};
use near_primitives::types::{
    AccountId, Balance, BlockIndex, EpochId, Gas, MerkleHash, ShardId, StateRoot, ValidatorStake,
};
use near_primitives::views::QueryResponse;
use near_store::{PartialStorage, StoreUpdate, WrappedTrieChanges};

use crate::error::Error;

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct ReceiptResponse(pub CryptoHash, pub Vec<Receipt>);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct ReceiptProofResponse(pub CryptoHash, pub Vec<ReceiptProof>);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct RootProof(pub CryptoHash, pub MerklePath);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateHeaderKey(pub ShardId, pub CryptoHash);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StatePartKey(pub u64, pub StateRoot);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StatePart {
    pub shard_id: ShardId,
    pub part_id: u64,
    pub data: Vec<u8>,
}

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
    pub gas_used: Gas,
    pub gas_limit: Gas,
}

/// Information about valid transaction that was processed by chain + runtime.
#[derive(Debug)]
pub struct ValidTransaction {
    pub transaction: SignedTransaction,
}

/// Map of shard to list of receipts to send to it.
pub type ReceiptResult = HashMap<ShardId, Vec<Receipt>>;

#[derive(Eq, PartialEq, Debug)]
pub enum ValidatorSignatureVerificationResult {
    Valid,
    Invalid,
    UnknownEpoch,
}

impl ValidatorSignatureVerificationResult {
    pub fn valid(&self) -> bool {
        *self == ValidatorSignatureVerificationResult::Valid
    }
}

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
            result.extend(outcome_with_id.outcome.to_hashes());
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

    /// Verify block producer validity and return weight of given block for fork choice rule.
    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error>;

    /// Validate transaction and return transaction information relevant to ordering it in the mempool.
    fn validate_tx(
        &self,
        block_index: BlockIndex,
        block_timestamp: u64,
        gas_price: Balance,
        state_root: StateRoot,
        transaction: SignedTransaction,
    ) -> Result<ValidTransaction, RuntimeError>;

    /// Filter transactions by verifying each one by one in the given order. Every successful
    /// verification stores the updated account balances to be used by next transactions.
    fn filter_transactions(
        &self,
        block_index: BlockIndex,
        block_timestamp: u64,
        gas_price: Balance,
        state_root: StateRoot,
        transactions: Vec<SignedTransaction>,
    ) -> Vec<SignedTransaction>;

    /// Verify validator signature for the given epoch.
    /// Note: doesnt't account for slashed accounts within given epoch. USE WITH CAUTION.
    fn verify_validator_signature(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> ValidatorSignatureVerificationResult;

    /// Verify header signature.
    fn verify_header_signature(&self, header: &BlockHeader)
        -> ValidatorSignatureVerificationResult;

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
    ) -> Result<Vec<(AccountId, bool)>, Error>;

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

    /// Add proposals for validators.
    fn add_validator_proposals(
        &self,
        parent_hash: CryptoHash,
        current_hash: CryptoHash,
        block_index: BlockIndex,
        proposals: Vec<ValidatorStake>,
        slashed_validators: Vec<AccountId>,
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

    /// Get the part of the state from given state root + proof.
    fn obtain_state_part(
        &self,
        shard_id: ShardId,
        part_id: u64,
        state_root: &StateRoot,
    ) -> Result<(StatePart, Vec<u8>), Box<dyn std::error::Error>>;

    /// Set state part that expected to be given state root with provided data.
    /// Returns error if:
    /// 1. Failed to parse, or
    /// 2. The proof is invalid, or
    /// 3. The resulting part doesn't match the expected one.
    fn accept_state_part(
        &self,
        state_root: &StateRoot,
        part: &StatePart,
        proof: &Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>>;

    /// Should be executed after accepting all the parts.
    /// Returns `true` if state is set successfully.
    fn confirm_state(&self, state_root: &StateRoot) -> Result<bool, Error>;

    /// Build receipts hashes.
    fn build_receipts_hashes(&self, receipts: &Vec<Receipt>) -> Result<Vec<CryptoHash>, Error> {
        let mut receipts_hashes = vec![];
        for shard_id in 0..self.num_shards() {
            // importance to save the same order while filtering
            let shard_receipts: Vec<Receipt> = receipts
                .iter()
                .filter(|&receipt| self.account_id_to_shard_id(&receipt.receiver_id) == shard_id)
                .cloned()
                .collect();
            receipts_hashes.push(hash(&ReceiptList(shard_id, shard_receipts).try_to_vec()?));
        }
        Ok(receipts_hashes)
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
    pub total_weight: Weight,
    /// Previous epoch id. Used for getting validator info.
    pub epoch_id: EpochId,
}

impl Tip {
    /// Creates a new tip based on provided header.
    pub fn from_header(header: &BlockHeader) -> Tip {
        Tip {
            height: header.inner.height,
            last_block_hash: header.hash(),
            prev_block_hash: header.inner.prev_hash,
            total_weight: header.inner.total_weight,
            epoch_id: header.inner.epoch_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ShardStateSyncResponseHeader {
    pub chunk: ShardChunk,
    pub chunk_proof: MerklePath,
    pub prev_chunk_header: ShardChunkHeader,
    pub prev_chunk_proof: MerklePath,
    pub incoming_receipts_proofs: Vec<ReceiptProofResponse>,
    pub root_proofs: Vec<Vec<RootProof>>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ShardStateSyncResponsePart {
    pub state_part: StatePart,
    pub proof: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ShardStateSyncResponse {
    pub header: Option<ShardStateSyncResponseHeader>,
    pub parts: Vec<ShardStateSyncResponsePart>,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use near_crypto::{InMemorySigner, KeyType, Signer};
    use near_primitives::block::genesis_chunks;

    use super::*;

    #[test]
    fn test_block_produce() {
        let num_shards = 32;
        let genesis_chunks = genesis_chunks(
            vec![StateRoot { hash: CryptoHash::default(), num_parts: 9 /* TODO MOO */ }],
            num_shards,
            1_000_000,
        );
        let genesis = Block::genesis(
            genesis_chunks.into_iter().map(|chunk| chunk.header).collect(),
            Utc::now(),
            1_000_000,
            100,
            1_000_000_000,
        );
        let signer = InMemorySigner::from_seed("other", KeyType::ED25519, "other");
        let b1 = Block::empty(&genesis, &signer);
        assert!(signer.verify(b1.hash().as_ref(), &b1.header.signature));
        assert_eq!(b1.header.inner.total_weight.to_num(), 1);
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
            b1.header.inner.epoch_id.clone(),
            approvals,
            &signer,
        );
        assert!(signer.verify(b2.hash().as_ref(), &b2.header.signature));
        assert_eq!(b2.header.inner.total_weight.to_num(), 3);
    }
}
