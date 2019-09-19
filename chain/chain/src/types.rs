use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use near_crypto::{Signature, Signer};
pub use near_primitives::block::{Block, BlockHeader, Weight};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkOnePart, ShardChunk, ShardChunkHeader};
use near_primitives::transaction::{SignedTransaction, TransactionLog};
use near_primitives::types::{
    AccountId, Balance, BlockIndex, EpochId, Gas, MerkleHash, ShardId, ValidatorStake,
};
use near_primitives::views::QueryResponse;
use near_store::{PartialStorage, StoreUpdate, WrappedTrieChanges};

use crate::error::Error;

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct ReceiptResponse(pub CryptoHash, pub Vec<Receipt>);

#[derive(Eq, PartialEq, Debug)]
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
#[derive(Eq, PartialEq)]
pub enum Provenance {
    /// No provenance.
    NONE,
    /// Adds block while in syncing mode.
    SYNC,
    /// Block we produced ourselves.
    PRODUCED,
}

/// Information about valid transaction that was processed by chain + runtime.
#[derive(Debug)]
pub struct ValidTransaction {
    pub transaction: SignedTransaction,
}

/// Map of shard to list of receipts to send to it.
pub type ReceiptResult = HashMap<ShardId, Vec<Receipt>>;

pub enum ShardFullChunkOrOnePart<'a> {
    // The validator follows the shard, and has the full chunk
    FullChunk(&'a ShardChunk),
    // The validator doesn't follow the shard, and only has one part
    OnePart(&'a ChunkOnePart),
    // The chunk for particular shard is not present in the block
    NoChunk,
}

#[derive(Eq, PartialEq, Debug)]
pub enum ValidatorSignatureVerificationResult {
    Valid,
    Invalid,
    UnknownEpoch,
}

pub struct ApplyTransactionResult {
    pub trie_changes: WrappedTrieChanges,
    pub new_root: MerkleHash,
    pub transaction_results: Vec<TransactionLog>,
    pub receipt_result: ReceiptResult,
    pub validator_proposals: Vec<ValidatorStake>,
    pub total_gas_burnt: Gas,
    pub total_rent_paid: Balance,
    pub proof: Option<PartialStorage>,
}

/// Bridge between the chain and the runtime.
/// Main function is to update state given transactions.
/// Additionally handles validators and block weight computation.
pub trait RuntimeAdapter: Send + Sync {
    /// Initialize state to genesis state and returns StoreUpdate, state root and initial validators.
    /// StoreUpdate can be discarded if the chain past the genesis.
    fn genesis_state(&self) -> (StoreUpdate, Vec<MerkleHash>);

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
        gas_price: Balance,
        state_root: CryptoHash,
        transaction: SignedTransaction,
    ) -> Result<ValidTransaction, Box<dyn std::error::Error>>;

    /// Filter transactions by verifying each one by one in the given order. Every successful
    /// verification stores the updated account balances to be used by next transactions.
    fn filter_transactions(
        &self,
        block_index: BlockIndex,
        gas_price: Balance,
        state_root: CryptoHash,
        transactions: Vec<SignedTransaction>,
    ) -> Vec<SignedTransaction>;

    /// Verify validator signature for the given epoch.
    fn verify_validator_signature(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> ValidatorSignatureVerificationResult;

    /// Verify chunk header signature.
    fn verify_chunk_header_signature(&self, header: &ShardChunkHeader) -> Result<bool, Error>;

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
        gas_used: Gas,
        gas_price: Balance,
        total_supply: Balance,
    ) -> Result<(), Error>;

    /// Apply transactions to given state root and return store update and new state root.
    /// Also returns transaction result for each transaction and new receipts.
    fn apply_transactions(
        &self,
        shard_id: ShardId,
        state_root: &MerkleHash,
        block_index: BlockIndex,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        receipts: &Vec<Receipt>,
        transactions: &Vec<SignedTransaction>,
        gas_price: Balance,
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
            gas_price,
            false,
        )
    }

    fn apply_transactions_with_optional_storage_proof(
        &self,
        shard_id: ShardId,
        state_root: &MerkleHash,
        block_index: BlockIndex,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        receipts: &Vec<Receipt>,
        transactions: &Vec<SignedTransaction>,
        gas_price: Balance,
        generate_storage_proof: bool,
    ) -> Result<ApplyTransactionResult, Error>;

    /// Query runtime with given `path` and `data`.
    fn query(
        &self,
        state_root: MerkleHash,
        height: BlockIndex,
        block_hash: &CryptoHash,
        path_parts: Vec<&str>,
        data: &[u8],
    ) -> Result<QueryResponse, Box<dyn std::error::Error>>;

    /// Read state as byte array from given state root.
    fn dump_state(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>>;

    /// Set state that expected to be given state root with provided payload.
    /// Returns error if failed to parse or if the resulting tree doesn't match the expected root.
    fn set_state(
        &self,
        _shard_id: ShardId,
        state_root: MerkleHash,
        payload: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>>;

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
            receipts_hashes.push(hash(&ReceiptList(shard_receipts).try_to_vec()?));
        }
        Ok(receipts_hashes)
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
struct ReceiptList(Vec<Receipt>);

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

/// Block approval by other block producers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockApproval {
    pub hash: CryptoHash,
    pub signature: Signature,
    pub target: AccountId,
}

impl BlockApproval {
    pub fn new(hash: CryptoHash, signer: &dyn Signer, target: AccountId) -> Self {
        let signature = signer.sign(hash.as_ref());
        BlockApproval { hash, signature, target }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;

    use near_crypto::{InMemorySigner, KeyType};

    use super::*;

    #[test]
    fn test_block_produce() {
        let num_shards = 32;
        let genesis = Block::genesis(
            vec![MerkleHash::default()],
            Utc::now(),
            num_shards,
            1_000_000,
            100,
            1_000_000_000,
        );
        let signer = Arc::new(InMemorySigner::from_seed("other", KeyType::ED25519, "other"));
        let b1 = Block::empty(&genesis, signer.clone());
        assert!(signer.verify(b1.hash().as_ref(), &b1.header.signature));
        assert_eq!(b1.header.inner.total_weight.to_num(), 1);
        let other_signer =
            Arc::new(InMemorySigner::from_seed("other2", KeyType::ED25519, "other2"));
        let approvals: HashMap<usize, Signature> =
            vec![(1, other_signer.sign(b1.hash().as_ref()))].into_iter().collect();
        let b2 = Block::empty_with_approvals(
            &b1,
            2,
            b1.header.inner.epoch_id.clone(),
            approvals,
            signer.clone(),
        );
        assert!(signer.verify(b2.hash().as_ref(), &b2.header.signature));
        assert_eq!(b2.header.inner.total_weight.to_num(), 3);
    }
}
