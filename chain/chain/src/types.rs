use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use num_rational::Rational;
use serde::Serialize;

use near_chain_configs::{GenesisConfig, ProtocolConfig};
use near_chain_primitives::Error;
use near_crypto::Signature;
use near_pool::types::PoolIterator;
pub use near_primitives::block::{Block, BlockHeader, Tip};
use near_primitives::challenge::{ChallengesResult, SlashedValidator};
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{merklize, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkHash, ReceiptList, ShardChunkHeader};
use near_primitives::transaction::{ExecutionOutcomeWithId, SignedTransaction};
use near_primitives::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
use near_primitives::types::{
    AccountId, ApprovalStake, Balance, BlockHeight, BlockHeightDelta, EpochId, Gas, MerkleHash,
    NumBlocks, ShardId, StateRoot, StateRootNode,
};
use near_primitives::version::{
    ProtocolVersion, MIN_GAS_PRICE_NEP_92, MIN_GAS_PRICE_NEP_92_FIX, MIN_PROTOCOL_VERSION_NEP_92,
    MIN_PROTOCOL_VERSION_NEP_92_FIX,
};
use near_primitives::views::{EpochValidatorInfo, QueryRequest, QueryResponse};
use near_store::{PartialStorage, ShardTries, Store, StoreUpdate, Trie, WrappedTrieChanges};

#[cfg(feature = "protocol_feature_block_header_v3")]
use crate::DoomslugThresholdMode;

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

/// Compressed information about block.
/// Useful for epoch manager.
#[derive(Default, Clone, Debug)]
pub struct BlockHeaderInfo {
    pub hash: CryptoHash,
    pub prev_hash: CryptoHash,
    pub height: BlockHeight,
    pub random_value: CryptoHash,
    pub last_finalized_height: BlockHeight,
    pub last_finalized_block_hash: CryptoHash,
    pub proposals: Vec<ValidatorStake>,
    pub slashed_validators: Vec<SlashedValidator>,
    pub chunk_mask: Vec<bool>,
    pub total_supply: Balance,
    pub latest_protocol_version: ProtocolVersion,
    pub timestamp_nanosec: u64,
}

impl BlockHeaderInfo {
    pub fn new(header: &BlockHeader, last_finalized_height: u64) -> Self {
        Self {
            hash: *header.hash(),
            prev_hash: *header.prev_hash(),
            height: header.height(),
            random_value: *header.random_value(),
            last_finalized_height,
            last_finalized_block_hash: *header.last_final_block(),
            proposals: header.validator_proposals().collect(),
            slashed_validators: vec![],
            chunk_mask: header.chunk_mask().to_vec(),
            total_supply: header.total_supply(),
            latest_protocol_version: header.latest_protocol_version(),
            timestamp_nanosec: header.raw_timestamp(),
        }
    }
}

/// Block economics config taken from genesis config
pub struct BlockEconomicsConfig {
    gas_price_adjustment_rate: Rational,
    min_gas_price: Balance,
    max_gas_price: Balance,
    genesis_protocol_version: ProtocolVersion,
}

impl BlockEconomicsConfig {
    /// Compute min gas price according to protocol version and genesis protocol version.
    pub fn min_gas_price(&self, protocol_version: ProtocolVersion) -> Balance {
        if self.genesis_protocol_version < MIN_PROTOCOL_VERSION_NEP_92 {
            if protocol_version >= MIN_PROTOCOL_VERSION_NEP_92_FIX {
                MIN_GAS_PRICE_NEP_92_FIX
            } else if protocol_version >= MIN_PROTOCOL_VERSION_NEP_92 {
                MIN_GAS_PRICE_NEP_92
            } else {
                self.min_gas_price
            }
        } else if self.genesis_protocol_version < MIN_PROTOCOL_VERSION_NEP_92_FIX {
            if protocol_version >= MIN_PROTOCOL_VERSION_NEP_92_FIX {
                MIN_GAS_PRICE_NEP_92_FIX
            } else {
                MIN_GAS_PRICE_NEP_92
            }
        } else {
            self.min_gas_price
        }
    }

    pub fn max_gas_price(&self, _protocol_version: ProtocolVersion) -> Balance {
        self.max_gas_price
    }

    pub fn gas_price_adjustment_rate(&self, _protocol_version: ProtocolVersion) -> Rational {
        self.gas_price_adjustment_rate
    }
}

impl From<&ChainGenesis> for BlockEconomicsConfig {
    fn from(chain_genesis: &ChainGenesis) -> Self {
        BlockEconomicsConfig {
            gas_price_adjustment_rate: chain_genesis.gas_price_adjustment_rate,
            min_gas_price: chain_genesis.min_gas_price,
            max_gas_price: chain_genesis.max_gas_price,
            genesis_protocol_version: chain_genesis.protocol_version,
        }
    }
}

/// Chain genesis configuration.
#[derive(Clone)]
pub struct ChainGenesis {
    pub time: DateTime<Utc>,
    pub height: BlockHeight,
    pub gas_limit: Gas,
    pub min_gas_price: Balance,
    pub max_gas_price: Balance,
    pub total_supply: Balance,
    pub gas_price_adjustment_rate: Rational,
    pub transaction_validity_period: NumBlocks,
    pub epoch_length: BlockHeightDelta,
    pub protocol_version: ProtocolVersion,
}

impl<T> From<T> for ChainGenesis
where
    T: AsRef<GenesisConfig>,
{
    fn from(genesis_config: T) -> Self {
        let genesis_config = genesis_config.as_ref();
        Self {
            time: genesis_config.genesis_time,
            height: genesis_config.genesis_height,
            gas_limit: genesis_config.gas_limit,
            min_gas_price: genesis_config.min_gas_price,
            max_gas_price: genesis_config.max_gas_price,
            total_supply: genesis_config.total_supply,
            gas_price_adjustment_rate: genesis_config.gas_price_adjustment_rate,
            transaction_validity_period: genesis_config.transaction_validity_period,
            epoch_length: genesis_config.epoch_length,
            protocol_version: genesis_config.protocol_version,
        }
    }
}

/// Bridge between the chain and the runtime.
/// Main function is to update state given transactions.
/// Additionally handles validators.
pub trait RuntimeAdapter: Send + Sync {
    /// Get store and genesis state roots
    fn genesis_state(&self) -> (Arc<Store>, Vec<StateRoot>);

    fn get_tries(&self) -> ShardTries;

    /// Returns trie.
    fn get_trie_for_shard(&self, shard_id: ShardId) -> Trie;

    /// Returns trie with view cache
    fn get_view_trie_for_shard(&self, shard_id: ShardId) -> Trie;

    fn verify_block_vrf(
        &self,
        epoch_id: &EpochId,
        block_height: BlockHeight,
        prev_random_value: &CryptoHash,
        vrf_value: &near_crypto::vrf::Value,
        vrf_proof: &near_crypto::vrf::Proof,
    ) -> Result<(), Error>;

    /// Validates a given signed transaction.
    /// If the state root is given, then the verification will use the account. Otherwise it will
    /// only validate the transaction math, limits and signatures.
    /// Returns an option of `InvalidTxError`, it contains `Some(InvalidTxError)` if there is
    /// a validation error, or `None` in case the transaction succeeded.
    /// Throws an `Error` with `ErrorKind::StorageError` in case the runtime throws
    /// `RuntimeError::StorageError`.
    fn validate_tx(
        &self,
        gas_price: Balance,
        state_root: Option<StateRoot>,
        transaction: &SignedTransaction,
        verify_signature: bool,
        current_protocol_version: ProtocolVersion,
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
        next_block_height: BlockHeight,
        pool_iterator: &mut dyn PoolIterator,
        chain_validate: &mut dyn FnMut(&SignedTransaction) -> bool,
        current_protocol_version: ProtocolVersion,
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
    fn verify_chunk_header_signature(&self, header: &ShardChunkHeader) -> Result<bool, Error> {
        self.verify_chunk_signature_with_header_parts(
            &header.chunk_hash(),
            header.signature(),
            &header.prev_block_hash(),
            header.height_created(),
            header.shard_id(),
        )
    }

    fn verify_chunk_signature_with_header_parts(
        &self,
        chunk_hash: &ChunkHash,
        signature: &Signature,
        prev_block_hash: &CryptoHash,
        height_created: BlockHeight,
        shard_id: ShardId,
    ) -> Result<bool, Error>;

    /// Verify aggregated bls signature
    fn verify_approval(
        &self,
        prev_block_hash: &CryptoHash,
        prev_block_height: BlockHeight,
        block_height: BlockHeight,
        approvals: &[Option<Signature>],
    ) -> Result<bool, Error>;

    /// Verify approvals and check threshold, but ignore next epoch approvals and slashing
    #[cfg(feature = "protocol_feature_block_header_v3")]
    fn verify_approvals_and_threshold_orphan(
        &self,
        epoch_id: &EpochId,
        doomslug_threshold_mode: DoomslugThresholdMode,
        prev_block_hash: &CryptoHash,
        prev_block_height: BlockHeight,
        block_height: BlockHeight,
        approvals: &[Option<Signature>],
    ) -> Result<(), Error>;

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
    ) -> Result<Vec<(ApprovalStake, bool)>, Error>;

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
    fn get_gc_stop_height(&self, block_hash: &CryptoHash) -> BlockHeight;

    /// Check if epoch exists.
    fn epoch_exists(&self, epoch_id: &EpochId) -> bool;

    /// Amount of tokens minted in given epoch.
    fn get_epoch_minted_amount(&self, epoch_id: &EpochId) -> Result<Balance, Error>;

    // TODO #3488 this likely to be updated
    /// Data that is necessary for prove Epochs in Epoch Sync.
    fn get_epoch_sync_data(
        &self,
        prev_epoch_last_block_hash: &CryptoHash,
        epoch_id: &EpochId,
        next_epoch_id: &EpochId,
    ) -> Result<(BlockInfo, BlockInfo, BlockInfo, EpochInfo, EpochInfo, EpochInfo), Error>;

    // TODO #3488 this likely to be updated
    /// Hash that is necessary for prove Epochs in Epoch Sync.
    fn get_epoch_sync_data_hash(
        &self,
        prev_epoch_last_block_hash: &CryptoHash,
        epoch_id: &EpochId,
        next_epoch_id: &EpochId,
    ) -> Result<CryptoHash, Error>;

    /// Epoch active protocol version.
    fn get_epoch_protocol_version(&self, epoch_id: &EpochId) -> Result<ProtocolVersion, Error>;

    /// Epoch Manager init procedure that is necessary after Epoch Sync.
    fn epoch_sync_init_epoch_manager(
        &self,
        prev_epoch_first_block_info: BlockInfo,
        prev_epoch_prev_last_block_info: BlockInfo,
        prev_epoch_last_block_info: BlockInfo,
        prev_epoch_id: &EpochId,
        prev_epoch_info: EpochInfo,
        epoch_id: &EpochId,
        epoch_info: EpochInfo,
        next_epoch_id: &EpochId,
        next_epoch_info: EpochInfo,
    ) -> Result<(), Error>;

    /// Add proposals for validators.
    fn add_validator_proposals(
        &self,
        block_header_info: BlockHeaderInfo,
    ) -> Result<StoreUpdate, Error>;

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
        last_validator_proposals: ValidatorStakeIter,
        gas_price: Balance,
        gas_limit: Gas,
        challenges_result: &ChallengesResult,
        random_seed: CryptoHash,
        is_new_chunk: bool,
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
            random_seed,
            false,
            is_new_chunk,
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
        last_validator_proposals: ValidatorStakeIter,
        gas_price: Balance,
        gas_limit: Gas,
        challenges_result: &ChallengesResult,
        random_seed: CryptoHash,
        generate_storage_proof: bool,
        is_new_chunk: bool,
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
        last_validator_proposals: ValidatorStakeIter,
        gas_price: Balance,
        gas_limit: Gas,
        challenges_result: &ChallengesResult,
        random_value: CryptoHash,
        is_new_chunk: bool,
    ) -> Result<ApplyTransactionResult, Error>;

    /// Query runtime with given `path` and `data`.
    fn query(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        block_height: BlockHeight,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        epoch_id: &EpochId,
        request: &QueryRequest,
    ) -> Result<QueryResponse, near_chain_primitives::error::QueryError>;

    fn get_validator_info(
        &self,
        epoch_id: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, Error>;

    /// Get the part of the state from given state root.
    fn obtain_state_part(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        part_id: u64,
        num_parts: u64,
    ) -> Result<Vec<u8>, Error>;

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
    fn apply_state_part(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        part_id: u64,
        num_parts: u64,
        part: &[u8],
    ) -> Result<(), Error>;

    /// Returns StateRootNode of a state.
    /// Panics if requested hash is not in storage.
    /// Never returns Error
    fn get_state_root_node(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
    ) -> Result<StateRootNode, Error>;

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

    fn chunk_needs_to_be_fetched_from_archival(
        &self,
        chunk_prev_block_hash: &CryptoHash,
        header_head: &CryptoHash,
    ) -> Result<bool, Error>;

    #[cfg(feature = "protocol_feature_evm")]
    fn evm_chain_id(&self) -> u64;

    fn get_protocol_config(&self, epoch_id: &EpochId) -> Result<ProtocolConfig, Error>;

    /// Build receipts hashes.
    // Due to borsh serialization constraints, we have to use `&Vec<Receipt>` instead of `&[Receipt]`
    // here.
    fn build_receipts_hashes(&self, receipts: &Vec<Receipt>) -> Vec<CryptoHash> {
        if self.num_shards() == 1 {
            return vec![hash(&ReceiptList(0, receipts).try_to_vec().unwrap())];
        }
        let mut account_id_to_shard_id = HashMap::new();
        let mut shard_receipts: Vec<_> = (0..self.num_shards()).map(|i| (i, Vec::new())).collect();
        for receipt in receipts.iter() {
            let shard_id = match account_id_to_shard_id.get(&receipt.receiver_id) {
                Some(id) => *id,
                None => {
                    let id = self.account_id_to_shard_id(&receipt.receiver_id);
                    account_id_to_shard_id.insert(receipt.receiver_id.clone(), id);
                    id
                }
            };
            shard_receipts[shard_id as usize].1.push(receipt);
        }
        shard_receipts
            .into_iter()
            .map(|(i, rs)| {
                let bytes = (i, rs).try_to_vec().unwrap();
                hash(&bytes)
            })
            .collect()
    }
}

/// The last known / checked height and time when we have processed it.
/// Required to keep track of skipped blocks and not fallback to produce blocks at lower height.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Default)]
pub struct LatestKnown {
    pub height: BlockHeight,
    pub seen: u64,
}

/// Either an epoch id or latest block hash
#[derive(Debug)]
pub enum ValidatorInfoIdentifier {
    EpochId(EpochId),
    BlockHash(CryptoHash),
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use near_crypto::KeyType;
    use near_primitives::block::{genesis_chunks, Approval};
    use near_primitives::merkle::verify_path;
    use near_primitives::transaction::{ExecutionOutcome, ExecutionStatus};
    use near_primitives::validator_signer::InMemoryValidatorSigner;
    use near_primitives::version::PROTOCOL_VERSION;

    use crate::Chain;

    use super::*;

    #[test]
    fn test_block_produce() {
        let num_shards = 32;
        let genesis_chunks =
            genesis_chunks(vec![StateRoot::default()], num_shards, 1_000_000, 0, PROTOCOL_VERSION);
        let genesis_bps: Vec<ValidatorStake> = Vec::new();
        let genesis = Block::genesis(
            PROTOCOL_VERSION,
            genesis_chunks.into_iter().map(|chunk| chunk.take_header()).collect(),
            Utc::now(),
            0,
            100,
            1_000_000_000,
            Chain::compute_collection_hash(genesis_bps).unwrap(),
        );
        let signer = InMemoryValidatorSigner::from_seed("other", KeyType::ED25519, "other");
        let b1 = Block::empty(&genesis, &signer);
        assert!(b1.header().verify_block_producer(&signer.public_key()));
        let other_signer = InMemoryValidatorSigner::from_seed("other2", KeyType::ED25519, "other2");
        let approvals = vec![Some(Approval::new(*b1.hash(), 1, 2, &other_signer).signature)];
        let b2 = Block::empty_with_approvals(
            &b1,
            2,
            b1.header().epoch_id().clone(),
            EpochId(*genesis.hash()),
            approvals,
            &signer,
            *genesis.header().next_bp_hash(),
            CryptoHash::default(),
        );
        b2.header().verify_block_producer(&signer.public_key());
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
                tokens_burnt: 10000,
                executor_id: "alice".to_string(),
            },
        };
        let outcome2 = ExecutionOutcomeWithId {
            id: Default::default(),
            outcome: ExecutionOutcome {
                status: ExecutionStatus::SuccessValue(vec![1]),
                logs: vec!["outcome2".to_string()],
                receipt_ids: vec![],
                gas_burnt: 0,
                tokens_burnt: 0,
                executor_id: "bob".to_string(),
            },
        };
        let outcomes = vec![outcome1, outcome2];
        let (outcome_root, paths) = ApplyTransactionResult::compute_outcomes_proof(&outcomes);
        for (outcome_with_id, path) in outcomes.into_iter().zip(paths.into_iter()) {
            assert!(verify_path(outcome_root, &path, &outcome_with_id.to_hashes()));
        }
    }
}
