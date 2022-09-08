use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use chrono::DateTime;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::time::Utc;
use num_rational::Rational32;

use crate::{metrics, DoomslugThresholdMode};
use near_chain_configs::{Genesis, ProtocolConfig};
use near_chain_primitives::Error;
use near_client_primitives::types::StateSplitApplyingStatus;
use near_crypto::Signature;
use near_pool::types::PoolIterator;
use near_primitives::challenge::{ChallengesResult, SlashedValidator};
use near_primitives::checked_feature;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::ShardConfig;
use near_primitives::errors::{EpochError, InvalidTxError};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::state_part::PartId;
use near_primitives::transaction::{ExecutionOutcomeWithId, SignedTransaction};
use near_primitives::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
use near_primitives::types::{
    AccountId, ApprovalStake, Balance, BlockHeight, BlockHeightDelta, EpochHeight, EpochId, Gas,
    MerkleHash, NumBlocks, ShardId, StateChangesForSplitStates, StateRoot, StateRootNode,
    ValidatorInfoIdentifier,
};
use near_primitives::version::{
    ProtocolVersion, MIN_GAS_PRICE_NEP_92, MIN_GAS_PRICE_NEP_92_FIX, MIN_PROTOCOL_VERSION_NEP_92,
    MIN_PROTOCOL_VERSION_NEP_92_FIX,
};
use near_primitives::views::{EpochValidatorInfo, QueryRequest, QueryResponse};
use near_store::{PartialStorage, ShardTries, Store, StoreUpdate, Trie, WrappedTrieChanges};

pub use near_primitives::block::{Block, BlockHeader, Tip};

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

pub struct ApplySplitStateResult {
    pub shard_uid: ShardUId,
    pub trie_changes: WrappedTrieChanges,
    pub new_root: StateRoot,
}

// This struct captures two cases
// when apply transactions, split states may or may not be ready
// if it's ready, apply transactions also apply updates to split states and this enum will be
//    ApplySplitStateResults
// otherwise, it simply returns the state changes needed to be applied to split states
pub enum ApplySplitStateResultOrStateChanges {
    ApplySplitStateResults(Vec<ApplySplitStateResult>),
    StateChangesForSplitStates(StateChangesForSplitStates),
}

pub struct ApplyTransactionResult {
    pub trie_changes: WrappedTrieChanges,
    pub new_root: StateRoot,
    pub outcomes: Vec<ExecutionOutcomeWithId>,
    pub outgoing_receipts: Vec<Receipt>,
    pub validator_proposals: Vec<ValidatorStake>,
    pub total_gas_burnt: Gas,
    pub total_balance_burnt: Balance,
    pub proof: Option<PartialStorage>,
    pub processed_delayed_receipts: Vec<Receipt>,
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
    gas_price_adjustment_rate: Rational32,
    genesis_min_gas_price: Balance,
    genesis_max_gas_price: Balance,
    genesis_protocol_version: ProtocolVersion,
}

impl BlockEconomicsConfig {
    /// Set max gas price to be this multiplier * min_gas_price
    const MAX_GAS_MULTIPLIER: u128 = 20;
    /// Compute min gas price according to protocol version and genesis protocol version.
    ///
    /// This returns the effective minimum gas price for a block with the given
    /// protocol version. The base value is defined in genesis.config but has
    /// been overwritten at specific protocol versions. Chains with a genesis
    /// version higher than those changes are not overwritten and will instead
    /// respect the value defined in genesis.
    pub fn min_gas_price(&self, protocol_version: ProtocolVersion) -> Balance {
        if self.genesis_protocol_version < MIN_PROTOCOL_VERSION_NEP_92 {
            if protocol_version >= MIN_PROTOCOL_VERSION_NEP_92_FIX {
                MIN_GAS_PRICE_NEP_92_FIX
            } else if protocol_version >= MIN_PROTOCOL_VERSION_NEP_92 {
                MIN_GAS_PRICE_NEP_92
            } else {
                self.genesis_min_gas_price
            }
        } else if self.genesis_protocol_version < MIN_PROTOCOL_VERSION_NEP_92_FIX {
            if protocol_version >= MIN_PROTOCOL_VERSION_NEP_92_FIX {
                MIN_GAS_PRICE_NEP_92_FIX
            } else {
                MIN_GAS_PRICE_NEP_92
            }
        } else {
            self.genesis_min_gas_price
        }
    }

    pub fn max_gas_price(&self, protocol_version: ProtocolVersion) -> Balance {
        if checked_feature!("stable", CapMaxGasPrice, protocol_version) {
            std::cmp::min(
                self.genesis_max_gas_price,
                Self::MAX_GAS_MULTIPLIER * self.min_gas_price(protocol_version),
            )
        } else {
            self.genesis_max_gas_price
        }
    }

    pub fn gas_price_adjustment_rate(&self, _protocol_version: ProtocolVersion) -> Rational32 {
        self.gas_price_adjustment_rate
    }
}

impl From<&ChainGenesis> for BlockEconomicsConfig {
    fn from(chain_genesis: &ChainGenesis) -> Self {
        BlockEconomicsConfig {
            gas_price_adjustment_rate: chain_genesis.gas_price_adjustment_rate,
            genesis_min_gas_price: chain_genesis.min_gas_price,
            genesis_max_gas_price: chain_genesis.max_gas_price,
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
    pub gas_price_adjustment_rate: Rational32,
    pub transaction_validity_period: NumBlocks,
    pub epoch_length: BlockHeightDelta,
    pub protocol_version: ProtocolVersion,
}

impl ChainGenesis {
    pub fn new(genesis: &Genesis) -> Self {
        Self {
            time: genesis.config.genesis_time,
            height: genesis.config.genesis_height,
            gas_limit: genesis.config.gas_limit,
            min_gas_price: genesis.config.min_gas_price,
            max_gas_price: genesis.config.max_gas_price,
            total_supply: genesis.config.total_supply,
            gas_price_adjustment_rate: genesis.config.gas_price_adjustment_rate,
            transaction_validity_period: genesis.config.transaction_validity_period,
            epoch_length: genesis.config.epoch_length,
            protocol_version: genesis.config.protocol_version,
        }
    }
}

/// Bridge between the chain and the runtime.
/// Main function is to update state given transactions.
/// Additionally handles validators.
pub trait RuntimeAdapter: Send + Sync {
    /// Get store and genesis state roots
    fn genesis_state(&self) -> (Store, Vec<StateRoot>);

    fn get_tries(&self) -> ShardTries;

    fn get_store(&self) -> Store;

    /// Returns trie. Since shard layout may change from epoch to epoch, `shard_id` itself is
    /// not enough to identify the trie. `prev_hash` is used to identify the epoch the given
    /// `shard_id` is at.
    fn get_trie_for_shard(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
        state_root: StateRoot,
    ) -> Result<Trie, Error>;

    /// Returns trie with view cache
    fn get_view_trie_for_shard(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
        state_root: StateRoot,
    ) -> Result<Trie, Error>;

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
        epoch_id: &EpochId,
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
        epoch_id: &EpochId,
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
    /// return false if the header signature does not match the key for the assigned chunk producer
    /// for this chunk, or if the chunk producer has been slashed
    /// return `Error::NotAValidator` if cannot find chunk producer info for this chunk
    /// `header`: chunk header
    /// `epoch_id`: epoch_id that the chunk header belongs to
    /// `last_known_hash`: used to determine the list of chunk producers that are slashed
    fn verify_chunk_header_signature(
        &self,
        header: &ShardChunkHeader,
        epoch_id: &EpochId,
        last_known_hash: &CryptoHash,
    ) -> Result<bool, Error> {
        self.verify_chunk_signature_with_header_parts(
            &header.chunk_hash(),
            header.signature(),
            epoch_id,
            last_known_hash,
            header.height_created(),
            header.shard_id(),
        )
    }

    fn verify_chunk_signature_with_header_parts(
        &self,
        chunk_hash: &ChunkHash,
        signature: &Signature,
        epoch_id: &EpochId,
        last_known_hash: &CryptoHash,
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

    /// Returns all the chunk producers for a given epoch.
    fn get_epoch_chunk_producers(&self, epoch_id: &EpochId) -> Result<Vec<ValidatorStake>, Error>;

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
    fn num_shards(&self, epoch_id: &EpochId) -> Result<ShardId, Error>;

    fn num_total_parts(&self) -> usize;

    fn num_data_parts(&self) -> usize;

    /// Account Id to Shard Id mapping, given current number of shards.
    fn account_id_to_shard_id(
        &self,
        account_id: &AccountId,
        epoch_id: &EpochId,
    ) -> Result<ShardId, Error>;

    /// Returns `account_id` that suppose to have the `part_id`.
    fn get_part_owner(&self, epoch_id: &EpochId, part_id: u64) -> Result<AccountId, Error>;

    fn get_shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, Error>;

    fn get_shard_config(&self, epoch_id: &EpochId) -> Result<ShardConfig, Error>;

    fn get_prev_shard_ids(
        &self,
        prev_hash: &CryptoHash,
        shard_ids: Vec<ShardId>,
    ) -> Result<Vec<ShardId>, Error>;

    /// Get shard layout given hash of previous block.
    fn get_shard_layout_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<ShardLayout, Error>;

    fn shard_id_to_uid(&self, shard_id: ShardId, epoch_id: &EpochId) -> Result<ShardUId, Error>;

    /// Returns true if the shard layout will change in the next epoch
    /// Current epoch is the epoch of the block after `parent_hash`
    fn will_shard_layout_change_next_epoch(&self, parent_hash: &CryptoHash) -> Result<bool, Error>;

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
    //  Note that `shard_id` always refers to a shard in the current epoch
    //  If shard layout will change next epoch,
    //  returns true if it cares about any shard that `shard_id` will split to
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

    /// Get epoch height given hash of previous block.
    fn get_epoch_height_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochHeight, Error>;

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
    ) -> Result<
        (
            Arc<BlockInfo>,
            Arc<BlockInfo>,
            Arc<BlockInfo>,
            Arc<EpochInfo>,
            Arc<EpochInfo>,
            Arc<EpochInfo>,
        ),
        Error,
    >;

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
        is_first_block_with_chunk_of_version: bool,
        state_patch: SandboxStatePatch,
    ) -> Result<ApplyTransactionResult, Error> {
        let _span = tracing::debug_span!(
            target: "runtime",
            "apply_transactions",
            shard_id)
        .entered();
        let _timer =
            metrics::APPLYING_CHUNKS_TIME.with_label_values(&[&shard_id.to_string()]).start_timer();
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
            is_first_block_with_chunk_of_version,
            state_patch,
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
        is_first_block_with_chunk_of_version: bool,
        state_patch: SandboxStatePatch,
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
        is_first_block_with_chunk_of_version: bool,
    ) -> Result<ApplyTransactionResult, Error>;

    /// Query runtime with given `path` and `data`.
    fn query(
        &self,
        shard_uid: ShardUId,
        state_root: &StateRoot,
        block_height: BlockHeight,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        epoch_id: &EpochId,
        request: &QueryRequest,
    ) -> Result<QueryResponse, near_chain_primitives::error::QueryError>;

    /// WARNING: this call may be expensive.
    fn get_validator_info(
        &self,
        epoch_id: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, Error>;

    /// Get the part of the state from given state root.
    /// `block_hash` is a block whose `prev_state_root` is `state_root`
    fn obtain_state_part(
        &self,
        shard_id: ShardId,
        block_hash: &CryptoHash,
        state_root: &StateRoot,
        part_id: PartId,
    ) -> Result<Vec<u8>, Error>;

    /// Validate state part that expected to be given state root with provided data.
    /// Returns false if the resulting part doesn't match the expected one.
    fn validate_state_part(&self, state_root: &StateRoot, part_id: PartId, data: &[u8]) -> bool;

    fn apply_update_to_split_states(
        &self,
        block_hash: &CryptoHash,
        state_roots: HashMap<ShardUId, StateRoot>,
        next_shard_layout: &ShardLayout,
        state_changes: StateChangesForSplitStates,
    ) -> Result<Vec<ApplySplitStateResult>, Error>;

    fn build_state_for_split_shards(
        &self,
        shard_uid: ShardUId,
        state_root: &StateRoot,
        next_epoch_shard_layout: &ShardLayout,
        state_split_status: Arc<StateSplitApplyingStatus>,
    ) -> Result<HashMap<ShardUId, StateRoot>, Error>;

    /// Should be executed after accepting all the parts to set up a new state.
    fn apply_state_part(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        part_id: PartId,
        part: &[u8],
        epoch_id: &EpochId,
    ) -> Result<(), Error>;

    /// Returns StateRootNode of a state.
    /// `block_hash` is a block whose `prev_state_root` is `state_root`
    /// Panics if requested hash is not in storage.
    /// Never returns Error
    fn get_state_root_node(
        &self,
        shard_id: ShardId,
        block_hash: &CryptoHash,
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

    fn get_protocol_config(&self, epoch_id: &EpochId) -> Result<ProtocolConfig, Error>;

    /// Get previous epoch id by hash of previous block.
    fn get_prev_epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochId, Error>;

    fn get_protocol_upgrade_block_height(
        &self,
        block_hash: CryptoHash,
    ) -> Result<Option<BlockHeight>, EpochError>;
}

/// The last known / checked height and time when we have processed it.
/// Required to keep track of skipped blocks and not fallback to produce blocks at lower height.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct LatestKnown {
    pub height: BlockHeight,
    pub seen: u64,
}

#[cfg(test)]
mod tests {
    use near_primitives::time::Utc;

    use near_crypto::KeyType;
    use near_primitives::block::{genesis_chunks, Approval};
    use near_primitives::hash::hash;
    use near_primitives::merkle::verify_path;
    use near_primitives::transaction::{ExecutionMetadata, ExecutionOutcome, ExecutionStatus};
    use near_primitives::validator_signer::InMemoryValidatorSigner;
    use near_primitives::version::PROTOCOL_VERSION;

    use crate::Chain;

    use super::*;

    #[test]
    fn test_block_produce() {
        let num_shards = 32;
        let genesis_chunks =
            genesis_chunks(vec![Trie::EMPTY_ROOT], num_shards, 1_000_000, 0, PROTOCOL_VERSION);
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
        let signer =
            InMemoryValidatorSigner::from_seed("other".parse().unwrap(), KeyType::ED25519, "other");
        let b1 = Block::empty(&genesis, &signer);
        assert!(b1.header().verify_block_producer(&signer.public_key()));
        let other_signer = InMemoryValidatorSigner::from_seed(
            "other2".parse().unwrap(),
            KeyType::ED25519,
            "other2",
        );
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
                executor_id: "alice".parse().unwrap(),
                metadata: ExecutionMetadata::V1,
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
                executor_id: "bob".parse().unwrap(),
                metadata: ExecutionMetadata::V1,
            },
        };
        let outcomes = vec![outcome1, outcome2];
        let (outcome_root, paths) = ApplyTransactionResult::compute_outcomes_proof(&outcomes);
        for (outcome_with_id, path) in outcomes.into_iter().zip(paths.into_iter()) {
            assert!(verify_path(outcome_root, &path, &outcome_with_id.to_hashes()));
        }
    }
}
