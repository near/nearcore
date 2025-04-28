use borsh::{BorshDeserialize, BorshSerialize};
use near_async::time::{Duration, Utc};
use near_chain_configs::GenesisConfig;
use near_chain_configs::MutableConfigValue;
use near_chain_configs::ProtocolConfig;
use near_chain_configs::ReshardingConfig;
use near_chain_primitives::Error;
pub use near_epoch_manager::EpochManagerAdapter;
use near_parameters::RuntimeConfig;
use near_pool::types::TransactionGroupIterator;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
pub use near_primitives::block::{Block, BlockHeader, Tip};
use near_primitives::chunk_apply_stats::ChunkApplyStatsV0;
use near_primitives::congestion_info::BlockCongestionInfo;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::congestion_info::ExtendedCongestionInfo;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{MerklePath, merklize};
use near_primitives::receipt::{PromiseYieldTimeout, Receipt};
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state_part::PartId;
use near_primitives::stateless_validation::contract_distribution::ContractUpdates;
use near_primitives::transaction::ValidatedTransaction;
use near_primitives::transaction::{ExecutionOutcomeWithId, SignedTransaction};
use near_primitives::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
use near_primitives::types::{
    Balance, BlockHeight, BlockHeightDelta, EpochId, Gas, MerkleHash, NumBlocks, ShardId,
    StateRoot, StateRootNode,
};
use near_primitives::utils::to_timestamp;
use near_primitives::version::PROD_GENESIS_PROTOCOL_VERSION;
use near_primitives::version::{MIN_GAS_PRICE_NEP_92_FIX, ProtocolVersion};
use near_primitives::views::{QueryRequest, QueryResponse};
use near_schema_checker_lib::ProtocolSchema;
use near_store::flat::FlatStorageManager;
use near_store::{PartialStorage, ShardTries, Store, Trie, WrappedTrieChanges};
use near_vm_runner::ContractCode;
use near_vm_runner::ContractRuntimeCache;
use node_runtime::SignedValidPeriodTransactions;
use num_rational::Rational32;
use tracing::instrument;

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

#[derive(Debug, Clone)]
pub struct ApplyChunkResult {
    pub trie_changes: WrappedTrieChanges,
    pub new_root: StateRoot,
    pub outcomes: Vec<ExecutionOutcomeWithId>,
    pub outgoing_receipts: Vec<Receipt>,
    pub validator_proposals: Vec<ValidatorStake>,
    pub total_gas_burnt: Gas,
    pub total_balance_burnt: Balance,
    pub proof: Option<PartialStorage>,
    pub processed_delayed_receipts: Vec<Receipt>,
    pub processed_yield_timeouts: Vec<PromiseYieldTimeout>,
    /// Hash of Vec<Receipt> which were applied in a chunk, later used for
    /// chunk validation with state witness.
    /// Note that applied receipts are not necessarily executed as they can
    /// be delayed.
    pub applied_receipts_hash: CryptoHash,
    /// The congestion info of the shard after applying the chunk. This field
    /// should be set to None for chunks before the CongestionControl protocol
    /// version and Some otherwise.
    pub congestion_info: Option<CongestionInfo>,
    /// Requests for bandwidth to send receipts to other shards.
    pub bandwidth_requests: BandwidthRequests,
    /// Used only for a sanity check.
    pub bandwidth_scheduler_state_hash: CryptoHash,
    /// Contracts accessed and deployed while applying the chunk.
    pub contract_updates: ContractUpdates,
    /// Extra information gathered during chunk application.
    pub stats: ChunkApplyStatsV0,
}

impl ApplyChunkResult {
    /// Returns root and paths for all the outcomes in the result.
    #[instrument(target = "runtime", level = "debug", "compute_outcomes_proof", skip_all, fields(
        num_outcomes = outcomes.len()
    ))]
    pub fn compute_outcomes_proof(
        outcomes: &[ExecutionOutcomeWithId],
    ) -> (MerkleHash, Vec<MerklePath>) {
        let mut result = Vec::with_capacity(outcomes.len());
        for outcome_with_id in outcomes {
            result.push(outcome_with_id.to_hashes());
        }
        merklize(&result)
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
    pub fn min_gas_price(&self) -> Balance {
        if self.genesis_protocol_version == PROD_GENESIS_PROTOCOL_VERSION {
            MIN_GAS_PRICE_NEP_92_FIX
        } else {
            self.genesis_min_gas_price
        }
    }

    pub fn max_gas_price(&self) -> Balance {
        std::cmp::min(self.genesis_max_gas_price, Self::MAX_GAS_MULTIPLIER * self.min_gas_price())
    }

    pub fn gas_price_adjustment_rate(&self) -> Rational32 {
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
    pub time: Utc,
    pub height: BlockHeight,
    pub gas_limit: Gas,
    pub min_gas_price: Balance,
    pub max_gas_price: Balance,
    pub total_supply: Balance,
    pub gas_price_adjustment_rate: Rational32,
    pub transaction_validity_period: NumBlocks,
    pub epoch_length: BlockHeightDelta,
    pub protocol_version: ProtocolVersion,
    pub chain_id: String,
}

#[derive(Clone)]
pub struct ChainConfig {
    /// Whether to save `TrieChanges` on disk or not.
    pub save_trie_changes: bool,
    /// Number of threads to execute background migration work.
    /// Currently used for flat storage background creation.
    pub background_migration_threads: usize,
    /// The resharding configuration.
    pub resharding_config: MutableConfigValue<ReshardingConfig>,
}

impl ChainConfig {
    pub fn test() -> Self {
        Self {
            save_trie_changes: true,
            background_migration_threads: 1,
            resharding_config: MutableConfigValue::new(
                ReshardingConfig::default(),
                "resharding_config",
            ),
        }
    }
}

impl ChainGenesis {
    pub fn new(genesis_config: &GenesisConfig) -> Self {
        Self {
            time: Utc::from_unix_timestamp_nanos(to_timestamp(genesis_config.genesis_time) as i128)
                .unwrap(),
            height: genesis_config.genesis_height,
            gas_limit: genesis_config.gas_limit,
            min_gas_price: genesis_config.min_gas_price,
            max_gas_price: genesis_config.max_gas_price,
            total_supply: genesis_config.total_supply,
            gas_price_adjustment_rate: genesis_config.gas_price_adjustment_rate,
            transaction_validity_period: genesis_config.transaction_validity_period,
            epoch_length: genesis_config.epoch_length,
            protocol_version: genesis_config.protocol_version,
            chain_id: genesis_config.chain_id.clone(),
        }
    }
}

pub enum StorageDataSource {
    /// Full state data is present in DB.
    Db,
    /// Trie is present in DB and flat storage is not.
    /// Used to reply past blocks and simulate gas costs as if flat storage
    /// was present.
    /// WARNING: do not use this variant in production!
    DbTrieOnly,
    /// State data is supplied from state witness, there is no state data
    /// stored on disk.
    Recorded(PartialStorage),
}

pub struct RuntimeStorageConfig {
    pub state_root: StateRoot,
    pub use_flat_storage: bool,
    pub source: StorageDataSource,
    pub state_patch: SandboxStatePatch,
}

impl RuntimeStorageConfig {
    pub fn new(state_root: StateRoot, use_flat_storage: bool) -> Self {
        Self {
            state_root,
            use_flat_storage,
            source: StorageDataSource::Db,
            state_patch: Default::default(),
        }
    }

    /// Creates a [RuntimeStorageConfig] with [StorageDataSource::DbTrieOnly].
    /// Flat storage is disabled because it is implied to be missing.
    ///
    /// This's meant to be used only to replay blocks.
    pub fn new_with_db_trie_only(state_root: StateRoot) -> Self {
        Self {
            state_root,
            use_flat_storage: false,
            source: StorageDataSource::DbTrieOnly,
            state_patch: Default::default(),
        }
    }
}

#[derive(Clone)]
pub struct ApplyChunkBlockContext {
    pub height: BlockHeight,
    pub block_hash: CryptoHash,
    pub prev_block_hash: CryptoHash,
    pub block_timestamp: u64,
    pub gas_price: Balance,
    pub random_seed: CryptoHash,
    pub congestion_info: BlockCongestionInfo,
    pub bandwidth_requests: BlockBandwidthRequests,
}

impl ApplyChunkBlockContext {
    pub fn from_header(
        header: &BlockHeader,
        gas_price: Balance,
        congestion_info: BlockCongestionInfo,
        bandwidth_requests: BlockBandwidthRequests,
    ) -> Self {
        Self {
            height: header.height(),
            block_hash: *header.hash(),
            prev_block_hash: *header.prev_hash(),
            block_timestamp: header.raw_timestamp(),
            gas_price,
            random_seed: *header.random_value(),
            congestion_info,
            bandwidth_requests,
        }
    }
}

pub struct ApplyChunkShardContext<'a> {
    pub shard_id: ShardId,
    pub last_validator_proposals: ValidatorStakeIter<'a>,
    pub gas_limit: Gas,
    pub is_new_chunk: bool,
}

/// Contains transactions that were fetched from the transaction pool
/// and prepared for adding them to a new chunk that is being produced.
#[derive(Debug, Clone)]
pub struct PreparedTransactions {
    /// Prepared transactions
    pub transactions: Vec<ValidatedTransaction>,
    /// Describes which limit was hit when preparing the transactions.
    pub limited_by: Option<PrepareTransactionsLimit>,
}

/// Chunk producer prepares transactions from the transaction pool
/// until it hits some limit (too many transactions, too much gas used, etc).
/// This enum describes which limit was hit when preparing transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr)]
pub enum PrepareTransactionsLimit {
    Gas,
    Size,
    Time,
    ReceiptCount,
    StorageProofSize,
}

pub struct PrepareTransactionsBlockContext {
    pub next_gas_price: Balance,
    pub height: BlockHeight,
    pub block_hash: CryptoHash,
    pub congestion_info: BlockCongestionInfo,
}

impl From<&Block> for PrepareTransactionsBlockContext {
    fn from(block: &Block) -> Self {
        let header = block.header();
        Self {
            next_gas_price: header.next_gas_price(),
            height: header.height(),
            block_hash: *header.hash(),
            congestion_info: block.block_congestion_info(),
        }
    }
}
pub struct PrepareTransactionsChunkContext {
    pub shard_id: ShardId,
    pub gas_limit: Gas,
}

/// Bridge between the chain and the runtime.
/// Main function is to update state given transactions.
/// Additionally handles validators.
/// Naming note: `state_root` is a pre state root for block `block_hash` and a
/// post state root for block `prev_hash`.
pub trait RuntimeAdapter: Send + Sync {
    fn get_tries(&self) -> ShardTries;

    fn store(&self) -> &Store;

    /// Returns trie with non-view cache for given `state_root`.
    /// `prev_hash` is a block whose post state root is `state_root`, used to
    /// access flat storage and to identify the epoch the given `shard_id` is at.
    fn get_trie_for_shard(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
        state_root: StateRoot,
        use_flat_storage: bool,
    ) -> Result<Trie, Error>;

    /// Same as `get_trie_for_shard` but returns trie with view cache.
    fn get_view_trie_for_shard(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
        state_root: StateRoot,
    ) -> Result<Trie, Error>;

    fn get_flat_storage_manager(&self) -> FlatStorageManager;

    fn get_shard_layout(&self, protocol_version: ProtocolVersion) -> ShardLayout;

    #[allow(clippy::result_large_err)]
    fn validate_tx(
        &self,
        shard_layout: &ShardLayout,
        signed_tx: SignedTransaction,
        current_protocol_version: ProtocolVersion,
        receiver_congestion_info: Option<ExtendedCongestionInfo>,
    ) -> Result<ValidatedTransaction, (InvalidTxError, SignedTransaction)>;

    fn can_verify_and_charge_tx(
        &self,
        shard_layout: &ShardLayout,
        gas_price: Balance,
        state_root: StateRoot,
        validated_tx: &ValidatedTransaction,
        current_protocol_version: ProtocolVersion,
    ) -> Result<(), InvalidTxError>;

    /// Returns an ordered list of valid transactions from the pool up the given limits.
    /// Pulls transactions from the given pool iterators one by one. Validates each transaction
    /// against the given `chain_validate` closure and runtime's transaction verifier.
    /// If the transaction is valid for both, it's added to the result and the temporary state
    /// update is preserved for validation of next transactions.
    /// Throws an `Error` with `ErrorKind::StorageError` in case the runtime throws
    /// `RuntimeError::StorageError`.
    fn prepare_transactions(
        &self,
        storage: RuntimeStorageConfig,
        chunk: PrepareTransactionsChunkContext,
        prev_block: PrepareTransactionsBlockContext,
        transaction_groups: &mut dyn TransactionGroupIterator,
        chain_validate: &dyn Fn(&SignedTransaction) -> bool,
        time_limit: Option<Duration>,
    ) -> Result<PreparedTransactions, Error>;

    /// Returns true if the shard layout will change in the next epoch
    /// Current epoch is the epoch of the block after `parent_hash`
    fn will_shard_layout_change_next_epoch(&self, parent_hash: &CryptoHash) -> Result<bool, Error>;

    /// Get the block height for which garbage collection should not go over
    fn get_gc_stop_height(&self, block_hash: &CryptoHash) -> BlockHeight;

    /// Apply transactions and receipts to given state root and return store update
    /// and new state root.
    /// Also returns transaction result for each transaction and new receipts.
    fn apply_chunk(
        &self,
        storage: RuntimeStorageConfig,
        apply_reason: ApplyChunkReason,
        chunk: ApplyChunkShardContext,
        block: ApplyChunkBlockContext,
        receipts: &[Receipt],
        transactions: SignedValidPeriodTransactions,
    ) -> Result<ApplyChunkResult, Error>;

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

    /// Get part of the state corresponding to the given state root.
    /// `prev_hash` is a block whose post state root is `state_root`.
    /// Returns error when storage is inconsistent.
    fn obtain_state_part(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
        state_root: &StateRoot,
        part_id: PartId,
    ) -> Result<Vec<u8>, Error>;

    /// Validate state part that expected to be given state root with provided data.
    /// Returns false if the resulting part doesn't match the expected one.
    fn validate_state_part(&self, state_root: &StateRoot, part_id: PartId, data: &[u8]) -> bool;

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

    fn get_protocol_config(&self, epoch_id: &EpochId) -> Result<ProtocolConfig, Error>;

    fn get_runtime_config(&self, protocol_version: ProtocolVersion) -> &RuntimeConfig;

    fn compiled_contract_cache(&self) -> &dyn ContractRuntimeCache;

    /// Precompiles the contracts and stores them in the compiled contract cache.
    fn precompile_contracts(
        &self,
        epoch_id: &EpochId,
        contract_codes: Vec<ContractCode>,
    ) -> Result<(), Error>;
}

/// The last known / checked height and time when we have processed it.
/// Required to keep track of skipped blocks and not fallback to produce blocks at lower height.
#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, Copy, Default, serde::Serialize, ProtocolSchema,
)]
pub struct LatestKnown {
    pub height: BlockHeight,
    pub seen: u64,
}

#[cfg(test)]
mod tests {
    use near_async::time::{Clock, Utc};
    use near_primitives::block::Approval;
    use near_primitives::genesis::{genesis_block, genesis_chunks};
    use near_primitives::hash::hash;
    use near_primitives::merkle::verify_path;
    use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
    use near_primitives::transaction::{ExecutionMetadata, ExecutionOutcome, ExecutionStatus};
    use near_primitives::version::PROTOCOL_VERSION;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_block_produce() {
        let shard_ids: Vec<_> = (0..32).map(ShardId::new).collect();
        let genesis_chunks = genesis_chunks(
            vec![Trie::EMPTY_ROOT],
            vec![Default::default(); shard_ids.len()],
            &shard_ids,
            1_000_000,
            0,
            PROTOCOL_VERSION,
        );
        let genesis_bps: Vec<ValidatorStake> = Vec::new();
        let genesis = genesis_block(
            PROTOCOL_VERSION,
            genesis_chunks.into_iter().map(|chunk| chunk.take_header()).collect(),
            Utc::now_utc(),
            0,
            100,
            1_000_000_000,
            &genesis_bps,
        );
        let signer = Arc::new(create_test_signer("other"));
        let b1 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).build();
        assert!(b1.header().verify_block_producer(&signer.public_key()));
        let other_signer = create_test_signer("other2");
        let approvals =
            vec![Some(Box::new(Approval::new(*b1.hash(), 1, 2, &other_signer).signature))];
        let b2 =
            TestBlockBuilder::new(Clock::real(), &b1, signer.clone()).approvals(approvals).build();
        b2.header().verify_block_producer(&signer.public_key());
    }

    #[test]
    fn test_execution_outcome_merkelization() {
        let outcome1 = ExecutionOutcomeWithId {
            id: Default::default(),
            outcome: ExecutionOutcome {
                status: ExecutionStatus::Unknown,
                logs: vec!["outcome1".to_string()],
                receipt_ids: vec![hash(&[1])],
                gas_burnt: 100,
                compute_usage: Some(200),
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
                compute_usage: Some(0),
                tokens_burnt: 0,
                executor_id: "bob".parse().unwrap(),
                metadata: ExecutionMetadata::V1,
            },
        };
        let outcomes = vec![outcome1, outcome2];
        let (outcome_root, paths) = ApplyChunkResult::compute_outcomes_proof(&outcomes);
        for (outcome_with_id, path) in outcomes.into_iter().zip(paths.into_iter()) {
            assert!(verify_path(outcome_root, &path, &outcome_with_id.to_hashes()));
        }
    }
}
