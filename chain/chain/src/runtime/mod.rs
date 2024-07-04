use crate::types::{
    ApplyChunkBlockContext, ApplyChunkResult, ApplyChunkShardContext, ApplyResultForResharding,
    PrepareTransactionsBlockContext, PrepareTransactionsChunkContext, PrepareTransactionsLimit,
    PreparedTransactions, RuntimeAdapter, RuntimeStorageConfig, StorageDataSource, Tip,
};
use crate::Error;
use borsh::BorshDeserialize;
use errors::FromStateViewerErrors;
use near_async::time::{Duration, Instant};
use near_chain_configs::{
    GenesisConfig, ProtocolConfig, DEFAULT_GC_NUM_EPOCHS_TO_KEEP, MIN_GC_NUM_EPOCHS_TO_KEEP,
};
use near_crypto::PublicKey;
use near_epoch_manager::{EpochManagerAdapter, EpochManagerHandle};
use near_parameters::{ActionCosts, ExtCosts, RuntimeConfig, RuntimeConfigStore};
use near_pool::types::TransactionGroupIterator;
use near_primitives::account::{AccessKey, Account};
use near_primitives::apply::ApplyChunkReason;
use near_primitives::checked_feature;
use near_primitives::congestion_info::{
    CongestionControl, ExtendedCongestionInfo, RejectTransactionReason, ShardAcceptsTransactions,
};
use near_primitives::errors::{InvalidTxError, RuntimeError, StorageError};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::{DelayedReceiptIndices, Receipt};
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::shard_layout::{
    account_id_to_shard_id, account_id_to_shard_uid, ShardLayout, ShardUId,
};
use near_primitives::state_part::PartId;
use near_primitives::transaction::SignedTransaction;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, EpochHeight, EpochId, EpochInfoProvider, Gas, MerkleHash,
    ShardId, StateChangeCause, StateChangesForResharding, StateRoot, StateRootNode,
};
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_primitives::views::{
    AccessKeyInfoView, CallResult, ContractCodeView, QueryRequest, QueryResponse,
    QueryResponseKind, ViewApplyState, ViewStateResult,
};
use near_store::config::StateSnapshotType;
use near_store::flat::FlatStorageManager;
use near_store::metadata::DbKind;
use near_store::{
    ApplyStatePartResult, DBCol, ShardTries, StateSnapshotConfig, Store, Trie, TrieConfig,
    TrieUpdate, WrappedTrieChanges, COLD_HEAD_KEY,
};
use near_vm_runner::ContractCode;
use near_vm_runner::{precompile_contract, ContractRuntimeCache, FilesystemContractRuntimeCache};
use node_runtime::adapter::ViewRuntimeAdapter;
use node_runtime::state_viewer::TrieViewer;
use node_runtime::{
    validate_transaction, verify_and_charge_transaction, ApplyState, Runtime,
    ValidatorAccountsUpdate,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, error, info, instrument};

pub mod errors;
mod metrics;
pub mod migrations;
#[cfg(test)]
mod tests;

/// Defines Nightshade state transition and validator rotation.
/// TODO: this possibly should be merged with the runtime cargo or at least reconciled on the interfaces.
pub struct NightshadeRuntime {
    genesis_config: GenesisConfig,
    runtime_config_store: RuntimeConfigStore,

    store: Store,
    compiled_contract_cache: Box<dyn ContractRuntimeCache>,
    tries: ShardTries,
    trie_viewer: TrieViewer,
    pub runtime: Runtime,
    epoch_manager: Arc<EpochManagerHandle>,
    migration_data: Arc<MigrationData>,
    gc_num_epochs_to_keep: u64,
}

impl NightshadeRuntime {
    pub fn new(
        store: Store,
        compiled_contract_cache: Box<dyn ContractRuntimeCache>,
        genesis_config: &GenesisConfig,
        epoch_manager: Arc<EpochManagerHandle>,
        trie_viewer_state_size_limit: Option<u64>,
        max_gas_burnt_view: Option<Gas>,
        runtime_config_store: Option<RuntimeConfigStore>,
        gc_num_epochs_to_keep: u64,
        trie_config: TrieConfig,
        state_snapshot_config: StateSnapshotConfig,
    ) -> Arc<Self> {
        let runtime_config_store = match runtime_config_store {
            Some(store) => store,
            None => RuntimeConfigStore::for_chain_id(&genesis_config.chain_id),
        };

        let runtime = Runtime::new();
        let trie_viewer = TrieViewer::new(trie_viewer_state_size_limit, max_gas_burnt_view);
        let flat_storage_manager = FlatStorageManager::new(store.clone());
        let shard_uids: Vec<_> = genesis_config.shard_layout.shard_uids().collect();
        let tries = ShardTries::new(
            store.clone(),
            trie_config,
            &shard_uids,
            flat_storage_manager,
            state_snapshot_config,
        );
        if let Err(err) = tries.maybe_open_state_snapshot(|prev_block_hash: CryptoHash| {
            let epoch_manager = epoch_manager.read();
            let epoch_id = epoch_manager.get_epoch_id(&prev_block_hash)?;
            let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
            Ok(shard_layout.shard_uids().collect())
        }) {
            tracing::debug!(target: "runtime", ?err, "The state snapshot is not available.");
        }

        let migration_data = Arc::new(migrations::load_migration_data(&genesis_config.chain_id));
        Arc::new(NightshadeRuntime {
            genesis_config: genesis_config.clone(),
            compiled_contract_cache,
            runtime_config_store,
            store,
            tries,
            runtime,
            trie_viewer,
            epoch_manager,
            migration_data,
            gc_num_epochs_to_keep: gc_num_epochs_to_keep.max(MIN_GC_NUM_EPOCHS_TO_KEEP),
        })
    }

    pub fn test_with_runtime_config_store(
        home_dir: &Path,
        store: Store,
        compiled_contract_cache: Box<dyn ContractRuntimeCache>,
        genesis_config: &GenesisConfig,
        epoch_manager: Arc<EpochManagerHandle>,
        runtime_config_store: RuntimeConfigStore,
        state_snapshot_type: StateSnapshotType,
    ) -> Arc<Self> {
        Self::new(
            store,
            compiled_contract_cache,
            genesis_config,
            epoch_manager,
            None,
            None,
            Some(runtime_config_store),
            DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
            Default::default(),
            StateSnapshotConfig {
                state_snapshot_type,
                home_dir: home_dir.to_path_buf(),
                hot_store_path: PathBuf::from("data"),
                state_snapshot_subdir: PathBuf::from("state_snapshot"),
            },
        )
    }

    pub fn test_with_trie_config(
        home_dir: &Path,
        store: Store,
        compiled_contract_cache: Box<dyn ContractRuntimeCache>,
        genesis_config: &GenesisConfig,
        epoch_manager: Arc<EpochManagerHandle>,
        runtime_config_store: Option<RuntimeConfigStore>,
        trie_config: TrieConfig,
        state_snapshot_type: StateSnapshotType,
    ) -> Arc<Self> {
        Self::new(
            store,
            compiled_contract_cache,
            genesis_config,
            epoch_manager,
            None,
            None,
            runtime_config_store,
            DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
            trie_config,
            StateSnapshotConfig {
                state_snapshot_type,
                home_dir: home_dir.to_path_buf(),
                hot_store_path: PathBuf::from("data"),
                state_snapshot_subdir: PathBuf::from("state_snapshot"),
            },
        )
    }

    pub fn test(
        home_dir: &Path,
        store: Store,
        genesis_config: &GenesisConfig,
        epoch_manager: Arc<EpochManagerHandle>,
    ) -> Arc<Self> {
        Self::test_with_runtime_config_store(
            home_dir,
            store,
            FilesystemContractRuntimeCache::with_memory_cache(home_dir, None::<&str>, 1)
                .expect("filesystem contract cache")
                .handle(),
            genesis_config,
            epoch_manager,
            RuntimeConfigStore::test(),
            StateSnapshotType::ForReshardingOnly,
        )
    }

    fn get_shard_uid_from_prev_hash(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
    ) -> Result<ShardUId, Error> {
        let epoch_manager = self.epoch_manager.read();
        let epoch_id =
            epoch_manager.get_epoch_id_from_prev_block(prev_hash).map_err(Error::from)?;
        let shard_version =
            epoch_manager.get_shard_layout(&epoch_id).map_err(Error::from)?.version();
        Ok(ShardUId { version: shard_version, shard_id: shard_id as u32 })
    }

    fn get_shard_uid_from_epoch_id(
        &self,
        shard_id: ShardId,
        epoch_id: &EpochId,
    ) -> Result<ShardUId, Error> {
        let epoch_manager = self.epoch_manager.read();
        let shard_version =
            epoch_manager.get_shard_layout(epoch_id).map_err(Error::from)?.version();
        Ok(ShardUId { version: shard_version, shard_id: shard_id as u32 })
    }

    fn account_id_to_shard_uid(
        &self,
        account_id: &AccountId,
        epoch_id: &EpochId,
    ) -> Result<ShardUId, Error> {
        let epoch_manager = self.epoch_manager.read();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id).map_err(Error::from)?;
        let shard_id = account_id_to_shard_id(account_id, &shard_layout);
        Ok(ShardUId::from_shard_id_and_layout(shard_id, &shard_layout))
    }

    /// Processes state update.
    #[instrument(target = "runtime", level = "debug", "process_state_update", skip_all)]
    fn process_state_update(
        &self,
        trie: Trie,
        apply_reason: ApplyChunkReason,
        chunk: ApplyChunkShardContext,
        block: ApplyChunkBlockContext,
        receipts: &[Receipt],
        transactions: &[SignedTransaction],
        state_patch: SandboxStatePatch,
    ) -> Result<ApplyChunkResult, Error> {
        let ApplyChunkBlockContext {
            height: block_height,
            block_hash,
            ref prev_block_hash,
            block_timestamp,
            gas_price,
            challenges_result,
            random_seed,
            congestion_info,
        } = block;
        let ApplyChunkShardContext {
            shard_id,
            last_validator_proposals,
            gas_limit,
            is_new_chunk,
            is_first_block_with_chunk_of_version,
        } = chunk;
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        let validator_accounts_update = {
            let epoch_manager = self.epoch_manager.read();
            let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
            debug!(target: "runtime",
                   next_block_epoch_start = epoch_manager.is_next_block_epoch_start(prev_block_hash).unwrap()
            );

            let mut slashing_info: HashMap<_, _> = challenges_result
                .iter()
                .filter_map(|s| {
                    if account_id_to_shard_id(&s.account_id, &shard_layout) == shard_id
                        && !s.is_double_sign
                    {
                        Some((s.account_id.clone(), None))
                    } else {
                        None
                    }
                })
                .collect();

            if epoch_manager.is_next_block_epoch_start(prev_block_hash)? {
                let (stake_info, validator_reward, double_sign_slashing_info) =
                    epoch_manager.compute_stake_return_info(prev_block_hash)?;
                let stake_info = stake_info
                    .into_iter()
                    .filter(|(account_id, _)| {
                        account_id_to_shard_id(account_id, &shard_layout) == shard_id
                    })
                    .collect();
                let validator_rewards = validator_reward
                    .into_iter()
                    .filter(|(account_id, _)| {
                        account_id_to_shard_id(account_id, &shard_layout) == shard_id
                    })
                    .collect();
                let last_proposals = last_validator_proposals
                    .filter(|v| account_id_to_shard_id(v.account_id(), &shard_layout) == shard_id)
                    .fold(HashMap::new(), |mut acc, v| {
                        let (account_id, stake) = v.account_and_stake();
                        acc.insert(account_id, stake);
                        acc
                    });
                let double_sign_slashing_info: HashMap<_, _> = double_sign_slashing_info
                    .into_iter()
                    .filter(|(account_id, _)| {
                        account_id_to_shard_id(account_id, &shard_layout) == shard_id
                    })
                    .map(|(account_id, stake)| (account_id, Some(stake)))
                    .collect();
                slashing_info.extend(double_sign_slashing_info);
                Some(ValidatorAccountsUpdate {
                    stake_info,
                    validator_rewards,
                    last_proposals,
                    protocol_treasury_account_id: Some(
                        self.genesis_config.protocol_treasury_account.clone(),
                    )
                    .filter(|account_id| {
                        account_id_to_shard_id(account_id, &shard_layout) == shard_id
                    }),
                    slashing_info,
                })
            } else if !challenges_result.is_empty() {
                Some(ValidatorAccountsUpdate {
                    stake_info: Default::default(),
                    validator_rewards: Default::default(),
                    last_proposals: Default::default(),
                    protocol_treasury_account_id: None,
                    slashing_info,
                })
            } else {
                None
            }
        };

        let epoch_height = self.epoch_manager.get_epoch_height_from_prev_block(prev_block_hash)?;
        let prev_block_epoch_id = self.epoch_manager.get_epoch_id(prev_block_hash)?;
        let current_protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        let prev_block_protocol_version =
            self.epoch_manager.get_epoch_protocol_version(&prev_block_epoch_id)?;
        let is_first_block_of_version = current_protocol_version != prev_block_protocol_version;

        debug!(
            target: "runtime",
            epoch_height,
            ?epoch_id,
            current_protocol_version,
            is_first_block_of_version
        );

        let apply_state = ApplyState {
            apply_reason: Some(apply_reason),
            block_height,
            prev_block_hash: *prev_block_hash,
            block_hash,
            shard_id,
            epoch_id,
            epoch_height,
            gas_price,
            block_timestamp,
            gas_limit: Some(gas_limit),
            random_seed,
            current_protocol_version,
            config: self.runtime_config_store.get_config(current_protocol_version).clone(),
            cache: Some(self.compiled_contract_cache.handle()),
            is_new_chunk,
            migration_data: Arc::clone(&self.migration_data),
            migration_flags: MigrationFlags {
                is_first_block_of_version,
                is_first_block_with_chunk_of_version,
            },
            congestion_info,
        };

        let instant = Instant::now();
        let apply_result = self
            .runtime
            .apply(
                trie,
                &validator_accounts_update,
                &apply_state,
                receipts,
                transactions,
                self.epoch_manager.as_ref(),
                state_patch,
            )
            .map_err(|e| match e {
                RuntimeError::InvalidTxError(err) => {
                    tracing::warn!("Invalid tx {:?}", err);
                    Error::InvalidTransactions
                }
                // TODO(#2152): process gracefully
                RuntimeError::BalanceMismatchError(e) => panic!("{}", e),
                // TODO(#2152): process gracefully
                RuntimeError::UnexpectedIntegerOverflow(reason) => {
                    panic!("RuntimeError::UnexpectedIntegerOverflow {reason}")
                }
                RuntimeError::StorageError(e) => Error::StorageError(e),
                // TODO(#2152): process gracefully
                RuntimeError::ReceiptValidationError(e) => panic!("{}", e),
                RuntimeError::ValidatorError(e) => e.into(),
            })?;
        let elapsed = instant.elapsed();

        let total_gas_burnt =
            apply_result.outcomes.iter().map(|tx_result| tx_result.outcome.gas_burnt).sum();
        metrics::APPLY_CHUNK_DELAY
            .with_label_values(&[&format_total_gas_burnt(total_gas_burnt)])
            .observe(elapsed.as_secs_f64());
        let shard_label = shard_id.to_string();
        metrics::DELAYED_RECEIPTS_COUNT
            .with_label_values(&[&shard_label])
            .set(apply_result.delayed_receipts_count as i64);
        if let Some(mut metrics) = apply_result.metrics {
            metrics.report(&shard_label);
        }

        let total_balance_burnt = apply_result
            .stats
            .tx_burnt_amount
            .checked_add(apply_result.stats.other_burnt_amount)
            .and_then(|result| result.checked_add(apply_result.stats.slashed_burnt_amount))
            .ok_or_else(|| {
                Error::Other("Integer overflow during burnt balance summation".to_string())
            })?;

        let shard_uid = self.get_shard_uid_from_prev_hash(shard_id, prev_block_hash)?;

        let result = ApplyChunkResult {
            trie_changes: WrappedTrieChanges::new(
                self.get_tries(),
                shard_uid,
                apply_result.trie_changes,
                apply_result.state_changes,
                block_hash,
                apply_state.block_height,
            ),
            new_root: apply_result.state_root,
            outcomes: apply_result.outcomes,
            outgoing_receipts: apply_result.outgoing_receipts,
            validator_proposals: apply_result.validator_proposals,
            total_gas_burnt,
            total_balance_burnt,
            proof: apply_result.proof,
            processed_delayed_receipts: apply_result.processed_delayed_receipts,
            processed_yield_timeouts: apply_result.processed_yield_timeouts,
            applied_receipts_hash: hash(&borsh::to_vec(receipts).unwrap()),
            congestion_info: apply_result.congestion_info,
        };

        Ok(result)
    }

    fn precompile_contracts(
        &self,
        epoch_id: &EpochId,
        contract_codes: Vec<ContractCode>,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(
            target: "runtime",
            "precompile_contracts",
            num_contracts = contract_codes.len())
        .entered();
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(epoch_id)?;
        let runtime_config = self.runtime_config_store.get_config(protocol_version);
        let compiled_contract_cache: Option<Box<dyn ContractRuntimeCache>> =
            Some(Box::new(self.compiled_contract_cache.handle()));
        // Execute precompile_contract in parallel but prevent it from using more than half of all
        // threads so that node will still function normally.
        rayon::scope(|scope| {
            let (slot_sender, slot_receiver) = std::sync::mpsc::channel();
            // Use up-to half of the threads for the compilation.
            let max_threads = std::cmp::max(rayon::current_num_threads() / 2, 1);
            for _ in 0..max_threads {
                slot_sender.send(()).expect("both sender and receiver are owned here");
            }
            for code in contract_codes {
                slot_receiver.recv().expect("could not receive a slot to compile contract");
                let contract_cache = compiled_contract_cache.as_deref();
                let slot_sender = slot_sender.clone();
                scope.spawn(move |_| {
                    precompile_contract(&code, &runtime_config.wasm_config, contract_cache).ok();
                    // If this fails, it just means there won't be any more attempts to recv the
                    // slots
                    let _ = slot_sender.send(());
                });
            }
        });
        Ok(())
    }

    fn get_gc_stop_height_impl(&self, block_hash: &CryptoHash) -> Result<BlockHeight, Error> {
        let epoch_manager = self.epoch_manager.read();
        // an epoch must have a first block.
        let epoch_first_block = *epoch_manager.get_block_info(block_hash)?.epoch_first_block();
        let epoch_first_block_info = epoch_manager.get_block_info(&epoch_first_block)?;
        // maintain pointers to avoid cloning.
        let mut last_block_in_prev_epoch = *epoch_first_block_info.prev_hash();
        let mut epoch_start_height = epoch_first_block_info.height();
        for _ in 0..self.gc_num_epochs_to_keep - 1 {
            let epoch_first_block =
                *epoch_manager.get_block_info(&last_block_in_prev_epoch)?.epoch_first_block();
            let epoch_first_block_info = epoch_manager.get_block_info(&epoch_first_block)?;
            epoch_start_height = epoch_first_block_info.height();
            last_block_in_prev_epoch = *epoch_first_block_info.prev_hash();
        }

        // An archival node with split storage should perform garbage collection
        // on the hot storage but not beyond the COLD_HEAD. In order to determine
        // if split storage is enabled *and* that the migration to split storage
        // is finished we can check the store kind. It's only set to hot after the
        // migration is finished.
        let kind = self.store.get_db_kind()?;
        let cold_head = self.store.get_ser::<Tip>(DBCol::BlockMisc, COLD_HEAD_KEY)?;

        if let (Some(DbKind::Hot), Some(cold_head)) = (kind, cold_head) {
            let cold_head_hash = cold_head.last_block_hash;
            let cold_epoch_first_block =
                *epoch_manager.get_block_info(&cold_head_hash)?.epoch_first_block();
            let cold_epoch_first_block_info =
                epoch_manager.get_block_info(&cold_epoch_first_block)?;
            return Ok(std::cmp::min(epoch_start_height, cold_epoch_first_block_info.height()));
        }
        Ok(epoch_start_height)
    }

    fn obtain_state_part_impl(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
        state_root: &StateRoot,
        part_id: PartId,
    ) -> Result<Vec<u8>, Error> {
        let _span = tracing::debug_span!(
            target: "runtime",
            "obtain_state_part",
            part_id = part_id.idx,
            shard_id,
            %prev_hash,
            num_parts = part_id.total)
        .entered();
        tracing::debug!(target: "state-parts", ?shard_id, ?prev_hash, ?state_root, ?part_id, "obtain_state_part");

        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_hash)?;
        let shard_uid = self.get_shard_uid_from_epoch_id(shard_id, &epoch_id)?;

        let trie_with_state =
            self.tries.get_trie_with_block_hash_for_shard(shard_uid, *state_root, &prev_hash, true);
        let (partial_state, nibbles_begin, nibbles_end) = match trie_with_state
            .get_state_part_boundaries(part_id)
        {
            Ok(res) => res,
            Err(err) => {
                error!(target: "runtime", ?err, part_id.idx, part_id.total, %prev_hash, %state_root, %shard_id, "Can't get trie nodes for state part boundaries");
                return Err(err.into());
            }
        };

        // TODO: Make it impossible for the snapshot data to be deleted while the snapshot is in use.
        let snapshot_trie = self
            .tries
            .get_trie_with_block_hash_for_shard_from_snapshot(shard_uid, *state_root, &prev_hash)
            .map_err(|err| Error::Other(err.to_string()))?;
        let state_part = borsh::to_vec(&match snapshot_trie.get_trie_nodes_for_part_with_flat_storage(part_id, partial_state, nibbles_begin, nibbles_end, &trie_with_state) {
            Ok(partial_state) => partial_state,
            Err(err) => {
                error!(target: "runtime", ?err, part_id.idx, part_id.total, %prev_hash, %state_root, %shard_id, "Can't get trie nodes for state part");
                return Err(err.into());
            }
        })
            .expect("serializer should not fail");

        Ok(state_part)
    }
}

fn format_total_gas_burnt(gas: Gas) -> String {
    // Rounds up the amount of teragas to hundreds of Tgas.
    // For example 123 Tgas gets rounded up to "200".
    format!("{:.0}", ((gas as f64) / 1e14).ceil() * 100.0)
}

impl RuntimeAdapter for NightshadeRuntime {
    fn store(&self) -> &Store {
        &self.store
    }

    fn get_tries(&self) -> ShardTries {
        self.tries.clone()
    }

    fn get_trie_for_shard(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
        state_root: StateRoot,
        use_flat_storage: bool,
    ) -> Result<Trie, Error> {
        let shard_uid = self.get_shard_uid_from_prev_hash(shard_id, prev_hash)?;
        if use_flat_storage {
            Ok(self
                .tries
                .get_trie_with_block_hash_for_shard(shard_uid, state_root, prev_hash, false))
        } else {
            Ok(self.tries.get_trie_for_shard(shard_uid, state_root))
        }
    }

    fn get_view_trie_for_shard(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
        state_root: StateRoot,
    ) -> Result<Trie, Error> {
        let shard_uid = self.get_shard_uid_from_prev_hash(shard_id, prev_hash)?;
        Ok(self.tries.get_view_trie_for_shard(shard_uid, state_root))
    }

    fn get_flat_storage_manager(&self) -> FlatStorageManager {
        self.tries.get_flat_storage_manager()
    }

    fn validate_tx(
        &self,
        gas_price: Balance,
        state_root: Option<StateRoot>,
        transaction: &SignedTransaction,
        verify_signature: bool,
        epoch_id: &EpochId,
        current_protocol_version: ProtocolVersion,
        receiver_congestion_info: Option<ExtendedCongestionInfo>,
    ) -> Result<Option<InvalidTxError>, Error> {
        let runtime_config = self.runtime_config_store.get_config(current_protocol_version);

        if let Some(congestion_info) = receiver_congestion_info {
            let congestion_control = CongestionControl::new(
                runtime_config.congestion_control_config,
                congestion_info.congestion_info,
                congestion_info.missed_chunks_count,
            );
            if let ShardAcceptsTransactions::No(reason) =
                congestion_control.shard_accepts_transactions()
            {
                let receiver_shard =
                    self.account_id_to_shard_uid(transaction.transaction.receiver_id(), epoch_id)?;
                let shard_id = receiver_shard.shard_id;
                let err = match reason {
                    RejectTransactionReason::IncomingCongestion { congestion_level }
                    | RejectTransactionReason::OutgoingCongestion { congestion_level }
                    | RejectTransactionReason::MemoryCongestion { congestion_level } => {
                        InvalidTxError::ShardCongested { shard_id, congestion_level }
                    }
                    RejectTransactionReason::MissedChunks { missed_chunks } => {
                        InvalidTxError::ShardStuck { shard_id, missed_chunks }
                    }
                };
                return Ok(Some(err));
            }
        }

        if let Some(state_root) = state_root {
            let shard_uid =
                self.account_id_to_shard_uid(transaction.transaction.signer_id(), epoch_id)?;
            let mut state_update = self.tries.new_trie_update(shard_uid, state_root);

            match verify_and_charge_transaction(
                runtime_config,
                &mut state_update,
                gas_price,
                transaction,
                verify_signature,
                // here we do not know which block the transaction will be included
                // and therefore skip the check on the nonce upper bound.
                None,
                current_protocol_version,
            ) {
                Ok(_) => Ok(None),
                Err(e) => Ok(Some(e)),
            }
        } else {
            // Doing basic validation without a state root
            match validate_transaction(
                runtime_config,
                gas_price,
                transaction,
                verify_signature,
                current_protocol_version,
            ) {
                Ok(_) => Ok(None),
                Err(e) => Ok(Some(e)),
            }
        }
    }

    fn prepare_transactions(
        &self,
        storage_config: RuntimeStorageConfig,
        chunk: PrepareTransactionsChunkContext,
        prev_block: PrepareTransactionsBlockContext,
        transaction_groups: &mut dyn TransactionGroupIterator,
        chain_validate: &mut dyn FnMut(&SignedTransaction) -> bool,
        time_limit: Option<Duration>,
    ) -> Result<PreparedTransactions, Error> {
        let start_time = std::time::Instant::now();
        let PrepareTransactionsChunkContext { shard_id, gas_limit, .. } = chunk;

        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(&prev_block.block_hash)?;
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;

        let next_epoch_id =
            self.epoch_manager.get_next_epoch_id_from_prev_block(&(&prev_block.block_hash))?;
        let next_protocol_version =
            self.epoch_manager.get_epoch_protocol_version(&next_epoch_id)?;

        let shard_uid = self.get_shard_uid_from_epoch_id(shard_id, &epoch_id)?;
        // While the height of the next block that includes the chunk might not be prev_height + 1,
        // using it will result in a more conservative check and will not accidentally allow
        // invalid transactions to be included.
        let next_block_height = prev_block.height + 1;

        let mut trie = match storage_config.source {
            StorageDataSource::Db => {
                self.tries.get_trie_for_shard(shard_uid, storage_config.state_root)
            }
            StorageDataSource::Recorded(storage) => Trie::from_recorded_storage(
                storage,
                storage_config.state_root,
                storage_config.use_flat_storage,
            ),
        };
        // We need to start recording reads if the stateless validation is
        // enabled in the next epoch. We need to save the state transition data
        // in the current epoch to be able to produce the state witness in the
        // next epoch.
        if checked_feature!("stable", StateWitnessSizeLimit, next_protocol_version)
            || cfg!(feature = "shadow_chunk_validation")
        {
            trie = trie.recording_reads();
        }
        let mut state_update = TrieUpdate::new(trie);

        // Total amount of gas burnt for converting transactions towards receipts.
        let mut total_gas_burnt = 0;
        let mut total_size = 0u64;

        let runtime_config = self.runtime_config_store.get_config(protocol_version);

        let transactions_gas_limit =
            chunk_tx_gas_limit(protocol_version, runtime_config, &prev_block, shard_id, gas_limit);

        let mut result = PreparedTransactions {
            transactions: Vec::new(),
            limited_by: None,
            storage_proof: None,
        };
        let mut num_checked_transactions = 0;

        // To avoid limiting the throughput of the network, we want to include enough receipts to
        // saturate the capacity of the chunk even in case when all of these receipts end up using
        // the smallest possible amount of gas, which is at least the cost of execution of action
        // receipt.
        // Currently, the min execution cost is ~100 GGas and the chunk capacity is 1 PGas, giving
        // a bound of at most 10000 receipts processed in a chunk.
        let delayed_receipts_indices: DelayedReceiptIndices =
            near_store::get(&state_update, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default();
        let min_fee = runtime_config.fees.fee(ActionCosts::new_action_receipt).exec_fee();
        let new_receipt_count_limit = if min_fee > 0 {
            // Round up to include at least one receipt.
            let max_processed_receipts_in_chunk = (gas_limit + min_fee - 1) / min_fee;
            // Allow at most 2 chunks worth of delayed receipts. This way under congestion,
            // after processing a single chunk, we will still have at least 1 chunk worth of
            // delayed receipts, ensuring the high throughput even if the next chunk producer
            // does not include any receipts.
            // This buffer size is a trade-off between the max queue size and system efficiency
            // under congestion.
            let delayed_receipt_count_limit = max_processed_receipts_in_chunk * 2;
            delayed_receipt_count_limit.saturating_sub(delayed_receipts_indices.len()) as usize
        } else {
            usize::MAX
        };

        let size_limit: u64 = calculate_transactions_size_limit(
            protocol_version,
            &runtime_config,
            chunk.last_chunk_transactions_size,
            transactions_gas_limit,
        );
        // for metrics only
        let mut rejected_due_to_congestion = 0;
        let mut rejected_invalid_tx = 0;
        let mut rejected_invalid_for_chain = 0;

        // Add new transactions to the result until some limit is hit or the transactions run out.
        'add_txs_loop: while let Some(transaction_group_iter) = transaction_groups.next() {
            if total_gas_burnt >= transactions_gas_limit {
                result.limited_by = Some(PrepareTransactionsLimit::Gas);
                break;
            }
            if total_size >= size_limit {
                result.limited_by = Some(PrepareTransactionsLimit::Size);
                break;
            }
            if !ProtocolFeature::CongestionControl.enabled(protocol_version) {
                // Keep this for the upgrade phase, afterwards it can be
                // removed. It does not need to be kept because it does not
                // affect replayability.
                // TODO: remove at release CongestionControl + 1 or later
                if result.transactions.len() >= new_receipt_count_limit {
                    result.limited_by = Some(PrepareTransactionsLimit::ReceiptCount);
                    break;
                }
            }

            if let Some(time_limit) = &time_limit {
                if start_time.elapsed() >= *time_limit {
                    result.limited_by = Some(PrepareTransactionsLimit::Time);
                    break;
                }
            }

            if checked_feature!("stable", WitnessTransactionLimits, protocol_version)
                && state_update.trie.recorded_storage_size()
                    > runtime_config
                        .witness_config
                        .new_transactions_validation_state_size_soft_limit
            {
                result.limited_by = Some(PrepareTransactionsLimit::StorageProofSize);
                break;
            }

            // Take a single transaction from this transaction group
            while let Some(tx_peek) = transaction_group_iter.peek_next() {
                // Stop adding transactions if the size limit would be exceeded
                if checked_feature!("stable", WitnessTransactionLimits, protocol_version)
                    && total_size.saturating_add(tx_peek.get_size()) > size_limit as u64
                {
                    result.limited_by = Some(PrepareTransactionsLimit::Size);
                    break 'add_txs_loop;
                }

                // Take the transaction out of the pool
                let tx = transaction_group_iter
                    .next()
                    .expect("peek_next() returned Some, so next() should return Some as well");
                num_checked_transactions += 1;

                if ProtocolFeature::CongestionControl.enabled(protocol_version) {
                    let receiving_shard = EpochManagerAdapter::account_id_to_shard_id(
                        self.epoch_manager.as_ref(),
                        tx.transaction.receiver_id(),
                        &epoch_id,
                    )?;
                    if let Some(congestion_info) = prev_block.congestion_info.get(&receiving_shard)
                    {
                        let congestion_control = CongestionControl::new(
                            runtime_config.congestion_control_config,
                            congestion_info.congestion_info,
                            congestion_info.missed_chunks_count,
                        );
                        if congestion_control.shard_accepts_transactions().is_no() {
                            tracing::trace!(target: "runtime", tx=?tx.get_hash(), "discarding transaction due to congestion");
                            rejected_due_to_congestion += 1;
                            continue;
                        }
                    }
                }

                // Verifying the transaction is on the same chain and hasn't expired yet.
                if !chain_validate(&tx) {
                    tracing::trace!(target: "runtime", tx=?tx.get_hash(), "discarding transaction that failed chain validation");
                    rejected_invalid_for_chain += 1;
                    continue;
                }

                // Verifying the validity of the transaction based on the current state.
                match verify_and_charge_transaction(
                    runtime_config,
                    &mut state_update,
                    prev_block.next_gas_price,
                    &tx,
                    false,
                    Some(next_block_height),
                    protocol_version,
                ) {
                    Ok(verification_result) => {
                        tracing::trace!(target: "runtime", tx=?tx.get_hash(), "including transaction that passed validation");
                        state_update.commit(StateChangeCause::NotWritableToDisk);
                        total_gas_burnt += verification_result.gas_burnt;
                        total_size += tx.get_size();
                        result.transactions.push(tx);
                        // Take one transaction from this group, no more.
                        break;
                    }
                    Err(err) => {
                        tracing::trace!(target: "runtime", tx=?tx.get_hash(), ?err, "discarding transaction that is invalid");
                        rejected_invalid_tx += 1;
                        state_update.rollback();
                    }
                }
            }
        }
        debug!(target: "runtime", "Transaction filtering results {} valid out of {} pulled from the pool", result.transactions.len(), num_checked_transactions);
        let shard_label = shard_id.to_string();
        metrics::PREPARE_TX_SIZE.with_label_values(&[&shard_label]).observe(total_size as f64);
        metrics::PREPARE_TX_REJECTED
            .with_label_values(&[&shard_label, "congestion"])
            .observe(rejected_due_to_congestion as f64);
        metrics::PREPARE_TX_REJECTED
            .with_label_values(&[&shard_label, "invalid_tx"])
            .observe(rejected_invalid_tx as f64);
        metrics::PREPARE_TX_REJECTED
            .with_label_values(&[&shard_label, "invalid_block_hash"])
            .observe(rejected_invalid_for_chain as f64);
        metrics::PREPARE_TX_GAS.with_label_values(&[&shard_label]).observe(total_gas_burnt as f64);
        metrics::CONGESTION_PREPARE_TX_GAS_LIMIT
            .with_label_values(&[&shard_label])
            .set(i64::try_from(transactions_gas_limit).unwrap_or(i64::MAX));
        result.storage_proof = state_update.trie.recorded_storage().map(|s| s.nodes);
        Ok(result)
    }

    fn get_gc_stop_height(&self, block_hash: &CryptoHash) -> BlockHeight {
        let result = self.get_gc_stop_height_impl(block_hash);
        match result {
            Ok(gc_stop_height) => gc_stop_height,
            Err(error) => {
                info!(target: "runtime", "Error when getting the gc stop height. This error may naturally occur after the gc_num_epochs_to_keep config is increased. It should disappear as soon as the node builds up all epochs it wants. Error: {}", error);
                self.genesis_config.genesis_height
            }
        }
    }

    #[instrument(target = "runtime", level = "info", skip_all, fields(shard_id = chunk.shard_id))]
    fn apply_chunk(
        &self,
        storage_config: RuntimeStorageConfig,
        apply_reason: ApplyChunkReason,
        chunk: ApplyChunkShardContext,
        block: ApplyChunkBlockContext,
        receipts: &[Receipt],
        transactions: &[SignedTransaction],
    ) -> Result<ApplyChunkResult, Error> {
        let shard_id = chunk.shard_id;
        let _timer =
            metrics::APPLYING_CHUNKS_TIME.with_label_values(&[&shard_id.to_string()]).start_timer();

        let mut trie = match storage_config.source {
            StorageDataSource::Db => self.get_trie_for_shard(
                shard_id,
                &block.prev_block_hash,
                storage_config.state_root,
                storage_config.use_flat_storage,
            )?,
            StorageDataSource::Recorded(storage) => Trie::from_recorded_storage(
                storage,
                storage_config.state_root,
                storage_config.use_flat_storage,
            ),
        };
        let next_epoch_id =
            self.epoch_manager.get_next_epoch_id_from_prev_block(&block.prev_block_hash)?;
        let next_protocol_version =
            self.epoch_manager.get_epoch_protocol_version(&next_epoch_id)?;

        // We need to start recording reads if the stateless validation is
        // enabled in the next epoch. We need to save the state transition data
        // in the current epoch to be able to produce the state witness in the
        // next epoch.
        if checked_feature!("stable", StateWitnessSizeLimit, next_protocol_version)
            || cfg!(feature = "shadow_chunk_validation")
        {
            trie = trie.recording_reads();
        }

        match self.process_state_update(
            trie,
            apply_reason,
            chunk,
            block,
            receipts,
            transactions,
            storage_config.state_patch,
        ) {
            Ok(result) => Ok(result),
            Err(e) => match e {
                Error::StorageError(err) => match &err {
                    StorageError::FlatStorageBlockNotSupported(_)
                    | StorageError::MissingTrieValue(..) => Err(err.into()),
                    _ => panic!("{err}"),
                },
                _ => Err(e),
            },
        }
    }

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
    ) -> Result<QueryResponse, crate::near_chain_primitives::error::QueryError> {
        match request {
            QueryRequest::ViewAccount { account_id } => {
                let account =
                    self.view_account(&shard_uid, *state_root, account_id).map_err(|err| {
                        crate::near_chain_primitives::error::QueryError::from_view_account_error(
                            err,
                            block_height,
                            *block_hash,
                        )
                    })?;
                Ok(QueryResponse {
                    kind: QueryResponseKind::ViewAccount(account.into()),
                    block_height,
                    block_hash: *block_hash,
                })
            }
            QueryRequest::ViewCode { account_id } => {
                let contract_code = self
                    .view_contract_code(&shard_uid,  *state_root, account_id)
                    .map_err(|err| crate::near_chain_primitives::error::QueryError::from_view_contract_code_error(err, block_height, *block_hash))?;
                let hash = *contract_code.hash();
                let contract_code_view = ContractCodeView { hash, code: contract_code.into_code() };
                Ok(QueryResponse {
                    kind: QueryResponseKind::ViewCode(contract_code_view),
                    block_height,
                    block_hash: *block_hash,
                })
            }
            QueryRequest::CallFunction { account_id, method_name, args } => {
                let mut logs = vec![];
                let (epoch_height, current_protocol_version) = {
                    let epoch_manager = self.epoch_manager.read();
                    let epoch_info = epoch_manager.get_epoch_info(epoch_id).map_err(|err| {
                        crate::near_chain_primitives::error::QueryError::from_epoch_error(
                            err,
                            block_height,
                            *block_hash,
                        )
                    })?;
                    (epoch_info.epoch_height(), epoch_info.protocol_version())
                };

                let call_function_result = self
                    .call_function(
                        &shard_uid,
                        *state_root,
                        block_height,
                        block_timestamp,
                        prev_block_hash,
                        block_hash,
                        epoch_height,
                        epoch_id,
                        account_id,
                        method_name,
                        args.as_ref(),
                        &mut logs,
                        self.epoch_manager.as_ref(),
                        current_protocol_version,
                    )
                    .map_err(|err| {
                        crate::near_chain_primitives::error::QueryError::from_call_function_error(
                            err,
                            block_height,
                            *block_hash,
                        )
                    })?;
                Ok(QueryResponse {
                    kind: QueryResponseKind::CallResult(CallResult {
                        result: call_function_result,
                        logs,
                    }),
                    block_height,
                    block_hash: *block_hash,
                })
            }
            QueryRequest::ViewState { account_id, prefix, include_proof } => {
                let view_state_result = self
                    .view_state(
                        &shard_uid,
                        *state_root,
                        account_id,
                        prefix.as_ref(),
                        *include_proof,
                    )
                    .map_err(|err| {
                        crate::near_chain_primitives::error::QueryError::from_view_state_error(
                            err,
                            block_height,
                            *block_hash,
                        )
                    })?;
                Ok(QueryResponse {
                    kind: QueryResponseKind::ViewState(view_state_result),
                    block_height,
                    block_hash: *block_hash,
                })
            }
            QueryRequest::ViewAccessKeyList { account_id } => {
                let access_key_list =
                    self.view_access_keys(&shard_uid, *state_root, account_id).map_err(|err| {
                        crate::near_chain_primitives::error::QueryError::from_view_access_key_error(
                            err,
                            block_height,
                            *block_hash,
                        )
                    })?;
                Ok(QueryResponse {
                    kind: QueryResponseKind::AccessKeyList(
                        access_key_list
                            .into_iter()
                            .map(|(public_key, access_key)| AccessKeyInfoView {
                                public_key,
                                access_key: access_key.into(),
                            })
                            .collect(),
                    ),
                    block_height,
                    block_hash: *block_hash,
                })
            }
            QueryRequest::ViewAccessKey { account_id, public_key } => {
                let access_key = self
                    .view_access_key(&shard_uid, *state_root, account_id, public_key)
                    .map_err(|err| {
                        crate::near_chain_primitives::error::QueryError::from_view_access_key_error(
                            err,
                            block_height,
                            *block_hash,
                        )
                    })?;
                Ok(QueryResponse {
                    kind: QueryResponseKind::AccessKey(access_key.into()),
                    block_height,
                    block_hash: *block_hash,
                })
            }
        }
    }

    // Wrapper to get the metrics.
    fn obtain_state_part(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
        state_root: &StateRoot,
        part_id: PartId,
    ) -> Result<Vec<u8>, Error> {
        let _span = tracing::debug_span!(
            target: "runtime",
            "obtain_state_part",
            part_id = part_id.idx,
            shard_id,
            %prev_hash,
            ?state_root,
            num_parts = part_id.total)
        .entered();
        let instant = Instant::now();
        let res = self.obtain_state_part_impl(shard_id, prev_hash, state_root, part_id);
        let elapsed = instant.elapsed();
        let is_ok = if res.is_ok() { "ok" } else { "error" };
        metrics::STATE_SYNC_OBTAIN_PART_DELAY
            .with_label_values(&[&shard_id.to_string(), is_ok])
            .observe(elapsed.as_secs_f64());
        res
    }

    fn validate_state_part(&self, state_root: &StateRoot, part_id: PartId, data: &[u8]) -> bool {
        match BorshDeserialize::try_from_slice(data) {
            Ok(trie_nodes) => {
                match Trie::validate_state_part(state_root, part_id, trie_nodes) {
                    Ok(_) => true,
                    // Storage error should not happen
                    Err(err) => {
                        tracing::error!(target: "state-parts", ?err, "State part storage error");
                        false
                    }
                }
            }
            // Deserialization error means we've got the data from malicious peer
            Err(err) => {
                tracing::error!(target: "state-parts", ?err, "State part deserialization error");
                false
            }
        }
    }

    fn apply_update_to_children_states(
        &self,
        block_hash: &CryptoHash,
        block_height: BlockHeight,
        state_roots: HashMap<ShardUId, StateRoot>,
        next_epoch_shard_layout: &ShardLayout,
        state_changes_for_resharding: StateChangesForResharding,
    ) -> Result<Vec<ApplyResultForResharding>, Error> {
        let trie_updates = self.tries.apply_state_changes_to_children_states(
            &state_roots,
            state_changes_for_resharding,
            &|account_id| account_id_to_shard_uid(account_id, next_epoch_shard_layout),
        )?;

        let mut applied_resharding_results: Vec<_> = vec![];
        for (shard_uid, trie_update) in trie_updates {
            let (_, trie_changes, state_changes) = trie_update.finalize()?;
            // All state changes that are related to resharding should have StateChangeCause as Resharding
            // We do not want to commit the state_changes from resharding as they are already handled while
            // processing parent shard
            debug_assert!(state_changes.iter().all(|raw_state_changes| raw_state_changes
                .changes
                .iter()
                .all(|state_change| state_change.cause == StateChangeCause::Resharding)));
            let new_root = trie_changes.new_root;
            let wrapped_trie_changes = WrappedTrieChanges::new(
                self.get_tries(),
                shard_uid,
                trie_changes,
                state_changes,
                *block_hash,
                block_height,
            );
            applied_resharding_results.push(ApplyResultForResharding {
                shard_uid,
                new_root,
                trie_changes: wrapped_trie_changes,
            });
        }

        Ok(applied_resharding_results)
    }

    fn apply_state_part(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        part_id: PartId,
        data: &[u8],
        epoch_id: &EpochId,
    ) -> Result<(), Error> {
        let _timer = metrics::STATE_SYNC_APPLY_PART_DELAY
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();

        let part = BorshDeserialize::try_from_slice(data)
            .expect("Part was already validated earlier, so could never fail here");
        let ApplyStatePartResult { trie_changes, flat_state_delta, contract_codes } =
            Trie::apply_state_part(state_root, part_id, part);
        let tries = self.get_tries();
        let shard_uid = self.get_shard_uid_from_epoch_id(shard_id, epoch_id)?;
        let mut store_update = tries.store_update();
        tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        debug!(target: "chain", %shard_id, "Inserting {} values to flat storage", flat_state_delta.len());
        // TODO: `apply_to_flat_state` inserts values with random writes, which can be time consuming.
        //       Optimize taking into account that flat state values always correspond to a consecutive range of keys.
        flat_state_delta.apply_to_flat_state(&mut store_update, shard_uid);
        self.precompile_contracts(epoch_id, contract_codes)?;
        Ok(store_update.commit()?)
    }

    /// `block_hash` is a block whose `prev_state_root` is `state_root`
    fn get_state_root_node(
        &self,
        shard_id: ShardId,
        block_hash: &CryptoHash,
        state_root: &StateRoot,
    ) -> Result<StateRootNode, Error> {
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
        let shard_uid = self.get_shard_uid_from_epoch_id(shard_id, &epoch_id)?;
        self.tries
            .get_view_trie_for_shard(shard_uid, *state_root)
            .retrieve_root_node()
            .map_err(Into::into)
    }

    fn validate_state_root_node(
        &self,
        state_root_node: &StateRootNode,
        state_root: &StateRoot,
    ) -> bool {
        if state_root == &Trie::EMPTY_ROOT {
            return state_root_node == &StateRootNode::empty();
        }
        if hash(&state_root_node.data) != *state_root {
            return false;
        }
        match Trie::get_memory_usage_from_serialized(&state_root_node.data) {
            Ok(memory_usage) => memory_usage == state_root_node.memory_usage,
            Err(_) => false, // Invalid state_root_node
        }
    }

    fn get_runtime_config(
        &self,
        protocol_version: ProtocolVersion,
    ) -> Result<RuntimeConfig, Error> {
        let runtime_config = self.runtime_config_store.get_config(protocol_version);
        Ok(runtime_config.as_ref().clone())
    }

    fn get_protocol_config(&self, epoch_id: &EpochId) -> Result<ProtocolConfig, Error> {
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(epoch_id)?;
        let mut genesis_config = self.genesis_config.clone();
        genesis_config.protocol_version = protocol_version;

        let epoch_config = self.epoch_manager.get_epoch_config(epoch_id)?;
        genesis_config.epoch_length = epoch_config.epoch_length;
        genesis_config.num_block_producer_seats = epoch_config.num_block_producer_seats;
        genesis_config.num_block_producer_seats_per_shard =
            epoch_config.num_block_producer_seats_per_shard;
        genesis_config.avg_hidden_validator_seats_per_shard =
            epoch_config.avg_hidden_validator_seats_per_shard;
        genesis_config.block_producer_kickout_threshold =
            epoch_config.block_producer_kickout_threshold;
        genesis_config.chunk_producer_kickout_threshold =
            epoch_config.chunk_producer_kickout_threshold;
        genesis_config.chunk_validator_only_kickout_threshold =
            epoch_config.chunk_validator_only_kickout_threshold;
        genesis_config.target_validator_mandates_per_shard =
            epoch_config.target_validator_mandates_per_shard;
        genesis_config.max_kickout_stake_perc = epoch_config.validator_max_kickout_stake_perc;
        genesis_config.online_min_threshold = epoch_config.online_min_threshold;
        genesis_config.online_max_threshold = epoch_config.online_max_threshold;
        genesis_config.fishermen_threshold = epoch_config.fishermen_threshold;
        genesis_config.minimum_stake_divisor = epoch_config.minimum_stake_divisor;
        genesis_config.protocol_upgrade_stake_threshold =
            epoch_config.protocol_upgrade_stake_threshold;
        genesis_config.shard_layout = epoch_config.shard_layout;
        genesis_config.num_chunk_only_producer_seats =
            epoch_config.validator_selection_config.num_chunk_only_producer_seats;
        genesis_config.minimum_validators_per_shard =
            epoch_config.validator_selection_config.minimum_validators_per_shard;
        genesis_config.minimum_stake_ratio =
            epoch_config.validator_selection_config.minimum_stake_ratio;
        genesis_config.shuffle_shard_assignment_for_chunk_producers =
            epoch_config.validator_selection_config.shuffle_shard_assignment_for_chunk_producers;

        let runtime_config =
            self.runtime_config_store.get_config(protocol_version).as_ref().clone();
        Ok(ProtocolConfig { genesis_config, runtime_config })
    }

    fn will_shard_layout_change_next_epoch(&self, parent_hash: &CryptoHash) -> Result<bool, Error> {
        let epoch_manager = self.epoch_manager.read();
        Ok(epoch_manager.will_shard_layout_change(parent_hash)?)
    }
}

/// How much gas of the next chunk we want to spend on converting new
/// transactions to receipts.
fn chunk_tx_gas_limit(
    protocol_version: u32,
    runtime_config: &RuntimeConfig,
    prev_block: &PrepareTransactionsBlockContext,
    shard_id: u64,
    gas_limit: u64,
) -> u64 {
    if ProtocolFeature::CongestionControl.enabled(protocol_version) {
        if let Some(own_congestion) = prev_block.congestion_info.get(&shard_id) {
            let congestion_control = CongestionControl::new(
                runtime_config.congestion_control_config,
                own_congestion.congestion_info,
                own_congestion.missed_chunks_count,
            );
            congestion_control.process_tx_limit()
        } else {
            // When a new shard is created, or when the feature is just being enabled.
            // Using the default (no congestion) is a reasonable choice in this case.
            let own_congestion = ExtendedCongestionInfo::default();
            let congestion_control = CongestionControl::new(
                runtime_config.congestion_control_config,
                own_congestion.congestion_info,
                own_congestion.missed_chunks_count,
            );
            congestion_control.process_tx_limit()
        }
    } else {
        gas_limit / 2
    }
}

fn calculate_transactions_size_limit(
    protocol_version: ProtocolVersion,
    runtime_config: &RuntimeConfig,
    last_chunk_transactions_size: usize,
    transactions_gas_limit: Gas,
) -> u64 {
    if checked_feature!("stable", WitnessTransactionLimits, protocol_version) {
        // Sum of transactions in the previous and current chunks should not exceed the limit.
        // Witness keeps transactions from both previous and current chunk, so we have to limit the sum of both.
        runtime_config
            .witness_config
            .combined_transactions_size_limit
            .saturating_sub(last_chunk_transactions_size)
            .try_into()
            .expect("Can't convert usize to u64!")
    } else {
        // In general, we limit the number of transactions via send_fees.
        // However, as a second line of defense, we want to limit the byte size
        // of transaction as well. Rather than introducing a separate config for
        // the limit, we compute it heuristically from the gas limit and the
        // cost of roundtripping a byte of data through disk. For today's value
        // of parameters, this corresponds to about 13megs worth of
        // transactions.
        let roundtripping_cost =
            runtime_config.wasm_config.ext_costs.gas_cost(ExtCosts::storage_write_value_byte)
                + runtime_config.wasm_config.ext_costs.gas_cost(ExtCosts::storage_read_value_byte);
        transactions_gas_limit / roundtripping_cost
    }
}

impl node_runtime::adapter::ViewRuntimeAdapter for NightshadeRuntime {
    fn view_account(
        &self,
        shard_uid: &ShardUId,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Account, node_runtime::state_viewer::errors::ViewAccountError> {
        let state_update = self.tries.new_trie_update_view(*shard_uid, state_root);
        self.trie_viewer.view_account(&state_update, account_id)
    }

    fn view_contract_code(
        &self,
        shard_uid: &ShardUId,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<ContractCode, node_runtime::state_viewer::errors::ViewContractCodeError> {
        let state_update = self.tries.new_trie_update_view(*shard_uid, state_root);
        self.trie_viewer.view_contract_code(&state_update, account_id)
    }

    fn call_function(
        &self,
        shard_uid: &ShardUId,
        state_root: MerkleHash,
        height: BlockHeight,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        epoch_height: EpochHeight,
        epoch_id: &EpochId,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
        epoch_info_provider: &dyn EpochInfoProvider,
        current_protocol_version: ProtocolVersion,
    ) -> Result<Vec<u8>, node_runtime::state_viewer::errors::CallFunctionError> {
        let state_update = self.tries.new_trie_update_view(*shard_uid, state_root);
        let view_state = ViewApplyState {
            shard_id: shard_uid.shard_id(),
            block_height: height,
            prev_block_hash: *prev_block_hash,
            block_hash: *block_hash,
            epoch_id: epoch_id.clone(),
            epoch_height,
            block_timestamp,
            current_protocol_version,
            cache: Some(Box::new(self.compiled_contract_cache.handle())),
        };
        self.trie_viewer.call_function(
            state_update,
            view_state,
            contract_id,
            method_name,
            args,
            logs,
            epoch_info_provider,
        )
    }

    fn view_access_key(
        &self,
        shard_uid: &ShardUId,
        state_root: MerkleHash,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKey, node_runtime::state_viewer::errors::ViewAccessKeyError> {
        let state_update = self.tries.new_trie_update_view(*shard_uid, state_root);
        self.trie_viewer.view_access_key(&state_update, account_id, public_key)
    }

    fn view_access_keys(
        &self,
        shard_uid: &ShardUId,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Vec<(PublicKey, AccessKey)>, node_runtime::state_viewer::errors::ViewAccessKeyError>
    {
        let state_update = self.tries.new_trie_update_view(*shard_uid, state_root);
        self.trie_viewer.view_access_keys(&state_update, account_id)
    }

    fn view_state(
        &self,
        shard_uid: &ShardUId,
        state_root: MerkleHash,
        account_id: &AccountId,
        prefix: &[u8],
        include_proof: bool,
    ) -> Result<ViewStateResult, node_runtime::state_viewer::errors::ViewStateError> {
        let state_update = self.tries.new_trie_update_view(*shard_uid, state_root);
        self.trie_viewer.view_state(&state_update, account_id, prefix, include_proof)
    }
}
