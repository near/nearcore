use crate::metrics;
use crate::migrations::load_migration_data;
use crate::shard_tracker::{ShardTracker, TrackedConfig};
use crate::NearConfig;
use borsh::ser::BorshSerialize;
use borsh::BorshDeserialize;
use errors::FromStateViewerErrors;
use near_chain::types::{
    ApplySplitStateResult, ApplyTransactionResult, BlockHeaderInfo, ValidatorInfoIdentifier,
};
use near_chain::{BlockHeader, Doomslug, DoomslugThresholdMode, Error, RuntimeAdapter};
use near_chain_configs::{
    Genesis, GenesisConfig, ProtocolConfig, DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
    MIN_GC_NUM_EPOCHS_TO_KEEP,
};
use near_crypto::{PublicKey, Signature};
use near_epoch_manager::EpochManager;
use near_pool::types::PoolIterator;
use near_primitives::account::{AccessKey, Account};
use near_primitives::block::{Approval, ApprovalInner};
use near_primitives::challenge::ChallengesResult;
use near_primitives::contract::ContractCode;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::{EpochConfig, ShardConfig};
use near_primitives::errors::{EpochError, InvalidTxError, RuntimeError};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::Receipt;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::shard_layout::{
    account_id_to_shard_id, account_id_to_shard_uid, ShardLayout, ShardUId,
};
use near_primitives::sharding::ChunkHash;
use near_primitives::state_part::PartId;
use near_primitives::state_record::{state_record_to_account_id, StateRecord};
use near_primitives::syncing::{get_num_state_parts, STATE_PART_MEMORY_LIMIT};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
use near_primitives::types::{
    AccountId, ApprovalStake, Balance, BlockHeight, CompiledContractCache, EpochHeight, EpochId,
    EpochInfoProvider, Gas, MerkleHash, NumShards, ShardId, StateChangeCause,
    StateChangesForSplitStates, StateRoot, StateRootNode,
};
use near_primitives::version::ProtocolVersion;
use near_primitives::views::{
    AccessKeyInfoView, CallResult, EpochValidatorInfo, QueryRequest, QueryResponse,
    QueryResponseKind, ViewApplyState, ViewStateResult,
};
use near_store::split_state::get_delayed_receipts;
use near_store::{
    get_genesis_hash, get_genesis_state_roots, set_genesis_hash, set_genesis_state_roots,
    ApplyStatePartResult, DBCol, PartialStorage, ShardTries, Store, StoreCompiledContractCache,
    StoreUpdate, Trie, TrieConfig, WrappedTrieChanges,
};
use near_vm_runner::precompile_contract;
use node_runtime::adapter::ViewRuntimeAdapter;
use node_runtime::config::RuntimeConfig;
use node_runtime::near_primitives::shard_layout::ShardLayoutError;
use node_runtime::state_viewer::TrieViewer;
use node_runtime::{
    validate_transaction, verify_and_charge_transaction, ApplyState, Runtime,
    ValidatorAccountsUpdate,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Instant;
use tracing::{debug, error, info, warn};

pub mod errors;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";
const STATE_DUMP_FILE: &str = "state_dump";
const GENESIS_ROOTS_FILE: &str = "genesis_roots";

/// Wrapper type for epoch manager to get avoid implementing trait for foreign types.
pub struct SafeEpochManager(pub Arc<RwLock<EpochManager>>);

impl SafeEpochManager {
    fn write(&self) -> RwLockWriteGuard<EpochManager> {
        self.0.write().expect(POISONED_LOCK_ERR)
    }

    fn read(&self) -> RwLockReadGuard<EpochManager> {
        self.0.read().expect(POISONED_LOCK_ERR)
    }
}

impl EpochInfoProvider for SafeEpochManager {
    fn validator_stake(
        &self,
        epoch_id: &EpochId,
        last_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<Option<Balance>, EpochError> {
        let epoch_manager = self.read();
        let last_block_info = epoch_manager.get_block_info(last_block_hash)?;
        if last_block_info.slashed().contains_key(account_id) {
            return Ok(None);
        }
        let epoch_info = epoch_manager.get_epoch_info(epoch_id)?;
        Ok(epoch_info.get_validator_id(account_id).map(|id| epoch_info.validator_stake(*id)))
    }

    fn validator_total_stake(
        &self,
        epoch_id: &EpochId,
        last_block_hash: &CryptoHash,
    ) -> Result<Balance, EpochError> {
        let epoch_manager = self.read();
        let last_block_info = epoch_manager.get_block_info(last_block_hash)?;
        let epoch_info = epoch_manager.get_epoch_info(epoch_id)?;
        Ok(epoch_info
            .validators_iter()
            .filter(|info| !last_block_info.slashed().contains_key(info.account_id()))
            .map(|info| info.stake())
            .sum())
    }

    fn minimum_stake(&self, prev_block_hash: &CryptoHash) -> Result<Balance, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.minimum_stake(prev_block_hash)
    }
}

/// Defines Nightshade state transition and validator rotation.
/// TODO: this possibly should be merged with the runtime cargo or at least reconciled on the interfaces.
pub struct NightshadeRuntime {
    genesis_config: GenesisConfig,
    runtime_config_store: RuntimeConfigStore,

    store: Store,
    tries: ShardTries,
    trie_viewer: TrieViewer,
    pub runtime: Runtime,
    epoch_manager: SafeEpochManager,
    shard_tracker: ShardTracker,
    genesis_state_roots: Vec<StateRoot>,
    migration_data: Arc<MigrationData>,
    gc_num_epochs_to_keep: u64,
}

impl NightshadeRuntime {
    pub fn from_config(home_dir: &Path, store: Store, config: &NearConfig) -> Self {
        let mut trie_config = TrieConfig::default();
        trie_config
            .shard_cache_config
            .override_max_entries
            .extend(config.config.store.trie_cache_capacities.iter().cloned());

        Self::new(
            home_dir,
            store,
            &config.genesis,
            TrackedConfig::from_config(&config.client_config),
            config.client_config.trie_viewer_state_size_limit,
            config.client_config.max_gas_burnt_view,
            None,
            config.config.gc.gc_num_epochs_to_keep(),
            trie_config,
        )
    }

    fn new(
        home_dir: &Path,
        store: Store,
        genesis: &Genesis,
        tracked_config: TrackedConfig,
        trie_viewer_state_size_limit: Option<u64>,
        max_gas_burnt_view: Option<Gas>,
        runtime_config_store: Option<RuntimeConfigStore>,
        gc_num_epochs_to_keep: u64,
        trie_config: TrieConfig,
    ) -> Self {
        let runtime_config_store = match runtime_config_store {
            Some(store) => store,
            None => NightshadeRuntime::create_runtime_config_store(&genesis.config.chain_id),
        };

        let runtime = Runtime::new();
        let trie_viewer = TrieViewer::new(trie_viewer_state_size_limit, max_gas_burnt_view);
        let genesis_config = genesis.config.clone();
        assert_eq!(
            genesis_config.shard_layout.num_shards(),
            genesis_config.num_block_producer_seats_per_shard.len() as NumShards,
            "genesis config shard_layout and num_block_producer_seats_per_shard indicate inconsistent number of shards {} vs {}",
            genesis_config.shard_layout.num_shards(),
            genesis_config.num_block_producer_seats_per_shard.len() as NumShards,
        );
        let state_roots =
            Self::initialize_genesis_state_if_needed(store.clone(), home_dir, genesis);
        let tries = ShardTries::new(
            store.clone(),
            trie_config,
            &genesis_config.shard_layout.get_shard_uids(),
        );
        let epoch_manager = Arc::new(RwLock::new(
            EpochManager::new_from_genesis_config(store.clone(), &genesis_config)
                .expect("Failed to start Epoch Manager"),
        ));
        let shard_tracker = ShardTracker::new(tracked_config, epoch_manager.clone());
        NightshadeRuntime {
            genesis_config,
            runtime_config_store,
            store,
            tries,
            runtime,
            trie_viewer,
            epoch_manager: SafeEpochManager(epoch_manager),
            shard_tracker,
            genesis_state_roots: state_roots,
            migration_data: Arc::new(load_migration_data(&genesis.config.chain_id)),
            gc_num_epochs_to_keep: gc_num_epochs_to_keep.max(MIN_GC_NUM_EPOCHS_TO_KEEP),
        }
    }

    pub fn test_with_runtime_config_store(
        home_dir: &Path,
        store: Store,
        genesis: &Genesis,
        tracked_config: TrackedConfig,
        runtime_config_store: RuntimeConfigStore,
    ) -> Self {
        Self::new(
            home_dir,
            store,
            genesis,
            tracked_config,
            None,
            None,
            Some(runtime_config_store),
            DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
            Default::default(),
        )
    }

    pub fn test(home_dir: &Path, store: Store, genesis: &Genesis) -> Self {
        Self::test_with_runtime_config_store(
            home_dir,
            store,
            genesis,
            TrackedConfig::new_empty(),
            RuntimeConfigStore::test(),
        )
    }

    pub fn get_epoch_id(&self, hash: &CryptoHash) -> Result<EpochId, Error> {
        let epoch_manager = self.epoch_manager.read();
        epoch_manager.get_epoch_id(hash).map_err(Error::from)
    }

    /// Create store of runtime configs for the given chain id.
    ///
    /// For mainnet and other chains except testnet we don't need to override runtime config for
    /// first protocol versions.
    /// For testnet, runtime config for genesis block was (incorrectly) different, that's why we
    /// need to override it specifically to preserve compatibility.
    fn create_runtime_config_store(chain_id: &str) -> RuntimeConfigStore {
        match chain_id {
            "testnet" => {
                let genesis_runtime_config = RuntimeConfig::initial_testnet_config();
                RuntimeConfigStore::new(Some(&genesis_runtime_config))
            }
            _ => RuntimeConfigStore::new(None),
        }
    }

    fn genesis_state_from_dump(store: Store, home_dir: &Path) -> Vec<StateRoot> {
        error!(target: "near", "Loading genesis from a state dump file. Do not use this outside of genesis-tools");
        let mut state_file = home_dir.to_path_buf();
        state_file.push(STATE_DUMP_FILE);
        store
            .load_from_file(DBCol::State, state_file.as_path())
            .expect("Failed to read state dump");
        let mut roots_files = home_dir.to_path_buf();
        roots_files.push(GENESIS_ROOTS_FILE);
        let data = fs::read(roots_files).expect("Failed to read genesis roots file.");
        let state_roots: Vec<StateRoot> =
            BorshDeserialize::try_from_slice(&data).expect("Failed to deserialize genesis roots");
        state_roots
    }

    fn genesis_state_from_records(store: Store, genesis: &Genesis) -> Vec<StateRoot> {
        if !genesis.records.as_ref().is_empty() {
            info!(target: "runtime", "Genesis state has {} records, computing state roots", genesis.records.0.len());
        } else {
            info!(target: "runtime", "Computing state roots from records in file {:?}", genesis.records_file);
        }
        let mut state_roots = vec![];
        let initial_epoch_config = EpochConfig::from(&genesis.config);
        let shard_layout = initial_epoch_config.shard_layout;
        let num_shards = shard_layout.num_shards();
        let mut shard_account_ids: Vec<HashSet<AccountId>> =
            (0..num_shards).map(|_| HashSet::new()).collect();
        let mut has_protocol_account = false;
        genesis.for_each_record(|record: &StateRecord| {
            shard_account_ids[state_record_to_shard_id(record, &shard_layout) as usize]
                .insert(state_record_to_account_id(record).clone());
            if let StateRecord::Account { account_id, .. } = record {
                if account_id == &genesis.config.protocol_treasury_account {
                    has_protocol_account = true;
                }
            }
        });
        assert!(has_protocol_account, "Genesis spec doesn't have protocol treasury account");
        let tries = ShardTries::new(
            store,
            TrieConfig::default(),
            &genesis.config.shard_layout.get_shard_uids(),
        );
        let runtime = Runtime::new();
        let runtime_config_store =
            NightshadeRuntime::create_runtime_config_store(&genesis.config.chain_id);
        let runtime_config = runtime_config_store.get_config(genesis.config.protocol_version);

        for shard_id in 0..num_shards {
            let validators = genesis
                .config
                .validators
                .iter()
                .filter_map(|account_info| {
                    if account_id_to_shard_id(&account_info.account_id, &shard_layout) == shard_id {
                        Some((
                            account_info.account_id.clone(),
                            account_info.public_key.clone(),
                            account_info.amount,
                        ))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            state_roots.push(runtime.apply_genesis_state(
                tries.clone(),
                shard_id,
                &validators,
                genesis,
                runtime_config,
                shard_account_ids[shard_id as usize].clone(),
            ));
        }
        state_roots
    }

    /// On first start: compute state roots, load genesis state into storage.
    /// After that: return genesis state roots. The state is not guaranteed to be in storage, as
    /// GC and state sync are allowed to delete it.
    pub fn initialize_genesis_state_if_needed(
        store: Store,
        home_dir: &Path,
        genesis: &Genesis,
    ) -> Vec<StateRoot> {
        let stored_hash = get_genesis_hash(&store).expect("Store failed on genesis intialization");
        if let Some(_hash) = stored_hash {
            // TODO: re-enable this check (#4447)
            //assert_eq!(hash, genesis_hash, "Storage already exists, but has a different genesis");
            get_genesis_state_roots(&store)
                .expect("Store failed on genesis intialization")
                .expect("Genesis state roots not found in storage")
        } else {
            let genesis_hash = genesis.json_hash();
            let state_roots = Self::initialize_genesis_state(store.clone(), home_dir, genesis);
            let mut store_update = store.store_update();
            set_genesis_hash(&mut store_update, &genesis_hash);
            set_genesis_state_roots(&mut store_update, &state_roots);
            store_update.commit().expect("Store failed on genesis intialization");
            state_roots
        }
    }

    pub fn initialize_genesis_state(
        store: Store,
        home_dir: &Path,
        genesis: &Genesis,
    ) -> Vec<StateRoot> {
        let has_records = !genesis.records.as_ref().is_empty();
        let has_dump = home_dir.join(STATE_DUMP_FILE).exists();
        if has_dump {
            if has_records {
                warn!(target: "runtime", "Found both records in genesis config and the state dump file. Will ignore the records.");
            }
            let state_roots = Self::genesis_state_from_dump(store, home_dir);
            state_roots
        } else {
            Self::genesis_state_from_records(store, genesis)
        }
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
    fn process_state_update(
        &self,
        trie: Trie,
        shard_id: ShardId,
        block_height: BlockHeight,
        block_hash: &CryptoHash,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
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
        let _span = tracing::debug_span!(target: "runtime", "process_state_update").entered();
        let epoch_id = self.get_epoch_id_from_prev_block(prev_block_hash)?;
        let validator_accounts_update = {
            let epoch_manager = self.epoch_manager.read();
            let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?.clone();
            debug!(target: "runtime",
                   "block height: {}, is next_block_epoch_start {}",
                   block_height,
                   epoch_manager.is_next_block_epoch_start(prev_block_hash).unwrap()
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

        let epoch_height = self.get_epoch_height_from_prev_block(prev_block_hash)?;
        let prev_block_epoch_id = self.get_epoch_id(prev_block_hash)?;
        let current_protocol_version = self.get_epoch_protocol_version(&epoch_id)?;
        let prev_block_protocol_version = self.get_epoch_protocol_version(&prev_block_epoch_id)?;
        let is_first_block_of_version = current_protocol_version != prev_block_protocol_version;

        debug!(target: "runtime", ?epoch_height, ?epoch_id, ?current_protocol_version, ?is_first_block_of_version);

        let apply_state = ApplyState {
            block_index: block_height,
            prev_block_hash: *prev_block_hash,
            block_hash: *block_hash,
            epoch_id,
            epoch_height,
            gas_price,
            block_timestamp,
            gas_limit: Some(gas_limit),
            random_seed,
            current_protocol_version,
            config: self.runtime_config_store.get_config(current_protocol_version).clone(),
            cache: Some(Box::new(StoreCompiledContractCache::new(&self.store))),
            is_new_chunk,
            migration_data: Arc::clone(&self.migration_data),
            migration_flags: MigrationFlags {
                is_first_block_of_version,
                is_first_block_with_chunk_of_version,
            },
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
                &self.epoch_manager,
                state_patch,
            )
            .map_err(|e| match e {
                RuntimeError::InvalidTxError(_) => Error::InvalidTransactions,
                // TODO(#2152): process gracefully
                RuntimeError::BalanceMismatchError(e) => panic!("{}", e),
                // TODO(#2152): process gracefully
                RuntimeError::UnexpectedIntegerOverflow => {
                    panic!("RuntimeError::UnexpectedIntegerOverflow")
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
        if total_gas_burnt > 0 {
            metrics::SECONDS_PER_PETAGAS
                .with_label_values(&[])
                .observe(elapsed.as_secs_f64() * 1e15 / total_gas_burnt as f64);
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

        let result = ApplyTransactionResult {
            trie_changes: WrappedTrieChanges::new(
                self.get_tries(),
                shard_uid,
                apply_result.trie_changes,
                apply_result.state_changes,
                *block_hash,
            ),
            new_root: apply_result.state_root,
            outcomes: apply_result.outcomes,
            outgoing_receipts: apply_result.outgoing_receipts,
            validator_proposals: apply_result.validator_proposals,
            total_gas_burnt,
            total_balance_burnt,
            proof: apply_result.proof,
            processed_delayed_receipts: apply_result.processed_delayed_receipts,
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
        let protocol_version = self.get_epoch_protocol_version(epoch_id)?;
        let runtime_config = self.runtime_config_store.get_config(protocol_version);
        let compiled_contract_cache: Option<Box<dyn CompiledContractCache>> =
            Some(Box::new(StoreCompiledContractCache::new(&self.store)));
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
                    precompile_contract(
                        &code,
                        &runtime_config.wasm_config,
                        protocol_version,
                        contract_cache,
                    )
                    .ok();
                    // If this fails, it just means there won't be any more attempts to recv the
                    // slots
                    let _ = slot_sender.send(());
                });
            }
        });
        Ok(())
    }
}

fn format_total_gas_burnt(gas: Gas) -> String {
    // Rounds up the amount of teragas to hundreds of Tgas.
    // For example 123 Tgas gets rounded up to "200".
    format!("{:.0}", ((gas as f64) / 1e14).ceil() * 100.0)
}

fn apply_delayed_receipts<'a>(
    tries: &ShardTries,
    orig_shard_uid: ShardUId,
    orig_state_root: StateRoot,
    state_roots: HashMap<ShardUId, StateRoot>,
    account_id_to_shard_id: &(dyn Fn(&AccountId) -> ShardUId + 'a),
) -> Result<HashMap<ShardUId, StateRoot>, Error> {
    let orig_trie_update = tries.new_trie_update_view(orig_shard_uid, orig_state_root);

    let mut start_index = None;
    let mut new_state_roots = state_roots;
    while let Some((next_index, receipts)) =
        get_delayed_receipts(&orig_trie_update, start_index, STATE_PART_MEMORY_LIMIT)?
    {
        let (store_update, updated_state_roots) = tries.apply_delayed_receipts_to_split_states(
            &new_state_roots,
            &receipts,
            account_id_to_shard_id,
        )?;
        new_state_roots = updated_state_roots;
        start_index = Some(next_index);
        store_update.commit()?;
    }

    Ok(new_state_roots)
}

pub fn state_record_to_shard_id(state_record: &StateRecord, shard_layout: &ShardLayout) -> ShardId {
    account_id_to_shard_id(state_record_to_account_id(state_record), shard_layout)
}

impl RuntimeAdapter for NightshadeRuntime {
    fn genesis_state(&self) -> (Store, Vec<StateRoot>) {
        (self.store.clone(), self.genesis_state_roots.clone())
    }

    fn get_store(&self) -> Store {
        self.store.clone()
    }

    fn get_tries(&self) -> ShardTries {
        self.tries.clone()
    }

    // TODO (#7327): Make usage of flat state conditional on prev_hash and call `get_trie_for_shard` if this is not the
    // case. Current implementation never creates flat state if `protocol_feature_flat_state` is not enabled.
    fn get_trie_for_shard(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
        state_root: StateRoot,
    ) -> Result<Trie, Error> {
        let shard_uid = self.get_shard_uid_from_prev_hash(shard_id, prev_hash)?;
        Ok(self.tries.get_trie_for_shard(shard_uid, state_root))
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

    fn verify_block_vrf(
        &self,
        epoch_id: &EpochId,
        block_height: BlockHeight,
        prev_random_value: &CryptoHash,
        vrf_value: &near_crypto::vrf::Value,
        vrf_proof: &near_crypto::vrf::Proof,
    ) -> Result<(), Error> {
        let epoch_manager = self.epoch_manager.read();
        let validator = epoch_manager.get_block_producer_info(epoch_id, block_height)?;
        let public_key = near_crypto::key_conversion::convert_public_key(
            validator.public_key().unwrap_as_ed25519(),
        )
        .unwrap();

        if !public_key.is_vrf_valid(&prev_random_value.as_ref(), vrf_value, vrf_proof) {
            return Err(Error::InvalidRandomnessBeaconOutput);
        }
        Ok(())
    }

    fn validate_tx(
        &self,
        gas_price: Balance,
        state_root: Option<StateRoot>,
        transaction: &SignedTransaction,
        verify_signature: bool,
        epoch_id: &EpochId,
        current_protocol_version: ProtocolVersion,
    ) -> Result<Option<InvalidTxError>, Error> {
        let runtime_config = self.runtime_config_store.get_config(current_protocol_version);

        if let Some(state_root) = state_root {
            let shard_uid =
                self.account_id_to_shard_uid(&transaction.transaction.signer_id, epoch_id)?;
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
                Err(RuntimeError::InvalidTxError(err)) => {
                    debug!(target: "runtime", "Tx {:?} validation failed: {:?}", transaction, err);
                    Ok(Some(err))
                }
                Err(RuntimeError::StorageError(err)) => Err(Error::StorageError(err)),
                Err(err) => unreachable!("Unexpected RuntimeError error {:?}", err),
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
                Err(RuntimeError::InvalidTxError(err)) => {
                    debug!(target: "runtime", "Tx {:?} validation failed: {:?}", transaction, err);
                    Ok(Some(err))
                }
                Err(RuntimeError::StorageError(err)) => Err(Error::StorageError(err)),
                Err(err) => unreachable!("Unexpected RuntimeError error {:?}", err),
            }
        }
    }

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
    ) -> Result<Vec<SignedTransaction>, Error> {
        let shard_uid = self.get_shard_uid_from_epoch_id(shard_id, epoch_id)?;
        let mut state_update = self.tries.new_trie_update(shard_uid, state_root);

        // Total amount of gas burnt for converting transactions towards receipts.
        let mut total_gas_burnt = 0;
        let mut total_size = 0u64;
        // TODO: Update gas limit for transactions
        let transactions_gas_limit = gas_limit / 2;
        let mut transactions = vec![];
        let mut num_checked_transactions = 0;

        let runtime_config = self.runtime_config_store.get_config(current_protocol_version);

        // In general, we limit the number of transactions via send_fees.
        // However, as a second line of defense, we want to limit the byte size
        // of transaction as well. Rather than introducing a separate config for
        // the limit, we compute it heuristically from the gas limit and the
        // cost of roundtripping a byte of data through disk. For today's value
        // of parameters, this corresponds to about 13megs worth of
        // transactions.
        let size_limit = transactions_gas_limit
            / (runtime_config.wasm_config.ext_costs.storage_write_value_byte
                + runtime_config.wasm_config.ext_costs.storage_read_value_byte);

        while total_gas_burnt < transactions_gas_limit && total_size < size_limit {
            if let Some(iter) = pool_iterator.next() {
                while let Some(tx) = iter.next() {
                    num_checked_transactions += 1;
                    // Verifying the transaction is on the same chain and hasn't expired yet.
                    if chain_validate(&tx) {
                        // Verifying the validity of the transaction based on the current state.
                        match verify_and_charge_transaction(
                            runtime_config,
                            &mut state_update,
                            gas_price,
                            &tx,
                            false,
                            Some(next_block_height),
                            current_protocol_version,
                        ) {
                            Ok(verification_result) => {
                                state_update.commit(StateChangeCause::NotWritableToDisk);
                                total_gas_burnt += verification_result.gas_burnt;
                                total_size += tx.get_size();
                                transactions.push(tx);
                                break;
                            }
                            Err(RuntimeError::InvalidTxError(_err)) => {
                                state_update.rollback();
                            }
                            Err(RuntimeError::StorageError(err)) => {
                                return Err(Error::StorageError(err))
                            }
                            Err(err) => unreachable!("Unexpected RuntimeError error {:?}", err),
                        }
                    }
                }
            } else {
                break;
            }
        }
        debug!(target: "runtime", "Transaction filtering results {} valid out of {} pulled from the pool", transactions.len(), num_checked_transactions);
        Ok(transactions)
    }

    fn verify_validator_signature(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> Result<bool, Error> {
        let (validator, is_slashed) =
            self.get_validator_by_account_id(epoch_id, last_known_block_hash, account_id)?;
        if is_slashed {
            return Ok(false);
        }
        Ok(signature.verify(data, validator.public_key()))
    }

    fn verify_validator_or_fisherman_signature(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> Result<bool, Error> {
        match self.verify_validator_signature(
            epoch_id,
            last_known_block_hash,
            account_id,
            data,
            signature,
        ) {
            Err(Error::NotAValidator) => {
                let (fisherman, is_slashed) =
                    self.get_fisherman_by_account_id(epoch_id, last_known_block_hash, account_id)?;
                if is_slashed {
                    return Ok(false);
                }
                Ok(signature.verify(data, fisherman.public_key()))
            }
            other => other,
        }
    }

    fn verify_header_signature(&self, header: &BlockHeader) -> Result<bool, Error> {
        let epoch_manager = self.epoch_manager.read();
        let block_producer =
            epoch_manager.get_block_producer_info(header.epoch_id(), header.height())?;
        match epoch_manager.get_block_info(header.prev_hash()) {
            Ok(block_info) => {
                if block_info.slashed().contains_key(block_producer.account_id()) {
                    return Ok(false);
                }
                Ok(header.signature().verify(header.hash().as_ref(), block_producer.public_key()))
            }
            Err(_) => return Err(EpochError::MissingBlock(*header.prev_hash()).into()),
        }
    }

    fn verify_chunk_signature_with_header_parts(
        &self,
        chunk_hash: &ChunkHash,
        signature: &Signature,
        epoch_id: &EpochId,
        last_known_hash: &CryptoHash,
        height_created: BlockHeight,
        shard_id: ShardId,
    ) -> Result<bool, Error> {
        let epoch_manager = self.epoch_manager.read();
        if let Ok(chunk_producer) =
            epoch_manager.get_chunk_producer_info(epoch_id, height_created, shard_id)
        {
            let block_info = epoch_manager.get_block_info(last_known_hash)?;
            if block_info.slashed().contains_key(chunk_producer.account_id()) {
                return Ok(false);
            }
            Ok(signature.verify(chunk_hash.as_ref(), chunk_producer.public_key()))
        } else {
            Err(Error::NotAValidator)
        }
    }

    fn verify_approvals_and_threshold_orphan(
        &self,
        epoch_id: &EpochId,
        doomslug_threshold_mode: DoomslugThresholdMode,
        prev_block_hash: &CryptoHash,
        prev_block_height: BlockHeight,
        block_height: BlockHeight,
        approvals: &[Option<Signature>],
    ) -> Result<(), Error> {
        let info = {
            let epoch_manager = self.epoch_manager.read();
            epoch_manager.get_heuristic_block_approvers_ordered(epoch_id).map_err(Error::from)?
        };

        let message_to_sign = Approval::get_data_for_sig(
            &if prev_block_height + 1 == block_height {
                ApprovalInner::Endorsement(*prev_block_hash)
            } else {
                ApprovalInner::Skip(prev_block_height)
            },
            block_height,
        );

        for (validator, may_be_signature) in info.iter().zip(approvals.iter()) {
            if let Some(signature) = may_be_signature {
                if !signature.verify(message_to_sign.as_ref(), &validator.public_key) {
                    return Err(Error::InvalidApprovals);
                }
            }
        }
        let stakes = info
            .iter()
            .map(|stake| (stake.stake_this_epoch, stake.stake_next_epoch, false))
            .collect::<Vec<_>>();
        if !Doomslug::can_approved_block_be_produced(doomslug_threshold_mode, approvals, &stakes) {
            Err(Error::NotEnoughApprovals)
        } else {
            Ok(())
        }
    }

    fn verify_approval(
        &self,
        prev_block_hash: &CryptoHash,
        prev_block_height: BlockHeight,
        block_height: BlockHeight,
        approvals: &[Option<Signature>],
    ) -> Result<bool, Error> {
        let info = {
            let epoch_manager = self.epoch_manager.read();
            epoch_manager.get_all_block_approvers_ordered(prev_block_hash).map_err(Error::from)?
        };
        if approvals.len() > info.len() {
            return Ok(false);
        }

        let message_to_sign = Approval::get_data_for_sig(
            &if prev_block_height + 1 == block_height {
                ApprovalInner::Endorsement(*prev_block_hash)
            } else {
                ApprovalInner::Skip(prev_block_height)
            },
            block_height,
        );

        for ((validator, is_slashed), may_be_signature) in info.into_iter().zip(approvals.iter()) {
            if let Some(signature) = may_be_signature {
                if is_slashed || !signature.verify(message_to_sign.as_ref(), &validator.public_key)
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, Error> {
        let epoch_manager = self.epoch_manager.read();
        Ok(epoch_manager.get_all_block_producers_ordered(epoch_id, last_known_block_hash)?.to_vec())
    }

    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<(ApprovalStake, bool)>, Error> {
        let epoch_manager = self.epoch_manager.read();
        epoch_manager.get_all_block_approvers_ordered(parent_hash).map_err(Error::from)
    }
    fn get_epoch_chunk_producers(&self, epoch_id: &EpochId) -> Result<Vec<ValidatorStake>, Error> {
        let epoch_manager = self.epoch_manager.read();
        Ok(epoch_manager.get_all_chunk_producers(epoch_id)?.to_vec())
    }

    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, Error> {
        let epoch_manager = self.epoch_manager.read();
        Ok(epoch_manager.get_block_producer_info(epoch_id, height)?.take_account_id())
    }

    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<AccountId, Error> {
        let epoch_manager = self.epoch_manager.read();
        Ok(epoch_manager.get_chunk_producer_info(epoch_id, height, shard_id)?.take_account_id())
    }

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error> {
        let epoch_manager = self.epoch_manager.read();
        match epoch_manager.get_validator_by_account_id(epoch_id, account_id) {
            Ok(Some(validator)) => {
                let block_info = epoch_manager.get_block_info(last_known_block_hash)?;
                Ok((validator, block_info.slashed().contains_key(account_id)))
            }
            Ok(None) => Err(Error::NotAValidator),
            Err(e) => Err(e.into()),
        }
    }

    fn get_fisherman_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error> {
        let epoch_manager = self.epoch_manager.read();
        match epoch_manager.get_fisherman_by_account_id(epoch_id, account_id) {
            Ok(Some(fisherman)) => {
                let block_info = epoch_manager.get_block_info(last_known_block_hash)?;
                Ok((fisherman, block_info.slashed().contains_key(account_id)))
            }
            Ok(None) => Err(Error::NotAValidator),
            Err(e) => Err(e.into()),
        }
    }

    fn num_shards(&self, epoch_id: &EpochId) -> Result<NumShards, Error> {
        let epoch_manager = self.epoch_manager.read();
        Ok(epoch_manager.get_shard_layout(epoch_id).map_err(Error::from)?.num_shards())
    }

    fn get_shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, Error> {
        let epoch_manager = self.epoch_manager.read();
        Ok(epoch_manager.get_shard_layout(epoch_id).map_err(Error::from)?.clone())
    }

    fn get_shard_config(&self, epoch_id: &EpochId) -> Result<ShardConfig, Error> {
        let epoch_manager = self.epoch_manager.read();
        let epoch_config = epoch_manager.get_epoch_config(epoch_id).map_err(Error::from)?;
        Ok(ShardConfig::new(epoch_config))
    }

    fn get_prev_shard_ids(
        &self,
        prev_hash: &CryptoHash,
        shard_ids: Vec<ShardId>,
    ) -> Result<Vec<ShardId>, Error> {
        if self.is_next_block_epoch_start(prev_hash)? {
            let shard_layout = self.get_shard_layout_from_prev_block(prev_hash)?;
            let prev_shard_layout = self.get_shard_layout(&self.get_epoch_id(prev_hash)?)?;
            if prev_shard_layout != shard_layout {
                return Ok(shard_ids
                    .into_iter()
                    .map(|shard_id| {
                        shard_layout.get_parent_shard_id(shard_id).map(|parent_shard_id|{
                            assert!(parent_shard_id < prev_shard_layout.num_shards(),
                                    "invalid shard layout {:?}: parent shard {} does not exist in last shard layout",
                                    shard_layout,
                                    parent_shard_id
                            );
                            parent_shard_id
                        })
                    })
                    .collect::<Result<_, ShardLayoutError>>()?);
            }
        }
        Ok(shard_ids)
    }

    fn get_shard_layout_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<ShardLayout, Error> {
        let epoch_id = self.get_epoch_id_from_prev_block(parent_hash)?;
        self.get_shard_layout(&epoch_id)
    }

    fn shard_id_to_uid(&self, shard_id: ShardId, epoch_id: &EpochId) -> Result<ShardUId, Error> {
        let epoch_manager = self.epoch_manager.read();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id).map_err(Error::from)?;
        Ok(ShardUId::from_shard_id_and_layout(shard_id, &shard_layout))
    }

    fn num_total_parts(&self) -> usize {
        let seats = self.genesis_config.num_block_producer_seats;
        if seats > 1 {
            seats as usize
        } else {
            2
        }
    }

    fn num_data_parts(&self) -> usize {
        let total_parts = self.num_total_parts();
        if total_parts <= 3 {
            1
        } else {
            (total_parts - 1) / 3
        }
    }

    fn account_id_to_shard_id(
        &self,
        account_id: &AccountId,
        epoch_id: &EpochId,
    ) -> Result<ShardId, Error> {
        let epoch_manager = self.epoch_manager.read();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id).map_err(Error::from)?;
        Ok(account_id_to_shard_id(account_id, &shard_layout))
    }

    fn get_part_owner(&self, epoch_id: &EpochId, part_id: u64) -> Result<AccountId, Error> {
        let epoch_manager = self.epoch_manager.read();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id)?;
        let settlement = epoch_info.block_producers_settlement();
        let validator_id = settlement[part_id as usize % settlement.len()];
        Ok(epoch_info.get_validator(validator_id).account_id().clone())
    }

    fn cares_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool {
        self.shard_tracker.care_about_shard(account_id, parent_hash, shard_id, is_me)
    }

    fn will_care_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool {
        self.shard_tracker.will_care_about_shard(account_id, parent_hash, shard_id, is_me)
    }

    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, Error> {
        let epoch_manager = self.epoch_manager.read();
        epoch_manager.is_next_block_epoch_start(parent_hash).map_err(Error::from)
    }

    fn get_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        let epoch_manager = self.epoch_manager.read();
        epoch_manager.get_epoch_id_from_prev_block(parent_hash).map_err(Error::from)
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        let epoch_manager = self.epoch_manager.read();
        epoch_manager.get_next_epoch_id_from_prev_block(parent_hash).map_err(Error::from)
    }

    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, Error> {
        let epoch_manager = self.epoch_manager.read();
        epoch_manager.get_epoch_start_height(block_hash).map_err(Error::from)
    }

    fn get_gc_stop_height(&self, block_hash: &CryptoHash) -> BlockHeight {
        (|| -> Result<BlockHeight, Error> {
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
            Ok(epoch_start_height)
        }())
        .unwrap_or(self.genesis_config.genesis_height)
    }

    fn epoch_exists(&self, epoch_id: &EpochId) -> bool {
        let epoch_manager = self.epoch_manager.read();
        epoch_manager.get_epoch_info(epoch_id).is_ok()
    }

    fn get_epoch_minted_amount(&self, epoch_id: &EpochId) -> Result<Balance, Error> {
        let epoch_manager = self.epoch_manager.read();
        Ok(epoch_manager.get_epoch_info(epoch_id)?.minted_amount())
    }

    // TODO #3488 this likely to be updated
    fn get_epoch_sync_data_hash(
        &self,
        prev_epoch_last_block_hash: &CryptoHash,
        epoch_id: &EpochId,
        next_epoch_id: &EpochId,
    ) -> Result<CryptoHash, Error> {
        let (
            prev_epoch_first_block_info,
            prev_epoch_prev_last_block_info,
            prev_epoch_last_block_info,
            prev_epoch_info,
            cur_epoch_info,
            next_epoch_info,
        ) = self.get_epoch_sync_data(prev_epoch_last_block_hash, epoch_id, next_epoch_id)?;
        let mut data = prev_epoch_first_block_info.try_to_vec().unwrap();
        data.extend(prev_epoch_prev_last_block_info.try_to_vec().unwrap());
        data.extend(prev_epoch_last_block_info.try_to_vec().unwrap());
        data.extend(prev_epoch_info.try_to_vec().unwrap());
        data.extend(cur_epoch_info.try_to_vec().unwrap());
        data.extend(next_epoch_info.try_to_vec().unwrap());
        Ok(hash(data.as_slice()))
    }

    // TODO #3488 this likely to be updated
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
    > {
        let epoch_manager = self.epoch_manager.read();
        let last_block_info = epoch_manager.get_block_info(prev_epoch_last_block_hash)?;
        let prev_epoch_id = last_block_info.epoch_id().clone();
        Ok((
            epoch_manager.get_block_info(last_block_info.epoch_first_block())?,
            epoch_manager.get_block_info(last_block_info.prev_hash())?,
            last_block_info,
            epoch_manager.get_epoch_info(&prev_epoch_id)?,
            epoch_manager.get_epoch_info(epoch_id)?,
            epoch_manager.get_epoch_info(next_epoch_id)?,
        ))
    }

    fn get_epoch_protocol_version(&self, epoch_id: &EpochId) -> Result<ProtocolVersion, Error> {
        let epoch_manager = self.epoch_manager.read();
        Ok(epoch_manager.get_epoch_info(epoch_id)?.protocol_version())
    }

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
    ) -> Result<(), Error> {
        let mut epoch_manager = self.epoch_manager.write();
        epoch_manager
            .init_after_epoch_sync(
                prev_epoch_first_block_info,
                prev_epoch_prev_last_block_info,
                prev_epoch_last_block_info,
                prev_epoch_id,
                prev_epoch_info,
                epoch_id,
                epoch_info,
                next_epoch_id,
                next_epoch_info,
            )?
            .commit()
            .map_err(|err| err.into())
    }

    fn add_validator_proposals(
        &self,
        block_header_info: BlockHeaderInfo,
    ) -> Result<StoreUpdate, Error> {
        // Check that genesis block doesn't have any proposals.
        assert!(
            block_header_info.height > 0
                || (block_header_info.proposals.is_empty()
                    && block_header_info.slashed_validators.is_empty())
        );
        debug!(target: "runtime",
            height = block_header_info.height,
            proposals = ?block_header_info.proposals,
            "add_validator_proposals");
        // Deal with validator proposals and epoch finishing.
        let mut epoch_manager = self.epoch_manager.write();
        let block_info = BlockInfo::new(
            block_header_info.hash,
            block_header_info.height,
            block_header_info.last_finalized_height,
            block_header_info.last_finalized_block_hash,
            block_header_info.prev_hash,
            block_header_info.proposals,
            block_header_info.chunk_mask,
            block_header_info.slashed_validators,
            block_header_info.total_supply,
            block_header_info.latest_protocol_version,
            block_header_info.timestamp_nanosec,
        );
        let rng_seed = block_header_info.random_value.0;
        epoch_manager.record_block_info(block_info, rng_seed).map_err(|err| err.into())
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
        challenges: &ChallengesResult,
        random_seed: CryptoHash,
        generate_storage_proof: bool,
        is_new_chunk: bool,
        is_first_block_with_chunk_of_version: bool,
        states_to_patch: SandboxStatePatch,
    ) -> Result<ApplyTransactionResult, Error> {
        let trie = self.get_trie_for_shard(shard_id, prev_block_hash, state_root.clone())?;

        // TODO (#6316): support chunk nodes caching for TrieRecordingStorage
        if generate_storage_proof {
            panic!("Storage proof generation is not enabled yet");
        }
        // let trie = if generate_storage_proof { trie.recording_reads() } else { trie };
        match self.process_state_update(
            trie,
            shard_id,
            height,
            block_hash,
            block_timestamp,
            prev_block_hash,
            receipts,
            transactions,
            last_validator_proposals,
            gas_price,
            gas_limit,
            challenges,
            random_seed,
            is_new_chunk,
            is_first_block_with_chunk_of_version,
            states_to_patch,
        ) {
            Ok(result) => Ok(result),
            Err(e) => match e {
                Error::StorageError(_) => panic!("{e}"),
                _ => Err(e),
            },
        }
    }

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
        challenges: &ChallengesResult,
        random_value: CryptoHash,
        is_new_chunk: bool,
        is_first_block_with_chunk_of_version: bool,
    ) -> Result<ApplyTransactionResult, Error> {
        let trie = Trie::from_recorded_storage(partial_storage, state_root.clone());
        self.process_state_update(
            trie,
            shard_id,
            height,
            block_hash,
            block_timestamp,
            prev_block_hash,
            receipts,
            transactions,
            last_validator_proposals,
            gas_price,
            gas_limit,
            challenges,
            random_value,
            is_new_chunk,
            is_first_block_with_chunk_of_version,
            Default::default(),
        )
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
    ) -> Result<QueryResponse, near_chain::near_chain_primitives::error::QueryError> {
        match request {
            QueryRequest::ViewAccount { account_id } => {
                let account = self
                    .view_account(&shard_uid, *state_root, account_id)
                    .map_err(|err| {
                    near_chain::near_chain_primitives::error::QueryError::from_view_account_error(
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
                    .map_err(|err| near_chain::near_chain_primitives::error::QueryError::from_view_contract_code_error(err, block_height, *block_hash))?;
                Ok(QueryResponse {
                    kind: QueryResponseKind::ViewCode(contract_code.into()),
                    block_height,
                    block_hash: *block_hash,
                })
            }
            QueryRequest::CallFunction { account_id, method_name, args } => {
                let mut logs = vec![];
                let (epoch_height, current_protocol_version) = {
                    let epoch_manager = self.epoch_manager.read();
                    let epoch_info = epoch_manager.get_epoch_info(epoch_id).map_err(|err| {
                        near_chain::near_chain_primitives::error::QueryError::from_epoch_error(
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
                        &self.epoch_manager,
                        current_protocol_version,
                    )
                    .map_err(|err| near_chain::near_chain_primitives::error::QueryError::from_call_function_error(err, block_height, *block_hash))?;
                Ok(QueryResponse {
                    kind: QueryResponseKind::CallResult(CallResult {
                        result: call_function_result,
                        logs,
                    }),
                    block_height,
                    block_hash: *block_hash,
                })
            }
            QueryRequest::ViewState { account_id, prefix } => {
                let view_state_result = self
                    .view_state(&shard_uid, *state_root, account_id, prefix.as_ref())
                    .map_err(|err| {
                        near_chain::near_chain_primitives::error::QueryError::from_view_state_error(
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
                        near_chain::near_chain_primitives::error::QueryError::from_view_access_key_error(
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
                        near_chain::near_chain_primitives::error::QueryError::from_view_access_key_error(
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

    /// WARNING: this function calls EpochManager::get_epoch_info_aggregator_upto_last
    /// underneath which can be very expensive.
    fn get_validator_info(
        &self,
        epoch_id: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, Error> {
        let epoch_manager = self.epoch_manager.read();
        epoch_manager.get_validator_info(epoch_id).map_err(|e| e.into())
    }

    /// Returns StorageError when storage is inconsistent.
    /// This is possible with the used isolation level + running ViewClient in a separate thread
    /// `block_hash` is a block whose `prev_state_root` is `state_root`
    fn obtain_state_part(
        &self,
        shard_id: ShardId,
        block_hash: &CryptoHash,
        state_root: &StateRoot,
        part_id: PartId,
    ) -> Result<Vec<u8>, Error> {
        let epoch_id = self.get_epoch_id(block_hash)?;
        let shard_uid = self.get_shard_uid_from_epoch_id(shard_id, &epoch_id)?;
        let trie = self.tries.get_view_trie_for_shard(shard_uid, state_root.clone());
        let result = match trie.get_trie_nodes_for_part(part_id) {
            Ok(partial_state) => partial_state,
            Err(e) => {
                error!(target: "runtime",
                       "Can't get_trie_nodes_for_part for block {:?} state root {:?}, part_id {:?}, num_parts {:?}, {:?}",
                       block_hash, state_root, part_id.idx, part_id.total, e
                );
                return Err(e.into());
            }
        }
        .try_to_vec()
        .expect("serializer should not fail");
        Ok(result)
    }

    fn validate_state_part(&self, state_root: &StateRoot, part_id: PartId, data: &Vec<u8>) -> bool {
        match BorshDeserialize::try_from_slice(data) {
            Ok(trie_nodes) => {
                match Trie::validate_trie_nodes_for_part(state_root, part_id, trie_nodes) {
                    Ok(_) => true,
                    // Storage error should not happen
                    Err(_) => false,
                }
            }
            // Deserialization error means we've got the data from malicious peer
            Err(_) => false,
        }
    }

    fn apply_update_to_split_states(
        &self,
        block_hash: &CryptoHash,
        state_roots: HashMap<ShardUId, StateRoot>,
        next_epoch_shard_layout: &ShardLayout,
        state_changes: StateChangesForSplitStates,
    ) -> Result<Vec<ApplySplitStateResult>, Error> {
        let trie_changes = self.tries.apply_state_changes_to_split_states(
            &state_roots,
            state_changes,
            &|account_id| account_id_to_shard_uid(account_id, next_epoch_shard_layout),
        )?;

        Ok(trie_changes
            .into_iter()
            .map(|(shard_uid, trie_changes)| ApplySplitStateResult {
                shard_uid,
                new_root: trie_changes.new_root,
                trie_changes: WrappedTrieChanges::new(
                    self.get_tries(),
                    shard_uid,
                    trie_changes,
                    vec![],
                    *block_hash,
                ),
            })
            .collect())
    }

    fn build_state_for_split_shards(
        &self,
        shard_uid: ShardUId,
        state_root: &StateRoot,
        next_epoch_shard_layout: &ShardLayout,
    ) -> Result<HashMap<ShardUId, StateRoot>, Error> {
        let trie = self.tries.get_view_trie_for_shard(shard_uid, state_root.clone());
        let shard_id = shard_uid.shard_id();
        let new_shards = next_epoch_shard_layout
            .get_split_shard_uids(shard_id)
            .ok_or(Error::InvalidShardId(shard_id))?;
        let mut state_roots: HashMap<_, _> =
            new_shards.iter().map(|shard_uid| (*shard_uid, Trie::EMPTY_ROOT)).collect();
        let split_shard_ids: HashSet<_> = new_shards.into_iter().collect();
        let checked_account_id_to_shard_id = |account_id: &AccountId| {
            let new_shard_uid = account_id_to_shard_uid(account_id, next_epoch_shard_layout);
            // check that all accounts in the shard are mapped the shards that this shard will split
            // to according to shard layout
            assert!(
                split_shard_ids.contains(&new_shard_uid),
                "Inconsistent shard_layout specs. Account {:?} in shard {:?} and in shard {:?}, but the former is not parent shard for the latter",
                account_id,
                shard_uid,
                new_shard_uid,
            );
            new_shard_uid
        };

        let state_root_node = trie.retrieve_root_node()?;
        let num_parts = get_num_state_parts(state_root_node.memory_usage);
        debug!(target: "runtime", "splitting state for shard {} to {} parts to build new states", shard_id, num_parts);
        for part_id in 0..num_parts {
            let trie_items = trie.get_trie_items_for_part(PartId::new(part_id, num_parts))?;
            let (store_update, new_state_roots) = self.tries.add_values_to_split_states(
                &state_roots,
                trie_items.into_iter().map(|(key, value)| (key, Some(value))).collect(),
                &checked_account_id_to_shard_id,
            )?;
            state_roots = new_state_roots;
            store_update.commit()?;
        }
        state_roots = apply_delayed_receipts(
            &self.tries,
            shard_uid,
            *state_root,
            state_roots,
            &checked_account_id_to_shard_id,
        )?;
        Ok(state_roots)
    }

    fn apply_state_part(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        part_id: PartId,
        data: &[u8],
        epoch_id: &EpochId,
    ) -> Result<(), Error> {
        let part = BorshDeserialize::try_from_slice(data)
            .expect("Part was already validated earlier, so could never fail here");
        let ApplyStatePartResult { trie_changes, contract_codes } =
            Trie::apply_state_part(state_root, part_id, part);
        let tries = self.get_tries();
        let shard_uid = self.get_shard_uid_from_epoch_id(shard_id, epoch_id)?;
        let (store_update, _) = tries.apply_all(&trie_changes, shard_uid);
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
        let epoch_id = self.get_epoch_id(block_hash)?;
        let shard_uid = self.get_shard_uid_from_epoch_id(shard_id, &epoch_id)?;
        self.tries
            .get_view_trie_for_shard(shard_uid, state_root.clone())
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
            false
        } else {
            match Trie::get_memory_usage_from_serialized(&state_root_node.data) {
                Ok(memory_usage) => {
                    if memory_usage != state_root_node.memory_usage {
                        // Invalid value of memory_usage
                        false
                    } else {
                        true
                    }
                }
                Err(_) => false, // Invalid state_root_node
            }
        }
    }

    fn compare_epoch_id(
        &self,
        epoch_id: &EpochId,
        other_epoch_id: &EpochId,
    ) -> Result<Ordering, Error> {
        let epoch_manager = self.epoch_manager.read();
        epoch_manager.compare_epoch_id(epoch_id, other_epoch_id).map_err(|e| e.into())
    }

    fn chunk_needs_to_be_fetched_from_archival(
        &self,
        chunk_prev_block_hash: &CryptoHash,
        header_head: &CryptoHash,
    ) -> Result<bool, Error> {
        let epoch_manager = self.epoch_manager.read();
        let head_epoch_id = epoch_manager.get_epoch_id(header_head)?;
        let head_next_epoch_id = epoch_manager.get_next_epoch_id(header_head)?;
        let chunk_epoch_id = epoch_manager.get_epoch_id_from_prev_block(chunk_prev_block_hash)?;
        let chunk_next_epoch_id =
            epoch_manager.get_next_epoch_id_from_prev_block(chunk_prev_block_hash)?;

        // `chunk_epoch_id != head_epoch_id && chunk_next_epoch_id != head_epoch_id` covers the
        // common case: the chunk is in the current epoch, or in the previous epoch, relative to the
        // header head. The third condition (`chunk_epoch_id != head_next_epoch_id`) covers a
        // corner case, in which the `header_head` is the last block of an epoch, and the chunk is
        // for the next block. In this case the `chunk_epoch_id` will be one epoch ahead of the
        // `header_head`.
        Ok(chunk_epoch_id != head_epoch_id
            && chunk_next_epoch_id != head_epoch_id
            && chunk_epoch_id != head_next_epoch_id)
    }

    fn get_protocol_config(&self, epoch_id: &EpochId) -> Result<ProtocolConfig, Error> {
        let protocol_version = self.get_epoch_protocol_version(epoch_id)?;
        let mut genesis_config = self.genesis_config.clone();
        genesis_config.protocol_version = protocol_version;
        let shard_config = {
            let epoch_manager = self.epoch_manager.read();
            epoch_manager.get_shard_config(epoch_id)?
        };
        genesis_config.num_block_producer_seats_per_shard =
            shard_config.num_block_producer_seats_per_shard;
        genesis_config.avg_hidden_validator_seats_per_shard =
            shard_config.avg_hidden_validator_seats_per_shard;
        genesis_config.shard_layout = shard_config.shard_layout;
        let runtime_config =
            self.runtime_config_store.get_config(protocol_version).as_ref().clone();
        Ok(ProtocolConfig { genesis_config, runtime_config })
    }

    fn get_prev_epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        let epoch_manager = self.epoch_manager.read();
        if epoch_manager.is_next_block_epoch_start(prev_block_hash)? {
            epoch_manager.get_epoch_id(prev_block_hash).map_err(Error::from)
        } else {
            epoch_manager.get_prev_epoch_id(prev_block_hash).map_err(Error::from)
        }
    }

    fn will_shard_layout_change_next_epoch(&self, parent_hash: &CryptoHash) -> Result<bool, Error> {
        let epoch_manager = self.epoch_manager.read();
        Ok(epoch_manager.will_shard_layout_change(parent_hash)?)
    }

    fn get_epoch_height_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochHeight, Error> {
        let epoch_manager = self.epoch_manager.read();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        epoch_manager.get_epoch_info(&epoch_id).map(|info| info.epoch_height()).map_err(Error::from)
    }

    fn get_protocol_upgrade_block_height(
        &self,
        block_hash: CryptoHash,
    ) -> Result<Option<BlockHeight>, EpochError> {
        let epoch_manager = self.epoch_manager.read();
        epoch_manager.get_protocol_upgrade_block_height(block_hash)
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
            block_height: height,
            prev_block_hash: *prev_block_hash,
            block_hash: *block_hash,
            epoch_id: epoch_id.clone(),
            epoch_height,
            block_timestamp,
            current_protocol_version,
            cache: Some(Box::new(StoreCompiledContractCache::new(&self.tries.get_store()))),
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
    ) -> Result<ViewStateResult, node_runtime::state_viewer::errors::ViewStateError> {
        let state_update = self.tries.new_trie_update_view(*shard_uid, state_root);
        self.trie_viewer.view_state(&state_update, account_id, prefix)
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use num_rational::Ratio;

    use near_chain_configs::DEFAULT_GC_NUM_EPOCHS_TO_KEEP;
    use near_crypto::{InMemorySigner, KeyType, Signer};
    use near_logger_utils::init_test_logger;
    use near_primitives::block::Tip;
    use near_primitives::challenge::SlashedValidator;
    use near_primitives::transaction::{Action, DeleteAccountAction, StakeAction, TransferAction};
    use near_primitives::types::{BlockHeightDelta, Nonce, ValidatorId, ValidatorKickoutReason};
    use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
    use near_primitives::views::{
        AccountView, CurrentEpochValidatorInfo, NextEpochValidatorInfo, ValidatorKickoutView,
    };

    use crate::config::{GenesisExt, TESTING_INIT_BALANCE, TESTING_INIT_STAKE};

    use super::*;

    use near_primitives::trie_key::TrieKey;
    use primitive_types::U256;

    fn stake(
        nonce: Nonce,
        signer: &dyn Signer,
        sender: &dyn ValidatorSigner,
        stake: Balance,
    ) -> SignedTransaction {
        SignedTransaction::from_actions(
            nonce,
            sender.validator_id().clone(),
            sender.validator_id().clone(),
            &*signer,
            vec![Action::Stake(StakeAction { stake, public_key: sender.public_key() })],
            // runtime does not validate block history
            CryptoHash::default(),
        )
    }

    impl NightshadeRuntime {
        fn update(
            &self,
            state_root: &StateRoot,
            shard_id: ShardId,
            height: BlockHeight,
            block_timestamp: u64,
            prev_block_hash: &CryptoHash,
            block_hash: &CryptoHash,
            receipts: &[Receipt],
            transactions: &[SignedTransaction],
            last_proposals: ValidatorStakeIter,
            gas_price: Balance,
            gas_limit: Gas,
            challenges: &ChallengesResult,
        ) -> (StateRoot, Vec<ValidatorStake>, Vec<Receipt>) {
            let mut result = self
                .apply_transactions(
                    shard_id,
                    state_root,
                    height,
                    block_timestamp,
                    prev_block_hash,
                    block_hash,
                    receipts,
                    transactions,
                    last_proposals,
                    gas_price,
                    gas_limit,
                    challenges,
                    CryptoHash::default(),
                    true,
                    false,
                    Default::default(),
                )
                .unwrap();
            let mut store_update = self.store.store_update();
            result.trie_changes.insertions_into(&mut store_update);
            result.trie_changes.state_changes_into(&mut store_update);
            store_update.commit().unwrap();
            (result.new_root, result.validator_proposals, result.outgoing_receipts)
        }
    }

    struct TestEnv {
        pub runtime: NightshadeRuntime,
        pub head: Tip,
        state_roots: Vec<StateRoot>,
        pub last_receipts: HashMap<ShardId, Vec<Receipt>>,
        pub last_shard_proposals: HashMap<ShardId, Vec<ValidatorStake>>,
        pub last_proposals: Vec<ValidatorStake>,
        time: u64,
    }

    impl TestEnv {
        pub fn new(
            validators: Vec<Vec<AccountId>>,
            epoch_length: BlockHeightDelta,
            has_reward: bool,
        ) -> Self {
            Self::new_with_tracking(
                validators,
                epoch_length,
                TrackedConfig::new_empty(),
                has_reward,
            )
        }

        pub fn new_with_minimum_stake_divisor(
            validators: Vec<Vec<AccountId>>,
            epoch_length: BlockHeightDelta,
            has_reward: bool,
            stake_divisor: u64,
        ) -> Self {
            Self::new_with_tracking_and_minimum_stake_divisor(
                validators,
                epoch_length,
                TrackedConfig::new_empty(),
                has_reward,
                Some(stake_divisor),
            )
        }

        pub fn new_with_tracking(
            validators: Vec<Vec<AccountId>>,
            epoch_length: BlockHeightDelta,
            tracked_config: TrackedConfig,
            has_reward: bool,
        ) -> Self {
            Self::new_with_tracking_and_minimum_stake_divisor(
                validators,
                epoch_length,
                tracked_config,
                has_reward,
                None,
            )
        }

        fn new_with_tracking_and_minimum_stake_divisor(
            validators: Vec<Vec<AccountId>>,
            epoch_length: BlockHeightDelta,
            tracked_config: TrackedConfig,
            has_reward: bool,
            minimum_stake_divisor: Option<u64>,
        ) -> Self {
            let (dir, opener) = Store::test_opener();
            let store = opener.open();
            let all_validators = validators.iter().fold(BTreeSet::new(), |acc, x| {
                acc.union(&x.iter().cloned().collect()).cloned().collect()
            });
            let validators_len = all_validators.len() as ValidatorId;
            let mut genesis = Genesis::test_sharded_new_version(
                all_validators.into_iter().collect(),
                validators_len,
                validators.iter().map(|x| x.len() as ValidatorId).collect(),
            );
            // No fees mode.
            genesis.config.epoch_length = epoch_length;
            genesis.config.chunk_producer_kickout_threshold =
                genesis.config.block_producer_kickout_threshold;
            if !has_reward {
                genesis.config.max_inflation_rate = Ratio::from_integer(0);
            }
            if let Some(minimum_stake_divisor) = minimum_stake_divisor {
                genesis.config.minimum_stake_divisor = minimum_stake_divisor;
            }
            let genesis_total_supply = genesis.config.total_supply;
            let genesis_protocol_version = genesis.config.protocol_version;
            let runtime = NightshadeRuntime::new(
                dir.path(),
                store,
                &genesis,
                tracked_config,
                None,
                None,
                Some(RuntimeConfigStore::free()),
                DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
                Default::default(),
            );
            let (_store, state_roots) = runtime.genesis_state();
            let genesis_hash = hash(&vec![0]);
            runtime
                .add_validator_proposals(BlockHeaderInfo {
                    prev_hash: CryptoHash::default(),
                    hash: genesis_hash,
                    random_value: [0; 32].as_ref().try_into().unwrap(),
                    height: 0,
                    last_finalized_height: 0,
                    last_finalized_block_hash: CryptoHash::default(),
                    proposals: vec![],
                    slashed_validators: vec![],
                    chunk_mask: vec![],
                    total_supply: genesis_total_supply,
                    latest_protocol_version: genesis_protocol_version,
                    timestamp_nanosec: 0,
                })
                .unwrap()
                .commit()
                .unwrap();
            Self {
                runtime,
                head: Tip {
                    last_block_hash: genesis_hash,
                    prev_block_hash: CryptoHash::default(),
                    height: 0,
                    epoch_id: EpochId::default(),
                    next_epoch_id: Default::default(),
                },
                state_roots,
                last_receipts: HashMap::default(),
                last_proposals: vec![],
                last_shard_proposals: HashMap::default(),
                time: 0,
            }
        }

        pub fn step(
            &mut self,
            transactions: Vec<Vec<SignedTransaction>>,
            chunk_mask: Vec<bool>,
            challenges_result: ChallengesResult,
        ) {
            let new_hash = hash(&vec![(self.head.height + 1) as u8]);
            let num_shards = self.runtime.num_shards(&self.head.epoch_id).unwrap();
            assert_eq!(transactions.len() as NumShards, num_shards);
            assert_eq!(chunk_mask.len() as NumShards, num_shards);
            let mut all_proposals = vec![];
            let mut all_receipts = vec![];
            for i in 0..num_shards {
                let (state_root, proposals, receipts) = self.runtime.update(
                    &self.state_roots[i as usize],
                    i,
                    self.head.height + 1,
                    0,
                    &self.head.last_block_hash,
                    &new_hash,
                    self.last_receipts.get(&i).unwrap_or(&vec![]),
                    &transactions[i as usize],
                    ValidatorStakeIter::new(self.last_shard_proposals.get(&i).unwrap_or(&vec![])),
                    self.runtime.genesis_config.min_gas_price,
                    u64::max_value(),
                    &challenges_result,
                );
                self.state_roots[i as usize] = state_root;
                all_receipts.extend(receipts);
                all_proposals.append(&mut proposals.clone());
                self.last_shard_proposals.insert(i as ShardId, proposals);
            }
            self.runtime
                .add_validator_proposals(BlockHeaderInfo {
                    prev_hash: self.head.last_block_hash,
                    hash: new_hash,
                    random_value: [0; 32].as_ref().try_into().unwrap(),
                    height: self.head.height + 1,
                    last_finalized_height: self.head.height.saturating_sub(1),
                    last_finalized_block_hash: self.head.last_block_hash,
                    proposals: self.last_proposals.clone(),
                    slashed_validators: challenges_result,
                    chunk_mask,
                    total_supply: self.runtime.genesis_config.total_supply,
                    latest_protocol_version: self.runtime.genesis_config.protocol_version,
                    timestamp_nanosec: self.time + 10u64.pow(9),
                })
                .unwrap()
                .commit()
                .unwrap();
            let shard_layout = self.runtime.get_shard_layout_from_prev_block(&new_hash).unwrap();
            let mut new_receipts = HashMap::<_, Vec<Receipt>>::new();
            for receipt in all_receipts {
                let shard_id = account_id_to_shard_id(&receipt.receiver_id, &shard_layout);
                new_receipts.entry(shard_id).or_default().push(receipt);
            }
            self.last_receipts = new_receipts;
            self.last_proposals = all_proposals;
            self.time += 10u64.pow(9);

            self.head = Tip {
                last_block_hash: new_hash,
                prev_block_hash: self.head.last_block_hash,
                height: self.head.height + 1,
                epoch_id: self
                    .runtime
                    .get_epoch_id_from_prev_block(&self.head.last_block_hash)
                    .unwrap(),
                next_epoch_id: self
                    .runtime
                    .get_next_epoch_id_from_prev_block(&self.head.last_block_hash)
                    .unwrap(),
            };
        }

        /// Step when there is only one shard
        pub fn step_default(&mut self, transactions: Vec<SignedTransaction>) {
            self.step(vec![transactions], vec![true], ChallengesResult::default());
        }

        pub fn view_account(&self, account_id: &AccountId) -> AccountView {
            let shard_id =
                self.runtime.account_id_to_shard_id(account_id, &self.head.epoch_id).unwrap();
            let shard_uid = self.runtime.shard_id_to_uid(shard_id, &self.head.epoch_id).unwrap();
            self.runtime
                .view_account(&shard_uid, self.state_roots[shard_id as usize], account_id)
                .unwrap()
                .into()
        }

        /// Compute per epoch per validator reward and per epoch protocol treasury reward
        pub fn compute_reward(
            &self,
            num_validators: usize,
            epoch_duration: u64,
        ) -> (Balance, Balance) {
            let num_seconds_per_year = 60 * 60 * 24 * 365;
            let num_ns_in_second = 1_000_000_000;
            let per_epoch_total_reward =
                (U256::from(*self.runtime.genesis_config.max_inflation_rate.numer() as u64)
                    * U256::from(self.runtime.genesis_config.total_supply)
                    * U256::from(epoch_duration)
                    / (U256::from(num_seconds_per_year)
                        * U256::from(
                            *self.runtime.genesis_config.max_inflation_rate.denom() as u128
                        )
                        * U256::from(num_ns_in_second)))
                .as_u128();
            let per_epoch_protocol_treasury = per_epoch_total_reward
                * *self.runtime.genesis_config.protocol_reward_rate.numer() as u128
                / *self.runtime.genesis_config.protocol_reward_rate.denom() as u128;
            let per_epoch_per_validator_reward =
                (per_epoch_total_reward - per_epoch_protocol_treasury) / num_validators as u128;
            (per_epoch_per_validator_reward, per_epoch_protocol_treasury)
        }
    }

    /// Start with 2 validators with default stake X.
    /// 1. Validator 0 stakes 2 * X
    /// 2. Validator 0 creates new account Validator 2 with 3 * X in balance
    /// 3. Validator 2 stakes 2 * X
    /// 4. Validator 1 gets unstaked because not enough stake.
    /// 5. At the end Validator 0 and 2 with 2 * X are validators. Validator 1 has stake returned to balance.
    #[test]
    fn test_validator_rotation() {
        init_test_logger();
        let num_nodes = 2;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], 2, false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let signer = InMemorySigner::from_seed(
            validators[0].clone(),
            KeyType::ED25519,
            validators[0].as_ref(),
        );
        // test1 doubles stake and the new account stakes the same, so test2 will be kicked out.`
        let staking_transaction = stake(1, &signer, &block_producers[0], TESTING_INIT_STAKE * 2);
        let new_account = AccountId::try_from(format!("test{}", num_nodes + 1)).unwrap();
        let new_validator = InMemoryValidatorSigner::from_seed(
            new_account.clone(),
            KeyType::ED25519,
            new_account.as_ref(),
        );
        let new_signer =
            InMemorySigner::from_seed(new_account.clone(), KeyType::ED25519, new_account.as_ref());
        let create_account_transaction = SignedTransaction::create_account(
            2,
            block_producers[0].validator_id().clone(),
            new_account,
            TESTING_INIT_STAKE * 3,
            new_signer.public_key(),
            &signer,
            CryptoHash::default(),
        );
        let test2_stake_amount = 3600 * crate::NEAR_BASE;
        let transactions = {
            // With the new validator selection algorithm, test2 needs to have less stake to
            // become a fisherman.
            let signer = InMemorySigner::from_seed(
                validators[1].clone(),
                KeyType::ED25519,
                validators[1].as_ref(),
            );
            vec![
                staking_transaction,
                create_account_transaction,
                stake(1, &signer, &block_producers[1], test2_stake_amount),
            ]
        };
        env.step_default(transactions);
        env.step_default(vec![]);
        let account = env.view_account(block_producers[0].validator_id());
        assert_eq!(account.locked, 2 * TESTING_INIT_STAKE);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE * 5);

        let stake_transaction =
            stake(env.head.height * 1_000_000, &new_signer, &new_validator, TESTING_INIT_STAKE * 2);
        env.step_default(vec![stake_transaction]);
        env.step_default(vec![]);

        // Roll steps for 3 epochs to pass.
        for _ in 5..=9 {
            env.step_default(vec![]);
        }

        let epoch_id = env.runtime.get_epoch_id_from_prev_block(&env.head.last_block_hash).unwrap();
        assert_eq!(
            env.runtime
                .get_epoch_block_producers_ordered(&epoch_id, &env.head.last_block_hash)
                .unwrap()
                .iter()
                .map(|x| (x.0.account_id().clone(), x.1))
                .collect::<HashMap<_, _>>(),
            vec![("test3".parse().unwrap(), false), ("test1".parse().unwrap(), false)]
                .into_iter()
                .collect::<HashMap<_, _>>()
        );

        let test1_acc = env.view_account(&"test1".parse().unwrap());
        // Staked 2 * X, sent 3 * X to test3.
        assert_eq!(
            (test1_acc.amount, test1_acc.locked),
            (TESTING_INIT_BALANCE - 5 * TESTING_INIT_STAKE, 2 * TESTING_INIT_STAKE)
        );
        let test2_acc = env.view_account(&"test2".parse().unwrap());
        // Become fishermen instead
        assert_eq!(
            (test2_acc.amount, test2_acc.locked),
            (TESTING_INIT_BALANCE - test2_stake_amount, test2_stake_amount)
        );
        let test3_acc = env.view_account(&"test3".parse().unwrap());
        // Got 3 * X, staking 2 * X of them.
        assert_eq!(
            (test3_acc.amount, test3_acc.locked),
            (TESTING_INIT_STAKE, 2 * TESTING_INIT_STAKE)
        );
    }

    /// One validator tries to decrease their stake in epoch T. Make sure that the stake return happens in epoch T+3.
    #[test]
    fn test_validator_stake_change() {
        let num_nodes = 2;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], 2, false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let signer = InMemorySigner::from_seed(
            validators[0].clone(),
            KeyType::ED25519,
            validators[0].as_ref(),
        );

        let desired_stake = 2 * TESTING_INIT_STAKE / 3;
        let staking_transaction = stake(1, &signer, &block_producers[0], desired_stake);
        env.step_default(vec![staking_transaction]);
        let account = env.view_account(block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);
        for _ in 2..=4 {
            env.step_default(vec![]);
        }

        let account = env.view_account(block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        for _ in 5..=7 {
            env.step_default(vec![]);
        }

        let account = env.view_account(block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - desired_stake);
        assert_eq!(account.locked, desired_stake);
    }

    #[test]
    fn test_validator_stake_change_multiple_times() {
        init_test_logger();
        let num_nodes = 4;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], 4, false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();

        let staking_transaction =
            stake(1, &signers[0], &block_producers[0], TESTING_INIT_STAKE - 1);
        let staking_transaction1 =
            stake(2, &signers[0], &block_producers[0], TESTING_INIT_STAKE - 2);
        let staking_transaction2 =
            stake(1, &signers[1], &block_producers[1], TESTING_INIT_STAKE + 1);
        env.step_default(vec![staking_transaction, staking_transaction1, staking_transaction2]);
        let account = env.view_account(block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let staking_transaction =
            stake(3, &signers[0], &block_producers[0], TESTING_INIT_STAKE + 1);
        let staking_transaction1 =
            stake(2, &signers[1], &block_producers[1], TESTING_INIT_STAKE + 2);
        let staking_transaction2 =
            stake(3, &signers[1], &block_producers[1], TESTING_INIT_STAKE - 1);
        let staking_transaction3 =
            stake(1, &signers[3], &block_producers[3], TESTING_INIT_STAKE - 1);
        env.step_default(vec![
            staking_transaction,
            staking_transaction1,
            staking_transaction2,
            staking_transaction3,
        ]);

        for _ in 3..=8 {
            env.step_default(vec![]);
        }

        let account = env.view_account(block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

        let account = env.view_account(block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(block_producers[2].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(block_producers[3].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        for _ in 9..=12 {
            env.step_default(vec![]);
        }

        let account = env.view_account(block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

        let account = env.view_account(block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(block_producers[2].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(block_producers[3].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        for _ in 13..=16 {
            env.step_default(vec![]);
        }

        let account = env.view_account(block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

        let account = env.view_account(block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE - 1);

        let account = env.view_account(block_producers[2].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(block_producers[3].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE - 1);
    }

    #[test]
    fn test_stake_in_last_block_of_an_epoch() {
        init_test_logger();
        let num_nodes = 4;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], 5, false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let staking_transaction =
            stake(1, &signers[0], &block_producers[0], TESTING_INIT_STAKE + TESTING_INIT_STAKE / 6);
        env.step_default(vec![staking_transaction]);
        for _ in 2..10 {
            env.step_default(vec![]);
        }
        let staking_transaction =
            stake(2, &signers[0], &block_producers[0], TESTING_INIT_STAKE + TESTING_INIT_STAKE / 2);
        env.step_default(vec![staking_transaction]);
        env.step_default(vec![]);
        let staking_transaction = stake(3, &signers[0], &block_producers[0], TESTING_INIT_STAKE);
        env.step_default(vec![staking_transaction]);
        for _ in 13..=16 {
            env.step_default(vec![]);
        }
        let account = env.view_account(block_producers[0].validator_id());
        let return_stake = (TESTING_INIT_STAKE + TESTING_INIT_STAKE / 2)
            - (TESTING_INIT_STAKE + TESTING_INIT_STAKE / 6);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - (TESTING_INIT_STAKE + TESTING_INIT_STAKE / 2) + return_stake
        );
        assert_eq!(account.locked, TESTING_INIT_STAKE + TESTING_INIT_STAKE / 2 - return_stake);
    }

    #[test]
    fn test_verify_validator_signature() {
        let validators = (0..2)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let env = TestEnv::new(vec![validators.clone()], 2, true);
        let data = [0; 32];
        let signer = InMemorySigner::from_seed(
            validators[0].clone(),
            KeyType::ED25519,
            validators[0].as_ref(),
        );
        let signature = signer.sign(&data);
        assert!(env
            .runtime
            .verify_validator_signature(
                &env.head.epoch_id,
                &env.head.last_block_hash,
                &validators[0],
                &data,
                &signature
            )
            .unwrap());
    }

    #[test]
    fn test_split_states() {
        init_test_logger();
    }

    #[test]
    fn test_state_sync() {
        init_test_logger();
        let num_nodes = 2;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], 2, false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let signer = InMemorySigner::from_seed(
            validators[0].clone(),
            KeyType::ED25519,
            validators[0].as_ref(),
        );
        let staking_transaction = stake(1, &signer, &block_producers[0], TESTING_INIT_STAKE + 1);
        env.step_default(vec![staking_transaction]);
        env.step_default(vec![]);
        let block_hash = hash(&vec![env.head.height as u8]);
        let state_part = env
            .runtime
            .obtain_state_part(0, &block_hash, &env.state_roots[0], PartId::new(0, 1))
            .unwrap();
        let root_node =
            env.runtime.get_state_root_node(0, &block_hash, &env.state_roots[0]).unwrap();
        let mut new_env = TestEnv::new(vec![validators], 2, false);
        for i in 1..=2 {
            let prev_hash = hash(&[new_env.head.height as u8]);
            let cur_hash = hash(&[(new_env.head.height + 1) as u8]);
            let proposals = if i == 1 {
                vec![ValidatorStake::new(
                    block_producers[0].validator_id().clone(),
                    block_producers[0].public_key(),
                    TESTING_INIT_STAKE + 1,
                )]
            } else {
                vec![]
            };
            new_env
                .runtime
                .add_validator_proposals(BlockHeaderInfo {
                    prev_hash,
                    hash: cur_hash,
                    random_value: [0; 32].as_ref().try_into().unwrap(),
                    height: i,
                    last_finalized_height: i.saturating_sub(2),
                    last_finalized_block_hash: prev_hash,
                    proposals: new_env.last_proposals,
                    slashed_validators: vec![],
                    chunk_mask: vec![true],
                    total_supply: new_env.runtime.genesis_config.total_supply,
                    latest_protocol_version: new_env.runtime.genesis_config.protocol_version,
                    timestamp_nanosec: new_env.time,
                })
                .unwrap()
                .commit()
                .unwrap();
            new_env.head.height = i;
            new_env.head.last_block_hash = cur_hash;
            new_env.head.prev_block_hash = prev_hash;
            new_env.last_proposals = proposals;
            new_env.time += 10u64.pow(9);
        }
        assert!(new_env.runtime.validate_state_root_node(&root_node, &env.state_roots[0]));
        let mut root_node_wrong = root_node;
        root_node_wrong.memory_usage += 1;
        assert!(!new_env.runtime.validate_state_root_node(&root_node_wrong, &env.state_roots[0]));
        root_node_wrong.data = vec![123];
        assert!(!new_env.runtime.validate_state_root_node(&root_node_wrong, &env.state_roots[0]));
        assert!(!new_env.runtime.validate_state_part(
            &Trie::EMPTY_ROOT,
            PartId::new(0, 1),
            &state_part
        ));
        new_env.runtime.validate_state_part(&env.state_roots[0], PartId::new(0, 1), &state_part);
        let epoch_id = &new_env.head.epoch_id;
        new_env
            .runtime
            .apply_state_part(0, &env.state_roots[0], PartId::new(0, 1), &state_part, epoch_id)
            .unwrap();
        new_env.state_roots[0] = env.state_roots[0];
        for _ in 3..=5 {
            new_env.step_default(vec![]);
        }

        let account = new_env.view_account(block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

        let account = new_env.view_account(block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);
    }

    #[test]
    fn test_get_validator_info() {
        let num_nodes = 2;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], 2, false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let signer = InMemorySigner::from_seed(
            validators[0].clone(),
            KeyType::ED25519,
            validators[0].as_ref(),
        );
        let staking_transaction = stake(1, &signer, &block_producers[0], 0);
        let mut expected_blocks = [0, 0];
        let mut expected_chunks = [0, 0];
        let update_validator_stats =
            |env: &mut TestEnv, expected_blocks: &mut [u64], expected_chunks: &mut [u64]| {
                let epoch_id = env.head.epoch_id.clone();
                let height = env.head.height;
                let em = env.runtime.epoch_manager.0.read().unwrap();
                let bp = em.get_block_producer_info(&epoch_id, height).unwrap();
                let cp = em.get_chunk_producer_info(&epoch_id, height, 0).unwrap();

                if bp.account_id().as_ref() == "test1" {
                    expected_blocks[0] += 1;
                } else {
                    expected_blocks[1] += 1;
                }

                if cp.account_id().as_ref() == "test1" {
                    expected_chunks[0] += 1;
                } else {
                    expected_chunks[1] += 1;
                }
            };
        env.step_default(vec![staking_transaction]);
        update_validator_stats(&mut env, &mut expected_blocks, &mut expected_chunks);
        assert!(env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::EpochId(env.head.epoch_id.clone()))
            .is_err());
        env.step_default(vec![]);
        update_validator_stats(&mut env, &mut expected_blocks, &mut expected_chunks);
        let mut current_epoch_validator_info = vec![
            CurrentEpochValidatorInfo {
                account_id: "test1".parse().unwrap(),
                public_key: block_producers[0].public_key(),
                is_slashed: false,
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
                num_produced_blocks: expected_blocks[0],
                num_expected_blocks: expected_blocks[0],
                num_produced_chunks: expected_chunks[0],
                num_expected_chunks: expected_chunks[0],
            },
            CurrentEpochValidatorInfo {
                account_id: "test2".parse().unwrap(),
                public_key: block_producers[1].public_key(),
                is_slashed: false,
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
                num_produced_blocks: expected_blocks[1],
                num_expected_blocks: expected_blocks[1],
                num_produced_chunks: expected_chunks[1],
                num_expected_chunks: expected_chunks[1],
            },
        ];
        let next_epoch_validator_info = vec![
            NextEpochValidatorInfo {
                account_id: "test1".parse().unwrap(),
                public_key: block_producers[0].public_key(),
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
            },
            NextEpochValidatorInfo {
                account_id: "test2".parse().unwrap(),
                public_key: block_producers[1].public_key(),
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
            },
        ];
        let response = env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
            .unwrap();
        assert_eq!(
            response,
            EpochValidatorInfo {
                current_validators: current_epoch_validator_info.clone(),
                next_validators: next_epoch_validator_info,
                current_fishermen: vec![],
                next_fishermen: vec![],
                current_proposals: vec![ValidatorStake::new(
                    "test1".parse().unwrap(),
                    block_producers[0].public_key(),
                    0,
                )
                .into()],
                prev_epoch_kickout: Default::default(),
                epoch_start_height: 1,
                epoch_height: 1,
            }
        );
        expected_blocks = [0, 0];
        expected_chunks = [0, 0];
        env.step_default(vec![]);
        update_validator_stats(&mut env, &mut expected_blocks, &mut expected_chunks);
        let response = env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
            .unwrap();

        current_epoch_validator_info[0].num_produced_blocks = expected_blocks[0];
        current_epoch_validator_info[0].num_expected_blocks = expected_blocks[0];
        current_epoch_validator_info[0].num_produced_chunks = expected_chunks[0];
        current_epoch_validator_info[0].num_expected_chunks = expected_chunks[0];
        current_epoch_validator_info[1].num_produced_blocks = expected_blocks[1];
        current_epoch_validator_info[1].num_expected_blocks = expected_blocks[1];
        current_epoch_validator_info[1].num_produced_chunks = expected_chunks[1];
        current_epoch_validator_info[1].num_expected_chunks = expected_chunks[1];
        assert_eq!(response.current_validators, current_epoch_validator_info);
        assert_eq!(
            response.next_validators,
            vec![NextEpochValidatorInfo {
                account_id: "test2".parse().unwrap(),
                public_key: block_producers[1].public_key(),
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
            }]
        );
        assert!(response.current_proposals.is_empty());
        assert_eq!(
            response.prev_epoch_kickout,
            vec![ValidatorKickoutView {
                account_id: "test1".parse().unwrap(),
                reason: ValidatorKickoutReason::Unstaked
            }]
        );
        assert_eq!(response.epoch_start_height, 3);
    }

    #[test]
    fn test_care_about_shard() {
        init_test_logger();
        let num_nodes = 2;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new_with_tracking(
            vec![validators.clone(), vec![validators[0].clone()]],
            2,
            TrackedConfig::Accounts(vec![validators[1].clone()]),
            true,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let signer = InMemorySigner::from_seed(
            validators[1].clone(),
            KeyType::ED25519,
            validators[1].as_ref(),
        );
        let staking_transaction = stake(1, &signer, &block_producers[1], 0);
        env.step(
            vec![vec![staking_transaction], vec![]],
            vec![true, true],
            ChallengesResult::default(),
        );
        env.step(vec![vec![], vec![]], vec![true, true], ChallengesResult::default());
        assert!(env.runtime.cares_about_shard(
            Some(&validators[0]),
            &env.head.last_block_hash,
            0,
            true
        ));
        // which validator is selected to shard 1 sole validator seat depends on which validator
        // selection algorithm is used
        assert!(
            env.runtime.cares_about_shard(Some(&validators[0]), &env.head.last_block_hash, 1, true)
                ^ env.runtime.cares_about_shard(
                    Some(&validators[1]),
                    &env.head.last_block_hash,
                    1,
                    true
                )
        );
        assert!(env.runtime.cares_about_shard(
            Some(&validators[1]),
            &env.head.last_block_hash,
            0,
            true
        ));

        assert!(env.runtime.will_care_about_shard(
            Some(&validators[0]),
            &env.head.last_block_hash,
            0,
            true
        ));
        assert!(env.runtime.will_care_about_shard(
            Some(&validators[0]),
            &env.head.last_block_hash,
            1,
            true
        ));
        assert!(env.runtime.will_care_about_shard(
            Some(&validators[1]),
            &env.head.last_block_hash,
            0,
            true
        ));
        assert!(!env.runtime.will_care_about_shard(
            Some(&validators[1]),
            &env.head.last_block_hash,
            1,
            true
        ));
    }

    #[test]
    fn test_challenges() {
        let mut env =
            TestEnv::new(vec![vec!["test1".parse().unwrap(), "test2".parse().unwrap()]], 2, true);
        env.step(
            vec![vec![]],
            vec![true],
            vec![SlashedValidator::new("test2".parse().unwrap(), false)],
        );
        assert_eq!(env.view_account(&"test2".parse().unwrap()).locked, 0);
        let mut bps = env
            .runtime
            .get_epoch_block_producers_ordered(&env.head.epoch_id, &env.head.last_block_hash)
            .unwrap()
            .iter()
            .map(|x| (x.0.account_id().clone(), x.1))
            .collect::<Vec<_>>();
        bps.sort_unstable();
        assert_eq!(bps, vec![("test1".parse().unwrap(), false), ("test2".parse().unwrap(), true)]);
        let msg = vec![0, 1, 2];
        let signer = InMemorySigner::from_seed("test2".parse().unwrap(), KeyType::ED25519, "test2");
        let signature = signer.sign(&msg);
        assert!(!env
            .runtime
            .verify_validator_signature(
                &env.head.epoch_id,
                &env.head.last_block_hash,
                &"test2".parse().unwrap(),
                &msg,
                &signature,
            )
            .unwrap());
        // Run for 3 epochs, to finalize the given block and make sure that slashed stake actually correctly propagates.
        for _ in 0..6 {
            env.step(vec![vec![]], vec![true], vec![]);
        }
    }

    /// Test that in case of a double sign, not all stake is slashed if the double signed stake is
    /// less than 33% and all stake is slashed if the stake is more than 33%
    #[test]
    fn test_double_sign_challenge_not_all_slashed() {
        init_test_logger();
        let num_nodes = 3;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], 3, false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();

        let signer = InMemorySigner::from_seed(
            validators[2].clone(),
            KeyType::ED25519,
            validators[2].as_ref(),
        );
        let staking_transaction = stake(1, &signer, &block_producers[2], TESTING_INIT_STAKE / 3);
        env.step(
            vec![vec![staking_transaction]],
            vec![true],
            vec![SlashedValidator::new("test2".parse().unwrap(), true)],
        );
        assert_eq!(env.view_account(&"test2".parse().unwrap()).locked, TESTING_INIT_STAKE);
        let mut bps = env
            .runtime
            .get_epoch_block_producers_ordered(&env.head.epoch_id, &env.head.last_block_hash)
            .unwrap()
            .iter()
            .map(|x| (x.0.account_id().clone(), x.1))
            .collect::<Vec<_>>();
        bps.sort_unstable();
        assert_eq!(
            bps,
            vec![
                ("test1".parse().unwrap(), false),
                ("test2".parse().unwrap(), true),
                ("test3".parse().unwrap(), false)
            ]
        );
        let msg = vec![0, 1, 2];
        let signer = InMemorySigner::from_seed("test2".parse().unwrap(), KeyType::ED25519, "test2");
        let signature = signer.sign(&msg);
        assert!(!env
            .runtime
            .verify_validator_signature(
                &env.head.epoch_id,
                &env.head.last_block_hash,
                &"test2".parse().unwrap(),
                &msg,
                &signature,
            )
            .unwrap());

        for _ in 2..11 {
            env.step(vec![vec![]], vec![true], vec![]);
        }
        env.step(
            vec![vec![]],
            vec![true],
            vec![SlashedValidator::new("test3".parse().unwrap(), true)],
        );
        let account = env.view_account(&"test3".parse().unwrap());
        assert_eq!(account.locked, TESTING_INIT_STAKE / 3);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE / 3);

        for _ in 11..14 {
            env.step_default(vec![]);
        }
        let account = env.view_account(&"test3".parse().unwrap());
        let slashed = (TESTING_INIT_STAKE / 3) * 3 / 4;
        let remaining = TESTING_INIT_STAKE / 3 - slashed;
        assert_eq!(account.locked, remaining);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE / 3);

        for _ in 14..=20 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&"test2".parse().unwrap());
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account = env.view_account(&"test3".parse().unwrap());
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE / 3 + remaining);
    }

    /// Test that double sign from multiple accounts may result in all of their stake slashed.
    #[test]
    fn test_double_sign_challenge_all_slashed() {
        init_test_logger();
        let num_nodes = 5;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], 5, false);
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        env.step(
            vec![vec![]],
            vec![true],
            vec![SlashedValidator::new("test1".parse().unwrap(), true)],
        );
        env.step(
            vec![vec![]],
            vec![true],
            vec![SlashedValidator::new("test2".parse().unwrap(), true)],
        );
        let msg = vec![0, 1, 2];
        for i in 0..=1 {
            let signature = signers[i].sign(&msg);
            assert!(!env
                .runtime
                .verify_validator_signature(
                    &env.head.epoch_id,
                    &env.head.last_block_hash,
                    &AccountId::try_from(format!("test{}", i + 1)).unwrap(),
                    &msg,
                    &signature,
                )
                .unwrap());
        }

        for _ in 3..17 {
            env.step_default(vec![]);
        }
        let account = env.view_account(&"test1".parse().unwrap());
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account = env.view_account(&"test2".parse().unwrap());
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    }

    /// Test that if double sign occurs in the same epoch as other type of challenges all stake
    /// is slashed.
    #[test]
    fn test_double_sign_with_other_challenges() {
        init_test_logger();
        let num_nodes = 3;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators], 5, false);
        env.step(
            vec![vec![]],
            vec![true],
            vec![
                SlashedValidator::new("test1".parse().unwrap(), true),
                SlashedValidator::new("test2".parse().unwrap(), false),
            ],
        );
        env.step(
            vec![vec![]],
            vec![true],
            vec![
                SlashedValidator::new("test1".parse().unwrap(), false),
                SlashedValidator::new("test2".parse().unwrap(), true),
            ],
        );

        for _ in 3..11 {
            env.step_default(vec![]);
        }
        let account = env.view_account(&"test1".parse().unwrap());
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account = env.view_account(&"test2".parse().unwrap());
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    }

    /// Run 4 validators. Two of them first change their stake to below validator threshold but above
    /// fishermen threshold. Make sure their balance is correct. Then one fisherman increases their
    /// stake to become a validator again while the other one decreases to below fishermen threshold.
    /// Check that the first one becomes a validator and the second one gets unstaked completely.
    #[test]
    fn test_fishermen_stake() {
        init_test_logger();
        let num_nodes = 4;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new_with_minimum_stake_divisor(
            vec![validators.clone()],
            4,
            false,
            // We need to be able to stake enough to be fisherman, but not enough to be
            // validator
            20000,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let fishermen_stake = 3300 * crate::NEAR_BASE + 1;

        let staking_transaction = stake(1, &signers[0], &block_producers[0], fishermen_stake);
        let staking_transaction1 = stake(1, &signers[1], &block_producers[1], fishermen_stake);
        env.step_default(vec![staking_transaction, staking_transaction1]);
        let account = env.view_account(block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);
        for _ in 2..=13 {
            env.step_default(vec![]);
        }
        let account0 = env.view_account(block_producers[0].validator_id());
        assert_eq!(account0.locked, fishermen_stake);
        assert_eq!(account0.amount, TESTING_INIT_BALANCE - fishermen_stake);
        let response = env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
            .unwrap();
        assert_eq!(
            response
                .current_fishermen
                .into_iter()
                .map(|fishermen| fishermen.take_account_id())
                .collect::<Vec<_>>(),
            vec!["test1".parse().unwrap(), "test2".parse().unwrap()]
        );
        let staking_transaction = stake(2, &signers[0], &block_producers[0], TESTING_INIT_STAKE);
        let staking_transaction2 = stake(2, &signers[1], &block_producers[1], 0);
        env.step_default(vec![staking_transaction, staking_transaction2]);

        for _ in 13..=25 {
            env.step_default(vec![]);
        }

        let account0 = env.view_account(block_producers[0].validator_id());
        assert_eq!(account0.locked, TESTING_INIT_STAKE);
        assert_eq!(account0.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account1 = env.view_account(block_producers[1].validator_id());
        assert_eq!(account1.locked, 0);
        assert_eq!(account1.amount, TESTING_INIT_BALANCE);
        let response = env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
            .unwrap();
        assert!(response.current_fishermen.is_empty());
    }

    /// Test that when fishermen unstake they get their tokens back.
    #[test]
    fn test_fishermen_unstake() {
        init_test_logger();
        let num_nodes = 2;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new_with_minimum_stake_divisor(
            vec![validators.clone()],
            2,
            false,
            // We need to be able to stake enough to be fisherman, but not enough to be
            // validator
            20000,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let fishermen_stake = 3300 * crate::NEAR_BASE + 1;

        let staking_transaction = stake(1, &signers[0], &block_producers[0], fishermen_stake);
        env.step_default(vec![staking_transaction]);
        for _ in 2..9 {
            env.step_default(vec![]);
        }

        let account0 = env.view_account(block_producers[0].validator_id());
        assert_eq!(account0.locked, fishermen_stake);
        assert_eq!(account0.amount, TESTING_INIT_BALANCE - fishermen_stake);
        let response = env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
            .unwrap();
        assert_eq!(
            response
                .current_fishermen
                .into_iter()
                .map(|fishermen| fishermen.take_account_id())
                .collect::<Vec<_>>(),
            vec!["test1".parse().unwrap()]
        );
        let staking_transaction = stake(2, &signers[0], &block_producers[0], 0);
        env.step_default(vec![staking_transaction]);
        for _ in 10..17 {
            env.step_default(vec![]);
        }

        let account0 = env.view_account(block_producers[0].validator_id());
        assert_eq!(account0.locked, 0);
        assert_eq!(account0.amount, TESTING_INIT_BALANCE);
        let response = env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
            .unwrap();
        assert!(response.current_fishermen.is_empty());
    }

    /// Enable reward and make sure that validators get reward proportional to their stake.
    #[test]
    fn test_validator_reward() {
        init_test_logger();
        let num_nodes = 4;
        let epoch_length = 40;
        let validators =
            (0..num_nodes).map(|i| format!("test{}", i + 1).parse().unwrap()).collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], epoch_length, true);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();

        for _ in 0..(epoch_length + 1) {
            env.step_default(vec![]);
        }

        let (validator_reward, protocol_treasury_reward) =
            env.compute_reward(num_nodes, epoch_length * 10u64.pow(9));
        for i in 0..4 {
            let account = env.view_account(block_producers[i].validator_id());
            assert_eq!(account.locked, TESTING_INIT_STAKE + validator_reward);
        }

        let protocol_treasury_account =
            env.view_account(&env.runtime.genesis_config.protocol_treasury_account);
        assert_eq!(
            protocol_treasury_account.amount,
            TESTING_INIT_BALANCE + protocol_treasury_reward
        );
    }

    #[test]
    fn test_delete_account_after_unstake() {
        init_test_logger();
        let num_nodes = 2;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], 4, false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();

        let staking_transaction1 = stake(1, &signers[1], &block_producers[1], 0);
        env.step_default(vec![staking_transaction1]);
        let account = env.view_account(block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);
        for _ in 2..=5 {
            env.step_default(vec![]);
        }
        let staking_transaction2 = stake(2, &signers[1], &block_producers[1], 1);
        env.step_default(vec![staking_transaction2]);
        for _ in 7..=13 {
            env.step_default(vec![]);
        }
        let account = env.view_account(block_producers[1].validator_id());
        assert_eq!(account.locked, 0);

        let delete_account_transaction = SignedTransaction::from_actions(
            4,
            signers[1].account_id.clone(),
            signers[1].account_id.clone(),
            &signers[1] as &dyn Signer,
            vec![Action::DeleteAccount(DeleteAccountAction {
                beneficiary_id: signers[0].account_id.clone(),
            })],
            // runtime does not validate block history
            CryptoHash::default(),
        );
        env.step_default(vec![delete_account_transaction]);
        for _ in 15..=17 {
            env.step_default(vec![]);
        }
    }

    #[test]
    fn test_proposal_deduped() {
        let num_nodes = 2;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], 4, false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();

        let staking_transaction1 =
            stake(1, &signers[1], &block_producers[1], TESTING_INIT_STAKE - 100);
        let staking_transaction2 =
            stake(2, &signers[1], &block_producers[1], TESTING_INIT_STAKE - 10);
        env.step_default(vec![staking_transaction1, staking_transaction2]);
        assert_eq!(env.last_proposals.len(), 1);
        assert_eq!(env.last_proposals[0].stake(), TESTING_INIT_STAKE - 10);
    }

    #[test]
    fn test_insufficient_stake() {
        let num_nodes = 2;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], 4, false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();

        let staking_transaction1 = stake(1, &signers[1], &block_producers[1], 100);
        let staking_transaction2 =
            stake(2, &signers[1], &block_producers[1], 100 * crate::NEAR_BASE);
        env.step_default(vec![staking_transaction1, staking_transaction2]);
        assert!(env.last_proposals.is_empty());
        let staking_transaction3 = stake(3, &signers[1], &block_producers[1], 0);
        env.step_default(vec![staking_transaction3]);
        assert_eq!(env.last_proposals.len(), 1);
        assert_eq!(env.last_proposals[0].stake(), 0);
    }

    /// Check that flat state is included into trie and is not included into view trie, because we can't apply flat
    /// state optimization to view calls.
    #[test]
    fn test_flat_state_usage() {
        let env = TestEnv::new(vec![vec!["test1".parse().unwrap()]], 4, false);
        let trie =
            env.runtime.get_trie_for_shard(0, &env.head.prev_block_hash, Trie::EMPTY_ROOT).unwrap();
        assert_eq!(trie.flat_state.is_some(), cfg!(feature = "protocol_feature_flat_state"));

        let trie = env
            .runtime
            .get_view_trie_for_shard(0, &env.head.prev_block_hash, Trie::EMPTY_ROOT)
            .unwrap();
        assert!(trie.flat_state.is_none());
    }

    /// Check that querying trie and flat state gives the same result.
    #[test]
    fn test_trie_and_flat_state_equality() {
        let num_nodes = 2;
        let validators = (0..num_nodes)
            .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
            .collect::<Vec<_>>();
        let mut env = TestEnv::new(vec![validators.clone()], 4, false);
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id.clone(), KeyType::ED25519, id.as_ref()))
            .collect();

        let transfer_tx = SignedTransaction::from_actions(
            4,
            signers[0].account_id.clone(),
            validators[1].clone(),
            &signers[0] as &dyn Signer,
            vec![Action::Transfer(TransferAction { deposit: 10 })],
            // runtime does not validate block history
            CryptoHash::default(),
        );
        env.step_default(vec![transfer_tx]);
        for _ in 1..=5 {
            env.step_default(vec![]);
        }

        // Extract account in two ways:
        // - using state trie, which should use flat state after enabling it in the protocol
        // - using view state, which should never use flat state
        let head_prev_block_hash = env.head.prev_block_hash;
        let state_root = env.state_roots[0];
        let state = env.runtime.get_trie_for_shard(0, &head_prev_block_hash, state_root).unwrap();
        let view_state =
            env.runtime.get_trie_for_shard(0, &head_prev_block_hash, state_root).unwrap();
        let trie_key = TrieKey::Account { account_id: validators[1].clone() };
        let key = trie_key.to_vec();

        let state_value = state.get(&key).unwrap().unwrap();
        let account = Account::try_from_slice(&state_value).unwrap();
        assert_eq!(account.amount(), TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 10);

        let view_state_value = view_state.get(&key).unwrap().unwrap();
        assert_eq!(state_value, view_state_value);
    }
}
