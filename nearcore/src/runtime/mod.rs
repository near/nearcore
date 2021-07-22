use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};

use borsh::ser::BorshSerialize;
use borsh::BorshDeserialize;
use tracing::{debug, error, info, warn};

use near_chain::chain::NUM_EPOCHS_TO_KEEP_STORE_DATA;
use near_chain::types::{ApplyTransactionResult, BlockHeaderInfo, ValidatorInfoIdentifier};
use near_chain::{BlockHeader, Error, ErrorKind, RuntimeAdapter};
#[cfg(feature = "protocol_feature_block_header_v3")]
use near_chain::{Doomslug, DoomslugThresholdMode};
use near_chain_configs::{Genesis, GenesisConfig, ProtocolConfig};
#[cfg(feature = "protocol_feature_evm")]
use near_chain_configs::{BETANET_EVM_CHAIN_ID, MAINNET_EVM_CHAIN_ID, TESTNET_EVM_CHAIN_ID};
use near_crypto::{PublicKey, Signature};
use near_epoch_manager::{EpochManager, RewardCalculator};
use near_pool::types::PoolIterator;
use near_primitives::account::{AccessKey, Account};
use near_primitives::block::{Approval, ApprovalInner};
use near_primitives::challenge::ChallengesResult;
use near_primitives::contract::ContractCode;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::EpochConfig;
use near_primitives::errors::{EpochError, InvalidTxError, RuntimeError};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::ChunkHash;
use near_primitives::state_record::{state_record_to_account_id, StateRecord};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
use near_primitives::types::{
    AccountId, ApprovalStake, Balance, BlockHeight, CompiledContractCache, EpochHeight, EpochId,
    EpochInfoProvider, Gas, MerkleHash, NumShards, ShardId, StateChangeCause, StateRoot,
    StateRootNode,
};
use near_primitives::version::ProtocolVersion;
use near_primitives::views::{
    AccessKeyInfoView, CallResult, EpochValidatorInfo, QueryRequest, QueryResponse,
    QueryResponseKind, ViewApplyState, ViewStateResult,
};
use near_vm_runner::precompile_contract;

use near_store::{
    get_genesis_hash, get_genesis_state_roots, set_genesis_hash, set_genesis_state_roots,
    ApplyStatePartResult, ColState, PartialStorage, ShardTries, Store, StoreCompiledContractCache,
    StoreUpdate, Trie, WrappedTrieChanges,
};
use node_runtime::adapter::ViewRuntimeAdapter;
use node_runtime::state_viewer::TrieViewer;
use node_runtime::{
    validate_transaction, verify_and_charge_transaction, ApplyState, Runtime,
    ValidatorAccountsUpdate,
};

use crate::shard_tracker::{account_id_to_shard_id, ShardTracker};
use near_primitives::runtime::config::ActualRuntimeConfig;

use crate::migrations::load_migration_data;
use errors::FromStateViewerErrors;
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

pub mod errors;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";
const STATE_DUMP_FILE: &str = "state_dump";
const GENESIS_ROOTS_FILE: &str = "genesis_roots";

/// Wrapper type for epoch manager to get avoid implementing trait for foreign types.
pub struct SafeEpochManager(pub Arc<RwLock<EpochManager>>);

impl AsRef<RwLock<EpochManager>> for SafeEpochManager {
    fn as_ref(&self) -> &RwLock<EpochManager> {
        self.0.as_ref()
    }
}

impl EpochInfoProvider for SafeEpochManager {
    fn validator_stake(
        &self,
        epoch_id: &EpochId,
        last_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<Option<Balance>, EpochError> {
        let mut epoch_manager = self.0.write().expect(POISONED_LOCK_ERR);
        let slashed = epoch_manager.get_slashed_validators(last_block_hash)?;
        if slashed.contains_key(account_id) {
            return Ok(None);
        }
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id)?;
        Ok(epoch_info.get_validator_id(account_id).map(|id| epoch_info.validator_stake(*id)))
    }

    fn validator_total_stake(
        &self,
        epoch_id: &EpochId,
        last_block_hash: &CryptoHash,
    ) -> Result<Balance, EpochError> {
        let mut epoch_manager = self.0.write().expect(POISONED_LOCK_ERR);
        let slashed = epoch_manager.get_slashed_validators(last_block_hash)?.clone();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id)?;
        Ok(epoch_info
            .validators_iter()
            .filter_map(|info| {
                if slashed.contains_key(info.account_id()) {
                    None
                } else {
                    Some(info.stake())
                }
            })
            .sum())
    }

    fn minimum_stake(&self, prev_block_hash: &CryptoHash) -> Result<Balance, EpochError> {
        let mut epoch_manager = self.0.write().expect(POISONED_LOCK_ERR);
        epoch_manager.minimum_stake(prev_block_hash)
    }
}

/// Defines Nightshade state transition and validator rotation.
/// TODO: this possibly should be merged with the runtime cargo or at least reconciled on the interfaces.
pub struct NightshadeRuntime {
    genesis_config: GenesisConfig,
    /// Runtime configuration.  Note that it may be slightly different than
    /// `genesis_config.runtime_config`.  Consider `max_gas_burnt_view` value
    /// which may be configured per node.
    runtime_config: ActualRuntimeConfig,

    store: Arc<Store>,
    tries: ShardTries,
    trie_viewer: TrieViewer,
    pub runtime: Runtime,
    epoch_manager: SafeEpochManager,
    shard_tracker: ShardTracker,
    genesis_state_roots: Vec<StateRoot>,
    migration_data: Arc<MigrationData>,
}

impl NightshadeRuntime {
    pub fn new(
        home_dir: &Path,
        store: Arc<Store>,
        genesis: &Genesis,
        initial_tracking_accounts: Vec<AccountId>,
        initial_tracking_shards: Vec<ShardId>,
        trie_viewer_state_size_limit: Option<u64>,
        max_gas_burnt_view: Option<Gas>,
    ) -> Self {
        let runtime = Runtime::new();
        let trie_viewer = TrieViewer::new(trie_viewer_state_size_limit, max_gas_burnt_view);
        let genesis_config = genesis.config.clone();
        let runtime_config =
            ActualRuntimeConfig::new(genesis_config.runtime_config.clone(), max_gas_burnt_view);
        let num_shards = genesis.config.num_block_producer_seats_per_shard.len() as NumShards;
        let initial_epoch_config = EpochConfig::from(&genesis_config);
        let reward_calculator = RewardCalculator::new(&genesis_config);
        let state_roots =
            Self::initialize_genesis_state_if_needed(store.clone(), home_dir, genesis);
        let tries = ShardTries::new(
            store.clone(),
            genesis.config.num_block_producer_seats_per_shard.len() as NumShards,
        );
        let epoch_manager = Arc::new(RwLock::new(
            EpochManager::new(
                store.clone(),
                initial_epoch_config,
                genesis_config.protocol_version,
                reward_calculator,
                genesis_config.validators(),
            )
            .expect("Failed to start Epoch Manager"),
        ));
        let shard_tracker = ShardTracker::new(
            initial_tracking_accounts,
            initial_tracking_shards,
            EpochId::default(),
            epoch_manager.clone(),
            num_shards,
        );
        NightshadeRuntime {
            genesis_config,
            runtime_config,
            store,
            tries,
            runtime,
            trie_viewer,
            epoch_manager: SafeEpochManager(epoch_manager),
            shard_tracker,
            genesis_state_roots: state_roots,
            migration_data: Arc::new(load_migration_data(&genesis.config.chain_id)),
        }
    }

    fn get_epoch_height_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochHeight, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        epoch_manager.get_epoch_info(&epoch_id).map(|info| info.epoch_height()).map_err(Error::from)
    }

    fn get_epoch_id(&self, hash: &CryptoHash) -> Result<EpochId, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_epoch_id(hash).map_err(Error::from)
    }

    fn genesis_state_from_dump(store: Arc<Store>, home_dir: &Path) -> Vec<StateRoot> {
        error!(target: "near", "Loading genesis from a state dump file. Do not use this outside of genesis-tools");
        let mut state_file = home_dir.to_path_buf();
        state_file.push(STATE_DUMP_FILE);
        store.load_from_file(ColState, state_file.as_path()).expect("Failed to read state dump");
        let mut roots_files = home_dir.to_path_buf();
        roots_files.push(GENESIS_ROOTS_FILE);
        let data = fs::read(roots_files).expect("Failed to read genesis roots file.");
        let state_roots: Vec<StateRoot> =
            BorshDeserialize::try_from_slice(&data).expect("Failed to deserialize genesis roots");
        state_roots
    }

    fn genesis_state_from_records(store: Arc<Store>, genesis: &Genesis) -> Vec<StateRoot> {
        if !genesis.records.as_ref().is_empty() {
            info!(target: "runtime", "Genesis state has {} records, computing state roots", genesis.records.0.len());
        } else {
            info!(target: "runtime", "Computing state roots from records in file {:?}", genesis.records_file);
        }
        let mut state_roots = vec![];
        let num_shards = genesis.config.num_block_producer_seats_per_shard.len() as NumShards;
        let mut shard_account_ids: Vec<HashSet<AccountId>> =
            (0..num_shards).map(|_| HashSet::new()).collect();
        let mut has_protocol_account = false;
        genesis.for_each_record(|record: &StateRecord| {
            shard_account_ids[state_record_to_shard_id(record, num_shards) as usize]
                .insert(state_record_to_account_id(record).clone());
            if let StateRecord::Account { account_id, .. } = record {
                if account_id == &genesis.config.protocol_treasury_account {
                    has_protocol_account = true;
                }
            }
        });
        assert!(has_protocol_account, "Genesis spec doesn't have protocol treasury account");
        let tries = ShardTries::new(store.clone(), num_shards);
        let runtime = Runtime::new();
        for shard_id in 0..num_shards {
            let validators = genesis
                .config
                .validators
                .iter()
                .filter_map(|account_info| {
                    if account_id_to_shard_id(&account_info.account_id, num_shards) == shard_id {
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
                &genesis,
                &genesis.config.runtime_config,
                shard_account_ids[shard_id as usize].clone(),
            ));
        }
        state_roots
    }

    /// On first start: compute state roots, load genesis state into storage.
    /// After that: return genesis state roots. The state is not guaranteed to be in storage, as
    /// GC and state sync are allowed to delete it.
    pub fn initialize_genesis_state_if_needed(
        store: Arc<Store>,
        home_dir: &Path,
        genesis: &Genesis,
    ) -> Vec<StateRoot> {
        let genesis_hash = genesis.json_hash();
        let stored_hash = get_genesis_hash(&store).expect("Store failed on genesis intialization");
        if let Some(_hash) = stored_hash {
            // TODO: re-enable this check (#4447)
            //assert_eq!(hash, genesis_hash, "Storage already exists, but has a different genesis");
            get_genesis_state_roots(&store)
                .expect("Store failed on genesis intialization")
                .expect("Genesis state roots not found in storage")
        } else {
            let state_roots = Self::initialize_genesis_state(store.clone(), home_dir, genesis);
            let mut store_update = store.store_update();
            set_genesis_hash(&mut store_update, &genesis_hash);
            set_genesis_state_roots(&mut store_update, &state_roots);
            store_update.commit().expect("Store failed on genesis intialization");
            state_roots
        }
    }

    pub fn initialize_genesis_state(
        store: Arc<Store>,
        home_dir: &Path,
        genesis: &Genesis,
    ) -> Vec<StateRoot> {
        let has_records = !genesis.records.as_ref().is_empty();
        let has_dump = {
            let mut state_dump = home_dir.to_path_buf();
            state_dump.push(STATE_DUMP_FILE);
            state_dump.exists()
        };
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

    /// Processes state update.
    fn process_state_update(
        &self,
        trie: Trie,
        state_root: CryptoHash,
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
        states_to_patch: Option<Vec<StateRecord>>,
    ) -> Result<ApplyTransactionResult, Error> {
        let validator_accounts_update = {
            let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
            debug!(target: "runtime",
                   "block height: {}, is next_block_epoch_start {}",
                   block_height,
                   epoch_manager.is_next_block_epoch_start(prev_block_hash).unwrap()
            );

            let mut slashing_info: HashMap<_, _> = challenges_result
                .iter()
                .filter_map(|s| {
                    if self.account_id_to_shard_id(&s.account_id) == shard_id && !s.is_double_sign {
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
                    .filter(|(account_id, _)| self.account_id_to_shard_id(account_id) == shard_id)
                    .collect();
                let validator_rewards = validator_reward
                    .into_iter()
                    .filter(|(account_id, _)| self.account_id_to_shard_id(account_id) == shard_id)
                    .collect();
                let last_proposals = last_validator_proposals
                    .filter(|v| self.account_id_to_shard_id(v.account_id()) == shard_id)
                    .fold(HashMap::new(), |mut acc, v| {
                        let (account_id, stake) = v.account_and_stake();
                        acc.insert(account_id, stake);
                        acc
                    });
                let double_sign_slashing_info: HashMap<_, _> = double_sign_slashing_info
                    .into_iter()
                    .filter(|(account_id, _)| self.account_id_to_shard_id(account_id) == shard_id)
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
                    .filter(|account_id| self.account_id_to_shard_id(account_id) == shard_id),
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
        let epoch_id = self.get_epoch_id_from_prev_block(prev_block_hash)?;
        let prev_block_epoch_id = self.get_epoch_id(prev_block_hash)?;
        let current_protocol_version = self.get_epoch_protocol_version(&epoch_id)?;
        let prev_block_protocol_version = self.get_epoch_protocol_version(&prev_block_epoch_id)?;
        let is_first_block_of_version = current_protocol_version != prev_block_protocol_version;

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
            config: self.runtime_config.for_protocol_version(current_protocol_version).clone(),
            cache: Some(Arc::new(StoreCompiledContractCache { store: self.store.clone() })),
            is_new_chunk,
            #[cfg(feature = "protocol_feature_evm")]
            evm_chain_id: self.evm_chain_id(),
            profile: Default::default(),
            migration_data: Arc::clone(&self.migration_data),
            migration_flags: MigrationFlags {
                is_first_block_of_version,
                is_first_block_with_chunk_of_version,
            },
        };

        let apply_result = self
            .runtime
            .apply(
                trie,
                state_root,
                &validator_accounts_update,
                &apply_state,
                &receipts,
                &transactions,
                &self.epoch_manager,
                states_to_patch,
            )
            .map_err(|e| match e {
                RuntimeError::InvalidTxError(_) => Error::from(ErrorKind::InvalidTransactions),
                // TODO(#2152): process gracefully
                RuntimeError::BalanceMismatchError(e) => panic!("{}", e),
                // TODO(#2152): process gracefully
                RuntimeError::UnexpectedIntegerOverflow => {
                    panic!("RuntimeError::UnexpectedIntegerOverflow")
                }
                RuntimeError::StorageError(e) => Error::from(ErrorKind::StorageError(e)),
                // TODO(#2152): process gracefully
                RuntimeError::ReceiptValidationError(e) => panic!("{}", e),
                RuntimeError::ValidatorError(e) => e.into(),
            })?;

        let total_gas_burnt =
            apply_result.outcomes.iter().map(|tx_result| tx_result.outcome.gas_burnt).sum();
        let total_balance_burnt = apply_result
            .stats
            .tx_burnt_amount
            .checked_add(apply_result.stats.other_burnt_amount)
            .and_then(|result| result.checked_add(apply_result.stats.slashed_burnt_amount))
            .ok_or_else(|| {
                ErrorKind::Other("Integer overflow during burnt balance summation".to_string())
            })?;

        // Sort the receipts into appropriate outgoing shards.
        let mut receipt_result = HashMap::default();
        for receipt in apply_result.outgoing_receipts {
            receipt_result
                .entry(self.account_id_to_shard_id(&receipt.receiver_id))
                .or_insert_with(|| vec![])
                .push(receipt);
        }

        let result = ApplyTransactionResult {
            trie_changes: WrappedTrieChanges::new(
                self.get_tries(),
                shard_id,
                apply_result.trie_changes,
                apply_result.state_changes,
                block_hash.clone(),
            ),
            new_root: apply_result.state_root,
            outcomes: apply_result.outcomes,
            receipt_result,
            validator_proposals: apply_result.validator_proposals,
            total_gas_burnt,
            total_balance_burnt,
            proof: apply_result.proof,
        };

        Ok(result)
    }

    fn precompile_contracts(
        &self,
        epoch_id: &EpochId,
        contract_codes: Vec<ContractCode>,
    ) -> Result<(), Error> {
        let protocol_version = self.get_epoch_protocol_version(epoch_id)?;
        let runtime_config = self.runtime_config.for_protocol_version(protocol_version);
        let compiled_contract_cache: Option<Arc<dyn CompiledContractCache>> =
            Some(Arc::new(StoreCompiledContractCache { store: self.store.clone() }));
        // Execute precompile_contract in parallel but prevent it from using more than half of all
        // threads so that node will still function normally.
        rayon::ThreadPoolBuilder::new()
            .num_threads(std::cmp::max(rayon::current_num_threads() / 2, 1))
            .build()
            .unwrap()
            .install(|| {
                contract_codes.par_iter().for_each(|code| {
                    precompile_contract(
                        &code,
                        &runtime_config.wasm_config,
                        compiled_contract_cache.as_deref(),
                    )
                    .ok();
                })
            });
        Ok(())
    }
}

pub fn state_record_to_shard_id(state_record: &StateRecord, num_shards: NumShards) -> ShardId {
    account_id_to_shard_id(state_record_to_account_id(state_record), num_shards)
}

impl RuntimeAdapter for NightshadeRuntime {
    fn genesis_state(&self) -> (Arc<Store>, Vec<StateRoot>) {
        (self.store.clone(), self.genesis_state_roots.clone())
    }

    fn get_tries(&self) -> ShardTries {
        self.tries.clone()
    }

    fn get_trie_for_shard(&self, shard_id: ShardId) -> Trie {
        self.tries.get_trie_for_shard(shard_id)
    }

    fn get_view_trie_for_shard(&self, shard_id: ShardId) -> Trie {
        self.tries.get_view_trie_for_shard(shard_id)
    }

    fn verify_block_vrf(
        &self,
        epoch_id: &EpochId,
        block_height: BlockHeight,
        prev_random_value: &CryptoHash,
        vrf_value: &near_crypto::vrf::Value,
        vrf_proof: &near_crypto::vrf::Proof,
    ) -> Result<(), Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        let validator = epoch_manager.get_block_producer_info(&epoch_id, block_height)?;
        let public_key = near_crypto::key_conversion::convert_public_key(
            validator.public_key().unwrap_as_ed25519(),
        )
        .unwrap();

        if !public_key.is_vrf_valid(&prev_random_value.as_ref(), vrf_value, vrf_proof) {
            return Err(ErrorKind::InvalidRandomnessBeaconOutput.into());
        }
        Ok(())
    }

    fn validate_tx(
        &self,
        gas_price: Balance,
        state_root: Option<StateRoot>,
        transaction: &SignedTransaction,
        verify_signature: bool,
        current_protocol_version: ProtocolVersion,
    ) -> Result<Option<InvalidTxError>, Error> {
        let runtime_config = self.runtime_config.for_protocol_version(current_protocol_version);

        if let Some(state_root) = state_root {
            let shard_id = self.account_id_to_shard_id(&transaction.transaction.signer_id);
            let mut state_update = self.get_tries().new_trie_update(shard_id, state_root);

            match verify_and_charge_transaction(
                runtime_config,
                &mut state_update,
                gas_price,
                &transaction,
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
                Err(RuntimeError::StorageError(err)) => {
                    Err(Error::from(ErrorKind::StorageError(err)))
                }
                Err(err) => unreachable!("Unexpected RuntimeError error {:?}", err),
            }
        } else {
            // Doing basic validation without a state root
            match validate_transaction(
                runtime_config,
                gas_price,
                &transaction,
                verify_signature,
                current_protocol_version,
            ) {
                Ok(_) => Ok(None),
                Err(RuntimeError::InvalidTxError(err)) => {
                    debug!(target: "runtime", "Tx {:?} validation failed: {:?}", transaction, err);
                    Ok(Some(err))
                }
                Err(RuntimeError::StorageError(err)) => {
                    Err(Error::from(ErrorKind::StorageError(err)))
                }
                Err(err) => unreachable!("Unexpected RuntimeError error {:?}", err),
            }
        }
    }

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
    ) -> Result<Vec<SignedTransaction>, Error> {
        let mut state_update = self.get_tries().new_trie_update(shard_id, state_root);

        // Total amount of gas burnt for converting transactions towards receipts.
        let mut total_gas_burnt = 0;
        // TODO: Update gas limit for transactions
        let transactions_gas_limit = gas_limit / 2;
        let mut transactions = vec![];
        let mut num_checked_transactions = 0;

        let runtime_config = self.runtime_config.for_protocol_version(current_protocol_version);

        while total_gas_burnt < transactions_gas_limit {
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
                                transactions.push(tx);
                                total_gas_burnt += verification_result.gas_burnt;
                                break;
                            }
                            Err(RuntimeError::InvalidTxError(_err)) => {
                                state_update.rollback();
                            }
                            Err(RuntimeError::StorageError(err)) => {
                                return Err(Error::from(ErrorKind::StorageError(err)))
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
            Err(e) if e.kind() == ErrorKind::NotAValidator => {
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
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        let block_producer =
            epoch_manager.get_block_producer_info(&header.epoch_id(), header.height())?;
        let slashed = match epoch_manager.get_slashed_validators(header.prev_hash()) {
            Ok(slashed) => slashed,
            Err(_) => return Err(EpochError::MissingBlock(*header.prev_hash()).into()),
        };
        if slashed.contains_key(block_producer.account_id()) {
            return Ok(false);
        }
        Ok(header.signature().verify(header.hash().as_ref(), block_producer.public_key()))
    }

    fn verify_chunk_signature_with_header_parts(
        &self,
        chunk_hash: &ChunkHash,
        signature: &Signature,
        prev_block_hash: &CryptoHash,
        height_created: BlockHeight,
        shard_id: ShardId,
    ) -> Result<bool, Error> {
        let epoch_id = self.get_epoch_id_from_prev_block(prev_block_hash)?;
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        if let Ok(chunk_producer) =
            epoch_manager.get_chunk_producer_info(&epoch_id, height_created, shard_id)
        {
            let slashed = epoch_manager.get_slashed_validators(prev_block_hash)?;
            if slashed.contains_key(chunk_producer.account_id()) {
                return Ok(false);
            }
            Ok(signature.verify(chunk_hash.as_ref(), chunk_producer.public_key()))
        } else {
            Err(ErrorKind::NotAValidator.into())
        }
    }

    #[cfg(feature = "protocol_feature_block_header_v3")]
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
            let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
            epoch_manager.get_heuristic_block_approvers_ordered(epoch_id).map_err(Error::from)?
        };

        let message_to_sign = Approval::get_data_for_sig(
            &if prev_block_height + 1 == block_height {
                ApprovalInner::Endorsement(prev_block_hash.clone())
            } else {
                ApprovalInner::Skip(prev_block_height)
            },
            block_height,
        );

        for (validator, may_be_signature) in info.iter().zip(approvals.iter()) {
            if let Some(signature) = may_be_signature {
                if !signature.verify(message_to_sign.as_ref(), &validator.public_key) {
                    return Err(ErrorKind::InvalidApprovals.into());
                }
            }
        }
        let stakes = info
            .iter()
            .map(|stake| (stake.stake_this_epoch, stake.stake_next_epoch, false))
            .collect::<Vec<_>>();
        if !Doomslug::can_approved_block_be_produced(doomslug_threshold_mode, approvals, &stakes) {
            Err(ErrorKind::NotEnoughApprovals.into())
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
            let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
            epoch_manager.get_all_block_approvers_ordered(prev_block_hash).map_err(Error::from)?
        };
        if approvals.len() > info.len() {
            return Ok(false);
        }

        let message_to_sign = Approval::get_data_for_sig(
            &if prev_block_height + 1 == block_height {
                ApprovalInner::Endorsement(prev_block_hash.clone())
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
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        Ok(epoch_manager.get_all_block_producers_ordered(epoch_id, last_known_block_hash)?.to_vec())
    }

    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<(ApprovalStake, bool)>, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_all_block_approvers_ordered(parent_hash).map_err(Error::from)
    }

    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        Ok(epoch_manager.get_block_producer_info(epoch_id, height)?.take_account_id())
    }

    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<AccountId, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        Ok(epoch_manager.get_chunk_producer_info(epoch_id, height, shard_id)?.take_account_id())
    }

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        match epoch_manager.get_validator_by_account_id(epoch_id, account_id) {
            Ok(Some(validator)) => {
                let slashed = epoch_manager.get_slashed_validators(&last_known_block_hash)?;
                Ok((validator, slashed.contains_key(account_id)))
            }
            Ok(None) => Err(ErrorKind::NotAValidator.into()),
            Err(e) => Err(e.into()),
        }
    }

    fn get_fisherman_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        match epoch_manager.get_fisherman_by_account_id(epoch_id, account_id) {
            Ok(Some(fisherman)) => {
                let slashed = epoch_manager.get_slashed_validators(&last_known_block_hash)?;
                Ok((fisherman, slashed.contains_key(account_id)))
            }
            Ok(None) => Err(ErrorKind::NotAValidator.into()),
            Err(e) => Err(e.into()),
        }
    }

    fn num_shards(&self) -> NumShards {
        // TODO: should be dynamic.
        self.genesis_config.num_block_producer_seats_per_shard.len() as NumShards
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

    fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        account_id_to_shard_id(account_id, self.num_shards())
    }

    fn get_part_owner(&self, parent_hash: &CryptoHash, part_id: u64) -> Result<String, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash)?;
        let settlement =
            epoch_manager.get_all_block_producers_settlement(&epoch_id, parent_hash)?;
        Ok(settlement[part_id as usize % settlement.len()].0.account_id().clone())
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
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        epoch_manager.is_next_block_epoch_start(parent_hash).map_err(Error::from)
    }

    fn get_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_epoch_id_from_prev_block(parent_hash).map_err(Error::from)
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_next_epoch_id_from_prev_block(parent_hash).map_err(Error::from)
    }

    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_epoch_start_height(block_hash).map_err(Error::from)
    }

    fn get_gc_stop_height(&self, block_hash: &CryptoHash) -> BlockHeight {
        let genesis_height = self.genesis_config.genesis_height;
        macro_rules! unwrap_result_or_return {
            ($obj: expr) => {
                match $obj {
                    Ok(value) => value,
                    Err(_) => {
                        return genesis_height;
                    }
                }
            };
        }
        let get_gc_stop_height_inner = || -> Result<BlockHeight, Error> {
            let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
            // an epoch must have a first block.
            let epoch_first_block = *epoch_manager.get_block_info(block_hash)?.epoch_first_block();
            let epoch_first_block_info = epoch_manager.get_block_info(&epoch_first_block)?;
            // maintain pointers to avoid cloning.
            let mut last_block_in_prev_epoch = *epoch_first_block_info.prev_hash();
            let mut epoch_start_height = *epoch_first_block_info.height();
            for _ in 0..NUM_EPOCHS_TO_KEEP_STORE_DATA - 1 {
                let epoch_first_block =
                    *epoch_manager.get_block_info(&last_block_in_prev_epoch)?.epoch_first_block();
                let epoch_first_block_info = epoch_manager.get_block_info(&epoch_first_block)?;
                epoch_start_height = *epoch_first_block_info.height();
                last_block_in_prev_epoch = *epoch_first_block_info.prev_hash();
            }
            Ok(epoch_start_height)
        };
        unwrap_result_or_return!(get_gc_stop_height_inner())
    }

    fn epoch_exists(&self, epoch_id: &EpochId) -> bool {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_epoch_info(epoch_id).is_ok()
    }

    fn get_epoch_minted_amount(&self, epoch_id: &EpochId) -> Result<Balance, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
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
    ) -> Result<(BlockInfo, BlockInfo, BlockInfo, EpochInfo, EpochInfo, EpochInfo), Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        let last_block_info = epoch_manager.get_block_info(prev_epoch_last_block_hash)?.clone();
        let prev_epoch_id = last_block_info.epoch_id().clone();
        Ok((
            epoch_manager.get_block_info(last_block_info.epoch_first_block())?.clone(),
            epoch_manager.get_block_info(last_block_info.prev_hash())?.clone(),
            last_block_info,
            epoch_manager.get_epoch_info(&prev_epoch_id)?.clone(),
            epoch_manager.get_epoch_info(epoch_id)?.clone(),
            epoch_manager.get_epoch_info(next_epoch_id)?.clone(),
        ))
    }

    fn get_epoch_protocol_version(&self, epoch_id: &EpochId) -> Result<ProtocolVersion, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
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
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
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
        debug!(target: "runtime", "add validator proposals at block height {} {:?}", block_header_info.height, block_header_info.proposals);
        // Deal with validator proposals and epoch finishing.
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
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
        states_to_patch: Option<Vec<StateRecord>>,
    ) -> Result<ApplyTransactionResult, Error> {
        let trie = self.get_trie_for_shard(shard_id);
        let trie = if generate_storage_proof { trie.recording_reads() } else { trie };
        match self.process_state_update(
            trie,
            *state_root,
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
            Err(e) => match e.kind() {
                ErrorKind::StorageError(_) => {
                    panic!("{}", e);
                }
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
        let trie = Trie::from_recorded_storage(partial_storage);
        self.process_state_update(
            trie,
            *state_root,
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
            None,
        )
    }

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
    ) -> Result<QueryResponse, near_chain::near_chain_primitives::error::QueryError> {
        match request {
            QueryRequest::ViewAccount { account_id } => {
                let account = self
                    .view_account(shard_id, *state_root, account_id)
                    .map_err(|err| near_chain::near_chain_primitives::error::QueryError::from_view_account_error(err, block_height, *block_hash))?;
                Ok(QueryResponse {
                    kind: QueryResponseKind::ViewAccount(account.into()),
                    block_height,
                    block_hash: *block_hash,
                })
            }
            QueryRequest::ViewCode { account_id } => {
                let contract_code = self
                    .view_contract_code(shard_id, *state_root, account_id)
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
                    let mut epoch_manager =
                        self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
                    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).map_err(|err| {
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
                        shard_id,
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
                        #[cfg(feature = "protocol_feature_evm")]
                        self.evm_chain_id(),
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
                    .view_state(shard_id, *state_root, account_id, prefix.as_ref())
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
                    self.view_access_keys(shard_id, *state_root, account_id).map_err(|err| {
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
                    .view_access_key(shard_id, *state_root, account_id, public_key)
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

    fn get_validator_info(
        &self,
        epoch_id: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_validator_info(epoch_id).map_err(|e| e.into())
    }

    /// Returns StorageError when storage is inconsistent.
    /// This is possible with the used isolation level + running ViewClient in a separate thread
    fn obtain_state_part(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        part_id: u64,
        num_parts: u64,
    ) -> Result<Vec<u8>, Error> {
        assert!(part_id < num_parts);
        let trie = self.get_view_trie_for_shard(shard_id);
        let result = match trie.get_trie_nodes_for_part(part_id, num_parts, state_root) {
            Ok(partial_state) => partial_state,
            Err(e) => {
                error!(target: "runtime",
                       "Can't get_trie_nodes_for_part for {:?}, part_id {:?}, num_parts {:?}, {:?}",
                       state_root, part_id, num_parts, e
                );
                return Err(e.to_string().into());
            }
        }
        .try_to_vec()
        .expect("serializer should not fail");
        Ok(result)
    }

    fn validate_state_part(
        &self,
        state_root: &StateRoot,
        part_id: u64,
        num_parts: u64,
        data: &Vec<u8>,
    ) -> bool {
        assert!(part_id < num_parts);
        match BorshDeserialize::try_from_slice(data) {
            Ok(trie_nodes) => {
                match Trie::validate_trie_nodes_for_part(state_root, part_id, num_parts, trie_nodes)
                {
                    Ok(_) => true,
                    // Storage error should not happen
                    Err(_) => false,
                }
            }
            // Deserialization error means we've got the data from malicious peer
            Err(_) => false,
        }
    }

    fn apply_state_part(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        part_id: u64,
        num_parts: u64,
        data: &[u8],
        epoch_id: &EpochId,
    ) -> Result<(), Error> {
        let part = BorshDeserialize::try_from_slice(data)
            .expect("Part was already validated earlier, so could never fail here");
        let ApplyStatePartResult { trie_changes, contract_codes } =
            Trie::apply_state_part(&state_root, part_id, num_parts, part);
        let tries = self.get_tries();
        let (store_update, _) =
            tries.apply_all(&trie_changes, shard_id).expect("TrieChanges::into never fails");
        self.precompile_contracts(epoch_id, contract_codes)?;
        Ok(store_update.commit()?)
    }

    fn get_state_root_node(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
    ) -> Result<StateRootNode, Error> {
        self.get_view_trie_for_shard(shard_id)
            .retrieve_root_node(state_root)
            .map_err(|e| e.to_string().into())
    }

    fn validate_state_root_node(
        &self,
        state_root_node: &StateRootNode,
        state_root: &StateRoot,
    ) -> bool {
        if state_root == &CryptoHash::default() {
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
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        epoch_manager.compare_epoch_id(epoch_id, other_epoch_id).map_err(|e| e.into())
    }

    fn chunk_needs_to_be_fetched_from_archival(
        &self,
        chunk_prev_block_hash: &CryptoHash,
        header_head: &CryptoHash,
    ) -> Result<bool, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
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

    #[cfg(feature = "protocol_feature_evm")]
    /// ID of the EVM chain: https://github.com/ethereum-lists/chains
    fn evm_chain_id(&self) -> u64 {
        match self.genesis_config.chain_id.as_str() {
            "mainnet" => MAINNET_EVM_CHAIN_ID,
            "testnet" => TESTNET_EVM_CHAIN_ID,
            _ => BETANET_EVM_CHAIN_ID,
        }
    }

    fn get_protocol_config(&self, epoch_id: &EpochId) -> Result<ProtocolConfig, Error> {
        let protocol_version = self.get_epoch_protocol_version(epoch_id)?;
        let mut config = self.genesis_config.clone();
        config.protocol_version = protocol_version;
        // If we were initialised with a custom max_gas_burnt_view (see
        // ActualRuntimeConfig::new method), keep the value from genesis file.
        // Since max_gas_burnt_view never changes as a result of protocol
        // upgrade, we can use it when reporting protocol config.
        let gas = config.runtime_config.wasm_config.limit_config.max_gas_burnt_view;
        // Currently only runtime config is changed through protocol upgrades.
        config.runtime_config =
            (**self.runtime_config.for_protocol_version(protocol_version)).clone();
        config.runtime_config.wasm_config.limit_config.max_gas_burnt_view = gas;
        Ok(config)
    }

    fn get_prev_epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        let mut epoch_manager = self.epoch_manager.as_ref().write().expect(POISONED_LOCK_ERR);
        if epoch_manager.is_next_block_epoch_start(prev_block_hash)? {
            epoch_manager.get_epoch_id(prev_block_hash).map_err(Error::from)
        } else {
            epoch_manager.get_prev_epoch_id(prev_block_hash).map_err(Error::from)
        }
    }
}

impl node_runtime::adapter::ViewRuntimeAdapter for NightshadeRuntime {
    fn view_account(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Account, node_runtime::state_viewer::errors::ViewAccountError> {
        let state_update = self.get_tries().new_trie_update_view(shard_id, state_root);
        self.trie_viewer.view_account(&state_update, account_id)
    }

    fn view_contract_code(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<ContractCode, node_runtime::state_viewer::errors::ViewContractCodeError> {
        let state_update = self.get_tries().new_trie_update_view(shard_id, state_root);
        self.trie_viewer.view_contract_code(&state_update, account_id)
    }

    fn call_function(
        &self,
        shard_id: ShardId,
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
        #[cfg(feature = "protocol_feature_evm")] evm_chain_id: u64,
    ) -> Result<Vec<u8>, node_runtime::state_viewer::errors::CallFunctionError> {
        let state_update = self.get_tries().new_trie_update_view(shard_id, state_root);
        let view_state = ViewApplyState {
            block_height: height,
            prev_block_hash: *prev_block_hash,
            block_hash: *block_hash,
            epoch_id: epoch_id.clone(),
            epoch_height,
            block_timestamp,
            current_protocol_version,
            cache: Some(Arc::new(StoreCompiledContractCache { store: self.tries.get_store() })),
            #[cfg(feature = "protocol_feature_evm")]
            evm_chain_id,
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
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKey, node_runtime::state_viewer::errors::ViewAccessKeyError> {
        let state_update = self.get_tries().new_trie_update_view(shard_id, state_root);
        self.trie_viewer.view_access_key(&state_update, account_id, public_key)
    }

    fn view_access_keys(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Vec<(PublicKey, AccessKey)>, node_runtime::state_viewer::errors::ViewAccessKeyError>
    {
        let state_update = self.get_tries().new_trie_update_view(shard_id, state_root);
        self.trie_viewer.view_access_keys(&state_update, account_id)
    }

    fn view_state(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
        prefix: &[u8],
    ) -> Result<ViewStateResult, node_runtime::state_viewer::errors::ViewStateError> {
        let state_update = self.get_tries().new_trie_update_view(shard_id, state_root);
        self.trie_viewer.view_state(&state_update, account_id, prefix)
    }
}
