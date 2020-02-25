use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use borsh::ser::BorshSerialize;
use borsh::BorshDeserialize;
use log::debug;

use near_chain::types::ApplyTransactionResult;
use near_chain::{BlockHeader, ChainStore, ChainStoreAccess, Error, ErrorKind, RuntimeAdapter};
use near_chain_configs::GenesisConfig;
use near_crypto::{PublicKey, Signature};
use near_epoch_manager::{BlockInfo, EpochConfig, EpochError, EpochManager, RewardCalculator};
use near_pool::types::PoolIterator;
use near_primitives::account::{AccessKey, Account};
use near_primitives::block::Approval;
use near_primitives::challenge::{ChallengesResult, SlashedValidator};
use near_primitives::errors::{InvalidTxError, RuntimeError};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::Receipt;
use near_primitives::serialize::from_base64;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::state_record::StateRecord;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, EpochId, Gas, MerkleHash, NumShards, ShardId,
    StateChangeCause, StateChanges, StateChangesRequest, StateRoot, StateRootNode, ValidatorStake,
    ValidatorStats,
};
use near_primitives::utils::{prefix_for_access_key, ACCOUNT_DATA_SEPARATOR};
use near_primitives::views::{
    AccessKeyInfoView, CallResult, EpochValidatorInfo, QueryError, QueryRequest, QueryResponse,
    QueryResponseKind, ViewStateResult,
};
use near_store::{
    get_access_key_raw, ColState, PartialStorage, Store, StoreUpdate, Trie, TrieUpdate,
    WrappedTrieChanges,
};
use node_runtime::adapter::ViewRuntimeAdapter;
use node_runtime::state_viewer::TrieViewer;
use node_runtime::{verify_and_charge_transaction, ApplyState, Runtime, ValidatorAccountsUpdate};

use crate::shard_tracker::{account_id_to_shard_id, ShardTracker};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";
const STATE_DUMP_FILE: &str = "state_dump";
const GENESIS_ROOTS_FILE: &str = "genesis_roots";

/// Defines Nightshade state transition and validator rotation.
/// TODO: this possibly should be merged with the runtime cargo or at least reconciled on the interfaces.
pub struct NightshadeRuntime {
    genesis_config: GenesisConfig,
    home_dir: PathBuf,

    store: Arc<Store>,
    pub trie: Arc<Trie>,
    trie_viewer: TrieViewer,
    pub runtime: Runtime,
    epoch_manager: Arc<RwLock<EpochManager>>,
    shard_tracker: ShardTracker,
}

impl NightshadeRuntime {
    pub fn new(
        home_dir: &Path,
        store: Arc<Store>,
        genesis_config: GenesisConfig,
        initial_tracking_accounts: Vec<AccountId>,
        initial_tracking_shards: Vec<ShardId>,
    ) -> Self {
        let trie = Arc::new(Trie::new(store.clone()));
        let runtime = Runtime::new(genesis_config.runtime_config.clone());
        let trie_viewer = TrieViewer::new();
        let num_shards = genesis_config.num_block_producer_seats_per_shard.len() as NumShards;
        let initial_epoch_config = EpochConfig {
            epoch_length: genesis_config.epoch_length,
            num_shards,
            num_block_producer_seats: genesis_config.num_block_producer_seats,
            num_block_producer_seats_per_shard: genesis_config
                .num_block_producer_seats_per_shard
                .clone(),
            avg_hidden_validator_seats_per_shard: genesis_config
                .avg_hidden_validator_seats_per_shard
                .clone(),
            block_producer_kickout_threshold: genesis_config.block_producer_kickout_threshold,
            chunk_producer_kickout_threshold: genesis_config.chunk_producer_kickout_threshold,
            fishermen_threshold: genesis_config.fishermen_threshold,
        };
        let reward_calculator = RewardCalculator {
            max_inflation_rate: genesis_config.max_inflation_rate,
            num_blocks_per_year: genesis_config.num_blocks_per_year,
            epoch_length: genesis_config.epoch_length,
            validator_reward_percentage: 100 - genesis_config.developer_reward_percentage,
            protocol_reward_percentage: genesis_config.protocol_reward_percentage,
            protocol_treasury_account: genesis_config.protocol_treasury_account.to_string(),
        };
        let epoch_manager = Arc::new(RwLock::new(
            EpochManager::new(
                store.clone(),
                initial_epoch_config,
                reward_calculator,
                genesis_config
                    .validators
                    .iter()
                    .map(|account_info| ValidatorStake {
                        account_id: account_info.account_id.clone(),
                        public_key: account_info
                            .public_key
                            .clone()
                            .try_into()
                            .expect("Failed to deserialize validator public key"),
                        stake: account_info.amount,
                    })
                    .collect(),
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
            home_dir: home_dir.to_path_buf(),
            store,
            trie,
            runtime,
            trie_viewer,
            epoch_manager,
            shard_tracker,
        }
    }

    fn genesis_state_from_dump(&self) -> (StoreUpdate, Vec<StateRoot>) {
        let store_update = self.store.store_update();
        let mut state_file = self.home_dir.clone();
        state_file.push(STATE_DUMP_FILE);
        self.store
            .load_from_file(ColState, state_file.as_path())
            .expect("Failed to read state dump");
        let mut roots_files = self.home_dir.clone();
        roots_files.push(GENESIS_ROOTS_FILE);
        let mut file = File::open(roots_files).expect("Failed to open genesis roots file.");
        let mut data = vec![];
        file.read_to_end(&mut data).expect("Failed to read genesis roots file.");
        let state_roots: Vec<StateRoot> =
            BorshDeserialize::try_from_slice(&data).expect("Failed to deserialize genesis roots");
        (store_update, state_roots)
    }

    fn genesis_state_from_records(&self) -> (StoreUpdate, Vec<StateRoot>) {
        let mut store_update = self.store.store_update();
        let mut state_roots = vec![];
        let num_shards = self.genesis_config.num_block_producer_seats_per_shard.len() as NumShards;
        let mut shard_records: Vec<Vec<StateRecord>> = (0..num_shards).map(|_| vec![]).collect();
        let mut has_protocol_account = false;
        for record in self.genesis_config.records.iter() {
            shard_records[state_record_to_shard_id(record, num_shards) as usize]
                .push(record.clone());
            if let StateRecord::Account { account_id, .. } = record {
                if account_id == &self.genesis_config.protocol_treasury_account {
                    has_protocol_account = true;
                }
            }
        }
        assert!(has_protocol_account, "Genesis spec doesn't have protocol treasury account");
        for shard_id in 0..num_shards {
            let validators = self
                .genesis_config
                .validators
                .iter()
                .filter_map(|account_info| {
                    if self.account_id_to_shard_id(&account_info.account_id) == shard_id {
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
            let state_update = TrieUpdate::new(self.trie.clone(), MerkleHash::default());
            let (shard_store_update, state_root) = self.runtime.apply_genesis_state(
                state_update,
                &validators,
                &shard_records[shard_id as usize],
            );
            store_update.merge(shard_store_update);
            state_roots.push(state_root);
        }
        (store_update, state_roots)
    }

    /// Processes state update.
    fn process_state_update(
        &self,
        trie: Arc<Trie>,
        state_root: CryptoHash,
        shard_id: ShardId,
        block_height: BlockHeight,
        block_hash: &CryptoHash,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
        receipts: &[Receipt],
        transactions: &[SignedTransaction],
        last_validator_proposals: &[ValidatorStake],
        gas_price: Balance,
        gas_limit: Gas,
        challenges_result: &ChallengesResult,
    ) -> Result<ApplyTransactionResult, Error> {
        let validator_accounts_update = {
            let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
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
                    .iter()
                    .filter(|v| self.account_id_to_shard_id(&v.account_id) == shard_id)
                    .fold(HashMap::new(), |mut acc, v| {
                        acc.insert(v.account_id.clone(), v.stake);
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

        let apply_state = ApplyState {
            block_index: block_height,
            epoch_length: self.genesis_config.epoch_length,
            gas_price,
            block_timestamp,
            gas_limit: Some(gas_limit),
        };

        let apply_result = self
            .runtime
            .apply(
                trie.clone(),
                state_root,
                &validator_accounts_update,
                &apply_state,
                &receipts,
                &transactions,
            )
            .map_err(|e| match e {
                RuntimeError::InvalidTxError(_) => ErrorKind::InvalidTransactions,
                RuntimeError::BalanceMismatchError(e) => panic!("{}", e),
                // TODO: process gracefully
                RuntimeError::UnexpectedIntegerOverflow => {
                    panic!("RuntimeError::UnexpectedIntegerOverflow")
                }
                RuntimeError::StorageError(_) => ErrorKind::StorageError,
            })?;

        // Sort the receipts into appropriate outgoing shards.
        let mut receipt_result = HashMap::default();
        for receipt in apply_result.outgoing_receipts {
            receipt_result
                .entry(self.account_id_to_shard_id(&receipt.receiver_id))
                .or_insert_with(|| vec![])
                .push(receipt);
        }
        let total_gas_burnt =
            apply_result.outcomes.iter().map(|tx_result| tx_result.outcome.gas_burnt).sum();

        let result = ApplyTransactionResult {
            trie_changes: WrappedTrieChanges::new(
                self.trie.clone(),
                apply_result.trie_changes,
                apply_result.key_value_changes,
                block_hash.clone(),
            ),
            new_root: apply_result.state_root,
            outcomes: apply_result.outcomes,
            receipt_result,
            validator_proposals: apply_result.validator_proposals,
            total_gas_burnt,
            total_rent_paid: apply_result.stats.total_rent_paid,
            total_validator_reward: apply_result.stats.total_validator_reward,
            total_balance_burnt: apply_result.stats.total_balance_burnt
                + apply_result.stats.total_balance_slashed,
            proof: trie.recorded_storage(),
        };

        Ok(result)
    }
}

pub fn state_record_to_shard_id(state_record: &StateRecord, num_shards: NumShards) -> ShardId {
    match &state_record {
        StateRecord::Account { account_id, .. }
        | StateRecord::AccessKey { account_id, .. }
        | StateRecord::Contract { account_id, .. }
        | StateRecord::ReceivedData { account_id, .. } => {
            account_id_to_shard_id(account_id, num_shards)
        }
        StateRecord::Data { key, .. } => {
            let key = from_base64(key).unwrap();
            let separator = (1..key.len())
                .find(|&x| key[x] == ACCOUNT_DATA_SEPARATOR[0])
                .expect("Invalid data record");
            account_id_to_shard_id(
                &String::from_utf8(key[1..separator].to_vec()).expect("Must be account id"),
                num_shards,
            )
        }
        StateRecord::PostponedReceipt(receipt) => {
            account_id_to_shard_id(&receipt.receiver_id, num_shards)
        }
    }
}

impl RuntimeAdapter for NightshadeRuntime {
    fn genesis_state(&self) -> (StoreUpdate, Vec<StateRoot>) {
        let has_records = !self.genesis_config.records.is_empty();
        let has_dump = {
            let mut state_dump = self.home_dir.clone();
            state_dump.push(STATE_DUMP_FILE);
            state_dump.exists()
        };
        if has_dump {
            if has_records {
                log::warn!("Found both records in genesis config and the state dump file. Will ignore the records.");
            }
            self.genesis_state_from_dump()
        } else if has_records {
            self.genesis_state_from_records()
        } else {
            panic!("Found neither records in the config nor the state dump file. Either one should be present")
        }
    }

    fn verify_block_signature(&self, header: &BlockHeader) -> Result<(), Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let validator = epoch_manager
            .get_block_producer_info(&header.inner_lite.epoch_id, header.inner_lite.height)?;
        if !header.verify_block_producer(&validator.public_key) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(())
    }

    fn verify_block_vrf(
        &self,
        epoch_id: &EpochId,
        block_height: BlockHeight,
        prev_random_value: &CryptoHash,
        vrf_value: near_crypto::vrf::Value,
        vrf_proof: near_crypto::vrf::Proof,
    ) -> Result<(), Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let validator = epoch_manager.get_block_producer_info(&epoch_id, block_height)?;
        let public_key = near_crypto::key_conversion::convert_public_key(
            validator.public_key.unwrap_as_ed25519(),
        )
        .unwrap();

        if !public_key.is_vrf_valid(&prev_random_value.as_ref(), &vrf_value, &vrf_proof) {
            return Err(ErrorKind::InvalidRandomnessBeaconOutput.into());
        }
        Ok(())
    }

    fn validate_tx(
        &self,
        block_height: BlockHeight,
        block_timestamp: u64,
        gas_price: Balance,
        state_root: StateRoot,
        transaction: &SignedTransaction,
    ) -> Result<Option<InvalidTxError>, Error> {
        let mut state_update = TrieUpdate::new(self.trie.clone(), state_root);
        let apply_state = ApplyState {
            block_index: block_height,
            epoch_length: self.genesis_config.epoch_length,
            gas_price,
            block_timestamp,
            // NOTE: verify transaction doesn't use gas limit
            gas_limit: None,
        };

        match verify_and_charge_transaction(
            &self.runtime.config,
            &mut state_update,
            &apply_state,
            &transaction,
        ) {
            Ok(_) => Ok(None),
            Err(RuntimeError::InvalidTxError(err)) => {
                debug!(target: "runtime", "Tx {:?} validation failed: {:?}", transaction, err);
                Ok(Some(err))
            }
            Err(RuntimeError::StorageError(_err)) => Err(Error::from(ErrorKind::StorageError)),
            Err(err) => unreachable!("Unexpected RuntimeError error {:?}", err),
        }
    }

    fn prepare_transactions(
        &self,
        block_height: BlockHeight,
        block_timestamp: u64,
        gas_price: Balance,
        gas_limit: Gas,
        state_root: StateRoot,
        max_number_of_transactions: usize,
        pool_iterator: &mut dyn PoolIterator,
        chain_validate: &mut dyn FnMut(&SignedTransaction) -> bool,
    ) -> Result<Vec<SignedTransaction>, Error> {
        let mut state_update = TrieUpdate::new(self.trie.clone(), state_root);
        let apply_state = ApplyState {
            block_index: block_height,
            epoch_length: self.genesis_config.epoch_length,
            gas_price,
            block_timestamp,
            gas_limit: Some(gas_limit),
        };

        // Total amount of gas burnt for converting transactions towards receipts.
        let mut total_gas_burnt = 0;
        // TODO: Update gas limit for transactions
        let transactions_gas_limit = gas_limit / 2;
        let mut transactions = vec![];
        let mut num_checked_transactions = 0;

        while transactions.len() < max_number_of_transactions
            && total_gas_burnt < transactions_gas_limit
        {
            if let Some(iter) = pool_iterator.next() {
                while let Some(tx) = iter.next() {
                    num_checked_transactions += 1;
                    // Verifying the transaction is on the same chain and hasn't expired yet.
                    if chain_validate(&tx) {
                        // Verifying the validity of the transaction based on the current state.
                        match verify_and_charge_transaction(
                            &self.runtime.config,
                            &mut state_update,
                            &apply_state,
                            &tx,
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
                            Err(RuntimeError::StorageError(_err)) => {
                                return Err(Error::from(ErrorKind::StorageError))
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
        Ok(signature.verify(data, &validator.public_key))
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
                Ok(signature.verify(data, &fisherman.public_key))
            }
            other => other,
        }
    }

    fn verify_header_signature(&self, header: &BlockHeader) -> Result<bool, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let block_producer = epoch_manager
            .get_block_producer_info(&header.inner_lite.epoch_id, header.inner_lite.height)?;
        let slashed = match epoch_manager.get_slashed_validators(&header.prev_hash) {
            Ok(slashed) => slashed,
            Err(_) => return Err(EpochError::MissingBlock(header.prev_hash).into()),
        };
        if slashed.contains_key(&block_producer.account_id) {
            return Ok(false);
        }
        Ok(header.signature.verify(header.hash.as_ref(), &block_producer.public_key))
    }

    fn verify_chunk_header_signature(&self, header: &ShardChunkHeader) -> Result<bool, Error> {
        let epoch_id = self.get_epoch_id_from_prev_block(&header.inner.prev_block_hash)?;
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        if let Ok(chunk_producer) = epoch_manager.get_chunk_producer_info(
            &epoch_id,
            header.inner.height_created,
            header.inner.shard_id,
        ) {
            let slashed = epoch_manager.get_slashed_validators(&header.inner.prev_block_hash)?;
            if slashed.contains_key(&chunk_producer.account_id) {
                return Ok(false);
            }
            Ok(header.signature.verify(header.chunk_hash().as_ref(), &chunk_producer.public_key))
        } else {
            Err(ErrorKind::NotAValidator.into())
        }
    }

    fn verify_approval_signature(
        &self,
        epoch_id: &EpochId,
        prev_block_hash: &CryptoHash,
        approvals: &[Approval],
    ) -> Result<bool, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let info = epoch_manager
            .get_all_block_producers_ordered(epoch_id, prev_block_hash)
            .map_err(Error::from)?;
        let approvals_hash_map =
            approvals.iter().map(|x| (x.account_id.clone(), x)).collect::<HashMap<_, _>>();
        let mut signatures_verified = 0;
        for (validator, is_slashed) in info.into_iter() {
            if !is_slashed {
                if let Some(approval) = approvals_hash_map.get(&validator.account_id) {
                    if &approval.parent_hash != prev_block_hash {
                        return Ok(false);
                    }
                    if !approval.signature.verify(
                        Approval::get_data_for_sig(
                            &approval.parent_hash,
                            &approval.reference_hash,
                            approval.target_height,
                            approval.is_endorsement,
                        )
                        .as_ref(),
                        &validator.public_key,
                    ) {
                        return Ok(false);
                    }
                    signatures_verified += 1;
                }
            }
        }
        Ok(signatures_verified == approvals.len())
    }

    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager
            .get_all_block_producers_ordered(epoch_id, last_known_block_hash)
            .map_err(Error::from)
    }

    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        Ok(epoch_manager.get_block_producer_info(epoch_id, height)?.account_id)
    }

    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<AccountId, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        Ok(epoch_manager.get_chunk_producer_info(epoch_id, height, shard_id)?.account_id)
    }

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
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
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        match epoch_manager.get_fisherman_by_account_id(epoch_id, account_id) {
            Ok(Some(fisherman)) => {
                let slashed = epoch_manager.get_slashed_validators(&last_known_block_hash)?;
                Ok((fisherman, slashed.contains_key(account_id)))
            }
            Ok(None) => Err(ErrorKind::NotAValidator.into()),
            Err(e) => Err(e.into()),
        }
    }

    fn get_num_validator_blocks(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<ValidatorStats, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager
            .get_num_validator_blocks(epoch_id, last_known_block_hash, account_id)
            .map_err(Error::from)
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
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash)?;
        let settlement =
            epoch_manager.get_all_block_producers_settlement(&epoch_id, parent_hash)?;
        Ok(settlement[part_id as usize % settlement.len()].0.account_id.clone())
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
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.is_next_block_epoch_start(parent_hash).map_err(Error::from)
    }

    fn get_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_epoch_id_from_prev_block(parent_hash).map_err(Error::from)
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_next_epoch_id_from_prev_block(parent_hash).map_err(Error::from)
    }

    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_epoch_start_height(block_hash).map_err(Error::from)
    }

    fn get_epoch_inflation(&self, epoch_id: &EpochId) -> Result<Balance, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        Ok(epoch_manager.get_epoch_inflation(epoch_id)?)
    }

    fn push_final_block_back_if_needed(
        &self,
        parent_hash: CryptoHash,
        last_final_hash: CryptoHash,
    ) -> Result<CryptoHash, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        Ok(epoch_manager.push_final_block_back_if_needed(parent_hash, last_final_hash)?)
    }

    fn add_validator_proposals(
        &self,
        parent_hash: CryptoHash,
        current_hash: CryptoHash,
        rng_seed: CryptoHash,
        height: BlockHeight,
        last_finalized_height: BlockHeight,
        proposals: Vec<ValidatorStake>,
        slashed_validators: Vec<SlashedValidator>,
        chunk_mask: Vec<bool>,
        rent_paid: Balance,
        validator_reward: Balance,
        total_supply: Balance,
    ) -> Result<(), Error> {
        // Check that genesis block doesn't have any proposals.
        assert!(height > 0 || (proposals.is_empty() && slashed_validators.is_empty()));
        debug!(target: "runtime", "add validator proposals at block height {} {:?}", height, proposals);
        // Deal with validator proposals and epoch finishing.
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let block_info = BlockInfo::new(
            height,
            last_finalized_height,
            parent_hash,
            proposals,
            chunk_mask,
            slashed_validators,
            rent_paid,
            validator_reward,
            total_supply,
        );
        let rng_seed = (rng_seed.0).0;
        // TODO: don't commit here, instead contribute to upstream store update.
        epoch_manager
            .record_block_info(&current_hash, block_info, rng_seed)?
            .commit()
            .map_err(|err| err.into())
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
        challenges: &ChallengesResult,
        generate_storage_proof: bool,
    ) -> Result<ApplyTransactionResult, Error> {
        let trie = if generate_storage_proof {
            Arc::new(self.trie.recording_reads())
        } else {
            self.trie.clone()
        };
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
        ) {
            Ok(result) => Ok(result),
            Err(e) => match e.kind() {
                ErrorKind::StorageError => {
                    panic!("Storage error. Corrupted db or invalid state.");
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
        last_validator_proposals: &[ValidatorStake],
        gas_price: Balance,
        gas_limit: Gas,
        challenges: &ChallengesResult,
    ) -> Result<ApplyTransactionResult, Error> {
        let trie = Arc::new(Trie::from_recorded_storage(partial_storage));
        self.process_state_update(
            trie.clone(),
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
        )
    }

    fn query(
        &self,
        state_root: &StateRoot,
        block_height: BlockHeight,
        block_timestamp: u64,
        block_hash: &CryptoHash,
        request: &QueryRequest,
    ) -> Result<QueryResponse, Box<dyn std::error::Error>> {
        match request {
            QueryRequest::ViewAccount { account_id } => {
                match self.view_account(*state_root, account_id) {
                    Ok(r) => Ok(QueryResponse {
                        kind: QueryResponseKind::ViewAccount(r.into()),
                        block_height,
                        block_hash: *block_hash,
                    }),
                    Err(e) => Err(e),
                }
            }
            QueryRequest::CallFunction { account_id, method_name, args } => {
                let mut logs = vec![];
                match self.call_function(
                    *state_root,
                    block_height,
                    block_timestamp,
                    account_id,
                    method_name,
                    args.as_ref(),
                    &mut logs,
                ) {
                    Ok(result) => Ok(QueryResponse {
                        kind: QueryResponseKind::CallResult(CallResult { result, logs }),
                        block_height,
                        block_hash: *block_hash,
                    }),
                    Err(err) => Ok(QueryResponse {
                        kind: QueryResponseKind::Error(QueryError { error: err.to_string(), logs }),
                        block_height,
                        block_hash: *block_hash,
                    }),
                }
            }
            QueryRequest::ViewState { account_id, prefix } => {
                match self.view_state(*state_root, account_id, prefix.as_ref()) {
                    Ok(result) => Ok(QueryResponse {
                        kind: QueryResponseKind::ViewState(result),
                        block_height,
                        block_hash: *block_hash,
                    }),
                    Err(err) => Ok(QueryResponse {
                        kind: QueryResponseKind::Error(QueryError {
                            error: err.to_string(),
                            logs: vec![],
                        }),
                        block_height,
                        block_hash: *block_hash,
                    }),
                }
            }
            QueryRequest::ViewAccessKeyList { account_id } => {
                match self.view_access_keys(*state_root, account_id) {
                    Ok(result) => Ok(QueryResponse {
                        kind: QueryResponseKind::AccessKeyList(
                            result
                                .into_iter()
                                .map(|(public_key, access_key)| AccessKeyInfoView {
                                    public_key,
                                    access_key: access_key.into(),
                                })
                                .collect(),
                        ),
                        block_height,
                        block_hash: *block_hash,
                    }),
                    Err(err) => Ok(QueryResponse {
                        kind: QueryResponseKind::Error(QueryError {
                            error: err.to_string(),
                            logs: vec![],
                        }),
                        block_height,
                        block_hash: *block_hash,
                    }),
                }
            }
            QueryRequest::ViewAccessKey { account_id, public_key } => {
                match self.view_access_key(*state_root, account_id, public_key) {
                    Ok(access_key) => Ok(QueryResponse {
                        kind: QueryResponseKind::AccessKey(access_key.into()),
                        block_height,
                        block_hash: *block_hash,
                    }),
                    Err(err) => Ok(QueryResponse {
                        kind: QueryResponseKind::Error(QueryError {
                            error: err.to_string(),
                            logs: vec![],
                        }),
                        block_height,
                        block_hash: *block_hash,
                    }),
                }
            }
        }
    }

    fn get_validator_info(&self, block_hash: &CryptoHash) -> Result<EpochValidatorInfo, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_validator_info(block_hash).map_err(|e| e.into())
    }

    fn obtain_state_part(&self, state_root: &StateRoot, part_id: u64, num_parts: u64) -> Vec<u8> {
        assert!(part_id < num_parts);
        self.trie
            .get_trie_nodes_for_part(part_id, num_parts, state_root)
            .expect("storage should not fail")
            .try_to_vec()
            .expect("serializer should not fail")
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
            Ok(trie_nodes) => match Trie::validate_trie_nodes_for_part(
                state_root,
                part_id,
                num_parts,
                &trie_nodes,
            ) {
                Ok(_) => true,
                // Storage error should not happen
                Err(_) => false,
            },
            // Deserialization error means we've got the data from malicious peer
            Err(_) => false,
        }
    }

    fn confirm_state(&self, state_root: &StateRoot, data: &Vec<Vec<u8>>) -> Result<(), Error> {
        let mut parts = vec![];
        for part in data {
            parts.push(
                BorshDeserialize::try_from_slice(part)
                    .expect("Part was already validated earlier, so could never fail here"),
            );
        }
        let trie_changes = Trie::combine_state_parts(&state_root, &parts)
            .expect("combine_state_parts is guaranteed to succeed when each part is valid");
        // TODO clean old states
        let trie = self.trie.clone();
        let (store_update, _) = trie_changes.into(trie).expect("TrieChanges::into never fails");
        Ok(store_update.commit()?)
    }

    fn get_state_root_node(&self, state_root: &StateRoot) -> StateRootNode {
        self.trie.retrieve_root_node(state_root).expect("Failed to get root node")
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

    fn get_key_value_changes(
        &self,
        block_hash: &CryptoHash,
        state_changes_request: &StateChangesRequest,
    ) -> Result<StateChanges, Box<dyn std::error::Error>> {
        let chain_store = ChainStore::new(Arc::clone(&self.store));
        chain_store.get_key_value_changes(block_hash, state_changes_request).map_err(|e| e.into())
    }

    fn compare_epoch_id(
        &self,
        epoch_id: &EpochId,
        other_epoch_id: &EpochId,
    ) -> Result<Ordering, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.compare_epoch_id(epoch_id, other_epoch_id).map_err(|e| e.into())
    }
}

impl node_runtime::adapter::ViewRuntimeAdapter for NightshadeRuntime {
    fn view_account(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Account, Box<dyn std::error::Error>> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.view_account(&state_update, account_id)
    }

    fn call_function(
        &self,
        state_root: MerkleHash,
        height: BlockHeight,
        block_timestamp: u64,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.call_function(
            state_update,
            height,
            block_timestamp,
            contract_id,
            method_name,
            args,
            logs,
        )
    }

    fn view_access_key(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKey, Box<dyn std::error::Error>> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.view_access_key(&state_update, account_id, public_key)
    }

    fn view_access_keys(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Vec<(PublicKey, AccessKey)>, Box<dyn std::error::Error>> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        let prefix = prefix_for_access_key(account_id);
        let access_keys = match state_update.iter(&prefix) {
            Ok(iter) => iter
                .map(|key| {
                    let key = key?;
                    let public_key = &key[prefix.len()..];
                    let access_key = get_access_key_raw(&state_update, &key)?
                        .ok_or("Missing key from iterator")?;
                    PublicKey::try_from_slice(public_key)
                        .map_err(|err| format!("{}", err).into())
                        .map(|key| (key, access_key))
                })
                .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>(),
            Err(e) => Err(e.into()),
        };
        access_keys
    }

    fn view_state(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
        prefix: &[u8],
    ) -> Result<ViewStateResult, Box<dyn std::error::Error>> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.view_state(&state_update, account_id, prefix)
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use tempdir::TempDir;

    use near_chain::{ReceiptResult, Tip};
    use near_crypto::{InMemorySigner, KeyType, Signer};
    use near_primitives::test_utils::init_test_logger;
    use near_primitives::transaction::{Action, CreateAccountAction, StakeAction};
    use near_primitives::types::{BlockHeightDelta, Nonce, ValidatorId};
    use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
    use near_primitives::views::{AccountView, CurrentEpochValidatorInfo, NextEpochValidatorInfo};
    use near_store::create_store;
    use node_runtime::config::RuntimeConfig;

    use crate::config::{
        GenesisConfigExt, FISHERMEN_THRESHOLD, TESTING_INIT_BALANCE, TESTING_INIT_STAKE,
    };
    use crate::get_store_path;

    use super::*;

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
            last_proposals: &[ValidatorStake],
            gas_price: Balance,
            gas_limit: Gas,
            challenges: &ChallengesResult,
        ) -> (StateRoot, Vec<ValidatorStake>, ReceiptResult) {
            let result = self
                .apply_transactions(
                    shard_id,
                    &state_root,
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
                )
                .unwrap();
            let mut store_update = self.store.store_update();
            result.trie_changes.insertions_into(&mut store_update).unwrap();
            result.trie_changes.key_value_changes_into(&mut store_update).unwrap();
            store_update.commit().unwrap();
            (result.new_root, result.validator_proposals, result.receipt_result)
        }
    }

    struct TestEnv {
        pub runtime: NightshadeRuntime,
        pub head: Tip,
        state_roots: Vec<StateRoot>,
        pub last_receipts: HashMap<ShardId, Vec<Receipt>>,
        pub last_shard_proposals: HashMap<ShardId, Vec<ValidatorStake>>,
        pub last_proposals: Vec<ValidatorStake>,
    }

    impl TestEnv {
        pub fn new(
            prefix: &str,
            validators: Vec<Vec<AccountId>>,
            epoch_length: BlockHeightDelta,
            initial_tracked_accounts: Vec<AccountId>,
            initial_tracked_shards: Vec<ShardId>,
            has_reward: bool,
        ) -> Self {
            let dir = TempDir::new(prefix).unwrap();
            let store = create_store(&get_store_path(dir.path()));
            let all_validators = validators.iter().fold(BTreeSet::new(), |acc, x| {
                acc.union(&x.iter().map(|x| x.as_str()).collect()).cloned().collect()
            });
            let validators_len = all_validators.len() as ValidatorId;
            let mut genesis_config = GenesisConfig::test_sharded(
                all_validators.into_iter().collect(),
                validators_len,
                validators.iter().map(|x| x.len() as ValidatorId).collect(),
            );
            // No fees mode.
            genesis_config.runtime_config = RuntimeConfig::free();
            genesis_config.epoch_length = epoch_length;
            genesis_config.chunk_producer_kickout_threshold =
                genesis_config.block_producer_kickout_threshold;
            if !has_reward {
                genesis_config.max_inflation_rate = 0;
            }
            let runtime = NightshadeRuntime::new(
                dir.path(),
                store,
                genesis_config.clone(),
                initial_tracked_accounts,
                initial_tracked_shards,
            );
            let (store_update, state_roots) = runtime.genesis_state();
            store_update.commit().unwrap();
            let genesis_hash = hash(&vec![0]);
            runtime
                .add_validator_proposals(
                    CryptoHash::default(),
                    genesis_hash,
                    [0; 32].as_ref().try_into().unwrap(),
                    0,
                    0,
                    vec![],
                    vec![],
                    vec![],
                    0,
                    0,
                    genesis_config.total_supply,
                )
                .unwrap();
            Self {
                runtime,
                head: Tip {
                    last_block_hash: genesis_hash,
                    prev_block_hash: CryptoHash::default(),
                    height: 0,
                    epoch_id: EpochId::default(),
                    score: 0.into(),
                },
                state_roots,
                last_receipts: HashMap::default(),
                last_proposals: vec![],
                last_shard_proposals: HashMap::default(),
            }
        }

        pub fn step(
            &mut self,
            transactions: Vec<Vec<SignedTransaction>>,
            chunk_mask: Vec<bool>,
            challenges_result: ChallengesResult,
        ) {
            let new_hash = hash(&vec![(self.head.height + 1) as u8]);
            let num_shards = self.runtime.num_shards();
            assert_eq!(transactions.len() as NumShards, num_shards);
            assert_eq!(chunk_mask.len() as NumShards, num_shards);
            let mut all_proposals = vec![];
            let mut new_receipts = HashMap::new();
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
                    self.last_shard_proposals.get(&i).unwrap_or(&vec![]),
                    self.runtime.genesis_config.min_gas_price,
                    u64::max_value(),
                    &challenges_result,
                );
                self.state_roots[i as usize] = state_root;
                for (shard_id, mut shard_receipts) in receipts {
                    new_receipts
                        .entry(shard_id)
                        .or_insert_with(|| vec![])
                        .append(&mut shard_receipts);
                }
                all_proposals.append(&mut proposals.clone());
                self.last_shard_proposals.insert(i as ShardId, proposals);
            }
            self.runtime
                .add_validator_proposals(
                    self.head.last_block_hash,
                    new_hash,
                    [0; 32].as_ref().try_into().unwrap(),
                    self.head.height + 1,
                    self.head.height.saturating_sub(1),
                    self.last_proposals.clone(),
                    challenges_result,
                    chunk_mask,
                    0,
                    0,
                    self.runtime.genesis_config.total_supply,
                )
                .unwrap();
            self.last_receipts = new_receipts;
            self.last_proposals = all_proposals;
            self.head = Tip {
                last_block_hash: new_hash,
                prev_block_hash: self.head.last_block_hash,
                height: self.head.height + 1,
                epoch_id: self.runtime.get_epoch_id_from_prev_block(&new_hash).unwrap(),
                score: self.head.score,
            };
        }

        /// Step when there is only one shard
        pub fn step_default(&mut self, transactions: Vec<SignedTransaction>) {
            self.step(vec![transactions], vec![true], ChallengesResult::default());
        }

        pub fn view_account(&self, account_id: &str) -> AccountView {
            let shard_id = self.runtime.account_id_to_shard_id(&account_id.to_string());
            self.runtime
                .view_account(self.state_roots[shard_id as usize], &account_id.to_string())
                .unwrap()
                .into()
        }

        /// Compute per epoch per validator reward and per epoch protocol treasury reward
        pub fn compute_reward(&self, num_validators: usize) -> (Balance, Balance) {
            let per_epoch_total_reward = self.runtime.genesis_config.max_inflation_rate as u128
                * self.runtime.genesis_config.total_supply
                * self.runtime.genesis_config.epoch_length as u128
                / (100 * self.runtime.genesis_config.num_blocks_per_year as u128);
            let per_epoch_protocol_treasury = per_epoch_total_reward
                * self.runtime.genesis_config.protocol_reward_percentage as u128
                / 100;
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
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_rotation",
            vec![validators.clone()],
            2,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);
        // test1 doubles stake and the new account stakes the same, so test2 will be kicked out.`
        let staking_transaction = stake(1, &signer, &block_producers[0], TESTING_INIT_STAKE * 2);
        let new_account = format!("test{}", num_nodes + 1);
        let new_validator =
            InMemoryValidatorSigner::from_seed(&new_account, KeyType::ED25519, &new_account);
        let new_signer = InMemorySigner::from_seed(&new_account, KeyType::ED25519, &new_account);
        let create_account_transaction = SignedTransaction::create_account(
            2,
            block_producers[0].validator_id().clone(),
            new_account,
            TESTING_INIT_STAKE * 3,
            new_signer.public_key(),
            &signer,
            CryptoHash::default(),
        );
        env.step_default(vec![staking_transaction, create_account_transaction]);
        env.step_default(vec![]);
        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.locked, 2 * TESTING_INIT_STAKE);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE * 5);

        // NOTE: The Runtime doesn't take invalid transactions anymore (e.g. one with a bad nonce),
        // because they should be filtered before producing the chunk.
        // Send invalid transaction to see if the rollback of the state affects the validator rewards.
        let invalid_transaction = SignedTransaction::from_actions(
            1,
            new_validator.validator_id().clone(),
            new_validator.validator_id().clone(),
            &new_signer,
            vec![Action::CreateAccount(CreateAccountAction {})],
            // runtime does not validate block history
            CryptoHash::default(),
        );
        let stake_transaction = stake(2, &new_signer, &new_validator, TESTING_INIT_STAKE * 2);
        env.step_default(vec![invalid_transaction, stake_transaction]);
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
                .map(|x| (x.0.account_id.clone(), x.1))
                .collect::<HashMap<_, _>>(),
            vec![("test3".to_string(), false), ("test1".to_string(), false)]
                .into_iter()
                .collect::<HashMap<_, _>>()
        );

        let test1_acc = env.view_account("test1");
        // Staked 2 * X, sent 3 * X to test3.
        assert_eq!(
            (test1_acc.amount, test1_acc.locked),
            (TESTING_INIT_BALANCE - 5 * TESTING_INIT_STAKE, 2 * TESTING_INIT_STAKE)
        );
        let test2_acc = env.view_account("test2");
        // Become fishermen instead
        assert_eq!(
            (test2_acc.amount, test2_acc.locked),
            (TESTING_INIT_BALANCE - TESTING_INIT_STAKE, TESTING_INIT_STAKE)
        );
        let test3_acc = env.view_account("test3");
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
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_stake_change",
            vec![validators.clone()],
            2,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);

        let desired_stake = 2 * TESTING_INIT_STAKE / 3;
        let staking_transaction = stake(1, &signer, &block_producers[0], desired_stake);
        env.step_default(vec![staking_transaction]);
        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);
        for _ in 2..=4 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        for _ in 5..=7 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - desired_stake);
        assert_eq!(account.locked, desired_stake);
    }

    #[test]
    fn test_validator_stake_change_multiple_times() {
        init_test_logger();
        let num_nodes = 4;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_stake_change_multiple_times",
            vec![validators.clone()],
            4,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id))
            .collect();

        let staking_transaction =
            stake(1, &signers[0], &block_producers[0], TESTING_INIT_STAKE - 1);
        let staking_transaction1 =
            stake(2, &signers[0], &block_producers[0], TESTING_INIT_STAKE - 2);
        let staking_transaction2 =
            stake(1, &signers[1], &block_producers[1], TESTING_INIT_STAKE + 1);
        env.step_default(vec![staking_transaction, staking_transaction1, staking_transaction2]);
        let account = env.view_account(&block_producers[0].validator_id());
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

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

        let account = env.view_account(&block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[2].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[3].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        for _ in 9..=12 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

        let account = env.view_account(&block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[2].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[3].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        for _ in 13..=16 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

        let account = env.view_account(&block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE - 1);

        let account = env.view_account(&block_producers[2].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[3].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE - 1);
    }

    #[test]
    fn test_stake_in_last_block_of_an_epoch() {
        init_test_logger();
        let num_nodes = 4;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_stake_change_multiple_times",
            vec![validators.clone()],
            5,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id))
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
        let account = env.view_account(&block_producers[0].validator_id());
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
        let validators = (0..2).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let env = TestEnv::new(
            "verify_validator_signature_failure",
            vec![validators.clone()],
            2,
            vec![],
            vec![],
            true,
        );
        let data = [0; 32];
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);
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
    fn test_state_sync() {
        init_test_logger();
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env =
            TestEnv::new("test_state_sync", vec![validators.clone()], 2, vec![], vec![], true);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let (per_epoch_per_validator_reward, _) = env.compute_reward(num_nodes);
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);
        let staking_transaction = stake(1, &signer, &block_producers[0], TESTING_INIT_STAKE + 1);
        env.step_default(vec![staking_transaction]);
        env.step_default(vec![]);
        let state_part = env.runtime.obtain_state_part(&env.state_roots[0], 0, 1);
        let root_node = env.runtime.get_state_root_node(&env.state_roots[0]);
        let mut new_env =
            TestEnv::new("test_state_sync", vec![validators.clone()], 2, vec![], vec![], true);
        for i in 1..=2 {
            let prev_hash = hash(&[new_env.head.height as u8]);
            let cur_hash = hash(&[(new_env.head.height + 1) as u8]);
            let proposals = if i == 1 {
                vec![ValidatorStake {
                    account_id: block_producers[0].validator_id().clone(),
                    stake: TESTING_INIT_STAKE + 1,
                    public_key: block_producers[0].public_key(),
                }]
            } else {
                vec![]
            };
            new_env
                .runtime
                .add_validator_proposals(
                    prev_hash,
                    cur_hash,
                    [0; 32].as_ref().try_into().unwrap(),
                    i,
                    i.saturating_sub(2),
                    new_env.last_proposals.clone(),
                    vec![],
                    vec![true],
                    0,
                    0,
                    new_env.runtime.genesis_config.total_supply,
                )
                .unwrap();
            new_env.head.height = i;
            new_env.head.last_block_hash = cur_hash;
            new_env.head.prev_block_hash = prev_hash;
            new_env.last_proposals = proposals;
        }
        assert!(new_env.runtime.validate_state_root_node(&root_node, &env.state_roots[0]));
        let mut root_node_wrong = root_node.clone();
        root_node_wrong.memory_usage += 1;
        assert!(!new_env.runtime.validate_state_root_node(&root_node_wrong, &env.state_roots[0]));
        root_node_wrong.data = vec![123];
        assert!(!new_env.runtime.validate_state_root_node(&root_node_wrong, &env.state_roots[0]));
        assert!(!new_env.runtime.validate_state_part(&StateRoot::default(), 0, 1, &state_part));
        new_env.runtime.validate_state_part(&env.state_roots[0], 0, 1, &state_part);
        new_env.runtime.confirm_state(&env.state_roots[0], &vec![state_part]).unwrap();
        new_env.state_roots[0] = env.state_roots[0].clone();
        for _ in 3..=5 {
            new_env.step_default(vec![]);
        }

        let account = new_env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 1 + 2 * per_epoch_per_validator_reward);

        let account = new_env.view_account(&block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 2 * per_epoch_per_validator_reward);
    }

    /// Test two shards: the first shard has 2 validators (test1, test4) and the second shard
    /// has 4 validators (test1, test2, test3, test4). Test that kickout and stake change
    /// work properly.
    #[test]
    fn test_multiple_shards() {
        init_test_logger();
        let num_nodes = 4;
        let first_shard_validators = (0..2).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let second_shard_validators =
            (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let validators = second_shard_validators.clone();
        let mut env = TestEnv::new(
            "test_multiple_shards",
            vec![first_shard_validators, second_shard_validators],
            4,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);
        let staking_transaction = stake(1, &signer, &block_producers[0], TESTING_INIT_STAKE - 1);
        let first_account_shard_id = env.runtime.account_id_to_shard_id(&"test1".to_string());
        let transactions = if first_account_shard_id == 0 {
            vec![vec![staking_transaction], vec![]]
        } else {
            vec![vec![], vec![staking_transaction]]
        };
        env.step(transactions, vec![false, true], ChallengesResult::default());
        for _ in 2..10 {
            env.step(vec![vec![], vec![]], vec![true, true], ChallengesResult::default());
        }
        let account = env.view_account(&block_producers[3].validator_id());
        assert_eq!(account.locked, TESTING_INIT_STAKE);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        for _ in 10..14 {
            env.step(vec![vec![], vec![]], vec![true, true], ChallengesResult::default());
        }
        let account = env.view_account(&block_producers[3].validator_id());
        assert_eq!(account.locked, 0);

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.locked, TESTING_INIT_STAKE - 1);
    }

    #[test]
    fn test_get_validator_info() {
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_get_validator_info",
            vec![validators.clone()],
            2,
            vec![],
            vec![],
            true,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let (per_epoch_per_validator_reward, _) = env.compute_reward(num_nodes);
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);
        let staking_transaction = stake(1, &signer, &block_producers[0], 0);
        env.step_default(vec![staking_transaction]);
        env.step_default(vec![]);
        let mut current_epoch_validator_info = vec![
            CurrentEpochValidatorInfo {
                account_id: "test1".to_string(),
                public_key: block_producers[0].public_key(),
                is_slashed: false,
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
                num_produced_blocks: 1,
                num_expected_blocks: 1,
            },
            CurrentEpochValidatorInfo {
                account_id: "test2".to_string(),
                public_key: block_producers[1].public_key(),
                is_slashed: false,
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
                num_produced_blocks: 1,
                num_expected_blocks: 1,
            },
        ];
        let next_epoch_validator_info = vec![
            NextEpochValidatorInfo {
                account_id: "test1".to_string(),
                public_key: block_producers[0].public_key(),
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
            },
            NextEpochValidatorInfo {
                account_id: "test2".to_string(),
                public_key: block_producers[1].public_key(),
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
            },
        ];
        let response = env.runtime.get_validator_info(&env.head.last_block_hash).unwrap();
        assert_eq!(
            response,
            EpochValidatorInfo {
                current_validators: current_epoch_validator_info.clone(),
                next_validators: next_epoch_validator_info.clone(),
                current_fishermen: vec![],
                next_fishermen: vec![],
                current_proposals: vec![ValidatorStake {
                    account_id: "test1".to_string(),
                    public_key: block_producers[0].public_key(),
                    stake: 0
                }
                .into()]
            }
        );
        env.step_default(vec![]);
        let response = env.runtime.get_validator_info(&env.head.last_block_hash).unwrap();

        current_epoch_validator_info[1].num_produced_blocks = 0;
        current_epoch_validator_info[1].num_expected_blocks = 0;
        assert_eq!(response.current_validators, current_epoch_validator_info);
        assert_eq!(
            response.next_validators,
            vec![NextEpochValidatorInfo {
                account_id: "test2".to_string(),
                public_key: block_producers[1].public_key(),
                stake: TESTING_INIT_STAKE + per_epoch_per_validator_reward,
                shards: vec![0],
            }
            .into()]
        );
        assert!(response.current_proposals.is_empty());
    }

    #[test]
    fn test_care_about_shard() {
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_get_validator_info",
            vec![validators.clone(), vec![validators[0].clone()]],
            2,
            vec![validators[1].clone()],
            vec![],
            true,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signer = InMemorySigner::from_seed(&validators[1], KeyType::ED25519, &validators[1]);
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
        assert!(!env.runtime.cares_about_shard(
            Some(&validators[0]),
            &env.head.last_block_hash,
            1,
            true
        ));
        assert!(
            env.runtime.cares_about_shard(Some(&validators[1]), &env.head.last_block_hash, 0, true),
            true
        );
        assert!(env.runtime.cares_about_shard(
            Some(&validators[1]),
            &env.head.last_block_hash,
            1,
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
        let mut env = TestEnv::new(
            "test_challenges",
            vec![vec!["test1".to_string(), "test2".to_string()]],
            2,
            vec![],
            vec![],
            true,
        );
        env.step(vec![vec![]], vec![true], vec![SlashedValidator::new("test2".to_string(), false)]);
        assert_eq!(env.view_account("test2").locked, 0);
        assert_eq!(
            env.runtime
                .get_epoch_block_producers_ordered(&env.head.epoch_id, &env.head.last_block_hash)
                .unwrap()
                .iter()
                .map(|x| (x.0.account_id.clone(), x.1))
                .collect::<Vec<_>>(),
            vec![("test2".to_string(), true), ("test1".to_string(), false)]
        );
        let msg = vec![0, 1, 2];
        let signer = InMemorySigner::from_seed("test2", KeyType::ED25519, "test2");
        let signature = signer.sign(&msg);
        assert!(!env
            .runtime
            .verify_validator_signature(
                &env.head.epoch_id,
                &env.head.last_block_hash,
                &"test2".to_string(),
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
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env =
            TestEnv::new("test_challenges", vec![validators.clone()], 3, vec![], vec![], false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();

        let signer = InMemorySigner::from_seed(&validators[2], KeyType::ED25519, &validators[2]);
        let staking_transaction = stake(1, &signer, &block_producers[2], TESTING_INIT_STAKE / 3);
        env.step(
            vec![vec![staking_transaction]],
            vec![true],
            vec![SlashedValidator::new("test2".to_string(), true)],
        );
        assert_eq!(env.view_account("test2").locked, TESTING_INIT_STAKE);
        assert_eq!(
            env.runtime
                .get_epoch_block_producers_ordered(&env.head.epoch_id, &env.head.last_block_hash)
                .unwrap()
                .iter()
                .map(|x| (x.0.account_id.clone(), x.1))
                .collect::<Vec<_>>(),
            vec![
                ("test3".to_string(), false),
                ("test2".to_string(), true),
                ("test1".to_string(), false)
            ]
        );
        let msg = vec![0, 1, 2];
        let signer = InMemorySigner::from_seed("test2", KeyType::ED25519, "test2");
        let signature = signer.sign(&msg);
        assert!(!env
            .runtime
            .verify_validator_signature(
                &env.head.epoch_id,
                &env.head.last_block_hash,
                &"test2".to_string(),
                &msg,
                &signature,
            )
            .unwrap());

        for _ in 2..11 {
            env.step(vec![vec![]], vec![true], vec![]);
        }
        env.step(vec![vec![]], vec![true], vec![SlashedValidator::new("test3".to_string(), true)]);
        let account = env.view_account("test3");
        assert_eq!(account.locked, TESTING_INIT_STAKE / 3);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE / 3);

        for _ in 11..14 {
            env.step_default(vec![]);
        }
        let account = env.view_account("test3");
        let slashed = (TESTING_INIT_STAKE / 3) * 3 / 4;
        let remaining = TESTING_INIT_STAKE / 3 - slashed;
        assert_eq!(account.locked, remaining);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE / 3);

        for _ in 14..=20 {
            env.step_default(vec![]);
        }

        let account = env.view_account("test2");
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account = env.view_account("test3");
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE / 3 + remaining);
    }

    /// Test that double sign from multiple accounts may result in all of their stake slashed.
    #[test]
    fn test_double_sign_challenge_all_slashed() {
        init_test_logger();
        let num_nodes = 5;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env =
            TestEnv::new("test_challenges", vec![validators.clone()], 5, vec![], vec![], false);
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        env.step(vec![vec![]], vec![true], vec![SlashedValidator::new("test1".to_string(), true)]);
        env.step(vec![vec![]], vec![true], vec![SlashedValidator::new("test2".to_string(), true)]);
        let msg = vec![0, 1, 2];
        for i in 0..=1 {
            let signature = signers[i].sign(&msg);
            assert!(!env
                .runtime
                .verify_validator_signature(
                    &env.head.epoch_id,
                    &env.head.last_block_hash,
                    &format!("test{}", i + 1),
                    &msg,
                    &signature,
                )
                .unwrap());
        }

        for _ in 3..17 {
            env.step_default(vec![]);
        }
        let account = env.view_account("test1");
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account = env.view_account("test2");
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    }

    /// Test that if double sign occurs in the same epoch as other type of challenges all stake
    /// is slashed.
    #[test]
    fn test_double_sign_with_other_challenges() {
        init_test_logger();
        let num_nodes = 3;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env =
            TestEnv::new("test_challenges", vec![validators.clone()], 5, vec![], vec![], false);
        env.step(
            vec![vec![]],
            vec![true],
            vec![
                SlashedValidator::new("test1".to_string(), true),
                SlashedValidator::new("test2".to_string(), false),
            ],
        );
        env.step(
            vec![vec![]],
            vec![true],
            vec![
                SlashedValidator::new("test1".to_string(), false),
                SlashedValidator::new("test2".to_string(), true),
            ],
        );

        for _ in 3..11 {
            env.step_default(vec![]);
        }
        let account = env.view_account("test1");
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account = env.view_account("test2");
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
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_stake_change_multiple_times",
            vec![validators.clone()],
            4,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id))
            .collect();

        let staking_transaction = stake(1, &signers[0], &block_producers[0], FISHERMEN_THRESHOLD);
        let staking_transaction1 = stake(1, &signers[1], &block_producers[1], FISHERMEN_THRESHOLD);
        env.step_default(vec![staking_transaction, staking_transaction1]);
        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);
        for _ in 2..=13 {
            env.step_default(vec![]);
        }
        let account0 = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account0.locked, FISHERMEN_THRESHOLD);
        assert_eq!(account0.amount, TESTING_INIT_BALANCE - FISHERMEN_THRESHOLD);
        let response = env.runtime.get_validator_info(&env.head.last_block_hash).unwrap();
        assert_eq!(
            response
                .current_fishermen
                .into_iter()
                .map(|fishermen| fishermen.account_id)
                .collect::<Vec<_>>(),
            vec!["test1", "test2"]
        );
        let staking_transaction = stake(2, &signers[0], &block_producers[0], TESTING_INIT_STAKE);
        let staking_transaction2 =
            stake(2, &signers[1], &block_producers[1], FISHERMEN_THRESHOLD / 2);
        env.step_default(vec![staking_transaction, staking_transaction2]);

        for _ in 13..=25 {
            env.step_default(vec![]);
        }

        let account0 = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account0.locked, TESTING_INIT_STAKE);
        assert_eq!(account0.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account1 = env.view_account(&block_producers[1].validator_id());
        assert_eq!(account1.locked, 0);
        assert_eq!(account1.amount, TESTING_INIT_BALANCE);
        let response = env.runtime.get_validator_info(&env.head.last_block_hash).unwrap();
        assert!(response.current_fishermen.is_empty());
    }

    /// Enable reward and make sure that validators get reward proportional to their stake.
    #[test]
    fn test_validator_reward() {
        init_test_logger();
        let num_nodes = 4;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_reward",
            vec![validators.clone()],
            4,
            vec![],
            vec![],
            true,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();

        for _ in 0..5 {
            env.step_default(vec![]);
        }

        let (validator_reward, protocol_treasury_reward) = env.compute_reward(num_nodes);
        for i in 0..4 {
            let account = env.view_account(&block_producers[i].validator_id());
            assert_eq!(account.locked, TESTING_INIT_STAKE + validator_reward);
        }

        let protocol_treasury_account =
            env.view_account(&env.runtime.genesis_config.protocol_treasury_account);
        assert_eq!(
            protocol_treasury_account.amount,
            TESTING_INIT_BALANCE + protocol_treasury_reward
        );
    }
}
