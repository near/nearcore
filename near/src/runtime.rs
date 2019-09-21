use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use borsh::BorshDeserialize;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use kvdb::DBValue;
use log::debug;

use near_chain::types::{ApplyTransactionResult, ValidatorSignatureVerificationResult};
use near_chain::{BlockHeader, Error, ErrorKind, RuntimeAdapter, ValidTransaction, Weight};
use near_crypto::{PublicKey, ReadablePublicKey, Signature};
use near_epoch_manager::{BlockInfo, EpochConfig, EpochManager, RewardCalculator};
use near_primitives::account::{AccessKey, Account};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::serialize::from_base64;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{
    AccountId, Balance, BlockIndex, EpochId, Gas, MerkleHash, ShardId, ValidatorStake,
};
use near_primitives::utils::{prefix_for_access_key, ACCOUNT_DATA_SEPARATOR};
use near_primitives::views::{
    AccessKeyInfoView, CallResult, QueryError, QueryResponse, ViewStateResult,
};
use near_store::{
    get_access_key_raw, get_account, set_account, StorageError, Store, StoreUpdate, Trie,
    TrieUpdate, WrappedTrieChanges,
};
use node_runtime::adapter::ViewRuntimeAdapter;
use node_runtime::ethereum::EthashProvider;
use node_runtime::state_viewer::TrieViewer;
use node_runtime::{ApplyState, Runtime, StateRecord, ETHASH_CACHE_PATH};

use crate::config::GenesisConfig;
use crate::shard_tracker::{account_id_to_shard_id, ShardTracker};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Defines Nightshade state transition, validator rotation and block weight for fork choice rule.
/// TODO: this possibly should be merged with the runtime cargo or at least reconciled on the interfaces.
pub struct NightshadeRuntime {
    genesis_config: GenesisConfig,

    store: Arc<Store>,
    pub trie: Arc<Trie>,
    trie_viewer: TrieViewer,
    runtime: Runtime,
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
        let mut ethash_dir = home_dir.to_owned();
        ethash_dir.push(ETHASH_CACHE_PATH);
        let ethash_provider = Arc::new(Mutex::new(EthashProvider::new(ethash_dir.as_path())));
        let runtime = Runtime::new(genesis_config.runtime_config.clone(), ethash_provider.clone());
        let trie_viewer = TrieViewer::new(ethash_provider);
        let num_shards = genesis_config.block_producers_per_shard.len() as ShardId;
        let initial_epoch_config = EpochConfig {
            epoch_length: genesis_config.epoch_length,
            num_shards,
            num_block_producers: genesis_config.num_block_producers,
            block_producers_per_shard: genesis_config.block_producers_per_shard.clone(),
            avg_fisherman_per_shard: genesis_config.avg_fisherman_per_shard.clone(),
            validator_kickout_threshold: genesis_config.validator_kickout_threshold,
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
                            .expect("Failed to deserialize"),
                        amount: account_info.amount,
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
            store,
            trie,
            runtime,
            trie_viewer,
            epoch_manager,
            shard_tracker,
        }
    }

    /// Iterates over validator accounts in the given shard and updates their accounts to return stake
    /// and allocate rewards.
    fn update_validator_accounts(
        &self,
        shard_id: ShardId,
        block_hash: &CryptoHash,
        state_update: &mut TrieUpdate,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let (stake_info, validator_reward) = epoch_manager.compute_stake_return_info(block_hash)?;

        for (account_id, max_of_stakes) in stake_info {
            if self.account_id_to_shard_id(&account_id) == shard_id {
                let account: Option<Account> = get_account(state_update, &account_id)?;
                if let Some(mut account) = account {
                    if let Some(reward) = validator_reward.get(&account_id) {
                        debug!(target: "runtime", "account {} adding reward {} to stake {}", account_id, reward, account.staked);
                        account.staked += *reward;
                    }

                    debug!(target: "runtime",
                        "account {} stake {} max_of_stakes: {}",
                        account_id, account.staked, max_of_stakes
                    );
                    assert!(
                        account.staked >= max_of_stakes,
                        "FATAL: staking invariant does not hold. Account stake {} is less than maximum of stakes {} in the past three epochs",
                        account.staked,
                        max_of_stakes
                    );
                    let return_stake = account.staked - max_of_stakes;
                    account.staked -= return_stake;
                    account.amount += return_stake;

                    set_account(state_update, &account_id, &account);
                }
            }
        }
        if self.account_id_to_shard_id(&self.genesis_config.protocol_treasury_account) == shard_id {
            let mut protocol_treasury_account =
                get_account(state_update, &self.genesis_config.protocol_treasury_account)?.unwrap();
            protocol_treasury_account.amount +=
                *validator_reward.get(&self.genesis_config.protocol_treasury_account).unwrap();
            set_account(
                state_update,
                &self.genesis_config.protocol_treasury_account,
                &protocol_treasury_account,
            );
        }
        state_update.commit();

        Ok(())
    }
}

pub fn state_record_to_shard_id(state_record: &StateRecord, num_shards: ShardId) -> ShardId {
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
    fn genesis_state(&self) -> (StoreUpdate, Vec<MerkleHash>) {
        let mut store_update = self.store.store_update();
        let mut state_roots = vec![];
        let num_shards = self.genesis_config.block_producers_per_shard.len() as ShardId;
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

    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let validator =
            epoch_manager.get_block_producer_info(&header.inner.epoch_id, header.inner.height)?;
        if !header.verify_block_producer(&validator.public_key) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.inner.total_weight.next(header.inner.approval_sigs.len() as u64))
    }

    fn verify_validator_signature(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> ValidatorSignatureVerificationResult {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        if let Ok(Some(validator)) = epoch_manager.get_validator_by_account_id(epoch_id, account_id)
        {
            if signature.verify(data, &validator.public_key) {
                ValidatorSignatureVerificationResult::Valid
            } else {
                ValidatorSignatureVerificationResult::Invalid
            }
        } else {
            ValidatorSignatureVerificationResult::UnknownEpoch
        }
    }

    fn verify_chunk_header_signature(&self, header: &ShardChunkHeader) -> Result<bool, Error> {
        let epoch_id = self.get_epoch_id_from_prev_block(&header.inner.prev_block_hash)?;
        let mut vm = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let public_key = &vm
            .get_chunk_producer_info(&epoch_id, header.inner.height_created, header.inner.shard_id)
            .map(|vs| vs.public_key);
        if let Ok(public_key) = public_key {
            Ok(header.signature.verify(header.chunk_hash().as_ref(), public_key))
        } else {
            Ok(false)
        }
    }

    fn get_epoch_block_producers(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(AccountId, bool)>, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager
            .get_all_block_producers(epoch_id, last_known_block_hash)
            .map_err(|err| Error::from(err))
    }

    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockIndex,
    ) -> Result<AccountId, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        Ok(epoch_manager.get_block_producer_info(epoch_id, height)?.account_id)
    }

    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockIndex,
        shard_id: ShardId,
    ) -> Result<AccountId, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        Ok(epoch_manager.get_chunk_producer_info(epoch_id, height, shard_id)?.account_id)
    }

    fn num_shards(&self) -> ShardId {
        // TODO: should be dynamic.
        self.genesis_config.block_producers_per_shard.len() as ShardId
    }

    fn num_total_parts(&self, parent_hash: &CryptoHash) -> usize {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash).unwrap();
        if let Ok(block_producers) = epoch_manager.get_all_block_producers(&epoch_id, &parent_hash)
        {
            let ret = block_producers.len();
            if ret > 1 {
                ret
            } else {
                2
            }
        } else {
            2
        }
    }

    fn num_data_parts(&self, parent_hash: &CryptoHash) -> usize {
        let total_parts = self.num_total_parts(parent_hash);
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
        let block_producers = epoch_manager.get_all_block_producers(&epoch_id, parent_hash)?;
        Ok(block_producers[part_id as usize % block_producers.len()].0.clone())
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
        epoch_manager.is_next_block_epoch_start(parent_hash).map_err(|err| err.into())
    }

    fn get_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_epoch_id_from_prev_block(parent_hash).map_err(|err| Error::from(err))
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_next_epoch_id_from_prev_block(parent_hash).map_err(|err| Error::from(err))
    }

    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockIndex, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_epoch_start_height(block_hash).map_err(|err| Error::from(err))
    }

    fn get_epoch_inflation(&self, epoch_id: &EpochId) -> Result<Balance, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        Ok(epoch_manager.get_epoch_inflation(epoch_id)?)
    }

    fn validate_tx(
        &self,
        block_index: BlockIndex,
        block_timestamp: u64,
        gas_price: Balance,
        state_root: CryptoHash,
        transaction: SignedTransaction,
    ) -> Result<ValidTransaction, Box<dyn std::error::Error>> {
        let mut state_update = TrieUpdate::new(self.trie.clone(), state_root);
        let apply_state = ApplyState {
            block_index,
            epoch_length: self.genesis_config.epoch_length,
            gas_price,
            block_timestamp,
        };

        if let Err(err) = self.runtime.verify_and_charge_transaction(
            &mut state_update,
            &apply_state,
            &transaction,
        ) {
            debug!(target: "runtime", "Tx {:?} validation failed: {:?}", transaction, err);
            return Err(err);
        }
        Ok(ValidTransaction { transaction })
    }

    fn filter_transactions(
        &self,
        block_index: BlockIndex,
        block_timestamp: u64,
        gas_price: Balance,
        state_root: CryptoHash,
        transactions: Vec<SignedTransaction>,
    ) -> Vec<SignedTransaction> {
        let mut state_update = TrieUpdate::new(self.trie.clone(), state_root);
        let apply_state = ApplyState {
            block_index,
            epoch_length: self.genesis_config.epoch_length,
            gas_price,
            block_timestamp,
        };
        transactions
            .into_iter()
            .filter(|transaction| {
                self.runtime
                    .verify_and_charge_transaction(&mut state_update, &apply_state, transaction)
                    .is_ok()
            })
            .collect()
    }

    fn add_validator_proposals(
        &self,
        parent_hash: CryptoHash,
        current_hash: CryptoHash,
        block_index: BlockIndex,
        proposals: Vec<ValidatorStake>,
        slashed_validators: Vec<AccountId>,
        chunk_mask: Vec<bool>,
        gas_used: Gas,
        gas_price: Balance,
        total_supply: Balance,
    ) -> Result<(), Error> {
        // Check that genesis block doesn't have any proposals.
        assert!(block_index > 0 || (proposals.len() == 0 && slashed_validators.len() == 0));
        debug!(target: "runtime", "add validator proposals at block index {} {:?}", block_index, proposals);
        // Deal with validator proposals and epoch finishing.
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let mut slashed = HashSet::default();
        for validator in slashed_validators {
            slashed.insert(validator);
        }
        let block_info = BlockInfo::new(
            block_index,
            parent_hash,
            proposals,
            chunk_mask,
            slashed,
            gas_used,
            gas_price,
            total_supply,
        );
        // TODO: add randomness here
        let rng_seed = [0; 32];
        // TODO: don't commit here, instead contribute to upstream store update.
        epoch_manager
            .record_block_info(&current_hash, block_info, rng_seed)?
            .commit()
            .map_err(|err| err.into())
    }

    fn apply_transactions_with_optional_storage_proof(
        &self,
        shard_id: ShardId,
        state_root: &MerkleHash,
        block_index: BlockIndex,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
        _block_hash: &CryptoHash,
        receipts: &Vec<Receipt>,
        transactions: &Vec<SignedTransaction>,
        gas_price: Balance,
        generate_storage_proof: bool,
    ) -> Result<ApplyTransactionResult, Error> {
        let trie = if generate_storage_proof {
            Arc::new(self.trie.recording_reads())
        } else {
            self.trie.clone()
        };
        let mut state_update = TrieUpdate::new(trie.clone(), *state_root);
        let should_update_account = {
            let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
            debug!(target: "runtime",
                "block index: {}, is next_block_epoch_start {}",
                block_index,
                epoch_manager.is_next_block_epoch_start(prev_block_hash).unwrap()
            );
            epoch_manager.is_next_block_epoch_start(prev_block_hash)?
        };

        // If we are starting to apply 1st block in the new epoch.
        if should_update_account {
            self.update_validator_accounts(shard_id, prev_block_hash, &mut state_update).map_err(
                |e| {
                    if let Some(e) = e.downcast_ref::<StorageError>() {
                        panic!(e.to_string())
                    }
                    Error::from(ErrorKind::ValidatorError(e.to_string()))
                },
            )?;
        }

        let apply_state = ApplyState {
            block_index,
            epoch_length: self.genesis_config.epoch_length,
            gas_price,
            block_timestamp,
        };

        let apply_result = self
            .runtime
            .apply(state_update, &apply_state, &receipts, &transactions)
            .expect("Storage error. Corrupted db or invalid state");

        // Sort the receipts into appropriate outgoing shards.
        let mut receipt_result = HashMap::default();
        for receipt in apply_result.new_receipts.into_iter() {
            receipt_result
                .entry(self.account_id_to_shard_id(&receipt.receiver_id))
                .or_insert_with(|| vec![])
                .push(receipt);
        }
        let total_gas_burnt =
            apply_result.tx_result.iter().map(|tx_result| tx_result.result.gas_burnt).sum();

        let result = ApplyTransactionResult {
            trie_changes: WrappedTrieChanges::new(self.trie.clone(), apply_result.trie_changes),
            new_root: apply_result.root,
            transaction_results: apply_result.tx_result,
            receipt_result,
            validator_proposals: apply_result.validator_proposals,
            total_gas_burnt,
            total_rent_paid: apply_result.total_rent_paid,
            proof: trie.recorded_storage(),
        };

        Ok(result)
    }

    fn query(
        &self,
        state_root: MerkleHash,
        height: BlockIndex,
        block_timestamp: u64,
        block_hash: &CryptoHash,
        path_parts: Vec<&str>,
        data: &[u8],
    ) -> Result<QueryResponse, Box<dyn std::error::Error>> {
        if path_parts.is_empty() {
            return Err("Path must contain at least single token".into());
        }
        match path_parts[0] {
            "account" => match self.view_account(state_root, &AccountId::from(path_parts[1])) {
                Ok(r) => Ok(QueryResponse::ViewAccount(r.into())),
                Err(e) => Err(e),
            },
            "call" => {
                let mut logs = vec![];
                match self.call_function(
                    state_root,
                    height,
                    block_timestamp,
                    &AccountId::from(path_parts[1]),
                    path_parts[2],
                    &data,
                    &mut logs,
                ) {
                    Ok(result) => Ok(QueryResponse::CallResult(CallResult { result, logs })),
                    Err(err) => {
                        Ok(QueryResponse::Error(QueryError { error: err.to_string(), logs }))
                    }
                }
            }
            "contract" => {
                match self.view_state(state_root, &AccountId::from(path_parts[1]), data) {
                    Ok(result) => Ok(QueryResponse::ViewState(result)),
                    Err(err) => Ok(QueryResponse::Error(QueryError {
                        error: err.to_string(),
                        logs: vec![],
                    })),
                }
            }
            "access_key" => {
                let result = if path_parts.len() == 2 {
                    self.view_access_keys(state_root, &AccountId::from(path_parts[1])).map(|r| {
                        QueryResponse::AccessKeyList(
                            r.into_iter()
                                .map(|(public_key, access_key)| AccessKeyInfoView {
                                    public_key: public_key.into(),
                                    access_key: access_key.into(),
                                })
                                .collect(),
                        )
                    })
                } else {
                    self.view_access_key(
                        state_root,
                        &AccountId::from(path_parts[1]),
                        &ReadablePublicKey::new(path_parts[2]).try_into()?,
                    )
                    .map(|r| QueryResponse::AccessKey(r.map(|access_key| access_key.into())))
                };
                match result {
                    Ok(result) => Ok(result),
                    Err(err) => Ok(QueryResponse::Error(QueryError {
                        error: err.to_string(),
                        logs: vec![],
                    })),
                }
            }
            "validators" => {
                let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
                match epoch_manager.get_validator_info(block_hash) {
                    Ok(info) => Ok(QueryResponse::Validators(info)),
                    Err(e) => {
                        Ok(QueryResponse::Error(QueryError { error: e.to_string(), logs: vec![] }))
                    }
                }
            }
            _ => Err(format!("Unknown path {}", path_parts[0]).into()),
        }
    }

    fn dump_state(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // TODO(1052): make sure state_root is present in the trie.
        // create snapshot.
        let mut result = vec![];
        let mut cursor = Cursor::new(&mut result);
        for item in self.trie.iter(&state_root)? {
            let (key, value) = item?;
            cursor.write_u32::<LittleEndian>(key.len() as u32)?;
            cursor.write_all(&key)?;
            cursor.write_u32::<LittleEndian>(value.len() as u32)?;
            cursor.write_all(value.as_ref())?;
        }
        // TODO(1048): Save on disk an snapshot, split into chunks and compressed. Send chunks instead of single blob.
        debug!(target: "runtime", "Dumped state for shard #{} @ {}, size = {}", shard_id, state_root, result.len());
        Ok(result)
    }

    fn set_state(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        payload: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!(target: "runtime", "Setting state for shard #{} @ {}, size = {}", shard_id, state_root, payload.len());
        let mut state_update = TrieUpdate::new(self.trie.clone(), CryptoHash::default());
        let payload_len = payload.len();
        let mut cursor = Cursor::new(payload);
        while cursor.position() < payload_len as u64 {
            let key_len = cursor.read_u32::<LittleEndian>()? as usize;
            let mut key = vec![0; key_len];
            cursor.read_exact(&mut key)?;
            let value_len = cursor.read_u32::<LittleEndian>()? as usize;
            let mut value = vec![0; value_len];
            cursor.read_exact(&mut value)?;
            state_update.set(key, DBValue::from_slice(&value));
        }
        let (store_update, root) = state_update.finalize()?.into(self.trie.clone())?;
        if root != state_root {
            return Err("Invalid state root".into());
        }
        store_update.commit()?;
        Ok(())
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
        height: BlockIndex,
        block_timestamp: u64,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.call_function(state_update, height, block_timestamp, contract_id, method_name, args, logs)
    }

    fn view_access_key(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Option<AccessKey>, Box<dyn std::error::Error>> {
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
        match state_update.iter(&prefix) {
            Ok(iter) => iter
                .map(|key| {
                    let public_key = &key[prefix.len()..];
                    let access_key = get_access_key_raw(&state_update, &key)?
                        .ok_or("Missing key from iterator")?;
                    PublicKey::try_from_slice(public_key)
                        .map_err(|err| format!("{}", err).into())
                        .map(|key| (key, access_key))
                })
                .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>(),
            Err(e) => Err(e.into()),
        }
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
    use std::collections::{BTreeSet, HashMap};

    use tempdir::TempDir;

    use near_chain::types::ValidatorSignatureVerificationResult;
    use near_chain::{ReceiptResult, RuntimeAdapter, Tip};
    use near_client::BlockProducer;
    use near_crypto::{InMemorySigner, KeyType, Signer};
    use near_primitives::block::Weight;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::receipt::Receipt;
    use near_primitives::test_utils::init_test_logger;
    use near_primitives::transaction::{Action, SignedTransaction, StakeAction};
    use near_primitives::types::{
        AccountId, Balance, BlockIndex, EpochId, MerkleHash, Nonce, ShardId, ValidatorStake,
    };
    use near_primitives::views::{AccountView, EpochValidatorInfo, QueryResponse};
    use near_store::create_store;
    use node_runtime::adapter::ViewRuntimeAdapter;
    use node_runtime::config::RuntimeConfig;

    use crate::config::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
    use crate::runtime::POISONED_LOCK_ERR;
    use crate::{get_store_path, GenesisConfig, NightshadeRuntime};

    fn stake(nonce: Nonce, sender: &BlockProducer, amount: Balance) -> SignedTransaction {
        SignedTransaction::from_actions(
            nonce,
            sender.account_id.clone(),
            sender.account_id.clone(),
            &*sender.signer,
            vec![Action::Stake(StakeAction {
                stake: amount,
                public_key: sender.signer.public_key(),
            })],
            // runtime does not validate block history
            CryptoHash::default(),
        )
    }

    impl NightshadeRuntime {
        fn update(
            &self,
            state_root: &CryptoHash,
            shard_id: ShardId,
            block_index: BlockIndex,
            block_timestamp: u64,
            prev_block_hash: &CryptoHash,
            block_hash: &CryptoHash,
            receipts: &Vec<Receipt>,
            transactions: &Vec<SignedTransaction>,
            gas_price: Balance,
        ) -> (CryptoHash, Vec<ValidatorStake>, ReceiptResult) {
            let result = self
                .apply_transactions(
                    shard_id,
                    &state_root,
                    block_index,
                    block_timestamp,
                    prev_block_hash,
                    block_hash,
                    receipts,
                    transactions,
                    gas_price,
                )
                .unwrap();
            let mut store_update = self.store.store_update();
            result.trie_changes.insertions_into(&mut store_update).unwrap();
            store_update.commit().unwrap();
            (result.new_root, result.validator_proposals, result.receipt_result)
        }
    }

    struct TestEnv {
        pub runtime: NightshadeRuntime,
        pub head: Tip,
        state_roots: Vec<MerkleHash>,
        pub last_receipts: HashMap<ShardId, Vec<Receipt>>,
    }

    impl TestEnv {
        pub fn new(
            prefix: &str,
            validators: Vec<Vec<AccountId>>,
            epoch_length: BlockIndex,
            initial_tracked_accounts: Vec<AccountId>,
            initial_tracked_shards: Vec<ShardId>,
        ) -> Self {
            let dir = TempDir::new(prefix).unwrap();
            let store = create_store(&get_store_path(dir.path()));
            let all_validators = validators.iter().fold(BTreeSet::new(), |acc, x| {
                acc.union(&x.iter().map(|x| x.as_str()).collect()).cloned().collect()
            });
            let validators_len = all_validators.len();
            let mut genesis_config = GenesisConfig::test_sharded(
                all_validators.into_iter().collect(),
                validators_len,
                validators.iter().map(|x| x.len()).collect(),
            );
            // No fees mode.
            genesis_config.runtime_config = RuntimeConfig::free();
            genesis_config.epoch_length = epoch_length;
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
                    0,
                    vec![],
                    vec![],
                    vec![],
                    0,
                    genesis_config.gas_price,
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
                    total_weight: Weight::default(),
                },
                state_roots,
                last_receipts: HashMap::new(),
            }
        }

        pub fn step(&mut self, transactions: Vec<Vec<SignedTransaction>>, chunk_mask: Vec<bool>) {
            let new_hash = hash(&vec![(self.head.height + 1) as u8]);
            let num_shards = self.runtime.num_shards();
            assert_eq!(transactions.len() as ShardId, num_shards);
            let mut all_proposals = vec![];
            let mut new_receipts = HashMap::new();
            for i in 0..num_shards {
                let (state_root, mut proposals, receipts) = self.runtime.update(
                    &self.state_roots[i as usize],
                    i,
                    self.head.height + 1,
                    0,
                    &self.head.last_block_hash,
                    &new_hash,
                    self.last_receipts.get(&i).unwrap_or(&vec![]),
                    &transactions[i as usize],
                    self.runtime.genesis_config.gas_price,
                );
                self.state_roots[i as usize] = state_root;
                for (shard_id, mut shard_receipts) in receipts {
                    new_receipts
                        .entry(shard_id)
                        .or_insert_with(|| vec![])
                        .append(&mut shard_receipts);
                }
                all_proposals.append(&mut proposals);
            }
            self.runtime
                .add_validator_proposals(
                    self.head.last_block_hash,
                    new_hash,
                    self.head.height + 1,
                    all_proposals,
                    vec![],
                    chunk_mask,
                    0,
                    self.runtime.genesis_config.gas_price,
                    self.runtime.genesis_config.total_supply,
                )
                .unwrap();
            self.last_receipts = new_receipts;
            self.head = Tip {
                last_block_hash: new_hash,
                prev_block_hash: self.head.last_block_hash,
                height: self.head.height + 1,
                epoch_id: self.runtime.get_epoch_id_from_prev_block(&new_hash).unwrap(),
                total_weight: Weight::from(self.head.total_weight.to_num() + 1),
            };
        }

        /// Step when there is only one shard
        pub fn step_default(&mut self, transactions: Vec<SignedTransaction>) {
            self.step(vec![transactions], vec![true]);
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
        let mut env =
            TestEnv::new("test_validator_rotation", vec![validators.clone()], 2, vec![], vec![]);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id).into())
            .collect();
        // test1 doubles stake and the new account stakes the same, so test2 will be kicked out.
        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE * 2);
        let new_account = format!("test{}", num_nodes + 1);
        let new_validator: BlockProducer =
            InMemorySigner::from_seed(&new_account, KeyType::ED25519, &new_account).into();
        let create_account_transaction = SignedTransaction::create_account(
            2,
            block_producers[0].account_id.clone(),
            new_account,
            TESTING_INIT_STAKE * 3,
            new_validator.signer.public_key(),
            &*block_producers[0].signer,
            CryptoHash::default(),
        );
        env.step_default(vec![staking_transaction, create_account_transaction]);
        env.step_default(vec![]);
        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(account.staked, 2 * TESTING_INIT_STAKE);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE * 5);

        // Send invalid transaction to see if the rollback of the state affects the validator rewards.
        let invalid_transaction = stake(0, &new_validator, TESTING_INIT_STAKE * 2);
        env.step_default(vec![invalid_transaction]);

        let stake_transaction = stake(1, &new_validator, TESTING_INIT_STAKE * 2);
        env.step_default(vec![stake_transaction]);

        // Roll steps for 3 epochs to pass.
        for _ in 5..=9 {
            env.step_default(vec![]);
        }

        let epoch_id = env.runtime.get_epoch_id_from_prev_block(&env.head.last_block_hash).unwrap();
        assert_eq!(
            env.runtime.get_epoch_block_producers(&epoch_id, &env.head.last_block_hash).unwrap(),
            vec![("test3".to_string(), false), ("test1".to_string(), false)]
        );

        let test1_acc = env.view_account("test1");
        // per epoch per validator reward
        let (per_epoch_per_validator_reward, per_epoch_protocol_treasury) =
            env.compute_reward(num_nodes);
        // Staked 2 * X, sent 3 * X to test3.
        assert_eq!(
            (test1_acc.amount, test1_acc.staked),
            (
                TESTING_INIT_BALANCE - 5 * TESTING_INIT_STAKE + per_epoch_per_validator_reward,
                2 * TESTING_INIT_STAKE + 3 * per_epoch_per_validator_reward
            )
        );
        let test2_acc = env.view_account("test2");
        // Got money back after being kicked out.
        assert_eq!(
            (test2_acc.amount, test2_acc.staked),
            (TESTING_INIT_BALANCE + 3 * per_epoch_per_validator_reward, 0)
        );
        let test3_acc = env.view_account("test3");
        // Got 3 * X, staking 2 * X of them.
        assert_eq!(
            (test3_acc.amount, test3_acc.staked),
            (TESTING_INIT_STAKE, 2 * TESTING_INIT_STAKE + per_epoch_per_validator_reward)
        );
        let protocol_treasury =
            env.view_account(&env.runtime.genesis_config.protocol_treasury_account);
        assert_eq!(
            (protocol_treasury.amount, protocol_treasury.staked),
            (TESTING_INIT_BALANCE + 4 * per_epoch_protocol_treasury, 0)
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
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id).into())
            .collect();
        let (per_epoch_per_validator_reward, _) = env.compute_reward(num_nodes);

        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE - 1);
        env.step_default(vec![staking_transaction]);
        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.staked, TESTING_INIT_STAKE);
        for _ in 2..=4 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE + per_epoch_per_validator_reward
        );
        assert_eq!(account.staked, TESTING_INIT_STAKE);

        for _ in 5..=7 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1 + per_epoch_per_validator_reward
        );
        assert_eq!(account.staked, TESTING_INIT_STAKE - 1 + per_epoch_per_validator_reward * 2);
    }

    #[test]
    fn test_validator_stake_change_multiple_times() {
        let num_nodes = 4;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_stake_change_multiple_times",
            vec![validators.clone()],
            4,
            vec![],
            vec![],
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id).into())
            .collect();
        let (per_epoch_per_validator_reward, _) = env.compute_reward(num_nodes);

        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE - 1);
        let staking_transaction1 = stake(2, &block_producers[0], TESTING_INIT_STAKE - 2);
        let staking_transaction2 = stake(1, &block_producers[1], TESTING_INIT_STAKE + 1);
        env.step_default(vec![staking_transaction, staking_transaction1, staking_transaction2]);
        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.staked, TESTING_INIT_STAKE);

        let staking_transaction = stake(3, &block_producers[0], TESTING_INIT_STAKE + 1);
        let staking_transaction1 = stake(2, &block_producers[1], TESTING_INIT_STAKE + 2);
        let staking_transaction2 = stake(3, &block_producers[1], TESTING_INIT_STAKE - 1);
        let staking_transaction3 =
            stake(1, &block_producers[3], TESTING_INIT_STAKE - per_epoch_per_validator_reward - 1);
        env.step_default(vec![
            staking_transaction,
            staking_transaction1,
            staking_transaction2,
            staking_transaction3,
        ]);

        for _ in 3..=8 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1 + per_epoch_per_validator_reward
        );
        assert_eq!(account.staked, TESTING_INIT_STAKE + 1);

        let account = env.view_account(&block_producers[1].account_id);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE + per_epoch_per_validator_reward
        );
        assert_eq!(account.staked, TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[2].account_id);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.staked, TESTING_INIT_STAKE + per_epoch_per_validator_reward);

        let account = env.view_account(&block_producers[3].account_id);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE + per_epoch_per_validator_reward
        );
        assert_eq!(account.staked, TESTING_INIT_STAKE);

        for _ in 9..=12 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1 + per_epoch_per_validator_reward
        );
        assert_eq!(account.staked, TESTING_INIT_STAKE + 1 + per_epoch_per_validator_reward);

        // Note: this is not a bug but rather a feature: when one changes their stake for
        // less than the reward they get in an epoch, and the stake change happens an epoch
        // after they stake, the difference in stakes will be returned in 2 epochs rather than
        // 3.
        let account = env.view_account(&block_producers[1].account_id);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE + per_epoch_per_validator_reward + 1
        );
        assert_eq!(account.staked, TESTING_INIT_STAKE + per_epoch_per_validator_reward - 1);

        let account = env.view_account(&block_producers[2].account_id);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.staked, TESTING_INIT_STAKE + 2 * per_epoch_per_validator_reward);

        let account = env.view_account(&block_producers[3].account_id);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 2 * per_epoch_per_validator_reward
        );
        assert_eq!(account.staked, TESTING_INIT_STAKE);

        for _ in 13..=16 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1 + per_epoch_per_validator_reward
        );
        assert_eq!(account.staked, TESTING_INIT_STAKE + 1 + 2 * per_epoch_per_validator_reward);

        let account = env.view_account(&block_producers[1].account_id);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1 + per_epoch_per_validator_reward
        );
        assert_eq!(account.staked, TESTING_INIT_STAKE - 1 + 2 * per_epoch_per_validator_reward);

        let account = env.view_account(&block_producers[2].account_id);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.staked, TESTING_INIT_STAKE + 3 * per_epoch_per_validator_reward);

        let account = env.view_account(&block_producers[3].account_id);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 2 * per_epoch_per_validator_reward + 1
        );
        assert_eq!(account.staked, TESTING_INIT_STAKE + per_epoch_per_validator_reward - 1);
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
        );
        let data = [0; 32];
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);
        let signature = signer.sign(&data);
        assert_eq!(
            ValidatorSignatureVerificationResult::Valid,
            env.runtime.verify_validator_signature(
                &EpochId::default(),
                &validators[0],
                &data,
                &signature
            )
        );
    }

    #[test]
    fn test_verify_validator_signature_failure() {
        let validators = (0..2).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let env = TestEnv::new(
            "verify_validator_signature_failure",
            vec![validators.clone()],
            2,
            vec![],
            vec![],
        );
        let data = [0; 32];
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);
        let signature = signer.sign(&data);
        assert_eq!(
            ValidatorSignatureVerificationResult::Invalid,
            env.runtime.verify_validator_signature(
                &EpochId::default(),
                &validators[1],
                &data,
                &signature
            )
        );
    }

    #[test]
    fn test_state_sync() {
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new("test_state_sync", vec![validators.clone()], 2, vec![], vec![]);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id).into())
            .collect();
        let (per_epoch_per_validator_reward, _) = env.compute_reward(num_nodes);
        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE + 1);
        env.step_default(vec![staking_transaction]);
        env.step_default(vec![]);
        let state_dump = env.runtime.dump_state(0, env.state_roots[0]).unwrap();
        let mut new_env =
            TestEnv::new("test_state_sync", vec![validators.clone()], 2, vec![], vec![]);
        for i in 1..=2 {
            let prev_hash = hash(&[new_env.head.height as u8]);
            let cur_hash = hash(&[(new_env.head.height + 1) as u8]);
            let proposals = if i == 1 {
                vec![ValidatorStake {
                    account_id: block_producers[0].account_id.clone(),
                    amount: TESTING_INIT_STAKE + 1,
                    public_key: block_producers[0].signer.public_key(),
                }]
            } else {
                vec![]
            };
            new_env
                .runtime
                .add_validator_proposals(
                    prev_hash,
                    cur_hash,
                    i,
                    proposals,
                    vec![],
                    vec![true],
                    0,
                    new_env.runtime.genesis_config.gas_price,
                    new_env.runtime.genesis_config.total_supply,
                )
                .unwrap();
            new_env.head.height = i;
            new_env.head.last_block_hash = cur_hash;
            new_env.head.prev_block_hash = prev_hash;
        }
        new_env.runtime.set_state(0, env.state_roots[0], state_dump).unwrap();
        new_env.state_roots[0] = env.state_roots[0];
        for _ in 3..=5 {
            new_env.step_default(vec![]);
        }

        let account = new_env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1 + per_epoch_per_validator_reward
        );
        assert_eq!(account.staked, TESTING_INIT_STAKE + 1 + per_epoch_per_validator_reward);

        let account = new_env.view_account(&block_producers[1].account_id);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.staked, TESTING_INIT_STAKE + 2 * per_epoch_per_validator_reward);
    }

    /// Test two shards: the first shard has 2 validators (test1, test4) and the second shard
    /// has 4 validators (test1, test2, test3, test4). Test that kickout and stake change
    /// work properly.
    #[test]
    fn test_multiple_shards() {
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
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id).into())
            .collect();
        let (per_epoch_per_validator_reward, _) = env.compute_reward(num_nodes);
        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE - 1);
        let first_account_shard_id = env.runtime.account_id_to_shard_id(&"test1".to_string());
        let transactions = if first_account_shard_id == 0 {
            vec![vec![staking_transaction], vec![]]
        } else {
            vec![vec![], vec![staking_transaction]]
        };
        env.step(transactions, vec![false, true]);
        for _ in 2..10 {
            env.step(vec![vec![], vec![]], vec![true, true]);
        }
        let account = env.view_account(&block_producers[3].account_id);
        assert_eq!(account.staked, TESTING_INIT_STAKE);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - TESTING_INIT_STAKE + per_epoch_per_validator_reward
        );

        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(account.staked, TESTING_INIT_STAKE + per_epoch_per_validator_reward - 1);

        for _ in 10..14 {
            env.step(vec![vec![], vec![]], vec![true, true]);
        }
        let account = env.view_account(&block_producers[3].account_id);
        assert_eq!(account.staked, 0);
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
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id).into())
            .collect();
        let (per_epoch_per_validator_reward, _) = env.compute_reward(num_nodes);
        let staking_transaction = stake(1, &block_producers[0], 0);
        env.step_default(vec![staking_transaction]);
        env.step_default(vec![]);
        let mut current_validators = env
            .runtime
            .epoch_manager
            .write()
            .expect(POISONED_LOCK_ERR)
            .get_epoch_info_from_hash(&env.head.last_block_hash)
            .unwrap()
            .validators
            .clone();
        let response = env
            .runtime
            .query(env.state_roots[0], 2, 0, &env.head.last_block_hash, vec!["validators"], &[])
            .unwrap();
        match response {
            QueryResponse::Validators(info) => assert_eq!(
                info,
                EpochValidatorInfo {
                    current_validators: current_validators
                        .clone()
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    next_validators: current_validators
                        .clone()
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    current_proposals: vec![ValidatorStake {
                        account_id: "test1".to_string(),
                        public_key: block_producers[0].signer.public_key(),
                        amount: 0
                    }
                    .into()]
                }
            ),
            _ => panic!("wrong response"),
        }
        env.step_default(vec![]);
        let response = env
            .runtime
            .query(env.state_roots[0], 3, 0, &env.head.last_block_hash, vec!["validators"], &[])
            .unwrap();
        match response {
            QueryResponse::Validators(info) => {
                for p in current_validators.iter_mut() {
                    p.amount += per_epoch_per_validator_reward;
                }
                let v: Vec<ValidatorStake> =
                    info.current_validators.clone().into_iter().map(Into::into).collect();
                assert_eq!(v, current_validators);
                assert_eq!(
                    info.next_validators,
                    vec![ValidatorStake {
                        account_id: "test2".to_string(),
                        public_key: block_producers[1].signer.public_key(),
                        amount: TESTING_INIT_STAKE + per_epoch_per_validator_reward
                    }
                    .into()]
                );
                assert!(info.current_proposals.is_empty());
            }
            _ => panic!("wrong response"),
        }
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
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id).into())
            .collect();
        let staking_transaction = stake(1, &block_producers[1], 0);
        env.step(vec![vec![staking_transaction], vec![]], vec![true, true]);
        env.step(vec![vec![], vec![]], vec![true, true]);
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
}
