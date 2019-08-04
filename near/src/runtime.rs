use std::collections::{BTreeSet, HashSet};
use std::convert::TryFrom;
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use kvdb::DBValue;
use log::{debug, error, info};

use near_chain::types::ApplyTransactionResult;
use near_chain::{BlockHeader, Error, ErrorKind, RuntimeAdapter, ValidTransaction, Weight};
use near_epoch_manager::{BlockInfo, EpochConfig, EpochManager};
use near_primitives::account::{AccessKey, Account};
use near_primitives::crypto::signature::{verify, PublicKey, Signature};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::rpc::{AccountViewCallResult, QueryResponse, ViewStateResult};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::transaction::{ReceiptTransaction, SignedTransaction};
use near_primitives::types::{
    AccountId, Balance, BlockIndex, EpochId, GasUsage, MerkleHash, ShardId, ValidatorStake,
};
use near_primitives::utils::prefix_for_access_key;
use near_store::{
    get_access_key_raw, get_account, set_account, Store, StoreUpdate, Trie, TrieUpdate,
    WrappedTrieChanges,
};
use near_verifier::TransactionVerifier;
use node_runtime::adapter::query_client;
use node_runtime::ethereum::EthashProvider;
use node_runtime::state_viewer::TrieViewer;
use node_runtime::{ApplyState, Runtime, ETHASH_CACHE_PATH};

use crate::config::GenesisConfig;
use std::cmp::max;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Defines Nightshade state transition, validator rotation and block weight for fork choice rule.
/// TODO: this possibly should be merged with the runtime cargo or at least reconciled on the interfaces.
pub struct NightshadeRuntime {
    genesis_config: GenesisConfig,

    store: Arc<Store>,
    pub trie: Arc<Trie>,
    trie_viewer: TrieViewer,
    runtime: Runtime,
    epoch_manager: RwLock<EpochManager>,
}

impl NightshadeRuntime {
    pub fn new(home_dir: &Path, store: Arc<Store>, genesis_config: GenesisConfig) -> Self {
        let trie = Arc::new(Trie::new(store.clone()));
        let mut ethash_dir = home_dir.to_owned();
        ethash_dir.push(ETHASH_CACHE_PATH);
        let ethash_provider = Arc::new(Mutex::new(EthashProvider::new(ethash_dir.as_path())));
        let runtime = Runtime::new(genesis_config.runtime_config.clone(), ethash_provider.clone());
        let trie_viewer = TrieViewer::new(ethash_provider);
        let initial_epoch_config = EpochConfig {
            epoch_length: genesis_config.epoch_length,
            num_shards: genesis_config.block_producers_per_shard.len() as ShardId,
            num_block_producers: genesis_config.num_block_producers,
            block_producers_per_shard: genesis_config.block_producers_per_shard.clone(),
            avg_fisherman_per_shard: genesis_config.avg_fisherman_per_shard.clone(),
            validator_kickout_threshold: genesis_config.validator_kickout_threshold,
        };
        let epoch_manager = RwLock::new(
            EpochManager::new(
                store.clone(),
                initial_epoch_config,
                genesis_config
                    .validators
                    .iter()
                    .map(|account_info| ValidatorStake {
                        account_id: account_info.account_id.clone(),
                        public_key: PublicKey::try_from(account_info.public_key.0.as_str())
                            .unwrap(),
                        amount: account_info.amount,
                    })
                    .collect(),
            )
            .expect("Failed to start Epoch Manager"),
        );
        NightshadeRuntime { genesis_config, store, trie, runtime, trie_viewer, epoch_manager }
    }

    /// Iterates over validator accounts in the given shard and updates their accounts to return stake
    /// and allocate rewards.
    fn update_validator_accounts(
        &self,
        shard_id: ShardId,
        state_root: &MerkleHash,
        block_hash: &CryptoHash,
        state_update: &mut TrieUpdate,
        gas_price: Balance,
        total_supply: Balance,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let prev_epoch_id = epoch_manager.get_prev_epoch_id(block_hash)?;
        let prev_prev_epoch_id = epoch_manager.get_prev_epoch_id_from_epoch_id(&prev_epoch_id)?;
        let prev_prev_stake_change =
            epoch_manager.get_epoch_info(&prev_prev_epoch_id)?.stake_change.clone();
        let prev_stake_change = epoch_manager.get_epoch_info(&prev_epoch_id)?.stake_change.clone();
        let prev_block_hash = epoch_manager.get_block_info(&block_hash)?.prev_hash;
        let slashed = epoch_manager.get_slashed_validators(&prev_block_hash)?.clone();
        let (stake_change, validator_to_index, total_gas_used) = {
            let epoch_info = epoch_manager.get_epoch_info_from_hash(block_hash)?;
            (&epoch_info.stake_change, &epoch_info.validator_to_index, epoch_info.total_gas_used)
        };
        let prev_keys: BTreeSet<_> = prev_stake_change.keys().collect();
        let keys: BTreeSet<_> = stake_change.keys().collect();

        // calculate reward for validators
        // TODO: include storage rent
        let total_tx_fee = gas_price * total_gas_used as u128;
        // first order taylor approximation of the inflation
        let max_inflation =
            self.genesis_config.max_inflation_rate as u128 * total_supply / (100 * 365);
        let total_reward = max(
            max_inflation,
            (100 - self.genesis_config.developer_reward_percentage) as u128 * total_tx_fee / 100,
        );
        // TODO: add to protocol treasury
        let protocol_reward =
            self.genesis_config.protocol_reward_percentage as u128 * total_reward / 100;
        let validator_total_reward = total_reward - protocol_reward;
        let num_validators = validator_to_index.len() - slashed.len();
        let reward = validator_total_reward / (100 * num_validators as u128);

        for account_id in prev_keys.union(&keys) {
            let account: Option<Account> = get_account(&state_update, account_id);
            if let Some(mut account) = account {
                let new_stake = *stake_change.get(*account_id).unwrap_or(&0);
                let prev_stake = *prev_stake_change.get(*account_id).unwrap_or(&0);
                let prev_prev_stake = *prev_prev_stake_change.get(*account_id).unwrap_or(&0);
                let max_of_stakes =
                    vec![prev_prev_stake, prev_stake, new_stake].into_iter().max().unwrap();
                if account.staked < max_of_stakes {
                    error!(target: "runtime", "FATAL: staking invariance does not hold");
                }
                let return_stake = account.staked - max_of_stakes;
                account.staked -= return_stake;
                account.amount += return_stake;

                if validator_to_index.contains_key(*account_id) && !slashed.contains(*account_id) {
                    account.staked += reward;
                }
                set_account(state_update, account_id, &account);
            }
        }
        Ok(())
    }
}

impl RuntimeAdapter for NightshadeRuntime {
    fn genesis_state(&self) -> (StoreUpdate, Vec<MerkleHash>) {
        let mut store_update = self.store.store_update();
        let mut state_roots = vec![];
        for shard_id in 0..self.genesis_config.block_producers_per_shard.len() as ShardId {
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
                &self.genesis_config.records[shard_id as usize],
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
        let validator = epoch_manager
            .get_block_producer_info(&header.epoch_id, header.height)
            .map_err(|err| Error::from(err))?;
        if !header.verify_block_producer(&validator.public_key) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.total_weight.next(header.approval_sigs.len() as u64))
    }

    fn verify_validator_signature(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> bool {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        if let Ok(Some(validator)) = epoch_manager.get_validator_by_account_id(epoch_id, account_id)
        {
            return verify(data, signature, &validator.public_key);
        }
        false
    }

    fn verify_chunk_header_signature(&self, header: &ShardChunkHeader) -> Result<bool, Error> {
        let epoch_id = self.get_epoch_id(&header.prev_block_hash)?;
        let mut vm = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let public_key = &vm
            .get_chunk_producer_info(&epoch_id, header.height_created, header.shard_id)
            .map(|vs| vs.public_key);
        if let Ok(public_key) = public_key {
            Ok(verify(header.chunk_hash().0.as_ref(), &header.signature, public_key))
        } else {
            Ok(false)
        }
    }

    fn get_epoch_block_proposers(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(AccountId, bool)>, Box<dyn std::error::Error>> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager
            .get_all_block_producers(epoch_id, last_known_block_hash)
            .map_err(|err| err.into())
    }

    fn get_block_proposer(
        &self,
        epoch_id: &EpochId,
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        Ok(epoch_manager.get_block_producer_info(epoch_id, height)?.account_id)
    }

    fn get_chunk_proposer(
        &self,
        epoch_id: &EpochId,
        height: BlockIndex,
        shard_id: ShardId,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        Ok(epoch_manager.get_chunk_producer_info(epoch_id, height, shard_id)?.account_id)
    }

    fn num_shards(&self) -> ShardId {
        // TODO: should be dynamic.
        self.genesis_config.block_producers_per_shard.len() as ShardId
    }

    fn num_total_parts(&self, parent_hash: &CryptoHash) -> usize {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let epoch_id = epoch_manager.get_epoch_id(parent_hash).unwrap();
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
        let mut cursor = Cursor::new((hash(&account_id.clone().into_bytes()).0).0);
        cursor.read_u64::<LittleEndian>().expect("Must not happened") % (self.num_shards())
    }

    fn get_part_owner(
        &self,
        parent_hash: &CryptoHash,
        part_id: u64,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let epoch_id = epoch_manager.get_epoch_id(parent_hash).map_err(|err| Box::new(err))?;
        let block_producers = epoch_manager
            .get_all_block_producers(&epoch_id, parent_hash)
            .map_err(|err| Box::new(err))?;
        Ok(block_producers[part_id as usize % block_producers.len()].0.clone())
    }

    fn cares_about_shard(
        &self,
        account_id: &AccountId,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.cares_about_shard(parent_hash, account_id, shard_id).unwrap_or(false)
    }

    fn will_care_about_shard(
        &self,
        account_id: &AccountId,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager
            .cares_about_shard_next_epoch(parent_hash, account_id, shard_id)
            .unwrap_or(false)
    }

    fn validate_tx(
        &self,
        _shard_id: ShardId,
        state_root: MerkleHash,
        transaction: SignedTransaction,
    ) -> Result<ValidTransaction, String> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        let verifier = TransactionVerifier::new(&state_update);
        if let Err(err) = verifier.verify_transaction(&transaction) {
            debug!(target: "runtime", "Tx {:?} validation failed: {:?}", transaction, err);
            return Err(err);
        }
        Ok(ValidTransaction { transaction })
    }

    fn add_validator_proposals(
        &self,
        parent_hash: CryptoHash,
        current_hash: CryptoHash,
        block_index: BlockIndex,
        proposals: Vec<ValidatorStake>,
        slashed_validators: Vec<AccountId>,
        validator_mask: Vec<bool>,
        gas_used: GasUsage,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Check that genesis block doesn't have any proposals.
        assert!(block_index > 0 || (proposals.len() == 0 && slashed_validators.len() == 0));
        println!("{} {:?}", block_index, proposals);
        // Deal with validator proposals and epoch finishing.
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let mut slashed = HashSet::default();
        for validator in slashed_validators {
            slashed.insert(validator);
        }
        let block_info =
            BlockInfo::new(block_index, parent_hash, proposals, validator_mask, slashed, gas_used);
        // TODO: add randomness here
        let rng_seed = [0; 32];
        // TODO: don't commit here, instead contribute to upstream store update.
        epoch_manager
            .record_block_info(&current_hash, block_info, rng_seed)?
            .commit()
            .map_err(|err| err.into())
    }

    fn apply_transactions(
        &self,
        shard_id: ShardId,
        state_root: &MerkleHash,
        block_index: BlockIndex,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        receipts: &Vec<ReceiptTransaction>,
        transactions: &Vec<SignedTransaction>,
        gas_price: Balance,
        total_supply: Balance,
    ) -> Result<ApplyTransactionResult, Box<dyn std::error::Error>> {
        let mut state_update = TrieUpdate::new(self.trie.clone(), *state_root);
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        // If we are starting to apply 2nd block in the next epoch.
        if epoch_manager.is_next_block_epoch_start(prev_block_hash).map_err(|err| Box::new(err))? {
            self.update_validator_accounts(
                shard_id,
                state_root,
                prev_block_hash,
                &mut state_update,
                gas_price,
                total_supply,
            )?;
        }
        let apply_state = ApplyState {
            root: *state_root,
            shard_id,
            block_index,
            parent_block_hash: *prev_block_hash,
            epoch_length: self.genesis_config.epoch_length,
        };

        let apply_result =
            self.runtime.apply(state_update, &apply_state, &receipts, &transactions)?;

        let result = ApplyTransactionResult {
            trie_changes: WrappedTrieChanges::new(self.trie.clone(), apply_result.trie_changes),
            new_root: apply_result.root,
            transaction_results: apply_result.tx_result,
            receipt_result: apply_result.new_receipts,
            validator_proposals: apply_result.validator_proposals,
            gas_used: 0,
        };

        Ok(result)
    }

    fn query(
        &self,
        state_root: MerkleHash,
        height: BlockIndex,
        path_parts: Vec<&str>,
        data: &[u8],
    ) -> Result<QueryResponse, Box<dyn std::error::Error>> {
        query_client(self, state_root, height, path_parts, data)
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
        info!(target: "runtime", "Dumped state for shard #{} @ {}, size = {}", shard_id, state_root, result.len());
        Ok(result)
    }

    fn set_state(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        payload: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!(target: "runtime", "Setting state for shard #{} @ {}, size = {}", shard_id, state_root, payload.len());
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

    fn is_epoch_second_block(
        &self,
        parent_hash: &CryptoHash,
        index: BlockIndex,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.is_next_block_epoch_second_block(parent_hash).map_err(|err| err.into())
    }

    fn is_epoch_start(
        &self,
        parent_hash: &CryptoHash,
        index: BlockIndex,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.is_next_block_epoch_start(parent_hash).map_err(|err| err.into())
    }

    fn get_epoch_id(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_epoch_id(parent_hash).map_err(|err| Error::from(err))
    }

    fn get_next_epoch_id(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager.get_next_epoch_id(parent_hash).map_err(|err| Error::from(err))
    }
}

impl node_runtime::adapter::ViewRuntimeAdapter for NightshadeRuntime {
    fn view_account(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<AccountViewCallResult, Box<dyn std::error::Error>> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.view_account(&state_update, account_id)
    }

    fn call_function(
        &self,
        state_root: MerkleHash,
        height: BlockIndex,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.call_function(state_update, height, contract_id, method_name, args, logs)
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
                    let access_key = get_access_key_raw(&state_update, &key)
                        .ok_or("Missing key from iterator")?;
                    PublicKey::try_from(public_key)
                        .map_err(|err| format!("{}", err).into())
                        .map(|key| (key, access_key))
                })
                .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>(),
            Err(e) => Err(e),
        }
    }

    fn view_state(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<ViewStateResult, Box<dyn std::error::Error>> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.view_state(&state_update, account_id)
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use near_chain::{RuntimeAdapter, Tip};
    use near_client::BlockProducer;
    use near_epoch_manager::test_utils::{change_stake, epoch_info, hash_range};
    use near_primitives::crypto::signer::{EDSigner, InMemorySigner};
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::rpc::AccountViewCallResult;
    use near_primitives::serialize::BaseEncode;
    use near_primitives::transaction::{
        CreateAccountTransaction, ReceiptTransaction, SignedTransaction, StakeTransaction,
        TransactionBody,
    };
    use near_primitives::types::{
        AccountId, Balance, BlockIndex, EpochId, MerkleHash, Nonce, ValidatorStake,
    };
    use near_store::create_store;
    use node_runtime::adapter::ViewRuntimeAdapter;

    use crate::config::{
        INITIAL_GAS_PRICE, INITIAL_TOKEN_SUPPLY, TESTING_INIT_BALANCE, TESTING_INIT_STAKE,
    };
    use crate::runtime::POISONED_LOCK_ERR;
    use crate::{get_store_path, GenesisConfig, NightshadeRuntime};
    use near_primitives::block::Weight;

    fn stake(nonce: Nonce, sender: &BlockProducer, amount: Balance) -> SignedTransaction {
        TransactionBody::Stake(StakeTransaction {
            nonce,
            originator: sender.account_id.clone(),
            amount,
            public_key: sender.signer.public_key().to_base(),
        })
        .sign(&*sender.signer.clone())
    }

    impl NightshadeRuntime {
        fn update(
            &self,
            state_root: &CryptoHash,
            block_index: BlockIndex,
            prev_block_hash: &CryptoHash,
            block_hash: &CryptoHash,
            receipts: &Vec<ReceiptTransaction>,
            transactions: &Vec<SignedTransaction>,
            gas_price: Balance,
            total_supply: Balance,
        ) -> (CryptoHash, Vec<ValidatorStake>, Vec<Vec<ReceiptTransaction>>) {
            let mut root = *state_root;
            let result = self
                .apply_transactions(
                    0,
                    &root,
                    block_index,
                    prev_block_hash,
                    block_hash,
                    receipts,
                    transactions,
                    gas_price,
                    total_supply,
                )
                .unwrap();
            let mut store_update = self.store.store_update();
            result.trie_changes.insertions_into(&mut store_update).unwrap();
            store_update.commit().unwrap();
            root = result.new_root;
            let new_receipts = result.receipt_result.into_iter().map(|(_, v)| v).collect();
            (root, result.validator_proposals, new_receipts)
        }
    }

    struct TestEnv {
        pub runtime: NightshadeRuntime,
        pub head: Tip,
        state_roots: Vec<MerkleHash>,
    }
    impl TestEnv {
        pub fn new(prefix: &str, validators: Vec<AccountId>, epoch_length: BlockIndex) -> Self {
            let dir = TempDir::new(prefix).unwrap();
            let store = create_store(&get_store_path(dir.path()));
            let mut genesis_config =
                GenesisConfig::test(validators.iter().map(|v| v.as_str()).collect());
            genesis_config.epoch_length = epoch_length;
            let runtime = NightshadeRuntime::new(dir.path(), store, genesis_config);
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
            }
        }

        pub fn step(
            &mut self,
            receipts: Vec<Vec<ReceiptTransaction>>,
            transactions: Vec<SignedTransaction>,
        ) -> Vec<Vec<ReceiptTransaction>> {
            // TODO: add support for shards.
            let new_hash = hash(&vec![(self.head.height + 1) as u8]);
            let (state_root, proposals, receipts) = self.runtime.update(
                &self.state_roots[0],
                self.head.height + 1,
                &self.head.last_block_hash,
                &new_hash,
                &receipts.iter().flatten().cloned().collect::<Vec<_>>(),
                &transactions,
                INITIAL_GAS_PRICE,
                INITIAL_TOKEN_SUPPLY,
            );
            self.state_roots[0] = state_root;
            self.runtime
                .add_validator_proposals(
                    self.head.last_block_hash,
                    new_hash,
                    self.head.height + 1,
                    proposals,
                    vec![],
                    vec![],
                    0,
                )
                .unwrap();
            self.head = Tip {
                last_block_hash: new_hash,
                prev_block_hash: self.head.last_block_hash,
                height: self.head.height + 1,
                epoch_id: self.runtime.get_epoch_id(&new_hash).unwrap(),
                total_weight: Weight::from(self.head.total_weight.to_num() + 1),
            };
            receipts
        }

        pub fn view_account(&self, account_id: &AccountId) -> AccountViewCallResult {
            // TODO: add support for shards.
            self.runtime.view_account(self.state_roots[0], account_id).unwrap()
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
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new("test_validator_rotation", validators.clone(), 2);
        let block_producers: Vec<_> =
            validators.iter().map(|id| InMemorySigner::from_seed(id, id).into()).collect();
        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE * 2);

        // test1 stakes twice the current stake, because test1 and test2 have the same amount of stake before, test2 will be
        // kicked out.
        env.step(vec![], vec![staking_transaction]);
        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                nonce: 1,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE * 2,
                stake: TESTING_INIT_STAKE * 2,
                public_keys: vec![block_producers[0].signer.public_key()],
                code_hash: account.code_hash,
            }
        );

        let new_account = format!("test{}", num_nodes + 1);
        let new_validator: BlockProducer =
            InMemorySigner::from_seed(&new_account, &new_account).into();
        let create_account_transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
            nonce: 2,
            originator: block_producers[0].account_id.clone(),
            new_account_id: new_account,
            amount: TESTING_INIT_STAKE * 3,
            public_key: new_validator.signer.public_key().0[..].to_vec(),
        })
        .sign(&*block_producers[0].signer.clone());
        let staking_transaction = stake(1, &new_validator, TESTING_INIT_STAKE * 2);
        let receipts = env.step(vec![], vec![create_account_transaction]);
        env.step(receipts, vec![staking_transaction]);
        env.step(vec![], vec![]);
        env.step(vec![], vec![]);
        env.step(vec![], vec![]);
        env.step(vec![], vec![]);
        env.step(vec![], vec![]);

        let epoch_id = env.runtime.get_epoch_id(&env.head.last_block_hash).unwrap();
        assert_eq!(
            env.runtime.get_epoch_block_proposers(&epoch_id, &env.head.last_block_hash).unwrap(),
            vec![("test3".to_string(), false), ("test1".to_string(), false)]
        );
        // TODO: add more checks about account status.

        //        state_root = new_root;
        //        assert_eq!(
        //            validator_stakes,
        //            vec![ValidatorStake::new(
        //                block_producers[0].account_id.clone(),
        //                block_producers[0].signer.public_key(),
        //                TESTING_INIT_STAKE * 2
        //            )]
        //        );
        //
        //
        //        state_root = nightshade
        //            .update(
        //                &state_root,
        //                2,
        //                &h[1],
        //                &h[2],
        //                &receipts.iter().flatten().cloned().collect::<Vec<_>>(),
        //                &vec![],
        //            )
        //            .0;
        //        nightshade.add_validator_proposals(h[1], h[2], 2, vec![], vec![], vec![], 0, 0).unwrap();
        //        // test3 stakes the same amount as test1 and will be confirmed as a validator in the next epoch
        //        let (new_root, validator_stakes, _) =
        //            nightshade.update(&state_root, 3, &h[2], &h[3], &vec![], &vec![staking_transaction]);
        //        state_root = new_root;
        //        assert_eq!(
        //            validator_stakes,
        //            vec![ValidatorStake::new(
        //                new_validator.account_id.clone(),
        //                new_validator.signer.public_key(),
        //                TESTING_INIT_STAKE * 2
        //            )]
        //        );
        //        nightshade
        //            .add_validator_proposals(h[2], h[3], 3, validator_stakes, vec![], vec![], 0, 0)
        //            .unwrap();
        //        nightshade.update(&state_root, 4, &h[3], &h[4], &vec![], &vec![]).0;
        //        nightshade.add_validator_proposals(h[3], h[4], 4, vec![], vec![], vec![], 0, 0).unwrap();
        //        state_root = nightshade.update(&state_root, 5, &h[4], &h[5], &vec![], &vec![]).0;
        //        nightshade.add_validator_proposals(h[4], h[5], 5, vec![], vec![], vec![], 0, 0).unwrap();
        //        {
        //            let mut vm = nightshade.epoch_manager.write().expect(POISONED_LOCK_ERR);
        //            let validators = vm.get_epoch_info_from_hash(&h[5]).unwrap();
        //            // for epoch 3 (starts at h[5]), test2 will be kicked out and test3 will join
        //            assert_eq!(
        //                validators,
        //                &epoch_info(
        //                    vec![("test3", TESTING_INIT_STAKE * 2), ("test1", TESTING_INIT_STAKE * 2)],
        //                    vec![1, 0],
        //                    vec![vec![1, 0]],
        //                    vec![],
        //                    change_stake(vec![
        //                        ("test1", TESTING_INIT_STAKE * 2),
        //                        ("test2", 0),
        //                        ("test3", TESTING_INIT_STAKE * 2)
        //                    ])
        //                )
        //            );
        //        }
        //        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
        //        assert_eq!(
        //            account,
        //            AccountViewCallResult {
        //                account_id: block_producers[0].account_id.clone(),
        //                nonce: 2,
        //                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE * 5,
        //                stake: TESTING_INIT_STAKE * 2,
        //                public_keys: vec![block_producers[0].signer.public_key()],
        //                code_hash: account.code_hash
        //            }
        //        );
        //        state_root = nightshade.update(&state_root, 6, &h[5], &h[6], &vec![], &vec![]).0;
        //        nightshade.add_validator_proposals(h[5], h[6], 6, vec![], vec![], vec![], 0, 0).unwrap();
        //
        //        let account = nightshade.view_account(state_root, &block_producers[1].account_id).unwrap();
        //        assert_eq!(
        //            account,
        //            AccountViewCallResult {
        //                account_id: block_producers[1].account_id.clone(),
        //                nonce: 0,
        //                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
        //                stake: TESTING_INIT_STAKE,
        //                public_keys: vec![block_producers[1].signer.public_key()],
        //                code_hash: account.code_hash
        //            }
        //        );
        //
        //        let account = nightshade.view_account(state_root, &new_validator.account_id).unwrap();
        //        assert_eq!(
        //            account,
        //            AccountViewCallResult {
        //                account_id: new_validator.account_id.clone(),
        //                nonce: 1,
        //                amount: TESTING_INIT_STAKE,
        //                stake: TESTING_INIT_STAKE * 2,
        //                public_keys: vec![new_validator.signer.public_key()],
        //                code_hash: account.code_hash
        //            }
        //        );
        //
        //        state_root = nightshade.update(&state_root, 7, &h[6], &h[7], &vec![], &vec![]).0;
        //        nightshade.add_validator_proposals(h[6], h[7], 7, vec![], vec![], vec![], 0, 0).unwrap();
        //        state_root = nightshade.update(&state_root, 8, &h[7], &h[8], &vec![], &vec![]).0;
        //        nightshade.add_validator_proposals(h[7], h[8], 8, vec![], vec![], vec![], 0, 0).unwrap();
        //
        //        let account = nightshade.view_account(state_root, &block_producers[1].account_id).unwrap();
        //        assert_eq!(
        //            account,
        //            AccountViewCallResult {
        //                account_id: block_producers[1].account_id.clone(),
        //                nonce: 0,
        //                amount: TESTING_INIT_BALANCE,
        //                stake: 0,
        //                public_keys: vec![block_producers[1].signer.public_key()],
        //                code_hash: account.code_hash
        //            }
        //        );
        //
        //        state_root = nightshade.update(&state_root, 9, &h[8], &h[9], &vec![], &vec![]).0;
        //        nightshade.add_validator_proposals(h[8], h[9], 9, vec![], vec![], vec![], 0, 0).unwrap();
        //        state_root = nightshade.update(&state_root, 10, &h[9], &h[10], &vec![], &vec![]).0;
        //        nightshade.add_validator_proposals(h[9], h[10], 10, vec![], vec![], vec![], 0, 0).unwrap();
        //
        //        // make sure their is no double return of stake
        //        let account = nightshade.view_account(state_root, &block_producers[1].account_id).unwrap();
        //        assert_eq!(
        //            account,
        //            AccountViewCallResult {
        //                account_id: block_producers[1].account_id.clone(),
        //                nonce: 0,
        //                amount: TESTING_INIT_BALANCE,
        //                stake: 0,
        //                public_keys: vec![block_producers[1].signer.public_key()],
        //                code_hash: account.code_hash
        //            }
        //        );
    }

    //    #[test]
    //    fn test_validator_stake_change() {
    //        let dir = TempDir::new("validator_stake_change").unwrap();
    //        let store = create_store(&get_store_path(dir.path()));
    //        let num_nodes = 2;
    //        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
    //        let mut genesis_config =
    //            GenesisConfig::test(validators.iter().map(|v| v.as_str()).collect());
    //        genesis_config.epoch_length = 2;
    //        let nightshade = NightshadeRuntime::new(dir.path(), store, genesis_config);
    //        let (store_update, state_roots) = nightshade.genesis_state();
    //        store_update.commit().unwrap();
    //        let mut state_root = state_roots[0];
    //        let block_producers: Vec<_> =
    //            validators.iter().map(|id| InMemorySigner::from_seed(id, id).into()).collect();
    //        let (h0, h1, h2, h3, h4, h5, h6) =
    //            (hash(&[0]), hash(&[1]), hash(&[2]), hash(&[3]), hash(&[4]), hash(&[5]), hash(&[6]));
    //        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE - 1);
    //        let (new_root, validator_stakes, _) = nightshade.update(
    //            &state_root,
    //            0,
    //            &CryptoHash::default(),
    //            &h0,
    //            &vec![],
    //            &vec![staking_transaction],
    //        );
    //        state_root = new_root;
    //        assert_eq!(
    //            validator_stakes,
    //            vec![ValidatorStake::new(
    //                block_producers[0].account_id.clone(),
    //                block_producers[0].signer.public_key(),
    //                TESTING_INIT_STAKE - 1
    //            )]
    //        );
    //        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
    //        assert_eq!(
    //            account,
    //            AccountViewCallResult {
    //                account_id: block_producers[0].account_id.clone(),
    //                nonce: 1,
    //                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
    //                stake: TESTING_INIT_STAKE,
    //                public_keys: vec![block_producers[0].signer.public_key()],
    //                code_hash: account.code_hash
    //            }
    //        );
    //
    //        nightshade
    //            .add_validator_proposals(
    //                CryptoHash::default(),
    //                h0,
    //                0,
    //                validator_stakes,
    //                vec![],
    //                vec![],
    //                0,
    //                0,
    //            )
    //            .unwrap();
    //
    //        state_root = nightshade.update(&state_root, 1, &h0, &h1, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h0, h1, 1, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        state_root = nightshade.update(&state_root, 2, &h1, &h2, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h1, h2, 2, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        state_root = nightshade.update(&state_root, 3, &h2, &h3, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h2, h3, 3, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        state_root = nightshade.update(&state_root, 4, &h3, &h4, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h3, h4, 4, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
    //        assert_eq!(
    //            account,
    //            AccountViewCallResult {
    //                account_id: block_producers[0].account_id.clone(),
    //                nonce: 1,
    //                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
    //                stake: TESTING_INIT_STAKE,
    //                public_keys: vec![block_producers[0].signer.public_key()],
    //                code_hash: account.code_hash
    //            }
    //        );
    //
    //        state_root = nightshade.update(&state_root, 5, &h4, &h5, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h4, h5, 5, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        state_root = nightshade.update(&state_root, 6, &h5, &h6, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h5, h6, 6, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
    //        assert_eq!(
    //            account,
    //            AccountViewCallResult {
    //                account_id: block_producers[0].account_id.clone(),
    //                nonce: 1,
    //                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1,
    //                stake: TESTING_INIT_STAKE - 1,
    //                public_keys: vec![block_producers[0].signer.public_key()],
    //                code_hash: account.code_hash
    //            }
    //        );
    //    }
    //
    //    #[test]
    //    fn test_validator_stake_change_multiple_times() {
    //        let dir = TempDir::new("validator_stake_change_multiple_times").unwrap();
    //        let store = create_store(&get_store_path(dir.path()));
    //        let num_nodes = 2;
    //        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
    //        let mut genesis_config =
    //            GenesisConfig::test(validators.iter().map(|v| v.as_str()).collect());
    //        genesis_config.epoch_length = 2;
    //        let nightshade = NightshadeRuntime::new(dir.path(), store, genesis_config);
    //        let (store_update, state_roots) = nightshade.genesis_state();
    //        store_update.commit().unwrap();
    //        let mut state_root = state_roots[0];
    //        let block_producers: Vec<_> =
    //            validators.iter().map(|id| InMemorySigner::from_seed(id, id).into()).collect();
    //        let (h0, h1, h2, h3, h4, h5, h6, h7, h8) = (
    //            hash(&[0]),
    //            hash(&[1]),
    //            hash(&[2]),
    //            hash(&[3]),
    //            hash(&[4]),
    //            hash(&[5]),
    //            hash(&[6]),
    //            hash(&[7]),
    //            hash(&[8]),
    //        );
    //        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE - 1);
    //        let staking_transaction1 = stake(2, &block_producers[0], TESTING_INIT_STAKE - 2);
    //        let staking_transaction2 = stake(1, &block_producers[1], TESTING_INIT_STAKE + 1);
    //        let (new_root, validator_stakes, _) = nightshade.update(
    //            &state_root,
    //            0,
    //            &CryptoHash::default(),
    //            &h0,
    //            &vec![],
    //            &vec![staking_transaction, staking_transaction1, staking_transaction2],
    //        );
    //        state_root = new_root;
    //        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
    //        assert_eq!(
    //            account,
    //            AccountViewCallResult {
    //                account_id: block_producers[0].account_id.clone(),
    //                nonce: 2,
    //                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
    //                stake: TESTING_INIT_STAKE,
    //                public_keys: vec![block_producers[0].signer.public_key()],
    //                code_hash: account.code_hash
    //            }
    //        );
    //
    //        nightshade
    //            .add_validator_proposals(
    //                CryptoHash::default(),
    //                h0,
    //                0,
    //                validator_stakes,
    //                vec![],
    //                vec![],
    //                0,
    //                0,
    //            )
    //            .unwrap();
    //
    //        state_root = nightshade.update(&state_root, 1, &h0, &h1, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h0, h1, 1, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        let staking_transaction = stake(3, &block_producers[0], TESTING_INIT_STAKE + 1);
    //        let staking_transaction1 = stake(2, &block_producers[1], TESTING_INIT_STAKE + 2);
    //        let staking_transaction2 = stake(3, &block_producers[1], TESTING_INIT_STAKE - 1);
    //
    //        let (new_root, validator_stakes, _) = nightshade.update(
    //            &state_root,
    //            2,
    //            &h1,
    //            &h2,
    //            &vec![],
    //            &vec![staking_transaction, staking_transaction1, staking_transaction2],
    //        );
    //        state_root = new_root;
    //        nightshade
    //            .add_validator_proposals(h1, h2, 2, validator_stakes, vec![], vec![], 0, 0)
    //            .unwrap();
    //
    //        state_root = nightshade.update(&state_root, 3, &h2, &h3, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h2, h3, 3, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        state_root = nightshade.update(&state_root, 4, &h3, &h4, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h3, h4, 4, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
    //        assert_eq!(
    //            account,
    //            AccountViewCallResult {
    //                account_id: block_producers[0].account_id.clone(),
    //                nonce: 3,
    //                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1,
    //                stake: TESTING_INIT_STAKE + 1,
    //                public_keys: vec![block_producers[0].signer.public_key()],
    //                code_hash: account.code_hash
    //            }
    //        );
    //
    //        let account = nightshade.view_account(state_root, &block_producers[1].account_id).unwrap();
    //        assert_eq!(
    //            account,
    //            AccountViewCallResult {
    //                account_id: block_producers[1].account_id.clone(),
    //                nonce: 3,
    //                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1,
    //                stake: TESTING_INIT_STAKE + 1,
    //                public_keys: vec![block_producers[1].signer.public_key()],
    //                code_hash: account.code_hash
    //            }
    //        );
    //
    //        state_root = nightshade.update(&state_root, 5, &h4, &h5, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h4, h5, 5, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        state_root = nightshade.update(&state_root, 6, &h5, &h6, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h5, h6, 6, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
    //        assert_eq!(
    //            account,
    //            AccountViewCallResult {
    //                account_id: block_producers[0].account_id.clone(),
    //                nonce: 3,
    //                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1,
    //                stake: TESTING_INIT_STAKE + 1,
    //                public_keys: vec![block_producers[0].signer.public_key()],
    //                code_hash: account.code_hash
    //            }
    //        );
    //
    //        let account = nightshade.view_account(state_root, &block_producers[1].account_id).unwrap();
    //        assert_eq!(
    //            account,
    //            AccountViewCallResult {
    //                account_id: block_producers[1].account_id.clone(),
    //                nonce: 3,
    //                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1,
    //                stake: TESTING_INIT_STAKE + 1,
    //                public_keys: vec![block_producers[1].signer.public_key()],
    //                code_hash: account.code_hash
    //            }
    //        );
    //
    //        state_root = nightshade.update(&state_root, 7, &h6, &h7, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h6, h7, 7, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        state_root = nightshade.update(&state_root, 8, &h7, &h8, &vec![], &vec![]).0;
    //        nightshade.add_validator_proposals(h7, h8, 8, vec![], vec![], vec![], 0, 0).unwrap();
    //
    //        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
    //        assert_eq!(
    //            account,
    //            AccountViewCallResult {
    //                account_id: block_producers[0].account_id.clone(),
    //                nonce: 3,
    //                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1,
    //                stake: TESTING_INIT_STAKE + 1,
    //                public_keys: vec![block_producers[0].signer.public_key()],
    //                code_hash: account.code_hash
    //            }
    //        );
    //
    //        let account = nightshade.view_account(state_root, &block_producers[1].account_id).unwrap();
    //        assert_eq!(
    //            account,
    //            AccountViewCallResult {
    //                account_id: block_producers[1].account_id.clone(),
    //                nonce: 3,
    //                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1,
    //                stake: TESTING_INIT_STAKE - 1,
    //                public_keys: vec![block_producers[1].signer.public_key()],
    //                code_hash: account.code_hash
    //            }
    //        );
    //    }
    //
    //    #[test]
    //    fn test_verify_validator_signature() {
    //        let dir = TempDir::new("verify_validator_signature").unwrap();
    //        let store = create_store(&get_store_path(dir.path()));
    //        let num_nodes = 2;
    //        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
    //        let mut genesis_config =
    //            GenesisConfig::test(validators.iter().map(|v| v.as_str()).collect());
    //        genesis_config.epoch_length = 2;
    //        let nightshade = NightshadeRuntime::new(dir.path(), store, genesis_config);
    //        let (store_update, _) = nightshade.genesis_state();
    //        store_update.commit().unwrap();
    //        let data = [0; 32];
    //        let signer = InMemorySigner::from_seed(&validators[0], &validators[0]);
    //        let signature = signer.sign(&data);
    //        assert!(nightshade.verify_validator_signature(
    //            &CryptoHash::default(),
    //            &validators[0],
    //            &data,
    //            &signature
    //        ));
    //    }
    //
    #[test]
    fn test_verify_validator_signature_failure() {
        let dir = TempDir::new("verify_validator_signature_failure").unwrap();
        let store = create_store(&get_store_path(dir.path()));
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut genesis_config =
            GenesisConfig::test(validators.iter().map(|v| v.as_str()).collect());
        genesis_config.epoch_length = 2;
        let nightshade = NightshadeRuntime::new(dir.path(), store, genesis_config);
        let (store_update, _) = nightshade.genesis_state();
        store_update.commit().unwrap();
        let data = [0; 32];
        let signer = InMemorySigner::from_seed(&validators[0], &validators[0]);
        let signature = signer.sign(&data);
        assert!(!nightshade.verify_validator_signature(
            &EpochId::default(),
            &validators[1],
            &data,
            &signature
        ));
    }
}
