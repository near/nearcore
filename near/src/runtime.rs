use std::collections::HashSet;
use std::convert::TryFrom;
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use kvdb::DBValue;
use log::{debug, info};

use near_chain::types::{ApplyTransactionResult, ValidatorSignatureVerificationResult};
use near_chain::{BlockHeader, Error, ErrorKind, RuntimeAdapter, ValidTransaction, Weight};
use near_epoch_manager::{BlockInfo, EpochConfig, EpochManager, RewardCalculator};
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
        let reward_calculator = RewardCalculator {
            max_inflation_rate: genesis_config.max_inflation_rate,
            num_blocks_per_year: genesis_config.num_blocks_per_year,
            epoch_length: genesis_config.epoch_length,
            validator_reward_percentage: 100 - genesis_config.developer_reward_percentage,
            protocol_reward_percentage: genesis_config.protocol_reward_percentage,
            protocol_treasury_account: genesis_config.protocol_treasury_account.to_string(),
        };
        let epoch_manager = RwLock::new(
            EpochManager::new(
                store.clone(),
                initial_epoch_config,
                reward_calculator,
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
        _shard_id: ShardId,
        _state_root: &MerkleHash,
        block_hash: &CryptoHash,
        state_update: &mut TrieUpdate,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        let (stake_info, validator_reward) = epoch_manager.compute_stake_return_info(block_hash)?;

        for (account_id, max_of_stakes) in stake_info {
            let account: Option<Account> = get_account(state_update, &account_id);
            if let Some(mut account) = account {
                if let Some(reward) = validator_reward.get(&account_id) {
                    println!(
                        "account {} adding reward {} to stake {}",
                        account_id, reward, account.stake
                    );
                    account.stake += *reward;
                }

                println!(
                    "account {} stake {} max_of_stakes: {}",
                    account_id, account.stake, max_of_stakes
                );
                assert!(account.stake >= max_of_stakes, "FATAL: staking invariance does not hold");
                let return_stake = account.stake - max_of_stakes;
                account.stake -= return_stake;
                account.amount += return_stake;

                set_account(state_update, &account_id, &account);
            }
        }
        if let Some(mut protocol_treasury_account) =
            get_account(state_update, &self.genesis_config.protocol_treasury_account)
        {
            protocol_treasury_account.amount +=
                *validator_reward.get(&self.genesis_config.protocol_treasury_account).unwrap();
            set_account(
                state_update,
                &self.genesis_config.protocol_treasury_account,
                &protocol_treasury_account,
            );
        }
        Ok(())
    }
}

pub fn account_id_to_shard_id(account_id: &AccountId, num_shards: ShardId) -> ShardId {
    let mut cursor = Cursor::new((hash(&account_id.clone().into_bytes()).0).0);
    cursor.read_u64::<LittleEndian>().expect("Must not happened") % (num_shards)
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
        let validator = epoch_manager.get_block_producer_info(&header.epoch_id, header.height)?;
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
    ) -> ValidatorSignatureVerificationResult {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        if let Ok(Some(validator)) = epoch_manager.get_validator_by_account_id(epoch_id, account_id)
        {
            if verify(data, signature, &validator.public_key) {
                ValidatorSignatureVerificationResult::Valid
            } else {
                ValidatorSignatureVerificationResult::Invalid
            }
        } else {
            ValidatorSignatureVerificationResult::UnknownEpoch
        }
    }

    fn verify_chunk_header_signature(&self, header: &ShardChunkHeader) -> Result<bool, Error> {
        let epoch_id = self.get_epoch_id_from_prev_block(&header.prev_block_hash)?;
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
        account_id: &AccountId,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager
            .cares_about_shard_from_prev_block(parent_hash, account_id, shard_id)
            .unwrap_or(false)
    }

    fn will_care_about_shard(
        &self,
        account_id: &AccountId,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
        epoch_manager
            .cares_about_shard_next_epoch_from_prev_block(parent_hash, account_id, shard_id)
            .unwrap_or(false)
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
        gas_price: Balance,
        total_supply: Balance,
    ) -> Result<(), Error> {
        // Check that genesis block doesn't have any proposals.
        assert!(block_index > 0 || (proposals.len() == 0 && slashed_validators.len() == 0));
        println!("add validator proposals at block index {} {:?}", block_index, proposals);
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
            validator_mask,
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

    fn apply_transactions(
        &self,
        shard_id: ShardId,
        state_root: &MerkleHash,
        block_index: BlockIndex,
        prev_block_hash: &CryptoHash,
        _block_hash: &CryptoHash,
        receipts: &Vec<ReceiptTransaction>,
        transactions: &Vec<SignedTransaction>,
    ) -> Result<ApplyTransactionResult, Error> {
        let mut state_update = TrieUpdate::new(self.trie.clone(), *state_root);
        let should_update_account = {
            let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
            println!(
                "block index: {}, is next_block_epoch_start {}",
                block_index,
                epoch_manager.is_next_block_epoch_start(prev_block_hash).unwrap()
            );
            epoch_manager.is_next_block_epoch_start(prev_block_hash)?
        };

        // If we are starting to apply 1st block in the new epoch.
        if should_update_account {
            println!("block index: {}", block_index);
            self.update_validator_accounts(
                shard_id,
                state_root,
                prev_block_hash,
                &mut state_update,
            )
            .map_err(|e| Error::from(ErrorKind::ValidatorError(e.to_string())))?;
        }
        let apply_state = ApplyState {
            root: *state_root,
            shard_id,
            block_index,
            parent_block_hash: *prev_block_hash,
            epoch_length: self.genesis_config.epoch_length,
        };

        let apply_result = self
            .runtime
            .apply(state_update, &apply_state, &receipts, &transactions)
            .map_err(|err| Error::from(ErrorKind::Other(err.to_string())))?;

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
    use near_primitives::block::Weight;
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

    use crate::config::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
    use crate::{get_store_path, GenesisConfig, NightshadeRuntime};

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
        pub last_receipts: Vec<Vec<ReceiptTransaction>>,
    }

    impl TestEnv {
        pub fn new(prefix: &str, validators: Vec<AccountId>, epoch_length: BlockIndex) -> Self {
            let dir = TempDir::new(prefix).unwrap();
            let store = create_store(&get_store_path(dir.path()));
            let mut genesis_config =
                GenesisConfig::test(validators.iter().map(|v| v.as_str()).collect());
            genesis_config.epoch_length = epoch_length;
            let runtime = NightshadeRuntime::new(dir.path(), store, genesis_config.clone());
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
                last_receipts: vec![],
            }
        }

        pub fn step(&mut self, transactions: Vec<SignedTransaction>) {
            // TODO: add support for shards.
            let new_hash = hash(&vec![(self.head.height + 1) as u8]);
            let (state_root, proposals, receipts) = self.runtime.update(
                &self.state_roots[0],
                self.head.height + 1,
                &self.head.last_block_hash,
                &new_hash,
                &self.last_receipts.iter().flatten().cloned().collect::<Vec<_>>(),
                &transactions,
            );
            self.state_roots[0] = state_root;
            self.last_receipts = receipts;
            self.runtime
                .add_validator_proposals(
                    self.head.last_block_hash,
                    new_hash,
                    self.head.height + 1,
                    proposals,
                    vec![],
                    vec![],
                    0,
                    self.runtime.genesis_config.gas_price,
                    self.runtime.genesis_config.total_supply,
                )
                .unwrap();
            self.head = Tip {
                last_block_hash: new_hash,
                prev_block_hash: self.head.last_block_hash,
                height: self.head.height + 1,
                epoch_id: self.runtime.get_epoch_id_from_prev_block(&new_hash).unwrap(),
                total_weight: Weight::from(self.head.total_weight.to_num() + 1),
            };
        }

        pub fn view_account(&self, account_id: &str) -> AccountViewCallResult {
            // TODO: add support for shards.
            self.runtime.view_account(self.state_roots[0], &account_id.to_string()).unwrap()
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
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new("test_validator_rotation", validators.clone(), 2);
        let block_producers: Vec<_> =
            validators.iter().map(|id| InMemorySigner::from_seed(id, id).into()).collect();
        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE * 2);

        // test1 stakes twice the current stake, because test1 and test2 have the same amount of stake before, test2 will be
        // kicked out.
        env.step(vec![staking_transaction]);
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
        env.step(vec![create_account_transaction]);
        env.step(vec![staking_transaction]);

        // Roll steps for 3 epochs to pass.
        for _ in 4..=9 {
            env.step(vec![]);
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
            (test1_acc.amount, test1_acc.stake),
            (
                TESTING_INIT_BALANCE - 5 * TESTING_INIT_STAKE + per_epoch_per_validator_reward,
                2 * TESTING_INIT_STAKE + 3 * per_epoch_per_validator_reward
            )
        );
        let test2_acc = env.view_account("test2");
        // Got money back after being kicked out.
        assert_eq!(
            (test2_acc.amount, test2_acc.stake),
            (TESTING_INIT_BALANCE + 3 * per_epoch_per_validator_reward, 0)
        );
        let test3_acc = env.view_account("test3");
        // Got 3 * X, staking 2 * X of them.
        assert_eq!(
            (test3_acc.amount, test3_acc.stake),
            (TESTING_INIT_STAKE, 2 * TESTING_INIT_STAKE + per_epoch_per_validator_reward)
        );
        let protocol_treasury =
            env.view_account(&env.runtime.genesis_config.protocol_treasury_account);
        assert_eq!(
            (protocol_treasury.amount, protocol_treasury.stake),
            (TESTING_INIT_BALANCE + 4 * per_epoch_protocol_treasury, 0)
        );
    }

    /// One validator tries to decrease their stake in epoch T. Make sure that the stake return happens in epoch T+3.
    #[test]
    fn test_validator_stake_change() {
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new("test_validator_stake_change", validators.clone(), 2);
        let block_producers: Vec<_> =
            validators.iter().map(|id| InMemorySigner::from_seed(id, id).into()).collect();
        let (per_epoch_per_validator_reward, _) = env.compute_reward(num_nodes);

        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE - 1);
        env.step(vec![staking_transaction]);
        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                nonce: 1,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
                stake: TESTING_INIT_STAKE,
                public_keys: vec![block_producers[0].signer.public_key()],
                code_hash: account.code_hash
            }
        );
        for _ in 2..=4 {
            env.step(vec![]);
        }

        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                nonce: 1,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE + per_epoch_per_validator_reward,
                stake: TESTING_INIT_STAKE,
                public_keys: vec![block_producers[0].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        for _ in 5..=7 {
            env.step(vec![]);
        }

        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                nonce: 1,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE
                    + 1
                    + per_epoch_per_validator_reward,
                stake: TESTING_INIT_STAKE - 1 + per_epoch_per_validator_reward * 2,
                public_keys: vec![block_producers[0].signer.public_key()],
                code_hash: account.code_hash
            }
        );
    }

    #[test]
    fn test_validator_stake_change_multiple_times() {
        let num_nodes = 4;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env =
            TestEnv::new("test_validator_stake_change_multiple_times", validators.clone(), 4);
        let block_producers: Vec<_> =
            validators.iter().map(|id| InMemorySigner::from_seed(id, id).into()).collect();
        let (per_epoch_per_validator_reward, _) = env.compute_reward(num_nodes);

        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE - 1);
        let staking_transaction1 = stake(2, &block_producers[0], TESTING_INIT_STAKE - 2);
        let staking_transaction2 = stake(1, &block_producers[1], TESTING_INIT_STAKE + 1);
        env.step(vec![staking_transaction, staking_transaction1, staking_transaction2]);
        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                nonce: 2,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
                stake: TESTING_INIT_STAKE,
                public_keys: vec![block_producers[0].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        let staking_transaction = stake(3, &block_producers[0], TESTING_INIT_STAKE + 1);
        let staking_transaction1 = stake(2, &block_producers[1], TESTING_INIT_STAKE + 2);
        let staking_transaction2 = stake(3, &block_producers[1], TESTING_INIT_STAKE - 1);
        let staking_transaction3 =
            stake(1, &block_producers[3], TESTING_INIT_STAKE - per_epoch_per_validator_reward - 1);
        env.step(vec![
            staking_transaction,
            staking_transaction1,
            staking_transaction2,
            staking_transaction3,
        ]);

        for _ in 3..=8 {
            env.step(vec![]);
        }

        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                nonce: 3,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1
                    + per_epoch_per_validator_reward,
                stake: TESTING_INIT_STAKE + 1,
                public_keys: vec![block_producers[0].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        let account = env.view_account(&block_producers[1].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[1].account_id.clone(),
                nonce: 3,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE + per_epoch_per_validator_reward,
                stake: TESTING_INIT_STAKE,
                public_keys: vec![block_producers[1].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        let account = env.view_account(&block_producers[2].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[2].account_id.clone(),
                nonce: 0,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
                stake: TESTING_INIT_STAKE + per_epoch_per_validator_reward,
                public_keys: vec![block_producers[2].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        let account = env.view_account(&block_producers[3].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[3].account_id.clone(),
                nonce: 1,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE + per_epoch_per_validator_reward,
                stake: TESTING_INIT_STAKE,
                public_keys: vec![block_producers[3].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        for _ in 9..=12 {
            env.step(vec![]);
        }

        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                nonce: 3,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1
                    + per_epoch_per_validator_reward,
                stake: TESTING_INIT_STAKE + 1 + per_epoch_per_validator_reward,
                public_keys: vec![block_producers[0].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        // Note: this is not a bug but rather a feature: when one changes their stake for
        // less than the reward they get in an epoch, and the stake change happens an epoch
        // after they stake, the difference in stakes will be returned in 2 epochs rather than
        // 3.
        let account = env.view_account(&block_producers[1].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[1].account_id.clone(),
                nonce: 3,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE
                    + per_epoch_per_validator_reward
                    + 1,
                stake: TESTING_INIT_STAKE + per_epoch_per_validator_reward - 1,
                public_keys: vec![block_producers[1].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        let account = env.view_account(&block_producers[2].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[2].account_id.clone(),
                nonce: 0,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
                stake: TESTING_INIT_STAKE + 2 * per_epoch_per_validator_reward,
                public_keys: vec![block_producers[2].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        let account = env.view_account(&block_producers[3].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[3].account_id.clone(),
                nonce: 1,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE
                    + 2 * per_epoch_per_validator_reward,
                stake: TESTING_INIT_STAKE,
                public_keys: vec![block_producers[3].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        for _ in 13..=16 {
            env.step(vec![]);
        }

        let account = env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                nonce: 3,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1
                    + per_epoch_per_validator_reward,
                stake: TESTING_INIT_STAKE + 1 + 2 * per_epoch_per_validator_reward,
                public_keys: vec![block_producers[0].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        let account = env.view_account(&block_producers[1].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[1].account_id.clone(),
                nonce: 3,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE
                    + 1
                    + per_epoch_per_validator_reward,
                stake: TESTING_INIT_STAKE - 1 + 2 * per_epoch_per_validator_reward,
                public_keys: vec![block_producers[1].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        let account = env.view_account(&block_producers[2].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[2].account_id.clone(),
                nonce: 0,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
                stake: TESTING_INIT_STAKE + 3 * per_epoch_per_validator_reward,
                public_keys: vec![block_producers[2].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        let account = env.view_account(&block_producers[3].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[3].account_id.clone(),
                nonce: 1,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE
                    + 2 * per_epoch_per_validator_reward
                    + 1,
                stake: TESTING_INIT_STAKE + per_epoch_per_validator_reward - 1,
                public_keys: vec![block_producers[3].signer.public_key()],
                code_hash: account.code_hash
            }
        );
    }

    #[test]
    fn test_verify_validator_signature() {
        let validators = (0..2).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let env = TestEnv::new("verify_validator_signature_failure", validators.clone(), 2);
        let data = [0; 32];
        let signer = InMemorySigner::from_seed(&validators[0], &validators[0]);
        let signature = signer.sign(&data);
        assert!(env.runtime.verify_validator_signature(
            &EpochId::default(),
            &validators[0],
            &data,
            &signature
        ));
    }

    #[test]
    fn test_verify_validator_signature_failure() {
        let validators = (0..2).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let env = TestEnv::new("verify_validator_signature_failure", validators.clone(), 2);
        let data = [0; 32];
        let signer = InMemorySigner::from_seed(&validators[0], &validators[0]);
        let signature = signer.sign(&data);
        assert!(!env.runtime.verify_validator_signature(
            &EpochId::default(),
            &validators[1],
            &data,
            &signature
        ));
    }

    #[test]
    fn test_state_sync() {
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env =
            TestEnv::new("test_validator_stake_change_multiple_times", validators.clone(), 2);
        let block_producers: Vec<_> =
            validators.iter().map(|id| InMemorySigner::from_seed(id, id).into()).collect();
        let (per_epoch_per_validator_reward, _) = env.compute_reward(num_nodes);
        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE + 1);
        env.step(vec![staking_transaction]);
        env.step(vec![]);
        let state_dump = env.runtime.dump_state(0, env.state_roots[0]).unwrap();
        let mut new_env =
            TestEnv::new("test_validator_stake_change_multiple_times1", validators.clone(), 2);
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
                    vec![],
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
            new_env.step(vec![]);
        }

        let account = new_env.view_account(&block_producers[0].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                nonce: 1,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1
                    + per_epoch_per_validator_reward,
                stake: TESTING_INIT_STAKE + 1 + per_epoch_per_validator_reward,
                public_keys: vec![block_producers[0].signer.public_key()],
                code_hash: account.code_hash
            }
        );

        let account = new_env.view_account(&block_producers[1].account_id);
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[1].account_id.clone(),
                nonce: 0,
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
                stake: TESTING_INIT_STAKE + 2 * per_epoch_per_validator_reward,
                public_keys: vec![block_producers[1].signer.public_key()],
                code_hash: account.code_hash
            }
        );
    }
}
