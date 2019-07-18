use std::convert::TryFrom;
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::{debug, info};

use near_chain::{
    BlockHeader, Error, ErrorKind, ReceiptResult, RuntimeAdapter, ValidTransaction, Weight,
};
use near_primitives::account::AccessKey;
use near_primitives::crypto::signature::{verify, PublicKey, Signature};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::rpc::{AccountViewCallResult, QueryResponse, ViewStateResult};
use near_primitives::transaction::{ReceiptTransaction, SignedTransaction, TransactionResult};
use near_primitives::types::{AccountId, BlockIndex, MerkleHash, ShardId, ValidatorStake};
use near_primitives::utils::prefix_for_access_key;
use near_store::{get, Store, StoreUpdate, Trie, TrieUpdate, WrappedTrieChanges};
use near_verifier::TransactionVerifier;
use node_runtime::adapter::query_client;
use node_runtime::ethereum::EthashProvider;
use node_runtime::state_viewer::TrieViewer;
use node_runtime::{ApplyState, Runtime, ETHASH_CACHE_PATH};

use crate::config::GenesisConfig;
use crate::validator_manager::{ValidatorEpochConfig, ValidatorManager};
use kvdb::DBValue;
use near_primitives::sharding::ShardChunkHeader;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Defines Nightshade state transition, validator rotation and block weight for fork choice rule.
/// TODO: this possibly should be merged with the runtime cargo or at least reconsiled on the interfaces.
pub struct NightshadeRuntime {
    genesis_config: GenesisConfig,

    store: Arc<Store>,
    pub trie: Arc<Trie>,
    trie_viewer: TrieViewer,
    runtime: Runtime,
    validator_manager: RwLock<ValidatorManager>,
}

impl NightshadeRuntime {
    pub fn new(home_dir: &Path, store: Arc<Store>, genesis_config: GenesisConfig) -> Self {
        let trie = Arc::new(Trie::new(store.clone()));
        let mut ethash_dir = home_dir.to_owned();
        ethash_dir.push(ETHASH_CACHE_PATH);
        let ethash_provider = Arc::new(Mutex::new(EthashProvider::new(ethash_dir.as_path())));
        let runtime = Runtime::new(ethash_provider.clone());
        let trie_viewer = TrieViewer::new(ethash_provider);
        let initial_epoch_config = ValidatorEpochConfig {
            epoch_length: genesis_config.epoch_length,
            rng_seed: [0; 32],
            num_shards: genesis_config.block_producers_per_shard.len() as ShardId,
            num_block_producers: genesis_config.num_block_producers,
            block_producers_per_shard: genesis_config.block_producers_per_shard.clone(),
            avg_fisherman_per_shard: genesis_config.avg_fisherman_per_shard.clone(),
            validator_kickout_threshold: genesis_config.validator_kickout_threshold,
        };
        let validator_manager = RwLock::new(
            ValidatorManager::new(
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
                store.clone(),
            )
            .expect("Failed to start Validator Manager"),
        );
        NightshadeRuntime { genesis_config, store, trie, runtime, trie_viewer, validator_manager }
    }

    fn get_block_proposer_info(
        &self,
        parent_hash: CryptoHash,
        height: BlockIndex,
    ) -> Result<ValidatorStake, Box<dyn std::error::Error>> {
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        let (epoch_hash, idx) = vm.get_epoch_offset(parent_hash, height)?;
        let validator_assignemnt = vm.get_validators(epoch_hash)?;
        let total_seats: u64 = validator_assignemnt.block_producers.iter().sum();
        let mut cur_seats = 0;
        for (i, seats) in validator_assignemnt.block_producers.iter().enumerate() {
            if cur_seats + seats > idx % total_seats {
                return Ok(validator_assignemnt.validators[i].clone());
            }
            cur_seats += seats;
        }
        unreachable!()
    }

    fn get_chunk_proposer_info(
        &self,
        parent_hash: CryptoHash,
        height: BlockIndex,
        shard_id: ShardId,
    ) -> Result<ValidatorStake, Box<dyn std::error::Error>> {
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        let (epoch_hash, idx) = vm.get_epoch_offset(parent_hash, height)?;
        let validator_assignemnt = vm.get_validators(epoch_hash)?;
        let total_seats: u64 = validator_assignemnt.chunk_producers[shard_id as usize]
            .iter()
            .map(|(_, seats)| seats)
            .sum();
        let mut cur_seats = 0;
        for (index, seats) in validator_assignemnt.chunk_producers[shard_id as usize].iter() {
            if cur_seats + seats > idx % total_seats {
                return Ok(validator_assignemnt.validators[*index].clone());
            }
            cur_seats += seats;
        }
        unreachable!()
    }
}

impl RuntimeAdapter for NightshadeRuntime {
    fn genesis_state(&self) -> (StoreUpdate, Vec<MerkleHash>) {
        let mut store_update = self.store.store_update();
        let mut state_roots = vec![];
        for shard_id in 0..self.genesis_config.block_producers_per_shard.len() as ShardId {
            let accounts = self
                .genesis_config
                .accounts
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
            let contracts = self
                .genesis_config
                .contracts
                .iter()
                .filter(|(account_id, _)| self.account_id_to_shard_id(account_id) == shard_id)
                .cloned()
                .collect::<Vec<_>>();
            let state_update = TrieUpdate::new(self.trie.clone(), MerkleHash::default());
            let (shard_store_update, state_root) =
                self.runtime.apply_genesis_state(state_update, &accounts, &validators, &contracts);
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
        let validator = self
            .get_block_proposer_info(header.prev_hash, header.height)
            .map_err(|err| ErrorKind::Other(err.to_string()))?;
        if !header.verify_block_producer(&validator.public_key) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.total_weight.next(header.approval_sigs.len() as u64))
    }

    fn verify_chunk_header_signature(&self, header: &ShardChunkHeader) -> bool {
        let public_key = &self
            .get_chunk_proposer_info(header.prev_block_hash, header.height_created, header.shard_id)
            .map(|vs| vs.public_key);
        if let Ok(public_key) = public_key {
            verify(header.chunk_hash().0.as_ref(), &header.signature, public_key)
        } else {
            false
        }
    }

    fn get_epoch_block_proposers(
        &self,
        parent_hash: CryptoHash,
        height: BlockIndex,
    ) -> Result<Vec<(AccountId, u64)>, Box<dyn std::error::Error>> {
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        let (epoch_hash, _) = vm.get_epoch_offset(parent_hash, height)?;
        let validator_assignemnt = vm.get_validators(epoch_hash)?;
        Ok(validator_assignemnt
            .block_producers
            .iter()
            .enumerate()
            .map(|(index, seats)| {
                (validator_assignemnt.validators[index].account_id.clone(), *seats)
            })
            .collect())
    }

    fn get_block_proposer(
        &self,
        parent_hash: CryptoHash,
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        Ok(self.get_block_proposer_info(parent_hash, height)?.account_id)
    }

    fn get_chunk_proposer(
        &self,
        parent_hash: CryptoHash,
        height: BlockIndex,
        shard_id: ShardId,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        Ok(self.get_chunk_proposer_info(parent_hash, height, shard_id)?.account_id)
    }

    fn check_validator_signature(&self, _account_id: &AccountId, _signature: &Signature) -> bool {
        true
    }

    fn num_shards(&self) -> ShardId {
        // TODO: should be dynamic.
        self.genesis_config.block_producers_per_shard.len() as ShardId
    }

    fn num_total_parts(&self, parent_hash: CryptoHash) -> usize {
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        let (epoch_hash, _idx) = vm.get_epoch_offset(parent_hash, 0).unwrap();
        if let Ok(validator_assignment) = vm.get_validators(epoch_hash) {
            let ret = validator_assignment.validators.len();
            if ret > 1 {
                ret
            } else {
                2
            }
        } else {
            2
        }
    }

    fn num_data_parts(&self, parent_hash: CryptoHash) -> usize {
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
        parent_hash: CryptoHash,
        part_id: u64,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        let (epoch_hash, _idx) = vm.get_epoch_offset(parent_hash, 0)?;
        let validator_assignment = vm.get_validators(epoch_hash)?;

        return Ok(validator_assignment.validators
            [part_id as usize % validator_assignment.validators.len()]
        .account_id
        .clone());
    }

    fn cares_about_shard(
        &self,
        account_id: &AccountId,
        parent_hash: CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        let (epoch_hash, _idx) = match vm.get_epoch_offset(parent_hash, 0) {
            Ok(tuple) => tuple,
            Err(_) => return false,
        };
        if let Ok(validator_assignment) = vm.get_validators(epoch_hash) {
            for (index, _seats) in validator_assignment.chunk_producers[shard_id as usize].iter() {
                if validator_assignment.validators[*index].account_id == *account_id {
                    return true;
                }
            }
        }
        false
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
        validator_mask: Vec<bool>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Deal with validator proposals and epoch finishing.
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        // TODO: don't commit here, instead contribute to upstream store update.
        vm.add_proposals(parent_hash, current_hash, block_index, proposals, validator_mask)?
            .commit()
            .map_err(|err| err.into())
    }

    fn apply_transactions(
        &self,
        shard_id: ShardId,
        state_root: &MerkleHash,
        block_index: BlockIndex,
        prev_block_hash: &CryptoHash,
        receipts: &Vec<ReceiptTransaction>,
        transactions: &Vec<SignedTransaction>,
    ) -> Result<
        (
            WrappedTrieChanges,
            MerkleHash,
            Vec<TransactionResult>,
            ReceiptResult,
            Vec<ValidatorStake>,
        ),
        Box<dyn std::error::Error>,
    > {
        let apply_state = ApplyState {
            root: state_root.clone(),
            shard_id,
            block_index,
            parent_block_hash: *prev_block_hash,
        };
        let state_update = TrieUpdate::new(self.trie.clone(), apply_state.root);
        let apply_result =
            self.runtime.apply(state_update, &apply_state, &receipts, &transactions)?;

        Ok((
            WrappedTrieChanges::new(self.trie.clone(), apply_result.trie_changes),
            apply_result.root,
            apply_result.tx_result,
            apply_result.new_receipts,
            apply_result.validator_proposals,
        ))
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

impl node_runtime::adapter::RuntimeAdapter for NightshadeRuntime {
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
                    let access_key =
                        get::<AccessKey>(&state_update, &key).ok_or("Missing key from iterator")?;
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
