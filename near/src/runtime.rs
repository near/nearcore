use std::convert::TryFrom;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use byteorder::{LittleEndian, ReadBytesExt};
use log::debug;

use near_chain::{
    BlockHeader, Error, ErrorKind, ReceiptResult, RuntimeAdapter, ValidTransaction, Weight,
};
use near_primitives::account::AccessKey;
use near_primitives::crypto::signature::{PublicKey, Signature};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::rpc::ABCIQueryResponse;
use near_primitives::transaction::{ReceiptTransaction, SignedTransaction, TransactionResult};
use near_primitives::types::{AccountId, BlockIndex, Epoch, MerkleHash, ShardId, ValidatorStake};
use near_primitives::utils::prefix_for_access_key;
use near_store::{get, Store, StoreUpdate, Trie, TrieUpdate, WrappedTrieChanges};
use near_verifier::TransactionVerifier;
use node_runtime::adapter::query_client;
use node_runtime::ethereum::EthashProvider;
use node_runtime::state_viewer::{AccountViewCallResult, TrieViewer, ViewStateResult};
use node_runtime::{ApplyState, Runtime, ETHASH_CACHE_PATH};

use crate::config::GenesisConfig;
use crate::validator_manager::{ValidatorEpochConfig, ValidatorManager};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Defines Nightshade state transition, validator rotation and block weight for fork choice rule.
/// TODO: this possibly should be merged with the runtime cargo or at least reconsiled on the interfaces.
pub struct NightshadeRuntime {
    genesis_config: GenesisConfig,

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
            rng_seed: [0; 32],
            num_shards: genesis_config.block_producers_per_shard.len() as ShardId,
            num_block_producers: genesis_config.num_block_producers,
            block_producers_per_shard: genesis_config.block_producers_per_shard.clone(),
            avg_fisherman_per_shard: genesis_config.avg_fisherman_per_shard.clone(),
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
                store,
            )
            .expect("Failed to start Validator Manager"),
        );
        NightshadeRuntime { genesis_config, trie, runtime, trie_viewer, validator_manager }
    }

    /// Returns current epoch and position in the epoch for given height.
    fn height_to_epoch(&self, height: BlockIndex) -> (Epoch, BlockIndex) {
        (height / self.genesis_config.epoch_length, height % self.genesis_config.epoch_length)
    }
}

impl RuntimeAdapter for NightshadeRuntime {
    fn genesis_state(&self, shard_id: ShardId) -> (StoreUpdate, MerkleHash) {
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
        let (store_update, state_root) =
            self.runtime.apply_genesis_state(state_update, &accounts, &validators, &contracts);
        (store_update, state_root)
    }

    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error> {
        let account_info = &self.genesis_config.validators
            [(header.height as usize) % self.genesis_config.validators.len()];
        if !header.verify_block_producer(
            &PublicKey::try_from(account_info.public_key.0.as_str()).unwrap(),
        ) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.total_weight.next(header.approval_sigs.len() as u64))
    }

    fn get_epoch_block_proposers(
        &self,
        height: BlockIndex,
    ) -> Result<Vec<(AccountId, u64)>, Box<dyn std::error::Error>> {
        let (epoch, _) = self.height_to_epoch(height);
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        let validator_assignemnt = vm.get_validators(epoch)?;
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
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        let (epoch, idx) = self.height_to_epoch(height);
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        let validator_assignemnt = vm.get_validators(epoch)?;
        let total_seats: u64 = validator_assignemnt.block_producers.iter().sum();
        let mut cur_seats = 0;
        for (i, seats) in validator_assignemnt.block_producers.iter().enumerate() {
            if cur_seats + seats > idx % total_seats {
                return Ok(validator_assignemnt.validators[i].account_id.clone());
            }
            cur_seats += seats;
        }
        unreachable!()
    }

    fn get_chunk_proposer(
        &self,
        shard_id: ShardId,
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        let (epoch, idx) = self.height_to_epoch(height);
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        let validator_assignemnt = vm.get_validators(epoch)?;
        let total_seats: u64 = validator_assignemnt.chunk_producers[shard_id as usize]
            .iter()
            .map(|(_, seats)| seats)
            .sum();
        let mut cur_seats = 0;
        for (index, seats) in validator_assignemnt.chunk_producers[shard_id as usize].iter() {
            if cur_seats + seats > idx % total_seats {
                return Ok(validator_assignemnt.validators[*index].account_id.clone());
            }
            cur_seats += seats;
        }
        unreachable!()
    }

    fn check_validator_signature(&self, _account_id: &AccountId, _signature: &Signature) -> bool {
        true
    }

    fn num_shards(&self) -> ShardId {
        // TODO: should be dynamic.
        self.genesis_config.block_producers_per_shard.len() as ShardId
    }

    fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        let mut cursor = Cursor::new((hash(&account_id.clone().into_bytes()).0).0);
        cursor.read_u32::<LittleEndian>().expect("Must not happened")
            % (self.genesis_config.block_producers_per_shard.len() as ShardId)
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

    fn apply_transactions(
        &self,
        shard_id: ShardId,
        state_root: &MerkleHash,
        block_index: BlockIndex,
        prev_block_hash: &CryptoHash,
        receipts: &Vec<Vec<ReceiptTransaction>>,
        transactions: &Vec<SignedTransaction>,
    ) -> Result<
        (WrappedTrieChanges, MerkleHash, Vec<TransactionResult>, ReceiptResult),
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

        // Deal with validator proposals and epoch finishing.
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        vm.add_proposals(
            state_root.clone(),
            apply_result.root.clone(),
            apply_result.validator_proposals,
        );
        // If epoch changed, finalize previous epoch.
        // TODO: right now at least one block per epoch is required.
        let (epoch, _) = self.height_to_epoch(block_index);
        if vm.last_epoch() != epoch {
            // TODO(779): provide source of randomness here.
            let mut rng_seed = [0; 32];
            rng_seed.copy_from_slice(prev_block_hash.as_ref());
            // TODO(973): calculate number of shards for dynamic resharding.
            let epoch_config = ValidatorEpochConfig {
                rng_seed,
                num_shards: self.genesis_config.block_producers_per_shard.len() as ShardId,
                num_block_producers: self.genesis_config.num_block_producers,
                block_producers_per_shard: self.genesis_config.block_producers_per_shard.clone(),
                avg_fisherman_per_shard: self.genesis_config.avg_fisherman_per_shard.clone(),
            };
            vm.finalize_epoch(epoch - 1, epoch_config, apply_result.root.clone())?;
        }

        Ok((
            WrappedTrieChanges::new(self.trie.clone(), apply_result.trie_changes),
            apply_result.root,
            apply_result.tx_result,
            apply_result.new_receipts,
        ))
    }

    fn query(
        &self,
        state_root: MerkleHash,
        height: BlockIndex,
        path: &str,
        data: &[u8],
    ) -> Result<ABCIQueryResponse, Box<dyn std::error::Error>> {
        query_client(self, state_root, height, path, data)
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
