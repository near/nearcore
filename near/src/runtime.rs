use std::collections::{BTreeSet, HashSet};
use std::convert::TryFrom;
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use kvdb::DBValue;
use log::{debug, error, info};

use near_chain::{
    BlockHeader, Error, ErrorKind, ReceiptResult, RuntimeAdapter, ValidTransaction, Weight,
};
use near_primitives::account::{AccessKey, Account};
use near_primitives::crypto::signature::{verify, PublicKey, Signature};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::Receipt;
use near_primitives::rpc::{AccountViewCallResult, QueryResponse, ViewStateResult};
use near_primitives::transaction::{SignedTransaction, TransactionLog};
use near_primitives::types::{AccountId, BlockIndex, MerkleHash, ShardId, ValidatorStake};
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
use crate::validator_manager::{ValidatorEpochConfig, ValidatorManager};

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
        let runtime = Runtime::new(genesis_config.runtime_config.clone(), ethash_provider.clone());
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
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        let validator = vm
            .get_block_proposer_info(header.epoch_hash, header.height)
            .map_err(|err| ErrorKind::Other(err.to_string()))?;
        if !header.verify_block_producer(&validator.public_key) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.total_weight.next(header.approval_sigs.len() as u64))
    }

    fn get_epoch_block_proposers(
        &self,
        epoch_hash: &CryptoHash,
        block_hash: &CryptoHash,
    ) -> Result<Vec<(AccountId, bool)>, Box<dyn std::error::Error>> {
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        let slashed = vm.get_slashed_validators(block_hash)?.clone();
        let validator_assignment = vm.get_validators(*epoch_hash)?;
        let mut included: HashSet<AccountId> = HashSet::default();
        let mut result = vec![];
        for index in validator_assignment.block_producers.iter() {
            let account_id = validator_assignment.validators[*index].account_id.clone();
            if !included.contains(&account_id) {
                let is_slashed = slashed.contains(&account_id);
                included.insert(account_id.clone());
                result.push((account_id, is_slashed));
            }
        }
        Ok(result)
    }

    fn get_block_proposer(
        &self,
        epoch_hash: &CryptoHash,
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        Ok(vm.get_block_proposer_info(*epoch_hash, height)?.account_id)
    }

    fn get_chunk_proposer(
        &self,
        shard_id: ShardId,
        parent_hash: CryptoHash,
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        let (epoch_hash, idx) = vm.get_epoch_offset(parent_hash, height)?;
        let validator_assignemnt = vm.get_validators(epoch_hash)?;
        let total_seats: u64 = validator_assignemnt.chunk_producers[shard_id as usize]
            .iter()
            .map(|(_, seats)| seats)
            .sum();
        let mut cur_seats = 0;
        for (index, seats) in validator_assignemnt.chunk_producers[shard_id as usize].iter() {
            if cur_seats + *seats > idx % total_seats {
                return Ok(validator_assignemnt.validators[*index].account_id.clone());
            }
            cur_seats += *seats;
        }
        unreachable!()
    }

    fn check_validator_signature(
        &self,
        epoch_hash: &CryptoHash,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> bool {
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        if let Ok(validators) = vm.get_validators(*epoch_hash) {
            if let Some(idx) = validators.validator_to_index.get(account_id) {
                let staking_key = &validators.validators[*idx].public_key;
                return verify(data, signature, staking_key);
            }
        }
        false
    }

    fn num_shards(&self) -> ShardId {
        // TODO: should be dynamic.
        self.genesis_config.block_producers_per_shard.len() as ShardId
    }

    fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        let mut cursor = Cursor::new((hash(&account_id.clone().into_bytes()).0).0);
        cursor.read_u64::<LittleEndian>().expect("Must not happened")
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

    fn add_validator_proposals(
        &self,
        parent_hash: CryptoHash,
        current_hash: CryptoHash,
        block_index: BlockIndex,
        proposals: Vec<ValidatorStake>,
        slashed_validators: Vec<AccountId>,
        validator_mask: Vec<bool>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Deal with validator proposals and epoch finishing.
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        // TODO: don't commit here, instead contribute to upstream store update.
        vm.add_proposals(
            parent_hash,
            current_hash,
            block_index,
            proposals,
            slashed_validators,
            validator_mask,
        )?
        .commit()
        .map_err(|err| err.into())
    }

    fn get_epoch_offset(
        &self,
        parent_hash: CryptoHash,
        block_index: BlockIndex,
    ) -> Result<(CryptoHash, BlockIndex), Box<dyn std::error::Error>> {
        let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
        Ok(vm.get_epoch_offset(parent_hash, block_index)?)
    }

    fn apply_transactions(
        &self,
        shard_id: ShardId,
        state_root: &MerkleHash,
        block_index: BlockIndex,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        receipts: &Vec<Vec<Receipt>>,
        transactions: &Vec<SignedTransaction>,
    ) -> Result<
        (WrappedTrieChanges, MerkleHash, Vec<TransactionLog>, ReceiptResult, Vec<ValidatorStake>),
        Box<dyn std::error::Error>,
    > {
        let mut state_update = TrieUpdate::new(self.trie.clone(), *state_root);
        {
            let mut vm = self.validator_manager.write().expect(POISONED_LOCK_ERR);
            let (epoch_hash, offset) = vm.get_epoch_offset(*prev_block_hash, block_index)?;
            if offset == 0 && epoch_hash != CryptoHash::default() {
                vm.finalize_epoch(&epoch_hash, prev_block_hash, block_hash)?;
                let prev_epoch_hash = vm.get_prev_epoch_hash(&epoch_hash)?;
                let prev_prev_stake_change =
                    vm.get_validators(prev_epoch_hash)?.stake_change.clone();
                let prev_stake_change = vm.get_validators(epoch_hash)?.stake_change.clone();
                let stake_change = &vm.get_validators(*block_hash)?.stake_change;
                let prev_keys: BTreeSet<_> = prev_stake_change.keys().collect();
                let keys: BTreeSet<_> = stake_change.keys().collect();

                for account_id in prev_keys.union(&keys) {
                    let account: Option<Account> = get_account(&state_update, account_id);
                    if let Some(mut account) = account {
                        let new_stake = *stake_change.get(*account_id).unwrap_or(&0);
                        let prev_stake = *prev_stake_change.get(*account_id).unwrap_or(&0);
                        let prev_prev_stake =
                            *prev_prev_stake_change.get(*account_id).unwrap_or(&0);
                        let max_of_stakes =
                            vec![prev_prev_stake, prev_stake, new_stake].into_iter().max().unwrap();
                        if account.staked < max_of_stakes {
                            error!("FATAL: staking invariance does not hold");
                        }
                        let return_stake = account.staked - max_of_stakes;
                        account.staked -= return_stake;
                        account.amount += return_stake;
                        set_account(&mut state_update, account_id, &account);
                    }
                }
            }
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
        path: &str,
        data: &[u8],
    ) -> Result<QueryResponse, Box<dyn std::error::Error>> {
        query_client(self, state_root, height, path, data)
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
        prefix: &[u8],
    ) -> Result<ViewStateResult, Box<dyn std::error::Error>> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.view_state(&state_update, account_id, prefix)
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use near_chain::RuntimeAdapter;
    use near_client::BlockProducer;
    use near_primitives::crypto::signer::{EDSigner, InMemorySigner};
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::receipt::Receipt;
    use near_primitives::rpc::AccountViewCallResult;
    use near_primitives::transaction::{
        Action, AddKeyAction, CreateAccountAction, SignedTransaction, StakeAction, TransferAction,
    };
    use near_primitives::types::{Balance, BlockIndex, Nonce, ValidatorStake};
    use near_store::create_store;
    use node_runtime::adapter::ViewRuntimeAdapter;

    use crate::config::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
    use crate::runtime::POISONED_LOCK_ERR;
    use crate::test_utils::*;

    use crate::{get_store_path, GenesisConfig, NightshadeRuntime};
    use near_primitives::account::AccessKey;

    fn stake(nonce: Nonce, sender: &BlockProducer, amount: Balance) -> SignedTransaction {
        SignedTransaction::from_actions(
            nonce,
            sender.account_id.clone(),
            sender.account_id.clone(),
            sender.signer.clone(),
            vec![Action::Stake(StakeAction {
                stake: amount,
                public_key: sender.signer.public_key(),
            })],
        )
    }

    impl NightshadeRuntime {
        fn update(
            &self,
            state_root: &CryptoHash,
            block_index: BlockIndex,
            prev_block_hash: &CryptoHash,
            block_hash: &CryptoHash,
            receipts: &Vec<Vec<Receipt>>,
            transactions: &Vec<SignedTransaction>,
        ) -> (CryptoHash, Vec<ValidatorStake>, Vec<Vec<Receipt>>) {
            let mut root = *state_root;
            let (wrapped_trie_changes, new_root, _tx_results, receipt_results, stakes) = self
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
            wrapped_trie_changes.insertions_into(&mut store_update).unwrap();
            store_update.commit().unwrap();
            root = new_root;
            let new_receipts = receipt_results.into_iter().map(|(_, v)| v).collect();
            (root, stakes, new_receipts)
        }
    }

    /// Start with 2 validators with default stake X.
    /// 1. Validator 0 stakes 2 * X
    /// 2. Validator 0 creates new account Validator 2 with 3 * X in balance
    /// 3. Validator 2 stakes 2 * X
    /// 4. Validator 1 doesn't produce blocks and gets kicked out
    /// 5. At the end Validator 0 and 2 with 2 * X are validators. Validator 1 has stake returned to balance.
    #[test]
    fn test_validator_rotation() {
        let dir = TempDir::new("validator_rotation").unwrap();
        let store = create_store(&get_store_path(dir.path()));
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut genesis_config =
            GenesisConfig::test(validators.iter().map(|v| v.as_str()).collect());
        genesis_config.epoch_length = 2;
        let nightshade = NightshadeRuntime::new(dir.path(), store, genesis_config);
        let (store_update, state_roots) = nightshade.genesis_state();
        store_update.commit().unwrap();
        let mut state_root = state_roots[0];
        let block_producers: Vec<_> =
            validators.iter().map(|id| InMemorySigner::from_seed(id, id).into()).collect();
        let (h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10) = (
            hash(&[0]),
            hash(&[1]),
            hash(&[2]),
            hash(&[3]),
            hash(&[4]),
            hash(&[5]),
            hash(&[6]),
            hash(&[7]),
            hash(&[8]),
            hash(&[9]),
            hash(&[10]),
        );

        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE * 2);
        // test1 stakes twice the current stake, because test1 and test2 have the same amount of stake before, test2 will be
        // kicked out.
        let (new_root, validator_stakes, _) = nightshade.update(
            &state_root,
            0,
            &CryptoHash::default(),
            &h0,
            &vec![],
            &vec![staking_transaction],
        );
        state_root = new_root;
        assert_eq!(
            validator_stakes,
            vec![ValidatorStake::new(
                block_producers[0].account_id.clone(),
                block_producers[0].signer.public_key(),
                TESTING_INIT_STAKE * 2
            )]
        );
        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE * 2,
                stake: TESTING_INIT_STAKE * 2,
                code_hash: account.code_hash,
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[0].account_id,
                    &block_producers[0].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            1
        );

        nightshade
            .add_validator_proposals(CryptoHash::default(), h0, 0, validator_stakes, vec![], vec![])
            .unwrap();

        let new_account = format!("test{}", num_nodes + 1);
        let new_validator: BlockProducer =
            InMemorySigner::from_seed(&new_account, &new_account).into();
        let create_account_transaction = SignedTransaction::from_actions(
            2,
            block_producers[0].account_id.clone(),
            new_account,
            block_producers[0].signer.clone(),
            vec![
                Action::CreateAccount(CreateAccountAction {}),
                Action::Transfer(TransferAction { deposit: TESTING_INIT_STAKE * 3 }),
                Action::AddKey(AddKeyAction {
                    public_key: new_validator.signer.public_key(),
                    access_key: AccessKey::full_access(),
                }),
            ],
        );
        let staking_transaction = stake(1, &new_validator, TESTING_INIT_STAKE * 2);

        let (new_root, _, receipts) =
            nightshade.update(&state_root, 1, &h0, &h1, &vec![], &vec![create_account_transaction]);
        state_root = new_root;
        nightshade.add_validator_proposals(h0, h1, 1, vec![], vec![], vec![]).unwrap();

        state_root = nightshade.update(&state_root, 2, &h1, &h2, &receipts, &vec![]).0;
        nightshade.add_validator_proposals(h1, h2, 2, vec![], vec![], vec![]).unwrap();
        // test3 stakes the same amount as test1 and will be confirmed as a validator in the next epoch
        let (new_root, validator_stakes, _) =
            nightshade.update(&state_root, 3, &h2, &h3, &vec![], &vec![staking_transaction]);
        state_root = new_root;
        assert_eq!(
            validator_stakes,
            vec![ValidatorStake::new(
                new_validator.account_id.clone(),
                new_validator.signer.public_key(),
                TESTING_INIT_STAKE * 2
            )]
        );
        nightshade.add_validator_proposals(h2, h3, 3, validator_stakes, vec![], vec![]).unwrap();
        nightshade.update(&state_root, 4, &h3, &h4, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h3, h4, 4, vec![], vec![], vec![]).unwrap();
        {
            let mut vm = nightshade.validator_manager.write().expect(POISONED_LOCK_ERR);
            let validators = vm.get_validators(h4).unwrap();
            // at the beginning of epoch 4, test2 will be kicked out and test3 will join
            assert_eq!(
                validators,
                &assignment(
                    vec![("test3", TESTING_INIT_STAKE * 2), ("test1", TESTING_INIT_STAKE * 2)],
                    vec![1, 0],
                    vec![vec![(1, 1), (0, 1)]],
                    vec![],
                    6,
                    change_stake(vec![
                        ("test1", TESTING_INIT_STAKE * 2),
                        ("test2", 0),
                        ("test3", TESTING_INIT_STAKE * 2)
                    ])
                )
            );
        }
        state_root = nightshade.update(&state_root, 4, &h3, &h4, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h3, h4, 4, vec![], vec![], vec![]).unwrap();
        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE * 5,
                stake: TESTING_INIT_STAKE * 2,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[0].account_id,
                    &block_producers[0].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            2
        );

        state_root = nightshade.update(&state_root, 5, &h4, &h5, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h4, h5, 5, vec![], vec![], vec![]).unwrap();
        state_root = nightshade.update(&state_root, 6, &h5, &h6, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h5, h6, 6, vec![], vec![], vec![]).unwrap();

        let account = nightshade.view_account(state_root, &block_producers[1].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[1].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
                stake: TESTING_INIT_STAKE,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[1].account_id,
                    &block_producers[1].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            0
        );

        let account = nightshade.view_account(state_root, &new_validator.account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: new_validator.account_id.clone(),
                amount: TESTING_INIT_STAKE,
                stake: TESTING_INIT_STAKE * 2,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &new_validator.account_id,
                    &new_validator.signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            1
        );

        state_root = nightshade.update(&state_root, 7, &h6, &h7, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h6, h7, 7, vec![], vec![], vec![]).unwrap();
        state_root = nightshade.update(&state_root, 8, &h7, &h8, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h7, h8, 8, vec![], vec![], vec![]).unwrap();

        let account = nightshade.view_account(state_root, &block_producers[1].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[1].account_id.clone(),
                amount: TESTING_INIT_BALANCE,
                stake: 0,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[1].account_id,
                    &block_producers[1].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            0
        );

        state_root = nightshade.update(&state_root, 9, &h8, &h9, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h8, h9, 9, vec![], vec![], vec![]).unwrap();
        state_root = nightshade.update(&state_root, 10, &h9, &h10, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h9, h10, 10, vec![], vec![], vec![]).unwrap();

        // make sure their is no double return of stake
        let account = nightshade.view_account(state_root, &block_producers[1].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[1].account_id.clone(),
                amount: TESTING_INIT_BALANCE,
                stake: 0,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[1].account_id,
                    &block_producers[1].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            0
        );
    }

    #[test]
    fn test_validator_stake_change() {
        let dir = TempDir::new("validator_stake_change").unwrap();
        let store = create_store(&get_store_path(dir.path()));
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut genesis_config =
            GenesisConfig::test(validators.iter().map(|v| v.as_str()).collect());
        genesis_config.epoch_length = 2;
        let nightshade = NightshadeRuntime::new(dir.path(), store, genesis_config);
        let (store_update, state_roots) = nightshade.genesis_state();
        store_update.commit().unwrap();
        let mut state_root = state_roots[0];
        let block_producers: Vec<_> =
            validators.iter().map(|id| InMemorySigner::from_seed(id, id).into()).collect();
        let (h0, h1, h2, h3, h4, h5, h6) =
            (hash(&[0]), hash(&[1]), hash(&[2]), hash(&[3]), hash(&[4]), hash(&[5]), hash(&[6]));
        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE - 1);
        let (new_root, validator_stakes, _) = nightshade.update(
            &state_root,
            0,
            &CryptoHash::default(),
            &h0,
            &vec![],
            &vec![staking_transaction],
        );
        state_root = new_root;
        assert_eq!(
            validator_stakes,
            vec![ValidatorStake::new(
                block_producers[0].account_id.clone(),
                block_producers[0].signer.public_key(),
                TESTING_INIT_STAKE - 1
            )]
        );
        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
                stake: TESTING_INIT_STAKE,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[0].account_id,
                    &block_producers[0].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            1
        );

        nightshade
            .add_validator_proposals(CryptoHash::default(), h0, 0, validator_stakes, vec![], vec![])
            .unwrap();

        state_root = nightshade.update(&state_root, 1, &h0, &h1, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h0, h1, 1, vec![], vec![], vec![]).unwrap();

        state_root = nightshade.update(&state_root, 2, &h1, &h2, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h1, h2, 2, vec![], vec![], vec![]).unwrap();

        state_root = nightshade.update(&state_root, 3, &h2, &h3, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h2, h3, 3, vec![], vec![], vec![]).unwrap();

        state_root = nightshade.update(&state_root, 4, &h3, &h4, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h3, h4, 4, vec![], vec![], vec![]).unwrap();

        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
                stake: TESTING_INIT_STAKE,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[0].account_id,
                    &block_producers[0].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            1
        );

        state_root = nightshade.update(&state_root, 5, &h4, &h5, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h4, h5, 5, vec![], vec![], vec![]).unwrap();

        state_root = nightshade.update(&state_root, 6, &h5, &h6, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h5, h6, 6, vec![], vec![], vec![]).unwrap();

        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1,
                stake: TESTING_INIT_STAKE - 1,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[0].account_id,
                    &block_producers[0].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            1
        );
    }

    #[test]
    fn test_validator_stake_change_multiple_times() {
        let dir = TempDir::new("validator_stake_change_multiple_times").unwrap();
        let store = create_store(&get_store_path(dir.path()));
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut genesis_config =
            GenesisConfig::test(validators.iter().map(|v| v.as_str()).collect());
        genesis_config.epoch_length = 2;
        let nightshade = NightshadeRuntime::new(dir.path(), store, genesis_config);
        let (store_update, state_roots) = nightshade.genesis_state();
        store_update.commit().unwrap();
        let mut state_root = state_roots[0];
        let block_producers: Vec<_> =
            validators.iter().map(|id| InMemorySigner::from_seed(id, id).into()).collect();
        let (h0, h1, h2, h3, h4, h5, h6, h7, h8) = (
            hash(&[0]),
            hash(&[1]),
            hash(&[2]),
            hash(&[3]),
            hash(&[4]),
            hash(&[5]),
            hash(&[6]),
            hash(&[7]),
            hash(&[8]),
        );
        let staking_transaction = stake(1, &block_producers[0], TESTING_INIT_STAKE - 1);
        let staking_transaction1 = stake(2, &block_producers[0], TESTING_INIT_STAKE - 2);
        let staking_transaction2 = stake(1, &block_producers[1], TESTING_INIT_STAKE + 1);
        let (new_root, validator_stakes, _) = nightshade.update(
            &state_root,
            0,
            &CryptoHash::default(),
            &h0,
            &vec![],
            &vec![staking_transaction, staking_transaction1, staking_transaction2],
        );
        state_root = new_root;
        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
                stake: TESTING_INIT_STAKE,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[0].account_id,
                    &block_producers[0].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            2
        );
        nightshade
            .add_validator_proposals(CryptoHash::default(), h0, 0, validator_stakes, vec![], vec![])
            .unwrap();

        state_root = nightshade.update(&state_root, 1, &h0, &h1, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h0, h1, 1, vec![], vec![], vec![]).unwrap();

        let staking_transaction = stake(3, &block_producers[0], TESTING_INIT_STAKE + 1);
        let staking_transaction1 = stake(2, &block_producers[1], TESTING_INIT_STAKE + 2);
        let staking_transaction2 = stake(3, &block_producers[1], TESTING_INIT_STAKE - 1);

        let (new_root, validator_stakes, _) = nightshade.update(
            &state_root,
            2,
            &h1,
            &h2,
            &vec![],
            &vec![staking_transaction, staking_transaction1, staking_transaction2],
        );
        state_root = new_root;
        nightshade.add_validator_proposals(h1, h2, 2, validator_stakes, vec![], vec![]).unwrap();

        state_root = nightshade.update(&state_root, 3, &h2, &h3, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h2, h3, 3, vec![], vec![], vec![]).unwrap();

        state_root = nightshade.update(&state_root, 4, &h3, &h4, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h3, h4, 4, vec![], vec![], vec![]).unwrap();

        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1,
                stake: TESTING_INIT_STAKE + 1,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[0].account_id,
                    &block_producers[0].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            3
        );

        let account = nightshade.view_account(state_root, &block_producers[1].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[1].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1,
                stake: TESTING_INIT_STAKE + 1,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[1].account_id,
                    &block_producers[1].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            3
        );

        state_root = nightshade.update(&state_root, 5, &h4, &h5, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h4, h5, 5, vec![], vec![], vec![]).unwrap();

        state_root = nightshade.update(&state_root, 6, &h5, &h6, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h5, h6, 6, vec![], vec![], vec![]).unwrap();

        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1,
                stake: TESTING_INIT_STAKE + 1,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[0].account_id,
                    &block_producers[0].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            3
        );

        let account = nightshade.view_account(state_root, &block_producers[1].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[1].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1,
                stake: TESTING_INIT_STAKE + 1,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[1].account_id,
                    &block_producers[1].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            3
        );

        state_root = nightshade.update(&state_root, 7, &h6, &h7, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h6, h7, 7, vec![], vec![], vec![]).unwrap();

        state_root = nightshade.update(&state_root, 8, &h7, &h8, &vec![], &vec![]).0;
        nightshade.add_validator_proposals(h7, h8, 8, vec![], vec![], vec![]).unwrap();

        let account = nightshade.view_account(state_root, &block_producers[0].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[0].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1,
                stake: TESTING_INIT_STAKE + 1,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[0].account_id,
                    &block_producers[0].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            3
        );

        let account = nightshade.view_account(state_root, &block_producers[1].account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                account_id: block_producers[1].account_id.clone(),
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1,
                stake: TESTING_INIT_STAKE - 1,
                code_hash: account.code_hash
            }
        );
        assert_eq!(
            nightshade
                .view_access_key(
                    state_root,
                    &block_producers[1].account_id,
                    &block_producers[1].signer.public_key()
                )
                .unwrap()
                .unwrap()
                .nonce,
            3
        );
    }

    #[test]
    fn test_check_validator_signature() {
        let dir = TempDir::new("check_validator_signature").unwrap();
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
        assert!(nightshade.check_validator_signature(
            &CryptoHash::default(),
            &validators[0],
            &data,
            &signature
        ));
    }

    #[test]
    fn test_check_validator_signature_failure() {
        let dir = TempDir::new("check_validator_signature_failure").unwrap();
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
        assert!(!nightshade.check_validator_signature(
            &CryptoHash::default(),
            &validators[1],
            &data,
            &signature
        ));
    }
}
