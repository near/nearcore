use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::{Arc, RwLock};

use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use log::debug;

use near_crypto::{InMemorySigner, KeyType, PublicKey, SecretKey, Signature};
use near_primitives::account::Account;
use near_primitives::challenge::ChallengesResult;
use near_primitives::errors::RuntimeError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{merklize, verify_path, MerklePath};
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum};
use near_primitives::serialize::to_base;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::transaction::{
    Action, ExecutionOutcome, ExecutionOutcomeWithId, ExecutionStatus, SignedTransaction,
    TransferAction,
};
use near_primitives::types::{
    AccountId, Balance, BlockIndex, EpochId, MerkleHash, Nonce, ShardId, StateRoot, ValidatorStake,
};
use near_primitives::views::QueryResponse;
use near_store::test_utils::create_test_store;
use near_store::{
    PartialStorage, Store, StoreUpdate, Trie, TrieChanges, WrappedTrieChanges, COL_BLOCK_HEADER,
};

use crate::error::{Error, ErrorKind};
use crate::store::ChainStoreAccess;
use crate::types::{
    ApplyTransactionResult, BlockHeader, RuntimeAdapter, StatePart, StatePartKey,
    ValidatorSignatureVerificationResult, Weight,
};
use crate::{Chain, ChainGenesis, ValidTransaction};

pub const DEFAULT_STATE_NUM_PARTS: u64 = 17; /* TODO MOO */

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Ord, PartialOrd, Clone, Debug)]
struct AccountNonce(AccountId, Nonce);

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug)]
struct KVState {
    amounts: HashMap<AccountId, u128>,
    receipt_nonces: HashSet<CryptoHash>,
    tx_nonces: HashSet<AccountNonce>,
}

/// Simple key value runtime for tests.
pub struct KeyValueRuntime {
    store: Arc<Store>,
    trie: Arc<Trie>,
    root: StateRoot,
    validators: Vec<Vec<ValidatorStake>>,
    validator_groups: u64,
    num_shards: ShardId,
    epoch_length: u64,

    // A mapping state_root => {account id => amounts}, for transactions and receipts
    state: RwLock<HashMap<MerkleHash, KVState>>,
    state_parts: RwLock<HashMap<CryptoHash, StatePart>>,
    state_proofs: RwLock<HashMap<CryptoHash, MerklePath>>,

    headers_cache: RwLock<HashMap<CryptoHash, BlockHeader>>,
    hash_to_epoch: RwLock<HashMap<CryptoHash, EpochId>>,
    hash_to_next_epoch: RwLock<HashMap<CryptoHash, EpochId>>,
    hash_to_valset: RwLock<HashMap<EpochId, u64>>,
    epoch_start: RwLock<HashMap<CryptoHash, u64>>,
}

pub fn account_id_to_shard_id(account_id: &AccountId, num_shards: ShardId) -> ShardId {
    u64::from((hash(&account_id.clone().into_bytes()).0).0[0]) % num_shards
}

#[derive(BorshSerialize, BorshDeserialize)]
struct ReceiptNonce {
    from: String,
    to: String,
    amount: Balance,
    nonce: Nonce,
}

fn create_receipt_nonce(from: String, to: String, amount: Balance, nonce: Nonce) -> CryptoHash {
    hash(&ReceiptNonce { from, to, amount, nonce }.try_to_vec().unwrap())
}

impl KeyValueRuntime {
    pub fn new(store: Arc<Store>) -> Self {
        Self::new_with_validators(store, vec![vec!["test".to_string()]], 1, 1, 5)
    }

    pub fn new_with_validators(
        store: Arc<Store>,
        validators: Vec<Vec<AccountId>>,
        validator_groups: u64,
        num_shards: ShardId,
        epoch_length: u64,
    ) -> Self {
        let trie = Arc::new(Trie::new(store.clone()));
        let mut initial_amounts = HashMap::new();
        for (i, validator) in validators.iter().flatten().enumerate() {
            initial_amounts.insert(validator.clone(), (1000 + 100 * i) as u128);
        }

        let mut map_with_default_hash1 = HashMap::new();
        map_with_default_hash1.insert(CryptoHash::default(), EpochId::default());
        let mut map_with_default_hash2 = HashMap::new();
        map_with_default_hash2.insert(CryptoHash::default(), 0);
        let mut map_with_default_hash3 = HashMap::new();
        map_with_default_hash3.insert(EpochId::default(), 0);

        let mut state = HashMap::new();
        state.insert(
            MerkleHash::default(),
            KVState {
                amounts: initial_amounts,
                receipt_nonces: HashSet::default(),
                tx_nonces: HashSet::default(),
            },
        );
        // TODO MOO initializing for StateRoot::default()?
        let state_parts = HashMap::new();
        let state_proofs = HashMap::new();
        KeyValueRuntime {
            store,
            trie,
            root: StateRoot::default(),
            validators: validators
                .iter()
                .map(|account_ids| {
                    account_ids
                        .iter()
                        .map(|account_id| ValidatorStake {
                            account_id: account_id.clone(),
                            public_key: SecretKey::from_seed(KeyType::ED25519, account_id)
                                .public_key(),
                            amount: 1_000_000,
                        })
                        .collect()
                })
                .collect(),
            validator_groups,
            num_shards,
            epoch_length,
            state: RwLock::new(state),
            state_parts: RwLock::new(state_parts),
            state_proofs: RwLock::new(state_proofs),
            headers_cache: RwLock::new(HashMap::new()),
            hash_to_epoch: RwLock::new(HashMap::new()),
            hash_to_next_epoch: RwLock::new(map_with_default_hash1),
            hash_to_valset: RwLock::new(map_with_default_hash3.clone()),
            epoch_start: RwLock::new(map_with_default_hash2.clone()),
        }
    }

    pub fn get_root(&self) -> CryptoHash {
        self.root.hash
    }

    fn get_block_header(&self, hash: &CryptoHash) -> Result<Option<BlockHeader>, Error> {
        let mut headers_cache = self.headers_cache.write().unwrap();
        if headers_cache.get(hash).is_some() {
            return Ok(Some(headers_cache.get(hash).unwrap().clone()));
        }
        if let Some(result) = self.store.get_ser(COL_BLOCK_HEADER, hash.as_ref())? {
            headers_cache.insert(hash.clone(), result);
            return Ok(Some(headers_cache.get(hash).unwrap().clone()));
        }
        Ok(None)
    }

    fn get_prev_height(
        &self,
        prev_hash: &CryptoHash,
    ) -> Result<BlockIndex, Box<dyn std::error::Error>> {
        if prev_hash == &CryptoHash::default() {
            return Ok(0);
        }
        let prev_block_header = self
            .get_block_header(prev_hash)?
            .ok_or_else(|| format!("Missing block {} when computing the epoch", prev_hash))?;
        Ok(prev_block_header.inner.height)
    }

    fn get_epoch_and_valset(
        &self,
        prev_hash: CryptoHash,
    ) -> Result<(EpochId, usize, EpochId), Error> {
        if prev_hash == CryptoHash::default() {
            return Ok((EpochId(prev_hash), 0, EpochId(prev_hash)));
        }
        let prev_block_header = self.get_block_header(&prev_hash)?.ok_or_else(|| {
            ErrorKind::Other(format!("Missing block {} when computing the epoch", prev_hash))
        })?;

        let mut hash_to_epoch = self.hash_to_epoch.write().unwrap();
        let mut hash_to_next_epoch = self.hash_to_next_epoch.write().unwrap();
        let mut hash_to_valset = self.hash_to_valset.write().unwrap();
        let mut epoch_start_map = self.epoch_start.write().unwrap();

        let prev_prev_hash = prev_block_header.inner.prev_hash;
        let prev_epoch = hash_to_epoch.get(&prev_prev_hash);
        let prev_next_epoch = hash_to_next_epoch.get(&prev_prev_hash).unwrap();
        let prev_valset = match prev_epoch {
            Some(prev_epoch) => Some(*hash_to_valset.get(&prev_epoch).unwrap()),
            None => None,
        };

        let prev_epoch_start = *epoch_start_map.get(&prev_prev_hash).unwrap();

        let increment_epoch = prev_prev_hash == CryptoHash::default() // genesis is in its own epoch
            || prev_block_header.inner.height - prev_epoch_start >= self.epoch_length;

        let (epoch, next_epoch, valset, epoch_start) = if increment_epoch {
            let new_valset = match prev_valset {
                None => 0,
                Some(prev_valset) => prev_valset + 1,
            };
            (
                prev_next_epoch.clone(),
                EpochId(prev_hash),
                new_valset,
                prev_block_header.inner.height,
            )
        } else {
            (
                prev_epoch.unwrap().clone(),
                prev_next_epoch.clone(),
                prev_valset.unwrap(),
                prev_epoch_start,
            )
        };

        hash_to_next_epoch.insert(prev_hash, next_epoch.clone());
        hash_to_epoch.insert(prev_hash, epoch.clone());
        hash_to_valset.insert(epoch.clone(), valset);
        hash_to_valset.insert(next_epoch.clone(), valset + 1);
        epoch_start_map.insert(prev_hash, epoch_start);

        Ok((epoch, valset as usize % self.validators.len(), next_epoch.clone()))
    }

    fn get_valset_for_epoch(&self, epoch_id: &EpochId) -> Result<usize, Error> {
        // conveniently here if the prev_hash is passed mistakenly instead of the epoch_hash,
        // the `unwrap` will trigger
        Ok(*self
            .hash_to_valset
            .read()
            .unwrap()
            .get(epoch_id)
            .ok_or_else(|| Error::from(ErrorKind::EpochOutOfBounds))? as usize
            % self.validators.len())
    }
}

impl RuntimeAdapter for KeyValueRuntime {
    fn genesis_state(&self) -> (StoreUpdate, Vec<StateRoot>) {
        (
            self.store.store_update(),
            ((0..self.num_shards())
                .map(|_| StateRoot {
                    hash: CryptoHash::default(),
                    num_parts: DEFAULT_STATE_NUM_PARTS, /* TODO MOO */
                })
                .collect()),
        )
    }

    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error> {
        let validators = &self.validators
            [self.get_epoch_and_valset(header.inner.prev_hash).map_err(|err| err.to_string())?.1];
        let validator = &validators[(header.inner.height as usize) % validators.len()];
        if !header.verify_block_producer(&validator.public_key) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.inner.total_weight.next(header.num_approvals() as u128))
    }

    fn verify_validator_signature(
        &self,
        _epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        _account_id: &AccountId,
        _data: &[u8],
        _signature: &Signature,
    ) -> ValidatorSignatureVerificationResult {
        ValidatorSignatureVerificationResult::Valid
    }

    fn verify_header_signature(
        &self,
        _header: &BlockHeader,
    ) -> ValidatorSignatureVerificationResult {
        ValidatorSignatureVerificationResult::Valid
    }

    fn verify_chunk_header_signature(&self, _header: &ShardChunkHeader) -> Result<bool, Error> {
        Ok(true)
    }

    fn verify_approval_signature(
        &self,
        _epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        _approval_mask: &[bool],
        _approval_sigs: &[Signature],
        _data: &[u8],
    ) -> Result<bool, Error> {
        Ok(true)
    }

    fn get_epoch_block_producers(
        &self,
        epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(AccountId, bool)>, Error> {
        let validators = &self.validators[self.get_valset_for_epoch(epoch_id)?];
        Ok(validators.iter().map(|x| (x.account_id.clone(), false)).collect())
    }

    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockIndex,
    ) -> Result<AccountId, Error> {
        let validators = &self.validators[self.get_valset_for_epoch(epoch_id)?];
        Ok(validators[(height as usize) % validators.len()].account_id.clone())
    }

    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockIndex,
        shard_id: ShardId,
    ) -> Result<AccountId, Error> {
        let validators = &self.validators[self.get_valset_for_epoch(epoch_id)?];
        assert_eq!((validators.len() as u64) % self.num_shards(), 0);
        assert_eq!(0, validators.len() as u64 % self.validator_groups);
        let validators_per_shard = validators.len() as ShardId / self.validator_groups;
        let coef = validators.len() as ShardId / self.num_shards();
        let offset = (shard_id * coef / validators_per_shard * validators_per_shard) as usize;
        let delta = ((shard_id + height + 1) % validators_per_shard) as usize;
        Ok(validators[offset + delta].account_id.clone())
    }

    fn num_shards(&self) -> ShardId {
        self.num_shards
    }

    fn num_total_parts(&self, parent_hash: &CryptoHash) -> usize {
        let height = self.get_prev_height(parent_hash).unwrap();
        1 + self.num_data_parts(parent_hash) * (2 + (height as usize) % 2)
    }

    fn num_data_parts(&self, parent_hash: &CryptoHash) -> usize {
        let height = self.get_prev_height(parent_hash).unwrap();
        // Test changing number of data parts
        12 + 2 * ((height as usize) % 4)
    }

    fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        account_id_to_shard_id(account_id, self.num_shards())
    }

    fn get_part_owner(&self, parent_hash: &CryptoHash, part_id: u64) -> Result<String, Error> {
        let validators = &self.validators[self.get_epoch_and_valset(*parent_hash)?.1];
        // if we don't use data_parts and total_parts as part of the formula here, the part owner
        //     would not depend on height, and tests wouldn't catch passing wrong height here
        let idx =
            part_id as usize + self.num_data_parts(parent_hash) + self.num_total_parts(parent_hash);
        Ok(validators[idx as usize % validators.len()].account_id.clone())
    }

    fn cares_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        _is_me: bool,
    ) -> bool {
        // This `unwrap` here tests that in all code paths we check that the epoch exists before
        //    we check if we care about a shard. Please do not remove the unwrap, fix the logic of
        //    the calling function.
        let epoch_valset = self.get_epoch_and_valset(*parent_hash).unwrap();
        let validators = &self.validators[epoch_valset.1];
        assert_eq!((validators.len() as u64) % self.num_shards(), 0);
        assert_eq!(0, validators.len() as u64 % self.validator_groups);
        let validators_per_shard = validators.len() as ShardId / self.validator_groups;
        let coef = validators.len() as ShardId / self.num_shards();
        let offset = (shard_id * coef / validators_per_shard * validators_per_shard) as usize;
        assert!(offset + validators_per_shard as usize <= validators.len());
        if let Some(account_id) = account_id {
            for validator in validators[offset..offset + (validators_per_shard as usize)].iter() {
                if validator.account_id == *account_id {
                    return true;
                }
            }
        }
        false
    }

    fn will_care_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        _is_me: bool,
    ) -> bool {
        // This `unwrap` here tests that in all code paths we check that the epoch exists before
        //    we check if we care about a shard. Please do not remove the unwrap, fix the logic of
        //    the calling function.
        let epoch_valset = self.get_epoch_and_valset(*parent_hash).unwrap();
        let validators = &self.validators[(epoch_valset.1 + 1) % self.validators.len()];
        assert_eq!((validators.len() as u64) % self.num_shards(), 0);
        assert_eq!(0, validators.len() as u64 % self.validator_groups);
        let validators_per_shard = validators.len() as ShardId / self.validator_groups;
        let coef = validators.len() as ShardId / self.num_shards();
        let offset = (shard_id * coef / validators_per_shard * validators_per_shard) as usize;
        if let Some(account_id) = account_id {
            for validator in validators[offset..offset + (validators_per_shard as usize)].iter() {
                if validator.account_id == *account_id {
                    return true;
                }
            }
        }
        false
    }

    fn filter_transactions(
        &self,
        _block_index: u64,
        _block_timestamp: u64,
        _gas_price: u128,
        _state_root: StateRoot,
        transactions: Vec<SignedTransaction>,
    ) -> Vec<SignedTransaction> {
        transactions
    }

    fn validate_tx(
        &self,
        _block_index: BlockIndex,
        _block_timestamp: u64,
        _gas_price: Balance,
        _state_root: StateRoot,
        transaction: SignedTransaction,
    ) -> Result<ValidTransaction, RuntimeError> {
        Ok(ValidTransaction { transaction })
    }

    fn add_validator_proposals(
        &self,
        _parent_hash: CryptoHash,
        _current_hash: CryptoHash,
        _block_index: u64,
        _proposals: Vec<ValidatorStake>,
        _slashed_validators: Vec<AccountId>,
        _validator_mask: Vec<bool>,
        _rent_paid: Balance,
        _validator_reward: Balance,
        _total_supply: Balance,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn apply_transactions_with_optional_storage_proof(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        _block_index: BlockIndex,
        _block_timestamp: u64,
        _prev_block_hash: &CryptoHash,
        _block_hash: &CryptoHash,
        receipts: &[Receipt],
        transactions: &[SignedTransaction],
        _last_validator_proposals: &[ValidatorStake],
        gas_price: Balance,
        _challenges: &ChallengesResult,
        generate_storage_proof: bool,
    ) -> Result<ApplyTransactionResult, Error> {
        assert!(!generate_storage_proof);
        let mut tx_results = vec![];

        let mut state = self.state.read().unwrap().get(&state_root.hash).cloned().unwrap();

        let mut balance_transfers = vec![];

        for receipt in receipts.iter() {
            if let ReceiptEnum::Action(action) = &receipt.receipt {
                assert_eq!(self.account_id_to_shard_id(&receipt.receiver_id), shard_id);
                if !state.receipt_nonces.contains(&receipt.receipt_id) {
                    state.receipt_nonces.insert(receipt.receipt_id);
                    if let Action::Transfer(TransferAction { deposit }) = action.actions[0] {
                        balance_transfers.push((
                            receipt.get_hash(),
                            receipt.predecessor_id.clone(),
                            receipt.receiver_id.clone(),
                            deposit,
                            0,
                        ));
                    }
                } else {
                    panic!("receipts should never be applied twice");
                }
            } else {
                unreachable!();
            }
        }

        for transaction in transactions {
            assert_eq!(self.account_id_to_shard_id(&transaction.transaction.signer_id), shard_id);
            if transaction.transaction.actions.is_empty() {
                continue;
            }
            if let Action::Transfer(TransferAction { deposit }) = transaction.transaction.actions[0]
            {
                if !state.tx_nonces.contains(&AccountNonce(
                    transaction.transaction.receiver_id.clone(),
                    transaction.transaction.nonce,
                )) {
                    state.tx_nonces.insert(AccountNonce(
                        transaction.transaction.receiver_id.clone(),
                        transaction.transaction.nonce,
                    ));
                    balance_transfers.push((
                        transaction.get_hash(),
                        transaction.transaction.signer_id.clone(),
                        transaction.transaction.receiver_id.clone(),
                        deposit,
                        transaction.transaction.nonce,
                    ));
                } else {
                    balance_transfers.push((
                        transaction.get_hash(),
                        transaction.transaction.signer_id.clone(),
                        transaction.transaction.receiver_id.clone(),
                        0,
                        transaction.transaction.nonce,
                    ));
                }
            } else {
                unreachable!();
            }
        }

        let mut new_receipts = HashMap::new();

        for (hash, from, to, amount, nonce) in balance_transfers {
            let mut good_to_go = false;

            if self.account_id_to_shard_id(&from) != shard_id {
                // This is a receipt, was already debited
                good_to_go = true;
            } else if let Some(balance) = state.amounts.get(&from) {
                if *balance >= amount {
                    let new_balance = balance - amount;
                    state.amounts.insert(from.clone(), new_balance);
                    good_to_go = true;
                }
            }

            if good_to_go {
                let new_receipt_hashes = if self.account_id_to_shard_id(&to) == shard_id {
                    state.amounts.insert(to.clone(), state.amounts.get(&to).unwrap_or(&0) + amount);
                    vec![]
                } else {
                    assert_ne!(nonce, 0);
                    let receipt = Receipt {
                        predecessor_id: from.clone(),
                        receiver_id: to.clone(),
                        receipt_id: create_receipt_nonce(from.clone(), to.clone(), amount, nonce),
                        receipt: ReceiptEnum::Action(ActionReceipt {
                            signer_id: from.clone(),
                            signer_public_key: PublicKey::empty(KeyType::ED25519),
                            gas_price,
                            output_data_receivers: vec![],
                            input_data_ids: vec![],
                            actions: vec![Action::Transfer(TransferAction { deposit: amount })],
                        }),
                    };
                    let receipt_hash = receipt.get_hash();
                    new_receipts
                        .entry(self.account_id_to_shard_id(&receipt.receiver_id))
                        .or_insert_with(|| vec![])
                        .push(receipt);
                    vec![receipt_hash]
                };

                tx_results.push(ExecutionOutcomeWithId {
                    id: hash,
                    outcome: ExecutionOutcome {
                        status: ExecutionStatus::SuccessValue(vec![]),
                        logs: vec![],
                        receipt_ids: new_receipt_hashes,
                        gas_burnt: 0,
                    },
                });
            }
        }

        let mut new_balances = vec![];
        for validator in self.validators.iter().flatten() {
            let mut seen = false;
            for (key, value) in state.amounts.iter() {
                if key == &validator.account_id {
                    assert!(!seen);
                    seen = true;
                    new_balances.push(*value);
                }
            }
            if !seen {
                new_balances.push(0);
            }
        }

        let data = state.try_to_vec()?;
        let state_num_parts = DEFAULT_STATE_NUM_PARTS as usize;
        let mut parts = vec![];
        for i in 0..state_num_parts {
            let begin = data.len() / state_num_parts * i;
            let mut end = data.len() / state_num_parts * (i + 1);
            if i + 1 == state_num_parts {
                end = data.len();
            }
            let part = StatePart { shard_id, part_id: i as u64, data: data[begin..end].to_vec() };
            parts.push(part);
        }
        let (state_hash, proofs) = merklize(&parts);
        let new_state_root = StateRoot { hash: state_hash, num_parts: state_num_parts as u64 };

        self.state.write().unwrap().insert(new_state_root.hash, state);
        for i in 0..state_num_parts {
            let key = hash(&StatePartKey(i as u64, new_state_root.clone()).try_to_vec().unwrap());
            assert!(verify_path(new_state_root.hash, &proofs[i], &parts[i]));
            self.state_parts.write().unwrap().insert(key, parts[i].clone());
            self.state_proofs.write().unwrap().insert(key, proofs[i].clone());
        }

        Ok(ApplyTransactionResult {
            trie_changes: WrappedTrieChanges::new(
                self.trie.clone(),
                TrieChanges::empty(state_root.hash),
            ),
            new_root: new_state_root,
            transaction_results: tx_results,
            receipt_result: new_receipts,
            validator_proposals: vec![],
            total_gas_burnt: 0,
            total_rent_paid: 0,
            total_validator_reward: 0,
            total_balance_burnt: 0,
            proof: None,
        })
    }

    fn check_state_transition(
        &self,
        _partial_storage: PartialStorage,
        _shard_id: ShardId,
        _state_root: &StateRoot,
        _block_index: BlockIndex,
        _block_timestamp: u64,
        _prev_block_hash: &CryptoHash,
        _block_hash: &CryptoHash,
        _receipts: &[Receipt],
        _transactions: &[SignedTransaction],
        _last_validator_proposals: &[ValidatorStake],
        _gas_price: Balance,
        _challenges: &ChallengesResult,
    ) -> Result<ApplyTransactionResult, Error> {
        unimplemented!();
    }

    fn query(
        &self,
        state_root: &StateRoot,
        _height: BlockIndex,
        _block_timestamp: u64,
        _block_hash: &CryptoHash,
        path: Vec<&str>,
        _data: &[u8],
    ) -> Result<QueryResponse, Box<dyn std::error::Error>> {
        let account_id = path[1].to_string();
        let account_id2 = account_id.clone();
        Ok(QueryResponse::ViewAccount(
            Account {
                amount: self
                    .state
                    .read()
                    .unwrap()
                    .get(&state_root.hash)
                    .map_or_else(|| 0, |state| *state.amounts.get(&account_id2).unwrap_or(&0)),
                locked: 0,
                code_hash: CryptoHash::default(),
                storage_usage: 0,
                storage_paid_at: 0,
            }
            .into(),
        ))
    }

    fn obtain_state_part(
        &self,
        shard_id: ShardId,
        part_id: u64,
        state_root: &StateRoot,
    ) -> Result<(StatePart, Vec<u8>), Box<dyn std::error::Error>> {
        if part_id >= state_root.num_parts {
            return Err("Invalid part_id in obtain_state_part".to_string().into());
        }
        if shard_id >= self.num_shards() {
            return Err("Invalid shard_id in obtain_state_part".to_string().into());
        }
        let key = hash(&StatePartKey(part_id, state_root.clone()).try_to_vec().unwrap());
        let part = self.state_parts.read().unwrap().get(&key).unwrap().clone();
        let proof = self.state_proofs.read().unwrap().get(&key).unwrap().clone();
        assert!(verify_path(state_root.hash, &proof, &part));
        Ok((part, proof.try_to_vec()?))
    }

    fn accept_state_part(
        &self,
        state_root: &StateRoot,
        part: &StatePart,
        proof: &Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let merkle_proof = MerklePath::try_from_slice(&proof)?;
        if !verify_path(state_root.hash, &merkle_proof, part) {
            return Err("set_shard_state failed: invalid StatePart".into());
        }
        let key = hash(&StatePartKey(part.part_id, state_root.clone()).try_to_vec().unwrap());
        self.state_parts.write().unwrap().insert(key, part.clone());
        self.state_proofs.write().unwrap().insert(key, merkle_proof);
        Ok(())
    }

    fn confirm_state(&self, state_root: &StateRoot) -> Result<bool, Error> {
        let mut data = vec![];
        for i in 0..state_root.num_parts as usize {
            let key = hash(&StatePartKey(i as u64, state_root.clone()).try_to_vec().unwrap());
            match self.state_parts.read().unwrap().get(&key) {
                Some(part) => {
                    data.push(part.data.clone());
                }
                None => {
                    return Err(format!("Invalid accept_state, no part {:?}", i)
                        .to_string()
                        .into());
                }
            }
        }
        let data_flatten: Vec<u8> = data.iter().flatten().cloned().collect();
        let state = KVState::try_from_slice(&data_flatten).unwrap();
        self.state.write().unwrap().insert(state_root.hash, state);
        Ok(true)
    }

    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, Error> {
        if parent_hash == &CryptoHash::default() {
            return Ok(true);
        }
        let prev_block_header = self.get_block_header(parent_hash)?.ok_or_else(|| {
            Error::from(ErrorKind::Other(format!(
                "Missing block {} when computing the epoch",
                parent_hash
            )))
        })?;
        let prev_prev_hash = prev_block_header.inner.prev_hash;
        Ok(self.get_epoch_and_valset(*parent_hash)?.0
            != self.get_epoch_and_valset(prev_prev_hash)?.0)
    }

    fn get_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        Ok(self.get_epoch_and_valset(*parent_hash)?.0)
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        Ok(self.get_epoch_and_valset(*parent_hash)?.2)
    }

    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockIndex, Error> {
        let epoch_id = self.get_epoch_and_valset(*block_hash)?.0;
        match self.get_block_header(&epoch_id.0)? {
            Some(block_header) => Ok(block_header.inner.height),
            None => Ok(0),
        }
    }

    fn get_epoch_inflation(&self, _epoch_id: &EpochId) -> Result<u128, Error> {
        Ok(0)
    }
}

pub fn setup() -> (Chain, Arc<KeyValueRuntime>, Arc<InMemorySigner>) {
    setup_with_tx_validity_period(100)
}

pub fn setup_with_tx_validity_period(
    validity: BlockIndex,
) -> (Chain, Arc<KeyValueRuntime>, Arc<InMemorySigner>) {
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new(store.clone()));
    let chain = Chain::new(
        store,
        runtime.clone(),
        &ChainGenesis::new(
            "unittest".to_string(),
            Utc::now(),
            1_000_000,
            100,
            1_000_000_000,
            0,
            0,
            validity,
        ),
    )
    .unwrap();
    let signer = Arc::new(InMemorySigner::from_seed("test", KeyType::ED25519, "test"));
    (chain, runtime, signer)
}

pub fn format_hash(h: CryptoHash) -> String {
    to_base(&h)[..6].to_string()
}

/// Displays chain from given store.
pub fn display_chain(me: &Option<AccountId>, chain: &mut Chain, tail: bool) {
    let runtime_adapter = chain.runtime_adapter();
    let chain_store = chain.mut_store();
    let head = chain_store.head().unwrap();
    debug!(
        "{:?} Chain head ({}): {} / {}",
        me,
        if tail { "tail" } else { "full" },
        head.height,
        head.last_block_hash
    );
    let mut headers = vec![];
    for (key, _) in chain_store.store().iter(COL_BLOCK_HEADER) {
        let header = chain_store
            .get_block_header(&CryptoHash::try_from(key.as_ref()).unwrap())
            .unwrap()
            .clone();
        if !tail || header.inner.height + 10 > head.height {
            headers.push(header);
        }
    }
    headers.sort_by(|h_left, h_right| {
        if h_left.inner.height > h_right.inner.height {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    });
    for header in headers {
        if header.inner.prev_hash == CryptoHash::default() {
            // Genesis block.
            debug!("{: >3} {}", header.inner.height, format_hash(header.hash()));
        } else {
            let parent_header =
                chain_store.get_block_header(&header.inner.prev_hash).unwrap().clone();
            let maybe_block = chain_store.get_block(&header.hash()).ok().cloned();
            let epoch_id =
                runtime_adapter.get_epoch_id_from_prev_block(&header.inner.prev_hash).unwrap();
            let block_producer =
                runtime_adapter.get_block_producer(&epoch_id, header.inner.height).unwrap();
            debug!(
                "{: >3} {} | {: >10} | parent: {: >3} {} | {}",
                header.inner.height,
                format_hash(header.hash()),
                block_producer,
                parent_header.inner.height,
                format_hash(parent_header.hash()),
                if let Some(block) = &maybe_block {
                    format!("chunks: {}", block.chunks.len())
                } else {
                    "-".to_string()
                }
            );
            if let Some(block) = maybe_block {
                for chunk_header in block.chunks.iter() {
                    let chunk_producer = runtime_adapter
                        .get_chunk_producer(
                            &epoch_id,
                            chunk_header.inner.height_created,
                            chunk_header.inner.shard_id,
                        )
                        .unwrap();
                    if let Ok(chunk) = chain_store.get_chunk(&chunk_header.chunk_hash()) {
                        debug!(
                            "    {: >3} {} | {} | {: >10} | tx = {: >2}, receipts = {: >2}",
                            chunk_header.inner.height_created,
                            format_hash(chunk_header.chunk_hash().0),
                            chunk_header.inner.shard_id,
                            chunk_producer,
                            chunk.transactions.len(),
                            chunk.receipts.len()
                        );
                    } else if let Ok(chunk_one_part) = chain_store.get_chunk_one_part(&chunk_header)
                    {
                        debug!(
                            "    {: >3} {} | {} | {: >10} | part = {}",
                            chunk_header.inner.height_created,
                            format_hash(chunk_header.chunk_hash().0),
                            chunk_header.inner.shard_id,
                            chunk_producer,
                            chunk_one_part.part_id
                        );
                    }
                }
            }
        }
    }
}

impl ChainGenesis {
    pub fn test() -> Self {
        ChainGenesis {
            chain_id: "unittest".to_string(),
            time: Utc::now(),
            gas_limit: 1_000_000,
            gas_price: 1,
            total_supply: 1_000_000_000,
            max_inflation_rate: 0,
            gas_price_adjustment_rate: 0,
            transaction_validity_period: 100,
        }
    }
}
