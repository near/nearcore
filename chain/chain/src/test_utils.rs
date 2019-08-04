use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use chrono::Utc;

use near_primitives::crypto::signature::{verify, PublicKey, Signature};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::hash::{hash, hash_struct, CryptoHash};
use near_primitives::merkle::merklize;
use near_primitives::rpc::{AccountViewCallResult, QueryResponse};
use near_primitives::serialize::{Decode, Encode};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::test_utils::get_public_key_from_seed;
use near_primitives::transaction::ReceiptBody::NewCall;
use near_primitives::transaction::TransactionBody::SendMoney;
use near_primitives::transaction::{
    AsyncCall, ReceiptTransaction, SignedTransaction, TransactionResult, TransactionStatus,
};
use near_primitives::types::{
    AccountId, Balance, BlockIndex, EpochId, GasUsage, MerkleHash, Nonce, ShardId, ValidatorStake,
};
use near_store::test_utils::create_test_store;
use near_store::{Store, StoreUpdate, Trie, TrieChanges, WrappedTrieChanges, COL_BLOCK_HEADER};

use crate::error::{Error, ErrorKind};
use crate::types::{ApplyTransactionResult, BlockHeader, RuntimeAdapter, Weight};
use crate::{Chain, ChainGenesis, ValidTransaction};

/// Simple key value runtime for tests.
pub struct KeyValueRuntime {
    store: Arc<Store>,
    trie: Arc<Trie>,
    root: MerkleHash,
    validators: Vec<Vec<ValidatorStake>>,
    validator_groups: u64,

    // A mapping state_root => {account id => amounts}, for transactions and receipts
    amounts: RwLock<HashMap<MerkleHash, HashMap<AccountId, u128>>>,
    receipt_nonces: RwLock<HashMap<MerkleHash, HashSet<CryptoHash>>>,
    tx_nonces: RwLock<HashMap<MerkleHash, HashSet<(AccountId, Nonce)>>>,

    hash_to_epoch: RwLock<HashMap<CryptoHash, EpochId>>,
    hash_to_next_epoch: RwLock<HashMap<CryptoHash, EpochId>>,
    hash_to_valset: RwLock<HashMap<EpochId, u64>>,
    epoch_start: RwLock<HashMap<CryptoHash, u64>>,
}

pub fn account_id_to_shard_id(account_id: &AccountId, num_shards: ShardId) -> ShardId {
    ((hash(&account_id.clone().into_bytes()).0).0[0] as u64) % num_shards
}

impl KeyValueRuntime {
    pub fn new(store: Arc<Store>) -> Self {
        Self::new_with_validators(store, vec![vec!["test".to_string()]], 1)
    }

    pub fn new_with_validators(
        store: Arc<Store>,
        validators: Vec<Vec<AccountId>>,
        validator_groups: u64,
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

        let mut amounts = HashMap::new();
        amounts.insert(MerkleHash::default(), initial_amounts);
        KeyValueRuntime {
            store,
            trie,
            root: MerkleHash::default(),
            validators: validators
                .iter()
                .map(|inner| {
                    inner
                        .iter()
                        .map(|account_id| ValidatorStake {
                            account_id: account_id.clone(),
                            public_key: get_public_key_from_seed(account_id),
                            amount: 1_000_000,
                        })
                        .collect()
                })
                .collect(),
            validator_groups,
            amounts: RwLock::new(amounts),
            receipt_nonces: RwLock::new(HashMap::new()),
            tx_nonces: RwLock::new(HashMap::new()),
            hash_to_epoch: RwLock::new(HashMap::new()),
            hash_to_next_epoch: RwLock::new(map_with_default_hash1),
            hash_to_valset: RwLock::new(map_with_default_hash3.clone()),
            epoch_start: RwLock::new(map_with_default_hash2.clone()),
        }
    }

    pub fn get_root(&self) -> MerkleHash {
        self.root
    }

    fn get_prev_height(
        &self,
        prev_hash: &CryptoHash,
    ) -> Result<BlockIndex, Box<dyn std::error::Error>> {
        if prev_hash == &CryptoHash::default() {
            return Ok(0);
        }
        let prev_block_header = self
            .store
            .get_ser::<BlockHeader>(COL_BLOCK_HEADER, prev_hash.as_ref())?
            .ok_or("Missing block when computing the epoch")?;
        Ok(prev_block_header.height)
    }

    fn get_epoch_and_valset(
        &self,
        prev_hash: CryptoHash,
    ) -> Result<(EpochId, usize, EpochId), Error> {
        if prev_hash == CryptoHash::default() {
            return Ok((EpochId(prev_hash), 0, EpochId(prev_hash)));
        }
        let prev_block_header = self
            .store
            .get_ser::<BlockHeader>(COL_BLOCK_HEADER, prev_hash.as_ref())?
            .ok_or(ErrorKind::Other("Missing block when computing the epoch".to_string()))?;

        let mut hash_to_epoch = self.hash_to_epoch.write().unwrap();
        let mut hash_to_next_epoch = self.hash_to_next_epoch.write().unwrap();
        let mut hash_to_valset = self.hash_to_valset.write().unwrap();
        let mut epoch_start_map = self.epoch_start.write().unwrap();

        let prev_prev_hash = prev_block_header.prev_hash;
        let prev_epoch = hash_to_epoch.get(&prev_prev_hash);
        let prev_next_epoch = hash_to_next_epoch.get(&prev_prev_hash).unwrap();
        let prev_valset = match prev_epoch {
            Some(prev_epoch) => Some(*hash_to_valset.get(&prev_epoch).unwrap()),
            None => None,
        };

        let prev_epoch_start = *epoch_start_map.get(&prev_prev_hash).unwrap();

        let increment_epoch = prev_prev_hash == CryptoHash::default() // genesis is in its own epoch
            || prev_block_header.height - prev_epoch_start >= 5;

        let (epoch, next_epoch, valset, epoch_start) = if increment_epoch {
            let new_valset = match prev_valset {
                None => 0,
                Some(prev_valset) => prev_valset + 1,
            };
            (prev_next_epoch.clone(), EpochId(prev_hash), new_valset, prev_block_header.height)
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

    fn get_valset_for_epoch(&self, epoch_id: &EpochId) -> usize {
        // conveniently here if the prev_hash is passed mistakenly instead of the epoch_hash,
        // the `unwrap` will trigger
        return *self.hash_to_valset.read().unwrap().get(epoch_id).unwrap() as usize
            % self.validators.len();
    }
}

impl RuntimeAdapter for KeyValueRuntime {
    fn genesis_state(&self) -> (StoreUpdate, Vec<MerkleHash>) {
        (
            self.store.store_update(),
            ((0..self.num_shards()).map(|_| MerkleHash::default()).collect()),
        )
    }

    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error> {
        let validators = &self.validators
            [self.get_epoch_and_valset(header.prev_hash).map_err(|err| err.to_string())?.1];
        let validator = &validators[(header.height as usize) % validators.len()];
        if !header.verify_block_producer(&validator.public_key) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.total_weight.next(header.approval_sigs.len() as u64))
    }

    fn verify_validator_signature(
        &self,
        _epoch_id: &EpochId,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> bool {
        if let Some(validator) = self
            .validators
            .iter()
            .flatten()
            .find(|&validator_stake| &validator_stake.account_id == account_id)
        {
            verify(data, signature, &validator.public_key)
        } else {
            false
        }
    }

    fn verify_chunk_header_signature(&self, _header: &ShardChunkHeader) -> Result<bool, Error> {
        Ok(true)
    }

    fn get_epoch_block_proposers(
        &self,
        epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(AccountId, bool)>, Box<dyn std::error::Error>> {
        let validators = &self.validators[self.get_valset_for_epoch(epoch_id)];
        Ok(validators.iter().map(|x| (x.account_id.clone(), false)).collect())
    }

    fn get_block_proposer(
        &self,
        epoch_id: &EpochId,
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        let validators = &self.validators[self.get_valset_for_epoch(epoch_id)];
        Ok(validators[(height as usize) % validators.len()].account_id.clone())
    }

    fn get_chunk_proposer(
        &self,
        epoch_id: &EpochId,
        height: BlockIndex,
        shard_id: ShardId,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        let validators = &self.validators[self.get_valset_for_epoch(epoch_id)];
        assert_eq!((validators.len() as u64) % self.num_shards(), 0);
        assert_eq!(0, validators.len() as u64 % self.validator_groups);
        let validators_per_shard = validators.len() as ShardId / self.validator_groups;
        let coef = validators.len() as ShardId / self.num_shards();
        let offset = (shard_id * coef / validators_per_shard * validators_per_shard) as usize;
        let delta = ((shard_id + height + 1) % validators_per_shard) as usize;
        Ok(validators[offset + delta].account_id.clone())
    }

    fn num_shards(&self) -> ShardId {
        let ret = self.validators.iter().map(|x| x.len()).min().unwrap();
        if ret < 64 || ret % 4 != 0 {
            ret as ShardId
        } else {
            (ret / 4) as ShardId
        }
    }

    fn num_total_parts(&self, parent_hash: &CryptoHash) -> usize {
        let height = self.get_prev_height(parent_hash).unwrap();
        1 + self.num_data_parts(parent_hash) * (1 + (height as usize) % 3)
    }

    fn num_data_parts(&self, parent_hash: &CryptoHash) -> usize {
        let height = self.get_prev_height(parent_hash).unwrap();
        // Test changing number of data parts
        12 + 2 * ((height as usize) % 4)
    }

    fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        account_id_to_shard_id(account_id, self.num_shards())
    }

    fn get_part_owner(
        &self,
        parent_hash: &CryptoHash,
        part_id: u64,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let validators = &self.validators[self.get_epoch_and_valset(*parent_hash)?.1];
        // if we don't use data_parts and total_parts as part of the formula here, the part owner
        //     would not depend on height, and tests wouldn't catch passing wrong height here
        let idx =
            part_id as usize + self.num_data_parts(parent_hash) + self.num_total_parts(parent_hash);
        Ok(validators[idx as usize % validators.len()].account_id.clone())
    }

    fn cares_about_shard(
        &self,
        account_id: &AccountId,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        let validators = &self.validators[self.get_epoch_and_valset(*parent_hash).unwrap().1];
        assert_eq!((validators.len() as u64) % self.num_shards(), 0);
        assert_eq!(0, validators.len() as u64 % self.validator_groups);
        let validators_per_shard = validators.len() as ShardId / self.validator_groups;
        let coef = validators.len() as ShardId / self.num_shards();
        let offset = (shard_id * coef / validators_per_shard * validators_per_shard) as usize;
        assert!(offset + validators_per_shard as usize <= validators.len());
        for validator in validators[offset..offset + (validators_per_shard as usize)].iter() {
            if validator.account_id == *account_id {
                return true;
            }
        }
        false
    }

    fn will_care_about_shard(
        &self,
        account_id: &AccountId,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        // figure out if this block is the first block of an epoch
        let first_block_of_the_epoch = self.is_epoch_start(parent_hash, 0).unwrap();

        // If it is the first block of the epoch, Nightshade runtime can't infer the next validator set
        //    emulate that behavior
        if first_block_of_the_epoch {
            return false;
        }

        let validators = &self.validators
            [(self.get_epoch_and_valset(*parent_hash).unwrap().1 + 1) % self.validators.len()];
        assert_eq!((validators.len() as u64) % self.num_shards(), 0);
        assert_eq!(0, validators.len() as u64 % self.validator_groups);
        let validators_per_shard = validators.len() as ShardId / self.validator_groups;
        let coef = validators.len() as ShardId / self.num_shards();
        let offset = (shard_id * coef / validators_per_shard * validators_per_shard) as usize;
        for validator in validators[offset..offset + (validators_per_shard as usize)].iter() {
            if validator.account_id == *account_id {
                return true;
            }
        }
        false
    }

    fn validate_tx(
        &self,
        _shard_id: ShardId,
        _state_root: MerkleHash,
        transaction: SignedTransaction,
    ) -> Result<ValidTransaction, String> {
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
        _gas_used: GasUsage,
        _gas_price: Balance,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn apply_transactions(
        &self,
        shard_id: ShardId,
        state_root: &MerkleHash,
        _block_index: BlockIndex,
        _prev_block_hash: &CryptoHash,
        _block_hash: &CryptoHash,
        receipts: &Vec<ReceiptTransaction>,
        transactions: &Vec<SignedTransaction>,
    ) -> Result<ApplyTransactionResult, Box<dyn std::error::Error>> {
        let mut tx_results = vec![];

        let mut accounts_mapping =
            self.amounts.read().unwrap().get(state_root).cloned().unwrap_or_else(|| HashMap::new());
        let mut receipt_nonces = self
            .receipt_nonces
            .read()
            .unwrap()
            .get(state_root)
            .cloned()
            .unwrap_or_else(|| HashSet::new());
        let mut tx_nonces = self
            .tx_nonces
            .read()
            .unwrap()
            .get(state_root)
            .cloned()
            .unwrap_or_else(|| HashSet::new());

        let mut balance_transfers = vec![];

        for receipt in receipts.iter() {
            if let NewCall(call) = &receipt.body {
                assert_eq!(self.account_id_to_shard_id(&receipt.receiver), shard_id);
                if !receipt_nonces.contains(&receipt.nonce) {
                    receipt_nonces.insert(receipt.nonce);
                    balance_transfers.push((
                        receipt.originator.clone(),
                        receipt.receiver.clone(),
                        call.amount,
                        0,
                    ));
                } else {
                    balance_transfers.push((
                        receipt.originator.clone(),
                        receipt.receiver.clone(),
                        0,
                        0,
                    ));
                }
            } else {
                unreachable!();
            }
        }

        for transaction in transactions {
            if let SendMoney(send_money_tx) = &transaction.body {
                assert_eq!(self.account_id_to_shard_id(&send_money_tx.originator), shard_id);
                if !tx_nonces.contains(&(send_money_tx.receiver.clone(), send_money_tx.nonce)) {
                    tx_nonces.insert((send_money_tx.receiver.clone(), send_money_tx.nonce));
                    balance_transfers.push((
                        send_money_tx.originator.clone(),
                        send_money_tx.receiver.clone(),
                        send_money_tx.amount,
                        send_money_tx.nonce,
                    ));
                } else {
                    balance_transfers.push((
                        send_money_tx.originator.clone(),
                        send_money_tx.receiver.clone(),
                        0,
                        send_money_tx.nonce,
                    ));
                }
            } else {
                unreachable!();
            }
        }

        let mut new_receipts = HashMap::new();

        for (from, to, amount, nonce) in balance_transfers {
            let mut good_to_go = false;

            if self.account_id_to_shard_id(&from) != shard_id {
                // This is a receipt, was already debited
                good_to_go = true;
            } else if let Some(balance) = accounts_mapping.get(&from) {
                if *balance >= amount {
                    let new_balance = balance - amount;
                    accounts_mapping.insert(from.clone(), new_balance);
                    good_to_go = true;
                }
            }

            if good_to_go {
                let new_receipt_hashes = if self.account_id_to_shard_id(&to) == shard_id {
                    accounts_mapping
                        .insert(to.clone(), accounts_mapping.get(&to).unwrap_or(&0) + amount);
                    vec![]
                } else {
                    assert_ne!(amount, 0);
                    assert_ne!(nonce, 0);
                    let receipt = ReceiptTransaction::new(
                        from.clone(),
                        to.clone(),
                        hash_struct(&(from.clone(), to.clone(), amount, nonce)),
                        NewCall(AsyncCall::new(
                            vec![],
                            vec![],
                            amount,
                            AccountId::default(),
                            from,
                            PublicKey::empty(),
                        )),
                    );
                    let receipt_hash = receipt.get_hash();
                    new_receipts.entry(receipt.shard_id()).or_insert_with(|| vec![]).push(receipt);
                    vec![receipt_hash]
                };

                tx_results.push(TransactionResult {
                    status: TransactionStatus::Completed,
                    logs: vec![],
                    receipts: new_receipt_hashes,
                    result: None,
                });
            }
        }

        let mut new_balances = vec![];
        for validator in self.validators.iter().flatten() {
            let mut seen = false;
            for (key, value) in accounts_mapping.iter() {
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
        let (new_state_root, _) = merklize(&new_balances);
        self.amounts.write().unwrap().insert(new_state_root, accounts_mapping);
        self.receipt_nonces.write().unwrap().insert(new_state_root, receipt_nonces);
        self.tx_nonces.write().unwrap().insert(new_state_root, tx_nonces);

        let apply_result = ApplyTransactionResult {
            trie_changes: WrappedTrieChanges::new(
                self.trie.clone(),
                TrieChanges::empty(state_root.clone()),
            ),
            new_root: new_state_root,
            transaction_results: tx_results,
            receipt_result: new_receipts,
            validator_proposals: vec![],
            new_total_supply: 0,
            gas_used: 0,
        };

        Ok(apply_result)
    }

    fn query(
        &self,
        state_root: MerkleHash,
        _height: BlockIndex,
        path: Vec<&str>,
        _data: &[u8],
    ) -> Result<QueryResponse, Box<dyn std::error::Error>> {
        let account_id = path[1].to_string();
        let account_id2 = account_id.clone();
        Ok(QueryResponse::ViewAccount(AccountViewCallResult {
            account_id,
            nonce: 0,
            amount: self
                .amounts
                .read()
                .unwrap()
                .get(&state_root)
                .map_or_else(|| 0, |mapping| *mapping.get(&account_id2).unwrap_or(&0)),
            stake: 0,
            public_keys: vec![],
            code_hash: CryptoHash::default(),
        }))
    }

    fn dump_state(
        &self,
        _shard_id: ShardId,
        state_root: MerkleHash,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Encode::encode(&(
            self.amounts.read().unwrap().get(&state_root),
            self.tx_nonces.read().unwrap().get(&state_root),
            self.receipt_nonces.read().unwrap().get(&state_root),
        ))
        .map_err(Into::into)
    }

    fn set_state(
        &self,
        _shard_id: ShardId,
        state_root: MerkleHash,
        payload: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (amt, txn, rcn) = Decode::decode(payload.as_ref()).unwrap();
        if let Some(amt) = amt {
            self.amounts.write().unwrap().insert(state_root, amt);
        }
        if let Some(txn) = txn {
            self.amounts.write().unwrap().insert(state_root, txn);
        }
        if let Some(rcn) = rcn {
            self.amounts.write().unwrap().insert(state_root, rcn);
        }
        Ok(())
    }

    fn is_epoch_start(
        &self,
        parent_hash: &CryptoHash,
        _index: BlockIndex,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        if parent_hash == &CryptoHash::default() {
            return Ok(true);
        }
        let prev_block_header = self
            .store
            .get_ser::<BlockHeader>(COL_BLOCK_HEADER, parent_hash.as_ref())?
            .ok_or("Missing block when computing the epoch")?;
        let prev_prev_hash = prev_block_header.prev_hash;
        Ok(self.get_epoch_and_valset(*parent_hash)?.0
            != self.get_epoch_and_valset(prev_prev_hash)?.0)
    }

    fn get_epoch_id(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        Ok(self.get_epoch_and_valset(*parent_hash)?.0)
    }

    fn get_next_epoch_id(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        Ok(self.get_epoch_and_valset(*parent_hash)?.2)
    }
}

pub fn setup() -> (Chain, Arc<KeyValueRuntime>, Arc<InMemorySigner>) {
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new(store.clone()));
    let chain =
        Chain::new(store, runtime.clone(), ChainGenesis::new(Utc::now(), 1_000_000, 100)).unwrap();
    let signer = Arc::new(InMemorySigner::from_seed("test", "test"));
    (chain, runtime, signer)
}
