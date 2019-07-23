use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;

use near_primitives::crypto::signature::Signature;
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::hash::CryptoHash;
use near_primitives::rpc::{AccountViewCallResult, QueryResponse};
use near_primitives::test_utils::get_public_key_from_seed;
use near_primitives::transaction::{
    ReceiptTransaction, SignedTransaction, TransactionResult, TransactionStatus,
};
use near_primitives::types::{AccountId, BlockIndex, MerkleHash, ShardId, ValidatorStake};
use near_store::test_utils::create_test_store;
use near_store::{Store, StoreUpdate, Trie, TrieChanges, WrappedTrieChanges};

use crate::error::{Error, ErrorKind};
use crate::types::{BlockHeader, ReceiptResult, RuntimeAdapter, Weight};
use crate::{Chain, ValidTransaction};

/// Simple key value runtime for tests.
pub struct KeyValueRuntime {
    store: Arc<Store>,
    trie: Arc<Trie>,
    root: MerkleHash,
    validators: Vec<ValidatorStake>,
}

impl KeyValueRuntime {
    pub fn new(store: Arc<Store>) -> Self {
        Self::new_with_validators(store, vec!["test".to_string()])
    }

    pub fn new_with_validators(store: Arc<Store>, validators: Vec<AccountId>) -> Self {
        let trie = Arc::new(Trie::new(store.clone()));
        KeyValueRuntime {
            store,
            trie,
            root: MerkleHash::default(),
            validators: validators
                .iter()
                .map(|account_id| ValidatorStake {
                    account_id: account_id.clone(),
                    public_key: get_public_key_from_seed(account_id),
                    amount: 1_000_000,
                })
                .collect(),
        }
    }

    pub fn get_root(&self) -> MerkleHash {
        self.root
    }
}

impl RuntimeAdapter for KeyValueRuntime {
    fn genesis_state(&self) -> (StoreUpdate, Vec<MerkleHash>) {
        (self.store.store_update(), vec![MerkleHash::default()])
    }

    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error> {
        let validator = &self.validators[(header.height as usize) % self.validators.len()];
        if !header.verify_block_producer(&validator.public_key) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.total_weight.next(header.approval_sigs.len() as u64))
    }

    fn get_epoch_block_proposers(
        &self,
        _parent_hash: CryptoHash,
        _height: BlockIndex,
    ) -> Result<Vec<AccountId>, Box<dyn std::error::Error>> {
        Ok(self.validators.iter().map(|x| x.account_id.clone()).collect())
    }

    fn get_block_proposer(
        &self,
        _parent_hash: CryptoHash,
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        Ok(self.validators[(height as usize) % self.validators.len()].account_id.clone())
    }

    fn get_chunk_proposer(
        &self,
        _shard_id: ShardId,
        _parent_hash: CryptoHash,
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        Ok(self.validators[(height as usize) % self.validators.len()].account_id.clone())
    }

    fn check_validator_signature(&self, _account_id: &AccountId, _signature: &Signature) -> bool {
        true
    }

    fn num_shards(&self) -> ShardId {
        1
    }

    fn account_id_to_shard_id(&self, _account_id: &AccountId) -> ShardId {
        0
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
        _validator_mask: Vec<bool>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn apply_transactions(
        &self,
        _shard_id: ShardId,
        state_root: &MerkleHash,
        _block_index: BlockIndex,
        _prev_block_hash: &CryptoHash,
        _block_hash: &CryptoHash,
        _receipts: &Vec<Vec<ReceiptTransaction>>,
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
        let mut tx_results = vec![];
        for _ in transactions {
            tx_results.push(TransactionResult {
                status: TransactionStatus::Completed,
                logs: vec![],
                receipts: vec![],
                result: None,
            });
        }
        Ok((
            WrappedTrieChanges::new(self.trie.clone(), TrieChanges::empty(state_root.clone())),
            *state_root,
            tx_results,
            HashMap::default(),
            vec![],
        ))
    }

    fn query(
        &self,
        _state_root: MerkleHash,
        _height: BlockIndex,
        path: &str,
        _data: &[u8],
    ) -> Result<QueryResponse, Box<dyn std::error::Error>> {
        let path = path.split("/").collect::<Vec<_>>();
        Ok(QueryResponse::ViewAccount(AccountViewCallResult {
            account_id: path[1].to_string(),
            nonce: 0,
            amount: 1000,
            stake: 0,
            public_keys: vec![],
            code_hash: CryptoHash::default(),
        }))
    }

    fn dump_state(
        &self,
        _shard_id: ShardId,
        _state_root: MerkleHash,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(vec![])
    }

    fn set_state(
        &self,
        _shard_id: ShardId,
        _state_root: MerkleHash,
        _payload: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

pub fn setup() -> (Chain, Arc<KeyValueRuntime>, Arc<InMemorySigner>) {
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new(store.clone()));
    let chain = Chain::new(store, runtime.clone(), Utc::now()).unwrap();
    let signer = Arc::new(InMemorySigner::from_seed("test", "test"));
    (chain, runtime, signer)
}
