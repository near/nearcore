use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;

use near_primitives::crypto::signature::{PublicKey, Signature};
use near_primitives::crypto::signer::{EDSigner, InMemorySigner};
use near_primitives::hash::CryptoHash;
use near_primitives::rpc::ABCIQueryResponse;
use near_primitives::test_utils::get_public_key_from_seed;
use near_primitives::transaction::{
    ReceiptTransaction, SignedTransaction, TransactionResult, TransactionStatus,
};
use near_primitives::types::{AccountId, BlockIndex, MerkleHash, ShardId};
use near_store::test_utils::create_test_store;
use near_store::{Store, StoreUpdate, Trie, TrieChanges, WrappedTrieChanges};
use node_runtime::state_viewer::AccountViewCallResult;

use crate::error::{Error, ErrorKind};
use crate::types::{BlockHeader, ReceiptResult, RuntimeAdapter, Weight};
use crate::{Block, Chain, ValidTransaction};

impl Block {
    pub fn empty(prev: &BlockHeader, signer: Arc<dyn EDSigner>) -> Self {
        Block::produce(
            prev,
            prev.height + 1,
            prev.prev_state_root,
            vec![],
            HashMap::default(),
            vec![],
            signer,
        )
    }
}

/// Simple key value runtime for tests.
pub struct KeyValueRuntime {
    store: Arc<Store>,
    trie: Arc<Trie>,
    root: MerkleHash,
    validators: Vec<(AccountId, PublicKey)>,
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
                .map(|account_id| (account_id.clone(), get_public_key_from_seed(account_id)))
                .collect(),
        }
    }

    pub fn get_root(&self) -> MerkleHash {
        self.root
    }
}

impl RuntimeAdapter for KeyValueRuntime {
    fn genesis_state(&self, _shard_id: ShardId) -> (StoreUpdate, MerkleHash) {
        (self.store.store_update(), MerkleHash::default())
    }

    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error> {
        let (_account_id, public_key) =
            &self.validators[(header.height as usize) % self.validators.len()];
        if !header.verify_block_producer(public_key) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.total_weight.next(header.approval_sigs.len() as u64))
    }

    fn get_epoch_block_proposers(
        &self,
        _height: BlockIndex,
    ) -> Result<Vec<(AccountId, u64)>, Box<dyn std::error::Error>> {
        Ok(self.validators.iter().map(|x| (x.0.clone(), 1)).collect())
    }

    fn get_block_proposer(
        &self,
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        Ok(self.validators[(height as usize) % self.validators.len()].0.clone())
    }

    fn get_chunk_proposer(
        &self,
        _shard_id: ShardId,
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>> {
        Ok(self.validators[(height as usize) % self.validators.len()].0.clone())
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

    fn apply_transactions(
        &self,
        _shard_id: ShardId,
        state_root: &MerkleHash,
        _block_index: BlockIndex,
        _prev_block_hash: &CryptoHash,
        _receipts: &Vec<Vec<ReceiptTransaction>>,
        transactions: &Vec<SignedTransaction>,
    ) -> Result<
        (WrappedTrieChanges, MerkleHash, Vec<TransactionResult>, ReceiptResult),
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
        ))
    }

    fn query(
        &self,
        _state_root: MerkleHash,
        _height: BlockIndex,
        path: &str,
        _data: &[u8],
    ) -> Result<ABCIQueryResponse, Box<dyn std::error::Error>> {
        let path = path.split("/").collect::<Vec<_>>();
        Ok(ABCIQueryResponse::account(
            path[1],
            AccountViewCallResult {
                account_id: path[1].to_string(),
                nonce: 0,
                amount: 1000,
                stake: 0,
                public_keys: vec![],
                code_hash: CryptoHash::default(),
            },
        ))
    }
}

pub fn setup() -> (Chain, Arc<KeyValueRuntime>, Arc<InMemorySigner>) {
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new(store.clone()));
    let chain = Chain::new(store, runtime.clone(), Utc::now()).unwrap();
    let signer = Arc::new(InMemorySigner::from_seed("test", "test"));
    (chain, runtime, signer)
}
