//! Set of methods that construct transactions of various kind.

use crate::node::Node;
use crate::sampler::{sample_one, sample_two};
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::{
    DeployContractTransaction, FunctionCallTransaction, SignedTransaction, TransactionBody,
};
use primitives::types::AccountId;
use std::collections::HashMap;
use std::iter::repeat_with;
use std::sync::{Arc, RwLock};

/// Keeps the context that is needed to generate a random transaction.
pub struct Generator {
    /// Nodes that can be used to generate nonces
    pub nodes: Vec<Arc<RwLock<dyn Node>>>,
    /// Tracks nonces for the accounts.
    nonces: RwLock<HashMap<AccountId, u64>>,
}

/// Type of transaction to generate.
pub enum TransactionType {
    Monetary,
    SetGet,
    HeavyStorage,
}

impl Generator {
    pub fn new(nodes: Vec<Arc<RwLock<dyn Node>>>) -> Self {
        Self { nodes, nonces: Default::default() }
    }

    /// Increments nonce and returns it.
    fn nonce(&self, node: &Arc<RwLock<dyn Node>>) -> u64 {
        *self
            .nonces
            .write()
            .unwrap()
            .entry(Self::account_id(node))
            .and_modify(|e| *e += 1)
            .or_insert(1)
    }

    /// Get account id of the given node.
    fn account_id(node: &Arc<RwLock<dyn Node>>) -> AccountId {
        node.read().unwrap().account_id().unwrap().clone()
    }

    /// Get in-memory-signer of the given node.
    fn signer(node: &Arc<RwLock<dyn Node>>) -> Arc<InMemorySigner> {
        node.read().unwrap().signer()
    }

    /// Create send money transaction.
    pub fn send_money(&mut self) -> SignedTransaction {
        let (alice, bob) = sample_two(&self.nodes);
        let nonce = self.nonce(alice);
        TransactionBody::send_money(
            nonce,
            Self::account_id(alice).as_str(),
            Self::account_id(bob).as_str(),
            1,
        )
        .sign(&*Self::signer(alice))
    }

    /// Returns transactions that deploy test contract to an account of every node.
    pub fn deploy_test_contract(&mut self) -> Vec<SignedTransaction> {
        let wasm_binary: &[u8] = include_bytes!("../../../tests/hello.wasm");
        let mut res = vec![];
        for node in &self.nodes {
            let t = DeployContractTransaction {
                nonce: self.nonce(node),
                contract_id: Self::account_id(node),
                wasm_byte_array: wasm_binary.to_vec(),
            };
            res.push(TransactionBody::DeployContract(t).sign(&*Self::signer(node)));
        }
        res
    }

    /// Returns a transaction that calls `setKeyValue` on a random contract from a random account.
    pub fn call_set(&mut self) -> SignedTransaction {
        let node = sample_one(&self.nodes);
        let key = rand::random::<usize>() % 1_000;
        let value = rand::random::<usize>() % 1_000;
        let t = FunctionCallTransaction {
            nonce: self.nonce(node),
            originator: Self::account_id(node),
            contract_id: Self::account_id(node),
            method_name: "setKeyValue".as_bytes().to_vec(),
            args: format!("{{\"key\":\"{}\", \"value\":\"{}\"}}", key, value).as_bytes().to_vec(),
            amount: 1,
        };
        TransactionBody::FunctionCall(t).sign(&*Self::signer(node))
    }

    /// Returns a transaction that calls `setKeyValue` on a random contract.
    pub fn call_get(&mut self) -> SignedTransaction {
        let node = sample_one(&self.nodes);
        let key = rand::random::<usize>() % 1_000;
        let t = FunctionCallTransaction {
            nonce: self.nonce(node),
            originator: Self::account_id(node),
            contract_id: Self::account_id(node),
            method_name: "getValueByKey".as_bytes().to_vec(),
            args: format!("{{\"key\":\"{}\"}}", key).as_bytes().to_vec(),
            amount: 1,
        };
        TransactionBody::FunctionCall(t).sign(&*Self::signer(node))
    }

    /// Returns a transaction that calls `heavy_storage_blocks` on a random contract.
    pub fn call_benchmark_storage(&mut self) -> SignedTransaction {
        let node = sample_one(&self.nodes);
        let t = FunctionCallTransaction {
            nonce: self.nonce(node),
            originator: Self::account_id(node),
            contract_id: Self::account_id(node),
            method_name: "heavy_storage_blocks".as_bytes().to_vec(),
            args: "{\"n\":1000}".as_bytes().to_vec(),
            amount: 1,
        };
        TransactionBody::FunctionCall(t).sign(&*Self::signer(node))
    }

    /// Endlessly generates transactions of the given type.
    pub fn iter(
        mut self,
        transaction_type: TransactionType,
    ) -> impl Iterator<Item = SignedTransaction> {
        // Create transactions that do initialization for other transactions to be successful.
        let initialization_transactions = match &transaction_type {
            TransactionType::SetGet | TransactionType::HeavyStorage => self.deploy_test_contract(),
            _ => vec![],
        };
        // Create transactions that constitute the main load.
        let transactions = repeat_with(move || {
            match transaction_type {
                TransactionType::HeavyStorage => self.call_benchmark_storage(),
                TransactionType::Monetary => self.send_money(),
                TransactionType::SetGet => {
                    // Randomly execute set and get operators.
                    if rand::random::<bool>() {
                        self.call_set()
                    } else {
                        self.call_get()
                    }
                }
            }
        });
        initialization_transactions.into_iter().chain(transactions)
    }
}
