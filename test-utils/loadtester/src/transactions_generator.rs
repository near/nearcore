//! Set of methods that construct transactions of various kind.

use crate::remote_node::RemoteNode;
use primitives::transaction::{SignedTransaction, TransactionBody};
use std::sync::{Arc, RwLock};

pub struct Generator {}

impl Generator {
    /// Create send money transaction.
    pub fn send_money(
        node_from: &Arc<RwLock<RemoteNode>>,
        node_to: &Arc<RwLock<RemoteNode>>,
    ) -> SignedTransaction {
        let (signer_from, nonce) = {
            let mut node_from = node_from.write().unwrap();
            let acc_ind = rand::random::<usize>() % node_from.nonces.len();
            node_from.nonces[acc_ind] += 1;
            (node_from.signers[acc_ind].clone(), node_from.nonces[acc_ind])
        };

        let signer_to = {
            let node_to = node_to.write().unwrap();
            let acc_ind = rand::random::<usize>() % node_to.nonces.len();
            node_to.signers[acc_ind].clone()
        };
        let acc_from = signer_from.account_id.clone();
        let acc_to = signer_to.account_id.clone();

        TransactionBody::send_money(nonce, acc_from.as_str(), acc_to.as_str(), 1)
            .sign(&*signer_from)
    }

    //    /// Returns transactions that deploy test contract to an account of every node.
    //    pub fn deploy_test_contract(&mut self) -> Vec<SignedTransaction> {
    //        let wasm_binary: &[u8] = include_bytes!("../../../tests/hello.wasm");
    //        let mut res = vec![];
    //        for node in &self.nodes {
    //            let t = DeployContractTransaction {
    //                nonce: self.nonce(node),
    //                contract_id: Self::account_id(node),
    //                wasm_byte_array: wasm_binary.to_vec(),
    //            };
    //            res.push(TransactionBody::DeployContract(t).sign(&*Self::signer(node)));
    //        }
    //        res
    //    }
    //
    //    /// Returns a transaction that calls `setKeyValue` on a random contract from a random account.
    //    pub fn call_set(&mut self) -> SignedTransaction {
    //        let node = sample_one(&self.nodes);
    //        let key = rand::random::<usize>() % 1_000;
    //        let value = rand::random::<usize>() % 1_000;
    //        let t = FunctionCallTransaction {
    //            nonce: self.nonce(node),
    //            originator: Self::account_id(node),
    //            contract_id: Self::account_id(node),
    //            method_name: b"setKeyValue".to_vec(),
    //            args: format!("{{\"key\":\"{}\", \"value\":\"{}\"}}", key, value).as_bytes().to_vec(),
    //            amount: 1,
    //        };
    //        TransactionBody::FunctionCall(t).sign(&*Self::signer(node))
    //    }
    //
    //    /// Returns a transaction that calls `setKeyValue` on a random contract.
    //    pub fn call_get(&mut self) -> SignedTransaction {
    //        let node = sample_one(&self.nodes);
    //        let key = rand::random::<usize>() % 1_000;
    //        let t = FunctionCallTransaction {
    //            nonce: self.nonce(node),
    //            originator: Self::account_id(node),
    //            contract_id: Self::account_id(node),
    //            method_name: b"getValueByKey".to_vec(),
    //            args: format!("{{\"key\":\"{}\"}}", key).as_bytes().to_vec(),
    //            amount: 1,
    //        };
    //        TransactionBody::FunctionCall(t).sign(&*Self::signer(node))
    //    }
    //
    //    /// Returns a transaction that calls `heavy_storage_blocks` on a random contract.
    //    pub fn call_benchmark_storage(&mut self) -> SignedTransaction {
    //        let node = sample_one(&self.nodes);
    //        let t = FunctionCallTransaction {
    //            nonce: self.nonce(node),
    //            originator: Self::account_id(node),
    //            contract_id: Self::account_id(node),
    //            method_name: b"heavy_storage_blocks".to_vec(),
    //            args: "{\"n\":1000}".as_bytes().to_vec(),
    //            amount: 1,
    //        };
    //        TransactionBody::FunctionCall(t).sign(&*Self::signer(node))
    //    }

    //    /// Endlessly generates transactions of the given type.
    //    pub fn iter(
    //        mut self,
    //        transaction_type: TransactionType,
    //    ) -> impl Iterator<Item = SignedTransaction> {
    //        // Create transactions that do initialization for other transactions to be successful.
    //        let initialization_transactions = match &transaction_type {
    //            TransactionType::SetGet | TransactionType::HeavyStorage => self.deploy_test_contract(),
    //            _ => vec![],
    //        };
    //        // Create transactions that constitute the main load.
    //        let transactions = repeat_with(move || {
    //            match transaction_type {
    //                TransactionType::Monetary => self.send_money(),
    //                _ => unimplemented!(),
    //            }
    //        });
    //        initialization_transactions.into_iter().chain(transactions)
    //    }
}
