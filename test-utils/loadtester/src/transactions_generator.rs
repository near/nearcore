//! Set of methods that construct transactions of various kind.

use crate::remote_node::RemoteNode;
use primitives::transaction::{DeployContractTransaction, SignedTransaction, TransactionBody};
use std::sync::{Arc, RwLock};

pub struct Generator {}

impl Generator {
    /// Create send money transaction.
    pub fn send_money(
        node: &Arc<RwLock<RemoteNode>>,
        signer_ind: usize,
        all_accounts: &Vec<String>,
    ) -> SignedTransaction {
        let (signer_from, nonce) = {
            let mut node = node.write().unwrap();
            node.nonces[signer_ind] += 1;
            (node.signers[signer_ind].clone(), node.nonces[signer_ind])
        };

        let acc_from = signer_from.account_id.clone();
        let acc_to = loop {
            let ind = rand::random::<usize>() % all_accounts.len();
            if all_accounts[ind] != acc_from {
                break all_accounts[ind].clone();
            }
        };

        TransactionBody::send_money(nonce, acc_from.as_str(), acc_to.as_str(), 1)
            .sign(&*signer_from)
    }

    /// Returns transactions that deploy test contract to an every account used by the node.
    pub fn deploy_test_contract(node: &Arc<RwLock<RemoteNode>>) -> Vec<SignedTransaction> {
        let wasm_binary: &[u8] = include_bytes!("../../../tests/hello.wasm");
        let mut res = vec![];
        let mut node = node.write().unwrap();
        for ind in 0..node.signers.len() {
            node.nonces[ind] += 1;
            let nonce = node.nonces[ind];
            let signer = node.signers[ind].clone();
            let contract_id = signer.account_id.clone();
            let t = DeployContractTransaction {
                nonce,
                contract_id,
                wasm_byte_array: wasm_binary.to_vec(),
            };
            res.push(TransactionBody::DeployContract(t).sign(&*signer));
        }
        res
    }
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
