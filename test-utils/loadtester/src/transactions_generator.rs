//! Set of methods that construct transactions of various kind.

use std::sync::{Arc, RwLock};

use near_primitives::transaction::{
    DeployContractTransaction, FunctionCallTransaction, SignedTransaction, TransactionBody,
};

use crate::remote_node::RemoteNode;

#[derive(Clone, Copy)]
pub enum TransactionType {
    SendMoney,
    Set,
    HeavyStorageBlock,
}

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

    /// Create set key/value transaction.
    pub fn call_set(node: &Arc<RwLock<RemoteNode>>, signer_ind: usize) -> SignedTransaction {
        let (signer_from, nonce) = {
            let mut node = node.write().unwrap();
            node.nonces[signer_ind] += 1;
            (node.signers[signer_ind].clone(), node.nonces[signer_ind])
        };
        let acc_from = signer_from.account_id.clone();

        let key = rand::random::<usize>() % 1_000;
        let value = rand::random::<usize>() % 1_000;
        let t = FunctionCallTransaction {
            nonce,
            originator_id: acc_from.clone(),
            contract_id: acc_from,
            method_name: b"setKeyValue".to_vec(),
            args: format!("{{\"key\":\"{}\", \"value\":\"{}\"}}", key, value).as_bytes().to_vec(),
            amount: 1,
        };
        TransactionBody::FunctionCall(t).sign(&*signer_from)
    }

    /// Returns a transaction that calls `heavy_storage_blocks` on a contract.
    pub fn call_heavy_storage_blocks(
        node: &Arc<RwLock<RemoteNode>>,
        signer_ind: usize,
    ) -> SignedTransaction {
        let (signer_from, nonce) = {
            let mut node = node.write().unwrap();
            node.nonces[signer_ind] += 1;
            (node.signers[signer_ind].clone(), node.nonces[signer_ind])
        };
        let acc_from = signer_from.account_id.clone();

        let t = FunctionCallTransaction {
            nonce,
            originator_id: acc_from.clone(),
            contract_id: acc_from,
            method_name: b"heavy_storage_blocks".to_vec(),
            args: "{\"n\":1000}".as_bytes().to_vec(),
            amount: 1,
        };
        TransactionBody::FunctionCall(t).sign(&*signer_from)
    }
}
