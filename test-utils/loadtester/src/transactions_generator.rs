//! Set of methods that construct transactions of various kind.

use std::sync::{Arc, RwLock};

use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
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
        let (signer_from, nonce, block_hash) = {
            let mut node = node.write().unwrap();
            node.nonces[signer_ind] += 1;
            let block_hash = node.get_current_block_hash().unwrap();
            (node.signers[signer_ind].clone(), node.nonces[signer_ind], block_hash)
        };

        let acc_from = signer_from.account_id.clone();
        let acc_to = loop {
            let ind = rand::random::<usize>() % all_accounts.len();
            if all_accounts[ind] != acc_from {
                break all_accounts[ind].clone();
            }
        };

        SignedTransaction::send_money(nonce, acc_from, acc_to, signer_from.clone(), 1, block_hash)
    }

    /// Returns transactions that deploy test contract to an every account used by the node.
    pub fn deploy_test_contract(node: &Arc<RwLock<RemoteNode>>) -> Vec<SignedTransaction> {
        let wasm_binary: &[u8] =
            include_bytes!("../../../runtime/near-vm-runner/tests/res/test_contract_rs.wasm");
        let mut res = vec![];
        let mut node = node.write().unwrap();
        for ind in 0..node.signers.len() {
            node.nonces[ind] += 1;
            let nonce = node.nonces[ind];
            let signer = node.signers[ind].clone();
            let contract_id = signer.account_id.clone();
            let block_hash = node.get_current_block_hash().unwrap();

            res.push(SignedTransaction::from_actions(
                nonce,
                contract_id.clone(),
                contract_id,
                signer.clone(),
                vec![Action::DeployContract(DeployContractAction { code: wasm_binary.to_vec() })],
                block_hash,
            ));
        }
        res
    }

    /// Create set key/value transaction.
    pub fn call_set(node: &Arc<RwLock<RemoteNode>>, signer_ind: usize) -> SignedTransaction {
        let (signer_from, nonce, block_hash) = {
            let mut node = node.write().unwrap();
            node.nonces[signer_ind] += 1;
            (
                node.signers[signer_ind].clone(),
                node.nonces[signer_ind],
                node.get_current_block_hash().unwrap(),
            )
        };
        let acc_from = signer_from.account_id.clone();

        let key = rand::random::<usize>() % 1_000;
        let value = rand::random::<usize>() % 1_000;
        SignedTransaction::from_actions(
            nonce,
            acc_from.clone(),
            acc_from,
            signer_from.clone(),
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "setKeyValue".to_string(),
                args: format!("{{\"key\":\"{}\", \"value\":\"{}\"}}", key, value)
                    .as_bytes()
                    .to_vec(),
                gas: 100000,
                deposit: 1,
            })],
            block_hash,
        )
    }

    /// Returns a transaction that calls `heavy_storage_blocks` on a contract.
    pub fn call_heavy_storage_blocks(
        node: &Arc<RwLock<RemoteNode>>,
        signer_ind: usize,
    ) -> SignedTransaction {
        let (signer_from, nonce, block_hash) = {
            let mut node = node.write().unwrap();
            node.nonces[signer_ind] += 1;
            (
                node.signers[signer_ind].clone(),
                node.nonces[signer_ind],
                node.get_current_block_hash().unwrap(),
            )
        };
        let acc_from = signer_from.account_id.clone();

        SignedTransaction::from_actions(
            nonce,
            acc_from.clone(),
            acc_from,
            signer_from.clone(),
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "heavy_storage_blocks".to_string(),
                args: "{\"n\":1000}".as_bytes().to_vec(),
                gas: 100000,
                deposit: 1,
            })],
            block_hash,
        )
    }
}
