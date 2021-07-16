//! Set of methods that construct transactions of various kind.

use std::sync::{Arc, RwLock};

use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};

use byteorder::ByteOrder;
use byteorder::LittleEndian;

use crate::remote_node::RemoteNode;

use std::mem::size_of;
use std::str::FromStr;

#[derive(Clone, Copy, Debug)]
pub enum TransactionType {
    SendMoney,
    Set,
    HeavyStorageBlock,
}

impl FromStr for TransactionType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "send_money" => Ok(TransactionType::SendMoney),
            "set" => Ok(TransactionType::Set),
            "heavy_storage" => Ok(TransactionType::HeavyStorageBlock),
            _ => Err("no match"),
        }
    }
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

        SignedTransaction::send_money(nonce, acc_from, acc_to, &*signer_from, 1, block_hash)
    }

    /// Returns transactions that deploy test contract to an every account used by the node.
    pub fn deploy_test_contract(node: &Arc<RwLock<RemoteNode>>) -> Vec<SignedTransaction> {
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
                &*signer,
                vec![Action::DeployContract(DeployContractAction {
                    code: near_test_contracts::rs_contract().to_vec(),
                })],
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

        let key = rand::random::<u64>() % 1_000;
        let value = rand::random::<u64>() % 1_000;
        let mut args = [0u8; 2 * size_of::<u64>()];
        LittleEndian::write_u64_into(&[key, value], &mut args);
        SignedTransaction::from_actions(
            nonce,
            acc_from.clone(),
            acc_from,
            &*signer_from,
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "write_key_value".to_string(),
                args: args.to_vec(),
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

        let mut args = [0u8; size_of::<u64>()];
        LittleEndian::write_u64(&mut args, 1000u64);

        SignedTransaction::from_actions(
            nonce,
            acc_from.clone(),
            acc_from,
            &*signer_from,
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "benchmark_storage_10kib".to_string(),
                args: args.to_vec(),
                gas: 1000000000,
                deposit: 1,
            })],
            block_hash,
        )
    }
}
