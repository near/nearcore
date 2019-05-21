use std::sync::RwLock;

use actix::System;
use protobuf::Message;

use near_client::StatusResponse;
use near_jsonrpc::client::{new_client, JsonRpcClient};
use near_primitives::account::AccessKey;
use near_primitives::crypto::signature::PublicKey;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::ReceiptInfo;
use near_primitives::transaction::{
    FinalTransactionResult, ReceiptTransaction, SignedTransaction, TransactionResult,
};
use near_primitives::types::{AccountId, MerkleHash};
use near_protos::signed_transaction as transaction_proto;
use node_runtime::state_viewer::{AccountViewCallResult, ViewStateResult};

use crate::user::{AsyncUser, User};

pub struct RpcUser {
    client: RwLock<JsonRpcClient>,
}

impl RpcUser {
    pub fn new(addr: &str) -> RpcUser {
        RpcUser { client: RwLock::new(new_client(&format!("http://{}", addr))) }
    }

    pub fn get_status(&self) -> Option<StatusResponse> {
        System::new("actix").block_on(self.client.write().unwrap().status()).ok()
    }
}

impl User for RpcUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let response = System::new("actix").block_on(
            self.client.write().unwrap().query(format!("account/{}", account_id), vec![]),
        )?;
        serde_json::from_slice(&response.value).map_err(|err| err.to_string())
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String> {
        // TDDO: implement
        Err("".to_string())
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        let proto: transaction_proto::SignedTransaction = transaction.into();
        let bytes = base64::encode(&proto.write_to_bytes().unwrap());
        let _ = System::new("actix")
            .block_on(self.client.write().unwrap().broadcast_tx_async(bytes))?;
        Ok(())
    }

    fn add_receipt(&self, receipt: ReceiptTransaction) -> Result<(), String> {
        // TDDO: implement
        Err("".to_string())
    }

    fn get_account_nonce(&self, account_id: &String) -> Option<u64> {
        self.view_account(account_id).ok().map(|acc| acc.nonce)
    }

    fn get_best_block_index(&self) -> Option<u64> {
        self.get_status().map(|status| status.sync_info.latest_block_height)
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        // TDDO: implement
        TransactionResult::default()
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalTransactionResult {
        // TDDO: implement
        FinalTransactionResult::default()
    }

    fn get_state_root(&self) -> MerkleHash {
        self.get_status().map(|status| status.sync_info.latest_state_root).unwrap()
    }

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        // TDDO: implement
        None
    }

    fn get_access_key(&self, public_key: &PublicKey) -> Result<Option<AccessKey>, String> {
        // TDDO: implement
        Err("".to_string())
    }
}
