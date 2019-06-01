use std::sync::RwLock;

use actix::System;
use protobuf::Message;

use near_chain::Block;
use near_client::StatusResponse;
use near_jsonrpc::client::{new_client, JsonRpcClient};
use near_primitives::account::AccessKey;
use near_primitives::crypto::signature::PublicKey;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::ReceiptInfo;
use near_primitives::serialize::{to_base, BaseEncode};
use near_primitives::transaction::{
    FinalTransactionResult, ReceiptTransaction, SignedTransaction, TransactionResult,
};
use near_primitives::types::{AccountId, MerkleHash};
use near_protos::signed_transaction as transaction_proto;
use node_runtime::state_viewer::{AccountViewCallResult, ViewStateResult};

use crate::user::User;

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

    pub fn query<T: serde::de::DeserializeOwned>(
        &self,
        path: String,
        data: Vec<u8>,
    ) -> Result<T, String> {
        let response =
            System::new("actix").block_on(self.client.write().unwrap().query(path, to_base(&data)))?;
        serde_json::from_slice(&response.value).map_err(|err| err.to_string())
    }
}

impl User for RpcUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        self.query(format!("account/{}", account_id), vec![])
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String> {
        self.query(format!("contract/{}", account_id), vec![])
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        let proto: transaction_proto::SignedTransaction = transaction.into();
        let bytes = to_base(&proto.write_to_bytes().unwrap());
        let _ = System::new("actix")
            .block_on(self.client.write().unwrap().broadcast_tx_async(bytes))?;
        Ok(())
    }

    fn commit_transaction(&self, transaction: SignedTransaction) -> Result<FinalTransactionResult, String> {
        let proto: transaction_proto::SignedTransaction = transaction.into();
        let bytes = to_base(&proto.write_to_bytes().unwrap());
        System::new("actix")
            .block_on(self.client.write().unwrap().broadcast_tx_commit(bytes))
    }

    fn add_receipt(&self, _receipt: ReceiptTransaction) -> Result<(), String> {
        // TDDO: figure out if rpc will support this
        unimplemented!()
    }

    fn get_account_nonce(&self, account_id: &String) -> Option<u64> {
        self.view_account(account_id).ok().map(|acc| acc.nonce)
    }

    fn get_best_block_index(&self) -> Option<u64> {
        self.get_status().map(|status| status.sync_info.latest_block_height)
    }

    fn get_block(&self, index: u64) -> Option<Block> {
        System::new("actix").block_on(self.client.write().unwrap().block(index)).ok()
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        System::new("actix").block_on(self.client.write().unwrap().tx_details(hash.into())).unwrap()
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalTransactionResult {
        System::new("actix").block_on(self.client.write().unwrap().tx(hash.into())).unwrap()
    }

    fn get_state_root(&self) -> MerkleHash {
        self.get_status().map(|status| status.sync_info.latest_state_root).unwrap()
    }

    fn get_receipt_info(&self, _hash: &CryptoHash) -> Option<ReceiptInfo> {
        // TDDO: figure out if rpc will support this
        unimplemented!()
    }

    fn get_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Option<AccessKey>, String> {
        self.query(format!("access_key/{}/{}", account_id, public_key.to_base()), vec![])
    }
}
