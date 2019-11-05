use std::convert::TryInto;
use std::sync::{Arc, RwLock};

use actix::System;
use borsh::BorshSerialize;

use near_client::StatusResponse;
use near_crypto::{PublicKey, Signer};
use near_jsonrpc::client::{new_client, JsonRpcClient};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::serialize::{to_base, to_base64};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::views::{
    AccessKeyView, AccountView, BlockView, ExecutionOutcomeView, FinalExecutionOutcomeView,
    QueryResponse, ViewStateResult,
};

use crate::user::User;
use near_jsonrpc_client::BlockId;

pub struct RpcUser {
    account_id: AccountId,
    signer: Arc<dyn Signer>,
    client: RwLock<JsonRpcClient>,
}

impl RpcUser {
    pub fn new(addr: &str, account_id: AccountId, signer: Arc<dyn Signer>) -> RpcUser {
        RpcUser { account_id, client: RwLock::new(new_client(&format!("http://{}", addr))), signer }
    }

    pub fn get_status(&self) -> Option<StatusResponse> {
        System::new("actix").block_on(self.client.write().unwrap().status()).ok()
    }

    pub fn query(&self, path: String, data: &[u8]) -> Result<QueryResponse, String> {
        System::new("actix").block_on(self.client.write().unwrap().query(path, to_base(data)))
    }
}

impl User for RpcUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountView, String> {
        self.query(format!("account/{}", account_id), &[])?.try_into()
    }

    fn view_state(&self, account_id: &AccountId, prefix: &[u8]) -> Result<ViewStateResult, String> {
        self.query(format!("contract/{}", account_id), prefix)?.try_into()
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        let bytes = transaction.try_to_vec().unwrap();
        let _ = System::new("actix")
            .block_on(self.client.write().unwrap().broadcast_tx_async(to_base64(&bytes)))?;
        Ok(())
    }

    fn commit_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<FinalExecutionOutcomeView, String> {
        let bytes = transaction.try_to_vec().unwrap();
        System::new("actix")
            .block_on(self.client.write().unwrap().broadcast_tx_commit(to_base64(&bytes)))
    }

    fn add_receipt(&self, _receipt: Receipt) -> Result<(), String> {
        // TDDO: figure out if rpc will support this
        unimplemented!()
    }

    fn get_best_block_index(&self) -> Option<u64> {
        self.get_status().map(|status| status.sync_info.latest_block_height)
    }

    fn get_best_block_hash(&self) -> Option<CryptoHash> {
        self.get_status().map(|status| status.sync_info.latest_block_hash.into())
    }

    fn get_block(&self, index: u64) -> Option<BlockView> {
        System::new("actix")
            .block_on(self.client.write().unwrap().block(BlockId::Height(index)))
            .ok()
    }

    fn get_transaction_result(&self, _hash: &CryptoHash) -> ExecutionOutcomeView {
        unimplemented!()
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalExecutionOutcomeView {
        let account_id = self.account_id.clone();
        System::new("actix")
            .block_on(self.client.write().unwrap().tx(hash.into(), account_id))
            .unwrap()
    }

    fn get_state_root(&self) -> CryptoHash {
        self.get_status().map(|status| status.sync_info.latest_state_root).unwrap()
    }

    fn get_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Option<AccessKeyView>, String> {
        self.query(format!("access_key/{}/{}", account_id, public_key), &[])?.try_into()
    }

    fn signer(&self) -> Arc<dyn Signer> {
        self.signer.clone()
    }

    fn set_signer(&mut self, signer: Arc<dyn Signer>) {
        self.signer = signer;
    }
}
