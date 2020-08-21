use std::convert::TryInto;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use actix::System;
use borsh::BorshSerialize;
use futures::{Future, TryFutureExt};

use near_client::StatusResponse;
use near_crypto::{PublicKey, Signer};
use near_jsonrpc::client::{new_client, JsonRpcClient};
use near_jsonrpc::ServerError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::serialize::{to_base, to_base64};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight, BlockId, BlockReference, MaybeBlockId};
use near_primitives::views::{
    AccessKeyView, AccountView, BlockView, EpochValidatorInfo, ExecutionOutcomeView,
    FinalExecutionOutcomeView, QueryResponse, ViewStateResult,
};

use crate::user::User;

pub struct RpcUser {
    account_id: AccountId,
    signer: Arc<dyn Signer>,
    addr: String,
}

impl RpcUser {
    fn actix<F, Fut, R>(&self, f: F) -> R
    where
        Fut: Future<Output = R> + 'static,
        F: FnOnce(JsonRpcClient) -> Fut + 'static,
    {
        let addr = self.addr.clone();
        System::new("actix")
            .block_on(async move { f(new_client(&format!("http://{}", addr))).await })
    }

    pub fn new(addr: &str, account_id: AccountId, signer: Arc<dyn Signer>) -> RpcUser {
        RpcUser { account_id, addr: addr.to_owned(), signer }
    }

    pub fn get_status(&self) -> Option<StatusResponse> {
        self.actix(|client| client.status()).ok()
    }

    pub fn query(&self, path: String, data: &[u8]) -> Result<QueryResponse, String> {
        let data = to_base(data);
        self.actix(move |client| client.query_by_path(path, data).map_err(|err| err.to_string()))
    }

    pub fn validators(&self, block_id: MaybeBlockId) -> Result<EpochValidatorInfo, String> {
        self.actix(move |client| client.validators(block_id).map_err(|err| err.to_string()))
    }
}

impl User for RpcUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountView, String> {
        self.query(format!("account/{}", account_id), &[])?.try_into()
    }

    fn view_state(&self, account_id: &AccountId, prefix: &[u8]) -> Result<ViewStateResult, String> {
        self.query(format!("contract/{}", account_id), prefix)?.try_into()
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), ServerError> {
        let bytes = transaction.try_to_vec().unwrap();
        let _ = self.actix(move |client| client.broadcast_tx_async(to_base64(&bytes))).map_err(
            |err| {
                serde_json::from_value::<ServerError>(
                    err.data.expect("server error must carry data"),
                )
                .expect("deserialize server error must be ok")
            },
        )?;
        Ok(())
    }

    fn commit_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<FinalExecutionOutcomeView, ServerError> {
        let bytes = transaction.try_to_vec().unwrap();
        let result = self.actix(move |client| client.broadcast_tx_commit(to_base64(&bytes)));
        // Wait for one more block, to make sure all nodes actually apply the state transition.
        let height = self.get_best_height().unwrap();
        while height == self.get_best_height().unwrap() {
            thread::sleep(Duration::from_millis(50));
        }
        match result {
            Ok(outcome) => Ok(outcome),
            Err(err) => Err(serde_json::from_value::<ServerError>(err.data.unwrap()).unwrap()),
        }
    }

    fn add_receipt(&self, _receipt: Receipt) -> Result<(), ServerError> {
        // TDDO: figure out if rpc will support this
        unimplemented!()
    }

    fn get_best_height(&self) -> Option<BlockHeight> {
        self.get_status().map(|status| status.sync_info.latest_block_height)
    }

    fn get_best_block_hash(&self) -> Option<CryptoHash> {
        self.get_status().map(|status| status.sync_info.latest_block_hash.into())
    }

    fn get_block(&self, height: BlockHeight) -> Option<BlockView> {
        self.actix(move |client| client.block(BlockReference::BlockId(BlockId::Height(height))))
            .ok()
    }

    fn get_transaction_result(&self, _hash: &CryptoHash) -> ExecutionOutcomeView {
        unimplemented!()
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalExecutionOutcomeView {
        let account_id = self.account_id.clone();
        let hash = *hash;
        self.actix(move |client| client.tx((&hash).into(), account_id)).unwrap()
    }

    fn get_state_root(&self) -> CryptoHash {
        self.get_status().map(|status| status.sync_info.latest_state_root).unwrap()
    }

    fn get_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKeyView, String> {
        self.query(format!("access_key/{}/{}", account_id, public_key), &[])?.try_into()
    }

    fn signer(&self) -> Arc<dyn Signer> {
        self.signer.clone()
    }

    fn set_signer(&mut self, signer: Arc<dyn Signer>) {
        self.signer = signer;
    }
}
