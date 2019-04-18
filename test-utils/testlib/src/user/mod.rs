use node_runtime::state_viewer::{AccountViewCallResult, ViewStateResult};
use primitives::hash::CryptoHash;
use primitives::transaction::{
    FinalTransactionResult, ReceiptTransaction, SignedTransaction, TransactionResult,
};
use primitives::types::{AccountId, Balance, MerkleHash};
use shard::ReceiptInfo;

pub mod thread_user;
pub use thread_user::ThreadUser;
pub mod rpc_user;
pub use rpc_user::RpcUser;
pub mod runtime_user;
pub use runtime_user::RuntimeUser;
pub mod shard_client_user;
use futures::Future;
use node_http::types::{GetBlocksByIndexRequest, SignedShardBlocksResponse};
pub use shard_client_user::ShardClientUser;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub trait User {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String>;

    fn view_balance(&self, account_id: &AccountId) -> Result<Balance, String> {
        Ok(self.view_account(account_id)?.amount)
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String>;

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String>;

    fn add_receipt(&self, receipt: ReceiptTransaction) -> Result<(), String>;

    fn get_account_nonce(&self, account_id: &AccountId) -> Option<u64>;

    fn get_best_block_index(&self) -> Option<u64>;

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult;

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalTransactionResult;

    fn get_state_root(&self) -> MerkleHash;

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo>;

    fn get_shard_blocks_by_index(
        &self,
        r: GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String>;
}

/// Same as `User` by provides async API that can be used inside tokio.
pub trait AsyncUser: Send + Sync {
    fn view_account(
        &self,
        account_id: &AccountId,
    ) -> Box<dyn Future<Item = AccountViewCallResult, Error = String>>;

    fn view_balance(
        &self,
        account_id: &AccountId,
    ) -> Box<dyn Future<Item = Balance, Error = String>> {
        Box::new(self.view_account(account_id).map(|acc| acc.amount))
    }

    fn view_state(
        &self,
        account_id: &AccountId,
    ) -> Box<dyn Future<Item = ViewStateResult, Error = String>>;

    fn add_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Box<dyn Future<Item = (), Error = String> + Send>;

    fn add_receipt(
        &self,
        receipt: ReceiptTransaction,
    ) -> Box<dyn Future<Item = (), Error = String>>;

    fn get_account_nonce(
        &self,
        account_id: &AccountId,
    ) -> Box<dyn Future<Item = u64, Error = String>>;

    fn get_best_block_index(&self) -> Box<dyn Future<Item = u64, Error = String>>;

    fn get_transaction_result(
        &self,
        hash: &CryptoHash,
    ) -> Box<dyn Future<Item = TransactionResult, Error = String>>;

    fn get_transaction_final_result(
        &self,
        hash: &CryptoHash,
    ) -> Box<Future<Item = FinalTransactionResult, Error = String>>;

    fn get_state_root(&self) -> Box<dyn Future<Item = MerkleHash, Error = String>>;

    fn get_receipt_info(
        &self,
        hash: &CryptoHash,
    ) -> Box<dyn Future<Item = ReceiptInfo, Error = String>>;

    fn get_shard_blocks_by_index(
        &self,
        r: GetBlocksByIndexRequest,
    ) -> Box<dyn Future<Item = SignedShardBlocksResponse, Error = String>>;
}
