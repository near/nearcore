use node_runtime::state_viewer::{AccountViewCallResult, ViewStateResult};
use primitives::hash::CryptoHash;
use primitives::transaction::{ReceiptTransaction, SignedTransaction, TransactionResult};
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

    fn get_state_root(&self) -> MerkleHash;

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo>;

    fn get_shard_blocks_by_index(
        &self,
        r: GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String>;
}

pub trait AsyncUser {
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
    ) -> Box<dyn Future<Item = (), Error = String>>;

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

/// Whenever we have async implementation of the user we can cheaply create a synchronous
/// version of it, by wrapping it into `AsyncUserWrapper`. In some cases, synchronous and asynchronous
/// implementations might be different.
pub struct AsyncUserWrapper {
    async_user: Box<dyn AsyncUser>,
}

impl AsyncUserWrapper {
    pub fn new(async_user: Box<dyn AsyncUser>) -> Box<dyn User> {
        Box::new(Self { async_user })
    }
}

impl User for AsyncUserWrapper {
    fn view_account(&self, account_id: &String) -> Result<AccountViewCallResult, String> {
        self.async_user.view_account(account_id).wait()
    }

    fn view_state(&self, account_id: &String) -> Result<ViewStateResult, String> {
        self.async_user.view_state(account_id).wait()
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        self.async_user.add_transaction(transaction).wait()
    }

    fn add_receipt(&self, receipt: ReceiptTransaction) -> Result<(), String> {
        self.async_user.add_receipt(receipt).wait()
    }

    fn get_account_nonce(&self, account_id: &String) -> Option<u64> {
        self.async_user.get_account_nonce(account_id).wait().ok()
    }

    fn get_best_block_index(&self) -> Option<u64> {
        self.async_user.get_best_block_index().wait().ok()
    }

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        self.async_user.get_transaction_result(hash).wait().unwrap()
    }

    fn get_state_root(&self) -> CryptoHash {
        self.async_user.get_state_root().wait().unwrap()
    }

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        self.async_user.get_receipt_info(hash).wait().ok()
    }

    fn get_shard_blocks_by_index(
        &self,
        r: GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String> {
        self.async_user.get_shard_blocks_by_index(r).wait()
    }
}
