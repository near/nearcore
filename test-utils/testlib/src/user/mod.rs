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

    // this should not be implemented for RpcUser
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
