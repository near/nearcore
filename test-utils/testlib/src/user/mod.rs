use node_runtime::state_viewer::AccountViewCallResult;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, Balance};

pub mod thread_user;
pub use thread_user::ThreadUser;
pub mod rpc_user;
pub use rpc_user::RpcUser;
use node_http::types::{GetBlocksByIndexRequest, SignedShardBlocksResponse};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub trait User {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String>;

    fn view_balance(&self, account_id: &AccountId) -> Result<Balance, String> {
        Ok(self.view_account(account_id)?.amount)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String>;

    fn get_account_nonce(&self, account_id: &AccountId) -> Option<u64>;

    fn get_best_block_index(&self) -> Option<u64>;

    fn get_shard_blocks_by_index(
        &self,
        r: GetBlocksByIndexRequest,
    ) -> Result<SignedShardBlocksResponse, String>;
}


