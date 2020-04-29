use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::types::{AccountId, BlockHeight, EpochHeight, MerkleHash, ShardId};
use near_primitives::views::ViewStateResult;

/// Adapter for querying runtime.
pub trait ViewRuntimeAdapter {
    fn view_account(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Account, Box<dyn std::error::Error>>;

    fn call_function(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        height: BlockHeight,
        block_timestamp: u64,
        epoch_height: EpochHeight,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>>;

    fn view_access_key(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKey, Box<dyn std::error::Error>>;

    fn view_access_keys(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Vec<(PublicKey, AccessKey)>, Box<dyn std::error::Error>>;

    fn view_state(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
        prefix: &[u8],
    ) -> Result<ViewStateResult, Box<dyn std::error::Error>>;
}
