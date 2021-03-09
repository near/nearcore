use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    AccountId, BlockHeight, EpochHeight, EpochId, EpochInfoProvider, MerkleHash, ShardId,
};
use near_primitives::version::ProtocolVersion;
use near_primitives::views::ViewStateResult;

/// Adapter for querying runtime.
pub trait ViewRuntimeAdapter {
    fn view_account(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Account, crate::state_viewer::errors::ViewAccountError>;

    fn view_contract_code(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<ContractCode, crate::state_viewer::errors::ViewContractCodeError>;

    fn call_function(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        height: BlockHeight,
        block_timestamp: u64,
        last_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        epoch_height: EpochHeight,
        epoch_id: &EpochId,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
        epoch_info_provider: &dyn EpochInfoProvider,
        current_protocol_version: ProtocolVersion,
        #[cfg(feature = "protocol_feature_evm")] evm_chain_id: u64,
    ) -> Result<Vec<u8>, crate::state_viewer::errors::CallFunctionError>;

    fn view_access_key(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKey, crate::state_viewer::errors::ViewAccessKeyError>;

    fn view_access_keys(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Vec<(PublicKey, AccessKey)>, crate::state_viewer::errors::ViewAccessKeyError>;

    fn view_state(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        account_id: &AccountId,
        prefix: &[u8],
    ) -> Result<ViewStateResult, crate::state_viewer::errors::ViewStateError>;
}
