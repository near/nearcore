use near_primitives::account::AccessKey;
use near_primitives::crypto::signature::PublicKey;
use near_primitives::rpc::{
    AccountViewCallResult, CallResult, QueryError, QueryResponse, ViewStateResult,
};
use near_primitives::serialize::BaseDecode;
use near_primitives::types::{AccountId, BlockIndex, MerkleHash};

/// Adapter for querying runtime.
pub trait RuntimeAdapter {
    fn view_account(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<AccountViewCallResult, Box<dyn std::error::Error>>;

    fn call_function(
        &self,
        state_root: MerkleHash,
        height: BlockIndex,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>>;

    fn view_access_key(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Option<AccessKey>, Box<dyn std::error::Error>>;

    fn view_access_keys(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Vec<(PublicKey, AccessKey)>, Box<dyn std::error::Error>>;

    fn view_state(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<ViewStateResult, Box<dyn std::error::Error>>;
}

/// Facade to query given client with <path> + <data> at <block height> with optional merkle prove request.
/// Given implementation only supports latest height, thus ignoring it.
pub fn query_client(
    adapter: &dyn RuntimeAdapter,
    state_root: MerkleHash,
    height: BlockIndex,
    path_parts: Vec<&str>,
    data: &[u8],
) -> Result<QueryResponse, Box<dyn std::error::Error>> {
    if path_parts.is_empty() {
        return Err("Path must contain at least single token".into());
    }
    debug!("PATH[0] = {}", path_parts[0]);
    match path_parts[0] {
        "account" => match adapter.view_account(state_root, &AccountId::from(path_parts[1])) {
            Ok(r) => Ok(QueryResponse::ViewAccount(r)),
            Err(e) => Err(e),
        },
        "call" => {
            let mut logs = vec![];
            match adapter.call_function(
                state_root,
                height,
                &AccountId::from(path_parts[1]),
                path_parts[2],
                &data,
                &mut logs,
            ) {
                Ok(result) => Ok(QueryResponse::CallResult(CallResult { result, logs })),
                Err(err) => Ok(QueryResponse::Error(QueryError { error: err.to_string(), logs })),
            }
        }
        "contract" => match adapter.view_state(state_root, &AccountId::from(path_parts[1])) {
            Ok(result) => Ok(QueryResponse::ViewState(result)),
            Err(err) => {
                Ok(QueryResponse::Error(QueryError { error: err.to_string(), logs: vec![] }))
            }
        },
        "access_key" => {
            let result = if path_parts.len() == 2 {
                adapter
                    .view_access_keys(state_root, &AccountId::from(path_parts[1]))
                    .map(|r| QueryResponse::AccessKeyList(r))
            } else {
                adapter
                    .view_access_key(
                        state_root,
                        &AccountId::from(path_parts[1]),
                        &PublicKey::from_base(path_parts[2])?,
                    )
                    .map(|r| QueryResponse::AccessKey(r))
            };
            match result {
                Ok(result) => Ok(result),
                Err(err) => {
                    Ok(QueryResponse::Error(QueryError { error: err.to_string(), logs: vec![] }))
                }
            }
        }
        _ => Err(format!("Unknown path {}", path_parts[0]).into()),
    }
}
