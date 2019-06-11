use near_primitives::account::AccessKey;
use near_primitives::crypto::signature::PublicKey;
use near_primitives::rpc::ABCIQueryResponse;
use near_primitives::serialize::BaseDecode;
use near_primitives::types::{AccountId, BlockIndex, MerkleHash};

use crate::state_viewer::{AccountViewCallResult, ViewStateResult};

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
    path: &str,
    data: &[u8],
) -> Result<ABCIQueryResponse, Box<dyn std::error::Error>> {
    let path_parts: Vec<&str> = path.split('/').collect();
    if path_parts.is_empty() {
        return Err("Path must contain at least single token".into());
    }
    match path_parts[0] {
        "account" => match adapter.view_account(state_root, &AccountId::from(path_parts[1])) {
            Ok(r) => Ok(ABCIQueryResponse::account(path, r)),
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
                Ok(result) => Ok(ABCIQueryResponse::result(path, result, logs)),
                Err(err) => Ok(ABCIQueryResponse::result_err(path, err.to_string(), logs)),
            }
        }
        "contract" => match adapter
            .view_state(state_root, &AccountId::from(path_parts[1]))
            .and_then(|r| serde_json::to_string(&r).map_err(|err| err.into()))
        {
            Ok(result) => Ok(ABCIQueryResponse::result(path, result.as_bytes().to_vec(), vec![])),
            Err(err) => Ok(ABCIQueryResponse::result_err(path, err.to_string(), vec![])),
        },
        "access_key" => {
            let result = if path_parts.len() == 2 {
                adapter
                    .view_access_keys(state_root, &AccountId::from(path_parts[1]))
                    .and_then(|r| serde_json::to_string(&r).map_err(|err| err.into()))
            } else {
                adapter
                    .view_access_key(
                        state_root,
                        &AccountId::from(path_parts[1]),
                        &PublicKey::from_base(path_parts[2])?,
                    )
                    .and_then(|r| serde_json::to_string(&r).map_err(|err| err.into()))
            };
            match result {
                Ok(result) => {
                    Ok(ABCIQueryResponse::result(path, result.as_bytes().to_vec(), vec![]))
                }
                Err(err) => Ok(ABCIQueryResponse::result_err(path, err.to_string(), vec![])),
            }
        }
        _ => Err(format!("Unknown path {}", path).into()),
    }
}
