use near_primitives::crypto::signature::PublicKey;
use near_primitives::rpc::ABCIQueryResponse;
use near_primitives::types::{AccountId, BlockIndex, MerkleHash};

use crate::state_viewer::AccountViewCallResult;

/// Adapter for querying runtime.
pub trait RuntimeAdapter {
    fn view_account(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<AccountViewCallResult, String>;

    fn call_function(
        &self,
        state_root: MerkleHash,
        height: BlockIndex,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, String>;

    fn view_access_key(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Vec<PublicKey>, String>;
}

/// Facade to query given client with <path> + <data> at <block height> with optional merkle prove request.
/// Given implementation only supports latest height, thus ignoring it.
pub fn query_client(
    adapter: &RuntimeAdapter,
    state_root: MerkleHash,
    height: BlockIndex,
    path: &str,
    data: &[u8],
) -> Result<ABCIQueryResponse, String> {
    let path_parts: Vec<&str> = path.split('/').collect();
    if path_parts.is_empty() {
        return Err("Path must contain at least single token".to_string());
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
                Err(e) => Ok(ABCIQueryResponse::result_err(path, e, logs)),
            }
        }
        "access_key" => {
            match adapter.view_access_key(state_root, &AccountId::from(path_parts[1])) {
                Ok(keys) => Ok(ABCIQueryResponse::result(
                    path,
                    serde_json::to_string(&keys).map_err(|e| format!("{}", e))?.as_bytes().to_vec(),
                    vec![],
                )),
                Err(e) => Ok(ABCIQueryResponse::result_err(path, e, vec![])),
            }
        }
        _ => Err(format!("Unknown path {}", path)),
    }
}
