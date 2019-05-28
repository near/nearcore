use crate::state_viewer::AccountViewCallResult;
use primitives::account::AccessKey;
use primitives::crypto::signature::PublicKey;
use primitives::rpc::ABCIQueryResponse;
use primitives::types::AccountId;
use std::convert::TryFrom;

/// Adapter for querying runtime.
pub trait RuntimeAdapter {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String>;
    fn call_function(
        &self,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, String>;
    fn view_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Option<AccessKey>, String>;

    fn view_access_keys(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<(PublicKey, AccessKey)>, String>;

    //    fn view_access_key(&self, account_id: &AccountId) -> Result<Vec<PublicKey>, String>;
}

/// Facade to query given client with <path> + <data> at <block height> with optional merkle prove request.
/// Given implementation only supports latest height, thus ignoring it.
pub fn query_client(
    adapter: &RuntimeAdapter,
    path: &str,
    data: &[u8],
    _height: u64,
    _prove: bool,
) -> Result<ABCIQueryResponse, String> {
    let path_parts: Vec<&str> = path.split('/').collect();
    if path_parts.is_empty() {
        return Err("Path must contain at least single token".to_string());
    }
    match path_parts[0] {
        "account" => match adapter.view_account(&AccountId::from(path_parts[1])) {
            Ok(r) => Ok(ABCIQueryResponse::account(path, r)),
            Err(e) => Err(e),
        },
        "call" => {
            let mut logs = vec![];
            match adapter.call_function(
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
            let result = if path_parts.len() == 2 {
                adapter
                    .view_access_keys(&AccountId::from(path_parts[1]))
                    .and_then(|r| serde_json::to_string(&r).map_err(|e| format!("{}", e)))
            } else {
                adapter
                    .view_access_key(
                        &AccountId::from(path_parts[1]),
                        &PublicKey::try_from(path_parts[2])?,
                    )
                    .and_then(|r| serde_json::to_string(&r).map_err(|e| format!("{}", e)))
            };
            match result {
                Ok(result) => {
                    Ok(ABCIQueryResponse::result(path, result.as_bytes().to_vec(), vec![]))
                }
                Err(e) => Ok(ABCIQueryResponse::result_err(path, e, vec![])),
            }
        }
        _ => Err(format!("Unknown path {}", path)),
    }
}
