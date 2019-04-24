use crate::state_viewer::AccountViewCallResult;
use primitives::rpc::ABCIQueryResponse;
use primitives::types::AccountId;

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
        _ => Err(format!("Unknown path {}", path)),
    }
}
