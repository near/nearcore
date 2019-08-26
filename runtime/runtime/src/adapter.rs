use near_crypto::{PublicKey, ReadablePublicKey};
use near_primitives::account::{AccessKey, Account};
use near_primitives::types::{AccountId, BlockIndex, MerkleHash};
use near_primitives::views::{
    AccessKeyInfoView, CallResult, QueryError, QueryResponse, ViewStateResult,
};
use std::convert::TryInto;

/// Adapter for querying runtime.
pub trait ViewRuntimeAdapter {
    fn view_account(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<Account, Box<dyn std::error::Error>>;

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
        prefix: &[u8],
    ) -> Result<ViewStateResult, Box<dyn std::error::Error>>;
}

/// Facade to query given client with <path> + <data> at <block height> with optional merkle prove request.
/// Given implementation only supports latest height, thus ignoring it.
pub fn query_client(
    adapter: &dyn ViewRuntimeAdapter,
    state_root: MerkleHash,
    height: BlockIndex,
    path: &str,
    data: &[u8],
) -> Result<QueryResponse, Box<dyn std::error::Error>> {
    let path_parts: Vec<&str> = path.split('/').collect();
    if path_parts.is_empty() {
        return Err("Path must contain at least single token".into());
    }
    match path_parts[0] {
        "account" => match adapter.view_account(state_root, &AccountId::from(path_parts[1])) {
            Ok(account) => Ok(QueryResponse::ViewAccount(account.into())),
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
        "contract" => match adapter.view_state(state_root, &AccountId::from(path_parts[1]), data) {
            Ok(result) => Ok(QueryResponse::ViewState(result)),
            Err(err) => {
                Ok(QueryResponse::Error(QueryError { error: err.to_string(), logs: vec![] }))
            }
        },
        "access_key" => {
            let result = if path_parts.len() == 2 {
                adapter.view_access_keys(state_root, &AccountId::from(path_parts[1])).map(|r| {
                    QueryResponse::AccessKeyList(
                        r.into_iter()
                            .map(|(public_key, access_key)| AccessKeyInfoView {
                                public_key: public_key.into(),
                                access_key: access_key.into(),
                            })
                            .collect(),
                    )
                })
            } else {
                adapter
                    .view_access_key(
                        state_root,
                        &AccountId::from(path_parts[1]),
                        &ReadablePublicKey::new(path_parts[2]).try_into()?,
                    )
                    .map(|r| QueryResponse::AccessKey(r.map(|access_key| access_key.into())))
            };
            match result {
                Ok(result) => Ok(result),
                Err(err) => {
                    Ok(QueryResponse::Error(QueryError { error: err.to_string(), logs: vec![] }))
                }
            }
        }
        _ => Err(format!("Unknown path {}", path).into()),
    }
}
