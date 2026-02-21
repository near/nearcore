use std::collections::HashMap;

use crate::errors::ErrorKind;
use futures::StreamExt;
use near_async::messaging::CanSendAsync;
use near_async::multithread::MultithreadRuntimeHandle;
use near_client::{Query, ViewClientActor};
use near_client_primitives::types::QueryError;
use near_crypto::PublicKey;
use near_primitives::types::{AccountId, Balance, BlockReference};
use near_primitives::views::{AccessKeyPermissionView, QueryRequest, QueryResponseKind};

/// Map from account ID to per-key gas key balances.
pub(crate) struct GasKeyInfo(HashMap<AccountId, AccountGasKeysBalance>);
pub(crate) type AccountGasKeysBalance = HashMap<PublicKey, Balance>;

impl GasKeyInfo {
    pub(crate) fn empty() -> Self {
        Self(Default::default())
    }

    pub(crate) fn from_entries(
        entries: impl IntoIterator<Item = (AccountId, AccountGasKeysBalance)>,
    ) -> Self {
        Self(entries.into_iter().collect())
    }

    pub(crate) fn into_inner(self) -> HashMap<AccountId, AccountGasKeysBalance> {
        self.0
    }

    /// Query gas key info for multiple accounts in parallel.
    pub(crate) async fn query(
        block_id: BlockReference,
        account_ids: impl Iterator<Item = AccountId>,
        view_client_addr: MultithreadRuntimeHandle<ViewClientActor>,
    ) -> Result<Self, ErrorKind> {
        let map = futures::stream::iter(account_ids)
            .map(move |account_id| {
                let block_id = block_id.clone();
                let view_client_addr = view_client_addr.clone();
                async move {
                    let info =
                        query_gas_key_info(block_id, account_id.clone(), &view_client_addr).await?;
                    Ok((account_id, info))
                }
            })
            .buffer_unordered(10)
            .collect::<Vec<Result<_, ErrorKind>>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        Ok(Self(map))
    }
}

/// Query aggregate gas key balance for an account at a given block.
pub(crate) async fn query_gas_key_balance(
    block_id: BlockReference,
    account_id: AccountId,
    view_client_addr: &MultithreadRuntimeHandle<ViewClientActor>,
) -> Result<Balance, ErrorKind> {
    let keys = query_gas_key_info(block_id, account_id, view_client_addr).await?;
    Ok(keys.values().fold(Balance::ZERO, |sum, b| sum.checked_add(*b).unwrap_or(Balance::MAX)))
}

/// Query gas key balance info (with per-key breakdown) for an account at a given block.
async fn query_gas_key_info(
    block_id: BlockReference,
    account_id: AccountId,
    view_client_addr: &MultithreadRuntimeHandle<ViewClientActor>,
) -> Result<AccountGasKeysBalance, ErrorKind> {
    let query =
        Query::new(block_id, QueryRequest::ViewAccessKeyList { account_id: account_id.clone() });
    let response = match view_client_addr.send_async(query).await? {
        Ok(r) => r,
        Err(QueryError::UnknownAccount { .. }) => return Ok(HashMap::new()),
        Err(err) => return Err(ErrorKind::InternalError(err.to_string())),
    };
    match response.kind {
        QueryResponseKind::AccessKeyList(list) => {
            let mut info = HashMap::new();
            for key_info in &list.keys {
                let balance = match &key_info.access_key.permission {
                    AccessKeyPermissionView::GasKeyFunctionCall { balance, .. }
                    | AccessKeyPermissionView::GasKeyFullAccess { balance, .. } => *balance,
                    _ => continue,
                };
                info.insert(key_info.public_key.clone(), balance);
            }
            Ok(info)
        }
        _ => Err(ErrorKind::InternalInvariantError(format!(
            "queried ViewAccessKeyList, but received {:?}.",
            response.kind
        ))),
    }
}
