use crate::errors::ErrorKind;
use futures::StreamExt;
use near_async::messaging::CanSendAsync;
use near_async::multithread::MultithreadRuntimeHandle;
use near_client::{Query, ViewClientActor};
use near_client_primitives::types::QueryError;
use near_crypto::PublicKeyHandle;
use near_primitives::types::{AccountId, Balance, BlockReference};
use near_primitives::views::{AccessKeyPermissionView, QueryRequest, QueryResponseKind};
use std::collections::HashMap;
use std::num::NonZeroU32;

// Access-key list page size; the node clamps it to its own limit.
const ACCESS_KEY_PAGE_SIZE: Option<NonZeroU32> = NonZeroU32::new(1000);

/// Map from account ID to per-key gas key balances.
pub(crate) struct GasKeyInfo(HashMap<AccountId, AccountGasKeysBalance>);
pub(crate) type AccountGasKeysBalance = HashMap<PublicKeyHandle, Balance>;

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
    // An account may hold more gas keys than a single `view_access_key_list`
    // page returns, so walk the pages following `last_key` until the listing is
    // exhausted
    let mut info = HashMap::new();
    let mut after_key = None;
    loop {
        let query = Query::new(
            block_id.clone(),
            QueryRequest::ViewAccessKeyList {
                account_id: account_id.clone(),
                after_key,
                limit: ACCESS_KEY_PAGE_SIZE,
            },
        );
        let response = match view_client_addr.send_async(query).await? {
            Ok(r) => r,
            Err(QueryError::UnknownAccount { .. }) => return Ok(HashMap::new()),
            Err(err) => return Err(ErrorKind::InternalError(err.to_string())),
        };
        let list = match response.kind {
            QueryResponseKind::AccessKeyList(list) => list,
            _ => {
                return Err(ErrorKind::InternalInvariantError(format!(
                    "queried ViewAccessKeyList, but received {:?}.",
                    response.kind
                )));
            }
        };
        for key_info in &list.keys {
            let balance = match &key_info.access_key.permission {
                AccessKeyPermissionView::GasKeyFunctionCall { balance, .. }
                | AccessKeyPermissionView::GasKeyFullAccess { balance, .. } => *balance,
                _ => continue,
            };
            info.insert(key_info.public_key.clone(), balance);
        }
        match list.last_key {
            Some(cursor) => after_key = Some(cursor),
            None => return Ok(info),
        }
    }
}
