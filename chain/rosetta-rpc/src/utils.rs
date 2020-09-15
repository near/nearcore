use actix::Addr;
use futures::StreamExt;

use near_client::ViewClientActor;

#[derive(Debug, Clone)]
pub(crate) struct SignedDiff<T>
where
    T: std::ops::Sub<Output = T> + std::cmp::PartialOrd + std::fmt::Display,
{
    is_positive: bool,
    absolute_difference: T,
}

impl<T> SignedDiff<T>
where
    T: std::ops::Sub<Output = T> + std::cmp::Ord + std::fmt::Display,
{
    pub fn cmp(lhs: T, rhs: T) -> Self {
        if lhs <= rhs {
            Self { is_positive: true, absolute_difference: rhs - lhs }
        } else {
            Self { is_positive: false, absolute_difference: lhs - rhs }
        }
    }
}

impl<T> std::fmt::Display for SignedDiff<T>
where
    T: std::ops::Sub<Output = T> + std::cmp::Ord + std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", if self.is_positive { "" } else { "-" }, self.absolute_difference)
    }
}

impl<T> std::ops::Neg for SignedDiff<T>
where
    T: std::ops::Sub<Output = T> + std::cmp::Ord + std::fmt::Display,
{
    type Output = Self;

    fn neg(mut self) -> Self::Output {
        self.is_positive = !self.is_positive;
        self
    }
}

fn get_liquid_balance_for_storage(
    mut account: near_primitives::account::Account,
    runtime_config: &near_runtime_configs::RuntimeConfig,
) -> near_primitives::types::Balance {
    account.amount = 0;
    near_runtime_configs::get_insufficient_storage_stake(&account, &runtime_config)
        .expect("get_insufficient_storage_stake never fails when state is consistent")
        .unwrap_or(0)
}

pub(crate) struct RosettaAccountBalances {
    pub liquid: near_primitives::types::Balance,
    pub liquid_for_storage: near_primitives::types::Balance,
    pub locked: near_primitives::types::Balance,
}

impl RosettaAccountBalances {
    pub fn zero() -> Self {
        Self { liquid: 0, liquid_for_storage: 0, locked: 0 }
    }

    pub fn from_account<T: Into<near_primitives::account::Account>>(
        account: T,
        runtime_config: &near_runtime_configs::RuntimeConfig,
    ) -> Self {
        let account = account.into();
        let amount = account.amount;
        let locked = account.locked;
        let liquid_for_storage = get_liquid_balance_for_storage(account, runtime_config);

        Self {
            liquid_for_storage,
            liquid: amount
                .checked_sub(liquid_for_storage)
                .expect("liquid balance for storage cannot be bigger than the total balance"),
            locked,
        }
    }
}

pub(crate) async fn query_accounts(
    account_ids: impl Iterator<Item = &near_primitives::types::AccountId>,
    block_id: &near_primitives::types::BlockReference,
    view_client_addr: &Addr<ViewClientActor>,
) -> Result<
    std::collections::HashMap<
        near_primitives::types::AccountId,
        near_primitives::views::AccountView,
    >,
    crate::errors::ErrorKind,
> {
    account_ids
        .map(|account_id| {
            async move {
                let query = near_client::Query::new(
                    block_id.clone(),
                    near_primitives::views::QueryRequest::ViewAccount {
                        account_id: account_id.clone(),
                    },
                );
                let account_info_response =
                    tokio::time::timeout(std::time::Duration::from_secs(10), async {
                        loop {
                            match view_client_addr.send(query.clone()).await? {
                                Ok(Some(query_response)) => return Ok(Some(query_response)),
                                Ok(None) => {}
                                // TODO: update this once we return structured errors in the
                                // view_client handlers
                                Err(err) => {
                                    if err.contains("does not exist") {
                                        return Ok(None);
                                    }
                                    return Err(crate::errors::ErrorKind::InternalError(err));
                                }
                            }
                            tokio::time::delay_for(std::time::Duration::from_millis(100)).await;
                        }
                    })
                    .await??;

                let kind = if let Some(account_info_response) = account_info_response {
                    account_info_response.kind
                } else {
                    return Ok(None);
                };

                match kind {
                    near_primitives::views::QueryResponseKind::ViewAccount(account_info) => {
                        Ok(Some((account_id.clone(), account_info)))
                    }
                    _ => Err(crate::errors::ErrorKind::InternalInvariantError(
                        "queried ViewAccount, but received something else.".to_string(),
                    )
                    .into()),
                }
            }
        })
        .collect::<futures::stream::FuturesUnordered<_>>()
        .collect::<Vec<
            Result<
                Option<(near_primitives::types::AccountId, near_primitives::views::AccountView)>,
                crate::errors::ErrorKind,
            >,
        >>()
        .await
        .into_iter()
        .filter_map(|account_info| account_info.transpose())
        .collect()
}
