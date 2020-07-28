use std::convert::TryInto;
use std::sync::Arc;

use actix::Addr;
use futures::StreamExt;

use near_chain_configs::Genesis;
use near_client::ViewClientActor;
use near_primitives::serialize::BaseEncode;

pub(crate) async fn fetch_transactions(
    genesis: Arc<Genesis>,
    view_client_addr: Addr<ViewClientActor>,
    block: &near_primitives::views::BlockView,
) -> Result<Vec<crate::models::Transaction>, crate::models::Error> {
    let state_changes = view_client_addr
        .send(near_client::GetStateChangesInBlock { block_hash: block.header.hash })
        .await?
        .unwrap();

    let touched_account_ids = state_changes
        .into_iter()
        .filter_map(|x| {
            if let near_primitives::views::StateChangeKindView::AccountTouched { account_id } = x {
                Some(account_id)
            } else {
                None
            }
        })
        .collect::<std::collections::HashSet<_>>();

    let prev_block_id = near_primitives::types::BlockReference::from(
        near_primitives::types::BlockId::Hash(block.header.prev_hash),
    );
    let mut accounts_previous_state = touched_account_ids.iter().map(|account_id| {
            let prev_block_id = &prev_block_id;
            let view_client_addr = &view_client_addr;
            async move {
                let query = near_client::Query::new(
                    prev_block_id.clone(),
                    near_primitives::views::QueryRequest::ViewAccount {
                        account_id: account_id.clone(),
                    },
                );
                let account_info_response = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                        loop {
                            match view_client_addr
                                .send(query.clone())
                                .await?
                            {
                                Ok(Some(query_response)) => return Ok(Some(query_response)),
                                Ok(None) => {}
                                // TODO: update this once we return structured errors in the view_client handlers
                                Err(err) => {
                                    if err.contains("does not exist") {
                                        return Ok(None);
                                    }
                                    return Err(crate::models::Error::from(crate::models::ErrorKind::Other(err)));
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
                    near_primitives::views::QueryResponseKind::ViewAccount(account_info) => Ok(Some((account_id.clone(), account_info))),
                    _ => {
                        Err(crate::models::ErrorKind::Other(
                            "Internal invariant is not held; we queried ViewAccount, but received something else."
                                .to_string(),
                        )
                        .into())
                    }
                }
            }
        })
        .collect::<futures::stream::FuturesUnordered<_>>()
        .collect::<Vec<Result<Option<(near_primitives::types::AccountId, near_primitives::views::AccountView)>, crate::models::Error>>>()
        .await
        .into_iter()
        .filter_map(|account_info| account_info.transpose())
        .collect::<Result<std::collections::HashMap<near_primitives::types::AccountId, near_primitives::views::AccountView>, crate::models::Error>>()?;

    let accounts_changes = view_client_addr
        .send(near_client::GetStateChanges {
            block_hash: block.header.hash,
            state_changes_request:
                near_primitives::views::StateChangesRequestView::AccountChanges {
                    account_ids: touched_account_ids.into_iter().collect(),
                },
        })
        .await?
        .map_err(crate::models::ErrorKind::Other)?;

    let mut transactions = Vec::<crate::models::Transaction>::new();
    for account_change in accounts_changes.into_iter() {
        let transaction_hash = match account_change.cause {
            near_primitives::views::StateChangeCauseView::TransactionProcessing { tx_hash } => {
                format!("tx:{}", tx_hash.to_base())
            }
            near_primitives::views::StateChangeCauseView::ActionReceiptProcessingStarted {
                receipt_hash,
            } => format!("receipt:{}", receipt_hash.to_base()),
            near_primitives::views::StateChangeCauseView::ActionReceiptGasReward {
                receipt_hash,
            } => format!("receipt:{}", receipt_hash.to_base()),
            near_primitives::views::StateChangeCauseView::ReceiptProcessing { receipt_hash } => {
                format!("receipt:{}", receipt_hash.to_base())
            }
            near_primitives::views::StateChangeCauseView::PostponedReceipt { receipt_hash } => {
                format!("receipt:{}", receipt_hash.to_base())
            }
            near_primitives::views::StateChangeCauseView::InitialState
            | near_primitives::views::StateChangeCauseView::ValidatorAccountsUpdate
            | near_primitives::views::StateChangeCauseView::UpdatedDelayedReceipts => {
                format!("block:{}", block.header.hash)
            }
            near_primitives::views::StateChangeCauseView::NotWritableToDisk => unreachable!(),
        };
        let current_transaction = if let Some(transaction) = transactions.last_mut() {
            if transaction.transaction_identifier.hash == transaction_hash {
                Some(transaction)
            } else {
                None
            }
        } else {
            None
        };
        let current_transaction = if let Some(transaction) = current_transaction {
            transaction
        } else {
            transactions.push(crate::models::Transaction {
                transaction_identifier: crate::models::TransactionIdentifier {
                    hash: transaction_hash.clone(),
                },
                operations: vec![],
                metadata: crate::models::TransactionMetadata {
                    type_: crate::models::TransactionType::Transaction,
                },
            });
            transactions.last_mut().unwrap()
        };
        let operations = &mut current_transaction.operations;
        match account_change.value {
            near_primitives::views::StateChangeValueView::AccountUpdate { account_id, account } => {
                let previous_account_state = accounts_previous_state.get(&account_id);

                let previous_liquid_balance_for_storage = if let Some(previous_account_state) =
                    previous_account_state
                {
                    let mut account =
                        near_primitives::account::Account::from(previous_account_state);
                    account.amount = 0;
                    near_runtime_configs::get_insufficient_storage_stake(
                        &account,
                        &genesis.config.runtime_config,
                    )
                    .expect("get_insufficient_storage_stake never fails when state is consistent")
                    .unwrap_or(0)
                } else {
                    0
                };
                let new_liquid_balance_for_storage = {
                    let mut account = near_primitives::account::Account::from(&account);
                    account.amount = 0;
                    near_runtime_configs::get_insufficient_storage_stake(
                        &account,
                        &genesis.config.runtime_config,
                    )
                    .expect("get_insufficient_storage_stake never fails when state is consistent")
                    .unwrap_or(0)
                };

                let previous_liquid_balance = previous_account_state.map(|account| account.amount).unwrap_or(0).checked_sub(previous_liquid_balance_for_storage).ok_or_else(|| crate::models::ErrorKind::Other("Internal invariant is not held; liquid balance for storage cannot be bigger than the total balance".into()))?;
                let new_liquid_balance = account.amount.checked_sub(new_liquid_balance_for_storage).ok_or_else(|| crate::models::ErrorKind::Other("Internal invariant is not held; liquid balance for storage cannot be bigger than the total balance".into()))?;

                let previous_locked_balance =
                    previous_account_state.map(|account| account.locked).unwrap_or(0);
                let new_locked_balance = account.locked;

                if previous_liquid_balance != new_liquid_balance {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier {
                            index: operations.len().try_into().expect("there cannot be more than i64::MAX operations in a single transaction"),
                            network_index: None,
                        },
                        related_operations: None,
                        account: Some(crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: None,
                            metadata: None,
                        }),
                        amount: Some(crate::models::Amount {
                            value: crate::utils::SignedDiff::cmp(previous_liquid_balance, new_liquid_balance).to_string(),
                            currency: crate::consts::YOCTO_NEAR_CURRENCY.clone(),
                            metadata: None,
                        }),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                if previous_liquid_balance_for_storage != new_liquid_balance_for_storage {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier {
                            index: operations.len().try_into().expect("there cannot be more than i64::MAX operations in a single transaction"),
                            network_index: None,
                        },
                        related_operations: None,
                        account: Some(crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: Some(crate::models::SubAccountIdentifier {
                                address: "liquid_for_storage".into(),
                                metadata: None,
                            }),
                            metadata: None,
                        }),
                        amount: Some(crate::models::Amount {
                            value: crate::utils::SignedDiff::cmp(previous_liquid_balance_for_storage, new_liquid_balance_for_storage).to_string(),
                            currency: crate::consts::YOCTO_NEAR_CURRENCY.clone(),
                            metadata: None,
                        }),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                if previous_locked_balance != new_locked_balance {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier {
                            index: operations.len().try_into().expect("there cannot be more than i64::MAX operations in a single transaction"),
                            network_index: None,
                        },
                        related_operations: None,
                        account: Some(crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: Some(crate::models::SubAccountIdentifier {
                                address: "locked".into(),
                                metadata: None,
                            }),
                            metadata: None,
                        }),
                        amount: Some(crate::models::Amount {
                            value: crate::utils::SignedDiff::cmp(previous_locked_balance, new_locked_balance).to_string(),
                            currency: crate::consts::YOCTO_NEAR_CURRENCY.clone(),
                            metadata: None,
                        }),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                accounts_previous_state.insert(account_id, account);
            }

            near_primitives::views::StateChangeValueView::AccountDeletion { account_id } => {
                let previous_account_state = accounts_previous_state.get(&account_id);

                let previous_liquid_balance_for_storage = if let Some(previous_account_state) =
                    previous_account_state
                {
                    let mut account =
                        near_primitives::account::Account::from(previous_account_state);
                    account.amount = 0;
                    near_runtime_configs::get_insufficient_storage_stake(
                        &account,
                        &genesis.config.runtime_config,
                    )
                    .expect("get_insufficient_storage_stake never fails when state is consistent")
                    .unwrap_or(0)
                } else {
                    0
                };
                let new_liquid_balance_for_storage = 0;

                let previous_liquid_balance = previous_account_state.map(|account| account.amount).unwrap_or(0).checked_sub(previous_liquid_balance_for_storage).ok_or_else(|| crate::models::ErrorKind::Other("Internal invariant is not held; liquid balance for storage cannot be bigger than the total balance".into()))?;
                let new_liquid_balance = 0;

                let previous_locked_balance =
                    previous_account_state.map(|account| account.locked).unwrap_or(0);
                let new_locked_balance = 0;

                if previous_liquid_balance != new_liquid_balance {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier {
                            index: operations.len().try_into().expect("there cannot be more than i64::MAX operations in a single transaction"),
                            network_index: None,
                        },
                        related_operations: None,
                        account: Some(crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: None,
                            metadata: None,
                        }),
                        amount: Some(crate::models::Amount {
                            value: crate::utils::SignedDiff::cmp(previous_liquid_balance, new_liquid_balance).to_string(),
                            currency: crate::consts::YOCTO_NEAR_CURRENCY.clone(),
                            metadata: None,
                        }),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                if previous_liquid_balance_for_storage != new_liquid_balance_for_storage {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier {
                            index: operations.len().try_into().expect("there cannot be more than i64::MAX operations in a single transaction"),
                            network_index: None,
                        },
                        related_operations: None,
                        account: Some(crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: Some(crate::models::SubAccountIdentifier {
                                address: "liquid_for_storage".into(),
                                metadata: None,
                            }),
                            metadata: None,
                        }),
                        amount: Some(crate::models::Amount {
                            value: crate::utils::SignedDiff::cmp(previous_liquid_balance_for_storage, new_liquid_balance_for_storage).to_string(),
                            currency: crate::consts::YOCTO_NEAR_CURRENCY.clone(),
                            metadata: None,
                        }),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                if previous_locked_balance != new_locked_balance {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier {
                            index: operations.len().try_into().expect("there cannot be more than i64::MAX operations in a single transaction"),
                            network_index: None,
                        },
                        related_operations: None,
                        account: Some(crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: Some(crate::models::SubAccountIdentifier {
                                address: "locked".into(),
                                metadata: None,
                            }),
                            metadata: None,
                        }),
                        amount: Some(crate::models::Amount {
                            value: crate::utils::SignedDiff::cmp(previous_locked_balance, new_locked_balance).to_string(),
                            currency: crate::consts::YOCTO_NEAR_CURRENCY.clone(),
                            metadata: None,
                        }),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                accounts_previous_state.remove(&account_id);
            }
            unexpected_value => {
                return Err(crate::models::ErrorKind::Other(format!(
                    "Internal invariant is not held; we queried AccountChanges, but received {:?}.",
                    unexpected_value
                ))
                .into())
            }
        }
    }

    Ok(transactions)
}
