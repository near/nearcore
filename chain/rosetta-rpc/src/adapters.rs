use std::convert::TryInto;
use std::sync::Arc;

use actix::Addr;

use near_chain_configs::Genesis;
use near_client::ViewClientActor;
use near_primitives::serialize::BaseEncode;

/// NEAR Protocol defines initial state in genesis records and treats the first
/// block differently (e.g. it cannot contain any transactions: https://stackoverflow.com/a/63347167/1178806).
///
/// Genesis records can be huge (order of gigabytes of JSON data), and Rosetta
/// API does not define any pagination, and suggests to use
/// `other_transactions` to deal with this: https://community.rosetta-api.org/t/how-to-return-data-without-being-able-to-paginate/98
/// We choose to do a proper implementation for the genesis block later.
async fn convert_genesis_records_to_transaction(
    genesis: Arc<Genesis>,
    view_client_addr: Addr<ViewClientActor>,
    block: &near_primitives::views::BlockView,
) -> Result<crate::models::Transaction, crate::errors::ErrorKind> {
    let genesis_account_ids = genesis.records.as_ref().iter().filter_map(|record| {
        if let near_primitives::state_record::StateRecord::Account { account_id, .. } = record {
            Some(account_id)
        } else {
            None
        }
    });
    let genesis_accounts = crate::utils::query_accounts(
        genesis_account_ids,
        &near_primitives::types::BlockId::Hash(block.header.hash).into(),
        &view_client_addr,
    )
    .await?;

    let mut operations = Vec::new();
    for (account_id, account) in genesis_accounts {
        let account_balances = crate::utils::RosettaAccountBalances::from_account(
            &account,
            &genesis.config.runtime_config,
        );

        if account_balances.liquid != 0 {
            operations.push(crate::models::Operation {
                operation_identifier: crate::models::OperationIdentifier {
                    index: operations.len().try_into().expect(
                        "there cannot be more than i64::MAX operations in a single transaction",
                    ),
                    network_index: None,
                },
                related_operations: None,
                account: Some(crate::models::AccountIdentifier {
                    address: account_id.clone(),
                    sub_account: None,
                    metadata: None,
                }),
                amount: Some(crate::models::Amount::from_yoctonear(account_balances.liquid)),
                type_: crate::models::OperationType::Transfer,
                status: crate::models::OperationStatusKind::Success,
                metadata: None,
            });
        }

        if account_balances.liquid_for_storage != 0 {
            operations.push(crate::models::Operation {
                operation_identifier: crate::models::OperationIdentifier {
                    index: operations.len().try_into().expect(
                        "there cannot be more than i64::MAX operations in a single transaction",
                    ),
                    network_index: None,
                },
                related_operations: None,
                account: Some(crate::models::AccountIdentifier {
                    address: account_id.clone(),
                    sub_account: Some(crate::consts::SubAccount::LiquidBalanceForStorage.into()),
                    metadata: None,
                }),
                amount: Some(crate::models::Amount::from_yoctonear(
                    account_balances.liquid_for_storage,
                )),
                type_: crate::models::OperationType::Transfer,
                status: crate::models::OperationStatusKind::Success,
                metadata: None,
            });
        }

        if account_balances.locked != 0 {
            operations.push(crate::models::Operation {
                operation_identifier: crate::models::OperationIdentifier {
                    index: operations.len().try_into().expect(
                        "there cannot be more than i64::MAX operations in a single transaction",
                    ),
                    network_index: None,
                },
                related_operations: None,
                account: Some(crate::models::AccountIdentifier {
                    address: account_id.clone(),
                    sub_account: Some(crate::consts::SubAccount::Locked.into()),
                    metadata: None,
                }),
                amount: Some(crate::models::Amount::from_yoctonear(account_balances.locked)),
                type_: crate::models::OperationType::Transfer,
                status: crate::models::OperationStatusKind::Success,
                metadata: None,
            });
        }
    }

    Ok(crate::models::Transaction {
        transaction_identifier: crate::models::TransactionIdentifier {
            hash: format!("block:{}", block.header.hash),
        },
        operations,
        metadata: crate::models::TransactionMetadata {
            type_: crate::models::TransactionType::Block,
        },
    })
}

pub(crate) async fn convert_block_to_transactions(
    genesis: Arc<Genesis>,
    view_client_addr: Addr<ViewClientActor>,
    block: &near_primitives::views::BlockView,
) -> Result<Vec<crate::models::Transaction>, crate::errors::ErrorKind> {
    let runtime_config = &genesis.config.runtime_config;

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
    let mut accounts_previous_state =
        crate::utils::query_accounts(touched_account_ids.iter(), &prev_block_id, &view_client_addr)
            .await?;

    let accounts_changes = view_client_addr
        .send(near_client::GetStateChanges {
            block_hash: block.header.hash,
            state_changes_request:
                near_primitives::views::StateChangesRequestView::AccountChanges {
                    account_ids: touched_account_ids.into_iter().collect(),
                },
        })
        .await?
        .map_err(crate::errors::ErrorKind::InternalError)?;

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

                let previous_account_balances = previous_account_state
                    .map(|account| {
                        crate::utils::RosettaAccountBalances::from_account(account, runtime_config)
                    })
                    .unwrap_or_else(crate::utils::RosettaAccountBalances::zero);

                let new_account_balances =
                    crate::utils::RosettaAccountBalances::from_account(&account, runtime_config);

                if previous_account_balances.liquid != new_account_balances.liquid {
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
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(previous_account_balances.liquid, new_account_balances.liquid))),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                if previous_account_balances.liquid_for_storage
                    != new_account_balances.liquid_for_storage
                {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier {
                            index: operations.len().try_into().expect("there cannot be more than i64::MAX operations in a single transaction"),
                            network_index: None,
                        },
                        related_operations: None,
                        account: Some(crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: Some(crate::consts::SubAccount::LiquidBalanceForStorage.into()),
                            metadata: None,
                        }),
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(previous_account_balances.liquid_for_storage, new_account_balances.liquid_for_storage))),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                if previous_account_balances.locked != new_account_balances.locked {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier {
                            index: operations.len().try_into().expect("there cannot be more than i64::MAX operations in a single transaction"),
                            network_index: None,
                        },
                        related_operations: None,
                        account: Some(crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: Some(crate::consts::SubAccount::Locked.into()),
                            metadata: None,
                        }),
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(previous_account_balances.locked, new_account_balances.locked))),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                accounts_previous_state.insert(account_id, account);
            }

            near_primitives::views::StateChangeValueView::AccountDeletion { account_id } => {
                let previous_account_state = accounts_previous_state.get(&account_id);

                let previous_account_balances =
                    if let Some(previous_account_state) = previous_account_state {
                        crate::utils::RosettaAccountBalances::from_account(
                            previous_account_state,
                            runtime_config,
                        )
                    } else {
                        continue;
                    };
                let new_account_balances = crate::utils::RosettaAccountBalances::zero();

                if previous_account_balances.liquid != new_account_balances.liquid {
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
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(previous_account_balances.liquid, new_account_balances.liquid))),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                if previous_account_balances.liquid_for_storage
                    != new_account_balances.liquid_for_storage
                {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier {
                            index: operations.len().try_into().expect("there cannot be more than i64::MAX operations in a single transaction"),
                            network_index: None,
                        },
                        related_operations: None,
                        account: Some(crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: Some(crate::consts::SubAccount::LiquidBalanceForStorage.into()),
                            metadata: None,
                        }),
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(previous_account_balances.liquid_for_storage, new_account_balances.liquid_for_storage))),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                if previous_account_balances.locked != new_account_balances.locked {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier {
                            index: operations.len().try_into().expect("there cannot be more than i64::MAX operations in a single transaction"),
                            network_index: None,
                        },
                        related_operations: None,
                        account: Some(crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: Some(crate::consts::SubAccount::Locked.into()),
                            metadata: None,
                        }),
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(previous_account_balances.locked, new_account_balances.locked))),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                accounts_previous_state.remove(&account_id);
            }
            unexpected_value => {
                return Err(crate::errors::ErrorKind::InternalInvariantError(format!(
                    "queried AccountChanges, but received {:?}.",
                    unexpected_value
                ))
                .into())
            }
        }
    }

    Ok(transactions)
}

pub(crate) async fn collect_transactions(
    genesis: Arc<Genesis>,
    view_client_addr: Addr<ViewClientActor>,
    block: &near_primitives::views::BlockView,
) -> Result<Vec<crate::models::Transaction>, crate::errors::ErrorKind> {
    if block.header.prev_hash == Default::default() {
        Ok(vec![convert_genesis_records_to_transaction(genesis, view_client_addr, block).await?])
    } else {
        convert_block_to_transactions(genesis, view_client_addr, block).await
    }
}
