use std::string::ToString;

/// Constructs a transaction identifier from the cause of a state change.
fn convert_cause_to_transaction_id(
    block_hash: &near_primitives::hash::CryptoHash,
    cause: near_primitives::views::StateChangeCauseView,
) -> crate::errors::Result<crate::models::TransactionIdentifier> {
    use crate::models::TransactionIdentifier;
    use near_primitives::views::StateChangeCauseView;
    match cause {
        StateChangeCauseView::TransactionProcessing { tx_hash } => {
            Ok(TransactionIdentifier::transaction(&tx_hash))
        }
        StateChangeCauseView::ActionReceiptProcessingStarted { receipt_hash }
        | StateChangeCauseView::ActionReceiptGasReward { receipt_hash }
        | StateChangeCauseView::ReceiptProcessing { receipt_hash }
        | StateChangeCauseView::PostponedReceipt { receipt_hash } => {
            Ok(TransactionIdentifier::receipt(&receipt_hash))
        }
        StateChangeCauseView::InitialState => {
            Ok(TransactionIdentifier::block_event("block", block_hash))
        }
        StateChangeCauseView::ValidatorAccountsUpdate => {
            Ok(TransactionIdentifier::block_event("block-validators-update", block_hash))
        }
        StateChangeCauseView::UpdatedDelayedReceipts => {
            Ok(TransactionIdentifier::block_event("block-delayed-receipts", block_hash))
        }
        StateChangeCauseView::NotWritableToDisk => {
            Err(crate::errors::ErrorKind::InternalInvariantError(
                "State Change 'NotWritableToDisk' should never be observed".to_string(),
            ))
        }
        StateChangeCauseView::Migration => {
            Ok(TransactionIdentifier::block_event("migration", block_hash))
        }
        StateChangeCauseView::Resharding => Err(crate::errors::ErrorKind::InternalInvariantError(
            "State Change 'Resharding' should never be observed".to_string(),
        )),
    }
}

pub(super) fn convert_block_changes_to_transactions(
    runtime_config: &near_primitives::runtime::config::RuntimeConfig,
    block_hash: &near_primitives::hash::CryptoHash,
    accounts_changes: near_primitives::views::StateChangesView,
    mut accounts_previous_state: std::collections::HashMap<
        near_primitives::types::AccountId,
        near_primitives::views::AccountView,
    >,
) -> crate::errors::Result<std::collections::HashMap<String, crate::models::Transaction>> {
    let mut transactions = std::collections::HashMap::<String, crate::models::Transaction>::new();
    for account_change in accounts_changes {
        let transaction_identifier =
            convert_cause_to_transaction_id(block_hash, account_change.cause)?;
        let current_transaction = transactions
            .entry(transaction_identifier.hash.clone())
            .or_insert_with(move || crate::models::Transaction {
                transaction_identifier,
                operations: vec![],
                metadata: crate::models::TransactionMetadata {
                    type_: crate::models::TransactionType::Transaction,
                },
            });

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
                        operation_identifier: crate::models::OperationIdentifier::new(operations),
                        related_operations: None,
                        account: crate::models::AccountIdentifier {
                            address: account_id.clone().into(),
                            sub_account: None,
                        },
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(
                                previous_account_balances.liquid,
                                new_account_balances.liquid,
                            ),
                        )),
                        type_: crate::models::OperationType::Transfer,
                        status: Some(crate::models::OperationStatusKind::Success),
                        metadata: None,
                    });
                }

                if previous_account_balances.liquid_for_storage
                    != new_account_balances.liquid_for_storage
                {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(operations),
                        related_operations: None,
                        account: crate::models::AccountIdentifier {
                            address: account_id.clone().into(),
                            sub_account: Some(
                                crate::models::SubAccount::LiquidBalanceForStorage.into(),
                            ),
                        },
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(
                                previous_account_balances.liquid_for_storage,
                                new_account_balances.liquid_for_storage,
                            ),
                        )),
                        type_: crate::models::OperationType::Transfer,
                        status: Some(crate::models::OperationStatusKind::Success),
                        metadata: None,
                    });
                }

                if previous_account_balances.locked != new_account_balances.locked {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(operations),
                        related_operations: None,
                        account: crate::models::AccountIdentifier {
                            address: account_id.clone().into(),
                            sub_account: Some(crate::models::SubAccount::Locked.into()),
                        },
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(
                                previous_account_balances.locked,
                                new_account_balances.locked,
                            ),
                        )),
                        type_: crate::models::OperationType::Transfer,
                        status: Some(crate::models::OperationStatusKind::Success),
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
                        operation_identifier: crate::models::OperationIdentifier::new(operations),
                        related_operations: None,
                        account: crate::models::AccountIdentifier {
                            address: account_id.clone().into(),
                            sub_account: None,
                        },
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(
                                previous_account_balances.liquid,
                                new_account_balances.liquid,
                            ),
                        )),
                        type_: crate::models::OperationType::Transfer,
                        status: Some(crate::models::OperationStatusKind::Success),
                        metadata: None,
                    });
                }

                if previous_account_balances.liquid_for_storage
                    != new_account_balances.liquid_for_storage
                {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(operations),
                        related_operations: None,
                        account: crate::models::AccountIdentifier {
                            address: account_id.clone().into(),
                            sub_account: Some(
                                crate::models::SubAccount::LiquidBalanceForStorage.into(),
                            ),
                        },
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(
                                previous_account_balances.liquid_for_storage,
                                new_account_balances.liquid_for_storage,
                            ),
                        )),
                        type_: crate::models::OperationType::Transfer,
                        status: Some(crate::models::OperationStatusKind::Success),
                        metadata: None,
                    });
                }

                if previous_account_balances.locked != new_account_balances.locked {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(operations),
                        related_operations: None,
                        account: crate::models::AccountIdentifier {
                            address: account_id.clone().into(),
                            sub_account: Some(crate::models::SubAccount::Locked.into()),
                        },
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(
                                previous_account_balances.locked,
                                new_account_balances.locked,
                            ),
                        )),
                        type_: crate::models::OperationType::Transfer,
                        status: Some(crate::models::OperationStatusKind::Success),
                        metadata: None,
                    });
                }

                accounts_previous_state.remove(&account_id);
            }
            unexpected_value => {
                return Err(crate::errors::ErrorKind::InternalInvariantError(format!(
                    "queried AccountChanges, but received {:?}.",
                    unexpected_value
                )))
            }
        }
    }

    Ok(transactions)
}
