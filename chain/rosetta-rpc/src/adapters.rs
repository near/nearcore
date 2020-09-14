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
                operation_identifier: crate::models::OperationIdentifier::new(&operations),
                related_operations: None,
                account: crate::models::AccountIdentifier {
                    address: account_id.clone(),
                    sub_account: None,
                },
                amount: Some(crate::models::Amount::from_yoctonear(account_balances.liquid)),
                type_: crate::models::OperationType::Transfer,
                status: crate::models::OperationStatusKind::Success,
                metadata: None,
            });
        }

        if account_balances.liquid_for_storage != 0 {
            operations.push(crate::models::Operation {
                operation_identifier: crate::models::OperationIdentifier::new(&operations),
                related_operations: None,
                account: crate::models::AccountIdentifier {
                    address: account_id.clone(),
                    sub_account: Some(crate::models::SubAccount::LiquidBalanceForStorage.into()),
                },
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
                operation_identifier: crate::models::OperationIdentifier::new(&operations),
                related_operations: None,
                account: crate::models::AccountIdentifier {
                    address: account_id.clone(),
                    sub_account: Some(crate::models::SubAccount::Locked.into()),
                },
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
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: None,
                        account: crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: None,
                        },
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(
                                previous_account_balances.liquid,
                                new_account_balances.liquid,
                            ),
                        )),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                if previous_account_balances.liquid_for_storage
                    != new_account_balances.liquid_for_storage
                {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: None,
                        account: crate::models::AccountIdentifier {
                            address: account_id.clone(),
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
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                if previous_account_balances.locked != new_account_balances.locked {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: None,
                        account: crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: Some(crate::models::SubAccount::Locked.into()),
                        },
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(
                                previous_account_balances.locked,
                                new_account_balances.locked,
                            ),
                        )),
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
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: None,
                        account: crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: None,
                        },
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(
                                previous_account_balances.liquid,
                                new_account_balances.liquid,
                            ),
                        )),
                        type_: crate::models::OperationType::Transfer,
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                if previous_account_balances.liquid_for_storage
                    != new_account_balances.liquid_for_storage
                {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: None,
                        account: crate::models::AccountIdentifier {
                            address: account_id.clone(),
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
                        status: crate::models::OperationStatusKind::Success,
                        metadata: None,
                    });
                }

                if previous_account_balances.locked != new_account_balances.locked {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: None,
                        account: crate::models::AccountIdentifier {
                            address: account_id.clone(),
                            sub_account: Some(crate::models::SubAccount::Locked.into()),
                        },
                        amount: Some(crate::models::Amount::from_yoctonear_diff(
                            crate::utils::SignedDiff::cmp(
                                previous_account_balances.locked,
                                new_account_balances.locked,
                            ),
                        )),
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

fn update_and_fail_if_different<'a>(
    known_value: &'a mut Option<String>,
    new_value: &'a str,
) -> Result<(), (&'a str, &'a str)> {
    if let Some(known_value) = known_value {
        if &new_value != known_value {
            Err((known_value, new_value))
        } else {
            Ok(())
        }
    } else {
        *known_value = Some(new_value.to_owned());
        Ok(())
    }
}

#[derive(Debug)]
pub struct NearActions {
    pub sender_account_id: near_primitives::types::AccountId,
    pub receiver_account_id: near_primitives::types::AccountId,
    pub actions: Vec<near_primitives::transaction::Action>,
}

impl From<&NearActions> for Vec<crate::models::Operation> {
    fn from(near_actions: &NearActions) -> Self {
        let NearActions { sender_account_id, receiver_account_id, actions } = near_actions;
        let sender_account_identifier: crate::models::AccountIdentifier =
            sender_account_id.clone().into();
        let receiver_account_identifier: crate::models::AccountIdentifier =
            receiver_account_id.clone().into();
        let mut operations = vec![];
        for action in actions {
            match action {
                near_primitives::transaction::Action::CreateAccount(_) => {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: None,
                        type_: crate::models::OperationType::CreateAccount,
                        account: receiver_account_identifier.clone(),
                        amount: None,
                        metadata: None,
                        status: crate::models::OperationStatusKind::Success,
                    });
                }
                near_primitives::transaction::Action::DeleteAccount(action) => {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: None,
                        type_: crate::models::OperationType::DeleteAccount,
                        account: receiver_account_identifier.clone(),
                        amount: None,
                        metadata: Some(crate::models::OperationMetadata {
                            beneficiary_id: Some(action.beneficiary_id.clone().into()),
                            ..Default::default()
                        }),
                        status: crate::models::OperationStatusKind::Success,
                    });
                }
                near_primitives::transaction::Action::AddKey(action) => {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: None,
                        type_: crate::models::OperationType::AddKey,
                        account: receiver_account_identifier.clone(),
                        amount: None,
                        metadata: Some(crate::models::OperationMetadata {
                            public_key: Some((&action.public_key).into()),
                            ..Default::default()
                        }),
                        status: crate::models::OperationStatusKind::Success,
                    });
                }
                near_primitives::transaction::Action::DeleteKey(action) => {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: None,
                        type_: crate::models::OperationType::DeleteKey,
                        account: receiver_account_identifier.clone(),
                        amount: None,
                        metadata: Some(crate::models::OperationMetadata {
                            public_key: Some((&action.public_key).into()),
                            ..Default::default()
                        }),
                        status: crate::models::OperationStatusKind::Success,
                    });
                }
                near_primitives::transaction::Action::Transfer(action) => {
                    let send_transfer_operation_identifier =
                        crate::models::OperationIdentifier::new(&operations);
                    operations.push(crate::models::Operation {
                        operation_identifier: send_transfer_operation_identifier.clone(),
                        related_operations: None,
                        type_: crate::models::OperationType::Transfer,
                        account: sender_account_identifier.clone(),
                        amount: Some(-crate::models::Amount::from_yoctonear(action.deposit)),
                        metadata: None,
                        status: crate::models::OperationStatusKind::Success,
                    });
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: Some(vec![send_transfer_operation_identifier]),
                        type_: crate::models::OperationType::Transfer,
                        account: receiver_account_identifier.clone(),
                        amount: Some(crate::models::Amount::from_yoctonear(action.deposit)),
                        metadata: None,
                        status: crate::models::OperationStatusKind::Success,
                    });
                }
                near_primitives::transaction::Action::Stake(action) => {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: None,
                        type_: crate::models::OperationType::Stake,
                        account: receiver_account_identifier.clone(),
                        amount: Some(crate::models::Amount::from_yoctonear(action.stake)),
                        metadata: Some(crate::models::OperationMetadata {
                            public_key: Some((&action.public_key).into()),
                            ..Default::default()
                        }),
                        status: crate::models::OperationStatusKind::Success,
                    });
                }
                near_primitives::transaction::Action::DeployContract(action) => {
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations: None,
                        type_: crate::models::OperationType::DeployContract,
                        account: receiver_account_identifier.clone(),
                        amount: None,
                        metadata: Some(crate::models::OperationMetadata {
                            code: Some(action.code.clone().into()),
                            ..Default::default()
                        }),
                        status: crate::models::OperationStatusKind::Success,
                    });
                }
                near_primitives::transaction::Action::FunctionCall(action) => {
                    let related_operations = if action.deposit > 0 {
                        let send_transfer_operation_identifier =
                            crate::models::OperationIdentifier::new(&operations);
                        operations.push(crate::models::Operation {
                            operation_identifier: send_transfer_operation_identifier.clone(),
                            related_operations: None,
                            type_: crate::models::OperationType::Transfer,
                            account: sender_account_identifier.clone(),
                            amount: Some(-crate::models::Amount::from_yoctonear(action.deposit)),
                            metadata: None,
                            status: crate::models::OperationStatusKind::Success,
                        });
                        Some(vec![send_transfer_operation_identifier])
                    } else {
                        None
                    };
                    operations.push(crate::models::Operation {
                        operation_identifier: crate::models::OperationIdentifier::new(&operations),
                        related_operations,
                        type_: crate::models::OperationType::FunctionCall,
                        account: receiver_account_identifier.clone(),
                        amount: if action.deposit > 0 {
                            Some(crate::models::Amount::from_yoctonear(action.deposit))
                        } else {
                            None
                        },
                        metadata: Some(crate::models::OperationMetadata {
                            method_name: Some(action.method_name.clone()),
                            args: Some(action.args.clone().into()),
                            attached_gas: Some(action.gas.into()),
                            ..Default::default()
                        }),
                        status: crate::models::OperationStatusKind::Success,
                    });
                }
            }
        }
        operations
    }
}

impl std::convert::TryFrom<&[crate::models::Operation]> for NearActions {
    type Error = crate::errors::ErrorKind;

    fn try_from(operations: &[crate::models::Operation]) -> Result<Self, Self::Error> {
        let mut sender_account_id: Option<String> = None;
        let mut receiver_account_id: Option<String> = None;
        let mut actions = vec![];

        // Iterate over operations backwards to handle the related operations
        let mut operations = operations.iter().rev();
        while let Some(receiver_operation) = operations.next() {
            match receiver_operation.type_ {
                crate::models::OperationType::Transfer => {
                    let receiver_amount = receiver_operation.amount.as_ref().ok_or_else(|| {
                        crate::errors::ErrorKind::InvalidInput(
                            "TRANSFER operations must specify `amount`".to_string(),
                        )
                    })?;
                    if !receiver_amount.value.is_positive() {
                        return Err(crate::errors::ErrorKind::InvalidInput(
                            "Receiver TRANSFER operations must have positive `amount`".to_string(),
                        )
                        .into());
                    }
                    update_and_fail_if_different(&mut receiver_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(
                                format!("A single transaction cannot be send to multiple recipients ('{}' and '{}')", old_value, new_value)
                            )
                        })?;
                    let sender_operation = operations.next().ok_or_else(|| {
                        crate::errors::ErrorKind::InvalidInput(
                            "Source TRANSFER operation is missing".to_string(),
                        )
                    })?;

                    let sender_amount = sender_operation.amount.as_ref().ok_or_else(|| {
                        crate::errors::ErrorKind::InvalidInput(
                            "TRANSFER operations must specify `amount`".to_string(),
                        )
                    })?;
                    if -sender_amount.value != receiver_amount.value {
                        return Err(crate::errors::ErrorKind::InvalidInput(
                            "The sum of amounts of Sender and Receiver TRANSFER operations must be zero"
                                .to_string(),
                        ));
                    }
                    update_and_fail_if_different(&mut sender_account_id, &sender_operation.account.address)
                        .map_err(|(old_value, new_value)|
                            crate::errors::ErrorKind::InvalidInput(
                                format!("A single transaction cannot be send from multiple senders ('{}' and '{}')", old_value, new_value)
                            )
                        )?;
                    actions.push(near_primitives::transaction::Action::Transfer(
                        near_primitives::transaction::TransferAction {
                            deposit: receiver_amount.value.absolute_difference(),
                        },
                    ))
                }
                crate::models::OperationType::Stake => {
                    let receiver_amount = receiver_operation.amount.as_ref().ok_or_else(|| {
                        crate::errors::ErrorKind::InvalidInput(
                            "STAKE operations must specify `amount`".to_string(),
                        )
                    })?;
                    if !receiver_amount.value.is_positive() {
                        return Err(crate::errors::ErrorKind::InvalidInput(
                            "Receiver STAKE operations must have positive `amount`".to_string(),
                        ));
                    }
                    update_and_fail_if_different(&mut receiver_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send to multiple recipients ('{}' and '{}')", old_value, new_value))
                        })?;
                    update_and_fail_if_different(&mut sender_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send from multiple senders ('{}' and '{}')", old_value, new_value))
                        })?;
                    let required_fields_error = || {
                        crate::errors::ErrorKind::InvalidInput(
                            "STAKE operation requires `public_key` being passed in the metadata"
                                .into(),
                        )
                    };
                    let metadata =
                        receiver_operation.metadata.as_ref().ok_or_else(required_fields_error)?;
                    let public_key = metadata
                        .public_key
                        .as_ref()
                        .ok_or_else(required_fields_error)?
                        .try_into()
                        .map_err(|_| {
                            crate::errors::ErrorKind::InvalidInput(format!(
                                "Invalid public_key: {:?}",
                                metadata.public_key
                            ))
                        })?;
                    actions.push(near_primitives::transaction::Action::Stake(
                        near_primitives::transaction::StakeAction {
                            stake: receiver_amount.value.absolute_difference(),
                            public_key,
                        },
                    ))
                }
                crate::models::OperationType::CreateAccount => {
                    update_and_fail_if_different(&mut receiver_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send to multiple recipients ('{}' and '{}')", old_value, new_value))
                        })?;
                    update_and_fail_if_different(&mut sender_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send from multiple senders ('{}' and '{}')", old_value, new_value))
                        })?;
                    actions.push(near_primitives::transaction::Action::CreateAccount(
                        near_primitives::transaction::CreateAccountAction {},
                    ))
                }
                crate::models::OperationType::DeployContract => {
                    update_and_fail_if_different(&mut receiver_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send to multiple recipients ('{}' and '{}')", old_value, new_value))
                        })?;
                    update_and_fail_if_different(&mut sender_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send from multiple senders ('{}' and '{}')", old_value, new_value))
                        })?;
                    let required_fields_error = || {
                        crate::errors::ErrorKind::InvalidInput("DEPLOY_CONTRACT operation requires `code` being passed in the metadata".into())
                    };
                    let metadata =
                        receiver_operation.metadata.as_ref().ok_or_else(required_fields_error)?;
                    let code =
                        metadata.code.clone().ok_or_else(required_fields_error)?.into_inner();
                    actions.push(near_primitives::transaction::Action::DeployContract(
                        near_primitives::transaction::DeployContractAction { code },
                    ))
                }
                crate::models::OperationType::FunctionCall => {
                    update_and_fail_if_different(&mut receiver_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send to multiple recipients ('{}' and '{}')", old_value, new_value))
                        })?;
                    update_and_fail_if_different(&mut sender_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send from multiple senders ('{}' and '{}')", old_value, new_value))
                        })?;
                    let required_fields_error = || {
                        crate::errors::ErrorKind::InvalidInput("FUNCTION_CALL operation requires `method_name`, `args`, and `gas` being passed in the metadata".into())
                    };
                    let metadata =
                        receiver_operation.metadata.as_ref().ok_or_else(required_fields_error)?;
                    let method_name =
                        metadata.method_name.clone().ok_or_else(required_fields_error)?;
                    let args =
                        metadata.args.clone().ok_or_else(required_fields_error)?.into_inner();
                    let attached_gas = metadata.attached_gas.ok_or_else(required_fields_error)?;
                    let attached_gas = if attached_gas.is_positive() {
                        attached_gas.absolute_difference()
                    } else {
                        return Err(crate::errors::ErrorKind::InvalidInput(
                            "FUNCTION_CALL operation requires `attached_gas` to be positive".into(),
                        ));
                    };

                    let function_call_attached_amount = if let Some(
                        ref function_call_attached_amount,
                    ) = receiver_operation.amount
                    {
                        if !function_call_attached_amount.value.is_positive() {
                            return Err(crate::errors::ErrorKind::InvalidInput(
                                "FUNCTION_CALL operations must have non-negative `amount`"
                                    .to_string(),
                            )
                            .into());
                        }
                        function_call_attached_amount.value.absolute_difference()
                    } else {
                        0
                    };
                    if function_call_attached_amount > 0 {
                        let sender_operation = operations.next().ok_or_else(|| {
                            crate::errors::ErrorKind::InvalidInput(
                                "Source TRANSFER operation is missing".to_string(),
                            )
                        })?;

                        let sender_amount = sender_operation.amount.as_ref().ok_or_else(|| {
                            crate::errors::ErrorKind::InvalidInput(
                                "TRANSFER operations must specify `amount`".to_string(),
                            )
                        })?;
                        if !sender_amount.value.is_positive()
                            && sender_amount.value.absolute_difference()
                                != function_call_attached_amount
                        {
                            return Err(crate::errors::ErrorKind::InvalidInput(
                                "The sum of amounts of Sender TRANSFER and Receiver FUNCTION_CALL operations must be zero"
                                    .to_string(),
                            )
                            .into());
                        }
                        update_and_fail_if_different(&mut sender_account_id, &sender_operation.account.address)
                            .map_err(|(old_value, new_value)|
                                crate::errors::ErrorKind::InvalidInput(
                                    format!("A single transaction cannot be send from multiple senders ('{}' and '{}')", old_value, new_value)
                                )
                            )?;
                    }
                    actions.push(near_primitives::transaction::Action::FunctionCall(
                        near_primitives::transaction::FunctionCallAction {
                            args,
                            deposit: function_call_attached_amount,
                            gas: attached_gas,
                            method_name,
                        },
                    ))
                }
                crate::models::OperationType::AddKey => {
                    update_and_fail_if_different(&mut receiver_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send to multiple recipients ('{}' and '{}')", old_value, new_value))
                        })?;
                    update_and_fail_if_different(&mut sender_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send from multiple senders ('{}' and '{}')", old_value, new_value))
                        })?;
                    let required_fields_error = || {
                        crate::errors::ErrorKind::InvalidInput(
                            "ADD_KEY operation requires `public_key` being passed in the metadata"
                                .into(),
                        )
                    };
                    let metadata =
                        receiver_operation.metadata.as_ref().ok_or_else(required_fields_error)?;
                    let public_key = metadata
                        .public_key
                        .as_ref()
                        .ok_or_else(required_fields_error)?
                        .try_into()
                        .map_err(|_| {
                            crate::errors::ErrorKind::InvalidInput(format!(
                                "Invalid public_key: {:?}",
                                metadata.public_key
                            ))
                        })?;

                    actions.push(near_primitives::transaction::Action::AddKey(
                        near_primitives::transaction::AddKeyAction {
                            access_key: near_primitives::account::AccessKey::full_access(),
                            public_key,
                        },
                    ))
                }
                crate::models::OperationType::DeleteKey => {
                    update_and_fail_if_different(&mut receiver_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send to multiple recipients ('{}' and '{}')", old_value, new_value))
                        })?;
                    update_and_fail_if_different(&mut sender_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send from multiple senders ('{}' and '{}')", old_value, new_value))
                        })?;
                    let required_fields_error = || {
                        crate::errors::ErrorKind::InvalidInput(
                            "DELETE_KEY operation requires `public_key` being passed in the metadata"
                                .into(),
                        )
                    };
                    let metadata =
                        receiver_operation.metadata.as_ref().ok_or_else(required_fields_error)?;
                    let public_key = metadata
                        .public_key
                        .as_ref()
                        .ok_or_else(required_fields_error)?
                        .try_into()
                        .map_err(|_| {
                            crate::errors::ErrorKind::InvalidInput(format!(
                                "Invalid public_key: {:?}",
                                metadata.public_key
                            ))
                        })?;

                    actions.push(near_primitives::transaction::Action::DeleteKey(
                        near_primitives::transaction::DeleteKeyAction { public_key },
                    ))
                }
                crate::models::OperationType::DeleteAccount => {
                    update_and_fail_if_different(&mut receiver_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send to multiple recipients ('{}' and '{}')", old_value, new_value))
                        })?;
                    update_and_fail_if_different(&mut sender_account_id, &receiver_operation.account.address)
                        .map_err(|(old_value, new_value)| {
                            crate::errors::ErrorKind::InvalidInput(format!("A single transaction cannot be send from multiple senders ('{}' and '{}')", old_value, new_value))
                        })?;
                    let required_fields_error = || {
                        crate::errors::ErrorKind::InvalidInput(
                            "DELETE_ACCOUNT operation requires `beneficiary_id` being passed in the metadata"
                                .into(),
                        )
                    };
                    let metadata =
                        receiver_operation.metadata.as_ref().ok_or_else(required_fields_error)?;
                    let beneficiary_id =
                        metadata.beneficiary_id.clone().ok_or_else(required_fields_error)?.address;

                    actions.push(near_primitives::transaction::Action::DeleteAccount(
                        near_primitives::transaction::DeleteAccountAction { beneficiary_id },
                    ))
                }
            }
        }

        // We need to reverse the actions since we iterated through the operations backwards.
        actions.reverse();

        Ok(Self {
            sender_account_id: sender_account_id.ok_or_else(|| {
                crate::errors::ErrorKind::InvalidInput(
                    "There are no operations specifying signer [sender] account".to_string(),
                )
            })?,
            receiver_account_id: receiver_account_id.ok_or_else(|| {
                crate::errors::ErrorKind::InvalidInput(
                    "There are no operations specifying receiver account".to_string(),
                )
            })?,
            actions,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use super::*;

    #[test]
    fn test_near_actions_bijection() {
        let near_actions_sender_is_receiver = NearActions {
            sender_account_id: "sender.near".into(),
            receiver_account_id: "receiver.near".into(),
            actions: vec![
                near_primitives::transaction::CreateAccountAction {}.into(),
                near_primitives::transaction::DeleteAccountAction {
                    beneficiary_id: "beneficiary.near".into(),
                }
                .into(),
                near_primitives::transaction::AddKeyAction {
                    public_key: near_crypto::PublicKey::empty(near_crypto::KeyType::ED25519),
                    access_key: near_primitives::account::AccessKey::full_access(),
                }
                .into(),
                near_primitives::transaction::DeleteKeyAction {
                    public_key: near_crypto::PublicKey::empty(near_crypto::KeyType::ED25519),
                }
                .into(),
                near_primitives::transaction::StakeAction {
                    stake: 456,
                    public_key: near_crypto::PublicKey::empty(near_crypto::KeyType::ED25519),
                }
                .into(),
                near_primitives::transaction::DeployContractAction {
                    code: b"binary-code".to_vec(),
                }
                .into(),
            ],
        };

        let near_actions_sender_is_not_receiver = NearActions {
            sender_account_id: "sender.near".into(),
            receiver_account_id: "receiver.near".into(),
            actions: vec![
                near_primitives::transaction::TransferAction { deposit: 123 }.into(),
                near_primitives::transaction::FunctionCallAction {
                    method_name: "method".into(),
                    args: b"binary-args".to_vec(),
                    gas: 789,
                    deposit: 0,
                }
                .into(),
                near_primitives::transaction::FunctionCallAction {
                    method_name: "method".into(),
                    args: b"binary-args".to_vec(),
                    gas: 1011,
                    deposit: 1213,
                }
                .into(),
            ],
        };

        for near_actions in &[near_actions_sender_is_receiver, near_actions_sender_is_not_receiver]
        {
            let operations: Vec<crate::models::Operation> = near_actions.into();
            println!("Operations: {:#?}", operations);

            let near_actions_recreated = NearActions::try_from(operations.as_slice()).unwrap();

            assert_eq!(near_actions_recreated.sender_account_id, near_actions.sender_account_id);
            assert_eq!(
                near_actions_recreated.receiver_account_id,
                near_actions.receiver_account_id
            );
            assert_eq!(near_actions_recreated.actions, near_actions.actions);
        }
    }
}
