use std::string::ToString;

use near_primitives::hash::CryptoHash;

use actix::Addr;

/// Constructs a Rosetta transaction hash for a change with a given cause.
///
/// If the change happened due to a transaction or a receipt, returns hash of
/// that transaction as well.  If the change was due to another reason, the
/// second returned value will be None.
///
/// Transactions are significant because we need to generate list of receipts as
/// related transactions so that Rosetta API clients can match transactions and
/// receipts together.
///
/// Returns error if unexpected cause was encountered.
fn convert_cause_to_transaction_id(
    block_hash: &CryptoHash,
    cause: near_primitives::views::StateChangeCauseView,
) -> crate::errors::Result<(crate::models::TransactionIdentifier, Option<CryptoHash>)> {
    use crate::models::TransactionIdentifier;
    use near_primitives::views::StateChangeCauseView;

    match cause {
        StateChangeCauseView::TransactionProcessing { tx_hash } => {
            Ok((TransactionIdentifier::transaction(&tx_hash), Some(tx_hash)))
        }
        StateChangeCauseView::ActionReceiptProcessingStarted { receipt_hash }
        | StateChangeCauseView::ActionReceiptGasReward { receipt_hash }
        | StateChangeCauseView::ReceiptProcessing { receipt_hash }
        | StateChangeCauseView::PostponedReceipt { receipt_hash } => {
            Ok((TransactionIdentifier::receipt(&receipt_hash), Some(receipt_hash)))
        }
        StateChangeCauseView::InitialState => {
            Ok((TransactionIdentifier::block_event("block", block_hash), None))
        }
        StateChangeCauseView::ValidatorAccountsUpdate => {
            Ok((TransactionIdentifier::block_event("block-validators-update", block_hash), None))
        }
        StateChangeCauseView::UpdatedDelayedReceipts => {
            Ok((TransactionIdentifier::block_event("block-delayed-receipts", block_hash), None))
        }
        StateChangeCauseView::NotWritableToDisk => {
            Err(crate::errors::ErrorKind::InternalInvariantError(
                "State Change 'NotWritableToDisk' should never be observed".to_string(),
            ))
        }
        StateChangeCauseView::Migration => {
            Ok((TransactionIdentifier::block_event("migration", block_hash), None))
        }
        StateChangeCauseView::Resharding => Err(crate::errors::ErrorKind::InternalInvariantError(
            "State Change 'Resharding' should never be observed".to_string(),
        )),
    }
}

/// An builder object for generating lists of Rosetta transactions based on
/// changes seen in given block.
pub(crate) struct BlockChangesToTransactionsConverter<'a> {
    map: std::collections::HashMap<String, crate::models::Transaction>,

    /// Hash of the block for which list of transaction is created.  This is
    /// used for generating Rosetta transaction identifiers for block events.
    block_hash: &'a CryptoHash,

    /// Factory for generating [`crate::models::Transaction`] objects.  In
    /// production code this is always Some and generates transactions with list
    /// of related transactions.  In test code this may be None in which case
    /// the transactions will be generated with empty related transactions list.
    factory: Option<TransactionsFactory<'a>>,
}

struct TransactionsFactory<'a> {
    view_client_addr: Addr<near_client::ViewClientActor>,

    block: &'a near_primitives::views::BlockView,

    transactions: Option<std::collections::HashMap<CryptoHash, Vec<CryptoHash>>>,
}

impl<'a> BlockChangesToTransactionsConverter<'a> {
    /// Creates a new converter for creating list of transactions in specified
    /// block.
    pub(crate) fn new(
        view_client_addr: Addr<near_client::ViewClientActor>,
        block: &'a near_primitives::views::BlockView,
    ) -> Self {
        Self {
            map: std::collections::HashMap::new(),
            block_hash: &block.header.hash,
            factory: Some(TransactionsFactory { view_client_addr, block, transactions: None }),
        }
    }

    /// Creates a new converter for creating list of transactions in specified
    /// block except that no related transactions will be included.
    #[cfg(test)]
    pub(crate) fn new_for_tests(hash: &'a CryptoHash) -> Self {
        Self { map: std::collections::HashMap::new(), block_hash: hash, factory: None }
    }

    /// Returns Rosetta transactions mapping to given account state changes.
    pub(crate) async fn convert(
        mut self,
        runtime_config: &near_primitives::runtime::config::RuntimeConfig,
        accounts_changes: near_primitives::views::StateChangesView,
        accounts_previous_state: std::collections::HashMap<
            near_primitives::types::AccountId,
            near_primitives::views::AccountView,
        >,
    ) -> crate::errors::Result<std::collections::HashMap<String, crate::models::Transaction>> {
        convert_block_changes_to_transactions(
            &mut self,
            runtime_config,
            accounts_changes,
            accounts_previous_state,
        )
        .await?;
        Ok(self.map)
    }

    /// Returns a Rosetta transaction object for given state change cause.
    ///
    /// `transaction_identifier`, `related_transactions` and `metadata` of the
    /// object will be populated but initially the `operations` will be an empty
    /// vector.  It’s caller’s responsibility to fill it out as required.
    async fn get_for_cause(
        &mut self,
        cause: near_primitives::views::StateChangeCauseView,
    ) -> crate::errors::Result<&mut crate::models::Transaction> {
        use std::collections::hash_map::Entry;
        let (identifier, hash) = convert_cause_to_transaction_id(&self.block_hash, cause)?;
        Ok(match self.map.entry(identifier.hash) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let tx = Self::make_transaction(&mut self.factory, entry.key(), hash).await?;
                entry.insert(tx)
            }
        })
    }

    /// Constructs a new Rosetta transaction object.
    ///
    /// If `trx_hash` is not `None`, fills out transaction’s
    /// `related_transactions` with receipts given transaction/receipt
    /// generated.
    async fn make_transaction(
        factory: &mut Option<TransactionsFactory<'a>>,
        hash: &String,
        trx_hash: Option<CryptoHash>,
    ) -> crate::errors::Result<crate::models::Transaction> {
        let id = crate::models::TransactionIdentifier { hash: hash.clone() };
        let related_transactions = if let (Some(factory), Some(hash)) = (factory, trx_hash) {
            factory.get_related(hash).await?
        } else {
            None
        };
        Ok(crate::models::Transaction {
            transaction_identifier: id,
            operations: Vec::new(),
            related_transactions: related_transactions.unwrap_or_default(),
            metadata: crate::models::TransactionMetadata {
                type_: crate::models::TransactionType::Transaction,
            },
        })
    }
}

impl<'a> TransactionsFactory<'a> {
    /// Returns related transactions for given NEAR transaction or receipt.
    async fn get_related(
        &mut self,
        trx_hash: CryptoHash,
    ) -> crate::errors::Result<Option<Vec<crate::models::RelatedTransaction>>> {
        let related = self.get_transactions_map().await?.remove(&trx_hash).map(|hashes| {
            hashes
                .iter()
                .map(crate::models::TransactionIdentifier::receipt)
                .map(crate::models::RelatedTransaction::forward)
                .collect()
        });
        Ok(related)
    }

    async fn get_transactions_map(
        &mut self,
    ) -> crate::errors::Result<&mut std::collections::HashMap<CryptoHash, Vec<CryptoHash>>> {
        Ok(if let Some(ref mut transactions) = self.transactions {
            transactions
        } else {
            let transactions = self
                .view_client_addr
                .send(near_client::GetExecutionOutcomesForBlock {
                    block_hash: self.block.header.hash,
                })
                .await?
                .map_err(crate::errors::ErrorKind::InternalInvariantError)?
                .into_values()
                .flat_map(|outcomes| outcomes)
                .map(|exec| (exec.id, exec.outcome.receipt_ids))
                .collect();
            self.transactions.insert(transactions)
        })
    }
}

async fn convert_block_changes_to_transactions<'a>(
    transactions: &mut BlockChangesToTransactionsConverter<'a>,
    runtime_config: &near_primitives::runtime::config::RuntimeConfig,
    accounts_changes: near_primitives::views::StateChangesView,
    mut accounts_previous_state: std::collections::HashMap<
        near_primitives::types::AccountId,
        near_primitives::views::AccountView,
    >,
) -> crate::errors::Result<()> {
    for account_change in accounts_changes {
        let transaction = transactions.get_for_cause(account_change.cause).await?;
        let operations = &mut transaction.operations;
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

    Ok(())
}
