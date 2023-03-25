use actix::Addr;
use near_chain_configs::Genesis;
use near_client::ViewClientActor;
use near_o11y::WithSpanContextExt;
use validated_operations::ValidatedOperation;

mod transactions;
mod validated_operations;

/// NEAR Protocol defines initial state in genesis records and treats the first
/// block differently (e.g. [it cannot contain any
/// transactions](https://stackoverflow.com/a/63347167/1178806).
///
/// Genesis records can be huge (order of gigabytes of JSON data).  Rosetta API
/// doesn’t define any pagination and suggests to use `other_transactions` to
/// deal with this:
/// <https://community.rosetta-api.org/t/how-to-return-data-without-being-able-to-paginate/98>
/// We choose to do a proper implementation for the genesis block later.
async fn convert_genesis_records_to_transaction(
    genesis: &Genesis,
    view_client_addr: &Addr<ViewClientActor>,
    block: &near_primitives::views::BlockView,
) -> crate::errors::Result<crate::models::Transaction> {
    let mut genesis_account_ids = std::collections::HashSet::new();
    genesis.for_each_record(|record| {
        if let near_primitives::state_record::StateRecord::Account { account_id, .. } = record {
            genesis_account_ids.insert(account_id.clone());
        }
    });
    // Collect genesis accounts into a BTreeMap rather than a HashMap so that
    // the order of accounts is deterministic.  This is needed because we need
    // operations to be created in deterministic order (so that their indexes
    // stay the same).
    let genesis_accounts: std::collections::BTreeMap<_, _> = crate::utils::query_accounts(
        &near_primitives::types::BlockId::Hash(block.header.hash).into(),
        genesis_account_ids.iter(),
        view_client_addr,
    )
    .await?;
    let runtime_config = crate::utils::query_protocol_config(block.header.hash, view_client_addr)
        .await?
        .runtime_config;

    let mut operations = Vec::new();
    for (account_id, account) in genesis_accounts {
        let account_id = super::types::AccountId::from(account_id);
        let account_balances =
            crate::utils::RosettaAccountBalances::from_account(&account, &runtime_config);

        if account_balances.liquid != 0 {
            operations.push(crate::models::Operation {
                operation_identifier: crate::models::OperationIdentifier::new(&operations),
                related_operations: None,
                account: crate::models::AccountIdentifier {
                    address: account_id.clone(),
                    sub_account: None,
                    metadata: None,
                },
                amount: Some(crate::models::Amount::from_yoctonear(account_balances.liquid)),
                type_: crate::models::OperationType::Transfer,
                status: Some(crate::models::OperationStatusKind::Success),
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
                    metadata: None,
                },
                amount: Some(crate::models::Amount::from_yoctonear(
                    account_balances.liquid_for_storage,
                )),
                type_: crate::models::OperationType::Transfer,
                status: Some(crate::models::OperationStatusKind::Success),
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
                    metadata: None,
                },
                amount: Some(crate::models::Amount::from_yoctonear(account_balances.locked)),
                type_: crate::models::OperationType::Transfer,
                status: Some(crate::models::OperationStatusKind::Success),
                metadata: None,
            });
        }
    }

    Ok(crate::models::Transaction {
        transaction_identifier: crate::models::TransactionIdentifier::block_event(
            "block",
            &block.header.hash,
        ),
        operations,
        related_transactions: Vec::new(),
        metadata: crate::models::TransactionMetadata {
            type_: crate::models::TransactionType::Block,
        },
    })
}

pub(crate) async fn convert_block_to_transactions(
    view_client_addr: &Addr<ViewClientActor>,
    block: &near_primitives::views::BlockView,
) -> crate::errors::Result<Vec<crate::models::Transaction>> {
    let state_changes = view_client_addr
        .send(
            near_client::GetStateChangesInBlock { block_hash: block.header.hash }
                .with_span_context(),
        )
        .await?
        .unwrap();

    // TODO(mina86): Do we actually need ‘seen’?  I’m kinda confused at this
    // point how changes are stored in the database and whether view_client can
    // return duplicate AccountTouched entries.
    let mut seen = std::collections::HashSet::new();
    let touched_account_ids = state_changes
        .into_iter()
        .filter_map(|x| {
            if let near_primitives::views::StateChangeKindView::AccountTouched { account_id } = x {
                Some(account_id)
            } else {
                None
            }
        })
        .filter(move |account_id| {
            // TODO(mina86): Convert this to seen.get_or_insert_with(account_id,
            // Clone::clone) once hash_set_entry stabilises.
            seen.insert(account_id.clone())
        })
        .collect::<Vec<_>>();

    let prev_block_id = near_primitives::types::BlockReference::from(
        near_primitives::types::BlockId::Hash(block.header.prev_hash),
    );
    let accounts_previous_state =
        crate::utils::query_accounts(&prev_block_id, touched_account_ids.iter(), view_client_addr)
            .await?;

    let accounts_changes = view_client_addr
        .send(
            near_client::GetStateChanges {
                block_hash: block.header.hash,
                state_changes_request:
                    near_primitives::views::StateChangesRequestView::AccountChanges {
                        account_ids: touched_account_ids,
                    },
            }
            .with_span_context(),
        )
        .await??;

    let runtime_config = crate::utils::query_protocol_config(block.header.hash, view_client_addr)
        .await?
        .runtime_config;
    let exec_to_rx =
        transactions::ExecutionToReceipts::for_block(view_client_addr, block.header.hash).await?;
    transactions::convert_block_changes_to_transactions(
        view_client_addr,
        &runtime_config,
        &block.header.hash,
        accounts_changes,
        accounts_previous_state,
        exec_to_rx,
    )
    .await
    .map(|dict| dict.into_values().collect())
}

pub(crate) async fn collect_transactions(
    genesis: &Genesis,
    view_client_addr: &Addr<ViewClientActor>,
    block: &near_primitives::views::BlockView,
) -> crate::errors::Result<Vec<crate::models::Transaction>> {
    if block.header.prev_hash == Default::default() {
        Ok(vec![convert_genesis_records_to_transaction(genesis, view_client_addr, block).await?])
    } else {
        convert_block_to_transactions(view_client_addr, block).await
    }
}

/// This is used as a common denominator for matching Rosetta Operations to
/// and from NEAR Actions (see From and TryFrom implementations).
///
/// A single NEAR Action expands into 1-3 Rosetta Operations. This
/// relation is bijective (NEAR Actions -> Rosetta Operations ->
/// NEAR Actions == original NEAR Actions).
///
/// There are some helper Operation Types defined since a single operation
/// has only a single "account" field, so to indicate "sender" and "receiver"
/// we use two operations (e.g. InitiateAddKey and AddKey).
#[derive(Debug, Clone, PartialEq)]
pub struct NearActions {
    pub sender_account_id: near_primitives::types::AccountId,
    pub receiver_account_id: near_primitives::types::AccountId,
    pub actions: Vec<near_primitives::transaction::Action>,
}

impl From<NearActions> for Vec<crate::models::Operation> {
    /// Convert NEAR Actions to Rosetta Operations. It never fails.
    fn from(near_actions: NearActions) -> Self {
        let NearActions { sender_account_id, receiver_account_id, actions } = near_actions;
        let sender_account_identifier: crate::models::AccountIdentifier = sender_account_id.into();
        let receiver_account_identifier: crate::models::AccountIdentifier =
            receiver_account_id.into();
        let mut operations = vec![];
        for action in actions {
            match action {
                near_primitives::transaction::Action::CreateAccount(_) => {
                    let initiate_create_account_operation_id =
                        crate::models::OperationIdentifier::new(&operations);
                    operations.push(
                        validated_operations::InitiateCreateAccountOperation {
                            sender_account: sender_account_identifier.clone(),
                        }
                        .into_operation(initiate_create_account_operation_id.clone()),
                    );

                    operations.push(
                        validated_operations::CreateAccountOperation {
                            account: receiver_account_identifier.clone(),
                        }
                        .into_related_operation(
                            crate::models::OperationIdentifier::new(&operations),
                            vec![initiate_create_account_operation_id],
                        ),
                    );
                }

                near_primitives::transaction::Action::DeleteAccount(action) => {
                    let initiate_delete_account_operation_id =
                        crate::models::OperationIdentifier::new(&operations);
                    operations.push(
                        validated_operations::InitiateDeleteAccountOperation {
                            sender_account: sender_account_identifier.clone(),
                        }
                        .into_operation(initiate_delete_account_operation_id.clone()),
                    );

                    let delete_account_operation_id =
                        crate::models::OperationIdentifier::new(&operations);
                    operations.push(
                        validated_operations::DeleteAccountOperation {
                            account: receiver_account_identifier.clone(),
                        }
                        .into_related_operation(
                            delete_account_operation_id.clone(),
                            vec![initiate_delete_account_operation_id],
                        ),
                    );

                    operations.push(
                        validated_operations::RefundDeleteAccountOperation {
                            beneficiary_account: action.beneficiary_id.into(),
                        }
                        .into_related_operation(
                            crate::models::OperationIdentifier::new(&operations),
                            vec![delete_account_operation_id],
                        ),
                    );
                }

                near_primitives::transaction::Action::AddKey(action) => {
                    let initiate_add_key_operation_id =
                        crate::models::OperationIdentifier::new(&operations);
                    operations.push(
                        validated_operations::InitiateAddKeyOperation {
                            sender_account: sender_account_identifier.clone(),
                        }
                        .into_operation(initiate_add_key_operation_id.clone()),
                    );

                    let add_key_operation_id = crate::models::OperationIdentifier::new(&operations);
                    operations.push(
                        validated_operations::AddKeyOperation {
                            account: receiver_account_identifier.clone(),
                            public_key: (&action.public_key).into(),
                        }
                        .into_related_operation(
                            add_key_operation_id,
                            vec![initiate_add_key_operation_id],
                        ),
                    );
                }

                near_primitives::transaction::Action::DeleteKey(action) => {
                    let initiate_delete_key_operation_id =
                        crate::models::OperationIdentifier::new(&operations);
                    operations.push(
                        validated_operations::InitiateDeleteKeyOperation {
                            sender_account: sender_account_identifier.clone(),
                        }
                        .into_operation(initiate_delete_key_operation_id.clone()),
                    );

                    operations.push(
                        validated_operations::DeleteKeyOperation {
                            account: receiver_account_identifier.clone(),
                            public_key: (&action.public_key).into(),
                        }
                        .into_related_operation(
                            crate::models::OperationIdentifier::new(&operations),
                            vec![initiate_delete_key_operation_id],
                        ),
                    );
                }

                near_primitives::transaction::Action::Transfer(action) => {
                    let transfer_amount = crate::models::Amount::from_yoctonear(action.deposit);

                    let sender_transfer_operation_id =
                        crate::models::OperationIdentifier::new(&operations);
                    operations.push(
                        validated_operations::TransferOperation {
                            account: sender_account_identifier.clone(),
                            amount: -transfer_amount.clone(),
                            predecessor_id: Some(sender_account_identifier.clone()),
                        }
                        .into_operation(sender_transfer_operation_id.clone()),
                    );

                    operations.push(
                        validated_operations::TransferOperation {
                            account: receiver_account_identifier.clone(),
                            amount: transfer_amount,
                            predecessor_id: Some(sender_account_identifier.clone()),
                        }
                        .into_related_operation(
                            crate::models::OperationIdentifier::new(&operations),
                            vec![sender_transfer_operation_id],
                        ),
                    );
                }

                near_primitives::transaction::Action::Stake(action) => {
                    operations.push(
                        validated_operations::StakeOperation {
                            account: receiver_account_identifier.clone(),
                            amount: action.stake,
                            public_key: (&action.public_key).into(),
                        }
                        .into_operation(crate::models::OperationIdentifier::new(&operations)),
                    );
                }

                near_primitives::transaction::Action::DeployContract(action) => {
                    let initiate_deploy_contract_operation_id =
                        crate::models::OperationIdentifier::new(&operations);
                    operations.push(
                        validated_operations::InitiateDeployContractOperation {
                            sender_account: sender_account_identifier.clone(),
                        }
                        .into_operation(initiate_deploy_contract_operation_id.clone()),
                    );

                    operations.push(
                        validated_operations::DeployContractOperation {
                            account: receiver_account_identifier.clone(),
                            code: action.code,
                        }
                        .into_related_operation(
                            crate::models::OperationIdentifier::new(&operations),
                            vec![initiate_deploy_contract_operation_id],
                        ),
                    );
                }

                near_primitives::transaction::Action::FunctionCall(action) => {
                    let attached_amount = crate::models::Amount::from_yoctonear(action.deposit);

                    let mut related_operations = vec![];
                    if action.deposit > 0 {
                        let fund_transfer_operation_id =
                            crate::models::OperationIdentifier::new(&operations);
                        operations.push(
                            validated_operations::TransferOperation {
                                account: sender_account_identifier.clone(),
                                amount: -attached_amount.clone(),
                                predecessor_id: Some(sender_account_identifier.clone()),
                            }
                            .into_operation(fund_transfer_operation_id.clone()),
                        );
                        related_operations.push(fund_transfer_operation_id);
                    }

                    let initiate_function_call_operation_id =
                        crate::models::OperationIdentifier::new(&operations);
                    let initiate_function_call_operation =
                        validated_operations::InitiateFunctionCallOperation {
                            sender_account: sender_account_identifier.clone(),
                        }
                        .into_operation(initiate_function_call_operation_id.clone());
                    operations.push(initiate_function_call_operation);

                    related_operations.push(initiate_function_call_operation_id);
                    let deploy_contract_operation = validated_operations::FunctionCallOperation {
                        account: receiver_account_identifier.clone(),
                        method_name: action.method_name,
                        args: action.args,
                        attached_gas: action.gas,
                        attached_amount: action.deposit,
                    }
                    .into_related_operation(
                        crate::models::OperationIdentifier::new(&operations),
                        related_operations,
                    );
                    operations.push(deploy_contract_operation);
                }
                near_primitives::transaction::Action::Delegate(action) => {
                    let initiate_signed_delegate_action_operation_id =
                        crate::models::OperationIdentifier::new(&operations);
                    operations.push(
                        validated_operations::InitiateSignedDelegateActionOperation {
                            sender_account: sender_account_identifier.clone(),
                        }
                        .into_operation(initiate_signed_delegate_action_operation_id.clone()),
                    );

                    let signed_delegate_action_operation_id =
                        crate::models::OperationIdentifier::new(&operations);

                    operations.push(
                        validated_operations::signed_delegate_action::SignedDelegateActionOperation {
                            receiver_id: receiver_account_identifier.clone(),
                            signature: action.signature,
                        }
                        .into_related_operation(
                            signed_delegate_action_operation_id.clone(),
                            vec![initiate_signed_delegate_action_operation_id],
                        )
                    );

                    let initiate_delegate_action_operation_id =
                        crate::models::OperationIdentifier::new(&operations);

                    operations.push(validated_operations::initiate_delegate_action::InitiateDelegateActionOperation{
                        sender_account: action.delegate_action.sender_id.clone().into()
                    }.into_related_operation(initiate_delegate_action_operation_id.clone(), vec![signed_delegate_action_operation_id]));

                    let delegate_action_operation_id =
                        crate::models::OperationIdentifier::new(&operations);
                    let delegate_action_operation: validated_operations::DelegateActionOperation =
                        action.delegate_action.clone().into();

                    operations.push(delegate_action_operation.into_related_operation(
                        delegate_action_operation_id,
                        vec![initiate_delegate_action_operation_id],
                    ));

                    // We know that there are no delegate actions inside so this is guaranteed to
                    // be a single-level recursion.
                    let delegated_operations: Vec<crate::models::Operation> = NearActions {
                        sender_account_id: action.delegate_action.sender_id.clone(),
                        receiver_account_id: action.delegate_action.receiver_id.clone(),
                        actions: action
                            .delegate_action
                            .actions
                            .into_iter()
                            .map(|a| a.into())
                            .collect::<Vec<near_primitives::transaction::Action>>(),
                    }
                    .into();

                    operations.extend(delegated_operations);
                } // TODO(#8469): Implement delegate action support, for now they are ignored.
            }
        }
        operations
    }
}

impl TryFrom<Vec<crate::models::Operation>> for NearActions {
    type Error = crate::errors::ErrorKind;

    /// Convert Rosetta Operations to NEAR Actions.
    ///
    /// See the inverted implementation of From<NearActions> for Vec<Operations>
    /// above to understand how a single NEAR Action is represented with Rosetta
    /// Operations. The implementations are bijective (there is a test below).
    fn try_from(operations: Vec<crate::models::Operation>) -> Result<Self, Self::Error> {
        let mut sender_account_id = crate::utils::InitializeOnce::new(
            "A single transaction cannot be sent from multiple senders",
        );
        let mut receiver_account_id = crate::utils::InitializeOnce::new(
            "A single transaction cannot be sent to multiple recipients",
        );
        let mut delegate_proxy_account_id = crate::utils::InitializeOnce::new(
            "A single transaction cannot be sent by multiple proxies",
        );
        let mut actions: Vec<near_primitives::transaction::Action> = vec![];

        // Iterate over operations backwards to handle the related operations
        let mut operations = operations.into_iter().rev();
        // A single iteration consumes at least one operation from the iterator
        while let Some(tail_operation) = operations.next() {
            match tail_operation.type_ {
                crate::models::OperationType::CreateAccount => {
                    let create_account_operation =
                        validated_operations::CreateAccountOperation::try_from(tail_operation)?;
                    receiver_account_id.try_set(&create_account_operation.account)?;

                    let initiate_create_account_operation =
                        validated_operations::InitiateCreateAccountOperation::try_from_option(
                            operations.next(),
                        )?;
                    sender_account_id.try_set(&initiate_create_account_operation.sender_account)?;

                    actions.push(near_primitives::transaction::CreateAccountAction {}.into())
                }

                crate::models::OperationType::RefundDeleteAccount => {
                    let refund_delete_account_operation =
                        validated_operations::RefundDeleteAccountOperation::try_from(
                            tail_operation,
                        )?;
                    let delete_account_operation =
                        validated_operations::DeleteAccountOperation::try_from_option(
                            operations.next(),
                        )?;
                    receiver_account_id.try_set(&delete_account_operation.account)?;
                    let initiate_delete_account_operation =
                        validated_operations::InitiateDeleteAccountOperation::try_from_option(
                            operations.next(),
                        )?;
                    sender_account_id.try_set(&initiate_delete_account_operation.sender_account)?;

                    actions.push(
                        near_primitives::transaction::DeleteAccountAction {
                            beneficiary_id: refund_delete_account_operation
                                .beneficiary_account
                                .address
                                .into(),
                        }
                        .into(),
                    )
                }

                crate::models::OperationType::AddKey => {
                    let add_key_operation =
                        validated_operations::AddKeyOperation::try_from(tail_operation)?;
                    receiver_account_id.try_set(&add_key_operation.account)?;

                    let initiate_add_key_operation =
                        validated_operations::InitiateAddKeyOperation::try_from_option(
                            operations.next(),
                        )?;
                    sender_account_id.try_set(&initiate_add_key_operation.sender_account)?;

                    let public_key = (&add_key_operation.public_key).try_into().map_err(|_| {
                        crate::errors::ErrorKind::InvalidInput(format!(
                            "Invalid public_key: {:?}",
                            add_key_operation.public_key
                        ))
                    })?;

                    actions.push(
                        near_primitives::transaction::AddKeyAction {
                            access_key: near_primitives::account::AccessKey::full_access(),
                            public_key,
                        }
                        .into(),
                    )
                }

                crate::models::OperationType::DeleteKey => {
                    let delete_key_operation =
                        validated_operations::DeleteKeyOperation::try_from(tail_operation)?;
                    receiver_account_id.try_set(&delete_key_operation.account)?;

                    let initiate_delete_key_operation =
                        validated_operations::InitiateDeleteKeyOperation::try_from_option(
                            operations.next(),
                        )?;
                    sender_account_id.try_set(&initiate_delete_key_operation.sender_account)?;

                    let public_key =
                        (&delete_key_operation.public_key).try_into().map_err(|_| {
                            crate::errors::ErrorKind::InvalidInput(format!(
                                "Invalid public_key: {:?}",
                                delete_key_operation.public_key
                            ))
                        })?;

                    actions
                        .push(near_primitives::transaction::DeleteKeyAction { public_key }.into())
                }

                crate::models::OperationType::Transfer => {
                    let receiver_transfer_operation =
                        validated_operations::TransferOperation::try_from(tail_operation)?;
                    receiver_account_id.try_set(&receiver_transfer_operation.account)?;
                    if !receiver_transfer_operation.amount.value.is_positive() {
                        return Err(crate::errors::ErrorKind::InvalidInput(
                            "Receiver TRANSFER operations must have positive `amount`".to_string(),
                        ));
                    }

                    let sender_transfer_operation =
                        validated_operations::TransferOperation::try_from_option(
                            operations.next(),
                        )?;
                    sender_account_id.try_set(&sender_transfer_operation.account)?;

                    if -sender_transfer_operation.amount.value
                        != receiver_transfer_operation.amount.value
                    {
                        return Err(crate::errors::ErrorKind::InvalidInput(
                            "The sum of amounts of Sender and Receiver TRANSFER operations must be zero"
                                .to_string(),
                        ));
                    }
                    actions.push(
                        near_primitives::transaction::TransferAction {
                            deposit: receiver_transfer_operation.amount.value.absolute_difference(),
                        }
                        .into(),
                    )
                }

                crate::models::OperationType::Stake => {
                    let stake_operation =
                        validated_operations::StakeOperation::try_from(tail_operation)?;
                    receiver_account_id.try_set(&stake_operation.account)?;

                    let public_key = (&stake_operation.public_key).try_into().map_err(|_| {
                        crate::errors::ErrorKind::InvalidInput(format!(
                            "Invalid public_key: {:?}",
                            stake_operation.public_key
                        ))
                    })?;

                    actions.push(
                        near_primitives::transaction::StakeAction {
                            stake: stake_operation.amount,
                            public_key,
                        }
                        .into(),
                    )
                }

                crate::models::OperationType::DeployContract => {
                    let deploy_contract_operation =
                        validated_operations::DeployContractOperation::try_from(tail_operation)?;
                    receiver_account_id.try_set(&deploy_contract_operation.account)?;

                    let initiate_deploy_contract_operation =
                        validated_operations::InitiateDeployContractOperation::try_from_option(
                            operations.next(),
                        )?;
                    sender_account_id
                        .try_set(&initiate_deploy_contract_operation.sender_account)?;

                    actions.push(
                        near_primitives::transaction::DeployContractAction {
                            code: deploy_contract_operation.code,
                        }
                        .into(),
                    )
                }
                crate::models::OperationType::FunctionCall => {
                    let function_call_operation =
                        validated_operations::FunctionCallOperation::try_from(tail_operation)?;
                    receiver_account_id.try_set(&function_call_operation.account)?;

                    let initiate_function_call_operation =
                        validated_operations::InitiateFunctionCallOperation::try_from_option(
                            operations.next(),
                        )?;
                    sender_account_id.try_set(&initiate_function_call_operation.sender_account)?;

                    if function_call_operation.attached_amount > 0 {
                        let transfer_operation =
                            validated_operations::TransferOperation::try_from_option(
                                operations.next(),
                            )?;
                        if transfer_operation.amount.value.is_positive()
                            || transfer_operation.amount.value.absolute_difference()
                                != function_call_operation.attached_amount
                        {
                            return Err(crate::errors::ErrorKind::InvalidInput(
                                "The sum of amounts of Sender TRANSFER and Receiver FUNCTION_CALL operations must be zero"
                                    .to_string(),
                            ));
                        }
                        sender_account_id.try_set(&transfer_operation.account)?;
                    }
                    actions.push(
                        near_primitives::transaction::FunctionCallAction {
                            method_name: function_call_operation.method_name,
                            args: function_call_operation.args,
                            gas: function_call_operation.attached_gas,
                            deposit: function_call_operation.attached_amount,
                        }
                        .into(),
                    )
                }
                crate::models::OperationType::DelegateAction => {
                    let delegate_action_operation =
                        validated_operations::delegate_action::DelegateActionOperation::try_from(
                            tail_operation,
                        )?;

                    let initiate_delegate_action_operation = validated_operations::initiate_delegate_action::InitiateDelegateActionOperation::try_from_option(operations.next())?;

                    let signed_delegate_action_operation = validated_operations::signed_delegate_action::SignedDelegateActionOperation::try_from_option(operations.next())?;
                    // the "sender" of this group of operations is considered to be the delegating account, not the proxy
                    sender_account_id.try_set(&signed_delegate_action_operation.receiver_id)?;

                    let intitiate_signed_delegate_action_operation = validated_operations::intitiate_signed_delegate_action::InitiateSignedDelegateActionOperation::try_from_option(operations.next())?;
                    delegate_proxy_account_id
                        .try_set(&intitiate_signed_delegate_action_operation.sender_account)?;

                    let delegate_action: near_primitives::transaction::Action =
                        near_primitives::delegate_action::SignedDelegateAction {
                            delegate_action: near_primitives::delegate_action::DelegateAction {
                                sender_id: initiate_delegate_action_operation
                                    .sender_account
                                    .address
                                    .into(),
                                receiver_id: delegate_action_operation.receiver_id.address.into(),
                                actions: {
                                    let mut non_delegate_actions = vec![];
                                    for action in actions.into_iter() {
                                        non_delegate_actions.push(match action.try_into() {
                                            Ok(a) => a,
                                            Err(_) => {
                                                return Err(crate::errors::ErrorKind::InvalidInput(
                                                    "Nested delegate actions not allowed"
                                                        .to_string(),
                                                ))
                                            }
                                        });
                                    }
                                    non_delegate_actions
                                },
                                nonce: delegate_action_operation.nonce,
                                max_block_height: delegate_action_operation.max_block_height,
                                public_key: match (&delegate_action_operation.public_key).try_into()
                                {
                                    Ok(o) => o,
                                    Err(_) => {
                                        return Err(crate::errors::ErrorKind::InvalidInput(
                                            "Invalid public key on delegate action".to_string(),
                                        ))
                                    }
                                },
                            },
                            signature: signed_delegate_action_operation.signature,
                        }
                        .into();

                    actions = vec![delegate_action];
                }
                crate::models::OperationType::InitiateCreateAccount
                | crate::models::OperationType::InitiateDeleteAccount
                | crate::models::OperationType::InitiateAddKey
                | crate::models::OperationType::InitiateDeleteKey
                | crate::models::OperationType::InitiateDeployContract
                | crate::models::OperationType::InitiateFunctionCall
                | crate::models::OperationType::SignedDelegateAction
                | crate::models::OperationType::InitiateSignedDelegateAction
                | crate::models::OperationType::InitiateDelegateAction
                | crate::models::OperationType::DeleteAccount => {
                    return Err(crate::errors::ErrorKind::InvalidInput(format!(
                        "Unexpected operation `{:?}`",
                        tail_operation.type_
                    )))
                }
            }
        }

        // We need to reverse the actions since we iterated through the operations
        // backwards.
        actions.reverse();

        let receiver_account_id: near_primitives::types::AccountId = receiver_account_id
            .into_inner()
            .ok_or_else(|| {
                crate::errors::ErrorKind::InvalidInput(
                    "There are no operations specifying receiver account".to_string(),
                )
            })?
            .address
            .into();

        let delegate_proxy_account_id: Option<near_account_id::AccountId> =
            delegate_proxy_account_id.into_inner().map(|a| a.address.into());
        let sender_account_id: Option<near_account_id::AccountId> =
            sender_account_id.into_inner().map(|a| a.address.into());

        let actual_receiver_account_id = if delegate_proxy_account_id.is_some() {
            sender_account_id.clone().unwrap_or_else(|| receiver_account_id.clone())
        } else {
            receiver_account_id.clone()
        };
        let actual_sender_account_id =
            delegate_proxy_account_id.or_else(|| sender_account_id.clone()).unwrap_or_else(|| {
                // in case of reflexive action
                receiver_account_id.clone()
            });
        Ok(Self {
            sender_account_id: actual_sender_account_id,
            receiver_account_id: actual_receiver_account_id,
            actions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix::System;
    use near_actix_test_utils::run_actix;
    use near_client::test_utils::setup_no_network;
    use near_crypto::{KeyType, SecretKey};
    use near_primitives::delegate_action::{DelegateAction, SignedDelegateAction};
    use near_primitives::runtime::config::RuntimeConfig;
    use near_primitives::transaction::{Action, TransferAction};
    use near_primitives::views::RuntimeConfigView;

    #[test]
    fn test_convert_block_changes_to_transactions() {
        run_actix(async {
            let runtime_config: RuntimeConfigView = RuntimeConfig::test().into();
            let actor_handles = setup_no_network(
                vec!["test".parse().unwrap()],
                "other".parse().unwrap(),
                true,
                false,
            );
            let block_hash = near_primitives::hash::CryptoHash::default();
            let nfvalidator1_receipt_processing_hash = near_primitives::hash::CryptoHash([1u8; 32]);
            let nfvalidator2_action_receipt_gas_reward_hash =
                near_primitives::hash::CryptoHash([2u8; 32]);
            let accounts_changes = vec![
                near_primitives::views::StateChangeWithCauseView {
                    cause: near_primitives::views::StateChangeCauseView::ValidatorAccountsUpdate,
                    value: near_primitives::views::StateChangeValueView::AccountUpdate {
                        account_id: "nfvalidator1.near".parse().unwrap(),
                        account: near_primitives::views::AccountView {
                            amount: 5000000000000000000,
                            code_hash: near_primitives::hash::CryptoHash::default(),
                            locked: 400000000000000000000000000000,
                            storage_paid_at: 0,
                            storage_usage: 200000,
                        },
                    },
                },
                near_primitives::views::StateChangeWithCauseView {
                    cause: near_primitives::views::StateChangeCauseView::ReceiptProcessing {
                        receipt_hash: nfvalidator1_receipt_processing_hash,
                    },
                    value: near_primitives::views::StateChangeValueView::AccountUpdate {
                        account_id: "nfvalidator1.near".parse().unwrap(),
                        account: near_primitives::views::AccountView {
                            amount: 4000000000000000000,
                            code_hash: near_primitives::hash::CryptoHash::default(),
                            locked: 400000000000000000000000000000,
                            storage_paid_at: 0,
                            storage_usage: 200000,
                        },
                    },
                },
                near_primitives::views::StateChangeWithCauseView {
                    cause: near_primitives::views::StateChangeCauseView::ValidatorAccountsUpdate,
                    value: near_primitives::views::StateChangeValueView::AccountUpdate {
                        account_id: "nfvalidator2.near".parse().unwrap(),
                        account: near_primitives::views::AccountView {
                            amount: 7000000000000000000,
                            code_hash: near_primitives::hash::CryptoHash::default(),
                            locked: 400000000000000000000000000000,
                            storage_paid_at: 0,
                            storage_usage: 200000,
                        },
                    },
                },
                near_primitives::views::StateChangeWithCauseView {
                    cause: near_primitives::views::StateChangeCauseView::ActionReceiptGasReward {
                        receipt_hash: nfvalidator2_action_receipt_gas_reward_hash,
                    },
                    value: near_primitives::views::StateChangeValueView::AccountUpdate {
                        account_id: "nfvalidator2.near".parse().unwrap(),
                        account: near_primitives::views::AccountView {
                            amount: 8000000000000000000,
                            code_hash: near_primitives::hash::CryptoHash::default(),
                            locked: 400000000000000000000000000000,
                            storage_paid_at: 0,
                            storage_usage: 200000,
                        },
                    },
                },
            ];
            let mut accounts_previous_state = std::collections::HashMap::new();
            accounts_previous_state.insert(
                "nfvalidator1.near".parse().unwrap(),
                near_primitives::views::AccountView {
                    amount: 4000000000000000000,
                    code_hash: near_primitives::hash::CryptoHash::default(),
                    locked: 400000000000000000000000000000,
                    storage_paid_at: 0,
                    storage_usage: 200000,
                },
            );
            accounts_previous_state.insert(
                "nfvalidator2.near".parse().unwrap(),
                near_primitives::views::AccountView {
                    amount: 6000000000000000000,
                    code_hash: near_primitives::hash::CryptoHash::default(),
                    locked: 400000000000000000000000000000,
                    storage_paid_at: 0,
                    storage_usage: 200000,
                },
            );
            let transactions = super::transactions::convert_block_changes_to_transactions(
                &actor_handles.view_client_actor,
                &runtime_config,
                &block_hash,
                accounts_changes,
                accounts_previous_state,
                super::transactions::ExecutionToReceipts::empty(),
            )
            .await
            .unwrap();
            assert_eq!(transactions.len(), 3);
            assert!(transactions.iter().all(|(transaction_hash, transaction)| {
                &transaction.transaction_identifier.hash == transaction_hash
            }));

            let validators_update_transaction =
                &transactions[&format!("block-validators-update:{}", block_hash)];
            insta::assert_debug_snapshot!(
                "validators_update_transaction",
                validators_update_transaction
            );

            let nfvalidator1_receipt_processing_transaction =
                &transactions[&format!("receipt:{}", nfvalidator1_receipt_processing_hash)];
            insta::assert_debug_snapshot!(
                "nfvalidator1_receipt_processing_transaction",
                nfvalidator1_receipt_processing_transaction
            );

            let nfvalidator2_action_receipt_gas_reward_transaction =
                &transactions[&format!("receipt:{}", nfvalidator2_action_receipt_gas_reward_hash)];
            insta::assert_debug_snapshot!(
                "nfvalidator2_action_receipt_gas_reward_transaction",
                nfvalidator2_action_receipt_gas_reward_transaction
            );
            System::current().stop();
        });
    }

    #[test]
    fn test_near_actions_bijection() {
        let create_account_actions =
            vec![near_primitives::transaction::CreateAccountAction {}.into()];
        let delete_account_actions = vec![near_primitives::transaction::DeleteAccountAction {
            beneficiary_id: "beneficiary.near".parse().unwrap(),
        }
        .into()];
        let add_key_actions = vec![near_primitives::transaction::AddKeyAction {
            access_key: near_primitives::account::AccessKey::full_access(),
            public_key: near_crypto::SecretKey::from_random(near_crypto::KeyType::ED25519)
                .public_key(),
        }
        .into()];
        let delete_key_actions = vec![near_primitives::transaction::DeleteKeyAction {
            public_key: near_crypto::SecretKey::from_random(near_crypto::KeyType::ED25519)
                .public_key(),
        }
        .into()];
        let transfer_actions = vec![near_primitives::transaction::TransferAction {
            deposit: near_primitives::types::Balance::MAX,
        }
        .into()];
        let stake_actions = vec![near_primitives::transaction::StakeAction {
            stake: 456,
            public_key: near_crypto::SecretKey::from_random(near_crypto::KeyType::ED25519)
                .public_key(),
        }
        .into()];
        let deploy_contract_actions = vec![near_primitives::transaction::DeployContractAction {
            code: b"binary-data".to_vec(),
        }
        .into()];
        let function_call_without_balance_actions =
            vec![near_primitives::transaction::FunctionCallAction {
                method_name: "method-name".parse().unwrap(),
                args: b"args".to_vec(),
                gas: 100500,
                deposit: 0,
            }
            .into()];
        let function_call_with_balance_actions =
            vec![near_primitives::transaction::FunctionCallAction {
                method_name: "method-name".parse().unwrap(),
                args: b"args".to_vec(),
                gas: 100500,
                deposit: near_primitives::types::Balance::MAX,
            }
            .into()];

        let wallet_style_create_account_actions =
            [create_account_actions.to_vec(), add_key_actions.to_vec(), transfer_actions.to_vec()]
                .concat();
        let create_account_and_stake_immediately_actions =
            [create_account_actions.to_vec(), transfer_actions.to_vec(), stake_actions.to_vec()]
                .concat();
        let deploy_contract_and_call_it_actions =
            [deploy_contract_actions.to_vec(), function_call_with_balance_actions.to_vec()]
                .concat();
        let two_factor_auth_actions = [
            delete_key_actions.to_vec(),
            add_key_actions.to_vec(),
            add_key_actions.to_vec(),
            deploy_contract_actions.to_vec(),
            function_call_without_balance_actions.to_vec(),
        ]
        .concat();

        let non_sir_compatible_actions = vec![
            create_account_actions,
            delete_account_actions,
            add_key_actions,
            delete_key_actions,
            transfer_actions,
            deploy_contract_actions,
            function_call_without_balance_actions,
            function_call_with_balance_actions,
            wallet_style_create_account_actions,
            create_account_and_stake_immediately_actions,
            deploy_contract_and_call_it_actions,
            two_factor_auth_actions,
        ];

        for actions in non_sir_compatible_actions.clone() {
            let near_actions = NearActions {
                sender_account_id: "sender.near".parse().unwrap(),
                receiver_account_id: "receiver.near".parse().unwrap(),
                actions,
            };
            println!("NEAR Actions: {:#?}", near_actions);
            let operations: Vec<crate::models::Operation> = near_actions.clone().into();
            println!("Operations: {:#?}", operations);

            let near_actions_recreated = NearActions::try_from(operations).unwrap();

            assert_eq!(near_actions_recreated.sender_account_id, near_actions.sender_account_id);
            assert_eq!(
                near_actions_recreated.receiver_account_id,
                near_actions.receiver_account_id
            );
            assert_eq!(near_actions_recreated.actions, near_actions.actions);
        }

        let sir_compatible_actions = [non_sir_compatible_actions, vec![stake_actions]].concat();
        for actions in sir_compatible_actions {
            let near_actions = NearActions {
                sender_account_id: "sender-is-receiver.near".parse().unwrap(),
                receiver_account_id: "sender-is-receiver.near".parse().unwrap(),
                actions,
            };
            println!("NEAR Actions: {:#?}", near_actions);
            let operations: Vec<crate::models::Operation> = near_actions.clone().into();
            println!("Operations: {:#?}", operations);

            let near_actions_recreated = NearActions::try_from(operations).unwrap();

            assert_eq!(near_actions_recreated.sender_account_id, near_actions.sender_account_id);
            assert_eq!(
                near_actions_recreated.receiver_account_id,
                near_actions.receiver_account_id
            );
            assert_eq!(near_actions_recreated.actions, near_actions.actions);
        }
    }

    #[test]
    fn test_delegate_actions_bijection() {
        // dummy key
        let sk = SecretKey::from_seed(KeyType::ED25519, "");

        let original_near_actions = NearActions {
            sender_account_id: "proxy.near".parse().unwrap(),
            receiver_account_id: "account.near".parse().unwrap(),
            actions: vec![Action::Delegate(SignedDelegateAction {
                delegate_action: DelegateAction {
                    sender_id: "account.near".parse().unwrap(),
                    receiver_id: "receiver.near".parse().unwrap(),
                    actions: vec![Action::Transfer(TransferAction { deposit: 1 })
                        .try_into()
                        .unwrap()],
                    nonce: 0,
                    max_block_height: 0,
                    public_key: sk.public_key(),
                },
                signature: sk.sign(&[0]),
            })],
        };

        let operations: Vec<crate::models::Operation> =
            original_near_actions.clone().try_into().unwrap();

        let converted_near_actions = NearActions::try_from(operations).unwrap();

        assert_eq!(converted_near_actions, original_near_actions);
    }

    #[test]
    fn test_near_actions_invalid_transfer_no_amount() {
        let operations = vec![crate::models::Operation {
            type_: crate::models::OperationType::Transfer,
            account: "sender.near".parse().unwrap(),
            amount: None,
            operation_identifier: crate::models::OperationIdentifier::new(&[]),
            related_operations: None,
            status: None,
            metadata: None,
        }];
        assert!(matches!(
            NearActions::try_from(operations),
            Err(crate::errors::ErrorKind::InvalidInput(_))
        ));
    }

    #[test]
    fn test_near_actions_invalid_transfer_negative_receiver_amount() {
        let operations = vec![crate::models::Operation {
            type_: crate::models::OperationType::Transfer,
            account: "sender.near".parse().unwrap(),
            amount: Some(-crate::models::Amount::from_yoctonear(1)),
            operation_identifier: crate::models::OperationIdentifier::new(&[]),
            related_operations: None,
            status: None,
            metadata: None,
        }];
        assert!(matches!(
            NearActions::try_from(operations),
            Err(crate::errors::ErrorKind::InvalidInput(_))
        ));
    }

    #[test]
    fn test_near_actions_invalid_transfer_mismatching_amount() {
        let sender_transfer_operation_id =
            crate::models::OperationIdentifier { index: 0, network_index: None };
        let receiver_transfer_operation_id =
            crate::models::OperationIdentifier { index: 1, network_index: None };

        let operations = vec![
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "sender.near".parse().unwrap(),
                amount: Some(-crate::models::Amount::from_yoctonear(2)),
                operation_identifier: sender_transfer_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "receiver.near".parse().unwrap(),
                amount: Some(crate::models::Amount::from_yoctonear(1)),
                operation_identifier: receiver_transfer_operation_id,
                related_operations: Some(vec![sender_transfer_operation_id]),
                status: None,
                metadata: None,
            },
        ];
        assert!(matches!(
            NearActions::try_from(operations),
            Err(crate::errors::ErrorKind::InvalidInput(_))
        ));
    }

    #[test]
    fn test_near_actions_invalid_transfer_mismatching_sign_amount() {
        let sender_transfer_operation_id =
            crate::models::OperationIdentifier { index: 0, network_index: None };
        let receiver_transfer_operation_id =
            crate::models::OperationIdentifier { index: 1, network_index: None };

        let operations = vec![
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "sender.near".parse().unwrap(),
                amount: Some(crate::models::Amount::from_yoctonear(1)),
                operation_identifier: sender_transfer_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "receiver.near".parse().unwrap(),
                amount: Some(crate::models::Amount::from_yoctonear(1)),
                operation_identifier: receiver_transfer_operation_id,
                related_operations: Some(vec![sender_transfer_operation_id]),
                status: None,
                metadata: None,
            },
        ];
        assert!(matches!(
            NearActions::try_from(operations),
            Err(crate::errors::ErrorKind::InvalidInput(_))
        ));
    }

    #[test]
    fn test_near_actions_invalid_transfer_mismatching_zero_sender_amount() {
        let sender_transfer_operation_id =
            crate::models::OperationIdentifier { index: 0, network_index: None };
        let receiver_transfer_operation_id =
            crate::models::OperationIdentifier { index: 1, network_index: None };

        let operations = vec![
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "sender.near".parse().unwrap(),
                amount: Some(crate::models::Amount::from_yoctonear(0)),
                operation_identifier: sender_transfer_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "receiver.near".parse().unwrap(),
                amount: Some(crate::models::Amount::from_yoctonear(1)),
                operation_identifier: receiver_transfer_operation_id,
                related_operations: Some(vec![sender_transfer_operation_id]),
                status: None,
                metadata: None,
            },
        ];
        assert!(matches!(
            NearActions::try_from(operations),
            Err(crate::errors::ErrorKind::InvalidInput(_))
        ));
    }

    #[test]
    fn test_near_actions_invalid_transfer_mismatching_zero_receiver_amount() {
        let sender_transfer_operation_id =
            crate::models::OperationIdentifier { index: 0, network_index: None };
        let receiver_transfer_operation_id =
            crate::models::OperationIdentifier { index: 1, network_index: None };

        let operations = vec![
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "sender.near".parse().unwrap(),
                amount: Some(-crate::models::Amount::from_yoctonear(1)),
                operation_identifier: sender_transfer_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "receiver.near".parse().unwrap(),
                amount: Some(crate::models::Amount::from_yoctonear(0)),
                operation_identifier: receiver_transfer_operation_id,
                related_operations: Some(vec![sender_transfer_operation_id]),
                status: None,
                metadata: None,
            },
        ];

        assert!(matches!(
            NearActions::try_from(operations),
            Err(crate::errors::ErrorKind::InvalidInput(_))
        ));
    }

    #[test]
    fn test_near_actions_invalid_function_call_without_fund_amount() {
        let fund_transfer_function_call_operation_id =
            crate::models::OperationIdentifier { index: 0, network_index: None };
        let initiate_function_call_operation_id =
            crate::models::OperationIdentifier { index: 1, network_index: None };
        let function_call_operation_id =
            crate::models::OperationIdentifier { index: 2, network_index: None };

        let operations = vec![
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "sender.near".parse().unwrap(),
                amount: None,
                operation_identifier: fund_transfer_function_call_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::InitiateFunctionCall,
                account: "sender.near".parse().unwrap(),
                amount: None,
                operation_identifier: initiate_function_call_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::FunctionCall,
                account: "sender.near".parse().unwrap(),
                amount: Some(crate::models::Amount::from_yoctonear(1)),
                operation_identifier: function_call_operation_id,
                related_operations: Some(vec![
                    fund_transfer_function_call_operation_id,
                    initiate_function_call_operation_id,
                ]),
                status: None,
                metadata: Some(crate::models::OperationMetadata {
                    method_name: Some("method-name".parse().unwrap()),
                    args: Some(b"binary-args".to_vec().into()),
                    attached_gas: Some(123.into()),
                    ..Default::default()
                }),
            },
        ];
        assert!(matches!(
            NearActions::try_from(operations),
            Err(crate::errors::ErrorKind::InvalidInput(_))
        ));
    }

    #[test]
    fn test_near_actions_invalid_function_call_with_zero_fund_amount() {
        let fund_transfer_function_call_operation_id =
            crate::models::OperationIdentifier { index: 0, network_index: None };
        let initiate_function_call_operation_id =
            crate::models::OperationIdentifier { index: 1, network_index: None };
        let function_call_operation_id =
            crate::models::OperationIdentifier { index: 2, network_index: None };

        let operations = vec![
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "sender.near".parse().unwrap(),
                // This is expected to be negative to match the amount in the FunctionCallOperation
                amount: Some(crate::models::Amount::from_yoctonear(0)),
                operation_identifier: fund_transfer_function_call_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::InitiateFunctionCall,
                account: "sender.near".parse().unwrap(),
                amount: None,
                operation_identifier: initiate_function_call_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::FunctionCall,
                account: "sender.near".parse().unwrap(),
                amount: Some(crate::models::Amount::from_yoctonear(1)),
                operation_identifier: function_call_operation_id,
                related_operations: Some(vec![
                    fund_transfer_function_call_operation_id,
                    initiate_function_call_operation_id,
                ]),
                status: None,
                metadata: Some(crate::models::OperationMetadata {
                    method_name: Some("method-name".parse().unwrap()),
                    args: Some(b"binary-args".to_vec().into()),
                    attached_gas: Some(123.into()),
                    ..Default::default()
                }),
            },
        ];
        assert!(matches!(
            NearActions::try_from(operations),
            Err(crate::errors::ErrorKind::InvalidInput(_))
        ));
    }

    #[test]
    fn test_near_actions_invalid_function_call_with_positive_fund_amount() {
        let fund_transfer_function_call_operation_id =
            crate::models::OperationIdentifier { index: 0, network_index: None };
        let initiate_function_call_operation_id =
            crate::models::OperationIdentifier { index: 1, network_index: None };
        let function_call_operation_id =
            crate::models::OperationIdentifier { index: 2, network_index: None };

        let operations = vec![
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "sender.near".parse().unwrap(),
                // This is expected to be negative to match the amount in the FunctionCallOperation
                amount: Some(crate::models::Amount::from_yoctonear(1)),
                operation_identifier: fund_transfer_function_call_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::InitiateFunctionCall,
                account: "sender.near".parse().unwrap(),
                amount: None,
                operation_identifier: initiate_function_call_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::FunctionCall,
                account: "sender.near".parse().unwrap(),
                amount: Some(crate::models::Amount::from_yoctonear(1)),
                operation_identifier: function_call_operation_id,
                related_operations: Some(vec![
                    fund_transfer_function_call_operation_id,
                    initiate_function_call_operation_id,
                ]),
                status: None,
                metadata: Some(crate::models::OperationMetadata {
                    method_name: Some("method-name".parse().unwrap()),
                    args: Some(b"binary-args".to_vec().into()),
                    attached_gas: Some(123.into()),
                    ..Default::default()
                }),
            },
        ];
        assert!(matches!(
            NearActions::try_from(operations),
            Err(crate::errors::ErrorKind::InvalidInput(_))
        ));
    }

    #[test]
    fn test_near_actions_invalid_function_call_non_matching_amounts() {
        let fund_transfer_function_call_operation_id =
            crate::models::OperationIdentifier { index: 0, network_index: None };
        let initiate_function_call_operation_id =
            crate::models::OperationIdentifier { index: 1, network_index: None };
        let function_call_operation_id =
            crate::models::OperationIdentifier { index: 2, network_index: None };

        let operations = vec![
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "sender.near".parse().unwrap(),
                // This is expected to match the amount in the FunctionCallOperation
                amount: Some(-crate::models::Amount::from_yoctonear(2)),
                operation_identifier: fund_transfer_function_call_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::InitiateFunctionCall,
                account: "sender.near".parse().unwrap(),
                amount: None,
                operation_identifier: initiate_function_call_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::FunctionCall,
                account: "sender.near".parse().unwrap(),
                amount: Some(crate::models::Amount::from_yoctonear(1)),
                operation_identifier: function_call_operation_id,
                related_operations: Some(vec![
                    fund_transfer_function_call_operation_id,
                    initiate_function_call_operation_id,
                ]),
                status: None,
                metadata: Some(crate::models::OperationMetadata {
                    method_name: Some("method-name".parse().unwrap()),
                    args: Some(b"binary-args".to_vec().into()),
                    attached_gas: Some(123.into()),
                    ..Default::default()
                }),
            },
        ];
        assert!(matches!(
            NearActions::try_from(operations),
            Err(crate::errors::ErrorKind::InvalidInput(_))
        ));
    }

    #[test]
    fn test_near_actions_invalid_function_call_with_negative_deposit() {
        let fund_transfer_function_call_operation_id =
            crate::models::OperationIdentifier { index: 0, network_index: None };
        let initiate_function_call_operation_id =
            crate::models::OperationIdentifier { index: 1, network_index: None };
        let function_call_operation_id =
            crate::models::OperationIdentifier { index: 2, network_index: None };

        let operations = vec![
            crate::models::Operation {
                type_: crate::models::OperationType::Transfer,
                account: "sender.near".parse().unwrap(),
                amount: Some(-crate::models::Amount::from_yoctonear(1)),
                operation_identifier: fund_transfer_function_call_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::InitiateFunctionCall,
                account: "sender.near".parse().unwrap(),
                amount: None,
                operation_identifier: initiate_function_call_operation_id.clone(),
                related_operations: None,
                status: None,
                metadata: None,
            },
            crate::models::Operation {
                type_: crate::models::OperationType::FunctionCall,
                account: "sender.near".parse().unwrap(),
                // This is expected to be positive
                amount: Some(-crate::models::Amount::from_yoctonear(1)),
                operation_identifier: function_call_operation_id,
                related_operations: Some(vec![
                    fund_transfer_function_call_operation_id,
                    initiate_function_call_operation_id,
                ]),
                status: None,
                metadata: Some(crate::models::OperationMetadata {
                    method_name: Some("method-name".parse().unwrap()),
                    args: Some(b"binary-args".to_vec().into()),
                    attached_gas: Some(123.into()),
                    ..Default::default()
                }),
            },
        ];

        assert!(matches!(
            NearActions::try_from(operations),
            Err(crate::errors::ErrorKind::InvalidInput(_))
        ));
    }
}
