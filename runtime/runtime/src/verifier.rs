use near_crypto::key_conversion::is_valid_staking_key;
use near_primitives::runtime::get_insufficient_storage_stake;
use near_primitives::{
    account::AccessKeyPermission,
    config::VMLimitConfig,
    errors::{
        ActionsValidationError, InvalidAccessKeyError, InvalidTxError, ReceiptValidationError,
        RuntimeError,
    },
    receipt::{ActionReceipt, DataReceipt, Receipt, ReceiptEnum},
    transaction::{
        Action, AddKeyAction, DeleteAccountAction, DeployContractAction, FunctionCallAction,
        SignedTransaction, StakeAction,
    },
    types::Balance,
    version::ProtocolVersion,
};
use near_runtime_utils::is_valid_account_id;
use near_store::{
    get_access_key, get_account, set_access_key, set_account, StorageError, TrieUpdate,
};

use crate::config::{total_prepaid_gas, tx_cost, TransactionCost};
use crate::VerificationResult;
use near_primitives::checked_feature;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::types::BlockHeight;

/// Validates the transaction without using the state. It allows any node to validate a
/// transaction before forwarding it to the node that tracks the `signer_id` account.
pub fn validate_transaction(
    config: &RuntimeConfig,
    gas_price: Balance,
    signed_transaction: &SignedTransaction,
    verify_signature: bool,
    current_protocol_version: ProtocolVersion,
) -> Result<TransactionCost, RuntimeError> {
    let transaction = &signed_transaction.transaction;
    let signer_id = &transaction.signer_id;
    if !is_valid_account_id(&signer_id) {
        return Err(InvalidTxError::InvalidSignerId { signer_id: signer_id.clone() }.into());
    }
    if !is_valid_account_id(&transaction.receiver_id) {
        return Err(InvalidTxError::InvalidReceiverId {
            receiver_id: transaction.receiver_id.clone(),
        }
        .into());
    }

    if verify_signature
        && !signed_transaction
            .signature
            .verify(signed_transaction.get_hash().as_ref(), &transaction.public_key)
    {
        return Err(InvalidTxError::InvalidSignature.into());
    }

    let transaction_size = signed_transaction.get_size();
    let max_transaction_size = config.wasm_config.limit_config.max_transaction_size;
    if transaction_size > max_transaction_size {
        return Err(InvalidTxError::TransactionSizeExceeded {
            size: transaction_size,
            limit: max_transaction_size,
        }
        .into());
    }

    validate_actions(&config.wasm_config.limit_config, &transaction.actions)
        .map_err(|e| InvalidTxError::ActionsValidation(e))?;

    let sender_is_receiver = &transaction.receiver_id == signer_id;

    tx_cost(
        &config.transaction_costs,
        &transaction,
        gas_price,
        sender_is_receiver,
        current_protocol_version,
    )
    .map_err(|_| InvalidTxError::CostOverflow.into())
}

/// Verifies the signed transaction on top of given state, charges transaction fees
/// and balances, and updates the state for the used account and access keys.
pub fn verify_and_charge_transaction(
    config: &RuntimeConfig,
    state_update: &mut TrieUpdate,
    gas_price: Balance,
    signed_transaction: &SignedTransaction,
    verify_signature: bool,
    #[allow(unused)] block_height: Option<BlockHeight>,
    current_protocol_version: ProtocolVersion,
) -> Result<VerificationResult, RuntimeError> {
    let TransactionCost { gas_burnt, gas_remaining, receipt_gas_price, total_cost, burnt_amount } =
        validate_transaction(
            config,
            gas_price,
            signed_transaction,
            verify_signature,
            current_protocol_version,
        )?;
    let transaction = &signed_transaction.transaction;
    let signer_id = &transaction.signer_id;

    let mut signer = match get_account(state_update, signer_id)? {
        Some(signer) => signer,
        None => {
            return Err(InvalidTxError::SignerDoesNotExist { signer_id: signer_id.clone() }.into());
        }
    };
    let mut access_key = match get_access_key(state_update, &signer_id, &transaction.public_key)? {
        Some(access_key) => access_key,
        None => {
            return Err(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::AccessKeyNotFound {
                    account_id: signer_id.clone(),
                    public_key: transaction.public_key.clone(),
                },
            )
            .into());
        }
    };

    if transaction.nonce <= access_key.nonce {
        return Err(InvalidTxError::InvalidNonce {
            tx_nonce: transaction.nonce,
            ak_nonce: access_key.nonce,
        }
        .into());
    }
    if checked_feature!("stable", AccessKeyNonceRange, current_protocol_version) {
        if let Some(height) = block_height {
            let upper_bound =
                height * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER;
            if transaction.nonce >= upper_bound {
                return Err(InvalidTxError::NonceTooLarge {
                    tx_nonce: transaction.nonce,
                    upper_bound,
                }
                .into());
            }
        }
    };

    access_key.nonce = transaction.nonce;

    signer.set_amount(signer.amount().checked_sub(total_cost).ok_or_else(|| {
        InvalidTxError::NotEnoughBalance {
            signer_id: signer_id.clone(),
            balance: signer.amount(),
            cost: total_cost,
        }
    })?);

    if let AccessKeyPermission::FunctionCall(ref mut function_call_permission) =
        access_key.permission
    {
        if let Some(ref mut allowance) = function_call_permission.allowance {
            *allowance = allowance.checked_sub(total_cost).ok_or_else(|| {
                InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::NotEnoughAllowance {
                    account_id: signer_id.clone(),
                    public_key: transaction.public_key.clone(),
                    allowance: *allowance,
                    cost: total_cost,
                })
            })?;
        }
    }

    match get_insufficient_storage_stake(&signer, &config) {
        Ok(None) => {}
        Ok(Some(amount)) => {
            return Err(InvalidTxError::LackBalanceForState {
                signer_id: signer_id.clone(),
                amount,
            }
            .into())
        }
        Err(err) => {
            return Err(RuntimeError::StorageError(StorageError::StorageInconsistentState(err)))
        }
    };

    if let AccessKeyPermission::FunctionCall(ref function_call_permission) = access_key.permission {
        if transaction.actions.len() != 1 {
            return Err(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::RequiresFullAccess,
            )
            .into());
        }
        if let Some(Action::FunctionCall(ref function_call)) = transaction.actions.get(0) {
            if function_call.deposit > 0 {
                return Err(InvalidTxError::InvalidAccessKeyError(
                    InvalidAccessKeyError::DepositWithFunctionCall,
                )
                .into());
            }
            if transaction.receiver_id != function_call_permission.receiver_id {
                return Err(InvalidTxError::InvalidAccessKeyError(
                    InvalidAccessKeyError::ReceiverMismatch {
                        tx_receiver: transaction.receiver_id.clone(),
                        ak_receiver: function_call_permission.receiver_id.clone(),
                    },
                )
                .into());
            }
            if !function_call_permission.method_names.is_empty()
                && function_call_permission
                    .method_names
                    .iter()
                    .all(|method_name| &function_call.method_name != method_name)
            {
                return Err(InvalidTxError::InvalidAccessKeyError(
                    InvalidAccessKeyError::MethodNameMismatch {
                        method_name: function_call.method_name.clone(),
                    },
                )
                .into());
            }
        } else {
            return Err(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::RequiresFullAccess,
            )
            .into());
        }
    };

    set_access_key(state_update, signer_id.clone(), transaction.public_key.clone(), &access_key);
    set_account(state_update, signer_id.clone(), &signer);

    Ok(VerificationResult { gas_burnt, gas_remaining, receipt_gas_price, burnt_amount })
}

/// Validates a given receipt. Checks validity of the predecessor and receiver account IDs and
/// the validity of the Action or Data receipt.
pub fn validate_receipt(
    limit_config: &VMLimitConfig,
    receipt: &Receipt,
) -> Result<(), ReceiptValidationError> {
    if !is_valid_account_id(&receipt.predecessor_id) {
        return Err(ReceiptValidationError::InvalidPredecessorId {
            account_id: receipt.predecessor_id.clone(),
        });
    }
    if !is_valid_account_id(&receipt.receiver_id) {
        return Err(ReceiptValidationError::InvalidReceiverId {
            account_id: receipt.receiver_id.clone(),
        });
    }
    match &receipt.receipt {
        ReceiptEnum::Action(action_receipt) => {
            validate_action_receipt(limit_config, action_receipt)
        }
        ReceiptEnum::Data(data_receipt) => validate_data_receipt(limit_config, data_receipt),
    }
}

/// Validates given ActionReceipt. Checks validity of the signer account ID, validity of all
/// data receiver account IDs, the number of input data dependencies and all actions.
pub fn validate_action_receipt(
    limit_config: &VMLimitConfig,
    receipt: &ActionReceipt,
) -> Result<(), ReceiptValidationError> {
    if !is_valid_account_id(&receipt.signer_id) {
        return Err(ReceiptValidationError::InvalidSignerId {
            account_id: receipt.signer_id.clone(),
        });
    }
    for data_receiver in &receipt.output_data_receivers {
        if !is_valid_account_id(&data_receiver.receiver_id) {
            return Err(ReceiptValidationError::InvalidDataReceiverId {
                account_id: data_receiver.receiver_id.clone(),
            });
        }
    }
    if receipt.input_data_ids.len() as u64 > limit_config.max_number_input_data_dependencies {
        return Err(ReceiptValidationError::NumberInputDataDependenciesExceeded {
            number_of_input_data_dependencies: receipt.input_data_ids.len() as u64,
            limit: limit_config.max_number_input_data_dependencies,
        });
    }
    validate_actions(limit_config, &receipt.actions)
        .map_err(|e| ReceiptValidationError::ActionsValidation(e))
}

/// Validates given data receipt. Checks validity of the length of the returned data.
pub fn validate_data_receipt(
    limit_config: &VMLimitConfig,
    receipt: &DataReceipt,
) -> Result<(), ReceiptValidationError> {
    let data_len = receipt.data.as_ref().map(|data| data.len()).unwrap_or(0);
    if data_len as u64 > limit_config.max_length_returned_data {
        return Err(ReceiptValidationError::ReturnedValueLengthExceeded {
            length: data_len as u64,
            limit: limit_config.max_length_returned_data,
        });
    }
    Ok(())
}

/// Validates given actions. Checks limits and validates `account_id` if applicable.
/// Checks that the total number of actions doesn't exceed the limit.
/// Validates each individual action.
/// Checks that the total prepaid gas doesn't exceed the limit.
pub fn validate_actions(
    limit_config: &VMLimitConfig,
    actions: &[Action],
) -> Result<(), ActionsValidationError> {
    if actions.len() as u64 > limit_config.max_actions_per_receipt {
        return Err(ActionsValidationError::TotalNumberOfActionsExceeded {
            total_number_of_actions: actions.len() as u64,
            limit: limit_config.max_actions_per_receipt,
        });
    }

    let mut iter = actions.iter().peekable();
    while let Some(action) = iter.next() {
        if let Action::DeleteAccount(_) = action {
            if iter.peek().is_some() {
                return Err(ActionsValidationError::DeleteActionMustBeFinal);
            }
        }
        validate_action(limit_config, action)?;
    }

    let total_prepaid_gas =
        total_prepaid_gas(actions).map_err(|_| ActionsValidationError::IntegerOverflow)?;
    if total_prepaid_gas > limit_config.max_total_prepaid_gas {
        return Err(ActionsValidationError::TotalPrepaidGasExceeded {
            total_prepaid_gas,
            limit: limit_config.max_total_prepaid_gas,
        });
    }

    Ok(())
}

/// Validates a single given action. Checks limits and validates `account_id` if applicable.
pub fn validate_action(
    limit_config: &VMLimitConfig,
    action: &Action,
) -> Result<(), ActionsValidationError> {
    match action {
        Action::CreateAccount(_) => Ok(()),
        Action::DeployContract(a) => validate_deploy_contract_action(limit_config, a),
        Action::FunctionCall(a) => validate_function_call_action(limit_config, a),
        Action::Transfer(_) => Ok(()),
        Action::Stake(a) => validate_stake_action(a),
        Action::AddKey(a) => validate_add_key_action(limit_config, a),
        Action::DeleteKey(_) => Ok(()),
        Action::DeleteAccount(a) => validate_delete_account_action(a),
    }
}

/// Validates `DeployContractAction`. Checks that the given contract size doesn't exceed the limit.
pub fn validate_deploy_contract_action(
    limit_config: &VMLimitConfig,
    action: &DeployContractAction,
) -> Result<(), ActionsValidationError> {
    if action.code.len() as u64 > limit_config.max_contract_size {
        return Err(ActionsValidationError::ContractSizeExceeded {
            size: action.code.len() as u64,
            limit: limit_config.max_contract_size,
        });
    }

    Ok(())
}

/// Validates `FunctionCallAction`. Checks that the method name length doesn't exceed the limit and
/// the length of the arguments doesn't exceed the limit.
fn validate_function_call_action(
    limit_config: &VMLimitConfig,
    action: &FunctionCallAction,
) -> Result<(), ActionsValidationError> {
    if action.gas == 0 {
        return Err(ActionsValidationError::FunctionCallZeroAttachedGas);
    }

    if action.method_name.len() as u64 > limit_config.max_length_method_name {
        return Err(ActionsValidationError::FunctionCallMethodNameLengthExceeded {
            length: action.method_name.len() as u64,
            limit: limit_config.max_length_method_name,
        });
    }

    if action.args.len() as u64 > limit_config.max_arguments_length {
        return Err(ActionsValidationError::FunctionCallArgumentsLengthExceeded {
            length: action.args.len() as u64,
            limit: limit_config.max_arguments_length,
        });
    }

    Ok(())
}

/// Validates `StakeAction`. Checks that the `public_key` is a valid staking key.
fn validate_stake_action(action: &StakeAction) -> Result<(), ActionsValidationError> {
    if !is_valid_staking_key(&action.public_key) {
        return Err(ActionsValidationError::UnsuitableStakingKey {
            public_key: action.public_key.clone(),
        });
    }

    Ok(())
}

/// Validates `AddKeyAction`. If the access key permission is `FunctionCall` checks that the
/// `receiver_id` is a valid account ID, checks the total number of bytes of the method names
/// doesn't exceed the limit and every method name length doesn't exceed the limit.
fn validate_add_key_action(
    limit_config: &VMLimitConfig,
    action: &AddKeyAction,
) -> Result<(), ActionsValidationError> {
    if let AccessKeyPermission::FunctionCall(fc) = &action.access_key.permission {
        if !is_valid_account_id(&fc.receiver_id) {
            return Err(ActionsValidationError::InvalidAccountId {
                account_id: fc.receiver_id.clone(),
            });
        }
        // Checking method name length limits
        let mut total_number_of_bytes = 0;
        for method_name in &fc.method_names {
            let length = method_name.len() as u64;
            if length > limit_config.max_length_method_name {
                return Err(ActionsValidationError::AddKeyMethodNameLengthExceeded {
                    length,
                    limit: limit_config.max_length_method_name,
                });
            }
            // Adding terminating character to the total number of bytes
            total_number_of_bytes += length + 1;
        }
        if total_number_of_bytes > limit_config.max_number_bytes_method_names {
            return Err(ActionsValidationError::AddKeyMethodNamesNumberOfBytesExceeded {
                total_number_of_bytes,
                limit: limit_config.max_number_bytes_method_names,
            });
        }
    }

    Ok(())
}

/// Validates `DeleteAccountAction`. Checks that the `beneficiary_id` is a valid account ID.
fn validate_delete_account_action(
    action: &DeleteAccountAction,
) -> Result<(), ActionsValidationError> {
    if !is_valid_account_id(&action.beneficiary_id) {
        return Err(ActionsValidationError::InvalidAccountId {
            account_id: action.beneficiary_id.clone(),
        });
    }

    Ok(())
}
