//! Settings of the parameters of the runtime.

use near_primitives::account::AccessKeyPermission;
use near_primitives::action::DeployGlobalContractAction;
use near_primitives::errors::IntegerOverflowError;
// Just re-exporting RuntimeConfig for backwards compatibility.
use near_parameters::{
    ActionCosts, RuntimeConfig, RuntimeFeesConfig, transfer_exec_fee, transfer_send_fee,
};
pub use near_primitives::num_rational::Rational32;
use near_primitives::transaction::{Action, DeployContractAction, Transaction};
use near_primitives::types::{AccountId, Balance, Compute, Gas};

/// Describes the cost of converting this transaction into a receipt.
#[derive(Debug)]
pub struct TransactionCost {
    /// Total amount of gas burnt for converting this transaction into a receipt.
    pub gas_burnt: Gas,
    /// The remaining amount of gas used for converting this transaction into a receipt.
    /// It includes gas that is not yet spent, e.g. prepaid gas for function calls and
    /// future execution fees.
    pub gas_remaining: Gas,
    /// The gas price at which the gas was purchased in the receipt.
    pub receipt_gas_price: Balance,
    /// Total costs in tokens for this transaction (including all deposits).
    pub total_cost: Balance,
    /// The amount of tokens burnt by converting this transaction to a receipt.
    pub burnt_amount: Balance,
}

pub fn safe_gas_to_balance(gas_price: Balance, gas: Gas) -> Result<Balance, IntegerOverflowError> {
    gas_price.checked_mul(u128::from(gas.as_gas())).ok_or(IntegerOverflowError {})
}

pub fn safe_add_balance(a: Balance, b: Balance) -> Result<Balance, IntegerOverflowError> {
    a.checked_add(b).ok_or(IntegerOverflowError {})
}

pub fn safe_add_compute(a: Compute, b: Compute) -> Result<Compute, IntegerOverflowError> {
    a.checked_add(b).ok_or(IntegerOverflowError {})
}

/// Total sum of gas that needs to be burnt to send these actions.
pub fn total_send_fees(
    config: &RuntimeConfig,
    sender_is_receiver: bool,
    actions: &[Action],
    receiver_id: &AccountId,
) -> Result<Gas, IntegerOverflowError> {
    let mut result = Gas::ZERO;
    let fees = &config.fees;

    for action in actions {
        use Action::*;
        let delta = match action {
            CreateAccount(_) => fees.fee(ActionCosts::create_account).send_fee(sender_is_receiver),
            DeployContract(DeployContractAction { code }) => {
                let num_bytes = code.len() as u64;
                let base_fee =
                    fees.fee(ActionCosts::deploy_contract_base).send_fee(sender_is_receiver);
                let byte_fee =
                    fees.fee(ActionCosts::deploy_contract_byte).send_fee(sender_is_receiver);
                let all_bytes_fee = byte_fee.checked_mul(num_bytes).unwrap();

                base_fee.checked_add(all_bytes_fee).unwrap()
            }
            FunctionCall(function_call_action) => {
                let num_bytes = function_call_action.method_name.as_bytes().len() as u64
                    + function_call_action.args.len() as u64;
                let base_fee =
                    fees.fee(ActionCosts::function_call_base).send_fee(sender_is_receiver);
                let byte_fee =
                    fees.fee(ActionCosts::function_call_byte).send_fee(sender_is_receiver);
                let all_bytes_fee = byte_fee.checked_mul(num_bytes).unwrap();

                base_fee.checked_add(all_bytes_fee).unwrap()
            }
            Transfer(_) => {
                // Account for implicit account creation
                transfer_send_fee(
                    fees,
                    sender_is_receiver,
                    config.wasm_config.eth_implicit_accounts,
                    receiver_id.get_account_type(),
                )
            }
            Stake(_) => fees.fee(ActionCosts::stake).send_fee(sender_is_receiver),
            AddKey(add_key_action) => permission_send_fees(
                &add_key_action.access_key.permission,
                fees,
                sender_is_receiver,
            ),
            DeleteKey(_) => fees.fee(ActionCosts::delete_key).send_fee(sender_is_receiver),
            DeleteAccount(_) => fees.fee(ActionCosts::delete_account).send_fee(sender_is_receiver),
            Delegate(signed_delegate_action) => {
                let delegate_cost = fees.fee(ActionCosts::delegate).send_fee(sender_is_receiver);
                let delegate_action = &signed_delegate_action.delegate_action;

                delegate_cost
                    .checked_add(total_send_fees(
                        config,
                        sender_is_receiver,
                        &delegate_action.get_actions(),
                        &delegate_action.receiver_id,
                    )?)
                    .unwrap()
            }
            DeployGlobalContract(DeployGlobalContractAction { code, .. }) => {
                let num_bytes = code.len() as u64;

                let base_fee =
                    fees.fee(ActionCosts::deploy_global_contract_base).send_fee(sender_is_receiver);
                let byte_fee =
                    fees.fee(ActionCosts::deploy_global_contract_byte).send_fee(sender_is_receiver);
                let all_bytes_fee = byte_fee.checked_mul(num_bytes).unwrap();

                base_fee.checked_add(all_bytes_fee).unwrap()
            }
            UseGlobalContract(action) => {
                let num_bytes = action.contract_identifier.len() as u64;
                let base_fee =
                    fees.fee(ActionCosts::use_global_contract_base).send_fee(sender_is_receiver);
                let byte_fee =
                    fees.fee(ActionCosts::use_global_contract_byte).send_fee(sender_is_receiver);
                let all_bytes_fee = byte_fee.checked_mul(num_bytes).unwrap();

                base_fee.checked_add(all_bytes_fee).unwrap()
            }
            DeterministicStateInit(action) => {
                let num_entries = action.state_init.data().len() as u64;
                let num_bytes = action.state_init.len_bytes();
                let base_fee = fees
                    .fee(ActionCosts::deterministic_state_init_base)
                    .send_fee(sender_is_receiver);
                let entry_fee = fees
                    .fee(ActionCosts::deterministic_state_init_entry)
                    .send_fee(sender_is_receiver);
                let all_entries_fee = entry_fee.checked_mul(num_entries).unwrap();
                let byte_fee = fees
                    .fee(ActionCosts::deterministic_state_init_byte)
                    .send_fee(sender_is_receiver);
                let all_bytes_fee = byte_fee.checked_mul(num_bytes as u64).unwrap();

                base_fee.checked_add(all_bytes_fee).unwrap().checked_add(all_entries_fee).unwrap()
            }
        };
        result = result.checked_add_result(delta)?;
    }
    Ok(result)
}

fn permission_send_fees(
    permission: &AccessKeyPermission,
    fees: &RuntimeFeesConfig,
    sender_is_receiver: bool,
) -> Gas {
    match permission {
        AccessKeyPermission::FunctionCall(call_perm) => {
            let num_bytes = call_perm
                .method_names
                .iter()
                // Account for null-terminating characters.
                .map(|name| name.as_bytes().len() as u64 + 1)
                .sum::<u64>();
            let base_fee =
                fees.fee(ActionCosts::add_function_call_key_base).send_fee(sender_is_receiver);
            let byte_fee =
                fees.fee(ActionCosts::add_function_call_key_byte).send_fee(sender_is_receiver);
            let all_bytes_fee = byte_fee.checked_mul(num_bytes).unwrap();

            base_fee.checked_add(all_bytes_fee).unwrap()
        }
        AccessKeyPermission::FullAccess => {
            fees.fee(ActionCosts::add_full_access_key).send_fee(sender_is_receiver)
        }
        // TODO(gas-keys): properly handle GasKey fees
        AccessKeyPermission::GasKeyFullAccess(_) => Gas::ZERO,
        AccessKeyPermission::GasKeyFunctionCall(_, _) => Gas::ZERO,
    }
}

/// Total sum of gas that needs to be burnt to send the inner actions of DelegateAction
///
/// This is only relevant for DelegateAction, where the send fees of the inner actions
/// need to be prepaid. All other actions burn send fees directly, so calling this function
/// with other actions will return 0.
pub fn total_prepaid_send_fees(
    config: &RuntimeConfig,
    actions: &[Action],
) -> Result<Gas, IntegerOverflowError> {
    let mut result = Gas::ZERO;
    for action in actions {
        use Action::*;
        let delta = match action {
            Delegate(signed_delegate_action) => {
                let delegate_action = &signed_delegate_action.delegate_action;
                let sender_is_receiver = delegate_action.sender_id == delegate_action.receiver_id;

                total_send_fees(
                    config,
                    sender_is_receiver,
                    &delegate_action.get_actions(),
                    &delegate_action.receiver_id,
                )?
            }
            _ => Gas::ZERO,
        };
        result = result.checked_add_result(delta)?;
    }
    Ok(result)
}

pub fn exec_fee(config: &RuntimeConfig, action: &Action, receiver_id: &AccountId) -> Gas {
    use Action::*;
    let fees = &config.fees;
    match action {
        CreateAccount(_) => fees.fee(ActionCosts::create_account).exec_fee(),
        DeployContract(DeployContractAction { code }) => {
            let num_bytes = code.len() as u64;
            let base_fee = fees.fee(ActionCosts::deploy_contract_base).exec_fee();
            let byte_fee = fees.fee(ActionCosts::deploy_contract_byte).exec_fee();
            let all_bytes_fee = byte_fee.checked_mul(num_bytes).unwrap();

            base_fee.checked_add(all_bytes_fee).unwrap()
        }
        FunctionCall(function_call_action) => {
            let num_bytes = function_call_action.method_name.as_bytes().len() as u64
                + function_call_action.args.len() as u64;
            let base_fee = fees.fee(ActionCosts::function_call_base).exec_fee();
            let byte_fee = fees.fee(ActionCosts::function_call_byte).exec_fee();
            let all_bytes_fee = byte_fee.checked_mul(num_bytes).unwrap();

            base_fee.checked_add(all_bytes_fee).unwrap()
        }
        Transfer(_) => {
            // Account for implicit account creation
            transfer_exec_fee(
                fees,
                config.wasm_config.eth_implicit_accounts,
                receiver_id.get_account_type(),
            )
        }
        Stake(_) => fees.fee(ActionCosts::stake).exec_fee(),
        AddKey(add_key_action) => permission_exec_fees(&add_key_action.access_key.permission, fees),
        DeleteKey(_) => fees.fee(ActionCosts::delete_key).exec_fee(),
        DeleteAccount(_) => fees.fee(ActionCosts::delete_account).exec_fee(),
        Delegate(_) => fees.fee(ActionCosts::delegate).exec_fee(),
        DeployGlobalContract(DeployGlobalContractAction { code, .. }) => {
            let num_bytes = code.len() as u64;
            let base_fee = fees.fee(ActionCosts::deploy_global_contract_base).exec_fee();
            let byte_fee = fees.fee(ActionCosts::deploy_global_contract_byte).exec_fee();
            let all_bytes_fee = byte_fee.checked_mul(num_bytes).unwrap();

            base_fee.checked_add(all_bytes_fee).unwrap()
        }
        UseGlobalContract(action) => {
            let num_bytes = action.contract_identifier.len() as u64;
            let base_fee = fees.fee(ActionCosts::use_global_contract_base).exec_fee();
            let byte_fee = fees.fee(ActionCosts::use_global_contract_byte).exec_fee();
            let all_bytes_fee = byte_fee.checked_mul(num_bytes).unwrap();

            base_fee.checked_add(all_bytes_fee).unwrap()
        }
        DeterministicStateInit(action) => {
            let num_entries = action.state_init.data().len() as u64;
            let num_bytes = action.state_init.len_bytes();
            let base_fee = fees.fee(ActionCosts::deterministic_state_init_base).exec_fee();
            let entry_fee = fees.fee(ActionCosts::deterministic_state_init_entry).exec_fee();
            let all_entries_fee = entry_fee.checked_mul(num_entries).unwrap();
            let byte_fee = fees.fee(ActionCosts::deterministic_state_init_byte).exec_fee();
            let all_bytes_fee = byte_fee.checked_mul(num_bytes as u64).unwrap();

            base_fee.checked_add(all_bytes_fee).unwrap().checked_add(all_entries_fee).unwrap()
        }
    }
}

fn permission_exec_fees(permission: &AccessKeyPermission, fees: &RuntimeFeesConfig) -> Gas {
    match permission {
        AccessKeyPermission::FunctionCall(call_perm) => {
            let num_bytes = call_perm
                .method_names
                .iter()
                // Account for null-terminating characters.
                .map(|name| name.as_bytes().len() as u64 + 1)
                .sum::<u64>();

            let base_fee = fees.fee(ActionCosts::add_function_call_key_base).exec_fee();
            let byte_fee = fees.fee(ActionCosts::add_function_call_key_byte).exec_fee();
            let all_bytes_fee = byte_fee.checked_mul(num_bytes).unwrap();

            base_fee.checked_add(all_bytes_fee).unwrap()
        }
        AccessKeyPermission::FullAccess => fees.fee(ActionCosts::add_full_access_key).exec_fee(),
        // TODO(gas-keys): properly handle GasKey fees
        AccessKeyPermission::GasKeyFullAccess(_) => Gas::ZERO,
        AccessKeyPermission::GasKeyFunctionCall(_, _) => Gas::ZERO,
    }
}

/// Returns transaction costs for a given transaction.
pub fn tx_cost(
    config: &RuntimeConfig,
    tx: &Transaction,
    receipt_gas_price: Balance,
) -> Result<TransactionCost, IntegerOverflowError> {
    calculate_tx_cost(tx.receiver_id(), tx.signer_id(), tx.actions(), config, receipt_gas_price)
}

pub fn calculate_tx_cost(
    receiver_id: &AccountId,
    signer_id: &AccountId,
    actions: &[Action],
    config: &RuntimeConfig,
    receipt_gas_price: Balance,
) -> Result<TransactionCost, IntegerOverflowError> {
    let sender_is_receiver = receiver_id == signer_id;
    let fees = &config.fees;
    let mut gas_burnt: Gas = fees.fee(ActionCosts::new_action_receipt).send_fee(sender_is_receiver);
    gas_burnt = gas_burnt.checked_add_result(total_send_fees(
        config,
        sender_is_receiver,
        actions,
        receiver_id,
    )?)?;
    let prepaid_gas = total_prepaid_gas(&actions)?
        .checked_add_result(total_prepaid_send_fees(config, &actions)?)?;
    let mut gas_remaining =
        prepaid_gas.checked_add_result(fees.fee(ActionCosts::new_action_receipt).exec_fee())?;
    gas_remaining =
        gas_remaining.checked_add_result(total_prepaid_exec_fees(config, actions, receiver_id)?)?;
    let burnt_amount = safe_gas_to_balance(receipt_gas_price, gas_burnt)?;
    let remaining_gas_amount = safe_gas_to_balance(receipt_gas_price, gas_remaining)?;
    let mut total_cost = safe_add_balance(burnt_amount, remaining_gas_amount)?;
    total_cost = safe_add_balance(total_cost, total_deposit(actions)?)?;
    Ok(TransactionCost { gas_burnt, gas_remaining, receipt_gas_price, total_cost, burnt_amount })
}

/// Total sum of gas that would need to be burnt before we start executing the given actions.
pub fn total_prepaid_exec_fees(
    config: &RuntimeConfig,
    actions: &[Action],
    receiver_id: &AccountId,
) -> Result<Gas, IntegerOverflowError> {
    let mut result = Gas::ZERO;
    let fees = &config.fees;
    for action in actions {
        let mut delta;
        // In case of Action::Delegate it's needed to add Gas which is required for the inner actions.
        if let Action::Delegate(signed_delegate_action) = action {
            let actions = signed_delegate_action.delegate_action.get_actions();
            delta = total_prepaid_exec_fees(
                config,
                &actions,
                &signed_delegate_action.delegate_action.receiver_id,
            )?;
            delta = delta.checked_add_result(exec_fee(
                config,
                action,
                &signed_delegate_action.delegate_action.receiver_id,
            ))?;
            delta =
                delta.checked_add_result(fees.fee(ActionCosts::new_action_receipt).exec_fee())?;
        } else {
            delta = exec_fee(config, action, receiver_id);
        }

        result = result.checked_add_result(delta)?;
    }
    Ok(result)
}
/// Get the total sum of deposits for given actions.
pub fn total_deposit(actions: &[Action]) -> Result<Balance, IntegerOverflowError> {
    let mut total_balance = Balance::ZERO;
    for action in actions {
        let action_balance;
        if let Action::Delegate(signed_delegate_action) = action {
            // Note, here Relayer pays the deposit but if actions fail, the deposit is
            // refunded to Sender of DelegateAction
            let actions = signed_delegate_action.delegate_action.get_actions();
            action_balance = total_deposit(&actions)?;
        } else {
            action_balance = action.get_deposit_balance();
        }

        total_balance = safe_add_balance(total_balance, action_balance)?;
    }
    Ok(total_balance)
}

/// Get the total sum of prepaid gas for given actions.
pub fn total_prepaid_gas(actions: &[Action]) -> Result<Gas, IntegerOverflowError> {
    let mut total_gas = Gas::ZERO;
    for action in actions {
        let action_gas;
        if let Action::Delegate(signed_delegate_action) = action {
            let actions = signed_delegate_action.delegate_action.get_actions();
            action_gas = total_prepaid_gas(&actions)?;
        } else {
            action_gas = action.get_prepaid_gas();
        }

        total_gas = total_gas.checked_add_result(action_gas)?;
    }
    Ok(total_gas)
}
