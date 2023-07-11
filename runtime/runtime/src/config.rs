//! Settings of the parameters of the runtime.

use near_vm_runner::logic::ActionCosts;
use num_bigint::BigUint;
use num_traits::cast::ToPrimitive;
use num_traits::pow::Pow;

use near_primitives::account::AccessKeyPermission;
use near_primitives::errors::IntegerOverflowError;
// Just re-exporting RuntimeConfig for backwards compatibility.
pub use near_primitives::num_rational::Rational32;
pub use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::runtime::fees::{transfer_exec_fee, transfer_send_fee, RuntimeFeesConfig};
use near_primitives::transaction::{
    Action, AddKeyAction, DeployContractAction, FunctionCallAction, Transaction,
};
use near_primitives::types::{AccountId, Balance, Compute, Gas};
use near_primitives::version::{checked_feature, ProtocolVersion};

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

/// Multiplies `gas_price` by the power of `inflation_base` with exponent `inflation_exponent`.
pub fn safe_gas_price_inflated(
    gas_price: Balance,
    inflation_base: Rational32,
    inflation_exponent: u8,
) -> Result<Balance, IntegerOverflowError> {
    let numer = BigUint::from(*inflation_base.numer() as usize).pow(inflation_exponent as u32);
    let denom = BigUint::from(*inflation_base.denom() as usize).pow(inflation_exponent as u32);
    // Rounding up
    let inflated_gas_price: BigUint = (numer * gas_price + &denom - 1u8) / denom;
    inflated_gas_price.to_u128().ok_or_else(|| IntegerOverflowError {})
}

pub fn safe_gas_to_balance(gas_price: Balance, gas: Gas) -> Result<Balance, IntegerOverflowError> {
    gas_price.checked_mul(Balance::from(gas)).ok_or_else(|| IntegerOverflowError {})
}

pub fn safe_add_gas(a: Gas, b: Gas) -> Result<Gas, IntegerOverflowError> {
    a.checked_add(b).ok_or_else(|| IntegerOverflowError {})
}

pub fn safe_add_balance(a: Balance, b: Balance) -> Result<Balance, IntegerOverflowError> {
    a.checked_add(b).ok_or_else(|| IntegerOverflowError {})
}

pub fn safe_add_compute(a: Compute, b: Compute) -> Result<Compute, IntegerOverflowError> {
    a.checked_add(b).ok_or_else(|| IntegerOverflowError {})
}

#[macro_export]
macro_rules! safe_add_balance_apply {
    ($x: expr) => {$x};
    ($x: expr, $($rest: expr),+) => {
        safe_add_balance($x, safe_add_balance_apply!($($rest),+))?
    }
}

/// Total sum of gas that needs to be burnt to send these actions.
pub fn total_send_fees(
    config: &RuntimeFeesConfig,
    sender_is_receiver: bool,
    actions: &[Action],
    receiver_id: &AccountId,
    current_protocol_version: ProtocolVersion,
) -> Result<Gas, IntegerOverflowError> {
    let mut result = 0;

    for action in actions {
        use Action::*;
        let delta = match action {
            CreateAccount(_) => {
                config.fee(ActionCosts::create_account).send_fee(sender_is_receiver)
            }
            DeployContract(DeployContractAction { code }) => {
                let num_bytes = code.len() as u64;
                config.fee(ActionCosts::deploy_contract_base).send_fee(sender_is_receiver)
                    + config.fee(ActionCosts::deploy_contract_byte).send_fee(sender_is_receiver)
                        * num_bytes
            }
            FunctionCall(FunctionCallAction { method_name, args, .. }) => {
                let num_bytes = method_name.as_bytes().len() as u64 + args.len() as u64;
                config.fee(ActionCosts::function_call_base).send_fee(sender_is_receiver)
                    + config.fee(ActionCosts::function_call_byte).send_fee(sender_is_receiver)
                        * num_bytes
            }
            Transfer(_) => {
                // Account for implicit account creation
                let is_receiver_implicit =
                    checked_feature!("stable", ImplicitAccountCreation, current_protocol_version)
                        && receiver_id.is_implicit();
                transfer_send_fee(config, sender_is_receiver, is_receiver_implicit)
            }
            Stake(_) => config.fee(ActionCosts::stake).send_fee(sender_is_receiver),
            AddKey(AddKeyAction { access_key, .. }) => match &access_key.permission {
                AccessKeyPermission::FunctionCall(call_perm) => {
                    let num_bytes = call_perm
                        .method_names
                        .iter()
                        // Account for null-terminating characters.
                        .map(|name| name.as_bytes().len() as u64 + 1)
                        .sum::<u64>();
                    config.fee(ActionCosts::add_function_call_key_base).send_fee(sender_is_receiver)
                        + num_bytes
                            * config
                                .fee(ActionCosts::add_function_call_key_byte)
                                .send_fee(sender_is_receiver)
                }
                AccessKeyPermission::FullAccess => {
                    config.fee(ActionCosts::add_full_access_key).send_fee(sender_is_receiver)
                }
            },
            DeleteKey(_) => config.fee(ActionCosts::delete_key).send_fee(sender_is_receiver),
            DeleteAccount(_) => {
                config.fee(ActionCosts::delete_account).send_fee(sender_is_receiver)
            }
            Delegate(signed_delegate_action) => {
                let delegate_cost = config.fee(ActionCosts::delegate).send_fee(sender_is_receiver);
                let delegate_action = &signed_delegate_action.delegate_action;

                delegate_cost
                    + total_send_fees(
                        config,
                        sender_is_receiver,
                        &delegate_action.get_actions(),
                        &delegate_action.receiver_id,
                        current_protocol_version,
                    )?
            }
        };
        result = safe_add_gas(result, delta)?;
    }
    Ok(result)
}

/// Total sum of gas that needs to be burnt to send the inner actions of DelegateAction
///
/// This is only relevant for DelegateAction, where the send fees of the inner actions
/// need to be prepaid. All other actions burn send fees directly, so calling this function
/// with other actions will return 0.
pub fn total_prepaid_send_fees(
    config: &RuntimeFeesConfig,
    actions: &[Action],
    current_protocol_version: ProtocolVersion,
) -> Result<Gas, IntegerOverflowError> {
    let mut result = 0;

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
                    current_protocol_version,
                )?
            }
            _ => 0,
        };
        result = safe_add_gas(result, delta)?;
    }
    Ok(result)
}

pub fn exec_fee(
    config: &RuntimeFeesConfig,
    action: &Action,
    receiver_id: &AccountId,
    current_protocol_version: ProtocolVersion,
) -> Gas {
    use Action::*;

    match action {
        CreateAccount(_) => config.fee(ActionCosts::create_account).exec_fee(),
        DeployContract(DeployContractAction { code }) => {
            let num_bytes = code.len() as u64;
            config.fee(ActionCosts::deploy_contract_base).exec_fee()
                + config.fee(ActionCosts::deploy_contract_byte).exec_fee() * num_bytes
        }
        FunctionCall(FunctionCallAction { method_name, args, .. }) => {
            let num_bytes = method_name.as_bytes().len() as u64 + args.len() as u64;
            config.fee(ActionCosts::function_call_base).exec_fee()
                + config.fee(ActionCosts::function_call_byte).exec_fee() * num_bytes
        }
        Transfer(_) => {
            // Account for implicit account creation
            let is_receiver_implicit =
                checked_feature!("stable", ImplicitAccountCreation, current_protocol_version)
                    && receiver_id.is_implicit();
            transfer_exec_fee(config, is_receiver_implicit)
        }
        Stake(_) => config.fee(ActionCosts::stake).exec_fee(),
        AddKey(AddKeyAction { access_key, .. }) => match &access_key.permission {
            AccessKeyPermission::FunctionCall(call_perm) => {
                let num_bytes = call_perm
                    .method_names
                    .iter()
                    // Account for null-terminating characters.
                    .map(|name| name.as_bytes().len() as u64 + 1)
                    .sum::<u64>();
                config.fee(ActionCosts::add_function_call_key_base).exec_fee()
                    + num_bytes * config.fee(ActionCosts::add_function_call_key_byte).exec_fee()
            }
            AccessKeyPermission::FullAccess => {
                config.fee(ActionCosts::add_full_access_key).exec_fee()
            }
        },
        DeleteKey(_) => config.fee(ActionCosts::delete_key).exec_fee(),
        DeleteAccount(_) => config.fee(ActionCosts::delete_account).exec_fee(),
        Delegate(_) => config.fee(ActionCosts::delegate).exec_fee(),
    }
}

/// Returns transaction costs for a given transaction.
pub fn tx_cost(
    config: &RuntimeFeesConfig,
    transaction: &Transaction,
    gas_price: Balance,
    sender_is_receiver: bool,
    current_protocol_version: ProtocolVersion,
) -> Result<TransactionCost, IntegerOverflowError> {
    let mut gas_burnt: Gas =
        config.fee(ActionCosts::new_action_receipt).send_fee(sender_is_receiver);
    gas_burnt = safe_add_gas(
        gas_burnt,
        total_send_fees(
            config,
            sender_is_receiver,
            &transaction.actions,
            &transaction.receiver_id,
            current_protocol_version,
        )?,
    )?;
    let prepaid_gas = safe_add_gas(
        total_prepaid_gas(&transaction.actions)?,
        total_prepaid_send_fees(config, &transaction.actions, current_protocol_version)?,
    )?;
    // If signer is equals to receiver the receipt will be processed at the same block as this
    // transaction. Otherwise it will processed in the next block and the gas might be inflated.
    let initial_receipt_hop = if transaction.signer_id == transaction.receiver_id { 0 } else { 1 };
    let minimum_new_receipt_gas = config.min_receipt_with_function_call_gas();
    // In case the config is free, we don't care about the maximum depth.
    let receipt_gas_price = if gas_price == 0 {
        0
    } else {
        let maximum_depth =
            if minimum_new_receipt_gas > 0 { prepaid_gas / minimum_new_receipt_gas } else { 0 };
        let inflation_exponent = u8::try_from(initial_receipt_hop + maximum_depth)
            .map_err(|_| IntegerOverflowError {})?;
        safe_gas_price_inflated(
            gas_price,
            config.pessimistic_gas_price_inflation_ratio,
            inflation_exponent,
        )?
    };

    let mut gas_remaining =
        safe_add_gas(prepaid_gas, config.fee(ActionCosts::new_action_receipt).exec_fee())?;
    gas_remaining = safe_add_gas(
        gas_remaining,
        total_prepaid_exec_fees(
            config,
            &transaction.actions,
            &transaction.receiver_id,
            current_protocol_version,
        )?,
    )?;
    let burnt_amount = safe_gas_to_balance(gas_price, gas_burnt)?;
    let remaining_gas_amount = safe_gas_to_balance(receipt_gas_price, gas_remaining)?;
    let mut total_cost = safe_add_balance(burnt_amount, remaining_gas_amount)?;
    total_cost = safe_add_balance(total_cost, total_deposit(&transaction.actions)?)?;
    Ok(TransactionCost { gas_burnt, gas_remaining, receipt_gas_price, total_cost, burnt_amount })
}

/// Total sum of gas that would need to be burnt before we start executing the given actions.
pub fn total_prepaid_exec_fees(
    config: &RuntimeFeesConfig,
    actions: &[Action],
    receiver_id: &AccountId,
    current_protocol_version: ProtocolVersion,
) -> Result<Gas, IntegerOverflowError> {
    let mut result = 0;
    for action in actions {
        let mut delta;
        // In case of Action::Delegate it's needed to add Gas which is required for the inner actions.
        if let Action::Delegate(signed_delegate_action) = action {
            let actions = signed_delegate_action.delegate_action.get_actions();
            delta = total_prepaid_exec_fees(
                config,
                &actions,
                &signed_delegate_action.delegate_action.receiver_id,
                current_protocol_version,
            )?;
            delta = safe_add_gas(
                delta,
                exec_fee(
                    config,
                    action,
                    &signed_delegate_action.delegate_action.receiver_id,
                    current_protocol_version,
                ),
            )?;
            delta = safe_add_gas(delta, config.fee(ActionCosts::new_action_receipt).exec_fee())?;
        } else {
            delta = exec_fee(config, action, receiver_id, current_protocol_version);
        }

        result = safe_add_gas(result, delta)?;
    }
    Ok(result)
}
/// Get the total sum of deposits for given actions.
pub fn total_deposit(actions: &[Action]) -> Result<Balance, IntegerOverflowError> {
    let mut total_balance: Balance = 0;
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
    let mut total_gas: Gas = 0;
    for action in actions {
        let action_gas;
        if let Action::Delegate(signed_delegate_action) = action {
            let actions = signed_delegate_action.delegate_action.get_actions();
            action_gas = total_prepaid_gas(&actions)?;
        } else {
            action_gas = action.get_prepaid_gas();
        }

        total_gas = safe_add_gas(total_gas, action_gas)?;
    }
    Ok(total_gas)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_gas_price_inflated() {
        assert_eq!(safe_gas_price_inflated(10000, Rational32::new(101, 100), 1).unwrap(), 10100);
        assert_eq!(safe_gas_price_inflated(10000, Rational32::new(101, 100), 2).unwrap(), 10201);
        // Rounded up
        assert_eq!(safe_gas_price_inflated(10000, Rational32::new(101, 100), 3).unwrap(), 10304);
        assert_eq!(safe_gas_price_inflated(10000, Rational32::new(101, 100), 32).unwrap(), 13750);
    }
}
