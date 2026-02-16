//! Settings of the parameters of the runtime.

use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, AccessKeyPermission};
use near_primitives::action::DeployGlobalContractAction;
use near_primitives::errors::IntegerOverflowError;
// Just re-exporting RuntimeConfig for backwards compatibility.
use near_parameters::{
    ActionCosts, ExtCosts, ExtCostsConfig, RuntimeConfig, RuntimeFeesConfig, transfer_exec_fee,
    transfer_send_fee,
};
pub use near_primitives::num_rational::Rational32;
use near_primitives::transaction::{Action, DeployContractAction, Transaction};
use near_primitives::trie_key::{access_key_key_len, gas_key_nonce_key_len};
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
    /// The amount of tokens burnt by converting this transaction to a receipt.
    pub burnt_amount: Balance,
    /// The total gas cost in tokens (burnt_amount + remaining gas amount).
    pub gas_cost: Balance,
    /// The total deposit cost in tokens (sum of action deposits).
    pub deposit_cost: Balance,
    /// Total costs in tokens for this transaction (including all deposits).
    pub total_cost: Balance,
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

fn storage_read_gas(ext: &ExtCostsConfig, key_len: usize, value_len: usize) -> Gas {
    let key_len = key_len as u64;
    let value_len = value_len as u64;
    ext.gas_cost(ExtCosts::storage_read_base)
        .checked_add(ext.gas_cost(ExtCosts::storage_read_key_byte).checked_mul(key_len).unwrap())
        .unwrap()
        .checked_add(
            ext.gas_cost(ExtCosts::storage_read_value_byte).checked_mul(value_len).unwrap(),
        )
        .unwrap()
}

fn storage_write_gas(ext: &ExtCostsConfig, key_len: usize, value_len: usize) -> Gas {
    let key_len = key_len as u64;
    let value_len = value_len as u64;
    ext.gas_cost(ExtCosts::storage_write_base)
        .checked_add(ext.gas_cost(ExtCosts::storage_write_key_byte).checked_mul(key_len).unwrap())
        .unwrap()
        .checked_add(
            ext.gas_cost(ExtCosts::storage_write_value_byte).checked_mul(value_len).unwrap(),
        )
        .unwrap()
}

/// Based on `Transfer` send fees, without implicit account creation. Charges
/// extra for the public_key field making the receipt larger.
fn gas_key_transfer_send_fee(
    fees: &RuntimeFeesConfig,
    sender_is_receiver: bool,
    public_key_len: usize,
) -> Gas {
    let transfer_fee = fees.fee(ActionCosts::transfer).send_fee(sender_is_receiver);
    let pk_byte_fee = fees.fee(ActionCosts::new_data_receipt_byte).send_fee(sender_is_receiver);
    transfer_fee.checked_add(pk_byte_fee.checked_mul(public_key_len as u64).unwrap()).unwrap()
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
            TransferToGasKey(action) => {
                gas_key_transfer_send_fee(fees, sender_is_receiver, action.public_key.len())
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
            WithdrawFromGasKey(action) => {
                gas_key_transfer_send_fee(fees, sender_is_receiver, action.public_key.len())
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
        AccessKeyPermission::FunctionCall(perm)
        | AccessKeyPermission::GasKeyFunctionCall(_, perm) => {
            let num_bytes = perm
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
        AccessKeyPermission::FullAccess | AccessKeyPermission::GasKeyFullAccess(_) => {
            fees.fee(ActionCosts::add_full_access_key).send_fee(sender_is_receiver)
        }
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
        AddKey(add_key_action) => permission_exec_fees(
            &add_key_action.access_key.permission,
            config,
            receiver_id,
            &add_key_action.public_key,
        ),
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
        TransferToGasKey(action) => {
            let ext = &config.wasm_config.ext_costs;
            let ak_key_len = access_key_key_len(receiver_id, &action.public_key);
            // Use minimum as an estimate for the value length. At the time of sending,
            // we don't know the variable portion (FunctionCallPermission) of the
            // specified gas key to transfer to.
            // Note tx_cost is calculated at the time of sending.
            let estimated_value_len = AccessKey::min_gas_key_borsh_len();
            fees.fee(ActionCosts::transfer)
                .exec_fee()
                .checked_add(storage_read_gas(ext, ak_key_len, estimated_value_len))
                .unwrap()
                .checked_add(storage_write_gas(ext, ak_key_len, estimated_value_len))
                .unwrap()
        }
        WithdrawFromGasKey(action) => {
            let ext = &config.wasm_config.ext_costs;
            let ak_key_len = access_key_key_len(receiver_id, &action.public_key);
            // Use minimum as an estimate for the value length. At the time of sending,
            // we don't know the variable portion (FunctionCallPermission) of the
            // specified gas key to withdraw from.
            let estimated_value_len = AccessKey::min_gas_key_borsh_len();
            fees.fee(ActionCosts::transfer)
                .exec_fee()
                .checked_add(storage_read_gas(ext, ak_key_len, estimated_value_len))
                .unwrap()
                .checked_add(storage_write_gas(ext, ak_key_len, estimated_value_len))
                .unwrap()
        }
    }
}

fn permission_exec_fees(
    permission: &AccessKeyPermission,
    config: &RuntimeConfig,
    account_id: &AccountId,
    public_key: &PublicKey,
) -> Gas {
    let fees = &config.fees;
    let key_fee = match permission {
        AccessKeyPermission::FunctionCall(perm)
        | AccessKeyPermission::GasKeyFunctionCall(_, perm) => {
            let num_bytes = perm
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
        AccessKeyPermission::FullAccess | AccessKeyPermission::GasKeyFullAccess(_) => {
            fees.fee(ActionCosts::add_full_access_key).exec_fee()
        }
    };
    // Add per-nonce write cost for gas key variants.
    let gas_key_info = match permission {
        AccessKeyPermission::GasKeyFullAccess(info)
        | AccessKeyPermission::GasKeyFunctionCall(info, _) => info,
        _ => return key_fee,
    };
    let ext = &config.wasm_config.ext_costs;
    let nonce_key_len = gas_key_nonce_key_len(account_id, public_key);
    let nonce_write_gas = storage_write_gas(ext, nonce_key_len, AccessKey::NONCE_VALUE_LEN);
    key_fee
        .checked_add(nonce_write_gas.checked_mul(gas_key_info.num_nonces as u64).unwrap())
        .unwrap()
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
    let gas_cost = safe_add_balance(burnt_amount, remaining_gas_amount)?;
    let deposit_cost = total_deposit(actions)?;
    let total_cost = safe_add_balance(gas_cost, deposit_cost)?;
    Ok(TransactionCost {
        gas_burnt,
        gas_remaining,
        receipt_gas_price,
        burnt_amount,
        gas_cost,
        deposit_cost,
        total_cost,
    })
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
