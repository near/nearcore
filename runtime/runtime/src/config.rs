//! Settings of the parameters of the runtime.

use near_crypto::{KeyType, PublicKey};
use near_primitives::account::AccessKeyPermission;
use near_primitives::action::DeployGlobalContractAction;
use near_primitives::action::delegate::VersionedDelegateActionRef;
use near_primitives::errors::IntegerOverflowError;
// Just re-exporting RuntimeConfig for backwards compatibility.
use near_parameters::{
    ActionCosts, ExtCosts, ExtCostsConfig, ParameterCost, RuntimeConfig, RuntimeFeesConfig,
    SignatureKind, gas_key_add_key_exec_fee, gas_key_add_key_send_fee, gas_key_transfer_exec_fee,
    gas_key_transfer_send_fee, transfer_exec_fee, transfer_send_fee,
};
pub use near_primitives::num_rational::Rational32;
use near_primitives::transaction::{Action, DeployContractAction, Transaction};
use near_primitives::types::{AccountId, Balance, Compute, Gas};

/// Describes the cost of converting this transaction into a receipt.
#[derive(Debug)]
pub struct TransactionCost {
    /// Total amount of gas burnt for converting this transaction into a receipt.
    pub gas_burnt: Gas,
    /// Total amount of the compute budget of a chunk used by converting this
    /// transaction into a receipt.
    pub compute_burnt: Compute,
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

/// Compute cost for `count` storage_remove operations with `total_key_bytes` total
/// key bytes and `total_value_bytes` total value bytes across all removals.
pub(crate) fn storage_removes_compute(
    ext: &ExtCostsConfig,
    count: usize,
    total_key_bytes: usize,
    total_value_bytes: usize,
) -> Compute {
    let count = count as u64;
    let total_key_bytes = total_key_bytes as u64;
    let total_value_bytes = total_value_bytes as u64;
    ext.compute_cost(ExtCosts::storage_remove_base) * count
        + ext.compute_cost(ExtCosts::storage_remove_key_byte) * total_key_bytes
        + ext.compute_cost(ExtCosts::storage_remove_ret_value_byte) * total_value_bytes
}

/// Total sum of gas that needs to be burnt to send these actions.
pub fn total_send_fees(
    config: &RuntimeConfig,
    sender_is_receiver: bool,
    actions: &[Action],
    receiver_id: &AccountId,
) -> Result<ParameterCost, IntegerOverflowError> {
    let mut result = ParameterCost::ZERO;
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
                gas_key_transfer_send_fee(fees, sender_is_receiver, action.public_key.trie_id_len())
                    .total()
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
            DelegateV2(signed_delegate_action) => {
                let delegate_cost = fees.fee(ActionCosts::delegate).send_fee(sender_is_receiver);
                let delegate_action =
                    VersionedDelegateActionRef::from(&signed_delegate_action.delegate_action);

                delegate_cost
                    .checked_add(total_send_fees(
                        config,
                        sender_is_receiver,
                        &delegate_action.get_actions(),
                        delegate_action.receiver_id(),
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
                gas_key_transfer_send_fee(fees, sender_is_receiver, action.public_key.trie_id_len())
                    .total()
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
) -> ParameterCost {
    let key_fee = match permission {
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
    };
    let gas_key_info_fee = match permission {
        AccessKeyPermission::GasKeyFunctionCall(..) | AccessKeyPermission::GasKeyFullAccess(_) => {
            gas_key_add_key_send_fee(fees, sender_is_receiver)
        }
        _ => ParameterCost::ZERO,
    };
    key_fee.checked_add(gas_key_info_fee).unwrap()
}

/// Total sum of gas that needs to be burnt to send the inner actions of DelegateAction
///
/// This is only relevant for DelegateAction, where the send fees of the inner actions
/// need to be prepaid. All other actions burn send fees directly, so calling this function
/// with other actions will return 0.
pub fn total_prepaid_send_fees(
    config: &RuntimeConfig,
    actions: &[Action],
) -> Result<ParameterCost, IntegerOverflowError> {
    let mut result = ParameterCost::ZERO;
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
            DelegateV2(signed_delegate_action) => {
                let delegate_action =
                    VersionedDelegateActionRef::from(&signed_delegate_action.delegate_action);
                let sender_is_receiver =
                    delegate_action.sender_id() == delegate_action.receiver_id();

                total_send_fees(
                    config,
                    sender_is_receiver,
                    &delegate_action.get_actions(),
                    delegate_action.receiver_id(),
                )?
            }
            _ => ParameterCost::ZERO,
        };
        result = result.checked_add_result(delta)?;
    }
    Ok(result)
}

pub fn exec_fee(config: &RuntimeConfig, action: &Action, receiver_id: &AccountId) -> ParameterCost {
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
        DelegateV2(_) => fees.fee(ActionCosts::delegate).exec_fee(),
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
            gas_key_transfer_exec_fee(fees, receiver_id.len(), action.public_key.trie_id_len())
                .total()
        }
        WithdrawFromGasKey(action) => {
            gas_key_transfer_exec_fee(fees, receiver_id.len(), action.public_key.trie_id_len())
                .total()
        }
    }
}

fn permission_exec_fees(
    permission: &AccessKeyPermission,
    config: &RuntimeConfig,
    account_id: &AccountId,
    public_key: &PublicKey,
) -> ParameterCost {
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
    // Additional costs for adding an access key with GasKeyFunctionCall or GasKeyFullAccess permissions.
    let gas_key_info = match permission {
        AccessKeyPermission::GasKeyFullAccess(info)
        | AccessKeyPermission::GasKeyFunctionCall(info, _) => info,
        _ => return key_fee,
    };
    let nonce_fee = gas_key_add_key_exec_fee(
        fees,
        account_id.len(),
        public_key.trie_id_len(),
        gas_key_info.num_nonces,
    );
    key_fee.checked_add(nonce_fee.total()).unwrap()
}

/// Returns the total cost of converting a `tx` into a receipt, including the
/// costs of the spawned receipts.
pub fn tx_cost(
    config: &RuntimeConfig,
    tx: &Transaction,
    current_gas_price: Balance,
) -> Result<TransactionCost, IntegerOverflowError> {
    calculate_tx_cost(
        tx.receiver_id(),
        tx.signer_id(),
        tx.public_key(),
        tx.actions(),
        config,
        current_gas_price,
    )
}

/// Like [`tx_cost`], for callers that have the transaction's fields but not a
/// `Transaction` (e.g. the indexer prices transaction views).
pub fn calculate_tx_cost(
    receiver_id: &AccountId,
    signer_id: &AccountId,
    signer_public_key: &PublicKey,
    actions: &[Action],
    config: &RuntimeConfig,
    current_gas_price: Balance,
) -> Result<TransactionCost, IntegerOverflowError> {
    let sender_is_receiver = receiver_id == signer_id;
    let fees = &config.fees;
    let mut burnt: ParameterCost =
        fees.fee(ActionCosts::new_action_receipt).send_fee(sender_is_receiver);
    burnt = burnt.checked_add_result(total_send_fees(
        config,
        sender_is_receiver,
        actions,
        receiver_id,
    )?)?;
    // Burn the signature-verification cost as part of converting the
    // transaction. This raises the gas the signer must buy (burnt_amount /
    // total_cost below) but never `gas_remaining` (the gas attached to / left
    // for the resulting receipts), so on-chain function-call gas budgets are
    // unaffected.
    burnt =
        burnt.checked_add_result(signature_verification_cost(fees, signer_public_key, actions)?)?;

    // Calculate `gas_remaining`, which are all gas costs minus what is already
    // burnt in the sending step. Compute is not relevant here, as this gas will
    // be burnt later and has no effect on the current chunk capacity.
    // Gas attached to function calls
    let prepaid_gas = total_prepaid_gas(actions)?;
    // Send/Exec costs for actions inside the receipt
    let prepaid_send_fee = total_prepaid_send_fees(config, actions)?;
    let prepaid_exec_fee = total_prepaid_exec_fees(config, actions, receiver_id)?;
    // Exec cost for the receipt that wraps the actions
    let receipt_cost = fees.fee(ActionCosts::new_action_receipt).exec_fee();
    let gas_remaining = prepaid_gas
        .checked_add_result(prepaid_send_fee.gas)?
        .checked_add_result(receipt_cost.gas)?
        .checked_add_result(prepaid_exec_fee.gas)?;

    // Gas burned on converting the transaction to a receipt is burned at the current price.
    let burnt_amount = safe_gas_to_balance(current_gas_price, burnt.gas)?;

    // Gas attached to the receipt is purchased at a price which should be at least as large as
    // min_gas_purchase_price. Later it might be burned at a lower price, in which case the price
    // difference will be refunded.
    let receipt_gas_price = std::cmp::max(current_gas_price, config.min_gas_purchase_price);
    let remaining_gas_amount = safe_gas_to_balance(receipt_gas_price, gas_remaining)?;
    let gas_cost = safe_add_balance(burnt_amount, remaining_gas_amount)?;
    let deposit_cost = total_deposit(actions)?;
    let total_cost = safe_add_balance(gas_cost, deposit_cost)?;
    Ok(TransactionCost {
        gas_burnt: burnt.gas,
        compute_burnt: burnt.compute,
        gas_remaining,
        receipt_gas_price,
        burnt_amount,
        gas_cost,
        deposit_cost,
        total_cost,
    })
}

/// The signature scheme of a signer key, used to key the per-scheme
/// verification-cost map. Kept as a separate enum (rather than reusing
/// `KeyType` directly as the map key) so `near-parameters` need not depend on
/// `near-crypto`.
fn signature_kind(key_type: KeyType) -> SignatureKind {
    match key_type {
        KeyType::ED25519 => SignatureKind::Ed25519,
        KeyType::SECP256K1 => SignatureKind::Secp256k1,
        KeyType::MLDSA65 => SignatureKind::MlDsa65,
    }
}

/// Inner delegate action of a delegate-style action (`Delegate` or
/// `DelegateV2`), used by the recursive cost/fee functions so both variants are
/// handled identically.
fn delegate_inner_action(action: &Action) -> Option<VersionedDelegateActionRef<'_>> {
    match action {
        Action::Delegate(a) => Some((&a.delegate_action).into()),
        Action::DelegateV2(a) => Some((&a.delegate_action).into()),
        _ => None,
    }
}

/// Extra cost burnt at conversion for the signature verifications this
/// transaction triggers: the signer's own signature plus each `Delegate`
/// action's inner signer, each looked up by scheme in
/// `signature_verification_costs`. The charge is the extra verification cost
/// relative to the classical schemes; only ML-DSA-65 is non-zero, while
/// ed25519/secp256k1 stay 0 for backwards compatibility. The compute cost
/// debits the chunk's wall-clock budget and may be set independently of the
/// gas cost.
fn signature_verification_cost(
    fees: &RuntimeFeesConfig,
    signer_public_key: &PublicKey,
    actions: &[Action],
) -> Result<ParameterCost, IntegerOverflowError> {
    let costs = &fees.signature_verification_costs;
    let mut total = costs[signature_kind(signer_public_key.key_type())];
    for action in actions {
        if let Some(delegate_action) = delegate_inner_action(action) {
            let kind = signature_kind(delegate_action.public_key().key_type());
            total = total.checked_add_result(costs[kind])?;
        }
    }
    Ok(total)
}

/// Total sum of gas that would need to be burnt before we start executing the given actions.
pub fn total_prepaid_exec_fees(
    config: &RuntimeConfig,
    actions: &[Action],
    receiver_id: &AccountId,
) -> Result<ParameterCost, IntegerOverflowError> {
    let mut result = ParameterCost::ZERO;
    let fees = &config.fees;
    for action in actions {
        let mut delta;
        // For a delegate action it's needed to add gas required for the inner actions.
        if let Some(delegate_action) = delegate_inner_action(action) {
            let actions = delegate_action.get_actions();
            delta = total_prepaid_exec_fees(config, &actions, delegate_action.receiver_id())?;
            delta = delta.checked_add_result(exec_fee(
                config,
                action,
                delegate_action.receiver_id(),
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
        if let Some(delegate_action) = delegate_inner_action(action) {
            // Note, here Relayer pays the deposit but if actions fail, the deposit is
            // refunded to Sender of DelegateAction
            let actions = delegate_action.get_actions();
            action_balance = total_deposit(&actions)?;
        } else {
            action_balance = action.get_deposit_balance();
        }

        total_balance = safe_add_balance(total_balance, action_balance)?;
    }
    Ok(total_balance)
}

/// Get the total sum of prepaid gas for given actions.
///
/// "Prepaid" in this context means the gas that was attached to function calls
/// for covering dynamic execution costs. This gas is burnt dynamically based on
/// usage. What is not burnt may be forwarded to outgoing receipts or it may
/// also be converted back to NEAR for a refund.
///
/// Don't confuse this with `prepaid_exec_fees`, those are static fees that are
/// burnt before execution happens.
///
/// Naming isn't great but it is consistent with the user-visible error
/// `TotalPrepaidGasExceeded`.
pub fn total_prepaid_gas(actions: &[Action]) -> Result<Gas, IntegerOverflowError> {
    let mut total_gas = Gas::ZERO;
    for action in actions {
        let action_gas;
        if let Some(delegate_action) = delegate_inner_action(action) {
            let actions = delegate_action.get_actions();
            action_gas = total_prepaid_gas(&actions)?;
        } else {
            action_gas = action.get_prepaid_gas();
        }

        total_gas = total_gas.checked_add_result(action_gas)?;
    }
    Ok(total_gas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_crypto::SecretKey;
    use near_primitives::action::TransferAction;
    use near_primitives::action::delegate::{
        DelegateAction, DelegateActionV2, SignedDelegateAction, VersionedSignedDelegateAction,
    };
    use near_primitives::transaction::{TransactionNonce, TransactionV0};
    use std::sync::Arc;

    const VERIFY_GAS: u64 = 80_000_000_000;

    fn config_with_verify_cost(gas: u64, compute: u64) -> RuntimeConfig {
        let mut config = RuntimeConfig::test();
        Arc::make_mut(&mut config.fees).signature_verification_costs[SignatureKind::MlDsa65] =
            ParameterCost::new(Gas::from_gas(gas), compute);
        config
    }

    fn config_with_verify_gas(gas: u64) -> RuntimeConfig {
        config_with_verify_cost(gas, gas)
    }

    fn transfer() -> Action {
        Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })
    }

    /// A `Delegate` action whose inner signer is of `inner_key_type`. The
    /// signature is a dummy - `tx_cost` reads only the inner public key's type.
    fn delegate_with_inner(inner_key_type: KeyType) -> Action {
        let public_key = SecretKey::from_seed(inner_key_type, "inner").public_key();
        let signature = SecretKey::from_seed(KeyType::ED25519, "dummy").sign(b"x");
        Action::Delegate(Box::new(SignedDelegateAction {
            delegate_action: DelegateAction {
                sender_id: "alice.near".parse().unwrap(),
                receiver_id: "bob.near".parse().unwrap(),
                actions: vec![transfer().try_into().unwrap()],
                nonce: 1,
                max_block_height: 100,
                public_key,
            },
            signature,
        }))
    }

    /// Cost of a tx signed with `signer_key_type` carrying `actions`.
    fn cost_of(
        config: &RuntimeConfig,
        signer_key_type: KeyType,
        actions: Vec<Action>,
    ) -> TransactionCost {
        let public_key = SecretKey::from_seed(signer_key_type, "signer").public_key();
        let tx = Transaction::V0(TransactionV0 {
            signer_id: "alice.near".parse().unwrap(),
            public_key,
            nonce: 1,
            receiver_id: "bob.near".parse().unwrap(),
            block_hash: Default::default(),
            actions,
        });
        tx_cost(config, &tx, Balance::from_yoctonear(1)).unwrap()
    }

    /// An ML-DSA-65 outer signature adds exactly `ml_dsa_65_verification_cost`
    /// of *burnt* gas at conversion (the signer pays it), while the gas
    /// available to the resulting receipts is unchanged.
    #[test]
    fn ml_dsa_65_outer_verify_charged_as_burnt_gas() {
        let config = config_with_verify_gas(VERIFY_GAS);
        let ed = cost_of(&config, KeyType::ED25519, vec![transfer()]);
        let pq = cost_of(&config, KeyType::MLDSA65, vec![transfer()]);

        assert_eq!(
            pq.gas_burnt.as_gas(),
            ed.gas_burnt.as_gas() + VERIFY_GAS,
            "verify gas burnt at conversion"
        );
        assert_eq!(
            pq.compute_burnt,
            ed.compute_burnt + VERIFY_GAS,
            "verify compute burnt at conversion"
        );
        // Function-call / attached gas budget is untouched - contracts unaffected.
        assert_eq!(pq.gas_remaining, ed.gas_remaining);
        // The signer pays more tokens for the transaction.
        assert!(pq.gas_cost > ed.gas_cost && pq.total_cost > ed.total_cost);
    }

    /// A `Delegate` action with an ML-DSA-65 inner signer adds the verify gas to
    /// the *outer* tx's burnt gas; an ML-DSA signer wrapping a PQ delegate pays
    /// for both verifications.
    #[test]
    fn ml_dsa_65_delegate_inner_verify_charged_as_burnt_gas() {
        let config = config_with_verify_gas(VERIFY_GAS);
        let ed_inner =
            cost_of(&config, KeyType::ED25519, vec![delegate_with_inner(KeyType::ED25519)]);
        let pq_inner =
            cost_of(&config, KeyType::ED25519, vec![delegate_with_inner(KeyType::MLDSA65)]);
        assert_eq!(
            pq_inner.gas_burnt.as_gas(),
            ed_inner.gas_burnt.as_gas() + VERIFY_GAS,
            "inner PQ verify charged once"
        );

        // Outer ML-DSA signer + PQ inner delegate => two verifications.
        let two = cost_of(&config, KeyType::MLDSA65, vec![delegate_with_inner(KeyType::MLDSA65)]);
        assert_eq!(
            two.gas_burnt.as_gas(),
            ed_inner.gas_burnt.as_gas() + 2 * VERIFY_GAS,
            "outer + inner verify charged"
        );
    }

    fn delegate_v2_with_inner(inner_key_type: KeyType) -> Action {
        let public_key = SecretKey::from_seed(inner_key_type, "inner").public_key();
        let signature = SecretKey::from_seed(KeyType::ED25519, "dummy").sign(b"x");
        Action::DelegateV2(Box::new(VersionedSignedDelegateAction {
            delegate_action: DelegateActionV2 {
                sender_id: "alice.near".parse().unwrap(),
                receiver_id: "bob.near".parse().unwrap(),
                actions: vec![transfer().try_into().unwrap()],
                nonce: TransactionNonce::from_nonce_and_index(1, 0),
                max_block_height: 100,
                public_key,
            }
            .into(),
            signature,
        }))
    }

    #[test]
    fn delegate_v2_inner_costs_counted() {
        assert_eq!(
            total_deposit(&[delegate_v2_with_inner(KeyType::ED25519)]).unwrap(),
            Balance::from_yoctonear(1),
            "inner transfer deposit must be counted"
        );

        let config = config_with_verify_gas(VERIFY_GAS);
        let ed = cost_of(&config, KeyType::ED25519, vec![delegate_v2_with_inner(KeyType::ED25519)]);
        let pq = cost_of(&config, KeyType::ED25519, vec![delegate_v2_with_inner(KeyType::MLDSA65)]);
        assert_eq!(
            pq.gas_burnt.as_gas(),
            ed.gas_burnt.as_gas() + VERIFY_GAS,
            "inner PQ verify charged for V2 delegate"
        );
    }

    /// The compute cost of the verification charge can be set independently of
    /// the gas cost (NEP-455): only `compute_burnt` reflects the difference,
    /// while gas and token amounts follow the gas cost.
    #[test]
    fn ml_dsa_65_verify_compute_cost_independent_from_gas() {
        let compute = 3 * VERIFY_GAS;
        let config = config_with_verify_cost(VERIFY_GAS, compute);
        let ed = cost_of(&config, KeyType::ED25519, vec![transfer()]);
        let pq = cost_of(&config, KeyType::MLDSA65, vec![transfer()]);

        assert_eq!(
            pq.gas_burnt.as_gas(),
            ed.gas_burnt.as_gas() + VERIFY_GAS,
            "gas follows the gas cost"
        );
        assert_eq!(
            pq.compute_burnt,
            ed.compute_burnt + compute,
            "compute follows the compute cost, not gas"
        );
        // The signer buys gas, not compute: token cost reflects only the gas part.
        let pq_with_eq_compute =
            cost_of(&config_with_verify_gas(VERIFY_GAS), KeyType::MLDSA65, vec![transfer()]);
        assert_eq!(pq.gas_cost, pq_with_eq_compute.gas_cost);
        assert_eq!(pq.total_cost, pq_with_eq_compute.total_cost);
    }

    /// With the base value (0, before `PostQuantumSignatures`) the charge is
    /// inert: an ML-DSA-65 signer costs exactly the same as ed25519.
    #[test]
    fn ml_dsa_65_verify_gas_zero_by_default() {
        let config = RuntimeConfig::test();
        assert_eq!(
            config.fees.signature_verification_costs[SignatureKind::MlDsa65],
            ParameterCost::ZERO
        );
        let ed = cost_of(&config, KeyType::ED25519, vec![transfer()]);
        let pq = cost_of(&config, KeyType::MLDSA65, vec![transfer()]);
        assert_eq!(pq.gas_burnt, ed.gas_burnt);
        assert_eq!(pq.gas_cost, ed.gas_cost);
    }
}
