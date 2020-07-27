use std::sync::Arc;

use borsh::BorshSerialize;
use log::debug;

use near_primitives::account::{AccessKeyPermission, Account};
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, Receipt};
use near_primitives::transaction::{
    Action, AddKeyAction, DeleteAccountAction, DeleteKeyAction, DeployContractAction,
    FunctionCallAction, StakeAction, TransferAction,
};
use near_primitives::types::{AccountId, Balance, EpochInfoProvider, ValidatorStake};
use near_primitives::utils::{
    is_valid_account_id, is_valid_sub_account_id, is_valid_top_level_account_id,
};
use near_runtime_fees::RuntimeFeesConfig;
use near_store::{
    get_access_key, get_code, remove_access_key, remove_account, set_access_key, set_code,
    StorageError, TrieUpdate,
};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::VMContext;

use crate::config::{safe_add_gas, RuntimeConfig};
use crate::ext::RuntimeExt;
use crate::{ActionResult, ApplyState};
use near_crypto::PublicKey;
use near_primitives::errors::{ActionError, ActionErrorKind, ExternalError, RuntimeError};
use near_runtime_configs::AccountCreationConfig;
use near_vm_errors::{CompilationError, FunctionCallError};
use near_vm_runner::VMError;

/// Checks if given account has enough balance for state stake, and returns:
///  - None if account has enough balance,
///  - Some(insufficient_balance) if account doesn't have enough and how much need to be added,
///  - Err(StorageError::StorageInconsistentState) if account has invalid storage usage or amount/locked.
///
/// Read details of state staking https://nomicon.io/Economics/README.html#state-stake
pub(crate) fn get_insufficient_storage_stake(
    account: &Account,
    runtime_config: &RuntimeConfig,
) -> Result<Option<Balance>, StorageError> {
    let required_amount = Balance::from(account.storage_usage)
        .checked_mul(runtime_config.storage_amount_per_byte)
        .ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "Account's storage_usage {} overflows multiplication",
                account.storage_usage
            ))
        })?;
    let available_amount = account.amount.checked_add(account.locked).ok_or_else(|| {
        StorageError::StorageInconsistentState(format!(
            "Account's amount {} and locked {} overflow addition",
            account.amount, account.locked
        ))
    })?;
    if available_amount >= required_amount {
        Ok(None)
    } else {
        Ok(Some(required_amount - available_amount))
    }
}

pub(crate) fn get_code_with_cache(
    state_update: &TrieUpdate,
    account_id: &AccountId,
    account: &Account,
) -> Result<Option<Arc<ContractCode>>, StorageError> {
    debug!(target:"runtime", "Calling the contract at account {}", account_id);
    let code_hash = account.code_hash;
    let code = || get_code(state_update, account_id);
    crate::cache::get_code(code_hash, code)
}

pub(crate) fn action_function_call(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    account: &mut Account,
    receipt: &Receipt,
    action_receipt: &ActionReceipt,
    promise_results: &[PromiseResult],
    result: &mut ActionResult,
    account_id: &AccountId,
    function_call: &FunctionCallAction,
    action_hash: &CryptoHash,
    config: &RuntimeConfig,
    is_last_action: bool,
    epoch_info_provider: &dyn EpochInfoProvider,
) -> Result<(), RuntimeError> {
    let code = match get_code_with_cache(state_update, account_id, &account) {
        Ok(Some(code)) => code,
        Ok(None) => {
            let error = FunctionCallError::CompilationError(CompilationError::CodeDoesNotExist {
                account_id: account_id.clone(),
            });
            result.result = Err(ActionErrorKind::FunctionCallError(error).into());
            return Ok(());
        }
        Err(e) => {
            return Err(e.into());
        }
    };

    if account.amount.checked_add(function_call.deposit).is_none() {
        return Err(StorageError::StorageInconsistentState(
            "Account balance integer overflow during function call deposit".to_string(),
        )
        .into());
    }

    let mut runtime_ext = RuntimeExt::new(
        state_update,
        account_id,
        &action_receipt.signer_id,
        &action_receipt.signer_public_key,
        action_receipt.gas_price,
        action_hash,
        &apply_state.epoch_id,
        &apply_state.last_block_hash,
        epoch_info_provider,
    );
    // Output data receipts are ignored if the function call is not the last action in the batch.
    let output_data_receivers: Vec<_> = if is_last_action {
        action_receipt.output_data_receivers.iter().map(|r| r.receiver_id.clone()).collect()
    } else {
        vec![]
    };
    let context = VMContext {
        current_account_id: account_id.clone(),
        signer_account_id: action_receipt.signer_id.clone(),
        signer_account_pk: action_receipt
            .signer_public_key
            .try_to_vec()
            .expect("Failed to serialize"),
        predecessor_account_id: receipt.predecessor_id.clone(),
        input: function_call.args.clone(),
        block_index: apply_state.block_index,
        block_timestamp: apply_state.block_timestamp,
        epoch_height: apply_state.epoch_height,
        account_balance: account.amount,
        account_locked_balance: account.locked,
        storage_usage: account.storage_usage,
        attached_deposit: function_call.deposit,
        prepaid_gas: function_call.gas,
        random_seed: action_hash.as_ref().to_vec(),
        is_view: false,
        output_data_receivers,
    };

    let (outcome, err) = near_vm_runner::run(
        code.hash.as_ref().to_vec(),
        &code.code,
        function_call.method_name.as_bytes(),
        &mut runtime_ext,
        context,
        &config.wasm_config,
        &config.transaction_costs,
        promise_results,
    );
    let execution_succeeded = match err {
        Some(VMError::FunctionCallError(err)) => {
            result.result = Err(ActionErrorKind::FunctionCallError(err).into());
            false
        }
        Some(VMError::ExternalError(serialized_error)) => {
            let err: ExternalError = borsh::BorshDeserialize::try_from_slice(&serialized_error)
                .expect("External error deserialization shouldn't fail");
            return match err {
                ExternalError::StorageError(err) => Err(err.into()),
                ExternalError::ValidatorError(err) => Err(RuntimeError::ValidatorError(err)),
            };
        }
        Some(VMError::InconsistentStateError(err)) => {
            return Err(StorageError::StorageInconsistentState(err.to_string()).into());
        }
        None => true,
    };
    if let Some(outcome) = outcome {
        result.gas_burnt = safe_add_gas(result.gas_burnt, outcome.burnt_gas)?;
        result.gas_burnt_for_function_call =
            safe_add_gas(result.gas_burnt_for_function_call, outcome.burnt_gas)?;
        // Runtime in `generate_refund_receipts` takes care of using proper value for refunds.
        // It uses `gas_used` for success and `gas_burnt` for failures. So it's not an issue to
        // return a real `gas_used` instead of the `gas_burnt` into `ActionResult` even for
        // `FunctionCall`s error.
        result.gas_used = safe_add_gas(result.gas_used, outcome.used_gas)?;
        result.logs.extend(outcome.logs.into_iter());
        if execution_succeeded {
            account.amount = outcome.balance;
            account.storage_usage = outcome.storage_usage;
            result.result = Ok(outcome.return_data);
            result.new_receipts.extend(runtime_ext.into_receipts(account_id));
        }
    } else {
        assert!(!execution_succeeded, "Outcome should always be available if execution succeeded")
    }
    Ok(())
}

pub(crate) fn action_stake(
    account: &mut Account,
    result: &mut ActionResult,
    account_id: &AccountId,
    stake: &StakeAction,
    last_block_hash: &CryptoHash,
    epoch_info_provider: &dyn EpochInfoProvider,
) -> Result<(), RuntimeError> {
    let increment = stake.stake.saturating_sub(account.locked);

    if account.amount >= increment {
        if account.locked == 0 && stake.stake == 0 {
            // if the account hasn't staked, it cannot unstake
            result.result =
                Err(ActionErrorKind::TriesToUnstake { account_id: account_id.clone() }.into());
            return Ok(());
        }

        if stake.stake > 0 {
            let minimum_stake = epoch_info_provider.minimum_stake(last_block_hash)?;
            if stake.stake < minimum_stake {
                result.result = Err(ActionErrorKind::InsufficientStake {
                    account_id: account_id.clone(),
                    stake: stake.stake,
                    minimum_stake,
                }
                .into());
                return Ok(());
            }
        }

        result.validator_proposals.push(ValidatorStake {
            account_id: account_id.clone(),
            public_key: stake.public_key.clone(),
            stake: stake.stake,
        });
        if stake.stake > account.locked {
            // We've checked above `account.amount >= increment`
            account.amount -= increment;
            account.locked = stake.stake;
        }
    } else {
        result.result = Err(ActionErrorKind::TriesToStake {
            account_id: account_id.clone(),
            stake: stake.stake,
            locked: account.locked,
            balance: account.amount,
        }
        .into());
    }
    Ok(())
}

/// Tries to refunds the allowance of the access key for a gas refund action.
pub(crate) fn try_refund_allowance(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
    public_key: &PublicKey,
    transfer: &TransferAction,
) -> Result<(), StorageError> {
    if let Some(mut access_key) = get_access_key(state_update, account_id, public_key)? {
        let mut updated = false;
        if let AccessKeyPermission::FunctionCall(function_call_permission) =
            &mut access_key.permission
        {
            if let Some(allowance) = function_call_permission.allowance.as_mut() {
                let new_allowance = allowance.saturating_add(transfer.deposit);
                if new_allowance > *allowance {
                    *allowance = new_allowance;
                    updated = true;
                }
            }
        }
        if updated {
            set_access_key(state_update, account_id.clone(), public_key.clone(), &access_key);
        }
    }
    Ok(())
}

pub(crate) fn action_transfer(
    account: &mut Account,
    transfer: &TransferAction,
) -> Result<(), StorageError> {
    account.amount = account.amount.checked_add(transfer.deposit).ok_or_else(|| {
        StorageError::StorageInconsistentState("Account balance integer overflow".to_string())
    })?;
    Ok(())
}

pub(crate) fn action_create_account(
    fee_config: &RuntimeFeesConfig,
    account_creation_config: &AccountCreationConfig,
    account: &mut Option<Account>,
    actor_id: &mut AccountId,
    account_id: &AccountId,
    predecessor_id: &AccountId,
    result: &mut ActionResult,
) {
    // NOTE: The account_id is valid, because the Receipt is validated before.
    debug_assert!(is_valid_account_id(account_id));

    if is_valid_top_level_account_id(account_id) {
        if account_id.len() < account_creation_config.min_allowed_top_level_account_length as usize
            && predecessor_id != &account_creation_config.registrar_account_id
        {
            // A short top-level account ID can only be created registrar account.
            result.result = Err(ActionErrorKind::CreateAccountOnlyByRegistrar {
                account_id: account_id.clone(),
                registrar_account_id: account_creation_config.registrar_account_id.clone(),
                predecessor_id: predecessor_id.clone(),
            }
            .into());
            return;
        } else {
            // OK: Valid top-level Account ID
        }
    } else if !is_valid_sub_account_id(&predecessor_id, account_id) {
        // The sub-account can only be created by its root account. E.g. `alice.near` only by `near`
        result.result = Err(ActionErrorKind::CreateAccountNotAllowed {
            account_id: account_id.clone(),
            predecessor_id: predecessor_id.clone(),
        }
        .into());
        return;
    } else {
        // OK: Valid sub-account ID by proper predecessor.
    }

    *actor_id = account_id.clone();
    *account = Some(Account {
        amount: 0,
        locked: 0,
        code_hash: CryptoHash::default(),
        storage_usage: fee_config.storage_usage_config.num_bytes_account,
    });
}

pub(crate) fn action_deploy_contract(
    state_update: &mut TrieUpdate,
    account: &mut Account,
    account_id: &AccountId,
    deploy_contract: &DeployContractAction,
) -> Result<(), StorageError> {
    let code = ContractCode::new(deploy_contract.code.clone());
    let prev_code = get_code(state_update, account_id)?;
    let prev_code_length = prev_code.map(|code| code.code.len() as u64).unwrap_or_default();
    account.storage_usage =
        account.storage_usage.checked_sub(prev_code_length).ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "Storage usage integer underflow for account {}",
                account_id
            ))
        })?;
    account.storage_usage =
        account.storage_usage.checked_add(code.code.len() as u64).ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "Storage usage integer overflow for account {}",
                account_id
            ))
        })?;
    account.code_hash = code.get_hash();
    set_code(state_update, account_id.clone(), &code);
    Ok(())
}

pub(crate) fn action_delete_account(
    state_update: &mut TrieUpdate,
    account: &mut Option<Account>,
    actor_id: &mut AccountId,
    receipt: &Receipt,
    result: &mut ActionResult,
    account_id: &AccountId,
    delete_account: &DeleteAccountAction,
) -> Result<(), StorageError> {
    // We use current amount as a pay out to beneficiary.
    let account_balance = account.as_ref().unwrap().amount;
    if account_balance > 0 {
        result
            .new_receipts
            .push(Receipt::new_balance_refund(&delete_account.beneficiary_id, account_balance));
    }
    remove_account(state_update, account_id)?;
    *actor_id = receipt.predecessor_id.clone();
    *account = None;
    Ok(())
}

pub(crate) fn action_delete_key(
    fee_config: &RuntimeFeesConfig,
    state_update: &mut TrieUpdate,
    account: &mut Account,
    result: &mut ActionResult,
    account_id: &AccountId,
    delete_key: &DeleteKeyAction,
) -> Result<(), StorageError> {
    let access_key = get_access_key(state_update, &account_id, &delete_key.public_key)?;
    if access_key.is_none() {
        result.result = Err(ActionErrorKind::DeleteKeyDoesNotExist {
            public_key: delete_key.public_key.clone(),
            account_id: account_id.clone(),
        }
        .into());
        return Ok(());
    }
    // Remove access key
    remove_access_key(state_update, account_id.clone(), delete_key.public_key.clone());
    let storage_usage_config = &fee_config.storage_usage_config;
    account.storage_usage = account
        .storage_usage
        .checked_sub(
            delete_key.public_key.try_to_vec().unwrap().len() as u64
                + access_key.try_to_vec().unwrap().len() as u64
                + storage_usage_config.num_extra_bytes_record,
        )
        .ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "Storage usage integer underflow for account {}",
                account_id
            ))
        })?;
    Ok(())
}

pub(crate) fn action_add_key(
    fees_config: &RuntimeFeesConfig,
    state_update: &mut TrieUpdate,
    account: &mut Account,
    result: &mut ActionResult,
    account_id: &AccountId,
    add_key: &AddKeyAction,
) -> Result<(), StorageError> {
    if get_access_key(state_update, account_id, &add_key.public_key)?.is_some() {
        result.result = Err(ActionErrorKind::AddKeyAlreadyExists {
            account_id: account_id.to_owned(),
            public_key: add_key.public_key.clone(),
        }
        .into());
        return Ok(());
    }
    set_access_key(
        state_update,
        account_id.clone(),
        add_key.public_key.clone(),
        &add_key.access_key,
    );
    let storage_config = &fees_config.storage_usage_config;
    account.storage_usage = account
        .storage_usage
        .checked_add(
            add_key.public_key.try_to_vec().unwrap().len() as u64
                + add_key.access_key.try_to_vec().unwrap().len() as u64
                + storage_config.num_extra_bytes_record,
        )
        .ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "Storage usage integer overflow for account {}",
                account_id
            ))
        })?;
    Ok(())
}

pub(crate) fn check_actor_permissions(
    action: &Action,
    account: &Option<Account>,
    actor_id: &AccountId,
    account_id: &AccountId,
) -> Result<(), ActionError> {
    match action {
        Action::DeployContract(_) | Action::Stake(_) | Action::AddKey(_) | Action::DeleteKey(_) => {
            if actor_id != account_id {
                return Err(ActionErrorKind::ActorNoPermission {
                    account_id: actor_id.clone(),
                    actor_id: account_id.clone(),
                }
                .into());
            }
        }
        Action::DeleteAccount(_) => {
            if actor_id != account_id {
                return Err(ActionErrorKind::ActorNoPermission {
                    account_id: account_id.clone(),
                    actor_id: actor_id.clone(),
                }
                .into());
            }
            let account = account.as_ref().unwrap();
            if account.locked != 0 {
                return Err(ActionErrorKind::DeleteAccountStaking {
                    account_id: account_id.clone(),
                }
                .into());
            }
        }
        Action::CreateAccount(_) | Action::FunctionCall(_) | Action::Transfer(_) => (),
    };
    Ok(())
}

pub(crate) fn check_account_existence(
    action: &Action,
    account: &mut Option<Account>,
    account_id: &AccountId,
) -> Result<(), ActionError> {
    match action {
        Action::CreateAccount(_) => {
            if account.is_some() {
                return Err(ActionErrorKind::AccountAlreadyExists {
                    account_id: account_id.clone().into(),
                }
                .into());
            }
        }
        Action::DeployContract(_)
        | Action::FunctionCall(_)
        | Action::Transfer(_)
        | Action::Stake(_)
        | Action::AddKey(_)
        | Action::DeleteKey(_)
        | Action::DeleteAccount(_) => {
            if account.is_none() {
                return Err(ActionErrorKind::AccountDoesNotExist {
                    account_id: account_id.clone(),
                }
                .into());
            }
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_action_create_account(
        account_id: AccountId,
        predecessor_id: AccountId,
        length: u8,
    ) -> ActionResult {
        let mut account = None;
        let mut actor_id = predecessor_id.clone();
        let mut action_result = ActionResult::default();
        action_create_account(
            &RuntimeFeesConfig::default(),
            &AccountCreationConfig {
                min_allowed_top_level_account_length: length,
                registrar_account_id: AccountId::from("registrar"),
            },
            &mut account,
            &mut actor_id,
            &account_id,
            &predecessor_id,
            &mut action_result,
        );
        if action_result.result.is_ok() {
            assert!(account.is_some());
            assert_eq!(actor_id, account_id);
        } else {
            assert!(account.is_none());
        }
        action_result
    }

    #[test]
    fn test_create_account_valid_top_level_long() {
        let account_id = AccountId::from("bob_near_long_name");
        let predecessor_id = AccountId::from("alice.near");
        let action_result = test_action_create_account(account_id, predecessor_id, 11);
        assert!(action_result.result.is_ok());
    }

    #[test]
    fn test_create_account_valid_top_level_by_registrar() {
        let account_id = AccountId::from("bob");
        let predecessor_id = AccountId::from("registrar");
        let action_result = test_action_create_account(account_id, predecessor_id, 11);
        assert!(action_result.result.is_ok());
    }

    #[test]
    fn test_create_account_valid_sub_account() {
        let account_id = AccountId::from("alice.near");
        let predecessor_id = AccountId::from("near");
        let action_result = test_action_create_account(account_id, predecessor_id, 11);
        assert!(action_result.result.is_ok());
    }

    #[test]
    fn test_create_account_invalid_sub_account() {
        let account_id = AccountId::from("alice.near");
        let predecessor_id = AccountId::from("bob");
        let action_result =
            test_action_create_account(account_id.clone(), predecessor_id.clone(), 11);
        assert_eq!(
            action_result.result,
            Err(ActionError {
                index: None,
                kind: ActionErrorKind::CreateAccountNotAllowed {
                    account_id: account_id.clone(),
                    predecessor_id: predecessor_id.clone(),
                }
            })
        );
    }

    #[test]
    fn test_create_account_invalid_short_top_level() {
        let account_id = AccountId::from("bob");
        let predecessor_id = AccountId::from("near");
        let action_result =
            test_action_create_account(account_id.clone(), predecessor_id.clone(), 11);
        assert_eq!(
            action_result.result,
            Err(ActionError {
                index: None,
                kind: ActionErrorKind::CreateAccountOnlyByRegistrar {
                    account_id: account_id.clone(),
                    registrar_account_id: AccountId::from("registrar"),
                    predecessor_id: predecessor_id.clone(),
                }
            })
        );
    }

    #[test]
    fn test_create_account_valid_short_top_level_len_allowed() {
        let account_id = AccountId::from("bob");
        let predecessor_id = AccountId::from("near");
        let action_result =
            test_action_create_account(account_id.clone(), predecessor_id.clone(), 0);
        assert!(action_result.result.is_ok());
    }
}
