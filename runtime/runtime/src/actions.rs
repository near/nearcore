use std::sync::Arc;

use borsh::BorshSerialize;
use log::debug;

use near_primitives::account::Account;
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, Receipt};
use near_primitives::transaction::{
    Action, AddKeyAction, DeleteAccountAction, DeleteKeyAction, DeployContractAction,
    FunctionCallAction, StakeAction, TransferAction,
};
use near_primitives::types::{AccountId, Balance, BlockIndex, ValidatorStake};
use near_primitives::utils::{
    is_valid_sub_account_id, is_valid_top_level_account_id, key_for_access_key,
};
use near_runtime_fees::RuntimeFeesConfig;
use near_store::{
    get_access_key, get_code, remove_account, set_access_key, set_code, total_account_storage,
    StorageError, TrieUpdate,
};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::VMContext;

use crate::config::RuntimeConfig;
use crate::ext::RuntimeExt;
use crate::{ActionResult, ApplyState};
use near_primitives::errors::ActionError;
use near_vm_errors::{CompilationError, FunctionCallError};
use near_vm_runner::VMError;

/// Number of epochs it takes to unstake.
const NUM_UNSTAKING_EPOCHS: BlockIndex = 3;

fn cost_per_block(
    account_id: &AccountId,
    account: &Account,
    runtime_config: &RuntimeConfig,
) -> Balance {
    let account_length_cost_per_block = if account_id.len() > 10 {
        0
    } else {
        runtime_config.account_length_baseline_cost_per_block
            / 3_u128.pow(account_id.len() as u32 - 2)
    };

    let storage_cost_per_block = u128::from(total_account_storage(account_id, account))
        * runtime_config.storage_cost_byte_per_block;

    account_length_cost_per_block + storage_cost_per_block
}

/// Returns Ok if the account has enough balance to pay storage rent for at least required number of blocks.
/// Otherwise returns the amount required.
/// Validators must have at least enough for `NUM_UNSTAKING_EPOCHS` * epoch_length of blocks,
/// regular users - `poke_threshold` blocks.
pub(crate) fn check_rent(
    account_id: &AccountId,
    account: &Account,
    runtime_config: &RuntimeConfig,
    epoch_length: BlockIndex,
) -> Result<(), u128> {
    let buffer_length = if account.locked > 0 {
        epoch_length * (NUM_UNSTAKING_EPOCHS + 1)
    } else {
        runtime_config.poke_threshold
    };
    let buffer_amount =
        u128::from(buffer_length) * cost_per_block(account_id, account, runtime_config);
    if account.amount >= buffer_amount {
        Ok(())
    } else {
        Err(buffer_amount)
    }
}

/// Subtracts the storage rent from the given account balance.
pub(crate) fn apply_rent(
    account_id: &AccountId,
    account: &mut Account,
    block_index: BlockIndex,
    runtime_config: &RuntimeConfig,
) -> Balance {
    let charge = u128::from(block_index - account.storage_paid_at)
        * cost_per_block(account_id, account, runtime_config);
    let actual_charge = std::cmp::min(account.amount, charge);
    account.amount -= actual_charge;
    account.storage_paid_at = block_index;
    actual_charge
}

pub(crate) fn get_code_with_cache(
    state_update: &TrieUpdate,
    account_id: &AccountId,
    account: &Account,
) -> Result<Option<Arc<ContractCode>>, StorageError> {
    debug!(target:"runtime", "Calling the contract at account {}", account_id);
    let code_hash = account.code_hash;
    let code = || get_code(state_update, account_id);
    crate::cache::get_code_with_cache(code_hash, code)
}

pub(crate) fn action_function_call(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    account: &mut Option<Account>,
    receipt: &Receipt,
    action_receipt: &ActionReceipt,
    promise_results: &[PromiseResult],
    result: &mut ActionResult,
    account_id: &AccountId,
    function_call: &FunctionCallAction,
    action_hash: &CryptoHash,
    config: &RuntimeConfig,
    is_last_action: bool,
) -> Result<(), StorageError> {
    let account = account.as_mut().unwrap();
    let code = match get_code_with_cache(state_update, account_id, &account) {
        Ok(Some(code)) => code,
        Ok(None) => {
            let error = FunctionCallError::CompilationError(CompilationError::CodeDoesNotExist(
                account_id.clone(),
            ));
            result.result = Err(ActionError::FunctionCallError(error.to_string()));
            return Ok(());
        }
        Err(e) => {
            return Err(e);
        }
    };
    let mut runtime_ext = RuntimeExt::new(
        state_update,
        account_id,
        &action_receipt.signer_id,
        &action_receipt.signer_public_key,
        action_receipt.gas_price,
        action_hash,
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
        account_balance: account.amount,
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
    if let Some(err) = err {
        if let VMError::StorageError(storage) = err {
            let err: StorageError =
                borsh::BorshDeserialize::try_from_slice(&storage).expect("Borsh cannot fail");
            return Err(err);
        }
        // TODO(#1731): Handle VMError::FunctionCallError better.
        result.result = Err(ActionError::FunctionCallError(err.to_string()));
        if let Some(outcome) = outcome {
            result.gas_burnt += outcome.burnt_gas;
            result.gas_burnt_for_function_call += outcome.burnt_gas;
            // Runtime in `generate_refund_receipts` takes care of using proper value for refunds.
            // It uses `gas_used` for success and `gas_burnt` for failures. So it's not an issue to
            // return a real `gas_used` instead of the `gas_burnt` into `ActionResult` for
            // `FunctionCall`s.
            result.gas_used += outcome.used_gas;
            result.logs.extend(outcome.logs.into_iter());
        }
        return Ok(());
    }
    let outcome = outcome.unwrap();
    result.logs.extend(outcome.logs.into_iter());
    account.amount = outcome.balance;
    account.storage_usage = outcome.storage_usage;
    result.gas_burnt += outcome.burnt_gas;
    result.gas_burnt_for_function_call += outcome.burnt_gas;
    result.gas_used += outcome.used_gas;
    result.result = Ok(outcome.return_data);
    result.new_receipts.append(&mut runtime_ext.into_receipts(account_id));
    Ok(())
}

pub(crate) fn action_stake(
    account: &mut Option<Account>,
    result: &mut ActionResult,
    account_id: &AccountId,
    stake: &StakeAction,
) {
    let mut account = account.as_mut().unwrap();
    let increment = stake.stake.saturating_sub(account.locked);
    if account.amount >= increment {
        if account.locked == 0 && stake.stake == 0 {
            // if the account hasn't staked, it cannot unstake
            result.result = Err(ActionError::TriesToUnstake(account_id.clone()));
            return;
        }
        result.validator_proposals.push(ValidatorStake {
            account_id: account_id.clone(),
            public_key: stake.public_key.clone(),
            amount: stake.stake,
        });
        if stake.stake > account.locked {
            account.amount -= increment;
            account.locked = stake.stake;
        }
    } else {
        result.result = Err(ActionError::TriesToStake(
            account_id.clone(),
            stake.stake,
            account.locked,
            account.amount,
        ));
    }
}

pub(crate) fn action_transfer(account: &mut Option<Account>, transfer: &TransferAction) {
    account.as_mut().unwrap().amount += transfer.deposit;
}

pub(crate) fn action_create_account(
    fee_config: &RuntimeFeesConfig,
    apply_state: &ApplyState,
    account: &mut Option<Account>,
    actor_id: &mut AccountId,
    receipt: &Receipt,
    result: &mut ActionResult,
) {
    let account_id = &receipt.receiver_id;
    if !is_valid_top_level_account_id(account_id)
        && !is_valid_sub_account_id(&receipt.predecessor_id, account_id)
    {
        result.result = Err(ActionError::CreateAccountNotAllowed(
            account_id.clone(),
            receipt.predecessor_id.clone(),
        ));
        return;
    }
    *actor_id = receipt.receiver_id.clone();
    *account = Some(Account::new(0, CryptoHash::default(), apply_state.block_index));
    account.as_mut().unwrap().storage_usage = fee_config.storage_usage_config.account_cost;
}

// TODO(#1461): Add Safe Math
pub(crate) fn action_deploy_contract(
    fee_config: &RuntimeFeesConfig,
    state_update: &mut TrieUpdate,
    account: &mut Option<Account>,
    account_id: &AccountId,
    deploy_contract: &DeployContractAction,
) -> Result<(), StorageError> {
    let account = account.as_mut().unwrap();
    let code = ContractCode::new(deploy_contract.code.clone());
    let prev_code = get_code(state_update, account_id)?;
    let prev_code_length = prev_code.map(|code| code.code.len() as u64).unwrap_or_default();
    let storage_usage_config = &fee_config.storage_usage_config;
    account.storage_usage -= prev_code_length * storage_usage_config.code_cost_per_byte;
    account.storage_usage += (code.code.len() as u64) * storage_usage_config.code_cost_per_byte;
    account.code_hash = code.get_hash();
    set_code(state_update, &account_id, &code);
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
    // We use current amount as a reward, because this account's storage rent was updated before
    // calling this function.
    let account_balance = account.as_ref().unwrap().amount;
    if account_balance > 0 {
        result
            .new_receipts
            .push(Receipt::new_refund(&delete_account.beneficiary_id, account_balance));
    }
    remove_account(state_update, account_id)?;
    *actor_id = receipt.predecessor_id.clone();
    *account = None;
    Ok(())
}

// TODO(#1461): Add Safe Math
pub(crate) fn action_delete_key(
    fee_config: &RuntimeFeesConfig,
    state_update: &mut TrieUpdate,
    account: &mut Option<Account>,
    result: &mut ActionResult,
    account_id: &AccountId,
    delete_key: &DeleteKeyAction,
) -> Result<(), StorageError> {
    let account = account.as_mut().unwrap();
    let access_key = get_access_key(state_update, account_id, &delete_key.public_key)?;
    if access_key.is_none() {
        result.result = Err(ActionError::DeleteKeyDoesNotExist(account_id.clone()));
        return Ok(());
    }
    // Remove access key
    state_update.remove(&key_for_access_key(account_id, &delete_key.public_key));
    let storage_usage_config = &fee_config.storage_usage_config;
    account.storage_usage -= (delete_key.public_key.try_to_vec().ok().unwrap_or_default().len()
        as u64)
        * storage_usage_config.key_cost_per_byte;
    account.storage_usage -= (access_key.unwrap().try_to_vec().ok().unwrap_or_default().len()
        as u64)
        * storage_usage_config.value_cost_per_byte;
    account.storage_usage -= storage_usage_config.data_record_cost;
    Ok(())
}

// TODO(#1461): Add Safe Math
pub(crate) fn action_add_key(
    fees_config: &RuntimeFeesConfig,
    state_update: &mut TrieUpdate,
    account: &mut Option<Account>,
    result: &mut ActionResult,
    account_id: &AccountId,
    add_key: &AddKeyAction,
) -> Result<(), StorageError> {
    let account = account.as_mut().unwrap();
    if get_access_key(state_update, account_id, &add_key.public_key)?.is_some() {
        result.result = Err(ActionError::AddKeyAlreadyExists(add_key.public_key.clone()));
        return Ok(());
    }
    set_access_key(state_update, account_id, &add_key.public_key, &add_key.access_key);
    let storage_config = &fees_config.storage_usage_config;
    account.storage_usage += (add_key.public_key.try_to_vec().ok().unwrap_or_default().len()
        as u64)
        * storage_config.key_cost_per_byte;
    account.storage_usage += (add_key.access_key.try_to_vec().ok().unwrap_or_default().len()
        as u64)
        * storage_config.value_cost_per_byte;
    account.storage_usage += storage_config.data_record_cost;
    Ok(())
}

pub(crate) fn check_actor_permissions(
    action: &Action,
    apply_state: &ApplyState,
    account: &mut Option<Account>,
    actor_id: &AccountId,
    account_id: &AccountId,
    config: &RuntimeConfig,
) -> Result<(), ActionError> {
    match action {
        Action::DeployContract(_) | Action::Stake(_) | Action::AddKey(_) | Action::DeleteKey(_) => {
            if actor_id != account_id {
                return Err(ActionError::ActorNoPermission(
                    actor_id.clone(),
                    account_id.clone(),
                    action_type_as_string(action).to_owned(),
                ));
            }
        }
        Action::DeleteAccount(_) => {
            if account.as_ref().unwrap().locked != 0 {
                return Err(ActionError::DeleteAccountStaking(account_id.clone()));
            }
            if actor_id != account_id
                && check_rent(
                    account_id,
                    account.as_mut().unwrap(),
                    config,
                    apply_state.epoch_length,
                )
                .is_ok()
            {
                return Err(ActionError::DeleteAccountHasRent(
                    account_id.clone(),
                    account.as_ref().unwrap().amount,
                ));
            }
        }
        Action::CreateAccount(_) | Action::FunctionCall(_) | Action::Transfer(_) => (),
    };
    Ok(())
}

fn action_type_as_string(action: &Action) -> &'static str {
    match action {
        Action::CreateAccount(_) => "CreateAccount",
        Action::DeployContract(_) => "DeployContract",
        Action::FunctionCall(_) => "FunctionCall",
        Action::Transfer(_) => "Transfer",
        Action::Stake(_) => "Stake",
        Action::AddKey(_) => "AddKey",
        Action::DeleteKey(_) => "DeleteKey",
        Action::DeleteAccount(_) => "DeleteAccount",
    }
}

pub(crate) fn check_account_existence(
    action: &Action,
    account: &mut Option<Account>,
    account_id: &AccountId,
) -> Result<(), ActionError> {
    match action {
        Action::CreateAccount(_) => {
            if account.is_some() {
                return Err(ActionError::AccountAlreadyExists(account_id.clone()));
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
                return Err(ActionError::AccountDoesNotExist(
                    action_type_as_string(action).to_owned(),
                    account_id.clone(),
                ));
            }
            //
        }
    };
    Ok(())
}
