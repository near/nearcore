use std::sync::Arc;

use borsh::BorshSerialize;

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

/// Returns true if the account has enough balance to pay storage rent for at least required number of blocks.
/// Validators must have at least enough for `NUM_UNSTAKING_EPOCHS` * epoch_length of blocks,
/// regular users - `poke_threshold` blocks.
pub(crate) fn check_rent(
    account_id: &AccountId,
    account: &Account,
    runtime_config: &RuntimeConfig,
    epoch_length: BlockIndex,
) -> bool {
    let buffer_length = if account.staked > 0 {
        epoch_length * (NUM_UNSTAKING_EPOCHS + 1)
    } else {
        runtime_config.poke_threshold
    };
    let buffer_amount =
        u128::from(buffer_length) * cost_per_block(account_id, account, runtime_config);
    account.amount >= buffer_amount
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
            result.result =
                Err(format!("cannot find contract code for account {}", account_id.clone()).into());
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
        free_of_charge: false,
        output_data_receivers,
    };

    let (outcome, err) = near_vm_runner::run(
        code.hash.as_ref().to_vec(),
        &code.code,
        function_call.method_name.as_bytes(),
        &mut runtime_ext,
        context,
        &config.wasm_config,
        promise_results,
    );
    if let Some(err) = err {
        result.result =
            Err(format!("wasm async call execution failed with error: {:?}", err).into());
        if let Some(outcome) = outcome {
            result.gas_burnt += outcome.burnt_gas;
            result.logs.extend(outcome.logs.into_iter());
        }
        return Ok(());
    }
    let outcome = outcome.unwrap();
    result.logs.extend(outcome.logs.into_iter());
    account.amount = outcome.balance;
    account.storage_usage = outcome.storage_usage;
    result.gas_burnt += outcome.burnt_gas;
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
    let increment = stake.stake.saturating_sub(account.staked);
    if account.amount >= increment {
        if account.staked == 0 && stake.stake == 0 {
            // if the account hasn't staked, it cannot unstake
            result.result =
                Err(format!("Account {:?} is not yet staked, but tries to unstake", account_id)
                    .into());
            return;
        }
        result.validator_proposals.push(ValidatorStake {
            account_id: account_id.clone(),
            public_key: stake.public_key.clone(),
            amount: stake.stake,
        });
        if stake.stake > account.staked {
            account.amount -= increment;
            account.staked = stake.stake;
        }
    } else {
        result.result = Err(format!(
            "Account {:?} tries to stake {}, but has staked {} and only has {}",
            account_id, stake.stake, account.staked, account.amount,
        )
        .into());
        return;
    }
}

pub(crate) fn action_transfer(account: &mut Option<Account>, transfer: &TransferAction) {
    account.as_mut().unwrap().amount += transfer.deposit;
}

pub(crate) fn action_create_account(
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
        result.result = Err(format!(
            "The new account_id {:?} can't be created by {:?}",
            account_id, &receipt.predecessor_id
        )
        .into());
        return;
    }
    *actor_id = receipt.receiver_id.clone();
    let storage_config = RuntimeFeesConfig::default().storage_usage_config;
    *account = Some(Account::new(0, CryptoHash::default(), apply_state.block_index));
    account.as_mut().unwrap().storage_usage = storage_config.account_cost;
}

pub(crate) fn action_deploy_contract(
    state_update: &mut TrieUpdate,
    account: &mut Option<Account>,
    account_id: &AccountId,
    deploy_contract: &DeployContractAction,
) -> Result<(), StorageError> {
    let account = account.as_mut().unwrap();
    let code = ContractCode::new(deploy_contract.code.clone());
    let prev_code = get_code(state_update, account_id)?;
    let prev_code_length = prev_code.map(|code| code.code.len() as u64).unwrap_or_default();
    let storage_config = RuntimeFeesConfig::default().storage_usage_config;
    account.storage_usage -= prev_code_length * storage_config.code_cost_per_byte;
    account.storage_usage += (code.code.len() as u64) * storage_config.code_cost_per_byte;
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
) {
    // We use current amount as a reward, because this account's storage rent was updated before
    // calling this function.
    let account_balance = account.as_ref().unwrap().amount;
    if account_balance > 0 {
        result
            .new_receipts
            .push(Receipt::new_refund(&delete_account.beneficiary_id, account_balance));
    }
    if remove_account(state_update, account_id).is_err() {
        result.result =
            Err(format!("Failed to delete all account data for account {:?}", account_id).into());
        return;
    }
    *actor_id = receipt.predecessor_id.clone();
    *account = None;
}

pub(crate) fn action_delete_key(
    state_update: &mut TrieUpdate,
    account: &mut Option<Account>,
    result: &mut ActionResult,
    account_id: &AccountId,
    delete_key: &DeleteKeyAction,
) -> Result<(), StorageError> {
    let account = account.as_mut().unwrap();
    let access_key = get_access_key(state_update, account_id, &delete_key.public_key)?;
    if access_key.is_none() {
        result.result = Err(format!(
            "Account {:?} tries to remove an access key that doesn't exist",
            account_id
        )
        .into());
        return Ok(());
    }
    // Remove access key
    state_update.remove(&key_for_access_key(account_id, &delete_key.public_key));
    let storage_config = RuntimeFeesConfig::default().storage_usage_config;
    account.storage_usage -= (delete_key.public_key.try_to_vec().ok().unwrap_or_default().len()
        as u64)
        * storage_config.key_cost_per_byte;
    account.storage_usage -= (access_key.unwrap().try_to_vec().ok().unwrap_or_default().len()
        as u64)
        * storage_config.value_cost_per_byte;
    account.storage_usage -= storage_config.data_record_cost;
    Ok(())
}

pub(crate) fn action_add_key(
    state_update: &mut TrieUpdate,
    account: &mut Option<Account>,
    result: &mut ActionResult,
    account_id: &AccountId,
    add_key: &AddKeyAction,
) -> Result<(), StorageError> {
    let account = account.as_mut().unwrap();
    if get_access_key(state_update, account_id, &add_key.public_key)?.is_some() {
        result.result = Err(format!(
            "The public key {:?} is already used for an existing access key",
            &add_key.public_key
        )
        .into());
        return Ok(());
    }
    set_access_key(state_update, account_id, &add_key.public_key, &add_key.access_key);
    let storage_config = RuntimeFeesConfig::default().storage_usage_config;
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
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        Action::DeployContract(_) | Action::Stake(_) | Action::AddKey(_) | Action::DeleteKey(_) => {
            if actor_id != account_id {
                return Err(format!(
                    "Actor {:?} doesn't have permission to account {:?} to complete the action {:?}", actor_id, account_id, action
                )
                    .into());
            }
        }
        Action::DeleteAccount(_) => {
            if account.as_ref().unwrap().staked != 0 {
                return Err(
                    format!("Account {:?} is staking, can not be deleted.", account_id).into()
                );
            }
            if actor_id != account_id
                && check_rent(
                    account_id,
                    account.as_mut().unwrap(),
                    config,
                    apply_state.epoch_length,
                )
            {
                return Err(format!(
                    "Account {:?} has {:?}, which is enough to cover the storage rent.",
                    account_id,
                    account.as_ref().unwrap().amount
                )
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
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        Action::CreateAccount(_) => {
            if account.is_some() {
                return Err(format!(
                    "Can't create a new account {:?}, because it already exists",
                    account_id
                )
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
                return Err(format!(
                    "Can't complete the action {:?}, because account {:?} doesn't exist",
                    action, account_id
                )
                .into());
            }
            //
        }
    };
    Ok(())
}
