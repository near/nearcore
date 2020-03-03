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
use near_primitives::types::{AccountId, Balance, BlockHeight, BlockHeightDelta, ValidatorStake};
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

use crate::config::{safe_add_gas, RuntimeConfig};
use crate::ext::RuntimeExt;
use crate::{ActionResult, ApplyState};
use near_crypto::key_conversion::convert_public_key;
use near_crypto::PublicKey;
use near_primitives::errors::{ActionError, ActionErrorKind, RuntimeError};
use near_vm_errors::{CompilationError, FunctionCallError};
use near_vm_runner::VMError;

/// Number of epochs it takes to unstake.
const NUM_UNSTAKING_EPOCHS: u64 = 3;

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
    epoch_length: BlockHeightDelta,
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
    block_height: BlockHeight,
    runtime_config: &RuntimeConfig,
) -> Result<Balance, StorageError> {
    let block_difference = block_height.checked_sub(account.storage_paid_at).ok_or_else(|| {
        StorageError::StorageInconsistentState(format!(
            "storage_paid_at {} for account {} is larger than current block height {}",
            account.storage_paid_at, account_id, block_height
        ))
    })?;
    let charge = u128::from(block_difference) * cost_per_block(account_id, account, runtime_config);
    let actual_charge = std::cmp::min(account.amount, charge);
    account.amount -= actual_charge;
    account.storage_paid_at = block_height;
    Ok(actual_charge)
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
        Some(VMError::ExternalError(storage)) => {
            let err: StorageError =
                borsh::BorshDeserialize::try_from_slice(&storage).expect("Borsh cannot fail");
            return Err(err.into());
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
) {
    let increment = stake.stake.saturating_sub(account.locked);

    // Make sure the key is ED25519, and can be converted to ristretto
    match stake.public_key {
        PublicKey::ED25519(key) => {
            if convert_public_key(&key).is_none() {
                result.result = Err(ActionErrorKind::UnsuitableStakingKey {
                    public_key: stake.public_key.clone(),
                }
                .into());
                return;
            }
        }
        PublicKey::SECP256K1(_) => {
            result.result =
                Err(ActionErrorKind::UnsuitableStakingKey { public_key: stake.public_key.clone() }
                    .into());
            return;
        }
    };

    if account.amount >= increment {
        if account.locked == 0 && stake.stake == 0 {
            // if the account hasn't staked, it cannot unstake
            result.result =
                Err(ActionErrorKind::TriesToUnstake { account_id: account_id.clone() }.into());
            return;
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
        result.result = Err(ActionErrorKind::CreateAccountNotAllowed {
            account_id: account_id.clone(),
            predecessor_id: receipt.predecessor_id.clone(),
        }
        .into());
        return;
    }
    *actor_id = receipt.receiver_id.clone();
    *account = Some(Account {
        amount: 0,
        locked: 0,
        code_hash: CryptoHash::default(),
        storage_usage: fee_config.storage_usage_config.num_bytes_account,
        storage_paid_at: apply_state.block_index,
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

pub(crate) fn action_delete_key(
    fee_config: &RuntimeFeesConfig,
    state_update: &mut TrieUpdate,
    account: &mut Account,
    result: &mut ActionResult,
    account_id: &AccountId,
    delete_key: &DeleteKeyAction,
) -> Result<(), StorageError> {
    let access_key = get_access_key(state_update, account_id, &delete_key.public_key)?;
    if access_key.is_none() {
        result.result = Err(ActionErrorKind::DeleteKeyDoesNotExist {
            public_key: delete_key.public_key.clone(),
            account_id: account_id.clone(),
        }
        .into());
        return Ok(());
    }
    // Remove access key
    state_update.remove(&key_for_access_key(account_id, &delete_key.public_key));
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
    set_access_key(state_update, account_id, &add_key.public_key, &add_key.access_key);
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
    apply_state: &ApplyState,
    account: &Option<Account>,
    actor_id: &AccountId,
    account_id: &AccountId,
    config: &RuntimeConfig,
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
            let account = account.as_ref().unwrap();
            if account.locked != 0 {
                return Err(ActionErrorKind::DeleteAccountStaking {
                    account_id: account_id.clone(),
                }
                .into());
            }
            if actor_id != account_id
                && check_rent(account_id, account, config, apply_state.epoch_length).is_ok()
            {
                return Err(ActionErrorKind::DeleteAccountHasRent {
                    account_id: account_id.clone(),
                    balance: account.amount,
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
