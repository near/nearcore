use crate::config::RuntimeConfig;
use crate::ethereum::EthashProvider;
use crate::ext::RuntimeExt;
use crate::{ActionResult, ApplyState};
use near_primitives::account::Account;
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, Receipt};
use near_primitives::transaction::{
    Action, AddKeyAction, DeleteAccountAction, DeleteKeyAction, DeployContractAction,
    FunctionCallAction, StakeAction, TransferAction,
};
use near_primitives::types::{AccountId, Balance, BlockIndex, ValidatorStake};
use near_primitives::utils::{is_valid_account_id, key_for_access_key};
use near_store::{
    get_access_key, get_code, remove_account, set_access_key, set_code, total_account_storage,
    TrieUpdate,
};
use std::sync::{Arc, Mutex};
use wasm::executor;
use wasm::types::RuntimeContext;

/// Number of epochs it takes to unstake.
const NUM_UNSTAKING_EPOCHS: BlockIndex = 3;

/// Returns true if the account has enough balance to pay storage rent for at least required number of blocks.
/// Validators must have at least enough for `NUM_UNSTAKING_EPOCHS` * epoch_length of blocks,
/// regular users - `poke_threshold` blocks.
pub(crate) fn check_rent(
    account_id: &AccountId,
    account: &mut Account,
    runtime_config: &RuntimeConfig,
    epoch_length: BlockIndex,
) -> bool {
    let buffer_length = if account.staked > 0 {
        epoch_length * (NUM_UNSTAKING_EPOCHS + 1)
    } else {
        runtime_config.poke_threshold
    };
    let buffer_amount = (buffer_length as u128)
        * (total_account_storage(account_id, account) as u128)
        * runtime_config.storage_cost_byte_per_block;
    account.amount >= buffer_amount
}

/// Subtracts the storage rent from the given account balance.
pub(crate) fn apply_rent(
    account_id: &AccountId,
    account: &mut Account,
    block_index: BlockIndex,
    config: &RuntimeConfig,
) {
    let charge = ((block_index - account.storage_paid_at) as u128)
        * (total_account_storage(account_id, account) as u128)
        * config.storage_cost_byte_per_block;
    account.amount = account.amount.saturating_sub(charge);
    account.storage_paid_at = block_index;
}

pub(crate) fn get_code_with_cache(
    state_update: &TrieUpdate,
    account_id: &AccountId,
    account: &Account,
) -> Result<Arc<ContractCode>, String> {
    debug!(target:"runtime", "Calling the contract at account {}", account_id);
    let code_hash = account.code_hash;
    let code = || {
        get_code(state_update, account_id)
            .ok_or_else(|| format!("cannot find contract code for account {}", account_id.clone()))
    };
    wasm::cache::get_code_with_cache(code_hash, code)
}

pub(crate) fn action_function_call(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    account: &mut Option<Account>,
    receipt: &Receipt,
    action_receipt: &ActionReceipt,
    input_data: &Vec<Option<Vec<u8>>>,
    result: &mut ActionResult,
    account_id: &AccountId,
    function_call: &FunctionCallAction,
    action_hash: &CryptoHash,
    config: &RuntimeConfig,
    ethash_provider: Arc<Mutex<EthashProvider>>,
) {
    let account = account.as_mut().unwrap();
    let code = match get_code_with_cache(state_update, account_id, &account) {
        Ok(code) => code,
        Err(e) => {
            result.result = Err(e.into());
            return;
        }
    };
    let mut runtime_ext = RuntimeExt::new(
        state_update,
        account_id,
        ethash_provider,
        &action_receipt.signer_id,
        &action_receipt.signer_public_key,
        action_receipt.gas_price,
        action_hash,
    );
    account.amount += function_call.deposit;
    let start_gas = function_call.gas;
    let mut wasm_res = match executor::execute(
        &code,
        &function_call.method_name.as_bytes().to_vec(),
        &function_call.args,
        &input_data,
        &mut runtime_ext,
        &config.wasm_config,
        &RuntimeContext::new(
            account.amount,
            (function_call.gas as Balance) * action_receipt.gas_price,
            &receipt.predecessor_id,
            account_id,
            account.storage_usage,
            apply_state.block_index,
            receipt.receipt_id.as_ref().to_vec(),
            false,
            &action_receipt.signer_id,
            &action_receipt.signer_public_key,
        ),
    ) {
        Ok(exo) => exo,
        Err(e) => {
            result.result =
                Err(format!("wasm async call preparation failed with error: {:?}", e).into());
            return;
        }
    };
    result.logs.append(&mut wasm_res.logs);
    account.storage_usage = wasm_res.storage_usage;
    let remaining_gas = (wasm_res.liquid_balance / action_receipt.gas_price) as u64;
    let gas_used = if start_gas > remaining_gas { start_gas - remaining_gas } else { 0 };
    result.gas_burnt += gas_used;
    result.gas_used += gas_used;
    result.result = wasm_res
        .return_data
        .map_err(|e| format!("wasm async call execution failed with error: {:?}", e).into());
    result.new_receipts.append(&mut runtime_ext.into_receipts(account_id));
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
            public_key: stake.public_key,
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
) {
    // TODO(#968): Validate new name according to ANS
    *actor_id = receipt.receiver_id.clone();
    *account = Some(Account::new(vec![], 0, CryptoHash::default(), apply_state.block_index));
}

pub(crate) fn action_deploy_contract(
    state_update: &mut TrieUpdate,
    account: &mut Option<Account>,
    account_id: &AccountId,
    deploy_contract: &DeployContractAction,
) {
    let code = ContractCode::new(deploy_contract.code.clone());
    account.as_mut().unwrap().code_hash = code.get_hash();
    set_code(state_update, &account_id, &code);
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
    if let Err(_) = remove_account(state_update, account_id) {
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
) {
    let account = account.as_mut().unwrap();
    let num_keys = account.public_keys.len();
    account.public_keys.retain(|&x| x != delete_key.public_key);
    if account.public_keys.len() == num_keys {
        if get_access_key(state_update, account_id, &delete_key.public_key).is_none() {
            result.result = Err(format!(
                "Account {:?} tries to remove a public key that it does not own",
                account_id
            )
            .into());
            return;
        }
        // Remove access key
        // TODO: No refunds for now
        state_update.remove(&key_for_access_key(account_id, &delete_key.public_key));
    }
}

pub(crate) fn action_add_key(
    state_update: &mut TrieUpdate,
    account: &mut Option<Account>,
    result: &mut ActionResult,
    account_id: &AccountId,
    add_key: &AddKeyAction,
) {
    // TODO: OOPS NO WAY TO ADD NORMAL PUBLIC KEYS
    let account = account.as_mut().unwrap();
    let num_keys = account.public_keys.len();
    account.public_keys.retain(|&x| x != add_key.public_key);
    if account.public_keys.len() < num_keys {
        result.result =
            Err("Cannot add a public key that already exists on the account".to_string().into());
        return;
    }
    if get_access_key(state_update, account_id, &add_key.public_key).is_some() {
        result.result =
            Err("Cannot add a public key that already used for an access key".to_string().into());
        return;
    }
    // TODO: FIX THIS HACK
    if add_key.access_key.contract_id.is_some() {
        if account.amount >= add_key.access_key.amount {
            account.amount -= add_key.access_key.amount;
        } else {
            result.result = Err(format!(
                "Account {:?} tries to create new access key with {} amount, but only has {}",
                account_id, add_key.access_key.amount, account.amount
            )
            .into());
            return;
        }
        if let Some(ref balance_owner) = add_key.access_key.balance_owner {
            if !is_valid_account_id(balance_owner) {
                result.result = Err("Invalid account ID for balance owner in the access key"
                    .to_string()
                    .into());
                return;
            }
        }
        if let Some(ref contract_id) = add_key.access_key.contract_id {
            if !is_valid_account_id(contract_id) {
                result.result =
                    Err("Invalid account ID for contract ID in the access key".to_string().into());
                return;
            }
        }
        set_access_key(state_update, account_id, &add_key.public_key, &add_key.access_key);
    } else {
        account.public_keys.push(add_key.public_key.clone());
    }
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
