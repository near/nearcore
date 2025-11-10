use crate::config::safe_add_balance;
use crate::global_contracts::use_global_contract;
use crate::verifier::{StorageStakingError, check_storage_stake};
use crate::{ActionResult, ApplyState};
use near_parameters::StorageUsageConfig;
use near_primitives::account::{Account, AccountContract};
use near_primitives::action::DeterministicStateInitAction;
use near_primitives::errors::{IntegerOverflowError, RuntimeError};
use near_primitives::receipt::Receipt;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, Balance};
use near_primitives_core::deterministic_account_id::DeterministicAccountStateInit;
use near_store::{StorageError, TrieUpdate};
use near_vm_runner::logic::ProtocolVersion;

pub(crate) fn action_deterministic_state_init(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    maybe_account: &mut Option<Account>,
    account_id: &AccountId,
    receipt: &Receipt,
    action: &DeterministicStateInitAction,
    result: &mut ActionResult,
) -> Result<(), RuntimeError> {
    // See https://github.com/near/NEPs/blob/master/neps/nep-0616.md#account-state
    // for the detailed description around deterministic account state.
    let storage_usage_config = &apply_state.config.fees.storage_usage_config;
    let account = match maybe_account {
        Some(account) => account,
        None => {
            // `nonexist` -> `uninit` account state transition
            // Create with zero balance now and check later how much of the
            // provided deposit is needed.
            let new_account = create_deterministic_account(Balance::ZERO, storage_usage_config);
            *maybe_account = Some(new_account);
            maybe_account.as_mut().expect("account must exist now")
        }
    };
    if account.contract().is_none() {
        // `uninit` -> `active` account state transition
        deploy_deterministic_account(
            state_update,
            account,
            account_id,
            &action.state_init,
            result,
            storage_usage_config,
            apply_state.current_protocol_version,
        )?;
    }
    if result.result.is_err() {
        return Ok(());
    }

    // Use attached deposit to satisfy storage staking requirements and refund
    // the rest.
    let deposit_refund = match check_storage_stake(account, account.amount(), &apply_state.config) {
        Ok(_) => {
            // no additional storage needed, refunding all
            action.deposit
        }
        Err(StorageStakingError::LackBalanceForStorageStaking(missing_amount)) => {
            if missing_amount <= action.deposit {
                // use exactly as much as needed and refund the rest
                let new_balance = safe_add_balance(account.amount(), missing_amount)?;
                account.set_amount(new_balance);
                action
                    .deposit
                    .checked_sub(missing_amount)
                    .expect("just checked missing_amount <= action.deposit")
            } else {
                // not enough balance at this point -> no refund
                // (following actions might be able to fix the balance
                // requirements but these will not affect this refund)
                let new_balance = safe_add_balance(account.amount(), action.deposit)?;
                account.set_amount(new_balance);
                Balance::ZERO
            }
        }
        Err(StorageStakingError::StorageError(err)) => {
            return Err(RuntimeError::StorageError(StorageError::StorageInconsistentState(err)));
        }
    };

    if deposit_refund > Balance::ZERO {
        result.new_receipts.push(Receipt::new_balance_refund(
            receipt.balance_refund_receiver(),
            deposit_refund,
            receipt.priority(),
        ));
    }

    Ok(())
}

pub(crate) fn create_deterministic_account(
    initial_balance: Balance,
    storage_usage_config: &StorageUsageConfig,
) -> Account {
    // Unlike `CreateAccount`, this account creation does not change
    // actor_id. This is important to prevent hijacking the account.
    // Actor id remains the predecessor, so any actions following will
    // be checked against that for actor permissions, preventing
    // `AddKey`, `DeployContract`, or any other actions that only the
    // account owner is permitted to do.
    Account::new(
        initial_balance,
        Balance::ZERO,
        AccountContract::None,
        storage_usage_config.num_bytes_account,
    )
}

/// Take the content of a `StateInit` and deploy it on the account.
///
/// Pre-condition: The account must not have any contract data stored when this
/// is called. Otherwise, the storage usage calculations would fail to take
/// overwritten existing values into account.
/// (It would be possible to read value refs first and subtract their length but
/// that is unnecessary work since the pre-condition above holds at the moment.)
fn deploy_deterministic_account(
    state_update: &mut TrieUpdate,
    account: &mut Account,
    account_id: &AccountId,
    state_init: &DeterministicAccountStateInit,
    result: &mut ActionResult,
    storage_usage_config: &StorageUsageConfig,
    current_protocol_version: ProtocolVersion,
) -> Result<(), RuntimeError> {
    // Step 1: set contract code (includes storage usage accounting)
    use_global_contract(
        state_update,
        account_id,
        account,
        state_init.code(),
        current_protocol_version,
        result,
    )?;
    if result.result.is_err() {
        return Ok(());
    }

    // Step 2: insert provided key-value pairs
    let mut required_storage_usage = account.storage_usage();
    for (key, value) in state_init.data() {
        let trie_key = TrieKey::ContractData { account_id: account_id.clone(), key: key.to_vec() };

        let value_bytes = value.len() as u64;
        let key_bytes = key.len() as u64;
        let extra_per_record_bytes = storage_usage_config.num_extra_bytes_record;

        let new_bytes = value_bytes
            .checked_add(key_bytes)
            .and_then(|acc| acc.checked_add(extra_per_record_bytes))
            .ok_or(IntegerOverflowError {})?;
        state_update.set(trie_key, value.clone());
        required_storage_usage =
            required_storage_usage.checked_add(new_bytes).ok_or(IntegerOverflowError {})?;
    }
    account.set_storage_usage(required_storage_usage);

    Ok(())
}
