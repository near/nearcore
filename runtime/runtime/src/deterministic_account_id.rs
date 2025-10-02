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

/// State machine of a deterministic account id on chain.
///
/// This follows the definition in [NEP-616](https://github.com/near/NEPs/pull/616/).
enum AccountState {
    /// There were no accepted receipts on this account, so it doesn't have any
    /// data (or the contract was deleted). Initially all deterministic accounts
    /// are in this state.
    NonExisting,
    /// Account exists and contains balance and meta info but no contract has
    /// been deployed. An account enters this state, for example, when it was in
    /// a non-exist state, and another account sent native transfer to it.
    Uninit,
    /// Account has contract code deployed, persistent data and balance. An
    /// account enters this state when it was in non-exist or uninit state and
    /// there was a valid incoming StateInit action.
    Active,
}

impl AccountState {
    fn of(maybe_account: Option<&Account>) -> AccountState {
        let Some(account) = maybe_account else {
            return AccountState::NonExisting;
        };

        if account.contract().is_none() {
            return AccountState::Uninit;
        }

        AccountState::Active
    }
}

pub(crate) fn action_deterministic_state_init(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    account: &mut Option<Account>,
    account_id: &AccountId,
    receipt: &Receipt,
    action: &DeterministicStateInitAction,
    result: &mut ActionResult,
) -> Result<(), RuntimeError> {
    let current_protocol_version = apply_state.current_protocol_version;
    let storage_usage_config = &apply_state.config.fees.storage_usage_config;
    match AccountState::of(account.as_ref()) {
        AccountState::NonExisting => {
            create_deterministic_account(account, storage_usage_config);
            deploy_deterministic_account(
                state_update,
                account.as_mut().expect("account must exist now"),
                account_id,
                &action.state_init,
                result,
                storage_usage_config,
                current_protocol_version,
            )?;
        }
        AccountState::Uninit => {
            deploy_deterministic_account(
                state_update,
                account.as_mut().expect("account must exist in uninit"),
                account_id,
                &action.state_init,
                result,
                storage_usage_config,
                current_protocol_version,
            )?;
        }
        AccountState::Active => {
            // Account already exists, do nothing.
        }
    }
    if result.result.is_err() {
        return Ok(());
    }
    debug_assert!(
        matches!(AccountState::of(account.as_ref()), AccountState::Active),
        "account must be active now"
    );

    // Use attached deposit to satisfy storage staking requirements and refund
    // the rest.
    let account = account.as_mut().expect("account must exist now");
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
    account: &mut Option<Account>,
    storage_usage_config: &StorageUsageConfig,
) {
    // Unlike `CreateAccount`, this account creation does not change
    // actor_id. This is important to prevent hijacking the account.
    // Actor id remains the predecessor, so any actions following will
    // be checked against that for actor permissions, preventing
    // `AddKey`, `DeployContract`, or any other actions that only the
    // account owner is permitted to do.
    *account = Some(Account::new(
        Balance::ZERO,
        Balance::ZERO,
        AccountContract::None,
        storage_usage_config.num_bytes_account,
    ));
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
