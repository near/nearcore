use crate::access_keys::initial_nonce_value;
use crate::deterministic_account_id::settle_state_init_deposit;
use crate::global_contracts::use_global_contract;
use crate::{ActionResult, ApplyState};
use near_parameters::RuntimeFeesConfig;
use near_primitives::account::{AccessKey, Account, AccountContract};
use near_primitives::action::UniversalStateInitAction;
use near_primitives::errors::{IntegerOverflowError, RuntimeError};
use near_primitives::receipt::Receipt;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, Balance, BlockHeight};
use near_primitives::universal_state_init::UniversalStateInit;
use near_store::{TrieUpdate, set_access_key_by_handle};

/// Create the `0u` universal account described by `action.state_init`. The
/// receiver id has already been checked to equal the state init's derived id
/// during action validation, so this only installs the state and settles the
/// attached deposit against storage staking.
pub(crate) fn action_universal_state_init(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    maybe_account: &mut Option<Account>,
    account_id: &AccountId,
    receipt: &Receipt,
    action: &UniversalStateInitAction,
    result: &mut ActionResult,
) -> Result<(), RuntimeError> {
    let fees = &apply_state.config.fees;
    let storage_usage_config = &fees.storage_usage_config;

    // A `0u` account can only come into existence through this action, so
    // an account that already exists here is the one this action initialized.
    // Initialize on first sight; on repeat, skip straight to the deposit
    // handling without touching the installed state.
    let needs_init = maybe_account.is_none();
    let account = match maybe_account {
        Some(account) => account,
        // Create without changing actor_id, so a same-receipt follow-up can't hijack the account.
        None => maybe_account.insert(Account::new(
            Balance::ZERO,
            Balance::ZERO,
            AccountContract::None,
            storage_usage_config.num_bytes_account,
        )),
    };

    if needs_init {
        install_universal_account(
            state_update,
            account,
            account_id,
            &action.state_init,
            result,
            fees,
            apply_state.block_height,
        )?;
        if result.result.is_err() {
            return Ok(());
        }
    }

    settle_state_init_deposit(
        account,
        action.deposit,
        account_id,
        receipt,
        &apply_state.config,
        result,
    )
}

/// Install a universal account's state on a freshly created account: optional
/// contract code, storage entries, and full-access keys. Storage usage is
/// accumulated as each piece is written.
///
/// Pre-condition: the account must not already carry contract data (holds for a
/// newly created account), so overwrites don't need to be netted out.
fn install_universal_account(
    state_update: &mut TrieUpdate,
    account: &mut Account,
    account_id: &AccountId,
    state_init: &UniversalStateInit,
    result: &mut ActionResult,
    fees: &RuntimeFeesConfig,
    block_height: BlockHeight,
) -> Result<(), RuntimeError> {
    let storage_usage_config = &fees.storage_usage_config;

    // Step 1: contract code (absent for a key-only account). This also accounts
    // for the code's storage usage on the account.
    if let Some(code) = state_init.code() {
        use_global_contract(state_update, account_id, account, code, result)?;
        if result.result.is_err() {
            return Ok(());
        }
    }

    // Step 2: storage entries.
    let mut required_storage_usage = account.storage_usage();
    for (key, value) in state_init.data() {
        let trie_key = TrieKey::ContractData { account_id: account_id.clone(), key: key.to_vec() };
        let new_bytes = (key.len() as u64)
            .checked_add(value.len() as u64)
            .and_then(|acc| acc.checked_add(storage_usage_config.num_extra_bytes_record))
            .ok_or(IntegerOverflowError {})?;
        state_update.set(trie_key, value.clone());
        required_storage_usage =
            required_storage_usage.checked_add(new_bytes).ok_or(IntegerOverflowError {})?;
    }

    // Step 3: full-access keys, stored directly as their on-trie handles (an
    // ML-DSA-65 handle is the pubkey hash, so no full pubkey is needed here).
    let nonce = initial_nonce_value(block_height);
    for handle in state_init.access_keys() {
        let mut access_key = AccessKey::full_access();
        access_key.nonce = nonce;
        set_access_key_by_handle(state_update, account_id.clone(), handle.clone(), &access_key);

        // Mirror `access_key_storage_usage`: on-trie handle length + the access
        // key's borsh length + the per-record overhead.
        let key_bytes = (handle.trie_id_len() as u64)
            .checked_add(borsh::object_length(&access_key).expect("borsh must not fail") as u64)
            .and_then(|acc| acc.checked_add(storage_usage_config.num_extra_bytes_record))
            .ok_or(IntegerOverflowError {})?;
        required_storage_usage =
            required_storage_usage.checked_add(key_bytes).ok_or(IntegerOverflowError {})?;
    }

    account.set_storage_usage(required_storage_usage);
    Ok(())
}
