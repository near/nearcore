use crate::access_keys::initial_nonce_value;
use crate::actions::OrInconsistentState;
use crate::deterministic_account_id::settle_state_init_deposit;
use crate::global_contracts::use_global_contract;
use crate::{ActionResult, ApplyState};
use near_parameters::RuntimeFeesConfig;
use near_primitives::account::{AccessKey, Account};
use near_primitives::action::UniversalStateInitAction;
use near_primitives::errors::{ActionErrorKind, IntegerOverflowError, RuntimeError};
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

    // The account may already exist without its state: a transfer to a `0u` id
    // creates an uninitialized account. Install on the first state init and skip
    // straight to the deposit handling on a repeat, without touching the state
    // already there.
    //
    // A half-installed account can never be observed here: a failed action
    // rolls the whole state update back (see `runtime::apply_action_receipt`),
    // so an initialized account always means a completed install.
    let account = match maybe_account {
        Some(account) => account,
        // Create without changing actor_id, so a same-receipt follow-up can't hijack the account.
        None => maybe_account.insert(Account::new_uninitialized(
            Balance::ZERO,
            storage_usage_config.num_bytes_account,
            initial_nonce_value(apply_state.block_height),
        )),
    };

    if !account.is_initialized() {
        // The action carries the bytes the producer serialized; installing the
        // state needs them decoded. Every receipt is validated before its actions
        // run and validation rejects a state init that does not decode, so this
        // only fires if that invariant has been broken. Failing the action rather
        // than the chunk keeps a hypothetical gap in that coverage from becoming a
        // halt, since the payload comes from outside.
        let Ok(state_init) = UniversalStateInit::from_raw(&action.state_init) else {
            result.result = Err(ActionErrorKind::MalformedUniversalStateInit.into());
            return Ok(());
        };
        account.initialize().or_inconsistent_state(account_id)?;
        install_universal_account(
            state_update,
            account,
            account_id,
            &state_init,
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

    // Step 2: storage entries. Size each record before writing it, so an
    // overflow bails out before touching the trie.
    let extra_bytes = storage_usage_config.num_extra_bytes_record;
    let mut required_storage_usage = account.storage_usage();
    for (key, value) in state_init.data() {
        let new_bytes = record_storage_usage(key.len() as u64, value.len() as u64, extra_bytes)?;
        required_storage_usage =
            required_storage_usage.checked_add(new_bytes).ok_or(IntegerOverflowError {})?;

        let trie_key = TrieKey::ContractData { account_id: account_id.clone(), key: key.to_vec() };
        state_update.set(trie_key, value.clone());
    }

    // Step 3: full-access keys, stored directly as their on-trie handles (an
    // ML-DSA-65 handle is the pubkey hash, so no full pubkey is needed here).
    // Every key is the same full-access value, so size it once.
    let mut access_key = AccessKey::full_access();
    access_key.nonce = initial_nonce_value(block_height);
    let access_key_bytes = borsh::object_length(&access_key).expect("borsh must not fail") as u64;
    for handle in state_init.access_keys() {
        // Mirror `access_key_storage_usage`: on-trie handle length + the access
        // key's borsh length + the per-record overhead.
        let key_bytes =
            record_storage_usage(handle.trie_id_len() as u64, access_key_bytes, extra_bytes)?;
        required_storage_usage =
            required_storage_usage.checked_add(key_bytes).ok_or(IntegerOverflowError {})?;

        set_access_key_by_handle(state_update, account_id.clone(), handle.clone(), &access_key);
    }

    account.set_storage_usage(required_storage_usage);
    Ok(())
}

/// Storage usage of a single trie record: key and value lengths plus the
/// per-record overhead.
fn record_storage_usage(
    key_bytes: u64,
    value_bytes: u64,
    extra_bytes: u64,
) -> Result<u64, IntegerOverflowError> {
    key_bytes
        .checked_add(value_bytes)
        .and_then(|acc| acc.checked_add(extra_bytes))
        .ok_or(IntegerOverflowError {})
}
