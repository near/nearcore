use borsh::{BorshDeserialize, BorshSerialize};

use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, AccessKeyPermission, Account};
use near_primitives::checked_feature;
use near_primitives::contract::ContractCode;
use near_primitives::errors::{
    ActionError, ActionErrorKind, ContractCallError, ExternalError, RuntimeError,
};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::ReceiptEnum;
use near_primitives::receipt::{ActionReceipt, Receipt};
use near_primitives::runtime::config::AccountCreationConfig;
use near_primitives::runtime::fees::{transfer_exec_fee, transfer_send_fee, RuntimeFeesConfig};
use near_primitives::transaction::{
    Action, AddKeyAction, DeleteAccountAction, DeleteKeyAction, DeployContractAction,
    FunctionCallAction, StakeAction, TransferAction,
};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, EpochInfoProvider};
use near_primitives::utils::create_random_seed;
use near_primitives::version::{
    is_implicit_account_creation_enabled, ProtocolFeature, ProtocolVersion,
    DELETE_KEY_STORAGE_USAGE_PROTOCOL_VERSION,
};
use near_runtime_utils::{
    is_account_evm, is_account_id_64_len_hex, is_valid_account_id, is_valid_sub_account_id,
    is_valid_top_level_account_id,
};
use near_store::{
    get_access_key, get_code, remove_access_key, remove_account, set_access_key, set_code,
    StorageError, TrieUpdate,
};
use near_vm_errors::{
    CacheError, CompilationError, FunctionCallError, InconsistentStateError, VMError,
};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{VMContext, VMOutcome};

use crate::config::{safe_add_gas, RuntimeConfig};
use crate::ext::RuntimeExt;
use crate::{ActionResult, ApplyState};
use near_vm_runner::precompile_contract;

/// Runs given function call with given context / apply state.
/// Precompiles:
///  - 0x1: EVM interpreter;
pub(crate) fn execute_function_call(
    apply_state: &ApplyState,
    runtime_ext: &mut RuntimeExt,
    account: &mut Account,
    predecessor_id: &AccountId,
    action_receipt: &ActionReceipt,
    promise_results: &[PromiseResult],
    function_call: &FunctionCallAction,
    action_hash: &CryptoHash,
    config: &RuntimeConfig,
    is_last_action: bool,
    is_view: bool,
) -> (Option<VMOutcome>, Option<VMError>) {
    let account_id = runtime_ext.account_id();
    if checked_feature!("protocol_feature_evm", EVM, runtime_ext.protocol_version())
        && is_account_evm(&account_id)
    {
        #[cfg(not(feature = "protocol_feature_evm"))]
        unreachable!();
        #[cfg(feature = "protocol_feature_evm")]
        near_evm_runner::run_evm(
            runtime_ext,
            apply_state.evm_chain_id,
            &config.wasm_config,
            &config.transaction_costs,
            &account_id,
            &action_receipt.signer_id,
            predecessor_id,
            account.amount(),
            function_call.deposit,
            account.storage_usage(),
            function_call.method_name.clone(),
            function_call.args.clone(),
            function_call.gas,
            is_view,
        )
    } else {
        let code = match runtime_ext.get_code(account.code_hash()) {
            Ok(Some(code)) => code,
            Ok(None) => {
                let error =
                    FunctionCallError::CompilationError(CompilationError::CodeDoesNotExist {
                        account_id: account_id.clone(),
                    });
                return (None, Some(VMError::FunctionCallError(error)));
            }
            Err(e) => {
                return (
                    None,
                    Some(VMError::InconsistentStateError(InconsistentStateError::StorageError(
                        e.to_string(),
                    ))),
                );
            }
        };
        // Output data receipts are ignored if the function call is not the last action in the batch.
        let output_data_receivers: Vec<_> = if is_last_action {
            action_receipt.output_data_receivers.iter().map(|r| r.receiver_id.clone()).collect()
        } else {
            vec![]
        };
        let random_seed = create_random_seed(
            apply_state.current_protocol_version,
            *action_hash,
            apply_state.random_seed,
        );
        let context = VMContext {
            current_account_id: runtime_ext.account_id().clone(),
            signer_account_id: action_receipt.signer_id.clone(),
            signer_account_pk: action_receipt
                .signer_public_key
                .try_to_vec()
                .expect("Failed to serialize"),
            predecessor_account_id: predecessor_id.clone(),
            input: function_call.args.clone(),
            block_index: apply_state.block_index,
            block_timestamp: apply_state.block_timestamp,
            epoch_height: apply_state.epoch_height,
            account_balance: account.amount(),
            account_locked_balance: account.locked(),
            storage_usage: account.storage_usage(),
            attached_deposit: function_call.deposit,
            prepaid_gas: function_call.gas,
            random_seed,
            is_view,
            output_data_receivers,
        };

        near_vm_runner::run(
            &code,
            &function_call.method_name,
            runtime_ext,
            context,
            &config.wasm_config,
            &config.transaction_costs,
            promise_results,
            apply_state.current_protocol_version,
            apply_state.cache.as_deref(),
            &apply_state.profile,
        )
    }
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
    if account.amount().checked_add(function_call.deposit).is_none() {
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
        &apply_state.prev_block_hash,
        &apply_state.block_hash,
        epoch_info_provider,
        apply_state.current_protocol_version,
    );
    let (outcome, err) = execute_function_call(
        apply_state,
        &mut runtime_ext,
        account,
        &receipt.predecessor_id,
        action_receipt,
        promise_results,
        function_call,
        action_hash,
        config,
        is_last_action,
        false,
    );
    let execution_succeeded = match err {
        Some(VMError::FunctionCallError(err)) => match err {
            FunctionCallError::Nondeterministic(msg) => {
                panic!("Contract runner returned non-deterministic error '{}', aborting", msg)
            }
            FunctionCallError::WasmUnknownError { debug_message } => {
                panic!("Wasmer returned unknown message: {}", debug_message)
            }
            FunctionCallError::CompilationError(err) => {
                result.result = Err(ActionErrorKind::FunctionCallError(
                    ContractCallError::CompilationError(err).into(),
                )
                .into());
                false
            }
            FunctionCallError::LinkError { msg } => {
                result.result = Err(ActionErrorKind::FunctionCallError(
                    ContractCallError::ExecutionError { msg: format!("Link Error: {}", msg) }
                        .into(),
                )
                .into());
                false
            }
            FunctionCallError::MethodResolveError(err) => {
                result.result = Err(ActionErrorKind::FunctionCallError(
                    ContractCallError::MethodResolveError(err).into(),
                )
                .into());
                false
            }
            FunctionCallError::WasmTrap(_)
            | FunctionCallError::HostError(_)
            | FunctionCallError::EvmError(_) => {
                result.result = Err(ActionErrorKind::FunctionCallError(
                    ContractCallError::ExecutionError { msg: err.to_string() }.into(),
                )
                .into());
                false
            }
        },
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
        Some(VMError::CacheError(err)) => {
            let message = match err {
                CacheError::DeserializationError => "Cache deserialization error",
                CacheError::SerializationError { hash: _hash } => "Cache serialization error",
                CacheError::ReadError => "Cache read error",
                CacheError::WriteError => "Cache write error",
            };
            return Err(StorageError::StorageInconsistentState(message.to_string()).into());
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
            account.set_amount(outcome.balance);
            account.set_storage_usage(outcome.storage_usage);
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
    let increment = stake.stake.saturating_sub(account.locked());

    if account.amount() >= increment {
        if account.locked() == 0 && stake.stake == 0 {
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

        result.validator_proposals.push(ValidatorStake::new(
            account_id.clone(),
            stake.public_key.clone(),
            stake.stake,
        ));
        if stake.stake > account.locked() {
            // We've checked above `account.amount >= increment`
            account.set_amount(account.amount() - increment);
            account.set_locked(stake.stake);
        }
    } else {
        result.result = Err(ActionErrorKind::TriesToStake {
            account_id: account_id.clone(),
            stake: stake.stake,
            locked: account.locked(),
            balance: account.amount(),
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
    account.set_amount(account.amount().checked_add(transfer.deposit).ok_or_else(|| {
        StorageError::StorageInconsistentState("Account balance integer overflow".to_string())
    })?);
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
    *account = Some(Account::new(
        0,
        0,
        CryptoHash::default(),
        fee_config.storage_usage_config.num_bytes_account,
    ));
}

pub(crate) fn action_implicit_account_creation_transfer(
    state_update: &mut TrieUpdate,
    fee_config: &RuntimeFeesConfig,
    account: &mut Option<Account>,
    actor_id: &mut AccountId,
    account_id: &AccountId,
    transfer: &TransferAction,
) {
    // NOTE: The account_id is hex like, because we've checked the permissions before.
    debug_assert!(is_account_id_64_len_hex(account_id));

    *actor_id = account_id.clone();

    let access_key = AccessKey::full_access();
    // 0 for ED25519
    let mut public_key_data = Vec::with_capacity(33);
    public_key_data.push(0u8);
    public_key_data.extend(
        hex::decode(account_id.as_bytes())
            .expect("account id was a valid hex of length 64 resulting in 32 bytes"),
    );
    debug_assert_eq!(public_key_data.len(), 33);
    let public_key = PublicKey::try_from_slice(&public_key_data)
        .expect("we should be able to deserialize ED25519 public key");

    *account = Some(Account::new(
        transfer.deposit,
        0,
        CryptoHash::default(),
        fee_config.storage_usage_config.num_bytes_account
            + public_key.len() as u64
            + access_key.try_to_vec().unwrap().len() as u64
            + fee_config.storage_usage_config.num_extra_bytes_record,
    ));

    set_access_key(state_update, account_id.clone(), public_key, &access_key);
}

pub(crate) fn action_deploy_contract(
    state_update: &mut TrieUpdate,
    account: &mut Account,
    account_id: &AccountId,
    deploy_contract: &DeployContractAction,
    apply_state: &ApplyState,
) -> Result<(), StorageError> {
    let code = ContractCode::new(deploy_contract.code.clone(), None);
    let prev_code = get_code(state_update, account_id, Some(account.code_hash()))?;
    let prev_code_length = prev_code.map(|code| code.code.len() as u64).unwrap_or_default();
    account.set_storage_usage(account.storage_usage().checked_sub(prev_code_length).unwrap_or(0));
    account.set_storage_usage(
        account.storage_usage().checked_add(code.code.len() as u64).ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "Storage usage integer overflow for account {}",
                account_id
            ))
        })?,
    );
    account.set_code_hash(code.get_hash());
    set_code(state_update, account_id.clone(), &code);
    // Precompile the contract and store result (compiled code or error) in the database.
    if false {
        let _ = precompile_contract(
            &code,
            &apply_state.config.wasm_config,
            apply_state.cache.as_deref(),
        );
    }
    Ok(())
}

pub(crate) fn action_delete_account(
    state_update: &mut TrieUpdate,
    account: &mut Option<Account>,
    actor_id: &mut AccountId,
    receipt: &Receipt,
    action_receipt: &ActionReceipt,
    result: &mut ActionResult,
    account_id: &AccountId,
    delete_account: &DeleteAccountAction,
    current_protocol_version: ProtocolVersion,
    config: &RuntimeFeesConfig,
) -> Result<(), StorageError> {
    if current_protocol_version >= ProtocolFeature::DeleteActionRestriction.protocol_version() {
        let account = account.as_ref().unwrap();
        let mut account_storage_usage = account.storage_usage();
        let contract_code = get_code(state_update, account_id, Some(account.code_hash()))?;
        if let Some(code) = contract_code {
            // account storage usage should be larger than code size
            let code_len = code.code.len() as u64;
            debug_assert!(account_storage_usage > code_len);
            account_storage_usage = account_storage_usage.saturating_sub(code_len);
        }
        if account_storage_usage > Account::MAX_ACCOUNT_DELETION_STORAGE_USAGE {
            result.result = Err(ActionErrorKind::DeleteAccountWithLargeState {
                account_id: account_id.clone(),
            }
            .into());
            return Ok(());
        }
    }
    // We use current amount as a pay out to beneficiary.
    let account_balance = account.as_ref().unwrap().amount();
    if account_balance > 0 {
        if checked_feature!(
            "protocol_feature_allow_create_account_on_delete",
            AllowCreateAccountOnDelete,
            current_protocol_version
        ) {
            let sender_is_receiver = account_id == &delete_account.beneficiary_id;
            let is_receiver_implicit =
                is_implicit_account_creation_enabled(current_protocol_version)
                    && is_account_id_64_len_hex(&delete_account.beneficiary_id);
            let exec_gas = config.action_receipt_creation_config.send_fee(sender_is_receiver)
                + transfer_send_fee(
                    &config.action_creation_config,
                    sender_is_receiver,
                    is_receiver_implicit,
                );
            result.gas_burnt += exec_gas;
            result.gas_used += exec_gas
                + config.action_receipt_creation_config.exec_fee()
                + transfer_exec_fee(&config.action_creation_config, is_receiver_implicit);

            result.new_receipts.push(Receipt {
                predecessor_id: account_id.clone(),
                receiver_id: delete_account.beneficiary_id.clone(),
                // Actual receipt ID is set in the Runtime.apply_action_receipt(...) in the
                // "Generating receipt IDs" section
                receipt_id: CryptoHash::default(),

                receipt: ReceiptEnum::Action(ActionReceipt {
                    signer_id: action_receipt.signer_id.clone(),
                    signer_public_key: action_receipt.signer_public_key.clone(),
                    gas_price: action_receipt.gas_price,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions: vec![Action::Transfer(TransferAction { deposit: account_balance })],
                }),
            });
        } else {
            result
                .new_receipts
                .push(Receipt::new_balance_refund(&delete_account.beneficiary_id, account_balance));
        }
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
    current_protocol_version: ProtocolVersion,
) -> Result<(), StorageError> {
    let access_key = get_access_key(state_update, &account_id, &delete_key.public_key)?;
    if let Some(access_key) = access_key {
        let storage_usage_config = &fee_config.storage_usage_config;
        let storage_usage = if current_protocol_version >= DELETE_KEY_STORAGE_USAGE_PROTOCOL_VERSION
        {
            delete_key.public_key.try_to_vec().unwrap().len() as u64
                + access_key.try_to_vec().unwrap().len() as u64
                + storage_usage_config.num_extra_bytes_record
        } else {
            delete_key.public_key.try_to_vec().unwrap().len() as u64
                + Some(access_key).try_to_vec().unwrap().len() as u64
                + storage_usage_config.num_extra_bytes_record
        };
        // Remove access key
        remove_access_key(state_update, account_id.clone(), delete_key.public_key.clone());
        account.set_storage_usage(account.storage_usage().checked_sub(storage_usage).unwrap_or(0));
    } else {
        result.result = Err(ActionErrorKind::DeleteKeyDoesNotExist {
            public_key: delete_key.public_key.clone(),
            account_id: account_id.clone(),
        }
        .into());
    }
    Ok(())
}

pub(crate) fn action_add_key(
    apply_state: &ApplyState,
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
    if checked_feature!("stable", AccessKeyNonceRange, apply_state.current_protocol_version) {
        let mut access_key = add_key.access_key.clone();
        access_key.nonce = (apply_state.block_index - 1)
            * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER;
        set_access_key(state_update, account_id.clone(), add_key.public_key.clone(), &access_key);
    } else {
        set_access_key(
            state_update,
            account_id.clone(),
            add_key.public_key.clone(),
            &add_key.access_key,
        );
    };
    let storage_config = &apply_state.config.transaction_costs.storage_usage_config;
    account.set_storage_usage(
        account
            .storage_usage()
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
            })?,
    );
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
                    account_id: account_id.clone(),
                    actor_id: actor_id.clone(),
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
            if account.locked() != 0 {
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
    current_protocol_version: ProtocolVersion,
    is_the_only_action: bool,
    is_refund: bool,
) -> Result<(), ActionError> {
    match action {
        Action::CreateAccount(_) => {
            if account.is_some() {
                return Err(ActionErrorKind::AccountAlreadyExists {
                    account_id: account_id.clone(),
                }
                .into());
            } else {
                if is_implicit_account_creation_enabled(current_protocol_version)
                    && is_account_id_64_len_hex(&account_id)
                {
                    // If the account doesn't exist and it's 64-length hex account ID, then you
                    // should only be able to create it using single transfer action.
                    // Because you should not be able to add another access key to the account in
                    // the same transaction.
                    // Otherwise you can hijack an account without having the private key for the
                    // public key. We've decided to make it an invalid transaction to have any other
                    // actions on the 64-length hex accounts.
                    // The easiest way is to reject the `CreateAccount` action.
                    // See https://github.com/nearprotocol/NEPs/pull/71
                    return Err(ActionErrorKind::OnlyImplicitAccountCreationAllowed {
                        account_id: account_id.clone(),
                    }
                    .into());
                }
            }
        }
        Action::Transfer(_) => {
            if account.is_none() {
                return if is_implicit_account_creation_enabled(current_protocol_version)
                    && is_the_only_action
                    && is_account_id_64_len_hex(&account_id)
                    && !is_refund
                {
                    // OK. It's implicit account creation.
                    // Notes:
                    // - The transfer action has to be the only action in the transaction to avoid
                    // abuse by hijacking this account with other public keys or contracts.
                    // - Refunds don't automatically create accounts, because refunds are free and
                    // we don't want some type of abuse.
                    // - Account deletion with beneficiary creates a refund, so it'll not create a
                    // new account.
                    Ok(())
                } else {
                    Err(ActionErrorKind::AccountDoesNotExist { account_id: account_id.clone() }
                        .into())
                };
            }
        }
        Action::DeployContract(_)
        | Action::FunctionCall(_)
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
    use near_primitives::hash::hash;
    use near_primitives::trie_key::TrieKey;
    use near_store::test_utils::create_tries;

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
                },
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
                },
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

    fn test_delete_large_account(
        account_id: &AccountId,
        code_hash: &CryptoHash,
        storage_usage: u64,
        state_update: &mut TrieUpdate,
    ) -> ActionResult {
        let mut account = Some(Account::new(100, 0, *code_hash, storage_usage));
        let mut actor_id = account_id.clone();
        let mut action_result = ActionResult::default();
        let receipt = Receipt::new_balance_refund(&"alice.near".to_string(), 0);
        let action_receipt = match &receipt.receipt {
            ReceiptEnum::Action(action_receipt) => action_receipt,
            _ => unreachable!("Balance refund should be an action receipt"),
        };
        let res = action_delete_account(
            state_update,
            &mut account,
            &mut actor_id,
            &receipt,
            &action_receipt,
            &mut action_result,
            account_id,
            &DeleteAccountAction { beneficiary_id: "bob".to_string() },
            ProtocolFeature::DeleteActionRestriction.protocol_version(),
            &RuntimeFeesConfig::default(),
        );
        assert!(res.is_ok());
        action_result
    }

    #[test]
    fn test_delete_account_too_large() {
        let tries = create_tries();
        let mut state_update = tries.new_trie_update(0, CryptoHash::default());
        let action_result = test_delete_large_account(
            &"alice".to_string(),
            &CryptoHash::default(),
            Account::MAX_ACCOUNT_DELETION_STORAGE_USAGE + 1,
            &mut state_update,
        );
        assert_eq!(
            action_result.result,
            Err(ActionError {
                index: None,
                kind: ActionErrorKind::DeleteAccountWithLargeState {
                    account_id: "alice".to_string()
                }
            })
        )
    }

    fn test_delete_account_with_contract(storage_usage: u64) -> ActionResult {
        let tries = create_tries();
        let mut state_update = tries.new_trie_update(0, CryptoHash::default());
        let account_id = "alice".to_string();
        let trie_key = TrieKey::ContractCode { account_id: account_id.clone() };
        let empty_contract = [0; 10_000].to_vec();
        let contract_hash = hash(&empty_contract);
        state_update.set(trie_key, empty_contract);
        test_delete_large_account(&account_id, &contract_hash, storage_usage, &mut state_update)
    }

    #[test]
    fn test_delete_account_with_contract_and_small_state() {
        let action_result =
            test_delete_account_with_contract(Account::MAX_ACCOUNT_DELETION_STORAGE_USAGE + 100);
        assert!(action_result.result.is_ok());
    }

    #[test]
    fn test_delete_account_with_contract_and_large_state() {
        let action_result =
            test_delete_account_with_contract(10 * Account::MAX_ACCOUNT_DELETION_STORAGE_USAGE);
        assert_eq!(
            action_result.result,
            Err(ActionError {
                index: None,
                kind: ActionErrorKind::DeleteAccountWithLargeState {
                    account_id: "alice".to_string()
                }
            })
        );
    }
}
