use crate::access_keys::initial_nonce_value;
use crate::config::{
    safe_add_compute, total_prepaid_exec_fees, total_prepaid_gas, total_prepaid_send_fees,
};
use crate::deterministic_account_id::create_deterministic_account;
use crate::{ActionResult, ApplyState};
use near_crypto::PublicKey;
use near_parameters::{AccountCreationConfig, ActionCosts, RuntimeConfig, RuntimeFeesConfig};
use near_primitives::account::{AccessKey, AccessKeyPermission, Account, AccountContract};
use near_primitives::action::delegate::{DelegateAction, SignedDelegateAction};
use near_primitives::errors::{ActionError, ActionErrorKind, InvalidAccessKeyError, RuntimeError};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ActionReceipt, Receipt, ReceiptEnum, ReceiptPriority, ReceiptV0, VersionedActionReceipt,
    VersionedReceiptEnum,
};
use near_primitives::transaction::{
    Action, DeleteAccountAction, DeployContractAction, StakeAction,
};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, EpochInfoProvider, Gas, StorageUsage,
};
use near_primitives::utils::account_is_implicit;
use near_primitives::version::ProtocolVersion;
use near_primitives_core::account::id::AccountType;
use near_primitives_core::version::ProtocolFeature;
use near_store::trie::AccessOptions;
use near_store::{
    StorageError, TrieAccess, TrieUpdate, get_access_key, remove_account, set_access_key,
};
use near_vm_runner::precompile_contract;
use near_vm_runner::{ContractCode, ContractRuntimeCache};
use near_wallet_contract::{wallet_contract, wallet_contract_magic_bytes};
use std::sync::Arc;

pub(crate) fn action_stake(
    account: &mut Account,
    result: &mut ActionResult,
    account_id: &AccountId,
    stake: &StakeAction,
    last_block_hash: &CryptoHash,
    epoch_info_provider: &dyn EpochInfoProvider,
) -> Result<(), RuntimeError> {
    let increment = stake.stake.saturating_sub(account.locked());

    if let Some(new_balance) = account.amount().checked_sub(increment) {
        if account.locked().is_zero() && stake.stake.is_zero() {
            // if the account hasn't staked, it cannot unstake
            result.result =
                Err(ActionErrorKind::TriesToUnstake { account_id: account_id.clone() }.into());
            return Ok(());
        }

        if stake.stake > Balance::ZERO {
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
            account.set_amount(new_balance);
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
    deposit: Balance,
) -> Result<(), StorageError> {
    if let Some(mut access_key) = get_access_key(state_update, account_id, public_key)? {
        let mut updated = false;
        if let AccessKeyPermission::FunctionCall(function_call_permission) =
            &mut access_key.permission
        {
            if let Some(allowance) = function_call_permission.allowance.as_mut() {
                let new_allowance = allowance.saturating_add(deposit);
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

pub(crate) fn action_transfer(account: &mut Account, deposit: Balance) -> Result<(), StorageError> {
    account.set_amount(account.amount().checked_add(deposit).ok_or_else(|| {
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
    if account_id.is_top_level() {
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
    } else if !account_id.is_sub_account_of(predecessor_id) {
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
        Balance::ZERO,
        Balance::ZERO,
        AccountContract::None,
        fee_config.storage_usage_config.num_bytes_account,
    ));
}

/// Can only be used for implicit accounts.
pub(crate) fn action_implicit_account_creation_transfer(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    fee_config: &RuntimeFeesConfig,
    account: &mut Option<Account>,
    actor_id: &mut AccountId,
    account_id: &AccountId,
    deposit: Balance,
    block_height: BlockHeight,
    epoch_info_provider: &dyn EpochInfoProvider,
) {
    *actor_id = account_id.clone();
    match account_id.get_account_type() {
        AccountType::NearImplicitAccount => {
            let mut access_key = AccessKey::full_access();
            access_key.nonce = initial_nonce_value(block_height);

            // unwrap: here it's safe because the `account_id` has already been determined to be implicit by `get_account_type`
            let public_key = PublicKey::from_near_implicit_account(account_id).unwrap();

            *account = Some(Account::new(
                deposit,
                Balance::ZERO,
                AccountContract::None,
                fee_config.storage_usage_config.num_bytes_account
                    + public_key.len() as u64
                    + borsh::object_length(&access_key).unwrap() as u64
                    + fee_config.storage_usage_config.num_extra_bytes_record,
            ));

            set_access_key(state_update, account_id.clone(), public_key, &access_key);
        }
        // Invariant: The `account_id` is implicit.
        // It holds because in the only calling site, we've checked the permissions before.
        AccountType::EthImplicitAccount => {
            let chain_id = epoch_info_provider.chain_id();

            // We deploy "near[wallet contract hash]" magic bytes as the contract code,
            // to mark that this is a neard-defined contract. It will not be used on a function call.
            // Instead, neard-defined Wallet Contract implementation will be used.
            let magic_bytes = wallet_contract_magic_bytes(&chain_id);

            let storage_usage = fee_config.storage_usage_config.num_bytes_account
                + magic_bytes.code().len() as u64
                + fee_config.storage_usage_config.num_extra_bytes_record;

            let contract_hash = *magic_bytes.hash();
            *account = Some(Account::new(
                deposit,
                Balance::ZERO,
                AccountContract::from_local_code_hash(contract_hash),
                storage_usage,
            ));
            state_update.set_code(account_id.clone(), &magic_bytes);

            // Precompile Wallet Contract and store result (compiled code or error) in the database.
            // Note this contract is shared among ETH-implicit accounts and `precompile_contract`
            // is a no-op if the contract was already compiled.
            precompile_contract(
                &wallet_contract(contract_hash).expect("should definitely exist"),
                Arc::clone(&apply_state.config.wasm_config),
                apply_state.cache.as_deref(),
            )
            .ok();
        }
        AccountType::NearDeterministicAccount => {
            *account = Some(create_deterministic_account(
                deposit,
                &apply_state.config.fees.storage_usage_config,
            ));
        }
        // This panic is unreachable as this is an implicit account creation transfer.
        // `check_account_existence` would fail because `account_is_implicit` would return false for a Named account.
        AccountType::NamedAccount => panic!("must be implicit"),
    }
}

pub(crate) fn action_deploy_contract(
    state_update: &mut TrieUpdate,
    account: &mut Account,
    account_id: &AccountId,
    deploy_contract: &DeployContractAction,
    config: Arc<near_parameters::vm::Config>,
    cache: Option<&dyn ContractRuntimeCache>,
    current_protocol_version: ProtocolVersion,
) -> Result<(), StorageError> {
    let _span = tracing::debug_span!(target: "runtime", "action_deploy_contract").entered();
    clear_account_contract_storage_usage(
        state_update,
        account_id,
        account,
        current_protocol_version,
    )?;

    let code = ContractCode::new(deploy_contract.code.clone(), None);
    account.set_storage_usage(
        account.storage_usage().checked_add(code.code().len() as u64).ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "Storage usage integer overflow for account {}",
                account_id
            ))
        })?,
    );
    account.set_contract(AccountContract::Local(*code.hash()));
    // Legacy: populate the mapping from `AccountId => sha256(code)` thus making contracts part of
    // The State. For the time being we are also relying on the `TrieUpdate` to actually write the
    // contracts into the storage as part of the commit routine, however no code should be relying
    // that the contracts are written to The State.
    state_update.set_code(account_id.clone(), &code);
    // Precompile the contract and store result (compiled code or error) in the contract runtime
    // cache.
    // Note, that contract compilation costs are already accounted in deploy cost using special
    // logic in estimator (see get_runtime_config() function).
    precompile_contract(&code, config, cache).ok();
    // Inform the `store::contract::Storage` about the new deploy (so that the `get` method can
    // return the contract before the contract is written out to the underlying storage as part of
    // the `TrieUpdate` commit.)
    state_update.record_contract_deploy(code);
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
    current_protocol_version: ProtocolVersion,
) -> Result<(), StorageError> {
    let account_ref = account.as_ref().unwrap();
    let mut account_storage_usage = account_ref.storage_usage();
    let code_len = get_code_len_or_default(
        state_update,
        account_id.clone(),
        account_ref.local_contract_hash().unwrap_or_default(),
        current_protocol_version,
    )?;
    debug_assert!(
        code_len == 0 || account_storage_usage > code_len,
        "Account storage usage should be larger than code size. Storage usage: {}, code size: {}",
        account_storage_usage,
        code_len
    );
    account_storage_usage = account_storage_usage.saturating_sub(code_len);
    if account_storage_usage > Account::MAX_ACCOUNT_DELETION_STORAGE_USAGE {
        result.result =
            Err(ActionErrorKind::DeleteAccountWithLargeState { account_id: account_id.clone() }
                .into());
        return Ok(());
    }
    // We use current amount as a pay out to beneficiary.
    let account_balance = account_ref.amount();
    if account_balance > Balance::ZERO {
        result.new_receipts.push(Receipt::new_balance_refund(
            &delete_account.beneficiary_id,
            account_balance,
            ReceiptPriority::NoPriority,
        ));
    }
    remove_account(state_update, account_id)?;
    *actor_id = receipt.predecessor_id().clone();
    *account = None;
    Ok(())
}

/// Returns the storage usage for the contract code with the given `code_hash` and deployed to the
/// given `account_id`. If no contract was deployed to the account, returns `0`.
///
/// This implements different behaviors based on the protocol version:
/// If `ExcludeExistingCodeFromWitnessForCodeLen` is enabled then the code-length is obtained without reading
/// the code but from the value-ref in the trie leaf node, otherwise it reads the code and returns its size.
fn get_code_len_or_default(
    state_update: &TrieUpdate,
    account_id: AccountId,
    code_hash: CryptoHash,
    protocol_version: ProtocolVersion,
) -> Result<StorageUsage, StorageError> {
    let code_len =
        if ProtocolFeature::ExcludeExistingCodeFromWitnessForCodeLen.enabled(protocol_version) {
            state_update.get_code_len(account_id, code_hash)?
        } else {
            let key = near_primitives::trie_key::TrieKey::ContractCode { account_id };
            state_update.get(&key, AccessOptions::DEFAULT)?.map(|code| code.len())
        };
    debug_assert!(
        code_len.is_some() || code_hash == CryptoHash::default(),
        "Non-default code hash for account with no contract deployed: {:?}",
        code_hash
    );
    Ok(code_len.unwrap_or_default().try_into().unwrap())
}

/// Clears the contract storage usage based on type for an account.
pub(crate) fn clear_account_contract_storage_usage(
    state_update: &TrieUpdate,
    account_id: &AccountId,
    account: &mut Account,
    current_protocol_version: ProtocolVersion,
) -> Result<(), StorageError> {
    match account.contract().as_ref() {
        AccountContract::None => {}
        AccountContract::Local(code_hash) => {
            let prev_code_len = get_code_len_or_default(
                state_update,
                account_id.clone(),
                *code_hash,
                current_protocol_version,
            )?;
            account.set_storage_usage(account.storage_usage().saturating_sub(prev_code_len));
        }
        AccountContract::Global(_) | AccountContract::GlobalByAccount(_) => {
            account.set_storage_usage(
                account
                    .storage_usage()
                    .saturating_sub(account.contract().identifier_storage_usage()),
            );
        }
    };
    Ok(())
}

pub(crate) fn apply_delegate_action(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    action_receipt: &VersionedActionReceipt,
    sender_id: &AccountId,
    signed_delegate_action: &SignedDelegateAction,
    result: &mut ActionResult,
    _priority: ReceiptPriority,
) -> Result<(), RuntimeError> {
    let delegate_action = &signed_delegate_action.delegate_action;

    if !signed_delegate_action.verify() {
        result.result = Err(ActionErrorKind::DelegateActionInvalidSignature.into());
        return Ok(());
    }
    if apply_state.block_height > delegate_action.max_block_height {
        result.result = Err(ActionErrorKind::DelegateActionExpired.into());
        return Ok(());
    }
    if delegate_action.sender_id.as_str() != sender_id.as_str() {
        result.result = Err(ActionErrorKind::DelegateActionSenderDoesNotMatchTxReceiver {
            sender_id: delegate_action.sender_id.clone(),
            receiver_id: sender_id.clone(),
        }
        .into());
        return Ok(());
    }

    validate_delegate_action_key(state_update, apply_state, delegate_action, result)?;
    if result.result.is_err() {
        // Validation failed. Need to return Ok() because this is not a runtime error.
        // "result.result" will be return to the User as the action execution result.
        return Ok(());
    }

    // Generate a new receipt from DelegateAction.
    let new_receipt = Receipt::V0(ReceiptV0 {
        predecessor_id: sender_id.clone(),
        receiver_id: delegate_action.receiver_id.clone(),
        receipt_id: CryptoHash::default(),

        receipt: ReceiptEnum::Action(ActionReceipt {
            signer_id: action_receipt.signer_id().clone(),
            signer_public_key: action_receipt.signer_public_key().clone(),
            gas_price: action_receipt.gas_price(),
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions: delegate_action.get_actions(),
        }),
    });

    // Note, Relayer prepaid all fees and all things required by actions: attached deposits and attached gas.
    // If something goes wrong, deposit is refunded to the predecessor, this is sender_id/Sender in DelegateAction.
    // Gas is refunded to the signer, this is Relayer.
    // Some contracts refund the deposit. Usually they refund the deposit to the predecessor and this is sender_id/Sender from DelegateAction.
    // Therefore Relayer should verify DelegateAction before submitting it because it spends the attached deposit.

    let prepaid_send_fees = total_prepaid_send_fees(&apply_state.config, action_receipt.actions())?;
    let required_gas = receipt_required_gas(apply_state, &new_receipt)?;
    // This gas will be burnt by the receiver of the created receipt,
    result.gas_used = result.gas_used.checked_add_result(required_gas)?;
    // This gas was prepaid on Relayer shard. Need to burn it because the receipt is going to be sent.
    // gas_used is incremented because otherwise the gas will be refunded. Refund function checks only gas_used.
    result.gas_used = result.gas_used.checked_add_result(prepaid_send_fees)?;
    result.gas_burnt = result.gas_burnt.checked_add_result(prepaid_send_fees)?;
    // TODO(#8806): Support compute costs for actions. For now they match burnt gas.
    result.compute_usage = safe_add_compute(result.compute_usage, prepaid_send_fees.as_gas())?;
    result.new_receipts.push(new_receipt);

    Ok(())
}

/// Returns Gas amount is required to execute Receipt and all actions it contains
fn receipt_required_gas(apply_state: &ApplyState, receipt: &Receipt) -> Result<Gas, RuntimeError> {
    Ok(match receipt.versioned_receipt() {
        VersionedReceiptEnum::Action(action_receipt)
        | VersionedReceiptEnum::PromiseYield(action_receipt) => {
            action_receipt_required_gas(apply_state, receipt, action_receipt.into())?
        }
        VersionedReceiptEnum::GlobalContractDistribution(_)
        | VersionedReceiptEnum::Data(_)
        | VersionedReceiptEnum::PromiseResume(_) => Gas::ZERO,
    })
}

fn action_receipt_required_gas(
    apply_state: &ApplyState,
    receipt: &Receipt,
    action_receipt: VersionedActionReceipt,
) -> Result<Gas, RuntimeError> {
    let mut required_gas = total_prepaid_exec_fees(
        &apply_state.config,
        &action_receipt.actions(),
        receipt.receiver_id(),
    )?
    .checked_add_result(total_prepaid_gas(&action_receipt.actions())?)?;
    required_gas = required_gas.checked_add_result(
        apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee(),
    )?;
    Ok(required_gas)
}

/// Validate access key which was used for signing DelegateAction:
///
/// - Checks whether the access key is present fo given public_key and sender_id.
/// - Validates nonce and updates it if it's ok.
/// - Validates access key permissions.
fn validate_delegate_action_key(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    delegate_action: &DelegateAction,
    result: &mut ActionResult,
) -> Result<(), RuntimeError> {
    // 'delegate_action.sender_id' account existence must be checked by a caller
    let mut access_key = match get_access_key(
        state_update,
        &delegate_action.sender_id,
        &delegate_action.public_key,
    )? {
        Some(access_key) => access_key,
        None => {
            result.result = Err(ActionErrorKind::DelegateActionAccessKeyError(
                InvalidAccessKeyError::AccessKeyNotFound {
                    account_id: delegate_action.sender_id.clone(),
                    public_key: delegate_action.public_key.clone().into(),
                },
            )
            .into());
            return Ok(());
        }
    };

    if delegate_action.nonce <= access_key.nonce {
        result.result = Err(ActionErrorKind::DelegateActionInvalidNonce {
            delegate_nonce: delegate_action.nonce,
            ak_nonce: access_key.nonce,
        }
        .into());
        return Ok(());
    }

    let upper_bound = apply_state.block_height
        * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER;
    if delegate_action.nonce >= upper_bound {
        result.result = Err(ActionErrorKind::DelegateActionNonceTooLarge {
            delegate_nonce: delegate_action.nonce,
            upper_bound,
        }
        .into());
        return Ok(());
    }

    access_key.nonce = delegate_action.nonce;

    let actions = delegate_action.get_actions();

    // The restriction of "function call" access keys:
    // the transaction must contain the only `FunctionCall` if "function call" access key is used
    if let AccessKeyPermission::FunctionCall(ref function_call_permission) = access_key.permission {
        if actions.len() != 1 {
            result.result = Err(ActionErrorKind::DelegateActionAccessKeyError(
                InvalidAccessKeyError::RequiresFullAccess,
            )
            .into());
            return Ok(());
        }
        if let Some(Action::FunctionCall(function_call)) = actions.get(0) {
            if function_call.deposit > Balance::ZERO {
                result.result = Err(ActionErrorKind::DelegateActionAccessKeyError(
                    InvalidAccessKeyError::DepositWithFunctionCall,
                )
                .into());
            }
            if delegate_action.receiver_id != function_call_permission.receiver_id {
                result.result = Err(ActionErrorKind::DelegateActionAccessKeyError(
                    InvalidAccessKeyError::ReceiverMismatch {
                        tx_receiver: delegate_action.receiver_id.clone(),
                        ak_receiver: function_call_permission.receiver_id.clone(),
                    },
                )
                .into());
                return Ok(());
            }
            if !function_call_permission.method_names.is_empty()
                && function_call_permission
                    .method_names
                    .iter()
                    .all(|method_name| &function_call.method_name != method_name)
            {
                result.result = Err(ActionErrorKind::DelegateActionAccessKeyError(
                    InvalidAccessKeyError::MethodNameMismatch {
                        method_name: function_call.method_name.clone(),
                    },
                )
                .into());
                return Ok(());
            }
        } else {
            // There should Action::FunctionCall when "function call" permission is used
            result.result = Err(ActionErrorKind::DelegateActionAccessKeyError(
                InvalidAccessKeyError::RequiresFullAccess,
            )
            .into());
            return Ok(());
        }
    };

    set_access_key(
        state_update,
        delegate_action.sender_id.clone(),
        delegate_action.public_key.clone(),
        &access_key,
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
        Action::DeployContract(_)
        | Action::Stake(_)
        | Action::AddKey(_)
        | Action::DeleteKey(_)
        | Action::DeployGlobalContract(_)
        | Action::UseGlobalContract(_)
        | Action::WithdrawFromGasKey(_) => {
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
            if !account.locked().is_zero() {
                return Err(ActionErrorKind::DeleteAccountStaking {
                    account_id: account_id.clone(),
                }
                .into());
            }
        }
        Action::CreateAccount(_)
        | Action::FunctionCall(_)
        | Action::Transfer(_)
        | Action::TransferToGasKey(_) => (),
        Action::Delegate(_) => (),
        Action::DeterministicStateInit(_) => (),
    };
    Ok(())
}

pub(crate) fn check_account_existence(
    action: &Action,
    account: &Option<Account>,
    account_id: &AccountId,
    config: &RuntimeConfig,
    implicit_account_creation_eligible: bool,
) -> Result<(), ActionError> {
    match action {
        Action::CreateAccount(_) => {
            if account.is_some() {
                return Err(ActionErrorKind::AccountAlreadyExists {
                    account_id: account_id.clone(),
                }
                .into());
            } else {
                if account_is_implicit(account_id, config.wasm_config.eth_implicit_accounts) {
                    // If the account doesn't exist and it's implicit, then you
                    // should only be able to create it using single transfer action.
                    // Because you should not be able to add another access key to the account in
                    // the same transaction.
                    // Otherwise you can hijack an account without having the private key for the
                    // public key. We've decided to make it an invalid transaction to have any other
                    // actions on the implicit hex accounts.
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
                return check_transfer_to_nonexisting_account(
                    config,
                    account_id,
                    implicit_account_creation_eligible,
                );
            }
        }
        Action::DeterministicStateInit(_) => {
            // Existing and non existing is valid for DeterministicStateInit.
            // Does not exist => The account will be created by the action.
            // Does exist => Nothing happens but the receipt is not aborted to
            // allow optional init before other actions.
        }
        Action::DeployContract(_)
        | Action::FunctionCall(_)
        | Action::Stake(_)
        | Action::AddKey(_)
        | Action::DeleteKey(_)
        | Action::DeleteAccount(_)
        | Action::Delegate(_)
        | Action::DeployGlobalContract(_)
        | Action::UseGlobalContract(_)
        | Action::TransferToGasKey(_)
        | Action::WithdrawFromGasKey(_) => {
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

fn check_transfer_to_nonexisting_account(
    config: &RuntimeConfig,
    account_id: &AccountId,
    implicit_account_creation_eligible: bool,
) -> Result<(), ActionError> {
    if implicit_account_creation_eligible
        && account_is_implicit(account_id, config.wasm_config.eth_implicit_accounts)
    {
        // OK. It's implicit account creation.
        // Notes:
        // - Transfer action has to be the only action in the transaction to avoid
        // abuse by hijacking this account with other public keys or contracts.
        // - Refunds don't automatically create accounts, because refunds are free and
        // we don't want some type of abuse.
        // - Account deletion with beneficiary creates a refund, so it'll not create a
        // new account.
        Ok(())
    } else {
        Err(ActionErrorKind::AccountDoesNotExist { account_id: account_id.clone() }.into())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::actions_test_utils::{setup_account, test_delete_large_account};
    use crate::near_primitives::shard_layout::ShardUId;
    use near_primitives::account::FunctionCallPermission;
    use near_primitives::action::FunctionCallAction;
    use near_primitives::action::delegate::NonDelegateAction;
    use near_primitives::apply::ApplyChunkReason;
    use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
    use near_primitives::congestion_info::BlockCongestionInfo;
    use near_primitives::errors::InvalidAccessKeyError;
    use near_primitives::transaction::CreateAccountAction;
    use near_primitives::types::EpochId;
    use near_primitives::types::Gas;
    use near_store::test_utils::TestTriesBuilder;
    use std::sync::Arc;

    fn test_action_create_account(
        account_id: AccountId,
        predecessor_id: AccountId,
        length: u8,
    ) -> ActionResult {
        let mut account = None;
        let mut actor_id = predecessor_id.clone();
        let mut action_result = ActionResult::default();
        action_create_account(
            &RuntimeFeesConfig::test(),
            &AccountCreationConfig {
                min_allowed_top_level_account_length: length,
                registrar_account_id: "registrar".parse().unwrap(),
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
        let account_id = "bob_near_long_name".parse().unwrap();
        let predecessor_id = "alice.near".parse().unwrap();
        let action_result = test_action_create_account(account_id, predecessor_id, 11);
        assert!(action_result.result.is_ok());
    }

    #[test]
    fn test_create_account_valid_top_level_by_registrar() {
        let account_id = "bob".parse().unwrap();
        let predecessor_id = "registrar".parse().unwrap();
        let action_result = test_action_create_account(account_id, predecessor_id, 11);
        assert!(action_result.result.is_ok());
    }

    #[test]
    fn test_create_account_valid_sub_account() {
        let account_id = "alice.near".parse().unwrap();
        let predecessor_id = "near".parse().unwrap();
        let action_result = test_action_create_account(account_id, predecessor_id, 11);
        assert!(action_result.result.is_ok());
    }

    #[test]
    fn test_create_account_invalid_sub_account() {
        let account_id = "alice.near".parse::<AccountId>().unwrap();
        let predecessor_id = "bob".parse::<AccountId>().unwrap();
        let action_result =
            test_action_create_account(account_id.clone(), predecessor_id.clone(), 11);
        assert_eq!(
            action_result.result,
            Err(ActionError {
                index: None,
                kind: ActionErrorKind::CreateAccountNotAllowed {
                    account_id: account_id,
                    predecessor_id: predecessor_id,
                },
            })
        );
    }

    #[test]
    fn test_create_account_invalid_short_top_level() {
        let account_id = "bob".parse::<AccountId>().unwrap();
        let predecessor_id = "near".parse::<AccountId>().unwrap();
        let action_result =
            test_action_create_account(account_id.clone(), predecessor_id.clone(), 11);
        assert_eq!(
            action_result.result,
            Err(ActionError {
                index: None,
                kind: ActionErrorKind::CreateAccountOnlyByRegistrar {
                    account_id: account_id,
                    registrar_account_id: "registrar".parse().unwrap(),
                    predecessor_id: predecessor_id,
                },
            })
        );
    }

    #[test]
    fn test_create_account_valid_short_top_level_len_allowed() {
        let account_id = "bob".parse().unwrap();
        let predecessor_id = "near".parse().unwrap();
        let action_result = test_action_create_account(account_id, predecessor_id, 0);
        assert!(action_result.result.is_ok());
    }

    #[test]
    fn test_delete_account_too_large() {
        let tries = TestTriesBuilder::new().build();
        let mut state_update =
            tries.new_trie_update(ShardUId::single_shard(), CryptoHash::default());
        let action_result = test_delete_large_account(
            &"alice".parse().unwrap(),
            &CryptoHash::default(),
            Account::MAX_ACCOUNT_DELETION_STORAGE_USAGE + 1,
            &mut state_update,
        );
        assert_eq!(
            action_result.result,
            Err(ActionError {
                index: None,
                kind: ActionErrorKind::DeleteAccountWithLargeState {
                    account_id: "alice".parse().unwrap()
                }
            })
        )
    }

    fn test_delete_account_with_contract(storage_usage: u64) -> ActionResult {
        let tries = TestTriesBuilder::new().build();
        let mut state_update =
            tries.new_trie_update(ShardUId::single_shard(), CryptoHash::default());
        let account_id = "alice".parse::<AccountId>().unwrap();
        let deploy_action = DeployContractAction { code: [0; 10_000].to_vec() };
        let mut account = Account::new(
            Balance::from_yoctonear(100),
            Balance::ZERO,
            AccountContract::None,
            storage_usage,
        );
        let apply_state = create_apply_state(0);
        let res = action_deploy_contract(
            &mut state_update,
            &mut account,
            &account_id,
            &deploy_action,
            Arc::clone(&apply_state.config.wasm_config),
            None,
            apply_state.current_protocol_version,
        );
        assert!(res.is_ok());
        test_delete_large_account(
            &account_id,
            &account.local_contract_hash().unwrap_or_default(),
            storage_usage,
            &mut state_update,
        )
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
                    account_id: "alice".parse().unwrap()
                }
            })
        );
    }

    fn create_delegate_action_receipt() -> (ActionReceipt, SignedDelegateAction) {
        let signed_delegate_action = SignedDelegateAction {
            delegate_action: DelegateAction {
                sender_id: "bob.test.near".parse().unwrap(),
                receiver_id: "token.test.near".parse().unwrap(),
                actions: vec![
                    non_delegate_action(
                        Action::FunctionCall(
                            Box::new(FunctionCallAction {
                                 method_name: "ft_transfer".parse().unwrap(),
                                 args: vec![123, 34, 114, 101, 99, 101, 105, 118, 101, 114, 95, 105, 100, 34, 58, 34, 106, 97, 110, 101, 46, 116, 101, 115, 116, 46, 110, 101, 97, 114, 34, 44, 34, 97, 109, 111, 117, 110, 116, 34, 58, 34, 52, 34, 125],
                                 gas: Gas::from_teragas(30),
                                 deposit: Balance::from_yoctonear(1),
                            })
                        )
                    )
                ],
                nonce: 19000001,
                max_block_height: 57,
                public_key: "ed25519:32LnPNBZQJ3uhY8yV6JqnNxtRW8E27Ps9YD1XeUNuA1m".parse::<PublicKey>().unwrap(),
            },
            signature: "ed25519:5oswo6yH6u7xduXHEC4aWc8EGmWdbFz49DaHvAVioS9tbdrxpUtoNQUa8ST9Fxpk7zS2ogWvuKaL29JjMFDi3DLe".parse().unwrap()
        };

        let action_receipt = ActionReceipt {
            signer_id: "alice.test.near".parse().unwrap(),
            signer_public_key: PublicKey::empty(near_crypto::KeyType::ED25519),
            gas_price: Balance::from_yoctonear(1),
            output_data_receivers: Vec::new(),
            input_data_ids: Vec::new(),
            actions: vec![Action::Delegate(Box::new(signed_delegate_action.clone()))],
        };

        (action_receipt, signed_delegate_action)
    }

    fn create_apply_state(block_height: BlockHeight) -> ApplyState {
        ApplyState {
            apply_reason: ApplyChunkReason::UpdateTrackedShard,
            block_height,
            prev_block_hash: CryptoHash::default(),
            shard_id: ShardUId::single_shard().shard_id(),
            epoch_id: EpochId::default(),
            epoch_height: 3,
            gas_price: Balance::from_yoctonear(2),
            block_timestamp: 1,
            gas_limit: None,
            random_seed: CryptoHash::default(),
            current_protocol_version: 1,
            config: Arc::new(RuntimeConfig::test()),
            cache: None,
            is_new_chunk: false,
            congestion_info: BlockCongestionInfo::default(),
            bandwidth_requests: BlockBandwidthRequests::empty(),
            trie_access_tracker_state: Default::default(),
            on_post_state_ready: None,
        }
    }

    fn non_delegate_action(action: Action) -> NonDelegateAction {
        NonDelegateAction::try_from(action)
            .expect("cannot violate type invariants, not even in test")
    }

    #[test]
    fn test_delegate_action() {
        let mut result = ActionResult::default();
        let (action_receipt, signed_delegate_action) = create_delegate_action_receipt();
        let sender_id = signed_delegate_action.delegate_action.sender_id.clone();
        let sender_pub_key = signed_delegate_action.delegate_action.public_key.clone();
        let access_key = AccessKey { nonce: 19000000, permission: AccessKeyPermission::FullAccess };

        let apply_state =
            create_apply_state(signed_delegate_action.delegate_action.max_block_height);
        let mut state_update = setup_account(&sender_id, &sender_pub_key, &access_key);

        apply_delegate_action(
            &mut state_update,
            &apply_state,
            &VersionedActionReceipt::from(&action_receipt),
            &sender_id,
            &signed_delegate_action,
            &mut result,
            ReceiptPriority::NoPriority,
        )
        .expect("Expect ok");

        assert!(result.result.is_ok(), "Result error: {:?}", result.result.err());
        assert_eq!(
            result.new_receipts,
            vec![Receipt::V0(ReceiptV0 {
                predecessor_id: sender_id.clone(),
                receiver_id: signed_delegate_action.delegate_action.receiver_id.clone(),
                receipt_id: CryptoHash::default(),
                receipt: ReceiptEnum::Action(ActionReceipt {
                    signer_id: action_receipt.signer_id.clone(),
                    signer_public_key: action_receipt.signer_public_key.clone(),
                    gas_price: action_receipt.gas_price,
                    output_data_receivers: Vec::new(),
                    input_data_ids: Vec::new(),
                    actions: signed_delegate_action.delegate_action.get_actions(),
                }),
            })]
        );
    }

    #[test]
    fn test_delegate_action_signature_verification() {
        let mut result = ActionResult::default();
        let (action_receipt, mut signed_delegate_action) = create_delegate_action_receipt();
        let sender_id = signed_delegate_action.delegate_action.sender_id.clone();
        let sender_pub_key = signed_delegate_action.delegate_action.public_key.clone();
        let access_key = AccessKey { nonce: 19000000, permission: AccessKeyPermission::FullAccess };

        let apply_state =
            create_apply_state(signed_delegate_action.delegate_action.max_block_height);
        let mut state_update = setup_account(&sender_id, &sender_pub_key, &access_key);

        // Corrupt receiver_id. Signature verification must fail.
        signed_delegate_action.delegate_action.receiver_id = "www.test.near".parse().unwrap();

        apply_delegate_action(
            &mut state_update,
            &apply_state,
            &VersionedActionReceipt::from(action_receipt),
            &sender_id,
            &signed_delegate_action,
            &mut result,
            ReceiptPriority::NoPriority,
        )
        .expect("Expect ok");

        assert_eq!(result.result, Err(ActionErrorKind::DelegateActionInvalidSignature.into()));
    }

    #[test]
    fn test_delegate_action_max_height() {
        let mut result = ActionResult::default();
        let (action_receipt, signed_delegate_action) = create_delegate_action_receipt();
        let sender_id = signed_delegate_action.delegate_action.sender_id.clone();
        let sender_pub_key = signed_delegate_action.delegate_action.public_key.clone();
        let access_key = AccessKey { nonce: 19000000, permission: AccessKeyPermission::FullAccess };

        // Setup current block as higher than max_block_height. Must fail.
        let apply_state =
            create_apply_state(signed_delegate_action.delegate_action.max_block_height + 1);
        let mut state_update = setup_account(&sender_id, &sender_pub_key, &access_key);

        apply_delegate_action(
            &mut state_update,
            &apply_state,
            &VersionedActionReceipt::from(action_receipt),
            &sender_id,
            &signed_delegate_action,
            &mut result,
            ReceiptPriority::NoPriority,
        )
        .expect("Expect ok");

        assert_eq!(result.result, Err(ActionErrorKind::DelegateActionExpired.into()));
    }

    #[test]
    fn test_delegate_action_validate_sender_account() {
        let mut result = ActionResult::default();
        let (action_receipt, signed_delegate_action) = create_delegate_action_receipt();
        let sender_id = signed_delegate_action.delegate_action.sender_id.clone();
        let sender_pub_key = signed_delegate_action.delegate_action.public_key.clone();
        let access_key = AccessKey { nonce: 19000000, permission: AccessKeyPermission::FullAccess };

        let apply_state =
            create_apply_state(signed_delegate_action.delegate_action.max_block_height);
        let mut state_update = setup_account(&sender_id, &sender_pub_key, &access_key);

        // Use a different sender_id. Must fail.
        apply_delegate_action(
            &mut state_update,
            &apply_state,
            &VersionedActionReceipt::from(action_receipt),
            &"www.test.near".parse().unwrap(),
            &signed_delegate_action,
            &mut result,
            ReceiptPriority::NoPriority,
        )
        .expect("Expect ok");

        assert_eq!(
            result.result,
            Err(ActionErrorKind::DelegateActionSenderDoesNotMatchTxReceiver {
                sender_id: sender_id.clone(),
                receiver_id: "www.test.near".parse().unwrap(),
            }
            .into())
        );

        // Sender account doesn't exist. Must fail.
        assert_eq!(
            check_account_existence(
                &Action::Delegate(Box::new(signed_delegate_action)),
                &mut None,
                &sender_id,
                &RuntimeConfig::test(),
                false,
            ),
            Err(ActionErrorKind::AccountDoesNotExist { account_id: sender_id.clone() }.into())
        );
    }

    #[test]
    fn test_validate_delegate_action_key_update_nonce() {
        let (_, signed_delegate_action) = create_delegate_action_receipt();
        let sender_id = &signed_delegate_action.delegate_action.sender_id;
        let sender_pub_key = &signed_delegate_action.delegate_action.public_key;
        let access_key = AccessKey { nonce: 19000000, permission: AccessKeyPermission::FullAccess };

        let apply_state =
            create_apply_state(signed_delegate_action.delegate_action.max_block_height);
        let mut state_update = setup_account(sender_id, sender_pub_key, &access_key);

        // Everything is ok
        let mut result = ActionResult::default();
        validate_delegate_action_key(
            &mut state_update,
            &apply_state,
            &signed_delegate_action.delegate_action,
            &mut result,
        )
        .expect("Expect ok");
        assert!(result.result.is_ok(), "Result error: {:?}", result.result);

        // Must fail, Nonce had been updated by previous step.
        result = ActionResult::default();
        validate_delegate_action_key(
            &mut state_update,
            &apply_state,
            &signed_delegate_action.delegate_action,
            &mut result,
        )
        .expect("Expect ok");
        assert_eq!(
            result.result,
            Err(ActionErrorKind::DelegateActionInvalidNonce {
                delegate_nonce: signed_delegate_action.delegate_action.nonce,
                ak_nonce: signed_delegate_action.delegate_action.nonce,
            }
            .into())
        );

        // Increment nonce. Must pass.
        result = ActionResult::default();
        let mut delegate_action = signed_delegate_action.delegate_action.clone();
        delegate_action.nonce += 1;
        validate_delegate_action_key(
            &mut state_update,
            &apply_state,
            &delegate_action,
            &mut result,
        )
        .expect("Expect ok");
        assert!(result.result.is_ok(), "Result error: {:?}", result.result);
    }

    #[test]
    fn test_delegate_action_key_does_not_exist() {
        let mut result = ActionResult::default();
        let (_, signed_delegate_action) = create_delegate_action_receipt();
        let sender_id = signed_delegate_action.delegate_action.sender_id.clone();
        let sender_pub_key = signed_delegate_action.delegate_action.public_key.clone();
        let access_key = AccessKey { nonce: 19000000, permission: AccessKeyPermission::FullAccess };

        let apply_state =
            create_apply_state(signed_delegate_action.delegate_action.max_block_height);
        let mut state_update = setup_account(
            &sender_id,
            &PublicKey::empty(near_crypto::KeyType::ED25519),
            &access_key,
        );

        validate_delegate_action_key(
            &mut state_update,
            &apply_state,
            &signed_delegate_action.delegate_action,
            &mut result,
        )
        .expect("Expect ok");
        assert_eq!(
            result.result,
            Err(ActionErrorKind::DelegateActionAccessKeyError(
                InvalidAccessKeyError::AccessKeyNotFound {
                    account_id: sender_id,
                    public_key: sender_pub_key.into(),
                },
            )
            .into())
        );
    }

    #[test]
    fn test_delegate_action_key_incorrect_nonce() {
        let mut result = ActionResult::default();
        let (_, signed_delegate_action) = create_delegate_action_receipt();
        let sender_id = signed_delegate_action.delegate_action.sender_id.clone();
        let sender_pub_key = signed_delegate_action.delegate_action.public_key.clone();
        let access_key = AccessKey {
            nonce: signed_delegate_action.delegate_action.nonce,
            permission: AccessKeyPermission::FullAccess,
        };

        let apply_state =
            create_apply_state(signed_delegate_action.delegate_action.max_block_height);
        let mut state_update = setup_account(&sender_id, &sender_pub_key, &access_key);

        validate_delegate_action_key(
            &mut state_update,
            &apply_state,
            &signed_delegate_action.delegate_action,
            &mut result,
        )
        .expect("Expect ok");
        assert_eq!(
            result.result,
            Err(ActionErrorKind::DelegateActionInvalidNonce {
                delegate_nonce: signed_delegate_action.delegate_action.nonce,
                ak_nonce: signed_delegate_action.delegate_action.nonce,
            }
            .into())
        );
    }

    #[test]
    fn test_delegate_action_key_nonce_too_large() {
        let mut result = ActionResult::default();
        let (_, signed_delegate_action) = create_delegate_action_receipt();
        let sender_id = signed_delegate_action.delegate_action.sender_id.clone();
        let sender_pub_key = signed_delegate_action.delegate_action.public_key.clone();
        let access_key = AccessKey { nonce: 19000000, permission: AccessKeyPermission::FullAccess };

        let apply_state = create_apply_state(1);
        let mut state_update = setup_account(&sender_id, &sender_pub_key, &access_key);

        validate_delegate_action_key(
            &mut state_update,
            &apply_state,
            &signed_delegate_action.delegate_action,
            &mut result,
        )
        .expect("Expect ok");
        assert_eq!(
            result.result,
            Err(ActionErrorKind::DelegateActionNonceTooLarge {
                delegate_nonce: signed_delegate_action.delegate_action.nonce,
                upper_bound: 1000000,
            }
            .into())
        );
    }

    fn test_delegate_action_key_permissions(
        access_key: &AccessKey,
        delegate_action: &DelegateAction,
    ) -> ActionResult {
        let mut result = ActionResult::default();
        let sender_id = delegate_action.sender_id.clone();
        let sender_pub_key = delegate_action.public_key.clone();

        let apply_state = create_apply_state(delegate_action.max_block_height);
        let mut state_update = setup_account(&sender_id, &sender_pub_key, &access_key);

        validate_delegate_action_key(
            &mut state_update,
            &apply_state,
            &delegate_action,
            &mut result,
        )
        .expect("Expect ok");

        result
    }

    #[test]
    fn test_delegate_action_key_permissions_function_call() {
        let (_, signed_delegate_action) = create_delegate_action_receipt();
        let access_key = AccessKey {
            nonce: 19000000,
            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: None,
                receiver_id: signed_delegate_action.delegate_action.receiver_id.to_string(),
                method_names: vec!["test_method".parse().unwrap()],
            }),
        };

        let mut delegate_action = signed_delegate_action.delegate_action;
        delegate_action.actions =
            vec![non_delegate_action(Action::FunctionCall(Box::new(FunctionCallAction {
                args: Vec::new(),
                deposit: Balance::ZERO,
                gas: Gas::from_gas(300),
                method_name: "test_method".parse().unwrap(),
            })))];
        let result = test_delegate_action_key_permissions(&access_key, &delegate_action);
        assert!(result.result.is_ok(), "Result error {:?}", result.result);
    }

    #[test]
    fn test_delegate_action_key_permissions_incorrect_action() {
        let (_, signed_delegate_action) = create_delegate_action_receipt();
        let access_key = AccessKey {
            nonce: 19000000,
            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: None,
                receiver_id: signed_delegate_action.delegate_action.receiver_id.to_string(),
                method_names: vec!["test_method".parse().unwrap()],
            }),
        };

        let mut delegate_action = signed_delegate_action.delegate_action;
        delegate_action.actions =
            vec![non_delegate_action(Action::CreateAccount(CreateAccountAction {}))];

        let result = test_delegate_action_key_permissions(&access_key, &delegate_action);

        assert_eq!(
            result.result,
            Err(ActionErrorKind::DelegateActionAccessKeyError(
                InvalidAccessKeyError::RequiresFullAccess,
            )
            .into())
        );
    }

    #[test]
    fn test_delegate_action_key_permissions_actions_number() {
        let (_, signed_delegate_action) = create_delegate_action_receipt();
        let access_key = AccessKey {
            nonce: 19000000,
            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: None,
                receiver_id: signed_delegate_action.delegate_action.receiver_id.to_string(),
                method_names: vec!["test_method".parse().unwrap()],
            }),
        };

        let mut delegate_action = signed_delegate_action.delegate_action;
        delegate_action.actions = vec![
            non_delegate_action(Action::FunctionCall(Box::new(FunctionCallAction {
                args: Vec::new(),
                deposit: Balance::ZERO,
                gas: Gas::from_gas(300),
                method_name: "test_method".parse().unwrap(),
            }))),
            non_delegate_action(Action::FunctionCall(Box::new(FunctionCallAction {
                args: Vec::new(),
                deposit: Balance::ZERO,
                method_name: "test_method".parse().unwrap(),
                gas: Gas::from_gas(300),
            }))),
        ];

        let result = test_delegate_action_key_permissions(&access_key, &delegate_action);

        assert_eq!(
            result.result,
            Err(ActionErrorKind::DelegateActionAccessKeyError(
                InvalidAccessKeyError::RequiresFullAccess,
            )
            .into())
        );
    }

    #[test]
    fn test_delegate_action_key_permissions_function_call_deposit() {
        let (_, signed_delegate_action) = create_delegate_action_receipt();
        let access_key = AccessKey {
            nonce: 19000000,
            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: None,
                receiver_id: signed_delegate_action.delegate_action.receiver_id.to_string(),
                method_names: Vec::new(),
            }),
        };

        let mut delegate_action = signed_delegate_action.delegate_action;
        delegate_action.actions =
            vec![non_delegate_action(Action::FunctionCall(Box::new(FunctionCallAction {
                args: Vec::new(),
                deposit: Balance::from_yoctonear(1),
                gas: Gas::from_gas(300),
                method_name: "test_method".parse().unwrap(),
            })))];

        let result = test_delegate_action_key_permissions(&access_key, &delegate_action);

        assert_eq!(
            result.result,
            Err(ActionErrorKind::DelegateActionAccessKeyError(
                InvalidAccessKeyError::DepositWithFunctionCall,
            )
            .into())
        );
    }

    #[test]
    fn test_delegate_action_key_permissions_receiver_id() {
        let (_, signed_delegate_action) = create_delegate_action_receipt();
        let access_key = AccessKey {
            nonce: 19000000,
            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: None,
                receiver_id: "another.near".parse().unwrap(),
                method_names: Vec::new(),
            }),
        };

        let mut delegate_action = signed_delegate_action.delegate_action;
        delegate_action.actions =
            vec![non_delegate_action(Action::FunctionCall(Box::new(FunctionCallAction {
                args: Vec::new(),
                deposit: Balance::ZERO,
                gas: Gas::from_gas(300),
                method_name: "test_method".parse().unwrap(),
            })))];

        let result = test_delegate_action_key_permissions(&access_key, &delegate_action);

        assert_eq!(
            result.result,
            Err(ActionErrorKind::DelegateActionAccessKeyError(
                InvalidAccessKeyError::ReceiverMismatch {
                    tx_receiver: delegate_action.receiver_id,
                    ak_receiver: "another.near".parse().unwrap(),
                },
            )
            .into())
        );
    }

    #[test]
    fn test_delegate_action_key_permissions_method() {
        let (_, signed_delegate_action) = create_delegate_action_receipt();
        let access_key = AccessKey {
            nonce: 19000000,
            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: None,
                receiver_id: signed_delegate_action.delegate_action.receiver_id.to_string(),
                method_names: vec!["another_method".parse().unwrap()],
            }),
        };

        let mut delegate_action = signed_delegate_action.delegate_action;
        delegate_action.actions =
            vec![non_delegate_action(Action::FunctionCall(Box::new(FunctionCallAction {
                args: Vec::new(),
                deposit: Balance::ZERO,
                gas: Gas::from_gas(300),
                method_name: "test_method".parse().unwrap(),
            })))];

        let result = test_delegate_action_key_permissions(&access_key, &delegate_action);

        assert_eq!(
            result.result,
            Err(ActionErrorKind::DelegateActionAccessKeyError(
                InvalidAccessKeyError::MethodNameMismatch {
                    method_name: "test_method".parse().unwrap(),
                },
            )
            .into())
        );
    }
}
