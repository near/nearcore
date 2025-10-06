use crate::VerificationResult;
use crate::config::{TransactionCost, total_prepaid_gas};
use crate::near_primitives::account::Account;
use near_crypto::key_conversion::is_valid_staking_key;
use near_parameters::RuntimeConfig;
use near_primitives::account::{AccessKey, AccessKeyPermission};
use near_primitives::action::delegate::SignedDelegateAction;
use near_primitives::action::{DeployGlobalContractAction, DeterministicStateInitAction};
use near_primitives::errors::{
    ActionsValidationError, InvalidAccessKeyError, InvalidTxError, ReceiptValidationError,
};
use near_primitives::receipt::{
    DataReceipt, Receipt, VersionedActionReceipt, VersionedReceiptEnum,
};
use near_primitives::transaction::{
    Action, AddKeyAction, DeployContractAction, FunctionCallAction, SignedTransaction, StakeAction,
    Transaction,
};
use near_primitives::transaction::{DeleteAccountAction, ValidatedTransaction};
use near_primitives::types::{AccountId, Balance, Gas};
use near_primitives::types::{BlockHeight, StorageUsage};
use near_primitives::utils::derive_near_deterministic_account_id;
use near_primitives::version::ProtocolFeature;
use near_primitives::version::ProtocolVersion;
use near_store::{
    StorageError, TrieUpdate, get_access_key, get_account, set_access_key, set_account,
};
use near_vm_runner::logic::LimitConfig;

pub const ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT: StorageUsage = 770;

/// Possible errors when checking whether an account has enough tokens for storage staking
/// Read details of state staking
/// <https://nomicon.io/Economics/README.html#state-stake>.
pub enum StorageStakingError {
    /// An account does not have enough and the additional amount needed for storage staking
    LackBalanceForStorageStaking(Balance),
    /// Storage consistency error: an account has invalid storage usage or amount or locked amount
    StorageError(String),
}

/// Checks if given account has enough balance for storage stake.
///
/// Note that the current account balance has to be provided separately. This is to accommodate
/// callers which want to check for specific balance and not necessarily the balance specified
/// inside the account.
///
/// Returns:
///
///  - Ok(()) if account has enough balance or is a zero-balance account
///  - Err(StorageStakingError::LackBalanceForStorageStaking(amount)) if account doesn't have enough and how much need to be added,
///  - Err(StorageStakingError::StorageError(err)) if account has invalid storage usage or amount/locked.
pub fn check_storage_stake(
    account: &Account,
    account_balance: Balance,
    runtime_config: &RuntimeConfig,
) -> Result<(), StorageStakingError> {
    let billable_storage_bytes = account.storage_usage();
    let required_amount = runtime_config
        .storage_amount_per_byte()
        .checked_mul(u128::from(billable_storage_bytes))
        .ok_or_else(|| {
            format!(
                "Account's billable storage usage {} overflows multiplication",
                billable_storage_bytes
            )
        })
        .map_err(StorageStakingError::StorageError)?;
    let available_amount = account_balance
        .checked_add(account.locked())
        .ok_or_else(|| {
            format!(
                "Account's amount {} and locked {} overflow addition",
                account.amount(),
                account.locked(),
            )
        })
        .map_err(StorageStakingError::StorageError)?;
    if available_amount >= required_amount {
        Ok(())
    } else {
        if is_zero_balance_account(account) {
            return Ok(());
        }
        Err(StorageStakingError::LackBalanceForStorageStaking(
            required_amount.checked_sub(available_amount).unwrap(),
        ))
    }
}

/// Zero Balance Account introduced in NEP 448 https://github.com/near/NEPs/pull/448
/// An account is a zero balance account if and only if the account uses no more than `ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT` bytes
fn is_zero_balance_account(account: &Account) -> bool {
    account.storage_usage() <= ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT
}

fn validate_transaction_actions(
    config: &RuntimeConfig,
    signed_tx: &SignedTransaction,
    current_protocol_version: ProtocolVersion,
) -> Result<(), InvalidTxError> {
    validate_actions(
        &config.wasm_config.limit_config,
        signed_tx.transaction.actions(),
        signed_tx.transaction.receiver_id(),
        current_protocol_version,
    )
    .map_err(InvalidTxError::ActionsValidation)
}

/// Validates the transaction without using the state. It allows any node to validate a
/// transaction before forwarding it to the node that tracks the `signer_id` account.
#[allow(clippy::result_large_err)]
pub fn validate_transaction(
    config: &RuntimeConfig,
    signed_tx: SignedTransaction,
    current_protocol_version: ProtocolVersion,
) -> Result<ValidatedTransaction, (InvalidTxError, SignedTransaction)> {
    if let Err(err) = validate_transaction_actions(&config, &signed_tx, current_protocol_version) {
        return Err((err, signed_tx));
    }
    ValidatedTransaction::new(config, signed_tx)
}

/// Validates a transaction contains well-formed actions and is valid for the given runtime config.
///
/// This function is similar to `validate_transaction` but does NOT verify the signature.
pub(crate) fn validate_transaction_well_formed<'a>(
    config: &RuntimeConfig,
    signed_tx: &SignedTransaction,
    current_protocol_version: ProtocolVersion,
) -> Result<(), InvalidTxError> {
    validate_transaction_actions(config, signed_tx, current_protocol_version)?;
    ValidatedTransaction::check_valid_for_config(config, signed_tx)
}

/// Set new `signer` and `access_key` in `state_update`.
///
/// Note that this does not commit state changes to the `TrieUpdate`.
pub fn set_tx_state_changes(
    state_update: &mut TrieUpdate,
    validated_tx: &ValidatedTransaction,
    signer: &Account,
    access_key: &AccessKey,
) {
    let tx = validated_tx.to_tx();
    set_access_key(state_update, tx.signer_id().clone(), tx.public_key().clone(), &access_key);
    set_account(state_update, tx.signer_id().clone(), &signer);
}

pub fn get_signer_and_access_key(
    state_update: &dyn near_store::TrieAccess,
    validated_tx: &ValidatedTransaction,
) -> Result<(Account, AccessKey), InvalidTxError> {
    let signer_id = validated_tx.signer_id();

    let signer = match get_account(state_update, signer_id)? {
        Some(signer) => signer,
        None => {
            return Err(InvalidTxError::SignerDoesNotExist { signer_id: signer_id.clone() });
        }
    };

    let access_key = match get_access_key(state_update, signer_id, validated_tx.public_key())? {
        Some(access_key) => access_key,
        None => {
            return Err(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::AccessKeyNotFound {
                    account_id: signer_id.clone(),
                    public_key: validated_tx.public_key().clone().into(),
                },
            )
            .into());
        }
    };
    Ok((signer, access_key))
}

/// Verify nonce, balance and access key for the transaction given the account state.
///
/// This will only modify the `signer` and `access_key` with the new state if the function returns
/// `Ok`.
pub fn verify_and_charge_tx_ephemeral(
    config: &RuntimeConfig,
    signer: &mut Account,
    access_key: &mut AccessKey,
    tx: &Transaction,
    transaction_cost: &TransactionCost,
    block_height: Option<BlockHeight>,
) -> Result<VerificationResult, InvalidTxError> {
    let _span = tracing::debug_span!(target: "runtime", "verify_and_charge_transaction").entered();
    let TransactionCost { gas_burnt, gas_remaining, receipt_gas_price, total_cost, burnt_amount } =
        *transaction_cost;
    let signer_id = tx.signer_id();
    if tx.nonce() <= access_key.nonce {
        let err = InvalidTxError::InvalidNonce { tx_nonce: tx.nonce(), ak_nonce: access_key.nonce };
        return Err(err.into());
    }
    if let Some(height) = block_height {
        let upper_bound =
            height * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER;
        if tx.nonce() >= upper_bound {
            return Err(InvalidTxError::NonceTooLarge { tx_nonce: tx.nonce(), upper_bound }.into());
        }
    }

    let balance = signer.amount();
    let Some(new_amount) = balance.checked_sub(total_cost) else {
        let signer_id = signer_id.clone();
        let err = InvalidTxError::NotEnoughBalance { signer_id, balance, cost: total_cost };
        return Err(err.into());
    };

    if let AccessKeyPermission::FunctionCall(ref mut perms) = access_key.permission {
        if let Some(ref mut allowance) = perms.allowance {
            *allowance = allowance.checked_sub(total_cost).ok_or_else(|| {
                InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::NotEnoughAllowance {
                    account_id: signer_id.clone(),
                    public_key: tx.public_key().clone().into(),
                    allowance: *allowance,
                    cost: total_cost,
                })
            })?;
        }
    }

    match check_storage_stake(&signer, new_amount, config) {
        Ok(()) => {}
        Err(StorageStakingError::LackBalanceForStorageStaking(amount)) => {
            let err = InvalidTxError::LackBalanceForState { signer_id: signer_id.clone(), amount };
            return Err(err.into());
        }
        Err(StorageStakingError::StorageError(err)) => {
            return Err(StorageError::StorageInconsistentState(err).into());
        }
    };

    if let AccessKeyPermission::FunctionCall(ref function_call_permission) = access_key.permission {
        if tx.actions().len() != 1 {
            let err = InvalidAccessKeyError::RequiresFullAccess;
            return Err(InvalidTxError::InvalidAccessKeyError(err).into());
        }
        if let Some(Action::FunctionCall(function_call)) = tx.actions().get(0) {
            if function_call.deposit > Balance::ZERO {
                let err = InvalidAccessKeyError::DepositWithFunctionCall;
                return Err(InvalidTxError::InvalidAccessKeyError(err).into());
            }
            let tx_receiver = tx.receiver_id();
            let ak_receiver = &function_call_permission.receiver_id;
            if tx_receiver != ak_receiver {
                let err = InvalidAccessKeyError::ReceiverMismatch {
                    tx_receiver: tx_receiver.clone(),
                    ak_receiver: ak_receiver.clone(),
                };
                return Err(InvalidTxError::InvalidAccessKeyError(err).into());
            }
            if !function_call_permission.method_names.is_empty()
                && function_call_permission
                    .method_names
                    .iter()
                    .all(|method_name| &function_call.method_name != method_name)
            {
                let err = InvalidAccessKeyError::MethodNameMismatch {
                    method_name: function_call.method_name.clone(),
                };
                return Err(InvalidTxError::InvalidAccessKeyError(err).into());
            }
        } else {
            let err = InvalidAccessKeyError::RequiresFullAccess;
            return Err(InvalidTxError::InvalidAccessKeyError(err).into());
        }
    };

    access_key.nonce = tx.nonce();
    signer.set_amount(new_amount);
    Ok(VerificationResult { gas_burnt, gas_remaining, receipt_gas_price, burnt_amount })
}

/// Validates a given receipt. Checks validity of the Action or Data receipt.
pub(crate) fn validate_receipt(
    limit_config: &LimitConfig,
    receipt: &Receipt,
    current_protocol_version: ProtocolVersion,
    mode: ValidateReceiptMode,
) -> Result<(), ReceiptValidationError> {
    if mode == ValidateReceiptMode::NewReceipt {
        let receipt_size: u64 =
            borsh::object_length(receipt).unwrap().try_into().expect("Can't convert usize to u64");
        if receipt_size > limit_config.max_receipt_size {
            return Err(ReceiptValidationError::ReceiptSizeExceeded {
                size: receipt_size,
                limit: limit_config.max_receipt_size,
            });
        }
    }

    // We retain these checks here as to maintain backwards compatibility
    // with AccountId validation since we illegally parse an AccountId
    // in near-vm-logic/logic.rs#fn(VMLogic::read_and_parse_account_id)
    AccountId::validate(receipt.predecessor_id().as_ref()).map_err(|_| {
        ReceiptValidationError::InvalidPredecessorId {
            account_id: receipt.predecessor_id().to_string(),
        }
    })?;
    AccountId::validate(receipt.receiver_id().as_ref()).map_err(|_| {
        ReceiptValidationError::InvalidReceiverId { account_id: receipt.receiver_id().to_string() }
    })?;

    match receipt.versioned_receipt() {
        VersionedReceiptEnum::Action(action_receipt)
        | VersionedReceiptEnum::PromiseYield(action_receipt) => validate_action_receipt(
            limit_config,
            action_receipt,
            receipt.receiver_id(),
            current_protocol_version,
        ),
        VersionedReceiptEnum::Data(data_receipt)
        | VersionedReceiptEnum::PromiseResume(data_receipt) => {
            validate_data_receipt(limit_config, &data_receipt)
        }
        VersionedReceiptEnum::GlobalContractDistribution(_) => Ok(()), // Distribution receipt can't be issued without a valid contract
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidateReceiptMode {
    /// Used for validating new receipts that were just created.
    /// More strict than `OldReceipt` mode, which has to handle older receipts.
    NewReceipt,
    /// Used for validating older receipts that were saved in the state/received. Less strict than
    /// NewReceipt validation. Tolerates some receipts that wouldn't pass new validation. It has to
    /// be less strict because:
    /// 1) Older receipts might have been created before new validation rules.
    /// 2) There is a bug which allows to create receipts that are above the size limit. Runtime has
    ///    to handle them gracefully until the receipt size limit bug is fixed.
    ///    See https://github.com/near/nearcore/issues/12606 for details.
    ExistingReceipt,
}

/// Validates given ActionReceipt. Checks validity of the number of input data dependencies and all actions.
fn validate_action_receipt(
    limit_config: &LimitConfig,
    receipt: VersionedActionReceipt,
    receiver: &AccountId,
    current_protocol_version: ProtocolVersion,
) -> Result<(), ReceiptValidationError> {
    if receipt.input_data_ids().len() as u64 > limit_config.max_number_input_data_dependencies {
        return Err(ReceiptValidationError::NumberInputDataDependenciesExceeded {
            number_of_input_data_dependencies: receipt.input_data_ids().len() as u64,
            limit: limit_config.max_number_input_data_dependencies,
        });
    }
    validate_actions(limit_config, receipt.actions(), receiver, current_protocol_version)
        .map_err(ReceiptValidationError::ActionsValidation)
}

/// Validates given data receipt. Checks validity of the length of the returned data.
fn validate_data_receipt(
    limit_config: &LimitConfig,
    receipt: &DataReceipt,
) -> Result<(), ReceiptValidationError> {
    let data_len = receipt.data.as_ref().map(|data| data.len()).unwrap_or(0);
    if data_len as u64 > limit_config.max_length_returned_data {
        return Err(ReceiptValidationError::ReturnedValueLengthExceeded {
            length: data_len as u64,
            limit: limit_config.max_length_returned_data,
        });
    }
    Ok(())
}

/// Validates given actions:
///
/// - Checks limits if applicable.
/// - Checks that the total number of actions doesn't exceed the limit.
/// - Checks that there not other action if Action::Delegate is present.
/// - Validates each individual action.
/// - Checks that the total prepaid gas doesn't exceed the limit.
pub(crate) fn validate_actions(
    limit_config: &LimitConfig,
    actions: &[Action],
    receiver: &AccountId,
    current_protocol_version: ProtocolVersion,
) -> Result<(), ActionsValidationError> {
    if actions.len() as u64 > limit_config.max_actions_per_receipt {
        return Err(ActionsValidationError::TotalNumberOfActionsExceeded {
            total_number_of_actions: actions.len() as u64,
            limit: limit_config.max_actions_per_receipt,
        });
    }

    let mut found_delegate_action = false;
    let mut iter = actions.iter().peekable();
    while let Some(action) = iter.next() {
        if let Action::DeleteAccount(_) = action {
            if iter.peek().is_some() {
                return Err(ActionsValidationError::DeleteActionMustBeFinal);
            }
        } else {
            if let Action::Delegate(_) = action {
                if found_delegate_action {
                    return Err(ActionsValidationError::DelegateActionMustBeOnlyOne);
                }
                found_delegate_action = true;
            }
        }
        validate_action(limit_config, action, receiver, current_protocol_version)?;
    }

    let total_prepaid_gas =
        total_prepaid_gas(actions).map_err(|_| ActionsValidationError::IntegerOverflow)?;
    if total_prepaid_gas > limit_config.max_total_prepaid_gas {
        return Err(ActionsValidationError::TotalPrepaidGasExceeded {
            total_prepaid_gas,
            limit: limit_config.max_total_prepaid_gas,
        });
    }

    Ok(())
}

/// Validates a single given action. Checks limits if applicable.
pub fn validate_action(
    limit_config: &LimitConfig,
    action: &Action,
    receiver: &AccountId,
    current_protocol_version: ProtocolVersion,
) -> Result<(), ActionsValidationError> {
    match action {
        Action::CreateAccount(_) => Ok(()),
        Action::DeployContract(a) => validate_deploy_contract_action(limit_config, a),
        Action::DeployGlobalContract(a) => validate_deploy_global_contract_action(limit_config, a),
        Action::UseGlobalContract(_) => validate_use_global_contract_action(),
        Action::FunctionCall(a) => validate_function_call_action(limit_config, a),
        Action::Transfer(_) => Ok(()),
        Action::Stake(a) => validate_stake_action(a),
        Action::AddKey(a) => validate_add_key_action(limit_config, a),
        Action::DeleteKey(_) => Ok(()),
        Action::DeleteAccount(a) => validate_delete_action(a),
        Action::Delegate(a) => {
            validate_delegate_action(limit_config, a, receiver, current_protocol_version)
        }
        Action::DeterministicStateInit(a) => {
            validate_deterministic_state_init(limit_config, a, receiver, current_protocol_version)
        }
    }
}

fn validate_delegate_action(
    limit_config: &LimitConfig,
    signed_delegate_action: &SignedDelegateAction,
    receiver: &AccountId,
    current_protocol_version: ProtocolVersion,
) -> Result<(), ActionsValidationError> {
    let actions = signed_delegate_action.delegate_action.get_actions();
    validate_actions(limit_config, &actions, receiver, current_protocol_version)?;
    Ok(())
}

/// Validates `DeployContractAction`. Checks that the given contract size doesn't exceed the limit.
fn validate_deploy_contract_action(
    limit_config: &LimitConfig,
    action: &DeployContractAction,
) -> Result<(), ActionsValidationError> {
    if action.code.len() as u64 > limit_config.max_contract_size {
        return Err(ActionsValidationError::ContractSizeExceeded {
            size: action.code.len() as u64,
            limit: limit_config.max_contract_size,
        });
    }

    Ok(())
}

/// Validates `DeployGlobalContractAction`. Checks that the given contract size doesn't exceed the limit.
fn validate_deploy_global_contract_action(
    limit_config: &LimitConfig,
    action: &DeployGlobalContractAction,
) -> Result<(), ActionsValidationError> {
    if action.code.len() as u64 > limit_config.max_contract_size {
        return Err(ActionsValidationError::ContractSizeExceeded {
            size: action.code.len() as u64,
            limit: limit_config.max_contract_size,
        });
    }

    Ok(())
}

/// Validates `UseGlobalContractAction`.
fn validate_use_global_contract_action() -> Result<(), ActionsValidationError> {
    Ok(())
}

/// Validates `FunctionCallAction`. Checks that the method name length doesn't exceed the limit and
/// the length of the arguments doesn't exceed the limit.
fn validate_function_call_action(
    limit_config: &LimitConfig,
    action: &FunctionCallAction,
) -> Result<(), ActionsValidationError> {
    if action.gas == Gas::ZERO {
        return Err(ActionsValidationError::FunctionCallZeroAttachedGas);
    }

    if action.method_name.len() as u64 > limit_config.max_length_method_name {
        return Err(ActionsValidationError::FunctionCallMethodNameLengthExceeded {
            length: action.method_name.len() as u64,
            limit: limit_config.max_length_method_name,
        });
    }

    if action.args.len() as u64 > limit_config.max_arguments_length {
        return Err(ActionsValidationError::FunctionCallArgumentsLengthExceeded {
            length: action.args.len() as u64,
            limit: limit_config.max_arguments_length,
        });
    }

    Ok(())
}

/// Validates `StakeAction`. Checks that the `public_key` is a valid staking key.
fn validate_stake_action(action: &StakeAction) -> Result<(), ActionsValidationError> {
    if !is_valid_staking_key(&action.public_key) {
        return Err(ActionsValidationError::UnsuitableStakingKey {
            public_key: Box::new(action.public_key.clone()),
        });
    }

    Ok(())
}

/// Validates `AddKeyAction`. If the access key permission is `FunctionCall`, checks that the
/// total number of bytes of the method names doesn't exceed the limit and
/// every method name length doesn't exceed the limit.
fn validate_add_key_action(
    limit_config: &LimitConfig,
    action: &AddKeyAction,
) -> Result<(), ActionsValidationError> {
    if let AccessKeyPermission::FunctionCall(fc) = &action.access_key.permission {
        // Check whether `receiver_id` is a valid account_id. Historically, we
        // allowed arbitrary strings there!
        match limit_config.account_id_validity_rules_version {
            near_primitives_core::config::AccountIdValidityRulesVersion::V0 => (),
            near_primitives_core::config::AccountIdValidityRulesVersion::V1 => {
                if let Err(_) = fc.receiver_id.parse::<AccountId>() {
                    return Err(ActionsValidationError::InvalidAccountId {
                        account_id: truncate_string(&fc.receiver_id, AccountId::MAX_LEN * 2),
                    });
                }
            }
        }

        // Checking method name length limits
        let mut total_number_of_bytes = 0;
        for method_name in &fc.method_names {
            let length = method_name.len() as u64;
            if length > limit_config.max_length_method_name {
                return Err(ActionsValidationError::AddKeyMethodNameLengthExceeded {
                    length,
                    limit: limit_config.max_length_method_name,
                });
            }
            // Adding terminating character to the total number of bytes
            total_number_of_bytes += length + 1;
        }
        if total_number_of_bytes > limit_config.max_number_bytes_method_names {
            return Err(ActionsValidationError::AddKeyMethodNamesNumberOfBytesExceeded {
                total_number_of_bytes,
                limit: limit_config.max_number_bytes_method_names,
            });
        }
    }

    Ok(())
}

/// Validates `DeleteAction`.
///
/// Checks that the `beneficiary_id` is a valid account ID.
fn validate_delete_action(action: &DeleteAccountAction) -> Result<(), ActionsValidationError> {
    if AccountId::validate(action.beneficiary_id.as_str()).is_err() {
        return Err(ActionsValidationError::InvalidAccountId {
            account_id: action.beneficiary_id.to_string(),
        });
    }

    Ok(())
}

fn validate_deterministic_state_init(
    limit_config: &LimitConfig,
    action: &DeterministicStateInitAction,
    receiver_id: &AccountId,
    current_protocol_version: u32,
) -> Result<(), ActionsValidationError> {
    if !ProtocolFeature::DeterministicAccountIds.enabled(current_protocol_version) {
        return Err(ActionsValidationError::UnsupportedProtocolFeature {
            protocol_feature: "DeterministicAccountIds".to_owned(),
            version: current_protocol_version,
        });
    }

    let derived_id = derive_near_deterministic_account_id(&action.state_init);

    if derived_id != *receiver_id {
        return Err(ActionsValidationError::InvalidDeterministicStateInitReceiver {
            derived_id,
            receiver_id: receiver_id.clone(),
        });
    }

    // State init entries must not violate limits of individual state keys and values.
    for (key, value) in action.state_init.data() {
        if key.len() as u64 > limit_config.max_length_storage_key {
            return Err(ActionsValidationError::DeterministicStateInitKeyLengthExceeded {
                length: key.len() as u64,
                limit: limit_config.max_length_storage_key,
            }
            .into());
        }

        if value.len() as u64 > limit_config.max_length_storage_value {
            return Err(ActionsValidationError::DeterministicStateInitValueLengthExceeded {
                length: value.len() as u64,
                limit: limit_config.max_length_storage_value,
            }
            .into());
        }
    }

    Ok(())
}

fn truncate_string(s: &str, limit: usize) -> String {
    for i in (0..=limit).rev() {
        if let Some(s) = s.get(..i) {
            return s.to_string();
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::tx_cost;
    use crate::near_primitives::shard_layout::ShardUId;
    use crate::near_primitives::trie_key::TrieKey;
    use near_crypto::{InMemorySigner, KeyType, PublicKey, Signature, Signer};
    use near_primitives::account::{AccessKey, AccountContract, FunctionCallPermission};
    use near_primitives::action::GlobalContractIdentifier;
    use near_primitives::action::delegate::{DelegateAction, NonDelegateAction};
    use near_primitives::deterministic_account_id::{
        DeterministicAccountStateInit, DeterministicAccountStateInitV1,
    };
    use near_primitives::hash::{CryptoHash, hash};
    use near_primitives::receipt::{ActionReceipt, ReceiptPriority};
    use near_primitives::test_utils::account_new;
    use near_primitives::transaction::{
        CreateAccountAction, DeleteAccountAction, DeleteKeyAction, StakeAction, TransferAction,
    };
    use near_primitives::types::{AccountId, Balance, MerkleHash, StateChangeCause};
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::test_utils::TestTriesBuilder;
    use near_store::{set, set_access_key, set_account};
    use near_vm_runner::ContractCode;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use testlib::runtime_utils::{alice_account, bob_account, eve_dot_alice_account};

    /// Initial balance used in tests.
    const TESTING_INIT_BALANCE: Balance = Balance::from_near(1_000_000_000);

    fn test_limit_config() -> LimitConfig {
        let store = near_parameters::RuntimeConfigStore::test();
        store.get_config(PROTOCOL_VERSION).wasm_config.limit_config.clone()
    }

    fn setup_common(
        initial_balance: Balance,
        initial_locked: Balance,
        access_key: Option<AccessKey>,
    ) -> (Arc<Signer>, TrieUpdate, Balance) {
        let access_keys = if let Some(key) = access_key { vec![key] } else { vec![] };
        setup_accounts(vec![(
            alice_account(),
            initial_balance,
            initial_locked,
            access_keys,
            false,
            false,
        )])
    }

    fn setup_accounts(
        // two bools: first one is whether the account has a contract, second one is whether the
        // account has data
        accounts: Vec<(AccountId, Balance, Balance, Vec<AccessKey>, bool, bool)>,
    ) -> (Arc<Signer>, TrieUpdate, Balance) {
        let tries = TestTriesBuilder::new().build();
        let root = MerkleHash::default();

        let account_id = alice_account();
        let signer: Arc<Signer> = Arc::new(InMemorySigner::test_signer(&account_id));

        let mut initial_state = tries.new_trie_update(ShardUId::single_shard(), root);
        for (account_id, initial_balance, initial_locked, access_keys, has_contract, has_data) in
            accounts
        {
            let mut initial_account = account_new(initial_balance, CryptoHash::default());
            initial_account.set_locked(initial_locked);
            let mut key_count = 0;
            for access_key in access_keys {
                let public_key = if key_count == 0 {
                    signer.public_key()
                } else {
                    PublicKey::from_seed(KeyType::ED25519, format!("{}", key_count).as_str())
                };
                set_access_key(
                    &mut initial_state,
                    account_id.clone(),
                    public_key.clone(),
                    &access_key,
                );
                initial_account.set_storage_usage(
                    initial_account
                        .storage_usage()
                        .checked_add(
                            borsh::object_length(&public_key).unwrap() as u64
                                + borsh::object_length(&access_key).unwrap() as u64
                                + 40, // storage_config.num_extra_bytes_record,
                        )
                        .unwrap(),
                );
                key_count += 1;
            }
            if has_contract {
                let code = vec![0; 100];
                let code_hash = hash(&code);
                initial_state.set_code(
                    account_id.clone(),
                    &ContractCode::new(code.clone(), Some(code_hash)),
                );
                initial_account.set_contract(AccountContract::Local(code_hash));
                initial_account.set_storage_usage(
                    initial_account.storage_usage().checked_add(code.len() as u64).unwrap(),
                );
            }
            if has_data {
                let key = b"test".to_vec();
                let value = vec![0u8; 100];
                set(
                    &mut initial_state,
                    TrieKey::ContractData { account_id: account_id.clone(), key: key.clone() },
                    &value,
                );
                initial_account.set_storage_usage(
                    initial_account
                        .storage_usage()
                        .checked_add(key.len() as u64 + value.len() as u64 + 40)
                        .unwrap(),
                );
            }
            set_account(&mut initial_state, account_id.clone(), &initial_account);
        }
        initial_state.commit(StateChangeCause::InitialState);
        let trie_changes = initial_state.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit().unwrap();

        (
            signer,
            tries.new_trie_update(ShardUId::single_shard(), root),
            Balance::from_yoctonear(100),
        )
    }

    fn assert_err_both_validations(
        config: &RuntimeConfig,
        state_update: &TrieUpdate,
        gas_price: Balance,
        signed_transaction: SignedTransaction,
        expected_err: InvalidTxError,
    ) {
        let validated_tx = match validate_transaction(config, signed_transaction, PROTOCOL_VERSION)
        {
            Ok(v) => v,
            Err((err, _tx)) => {
                assert_eq!(err, expected_err);
                return;
            }
        };
        let cost = match tx_cost(config, &validated_tx.to_tx(), gas_price, PROTOCOL_VERSION) {
            Ok(c) => c,
            Err(err) => {
                assert_eq!(InvalidTxError::from(err), expected_err);
                return;
            }
        };

        let (mut signer, mut access_key) =
            match get_signer_and_access_key(state_update, &validated_tx) {
                Ok((signer, access_key)) => (signer, access_key),
                Err(err) => {
                    assert_eq!(err, expected_err);
                    return;
                }
            };

        // Validation passed, now verification should fail with expected_err
        let err = verify_and_charge_tx_ephemeral(
            config,
            &mut signer,
            &mut access_key,
            validated_tx.to_tx(),
            &cost,
            None,
        )
        .expect_err("expected an error");
        assert_eq!(err, expected_err);
    }

    pub fn validate_verify_and_charge_transaction(
        config: &RuntimeConfig,
        state_update: &mut TrieUpdate,
        signed_tx: SignedTransaction,
        gas_price: Balance,
        block_height: Option<BlockHeight>,
        current_protocol_version: ProtocolVersion,
    ) -> Result<VerificationResult, InvalidTxError> {
        let validated_tx = match validate_transaction(config, signed_tx, current_protocol_version) {
            Ok(validated_tx) => validated_tx,
            Err((err, _tx)) => return Err(err),
        };
        let (mut signer, mut access_key) = get_signer_and_access_key(state_update, &validated_tx)?;

        let transaction_cost =
            tx_cost(config, &validated_tx.to_tx(), gas_price, current_protocol_version)?;
        let vr = verify_and_charge_tx_ephemeral(
            config,
            &mut signer,
            &mut access_key,
            validated_tx.to_tx(),
            &transaction_cost,
            block_height,
        )?;
        set_tx_state_changes(state_update, &validated_tx, &signer, &access_key);
        Ok(vr)
    }

    mod zero_balance_account_tests {
        use crate::near_primitives::account::id::AccountId;
        use crate::near_primitives::account::{
            AccessKey, AccessKeyPermission, Account, FunctionCallPermission,
        };
        use crate::near_primitives::types::Balance;
        use crate::verifier::tests::{TESTING_INIT_BALANCE, setup_accounts};
        use crate::verifier::{ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT, is_zero_balance_account};
        use near_store::{TrieUpdate, get_account};
        use testlib::runtime_utils::{alice_account, bob_account};

        fn set_up_test_account(
            account_id: &AccountId,
            num_full_access_keys: u64,
            num_function_call_access_keys: u64,
        ) -> (Account, TrieUpdate) {
            let mut access_keys = vec![];
            for _ in 0..num_full_access_keys {
                access_keys.push(AccessKey::full_access());
            }
            for _ in 0..num_function_call_access_keys {
                let access_key = AccessKey {
                    nonce: 0,
                    permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                        allowance: Some(Balance::from_yoctonear(100)),
                        receiver_id: "a".repeat(64),
                        method_names: vec![],
                    }),
                };
                access_keys.push(access_key);
            }
            let (_, state_update, _) = setup_accounts(vec![(
                account_id.clone(),
                TESTING_INIT_BALANCE,
                Balance::ZERO,
                access_keys,
                false,
                false,
            )]);
            let account = get_account(&state_update, account_id).unwrap().unwrap();
            (account, state_update)
        }

        /// Testing all combination of access keys in this test to make sure that an account
        /// is zero balance only if it uses <= `ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT` bytes in storage
        #[test]
        fn test_zero_balance_account_with_keys() {
            for num_full_access_key in 0..10 {
                for num_function_call_access_key in 0..10 {
                    let account_id: AccountId = format!(
                        "alice{}.near",
                        num_full_access_key * 1000 + num_function_call_access_key
                    )
                    .parse()
                    .unwrap();
                    let (account, _) = set_up_test_account(
                        &account_id,
                        num_full_access_key,
                        num_function_call_access_key,
                    );
                    let res = is_zero_balance_account(&account);
                    assert_eq!(
                        res,
                        num_full_access_key * 82
                            + num_function_call_access_key * 171
                            + std::mem::size_of::<Account>() as u64
                            <= ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT
                    );
                }
            }
        }

        /// A single function call access key that is too large (due to too many method names)
        #[test]
        fn test_zero_balance_account_with_invalid_access_key() {
            let account_id = alice_account();
            let method_names =
                (0..30).map(|i| format!("long_method_name_{}", i)).collect::<Vec<_>>();
            let (_, state_update, _) = setup_accounts(vec![(
                account_id.clone(),
                Balance::ZERO,
                Balance::ZERO,
                vec![AccessKey {
                    nonce: 0,
                    permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                        allowance: Some(Balance::from_yoctonear(100)),
                        receiver_id: bob_account().into(),
                        method_names,
                    }),
                }],
                false,
                false,
            )]);
            let account = get_account(&state_update, &account_id).unwrap().unwrap();
            assert!(!is_zero_balance_account(&account));
        }
    }

    // Transactions

    #[test]
    fn test_validate_transaction_valid() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let deposit = Balance::from_yoctonear(100);
        let signed_tx = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            deposit,
            CryptoHash::default(),
        );

        let verification_result = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect("valid transaction");
        // Should not be free. Burning for sending
        assert!(verification_result.gas_burnt > Gas::ZERO);
        // All burned gas goes to the validators at current gas price
        assert_eq!(
            verification_result.burnt_amount,
            gas_price.checked_mul(u128::from(verification_result.gas_burnt.as_gas())).unwrap()
        );

        let account = get_account(&state_update, &alice_account()).unwrap().unwrap();
        // Balance is decreased by (TX fees + transfer balance).
        assert_eq!(
            account.amount(),
            TESTING_INIT_BALANCE
                .checked_sub(
                    verification_result
                        .receipt_gas_price
                        .checked_mul(u128::from(verification_result.gas_remaining.as_gas()))
                        .unwrap()
                )
                .unwrap()
                .checked_sub(verification_result.burnt_amount)
                .unwrap()
                .checked_sub(deposit)
                .unwrap()
        );

        let access_key =
            get_access_key(&state_update, &alice_account(), &signer.public_key()).unwrap().unwrap();
        assert_eq!(access_key.nonce, 1);
    }

    #[test]
    fn test_validate_transaction_invalid_signature() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let mut tx = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            Balance::from_yoctonear(100),
            CryptoHash::default(),
        );
        tx.signature = signer.sign(CryptoHash::default().as_ref());

        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            tx,
            InvalidTxError::InvalidSignature,
        );
    }

    #[test]
    fn test_validate_transaction_invalid_access_key_not_found() {
        let config = RuntimeConfig::test();
        let (bad_signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, None);

        let transaction = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*bad_signer,
            Balance::from_yoctonear(100),
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            transaction,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(
            err,
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::AccessKeyNotFound {
                account_id: alice_account(),
                public_key: bad_signer.public_key().into(),
            })
        );
    }

    #[test]
    fn test_validate_transaction_invalid_bad_action() {
        let mut config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let wasm_config = Arc::make_mut(&mut config.wasm_config);
        wasm_config.limit_config.max_total_prepaid_gas = Gas::from_gas(100);

        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            SignedTransaction::from_actions(
                1,
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "hello".to_string(),
                    args: b"abc".to_vec(),
                    gas: Gas::from_gas(200),
                    deposit: Balance::ZERO,
                }))],
                CryptoHash::default(),
                0,
            ),
            InvalidTxError::ActionsValidation(ActionsValidationError::TotalPrepaidGasExceeded {
                total_prepaid_gas: Gas::from_gas(200),
                limit: Gas::from_gas(100),
            }),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_bad_signer() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let signed_tx = SignedTransaction::send_money(
            1,
            bob_account(),
            alice_account(),
            &*signer,
            Balance::from_yoctonear(100),
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(err, InvalidTxError::SignerDoesNotExist { signer_id: bob_account() });
    }

    #[test]
    fn test_validate_transaction_invalid_bad_nonce() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey { nonce: 2, permission: AccessKeyPermission::FullAccess }),
        );

        let transaction = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            Balance::from_yoctonear(100),
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            transaction,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(err, InvalidTxError::InvalidNonce { tx_nonce: 1, ak_nonce: 2 });
    }

    #[test]
    fn test_validate_transaction_invalid_balance_overflow() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            SignedTransaction::send_money(
                1,
                alice_account(),
                bob_account(),
                &*signer,
                Balance::MAX,
                CryptoHash::default(),
            ),
            InvalidTxError::CostOverflow,
        );
    }

    #[test]
    fn test_validate_transaction_invalid_transaction_version() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            SignedTransaction::from_actions_v1(
                1,
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
                CryptoHash::default(),
                1,
            ),
            InvalidTxError::InvalidTransactionVersion,
        );
    }

    #[test]
    fn test_validate_transaction_invalid_not_enough_balance() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let signed_tx = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            TESTING_INIT_BALANCE,
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        if let InvalidTxError::NotEnoughBalance { signer_id, balance, cost } = err {
            assert_eq!(signer_id, alice_account());
            assert_eq!(balance, TESTING_INIT_BALANCE);
            assert!(cost > balance);
        } else {
            panic!("Incorrect error");
        }
    }

    #[test]
    fn test_validate_transaction_invalid_not_enough_allowance() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: Some(Balance::from_yoctonear(100)),
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        let transaction = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: Gas::from_gas(300),
                deposit: Balance::ZERO,
            }))],
            CryptoHash::default(),
            0,
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            transaction,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        if let InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::NotEnoughAllowance {
            account_id,
            public_key,
            allowance,
            cost,
        }) = err
        {
            assert_eq!(account_id, alice_account());
            assert_eq!(*public_key, signer.public_key());
            assert_eq!(allowance, Balance::from_yoctonear(100));
            assert!(cost > allowance);
        } else {
            panic!("Incorrect error");
        }
    }

    #[test]
    fn test_validate_transaction_invalid_low_balance() {
        let mut config = RuntimeConfig::free();
        let fees = Arc::make_mut(&mut config.fees);
        fees.storage_usage_config.storage_amount_per_byte = Balance::from_yoctonear(10_000_000);
        let initial_balance = Balance::from_yoctonear(1_000_000_000);
        let transfer_amount = Balance::from_yoctonear(950_000_000);
        let (signer, mut state_update, gas_price) =
            setup_common(initial_balance, Balance::ZERO, Some(AccessKey::full_access()));

        let signed_tx = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            transfer_amount,
            CryptoHash::default(),
        );

        let verification_result = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .unwrap();
        assert_eq!(verification_result.gas_burnt, Gas::ZERO);
        assert_eq!(verification_result.gas_remaining, Gas::ZERO);
        assert!(verification_result.burnt_amount.is_zero());
    }

    #[test]
    fn test_validate_transaction_invalid_low_balance_many_keys() {
        let mut config = RuntimeConfig::free();
        let fees = Arc::make_mut(&mut config.fees);
        fees.storage_usage_config.storage_amount_per_byte = Balance::from_yoctonear(10_000_000);
        let initial_balance = Balance::from_yoctonear(1_000_000_000);
        let transfer_amount = Balance::from_yoctonear(950_000_000);
        let account_id = alice_account();
        let access_keys = vec![AccessKey::full_access(); 10];
        let (signer, mut state_update, gas_price) = setup_accounts(vec![(
            account_id.clone(),
            initial_balance,
            Balance::ZERO,
            access_keys,
            false,
            false,
        )]);

        let signed_tx = SignedTransaction::send_money(
            1,
            account_id.clone(),
            bob_account(),
            &*signer,
            transfer_amount,
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        let account = get_account(&state_update, &account_id).unwrap().unwrap();

        assert_eq!(
            err,
            InvalidTxError::LackBalanceForState {
                signer_id: account_id,
                amount: config
                    .storage_amount_per_byte()
                    .checked_mul(u128::from(account.storage_usage()))
                    .unwrap()
                    .checked_sub(initial_balance.checked_sub(transfer_amount).unwrap())
                    .unwrap()
            }
        );
    }

    #[test]
    fn test_validate_transaction_invalid_actions_for_function_call() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        // Case 1
        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![
                Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "hello".to_string(),
                    args: b"abc".to_vec(),
                    gas: Gas::from_gas(100),
                    deposit: Balance::ZERO,
                })),
                Action::CreateAccount(CreateAccountAction {}),
            ],
            CryptoHash::default(),
            0,
        );
        validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");

        // Case 2
        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![],
            CryptoHash::default(),
            0,
        );
        validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");

        // Case 3
        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::CreateAccount(CreateAccountAction {})],
            CryptoHash::default(),
            0,
        );
        validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
    }

    #[test]
    fn test_validate_transaction_invalid_receiver_for_function_call() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            eve_dot_alice_account(),
            &*signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: Gas::from_gas(100),
                deposit: Balance::ZERO,
            }))],
            CryptoHash::default(),
            0,
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(
            err,
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::ReceiverMismatch {
                tx_receiver: eve_dot_alice_account(),
                ak_receiver: bob_account().into()
            }),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_method_name_for_function_call() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec!["not_hello".to_string(), "world".to_string()],
                }),
            }),
        );

        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: Gas::from_gas(100),
                deposit: Balance::ZERO,
            }))],
            CryptoHash::default(),
            0,
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(
            err,
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::MethodNameMismatch {
                method_name: "hello".to_string()
            }),
        );
    }

    #[test]
    fn test_validate_transaction_deposit_with_function_call() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: Gas::from_gas(100),
                deposit: Balance::from_yoctonear(100),
            }))],
            CryptoHash::default(),
            0,
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(
            err,
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::DepositWithFunctionCall,)
        );
    }

    #[test]
    fn test_validate_transaction_exceeding_tx_size_limit() {
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::DeployContract(DeployContractAction { code: vec![1; 5] })],
            CryptoHash::default(),
            0,
        );
        let transaction_size = signed_tx.get_size();

        let mut config = RuntimeConfig::test();
        let max_transaction_size = transaction_size - 1;
        {
            let wasm_config = Arc::make_mut(&mut config.wasm_config);
            wasm_config.limit_config.max_transaction_size = transaction_size - 1;
        }

        let (err, _tx) = validate_transaction(&config, signed_tx.clone(), PROTOCOL_VERSION)
            .expect_err("expected validation error - size exceeded");
        assert_eq!(
            err,
            InvalidTxError::TransactionSizeExceeded {
                size: transaction_size,
                limit: max_transaction_size
            }
        );

        {
            let wasm_config = Arc::make_mut(&mut config.wasm_config);
            wasm_config.limit_config.max_transaction_size = transaction_size + 1;
        }

        validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect("valid transaction");
    }

    // Receipts

    #[test]
    fn test_validate_receipt_valid() {
        let limit_config = test_limit_config();
        validate_receipt(
            &limit_config,
            &Receipt::new_balance_refund(
                &alice_account(),
                Balance::from_yoctonear(10),
                ReceiptPriority::NoPriority,
            ),
            PROTOCOL_VERSION,
            ValidateReceiptMode::NewReceipt,
        )
        .expect("valid receipt");
    }

    #[test]
    fn test_validate_action_receipt_too_many_input_deps() {
        let mut limit_config = test_limit_config();
        limit_config.max_number_input_data_dependencies = 1;
        let receiver = "alice.near".parse().unwrap();
        assert_eq!(
            validate_action_receipt(
                &limit_config,
                ActionReceipt {
                    signer_id: alice_account(),
                    signer_public_key: PublicKey::empty(KeyType::ED25519),
                    gas_price: Balance::from_yoctonear(100),
                    output_data_receivers: vec![],
                    input_data_ids: vec![CryptoHash::default(), CryptoHash::default()],
                    actions: vec![]
                }
                .into(),
                &receiver,
                PROTOCOL_VERSION
            )
            .expect_err("expected an error"),
            ReceiptValidationError::NumberInputDataDependenciesExceeded {
                number_of_input_data_dependencies: 2,
                limit: 1
            }
        );
    }

    // DataReceipt

    #[test]
    fn test_validate_data_receipt_valid() {
        let limit_config = test_limit_config();
        validate_data_receipt(
            &limit_config,
            &DataReceipt { data_id: CryptoHash::default(), data: None },
        )
        .expect("valid data receipt");
        let data = b"hello".to_vec();
        validate_data_receipt(
            &limit_config,
            &DataReceipt { data_id: CryptoHash::default(), data: Some(data) },
        )
        .expect("valid data receipt");
    }

    #[test]
    fn test_validate_data_receipt_too_much_data() {
        let mut limit_config = test_limit_config();
        let data = b"hello".to_vec();
        limit_config.max_length_returned_data = data.len() as u64 - 1;
        assert_eq!(
            validate_data_receipt(
                &limit_config,
                &DataReceipt { data_id: CryptoHash::default(), data: Some(data.clone()) }
            )
            .expect_err("expected an error"),
            ReceiptValidationError::ReturnedValueLengthExceeded {
                length: data.len() as u64,
                limit: limit_config.max_length_returned_data
            }
        );
    }

    // Group of actions

    #[test]
    fn test_validate_actions_empty() {
        let limit_config = test_limit_config();
        let receiver = "alice.near".parse().unwrap();
        validate_actions(&limit_config, &[], &receiver, PROTOCOL_VERSION).expect("empty actions");
    }

    #[test]
    fn test_validate_actions_valid_function_call() {
        let limit_config = test_limit_config();
        validate_actions(
            &limit_config,
            &[Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: Gas::from_gas(100),
                deposit: Balance::ZERO,
            }))],
            &"alice.near".parse().unwrap(),
            PROTOCOL_VERSION,
        )
        .expect("valid function call action");
    }

    #[test]
    fn test_validate_actions_too_much_gas() {
        let mut limit_config = test_limit_config();
        limit_config.max_total_prepaid_gas = Gas::from_gas(220);
        assert_eq!(
            validate_actions(
                &limit_config,
                &[
                    Action::FunctionCall(Box::new(FunctionCallAction {
                        method_name: "hello".to_string(),
                        args: b"abc".to_vec(),
                        gas: Gas::from_gas(100),
                        deposit: Balance::ZERO,
                    })),
                    Action::FunctionCall(Box::new(FunctionCallAction {
                        method_name: "hello".to_string(),
                        args: b"abc".to_vec(),
                        gas: Gas::from_gas(150),
                        deposit: Balance::ZERO,
                    }))
                ],
                &"alice.near".parse().unwrap(),
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            ActionsValidationError::TotalPrepaidGasExceeded {
                total_prepaid_gas: Gas::from_gas(250),
                limit: Gas::from_gas(220)
            }
        );
    }

    #[test]
    fn test_validate_actions_gas_overflow() {
        let mut limit_config = test_limit_config();
        limit_config.max_total_prepaid_gas = Gas::from_gas(220);
        assert_eq!(
            validate_actions(
                &limit_config,
                &[
                    Action::FunctionCall(Box::new(FunctionCallAction {
                        method_name: "hello".to_string(),
                        args: b"abc".to_vec(),
                        gas: Gas::from_gas(u64::max_value() / 2 + 1),
                        deposit: Balance::ZERO,
                    })),
                    Action::FunctionCall(Box::new(FunctionCallAction {
                        method_name: "hello".to_string(),
                        args: b"abc".to_vec(),
                        gas: Gas::from_gas(u64::max_value() / 2 + 1),
                        deposit: Balance::ZERO,
                    }))
                ],
                &"alice.near".parse().unwrap(),
                PROTOCOL_VERSION,
            )
            .expect_err("Expected an error"),
            ActionsValidationError::IntegerOverflow,
        );
    }

    #[test]
    fn test_validate_actions_num_actions() {
        let mut limit_config = test_limit_config();
        limit_config.max_actions_per_receipt = 1;
        assert_eq!(
            validate_actions(
                &limit_config,
                &[
                    Action::CreateAccount(CreateAccountAction {}),
                    Action::CreateAccount(CreateAccountAction {}),
                ],
                &"alice.near".parse().unwrap(),
                PROTOCOL_VERSION,
            )
            .expect_err("Expected an error"),
            ActionsValidationError::TotalNumberOfActionsExceeded {
                total_number_of_actions: 2,
                limit: 1
            },
        );
    }

    #[test]
    fn test_validate_delete_must_be_final() {
        let mut limit_config = test_limit_config();
        limit_config.max_actions_per_receipt = 3;
        assert_eq!(
            validate_actions(
                &limit_config,
                &[
                    Action::DeleteAccount(DeleteAccountAction {
                        beneficiary_id: "bob".parse().unwrap()
                    }),
                    Action::CreateAccount(CreateAccountAction {}),
                ],
                &"alice.near".parse().unwrap(),
                PROTOCOL_VERSION,
            )
            .expect_err("Expected an error"),
            ActionsValidationError::DeleteActionMustBeFinal,
        );
    }

    #[test]
    fn test_validate_delete_must_work_if_its_final() {
        let mut limit_config = test_limit_config();
        limit_config.max_actions_per_receipt = 3;
        assert_eq!(
            validate_actions(
                &limit_config,
                &[
                    Action::CreateAccount(CreateAccountAction {}),
                    Action::DeleteAccount(DeleteAccountAction {
                        beneficiary_id: "bob".parse().unwrap()
                    }),
                ],
                &"alice.near".parse().unwrap(),
                PROTOCOL_VERSION,
            ),
            Ok(()),
        );
    }

    // Individual actions

    #[test]
    fn test_validate_action_valid_create_account() {
        validate_action(
            &test_limit_config(),
            &Action::CreateAccount(CreateAccountAction {}),
            &"alice.near".parse().unwrap(),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_function_call() {
        validate_action(
            &test_limit_config(),
            &Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: Gas::from_gas(100),
                deposit: Balance::ZERO,
            })),
            &"alice.near".parse().unwrap(),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_invalid_function_call_zero_gas() {
        assert_eq!(
            validate_action(
                &test_limit_config(),
                &Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "new".to_string(),
                    args: vec![],
                    gas: Gas::ZERO,
                    deposit: Balance::ZERO,
                })),
                &"alice.near".parse().unwrap(),
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            ActionsValidationError::FunctionCallZeroAttachedGas,
        );
    }

    #[test]
    fn test_validate_action_valid_transfer() {
        validate_action(
            &test_limit_config(),
            &Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(10) }),
            &"alice.near".parse().unwrap(),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_stake() {
        validate_action(
            &test_limit_config(),
            &Action::Stake(Box::new(StakeAction {
                stake: Balance::from_yoctonear(100),
                public_key: "ed25519:KuTCtARNzxZQ3YvXDeLjx83FDqxv2SdQTSbiq876zR7".parse().unwrap(),
            })),
            &"alice.near".parse().unwrap(),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_invalid_staking_key() {
        assert_eq!(
            validate_action(
                &test_limit_config(),
                &Action::Stake(Box::new(StakeAction {
                    stake: Balance::from_yoctonear(100),
                    public_key: PublicKey::empty(KeyType::ED25519),
                })),
                &"alice.near".parse().unwrap(),
                PROTOCOL_VERSION,
            )
            .expect_err("Expected an error"),
            ActionsValidationError::UnsuitableStakingKey {
                public_key: PublicKey::empty(KeyType::ED25519).into(),
            },
        );
    }

    #[test]
    fn test_validate_action_valid_add_key_full_permission() {
        validate_action(
            &test_limit_config(),
            &Action::AddKey(Box::new(AddKeyAction {
                public_key: PublicKey::empty(KeyType::ED25519),
                access_key: AccessKey::full_access(),
            })),
            &"alice.near".parse().unwrap(),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_add_key_function_call() {
        validate_action(
            &test_limit_config(),
            &Action::AddKey(Box::new(AddKeyAction {
                public_key: PublicKey::empty(KeyType::ED25519),
                access_key: AccessKey {
                    nonce: 0,
                    permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                        allowance: Some(Balance::from_yoctonear(1000)),
                        receiver_id: alice_account().into(),
                        method_names: vec!["hello".to_string(), "world".to_string()],
                    }),
                },
            })),
            &"alice.near".parse().unwrap(),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_delete_key() {
        validate_action(
            &test_limit_config(),
            &Action::DeleteKey(Box::new(DeleteKeyAction {
                public_key: PublicKey::empty(KeyType::ED25519),
            })),
            &"alice.near".parse().unwrap(),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_delete_account() {
        validate_action(
            &test_limit_config(),
            &Action::DeleteAccount(DeleteAccountAction { beneficiary_id: alice_account() }),
            &"alice.near".parse().unwrap(),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_delegate_action_must_be_only_one() {
        let receiver = "alice.near".parse().unwrap();
        let signed_delegate_action = SignedDelegateAction {
            delegate_action: DelegateAction {
                sender_id: "bob.test.near".parse().unwrap(),
                receiver_id: "token.test.near".parse().unwrap(),
                actions: vec![
                    NonDelegateAction::try_from(Action::CreateAccount(CreateAccountAction {}))
                        .unwrap(),
                ],
                nonce: 19000001,
                max_block_height: 57,
                public_key: PublicKey::empty(KeyType::ED25519),
            },
            signature: Signature::default(),
        };
        assert_eq!(
            validate_actions(
                &test_limit_config(),
                &[
                    Action::Delegate(Box::new(signed_delegate_action.clone())),
                    Action::Delegate(Box::new(signed_delegate_action.clone())),
                ],
                &receiver,
                PROTOCOL_VERSION,
            ),
            Err(ActionsValidationError::DelegateActionMustBeOnlyOne),
        );
        assert_eq!(
            validate_actions(
                &&test_limit_config(),
                &[Action::Delegate(Box::new(signed_delegate_action.clone())),],
                &receiver,
                PROTOCOL_VERSION,
            ),
            Ok(()),
        );
        assert_eq!(
            validate_actions(
                &test_limit_config(),
                &[
                    Action::CreateAccount(CreateAccountAction {}),
                    Action::Delegate(Box::new(signed_delegate_action)),
                ],
                &receiver,
                PROTOCOL_VERSION,
            ),
            Ok(()),
        );
    }

    #[test]
    fn test_validate_deterministic_state_init_receiver() {
        use expect_test::{Expect, expect};
        fn check_validate_state_init(
            receiver: &str,
            protocol_version: ProtocolVersion,
            want: Expect,
        ) {
            let validation_result = validate_actions(
                &test_limit_config(),
                &[Action::DeterministicStateInit(Box::new(DeterministicStateInitAction {
                    state_init: DeterministicAccountStateInit::V1(
                        DeterministicAccountStateInitV1 {
                            code: GlobalContractIdentifier::AccountId("ft.near".parse().unwrap()),
                            data: Default::default(),
                        },
                    ),
                    deposit: Balance::ZERO,
                }))],
                &receiver.parse().unwrap(),
                protocol_version,
            );

            want.assert_debug_eq(&validation_result);
        }

        // correct receiver
        check_validate_state_init(
            "0s69284a5453e7be5632b28b6a01baecf6c12c156d",
            ProtocolFeature::DeterministicAccountIds.protocol_version(),
            expect![[r#"
                Ok(
                    (),
                )
            "#]],
        );

        // deterministic id but incorrect receiver
        check_validate_state_init(
            "0s1234567890123456789012345678901234567890",
            ProtocolFeature::DeterministicAccountIds.protocol_version(),
            expect![[r#"
                Err(
                    InvalidDeterministicStateInitReceiver {
                        receiver_id: AccountId(
                            "0s1234567890123456789012345678901234567890",
                        ),
                        derived_id: AccountId(
                            "0s69284a5453e7be5632b28b6a01baecf6c12c156d",
                        ),
                    },
                )
            "#]],
        );

        // named receiver (invalid)
        check_validate_state_init(
            "alice.near",
            ProtocolFeature::DeterministicAccountIds.protocol_version(),
            expect![[r#"
                Err(
                    InvalidDeterministicStateInitReceiver {
                        receiver_id: AccountId(
                            "alice.near",
                        ),
                        derived_id: AccountId(
                            "0s69284a5453e7be5632b28b6a01baecf6c12c156d",
                        ),
                    },
                )
            "#]],
        );
        // NEAR implicit receiver (invalid)
        check_validate_state_init(
            "eab5a5da5a83e1ffb05ed0905a104e09b7e13159fd4daf82e43d047887ce4e47",
            ProtocolFeature::DeterministicAccountIds.protocol_version(),
            expect![[r#"
                Err(
                    InvalidDeterministicStateInitReceiver {
                        receiver_id: AccountId(
                            "eab5a5da5a83e1ffb05ed0905a104e09b7e13159fd4daf82e43d047887ce4e47",
                        ),
                        derived_id: AccountId(
                            "0s69284a5453e7be5632b28b6a01baecf6c12c156d",
                        ),
                    },
                )
            "#]],
        );

        // Without protocol features enabled, again with all receiver variations
        for receiver in [
            "alice.near",
            "0s69284a5453e7be5632b28b6a01baecf6c12c156d",
            "0s1234567890123456789012345678901234567890",
            "eab5a5da5a83e1ffb05ed0905a104e09b7e13159fd4daf82e43d047887ce4e47",
        ] {
            check_validate_state_init(
                receiver,
                ProtocolFeature::DeterministicAccountIds.protocol_version() - 1,
                expect![[r#"
                Err(
                    UnsupportedProtocolFeature {
                        protocol_feature: "DeterministicAccountIds",
                        version: 149,
                    },
                )
            "#]],
            );
        }
    }

    #[test]
    fn test_validate_deterministic_state_init_data() {
        use expect_test::{Expect, expect};
        fn check_validate_state_init(
            data: BTreeMap<Vec<u8>, Vec<u8>>,
            protocol_version: ProtocolVersion,
            want: Expect,
        ) {
            let state_init = DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
                code: GlobalContractIdentifier::AccountId("ft.near".parse().unwrap()),
                data,
            });
            let receiver = derive_near_deterministic_account_id(&state_init);
            let validation_result = validate_actions(
                &test_limit_config(),
                &[Action::DeterministicStateInit(Box::new(DeterministicStateInitAction {
                    state_init,
                    deposit: Balance::ZERO,
                }))],
                &receiver,
                protocol_version,
            );

            want.assert_debug_eq(&validation_result);
        }

        fn make_payload(key_size: usize, value_size: usize) -> BTreeMap<Vec<u8>, Vec<u8>> {
            let key = vec![1u8; key_size];
            let value = vec![2u8; value_size];
            BTreeMap::from_iter([(key, value)])
        }

        // small payload
        check_validate_state_init(
            make_payload(10, 20),
            ProtocolFeature::DeterministicAccountIds.protocol_version(),
            expect![[r#"
                Ok(
                    (),
                )
            "#]],
        );

        // key and value exactly at limit
        check_validate_state_init(
            make_payload(2_048, 4_194_304),
            ProtocolFeature::DeterministicAccountIds.protocol_version(),
            expect![[r#"
                Ok(
                    (),
                )
            "#]],
        );

        // key above limit
        check_validate_state_init(
            make_payload(2_049, 4_194_304),
            ProtocolFeature::DeterministicAccountIds.protocol_version(),
            expect![[r#"
                Err(
                    DeterministicStateInitKeyLengthExceeded {
                        length: 2049,
                        limit: 2048,
                    },
                )
            "#]],
        );

        // value above limit
        check_validate_state_init(
            make_payload(2_048, 4_194_305),
            ProtocolFeature::DeterministicAccountIds.protocol_version(),
            expect![[r#"
                Err(
                    DeterministicStateInitValueLengthExceeded {
                        length: 4194305,
                        limit: 4194304,
                    },
                )
            "#]],
        );
    }

    #[test]
    fn test_truncate_string() {
        fn check(input: &str, limit: usize, want: &str) {
            let got = truncate_string(input, limit);
            assert_eq!(got, want)
        }
        check("", 10, "");
        check("hello", 0, "");
        check("hello", 2, "he");
        check("hello", 4, "hell");
        check("hello", 5, "hello");
        check("hello", 6, "hello");
        check("hello", 10, "hello");
        // cspell:ignore привет
        check("привет", 3, "п");
    }
}
