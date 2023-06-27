use near_crypto::key_conversion::is_valid_staking_key;
use near_primitives::checked_feature;
use near_primitives::delegate_action::SignedDelegateAction;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::transaction::DeleteAccountAction;
use near_primitives::types::{BlockHeight, StorageUsage};
use near_primitives::version::ProtocolFeature;
use near_primitives::{
    account::AccessKeyPermission,
    config::VMLimitConfig,
    errors::{
        ActionsValidationError, InvalidAccessKeyError, InvalidTxError, ReceiptValidationError,
        RuntimeError,
    },
    receipt::{ActionReceipt, DataReceipt, Receipt, ReceiptEnum},
    transaction::{
        Action, AddKeyAction, DeployContractAction, FunctionCallAction, SignedTransaction,
        StakeAction,
    },
    types::{AccountId, Balance},
    version::ProtocolVersion,
};
use near_store::{
    get_access_key, get_account, set_access_key, set_account, StorageError, TrieUpdate,
};

use crate::config::{total_prepaid_gas, tx_cost, TransactionCost};
use crate::near_primitives::account::Account;
use crate::VerificationResult;

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

/// Checks if given account has enough balance for storage stake, and returns:
///  - Ok(()) if account has enough balance or is a zero-balance account
///  - Err(StorageStakingError::LackBalanceForStorageStaking(amount)) if account doesn't have enough and how much need to be added,
///  - Err(StorageStakingError::StorageError(err)) if account has invalid storage usage or amount/locked.
pub fn check_storage_stake(
    account: &Account,
    runtime_config: &RuntimeConfig,
    current_protocol_version: ProtocolVersion,
) -> Result<(), StorageStakingError> {
    let required_amount = Balance::from(account.storage_usage())
        .checked_mul(runtime_config.storage_amount_per_byte())
        .ok_or_else(|| {
            format!("Account's storage_usage {} overflows multiplication", account.storage_usage())
        })
        .map_err(StorageStakingError::StorageError)?;
    let available_amount = account
        .amount()
        .checked_add(account.locked())
        .ok_or_else(|| {
            format!(
                "Account's amount {} and locked {} overflow addition",
                account.amount(),
                account.locked()
            )
        })
        .map_err(StorageStakingError::StorageError)?;
    if available_amount >= required_amount {
        Ok(())
    } else {
        if checked_feature!("stable", ZeroBalanceAccount, current_protocol_version)
            && is_zero_balance_account(account)
        {
            return Ok(());
        }
        Err(StorageStakingError::LackBalanceForStorageStaking(required_amount - available_amount))
    }
}

/// Zero Balance Account introduced in NEP 448 https://github.com/near/NEPs/pull/448
/// An account is a zero balance account if and only if the account uses no more than `ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT` bytes
fn is_zero_balance_account(account: &Account) -> bool {
    account.storage_usage() <= ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT
}

/// Validates the transaction without using the state. It allows any node to validate a
/// transaction before forwarding it to the node that tracks the `signer_id` account.
pub fn validate_transaction(
    config: &RuntimeConfig,
    gas_price: Balance,
    signed_transaction: &SignedTransaction,
    verify_signature: bool,
    current_protocol_version: ProtocolVersion,
) -> Result<TransactionCost, RuntimeError> {
    let transaction = &signed_transaction.transaction;
    let signer_id = &transaction.signer_id;

    if verify_signature
        && !signed_transaction
            .signature
            .verify(signed_transaction.get_hash().as_ref(), &transaction.public_key)
    {
        return Err(InvalidTxError::InvalidSignature.into());
    }

    let transaction_size = signed_transaction.get_size();
    let max_transaction_size = config.wasm_config.limit_config.max_transaction_size;
    if transaction_size > max_transaction_size {
        return Err(InvalidTxError::TransactionSizeExceeded {
            size: transaction_size,
            limit: max_transaction_size,
        }
        .into());
    }

    validate_actions(
        &config.wasm_config.limit_config,
        &transaction.actions,
        current_protocol_version,
    )
    .map_err(InvalidTxError::ActionsValidation)?;

    let sender_is_receiver = &transaction.receiver_id == signer_id;

    tx_cost(&config.fees, transaction, gas_price, sender_is_receiver, current_protocol_version)
        .map_err(|_| InvalidTxError::CostOverflow.into())
}

/// Verifies the signed transaction on top of given state, charges transaction fees
/// and balances, and updates the state for the used account and access keys.
pub fn verify_and_charge_transaction(
    config: &RuntimeConfig,
    state_update: &mut TrieUpdate,
    gas_price: Balance,
    signed_transaction: &SignedTransaction,
    verify_signature: bool,
    #[allow(unused)] block_height: Option<BlockHeight>,
    current_protocol_version: ProtocolVersion,
) -> Result<VerificationResult, RuntimeError> {
    let TransactionCost { gas_burnt, gas_remaining, receipt_gas_price, total_cost, burnt_amount } =
        validate_transaction(
            config,
            gas_price,
            signed_transaction,
            verify_signature,
            current_protocol_version,
        )?;
    let transaction = &signed_transaction.transaction;
    let signer_id = &transaction.signer_id;

    let mut signer = match get_account(state_update, signer_id)? {
        Some(signer) => signer,
        None => {
            return Err(InvalidTxError::SignerDoesNotExist { signer_id: signer_id.clone() }.into());
        }
    };
    let mut access_key = match get_access_key(state_update, signer_id, &transaction.public_key)? {
        Some(access_key) => access_key,
        None => {
            return Err(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::AccessKeyNotFound {
                    account_id: signer_id.clone(),
                    public_key: transaction.public_key.clone(),
                },
            )
            .into());
        }
    };

    if transaction.nonce <= access_key.nonce {
        return Err(InvalidTxError::InvalidNonce {
            tx_nonce: transaction.nonce,
            ak_nonce: access_key.nonce,
        }
        .into());
    }
    if checked_feature!("stable", AccessKeyNonceRange, current_protocol_version) {
        if let Some(height) = block_height {
            let upper_bound =
                height * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER;
            if transaction.nonce >= upper_bound {
                return Err(InvalidTxError::NonceTooLarge {
                    tx_nonce: transaction.nonce,
                    upper_bound,
                }
                .into());
            }
        }
    };

    access_key.nonce = transaction.nonce;

    signer.set_amount(signer.amount().checked_sub(total_cost).ok_or_else(|| {
        InvalidTxError::NotEnoughBalance {
            signer_id: signer_id.clone(),
            balance: signer.amount(),
            cost: total_cost,
        }
    })?);

    if let AccessKeyPermission::FunctionCall(ref mut function_call_permission) =
        access_key.permission
    {
        if let Some(ref mut allowance) = function_call_permission.allowance {
            *allowance = allowance.checked_sub(total_cost).ok_or_else(|| {
                InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::NotEnoughAllowance {
                    account_id: signer_id.clone(),
                    public_key: transaction.public_key.clone(),
                    allowance: *allowance,
                    cost: total_cost,
                })
            })?;
        }
    }

    match check_storage_stake(&signer, config, current_protocol_version) {
        Ok(()) => {}
        Err(StorageStakingError::LackBalanceForStorageStaking(amount)) => {
            return Err(InvalidTxError::LackBalanceForState {
                signer_id: signer_id.clone(),
                amount,
            }
            .into())
        }
        Err(StorageStakingError::StorageError(err)) => {
            return Err(RuntimeError::StorageError(StorageError::StorageInconsistentState(err)))
        }
    };

    if let AccessKeyPermission::FunctionCall(ref function_call_permission) = access_key.permission {
        if transaction.actions.len() != 1 {
            return Err(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::RequiresFullAccess,
            )
            .into());
        }
        if let Some(Action::FunctionCall(ref function_call)) = transaction.actions.get(0) {
            if function_call.deposit > 0 {
                return Err(InvalidTxError::InvalidAccessKeyError(
                    InvalidAccessKeyError::DepositWithFunctionCall,
                )
                .into());
            }
            if transaction.receiver_id.as_ref() != function_call_permission.receiver_id {
                return Err(InvalidTxError::InvalidAccessKeyError(
                    InvalidAccessKeyError::ReceiverMismatch {
                        tx_receiver: transaction.receiver_id.clone(),
                        ak_receiver: function_call_permission.receiver_id.clone(),
                    },
                )
                .into());
            }
            if !function_call_permission.method_names.is_empty()
                && function_call_permission
                    .method_names
                    .iter()
                    .all(|method_name| &function_call.method_name != method_name)
            {
                return Err(InvalidTxError::InvalidAccessKeyError(
                    InvalidAccessKeyError::MethodNameMismatch {
                        method_name: function_call.method_name.clone(),
                    },
                )
                .into());
            }
        } else {
            return Err(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::RequiresFullAccess,
            )
            .into());
        }
    };

    set_access_key(state_update, signer_id.clone(), transaction.public_key.clone(), &access_key);
    set_account(state_update, signer_id.clone(), &signer);

    Ok(VerificationResult { gas_burnt, gas_remaining, receipt_gas_price, burnt_amount })
}

/// Validates a given receipt. Checks validity of the Action or Data receipt.
pub(crate) fn validate_receipt(
    limit_config: &VMLimitConfig,
    receipt: &Receipt,
    current_protocol_version: ProtocolVersion,
) -> Result<(), ReceiptValidationError> {
    // We retain these checks here as to maintain backwards compatibility
    // with AccountId validation since we illegally parse an AccountId
    // in near-vm-logic/logic.rs#fn(VMLogic::read_and_parse_account_id)
    AccountId::validate(receipt.predecessor_id.as_ref()).map_err(|_| {
        ReceiptValidationError::InvalidPredecessorId {
            account_id: receipt.predecessor_id.to_string(),
        }
    })?;
    AccountId::validate(receipt.receiver_id.as_ref()).map_err(|_| {
        ReceiptValidationError::InvalidReceiverId { account_id: receipt.receiver_id.to_string() }
    })?;

    match &receipt.receipt {
        ReceiptEnum::Action(action_receipt) => {
            validate_action_receipt(limit_config, action_receipt, current_protocol_version)
        }
        ReceiptEnum::Data(data_receipt) => validate_data_receipt(limit_config, data_receipt),
    }
}

/// Validates given ActionReceipt. Checks validity of the number of input data dependencies and all actions.
fn validate_action_receipt(
    limit_config: &VMLimitConfig,
    receipt: &ActionReceipt,
    current_protocol_version: ProtocolVersion,
) -> Result<(), ReceiptValidationError> {
    if receipt.input_data_ids.len() as u64 > limit_config.max_number_input_data_dependencies {
        return Err(ReceiptValidationError::NumberInputDataDependenciesExceeded {
            number_of_input_data_dependencies: receipt.input_data_ids.len() as u64,
            limit: limit_config.max_number_input_data_dependencies,
        });
    }
    validate_actions(limit_config, &receipt.actions, current_protocol_version)
        .map_err(ReceiptValidationError::ActionsValidation)
}

/// Validates given data receipt. Checks validity of the length of the returned data.
fn validate_data_receipt(
    limit_config: &VMLimitConfig,
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
    limit_config: &VMLimitConfig,
    actions: &[Action],
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
                if !checked_feature!("stable", DelegateAction, current_protocol_version) {
                    return Err(ActionsValidationError::UnsupportedProtocolFeature {
                        protocol_feature: String::from("DelegateAction"),
                        version: ProtocolFeature::DelegateAction.protocol_version(),
                    });
                }
                if found_delegate_action {
                    return Err(ActionsValidationError::DelegateActionMustBeOnlyOne);
                }
                found_delegate_action = true;
            }
        }
        validate_action(limit_config, action, current_protocol_version)?;
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
    limit_config: &VMLimitConfig,
    action: &Action,
    current_protocol_version: ProtocolVersion,
) -> Result<(), ActionsValidationError> {
    match action {
        Action::CreateAccount(_) => Ok(()),
        Action::DeployContract(a) => validate_deploy_contract_action(limit_config, a),
        Action::FunctionCall(a) => validate_function_call_action(limit_config, a),
        Action::Transfer(_) => Ok(()),
        Action::Stake(a) => validate_stake_action(a),
        Action::AddKey(a) => validate_add_key_action(limit_config, a),
        Action::DeleteKey(_) => Ok(()),
        Action::DeleteAccount(a) => validate_delete_action(a),
        Action::Delegate(a) => validate_delegate_action(limit_config, a, current_protocol_version),
    }
}

fn validate_delegate_action(
    limit_config: &VMLimitConfig,
    signed_delegate_action: &SignedDelegateAction,
    current_protocol_version: ProtocolVersion,
) -> Result<(), ActionsValidationError> {
    let actions = signed_delegate_action.delegate_action.get_actions();
    validate_actions(limit_config, &actions, current_protocol_version)?;
    Ok(())
}

/// Validates `DeployContractAction`. Checks that the given contract size doesn't exceed the limit.
fn validate_deploy_contract_action(
    limit_config: &VMLimitConfig,
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

/// Validates `FunctionCallAction`. Checks that the method name length doesn't exceed the limit and
/// the length of the arguments doesn't exceed the limit.
fn validate_function_call_action(
    limit_config: &VMLimitConfig,
    action: &FunctionCallAction,
) -> Result<(), ActionsValidationError> {
    if action.gas == 0 {
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
            public_key: action.public_key.clone(),
        });
    }

    Ok(())
}

/// Validates `AddKeyAction`. If the access key permission is `FunctionCall`, checks that the
/// total number of bytes of the method names doesn't exceed the limit and
/// every method name length doesn't exceed the limit.
fn validate_add_key_action(
    limit_config: &VMLimitConfig,
    action: &AddKeyAction,
) -> Result<(), ActionsValidationError> {
    if let AccessKeyPermission::FunctionCall(fc) = &action.access_key.permission {
        // Check whether `receiver_id` is a valid account_id. Historically, we
        // allowed arbitrary strings there!
        match limit_config.account_id_validity_rules_version {
            near_vm_runner::logic::AccountIdValidityRulesVersion::V0 => (),
            near_vm_runner::logic::AccountIdValidityRulesVersion::V1 => {
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
    if AccountId::validate(&action.beneficiary_id).is_err() {
        return Err(ActionsValidationError::InvalidAccountId {
            account_id: action.beneficiary_id.to_string(),
        });
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
    check("привет", 3, "п");
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::near_primitives::borsh::BorshSerialize;
    use near_crypto::{InMemorySigner, KeyType, PublicKey, Signature, Signer};
    use near_primitives::account::{AccessKey, FunctionCallPermission};
    use near_primitives::delegate_action::{DelegateAction, NonDelegateAction};
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::test_utils::account_new;
    use near_primitives::transaction::{
        CreateAccountAction, DeleteAccountAction, DeleteKeyAction, StakeAction, TransferAction,
    };
    use near_primitives::types::{AccountId, Balance, MerkleHash, StateChangeCause};
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::test_utils::create_tries;
    use testlib::runtime_utils::{alice_account, bob_account, eve_dot_alice_account};

    use crate::near_primitives::shard_layout::ShardUId;

    use super::*;
    use crate::near_primitives::contract::ContractCode;
    use crate::near_primitives::trie_key::TrieKey;
    use near_store::{set, set_code};

    /// Initial balance used in tests.
    const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

    /// One NEAR, divisible by 10^24.
    const NEAR_BASE: Balance = 1_000_000_000_000_000_000_000_000;

    fn setup_common(
        initial_balance: Balance,
        initial_locked: Balance,
        access_key: Option<AccessKey>,
    ) -> (Arc<InMemorySigner>, TrieUpdate, Balance) {
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
    ) -> (Arc<InMemorySigner>, TrieUpdate, Balance) {
        let tries = create_tries();
        let root = MerkleHash::default();

        let account_id = alice_account();
        let signer = Arc::new(InMemorySigner::from_seed(
            account_id.clone(),
            KeyType::ED25519,
            account_id.as_ref(),
        ));

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
                            public_key.try_to_vec().unwrap().len() as u64
                                + access_key.try_to_vec().unwrap().len() as u64
                                + 40, // storage_config.num_extra_bytes_record,
                        )
                        .unwrap(),
                );
                key_count += 1;
            }
            if has_contract {
                let code = vec![0; 100];
                let code_hash = hash(&code);
                set_code(
                    &mut initial_state,
                    account_id.clone(),
                    &ContractCode::new(code.clone(), Some(code_hash)),
                );
                initial_account.set_code_hash(code_hash);
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
        let trie_changes = initial_state.finalize().unwrap().1;
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit().unwrap();

        (signer, tries.new_trie_update(ShardUId::single_shard(), root), 100)
    }

    fn assert_err_both_validations(
        config: &RuntimeConfig,
        state_update: &mut TrieUpdate,
        gas_price: Balance,
        signed_transaction: &SignedTransaction,
        expected_err: RuntimeError,
    ) {
        assert_eq!(
            validate_transaction(config, gas_price, signed_transaction, true, PROTOCOL_VERSION)
                .expect_err("expected an error"),
            expected_err,
        );
        assert_eq!(
            verify_and_charge_transaction(
                config,
                state_update,
                gas_price,
                signed_transaction,
                true,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            expected_err,
        );
    }

    mod zero_balance_account_tests {
        use crate::near_primitives::account::id::AccountId;
        use crate::near_primitives::account::{
            AccessKeyPermission, Account, FunctionCallPermission,
        };
        use crate::verifier::tests::{setup_accounts, TESTING_INIT_BALANCE};
        use crate::verifier::{is_zero_balance_account, ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT};
        use near_primitives::account::AccessKey;
        use near_store::{get_account, TrieUpdate};
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
                        allowance: Some(100),
                        receiver_id: "a".repeat(64),
                        method_names: vec![],
                    }),
                };
                access_keys.push(access_key);
            }
            let (_, state_update, _) = setup_accounts(vec![(
                account_id.clone(),
                TESTING_INIT_BALANCE,
                0,
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
                0,
                0,
                vec![AccessKey {
                    nonce: 0,
                    permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                        allowance: Some(100),
                        receiver_id: bob_account().into(),
                        method_names: method_names,
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
            setup_common(TESTING_INIT_BALANCE, 0, Some(AccessKey::full_access()));

        let deposit = 100;
        let transaction = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            deposit,
            CryptoHash::default(),
        );
        validate_transaction(&config, gas_price, &transaction, true, PROTOCOL_VERSION)
            .expect("valid transaction");
        let verification_result = verify_and_charge_transaction(
            &config,
            &mut state_update,
            gas_price,
            &transaction,
            true,
            None,
            PROTOCOL_VERSION,
        )
        .expect("valid transaction");
        // Should not be free. Burning for sending
        assert!(verification_result.gas_burnt > 0);
        // All burned gas goes to the validators at current gas price
        assert_eq!(
            verification_result.burnt_amount,
            Balance::from(verification_result.gas_burnt) * gas_price
        );

        let account = get_account(&state_update, &alice_account()).unwrap().unwrap();
        // Balance is decreased by the (TX fees + transfer balance).
        assert_eq!(
            account.amount(),
            TESTING_INIT_BALANCE
                - Balance::from(verification_result.gas_remaining)
                    * verification_result.receipt_gas_price
                - verification_result.burnt_amount
                - deposit
        );

        let access_key =
            get_access_key(&state_update, &alice_account(), &signer.public_key()).unwrap().unwrap();
        assert_eq!(access_key.nonce, 1);
    }

    #[test]
    fn test_validate_transaction_invalid_signature() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, 0, Some(AccessKey::full_access()));

        let mut tx = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            100,
            CryptoHash::default(),
        );
        tx.signature = signer.sign(CryptoHash::default().as_ref());

        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            &tx,
            RuntimeError::InvalidTxError(InvalidTxError::InvalidSignature),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_access_key_not_found() {
        let config = RuntimeConfig::test();
        let (bad_signer, mut state_update, gas_price) = setup_common(TESTING_INIT_BALANCE, 0, None);

        assert_eq!(
            verify_and_charge_transaction(
                &config,
                &mut state_update,
                gas_price,
                &SignedTransaction::send_money(
                    1,
                    alice_account(),
                    bob_account(),
                    &*bad_signer,
                    100,
                    CryptoHash::default(),
                ),
                false,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            RuntimeError::InvalidTxError(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::AccessKeyNotFound {
                    account_id: alice_account(),
                    public_key: bad_signer.public_key(),
                },
            )),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_bad_action() {
        let mut config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, 0, Some(AccessKey::full_access()));

        config.wasm_config.limit_config.max_total_prepaid_gas = 100;

        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            &SignedTransaction::from_actions(
                1,
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::FunctionCall(FunctionCallAction {
                    method_name: "hello".to_string(),
                    args: b"abc".to_vec(),
                    gas: 200,
                    deposit: 0,
                })],
                CryptoHash::default(),
            ),
            RuntimeError::InvalidTxError(InvalidTxError::ActionsValidation(
                ActionsValidationError::TotalPrepaidGasExceeded {
                    total_prepaid_gas: 200,
                    limit: 100,
                },
            )),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_bad_signer() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, 0, Some(AccessKey::full_access()));

        assert_eq!(
            verify_and_charge_transaction(
                &config,
                &mut state_update,
                gas_price,
                &SignedTransaction::send_money(
                    1,
                    bob_account(),
                    alice_account(),
                    &*signer,
                    100,
                    CryptoHash::default(),
                ),
                true,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            RuntimeError::InvalidTxError(InvalidTxError::SignerDoesNotExist {
                signer_id: bob_account()
            }),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_bad_nonce() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            0,
            Some(AccessKey { nonce: 2, permission: AccessKeyPermission::FullAccess }),
        );

        assert_eq!(
            verify_and_charge_transaction(
                &config,
                &mut state_update,
                gas_price,
                &SignedTransaction::send_money(
                    1,
                    alice_account(),
                    bob_account(),
                    &*signer,
                    100,
                    CryptoHash::default(),
                ),
                true,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            RuntimeError::InvalidTxError(InvalidTxError::InvalidNonce { tx_nonce: 1, ak_nonce: 2 }),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_balance_overflow() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, 0, Some(AccessKey::full_access()));

        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            &SignedTransaction::send_money(
                1,
                alice_account(),
                bob_account(),
                &*signer,
                u128::max_value(),
                CryptoHash::default(),
            ),
            RuntimeError::InvalidTxError(InvalidTxError::CostOverflow),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_not_enough_balance() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, 0, Some(AccessKey::full_access()));

        let err = verify_and_charge_transaction(
            &config,
            &mut state_update,
            gas_price,
            &SignedTransaction::send_money(
                1,
                alice_account(),
                bob_account(),
                &*signer,
                TESTING_INIT_BALANCE,
                CryptoHash::default(),
            ),
            true,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        if let RuntimeError::InvalidTxError(InvalidTxError::NotEnoughBalance {
            signer_id,
            balance,
            cost,
        }) = err
        {
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
            0,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: Some(100),
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        let err = verify_and_charge_transaction(
            &config,
            &mut state_update,
            gas_price,
            &SignedTransaction::from_actions(
                1,
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::FunctionCall(FunctionCallAction {
                    method_name: "hello".to_string(),
                    args: b"abc".to_vec(),
                    gas: 300,
                    deposit: 0,
                })],
                CryptoHash::default(),
            ),
            true,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        if let RuntimeError::InvalidTxError(InvalidTxError::InvalidAccessKeyError(
            InvalidAccessKeyError::NotEnoughAllowance { account_id, public_key, allowance, cost },
        )) = err
        {
            assert_eq!(account_id, alice_account());
            assert_eq!(public_key, signer.public_key());
            assert_eq!(allowance, 100);
            assert!(cost > allowance);
        } else {
            panic!("Incorrect error");
        }
    }

    /// Setup: account has 1B yoctoN and is 180 bytes. Storage requirement is 1M per byte.
    /// Test that such account can not send 950M yoctoN out as that will leave it under storage requirements.
    /// If zero balance account is enabled, however, the transaction should succeed
    #[test]
    fn test_validate_transaction_invalid_low_balance() {
        let mut config = RuntimeConfig::free();
        config.fees.storage_usage_config.storage_amount_per_byte = 10_000_000;
        let initial_balance = 1_000_000_000;
        let transfer_amount = 950_000_000;
        let (signer, mut state_update, gas_price) =
            setup_common(initial_balance, 0, Some(AccessKey::full_access()));

        let res = verify_and_charge_transaction(
            &config,
            &mut state_update,
            gas_price,
            &SignedTransaction::send_money(
                1,
                alice_account(),
                bob_account(),
                &*signer,
                transfer_amount,
                CryptoHash::default(),
            ),
            true,
            None,
            PROTOCOL_VERSION,
        );
        let verification_result = res.unwrap();
        assert_eq!(verification_result.gas_burnt, 0);
        assert_eq!(verification_result.gas_remaining, 0);
        assert_eq!(verification_result.burnt_amount, 0);
    }

    #[test]
    fn test_validate_transaction_invalid_low_balance_many_keys() {
        let mut config = RuntimeConfig::free();
        config.fees.storage_usage_config.storage_amount_per_byte = 10_000_000;
        let initial_balance = 1_000_000_000;
        let transfer_amount = 950_000_000;
        let account_id = alice_account();
        let access_keys = vec![AccessKey::full_access(); 10];
        let (signer, mut state_update, gas_price) = setup_accounts(vec![(
            account_id.clone(),
            initial_balance,
            0,
            access_keys,
            false,
            false,
        )]);

        let res = verify_and_charge_transaction(
            &config,
            &mut state_update,
            gas_price,
            &SignedTransaction::send_money(
                1,
                account_id.clone(),
                bob_account(),
                &*signer,
                transfer_amount,
                CryptoHash::default(),
            ),
            true,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        let account = get_account(&state_update, &account_id).unwrap().unwrap();

        assert_eq!(
            res,
            RuntimeError::InvalidTxError(InvalidTxError::LackBalanceForState {
                signer_id: account_id,
                amount: Balance::from(account.storage_usage()) * config.storage_amount_per_byte()
                    - (initial_balance - transfer_amount)
            })
        );
    }

    #[test]
    fn test_validate_transaction_invalid_actions_for_function_call() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            0,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        assert_eq!(
            verify_and_charge_transaction(
                &config,
                &mut state_update,
                gas_price,
                &SignedTransaction::from_actions(
                    1,
                    alice_account(),
                    bob_account(),
                    &*signer,
                    vec![
                        Action::FunctionCall(FunctionCallAction {
                            method_name: "hello".to_string(),
                            args: b"abc".to_vec(),
                            gas: 100,
                            deposit: 0,
                        }),
                        Action::CreateAccount(CreateAccountAction {})
                    ],
                    CryptoHash::default(),
                ),
                true,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            RuntimeError::InvalidTxError(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::RequiresFullAccess
            )),
        );

        assert_eq!(
            verify_and_charge_transaction(
                &config,
                &mut state_update,
                gas_price,
                &SignedTransaction::from_actions(
                    1,
                    alice_account(),
                    bob_account(),
                    &*signer,
                    vec![],
                    CryptoHash::default(),
                ),
                true,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            RuntimeError::InvalidTxError(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::RequiresFullAccess
            )),
        );

        assert_eq!(
            verify_and_charge_transaction(
                &config,
                &mut state_update,
                gas_price,
                &SignedTransaction::from_actions(
                    1,
                    alice_account(),
                    bob_account(),
                    &*signer,
                    vec![Action::CreateAccount(CreateAccountAction {})],
                    CryptoHash::default(),
                ),
                true,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            RuntimeError::InvalidTxError(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::RequiresFullAccess
            )),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_receiver_for_function_call() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            0,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        assert_eq!(
            verify_and_charge_transaction(
                &config,
                &mut state_update,
                gas_price,
                &SignedTransaction::from_actions(
                    1,
                    alice_account(),
                    eve_dot_alice_account(),
                    &*signer,
                    vec![Action::FunctionCall(FunctionCallAction {
                        method_name: "hello".to_string(),
                        args: b"abc".to_vec(),
                        gas: 100,
                        deposit: 0,
                    }),],
                    CryptoHash::default(),
                ),
                true,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            RuntimeError::InvalidTxError(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::ReceiverMismatch {
                    tx_receiver: eve_dot_alice_account(),
                    ak_receiver: bob_account().into()
                }
            )),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_method_name_for_function_call() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            0,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec!["not_hello".to_string(), "world".to_string()],
                }),
            }),
        );

        assert_eq!(
            verify_and_charge_transaction(
                &config,
                &mut state_update,
                gas_price,
                &SignedTransaction::from_actions(
                    1,
                    alice_account(),
                    bob_account(),
                    &*signer,
                    vec![Action::FunctionCall(FunctionCallAction {
                        method_name: "hello".to_string(),
                        args: b"abc".to_vec(),
                        gas: 100,
                        deposit: 0,
                    }),],
                    CryptoHash::default(),
                ),
                true,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            RuntimeError::InvalidTxError(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::MethodNameMismatch { method_name: "hello".to_string() }
            )),
        );
    }

    #[test]
    fn test_validate_transaction_deposit_with_function_call() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            0,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        assert_eq!(
            verify_and_charge_transaction(
                &config,
                &mut state_update,
                gas_price,
                &SignedTransaction::from_actions(
                    1,
                    alice_account(),
                    bob_account(),
                    &*signer,
                    vec![Action::FunctionCall(FunctionCallAction {
                        method_name: "hello".to_string(),
                        args: b"abc".to_vec(),
                        gas: 100,
                        deposit: 100,
                    }),],
                    CryptoHash::default(),
                ),
                true,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            RuntimeError::InvalidTxError(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::DepositWithFunctionCall,
            )),
        );
    }

    #[test]
    fn test_validate_transaction_exceeding_tx_size_limit() {
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, 0, Some(AccessKey::full_access()));

        let transaction = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::DeployContract(DeployContractAction { code: vec![1; 5] })],
            CryptoHash::default(),
        );
        let transaction_size = transaction.get_size();

        let mut config = RuntimeConfig::test();
        let max_transaction_size = transaction_size - 1;
        config.wasm_config.limit_config.max_transaction_size = transaction_size - 1;

        assert_eq!(
            verify_and_charge_transaction(
                &config,
                &mut state_update,
                gas_price,
                &transaction,
                false,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            RuntimeError::InvalidTxError(InvalidTxError::TransactionSizeExceeded {
                size: transaction_size,
                limit: max_transaction_size
            }),
        );

        config.wasm_config.limit_config.max_transaction_size = transaction_size + 1;
        verify_and_charge_transaction(
            &config,
            &mut state_update,
            gas_price,
            &transaction,
            false,
            None,
            PROTOCOL_VERSION,
        )
        .expect("valid transaction");
    }

    // Receipts

    #[test]
    fn test_validate_receipt_valid() {
        let limit_config = VMLimitConfig::test();
        validate_receipt(
            &limit_config,
            &Receipt::new_balance_refund(&alice_account(), 10),
            PROTOCOL_VERSION,
        )
        .expect("valid receipt");
    }

    #[test]
    fn test_validate_action_receipt_too_many_input_deps() {
        let mut limit_config = VMLimitConfig::test();
        limit_config.max_number_input_data_dependencies = 1;
        assert_eq!(
            validate_action_receipt(
                &limit_config,
                &ActionReceipt {
                    signer_id: alice_account(),
                    signer_public_key: PublicKey::empty(KeyType::ED25519),
                    gas_price: 100,
                    output_data_receivers: vec![],
                    input_data_ids: vec![CryptoHash::default(), CryptoHash::default()],
                    actions: vec![]
                },
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
        let limit_config = VMLimitConfig::test();
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
        let mut limit_config = VMLimitConfig::test();
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
        let limit_config = VMLimitConfig::test();
        validate_actions(&limit_config, &[], PROTOCOL_VERSION).expect("empty actions");
    }

    #[test]
    fn test_validate_actions_valid_function_call() {
        let limit_config = VMLimitConfig::test();
        validate_actions(
            &limit_config,
            &[Action::FunctionCall(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: 100,
                deposit: 0,
            })],
            PROTOCOL_VERSION,
        )
        .expect("valid function call action");
    }

    #[test]
    fn test_validate_actions_too_much_gas() {
        let mut limit_config = VMLimitConfig::test();
        limit_config.max_total_prepaid_gas = 220;
        assert_eq!(
            validate_actions(
                &limit_config,
                &[
                    Action::FunctionCall(FunctionCallAction {
                        method_name: "hello".to_string(),
                        args: b"abc".to_vec(),
                        gas: 100,
                        deposit: 0,
                    }),
                    Action::FunctionCall(FunctionCallAction {
                        method_name: "hello".to_string(),
                        args: b"abc".to_vec(),
                        gas: 150,
                        deposit: 0,
                    })
                ],
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            ActionsValidationError::TotalPrepaidGasExceeded { total_prepaid_gas: 250, limit: 220 }
        );
    }

    #[test]
    fn test_validate_actions_gas_overflow() {
        let mut limit_config = VMLimitConfig::test();
        limit_config.max_total_prepaid_gas = 220;
        assert_eq!(
            validate_actions(
                &limit_config,
                &[
                    Action::FunctionCall(FunctionCallAction {
                        method_name: "hello".to_string(),
                        args: b"abc".to_vec(),
                        gas: u64::max_value() / 2 + 1,
                        deposit: 0,
                    }),
                    Action::FunctionCall(FunctionCallAction {
                        method_name: "hello".to_string(),
                        args: b"abc".to_vec(),
                        gas: u64::max_value() / 2 + 1,
                        deposit: 0,
                    })
                ],
                PROTOCOL_VERSION,
            )
            .expect_err("Expected an error"),
            ActionsValidationError::IntegerOverflow,
        );
    }

    #[test]
    fn test_validate_actions_num_actions() {
        let mut limit_config = VMLimitConfig::test();
        limit_config.max_actions_per_receipt = 1;
        assert_eq!(
            validate_actions(
                &limit_config,
                &[
                    Action::CreateAccount(CreateAccountAction {}),
                    Action::CreateAccount(CreateAccountAction {}),
                ],
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
        let mut limit_config = VMLimitConfig::test();
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
                PROTOCOL_VERSION,
            )
            .expect_err("Expected an error"),
            ActionsValidationError::DeleteActionMustBeFinal,
        );
    }

    #[test]
    fn test_validate_delete_must_work_if_its_final() {
        let mut limit_config = VMLimitConfig::test();
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
                PROTOCOL_VERSION,
            ),
            Ok(()),
        );
    }

    // Individual actions

    #[test]
    fn test_validate_action_valid_create_account() {
        validate_action(
            &VMLimitConfig::test(),
            &Action::CreateAccount(CreateAccountAction {}),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_function_call() {
        validate_action(
            &VMLimitConfig::test(),
            &Action::FunctionCall(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: 100,
                deposit: 0,
            }),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_invalid_function_call_zero_gas() {
        assert_eq!(
            validate_action(
                &VMLimitConfig::test(),
                &Action::FunctionCall(FunctionCallAction {
                    method_name: "new".to_string(),
                    args: vec![],
                    gas: 0,
                    deposit: 0,
                }),
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            ActionsValidationError::FunctionCallZeroAttachedGas,
        );
    }

    #[test]
    fn test_validate_action_valid_transfer() {
        validate_action(
            &VMLimitConfig::test(),
            &Action::Transfer(TransferAction { deposit: 10 }),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_stake() {
        validate_action(
            &VMLimitConfig::test(),
            &Action::Stake(StakeAction {
                stake: 100,
                public_key: "ed25519:KuTCtARNzxZQ3YvXDeLjx83FDqxv2SdQTSbiq876zR7".parse().unwrap(),
            }),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_invalid_staking_key() {
        assert_eq!(
            validate_action(
                &VMLimitConfig::test(),
                &Action::Stake(StakeAction {
                    stake: 100,
                    public_key: PublicKey::empty(KeyType::ED25519),
                }),
                PROTOCOL_VERSION,
            )
            .expect_err("Expected an error"),
            ActionsValidationError::UnsuitableStakingKey {
                public_key: PublicKey::empty(KeyType::ED25519),
            },
        );
    }

    #[test]
    fn test_validate_action_valid_add_key_full_permission() {
        validate_action(
            &VMLimitConfig::test(),
            &Action::AddKey(AddKeyAction {
                public_key: PublicKey::empty(KeyType::ED25519),
                access_key: AccessKey::full_access(),
            }),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_add_key_function_call() {
        validate_action(
            &VMLimitConfig::test(),
            &Action::AddKey(AddKeyAction {
                public_key: PublicKey::empty(KeyType::ED25519),
                access_key: AccessKey {
                    nonce: 0,
                    permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                        allowance: Some(1000),
                        receiver_id: alice_account().into(),
                        method_names: vec!["hello".to_string(), "world".to_string()],
                    }),
                },
            }),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_delete_key() {
        validate_action(
            &VMLimitConfig::test(),
            &Action::DeleteKey(DeleteKeyAction { public_key: PublicKey::empty(KeyType::ED25519) }),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_delete_account() {
        validate_action(
            &VMLimitConfig::test(),
            &Action::DeleteAccount(DeleteAccountAction { beneficiary_id: alice_account() }),
            PROTOCOL_VERSION,
        )
        .expect("valid action");
    }

    #[test]
    fn test_delegate_action_must_be_only_one() {
        let signed_delegate_action = SignedDelegateAction {
            delegate_action: DelegateAction {
                sender_id: "bob.test.near".parse().unwrap(),
                receiver_id: "token.test.near".parse().unwrap(),
                actions: vec![NonDelegateAction::try_from(Action::CreateAccount(
                    CreateAccountAction {},
                ))
                .unwrap()],
                nonce: 19000001,
                max_block_height: 57,
                public_key: PublicKey::empty(KeyType::ED25519),
            },
            signature: Signature::default(),
        };
        assert_eq!(
            validate_actions(
                &VMLimitConfig::test(),
                &[
                    Action::Delegate(signed_delegate_action.clone()),
                    Action::Delegate(signed_delegate_action.clone()),
                ],
                PROTOCOL_VERSION,
            ),
            Err(ActionsValidationError::DelegateActionMustBeOnlyOne),
        );
        assert_eq!(
            validate_actions(
                &&VMLimitConfig::test(),
                &[Action::Delegate(signed_delegate_action.clone()),],
                PROTOCOL_VERSION,
            ),
            Ok(()),
        );
        assert_eq!(
            validate_actions(
                &VMLimitConfig::test(),
                &[
                    Action::CreateAccount(CreateAccountAction {}),
                    Action::Delegate(signed_delegate_action),
                ],
                PROTOCOL_VERSION,
            ),
            Ok(()),
        );
    }
}
