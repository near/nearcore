use near_crypto::key_conversion::is_valid_staking_key;
use near_primitives::runtime::get_insufficient_storage_stake;
use near_primitives::{
    account::AccessKeyPermission,
    config::VMLimitConfig,
    errors::{
        ActionsValidationError, InvalidAccessKeyError, InvalidTxError, ReceiptValidationError,
        RuntimeError,
    },
    receipt::{ActionReceipt, DataReceipt, Receipt, ReceiptEnum},
    transaction::{
        Action, AddKeyAction, DeleteAccountAction, DeployContractAction, FunctionCallAction,
        SignedTransaction, StakeAction,
    },
    types::Balance,
    version::ProtocolVersion,
};
use near_runtime_utils::is_valid_account_id;
use near_store::{
    get_access_key, get_account, set_access_key, set_account, StorageError, TrieUpdate,
};

use crate::config::{total_prepaid_gas, tx_cost, TransactionCost};
use crate::VerificationResult;
use near_primitives::checked_feature;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::types::BlockHeight;

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
    if !is_valid_account_id(&signer_id) {
        return Err(InvalidTxError::InvalidSignerId { signer_id: signer_id.clone() }.into());
    }
    if !is_valid_account_id(&transaction.receiver_id) {
        return Err(InvalidTxError::InvalidReceiverId {
            receiver_id: transaction.receiver_id.clone(),
        }
        .into());
    }

    if verify_signature
        && !signed_transaction
            .signature
            .verify(signed_transaction.get_hash().as_ref(), &transaction.public_key)
    {
        return Err(InvalidTxError::InvalidSignature.into());
    }

    #[cfg(feature = "protocol_feature_tx_size_limit")]
    {
        let transaction_size = signed_transaction.get_size();
        let max_transaction_size = config.wasm_config.limit_config.max_transaction_size;
        if transaction_size > max_transaction_size {
            return Err(InvalidTxError::TransactionSizeExceeded {
                size: transaction_size,
                limit: max_transaction_size,
            }
            .into());
        }
    }

    validate_actions(&config.wasm_config.limit_config, &transaction.actions)
        .map_err(|e| InvalidTxError::ActionsValidation(e))?;

    let sender_is_receiver = &transaction.receiver_id == signer_id;

    tx_cost(
        &config.transaction_costs,
        &transaction,
        gas_price,
        sender_is_receiver,
        current_protocol_version,
    )
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
    let mut access_key = match get_access_key(state_update, &signer_id, &transaction.public_key)? {
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

    match get_insufficient_storage_stake(&signer, &config) {
        Ok(None) => {}
        Ok(Some(amount)) => {
            return Err(InvalidTxError::LackBalanceForState {
                signer_id: signer_id.clone(),
                amount,
            }
            .into())
        }
        Err(err) => {
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
            if transaction.receiver_id != function_call_permission.receiver_id {
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

/// Validates a given receipt. Checks validity of the predecessor and receiver account IDs and
/// the validity of the Action or Data receipt.
pub(crate) fn validate_receipt(
    limit_config: &VMLimitConfig,
    receipt: &Receipt,
) -> Result<(), ReceiptValidationError> {
    if !is_valid_account_id(&receipt.predecessor_id) {
        return Err(ReceiptValidationError::InvalidPredecessorId {
            account_id: receipt.predecessor_id.clone(),
        });
    }
    if !is_valid_account_id(&receipt.receiver_id) {
        return Err(ReceiptValidationError::InvalidReceiverId {
            account_id: receipt.receiver_id.clone(),
        });
    }
    match &receipt.receipt {
        ReceiptEnum::Action(action_receipt) => {
            validate_action_receipt(limit_config, action_receipt)
        }
        ReceiptEnum::Data(data_receipt) => validate_data_receipt(limit_config, data_receipt),
    }
}

/// Validates given ActionReceipt. Checks validity of the signer account ID, validity of all
/// data receiver account IDs, the number of input data dependencies and all actions.
fn validate_action_receipt(
    limit_config: &VMLimitConfig,
    receipt: &ActionReceipt,
) -> Result<(), ReceiptValidationError> {
    if !is_valid_account_id(&receipt.signer_id) {
        return Err(ReceiptValidationError::InvalidSignerId {
            account_id: receipt.signer_id.clone(),
        });
    }
    for data_receiver in &receipt.output_data_receivers {
        if !is_valid_account_id(&data_receiver.receiver_id) {
            return Err(ReceiptValidationError::InvalidDataReceiverId {
                account_id: data_receiver.receiver_id.clone(),
            });
        }
    }
    if receipt.input_data_ids.len() as u64 > limit_config.max_number_input_data_dependencies {
        return Err(ReceiptValidationError::NumberInputDataDependenciesExceeded {
            number_of_input_data_dependencies: receipt.input_data_ids.len() as u64,
            limit: limit_config.max_number_input_data_dependencies,
        });
    }
    validate_actions(limit_config, &receipt.actions)
        .map_err(|e| ReceiptValidationError::ActionsValidation(e))
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

/// Validates given actions. Checks limits and validates `account_id` if applicable.
/// Checks that the total number of actions doesn't exceed the limit.
/// Validates each individual action.
/// Checks that the total prepaid gas doesn't exceed the limit.
pub(crate) fn validate_actions(
    limit_config: &VMLimitConfig,
    actions: &[Action],
) -> Result<(), ActionsValidationError> {
    if actions.len() as u64 > limit_config.max_actions_per_receipt {
        return Err(ActionsValidationError::TotalNumberOfActionsExceeded {
            total_number_of_actions: actions.len() as u64,
            limit: limit_config.max_actions_per_receipt,
        });
    }

    let mut iter = actions.iter().peekable();
    while let Some(action) = iter.next() {
        if let Action::DeleteAccount(_) = action {
            if iter.peek().is_some() {
                return Err(ActionsValidationError::DeleteActionMustBeFinal);
            }
        }
        validate_action(limit_config, action)?;
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

/// Validates a single given action. Checks limits and validates `account_id` if applicable.
pub fn validate_action(
    limit_config: &VMLimitConfig,
    action: &Action,
) -> Result<(), ActionsValidationError> {
    match action {
        Action::CreateAccount(_) => Ok(()),
        Action::DeployContract(a) => validate_deploy_contract_action(limit_config, a),
        Action::FunctionCall(a) => validate_function_call_action(limit_config, a),
        Action::Transfer(_) => Ok(()),
        Action::Stake(a) => validate_stake_action(a),
        Action::AddKey(a) => validate_add_key_action(limit_config, a),
        Action::DeleteKey(_) => Ok(()),
        Action::DeleteAccount(a) => validate_delete_account_action(a),
    }
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

/// Validates `AddKeyAction`. If the access key permission is `FunctionCall` checks that the
/// `receiver_id` is a valid account ID, checks the total number of bytes of the method names
/// doesn't exceed the limit and every method name length doesn't exceed the limit.
fn validate_add_key_action(
    limit_config: &VMLimitConfig,
    action: &AddKeyAction,
) -> Result<(), ActionsValidationError> {
    if let AccessKeyPermission::FunctionCall(fc) = &action.access_key.permission {
        if !is_valid_account_id(&fc.receiver_id) {
            return Err(ActionsValidationError::InvalidAccountId {
                account_id: fc.receiver_id.clone(),
            });
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

/// Validates `DeleteAccountAction`. Checks that the `beneficiary_id` is a valid account ID.
fn validate_delete_account_action(
    action: &DeleteAccountAction,
) -> Result<(), ActionsValidationError> {
    if !is_valid_account_id(&action.beneficiary_id) {
        return Err(ActionsValidationError::InvalidAccountId {
            account_id: action.beneficiary_id.clone(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
    use near_primitives::account::{AccessKey, Account, FunctionCallPermission};
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::receipt::DataReceiver;
    use near_primitives::test_utils::account_new;
    use near_primitives::transaction::{
        CreateAccountAction, DeleteKeyAction, StakeAction, TransferAction,
    };
    use near_primitives::types::{AccountId, Balance, MerkleHash, StateChangeCause};
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::test_utils::create_tries;
    use testlib::runtime_utils::{alice_account, bob_account, eve_dot_alice_account};

    use super::*;

    /// Initial balance used in tests.
    const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

    /// One NEAR, divisible by 10^24.
    const NEAR_BASE: Balance = 1_000_000_000_000_000_000_000_000;

    fn setup_common(
        initial_balance: Balance,
        initial_locked: Balance,
        access_key: Option<AccessKey>,
    ) -> (Arc<InMemorySigner>, TrieUpdate, Balance) {
        setup_accounts(vec![(alice_account(), initial_balance, initial_locked, access_key)])
    }

    fn setup_accounts(
        accounts: Vec<(AccountId, Balance, Balance, Option<AccessKey>)>,
    ) -> (Arc<InMemorySigner>, TrieUpdate, Balance) {
        let tries = create_tries();
        let root = MerkleHash::default();

        let account_id = alice_account();
        let signer =
            Arc::new(InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id));

        let mut initial_state = tries.new_trie_update(0, root);
        for (account_id, initial_balance, initial_locked, access_key) in accounts {
            let mut initial_account = account_new(initial_balance, hash(&[]));
            initial_account.set_locked(initial_locked);
            set_account(&mut initial_state, account_id.clone(), &initial_account);
            if let Some(access_key) = access_key {
                set_access_key(
                    &mut initial_state,
                    account_id.clone(),
                    signer.public_key(),
                    &access_key,
                );
            }
        }
        initial_state.commit(StateChangeCause::InitialState);
        let trie_changes = initial_state.finalize().unwrap().0;
        let (store_update, root) = tries.apply_all(&trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        (signer, tries.new_trie_update(0, root), 100)
    }

    fn assert_err_both_validations(
        config: &RuntimeConfig,
        state_update: &mut TrieUpdate,
        gas_price: Balance,
        signed_transaction: &SignedTransaction,
        expected_err: RuntimeError,
    ) {
        assert_eq!(
            validate_transaction(&config, gas_price, &signed_transaction, true, PROTOCOL_VERSION)
                .expect_err("expected an error"),
            expected_err,
        );
        assert_eq!(
            verify_and_charge_transaction(
                &config,
                state_update,
                gas_price,
                &signed_transaction,
                true,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            expected_err,
        );
    }

    // Transactions

    #[test]
    fn test_validate_transaction_valid() {
        let config = RuntimeConfig::default();
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
    fn test_validate_transaction_invalid_signer_id() {
        let config = RuntimeConfig::default();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, 0, Some(AccessKey::full_access()));

        let invalid_account_id = "WHAT?".to_string();
        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            &SignedTransaction::send_money(
                1,
                invalid_account_id.clone(),
                bob_account(),
                &*signer,
                100,
                CryptoHash::default(),
            ),
            RuntimeError::InvalidTxError(InvalidTxError::InvalidSignerId {
                signer_id: invalid_account_id.clone(),
            }),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_receiver_id() {
        let config = RuntimeConfig::default();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, 0, Some(AccessKey::full_access()));

        let invalid_account_id = "WHAT?".to_string();
        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            &SignedTransaction::send_money(
                1,
                alice_account(),
                invalid_account_id.clone(),
                &*signer,
                100,
                CryptoHash::default(),
            ),
            RuntimeError::InvalidTxError(InvalidTxError::InvalidReceiverId {
                receiver_id: invalid_account_id,
            }),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_signature() {
        let config = RuntimeConfig::default();
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
        let config = RuntimeConfig::default();
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
        let mut config = RuntimeConfig::default();
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
        let config = RuntimeConfig::default();
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
        let config = RuntimeConfig::default();
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
        let config = RuntimeConfig::default();
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
        let config = RuntimeConfig::default();
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
        let config = RuntimeConfig::default();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            0,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: Some(100),
                    receiver_id: bob_account(),
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
    #[test]
    fn test_validate_transaction_invalid_low_balance() {
        let mut config = RuntimeConfig::free();
        config.storage_amount_per_byte = 10_000_000;
        let initial_balance = 1_000_000_000;
        let transfer_amount = 950_000_000;
        let (signer, mut state_update, gas_price) =
            setup_common(initial_balance, 0, Some(AccessKey::full_access()));

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
                    transfer_amount,
                    CryptoHash::default(),
                ),
                true,
                None,
                PROTOCOL_VERSION,
            )
            .expect_err("expected an error"),
            RuntimeError::InvalidTxError(InvalidTxError::LackBalanceForState {
                signer_id: alice_account(),
                amount: Balance::from(std::mem::size_of::<Account>() as u64)
                    * config.storage_amount_per_byte
                    - (initial_balance - transfer_amount)
            })
        );
    }

    #[test]
    fn test_validate_transaction_invalid_actions_for_function_call() {
        let config = RuntimeConfig::default();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            0,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account(),
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
        let config = RuntimeConfig::default();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            0,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account(),
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
                    ak_receiver: bob_account()
                }
            )),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_method_name_for_function_call() {
        let config = RuntimeConfig::default();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            0,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account(),
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
        let config = RuntimeConfig::default();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            0,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account(),
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
    #[cfg(feature = "protocol_feature_tx_size_limit")]
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

        let mut config = RuntimeConfig::default();
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
        let limit_config = VMLimitConfig::default();
        validate_receipt(&limit_config, &Receipt::new_balance_refund(&alice_account(), 10))
            .expect("valid receipt");
    }

    #[test]
    fn test_validate_receipt_incorrect_predecessor_id() {
        let limit_config = VMLimitConfig::default();
        let invalid_account_id = "WHAT?".to_string();
        let mut receipt = Receipt::new_balance_refund(&alice_account(), 10);
        receipt.predecessor_id = invalid_account_id.clone();
        assert_eq!(
            validate_receipt(&limit_config, &receipt).expect_err("expected an error"),
            ReceiptValidationError::InvalidPredecessorId { account_id: invalid_account_id }
        );
    }

    #[test]
    fn test_validate_receipt_incorrect_receiver_id() {
        let limit_config = VMLimitConfig::default();
        let invalid_account_id = "WHAT?".to_string();
        assert_eq!(
            validate_receipt(&limit_config, &Receipt::new_balance_refund(&invalid_account_id, 10))
                .expect_err("expected an error"),
            ReceiptValidationError::InvalidReceiverId { account_id: invalid_account_id }
        );
    }

    // ActionReceipt

    #[test]
    fn test_validate_action_receipt_invalid_signer_id() {
        let limit_config = VMLimitConfig::default();
        let invalid_account_id = "WHAT?".to_string();
        assert_eq!(
            validate_action_receipt(
                &limit_config,
                &ActionReceipt {
                    signer_id: invalid_account_id.clone(),
                    signer_public_key: PublicKey::empty(KeyType::ED25519),
                    gas_price: 100,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions: vec![]
                }
            )
            .expect_err("expected an error"),
            ReceiptValidationError::InvalidSignerId { account_id: invalid_account_id }
        );
    }

    #[test]
    fn test_validate_action_receipt_invalid_data_receiver_id() {
        let limit_config = VMLimitConfig::default();
        let invalid_account_id = "WHAT?".to_string();
        assert_eq!(
            validate_action_receipt(
                &limit_config,
                &ActionReceipt {
                    signer_id: alice_account(),
                    signer_public_key: PublicKey::empty(KeyType::ED25519),
                    gas_price: 100,
                    output_data_receivers: vec![DataReceiver {
                        data_id: CryptoHash::default(),
                        receiver_id: invalid_account_id.clone(),
                    }],
                    input_data_ids: vec![],
                    actions: vec![]
                }
            )
            .expect_err("expected an error"),
            ReceiptValidationError::InvalidDataReceiverId { account_id: invalid_account_id }
        );
    }

    #[test]
    fn test_validate_action_receipt_too_many_input_deps() {
        let mut limit_config = VMLimitConfig::default();
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
                }
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
        let limit_config = VMLimitConfig::default();
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
        let mut limit_config = VMLimitConfig::default();
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
        let limit_config = VMLimitConfig::default();
        validate_actions(&limit_config, &[]).expect("empty actions");
    }

    #[test]
    fn test_validate_actions_valid_function_call() {
        let limit_config = VMLimitConfig::default();
        validate_actions(
            &limit_config,
            &vec![Action::FunctionCall(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: 100,
                deposit: 0,
            })],
        )
        .expect("valid function call action");
    }

    #[test]
    fn test_validate_actions_too_much_gas() {
        let mut limit_config = VMLimitConfig::default();
        limit_config.max_total_prepaid_gas = 220;
        assert_eq!(
            validate_actions(
                &limit_config,
                &vec![
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
                ]
            )
            .expect_err("expected an error"),
            ActionsValidationError::TotalPrepaidGasExceeded { total_prepaid_gas: 250, limit: 220 }
        );
    }

    #[test]
    fn test_validate_actions_gas_overflow() {
        let mut limit_config = VMLimitConfig::default();
        limit_config.max_total_prepaid_gas = 220;
        assert_eq!(
            validate_actions(
                &limit_config,
                &vec![
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
                ]
            )
            .expect_err("Expected an error"),
            ActionsValidationError::IntegerOverflow,
        );
    }

    #[test]
    fn test_validate_actions_num_actions() {
        let mut limit_config = VMLimitConfig::default();
        limit_config.max_actions_per_receipt = 1;
        assert_eq!(
            validate_actions(
                &limit_config,
                &vec![
                    Action::CreateAccount(CreateAccountAction {}),
                    Action::CreateAccount(CreateAccountAction {}),
                ]
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
        let mut limit_config = VMLimitConfig::default();
        limit_config.max_actions_per_receipt = 3;
        assert_eq!(
            validate_actions(
                &limit_config,
                &vec![
                    Action::DeleteAccount(DeleteAccountAction { beneficiary_id: "bob".into() }),
                    Action::CreateAccount(CreateAccountAction {}),
                ]
            )
            .expect_err("Expected an error"),
            ActionsValidationError::DeleteActionMustBeFinal,
        );
    }

    #[test]
    fn test_validate_delete_must_work_if_its_final() {
        let mut limit_config = VMLimitConfig::default();
        limit_config.max_actions_per_receipt = 3;
        assert_eq!(
            validate_actions(
                &limit_config,
                &vec![
                    Action::CreateAccount(CreateAccountAction {}),
                    Action::DeleteAccount(DeleteAccountAction { beneficiary_id: "bob".into() }),
                ]
            ),
            Ok(()),
        );
    }

    // Individual actions

    #[test]
    fn test_validate_action_valid_create_account() {
        validate_action(&VMLimitConfig::default(), &Action::CreateAccount(CreateAccountAction {}))
            .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_function_call() {
        validate_action(
            &VMLimitConfig::default(),
            &Action::FunctionCall(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: 100,
                deposit: 0,
            }),
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_invalid_function_call_zero_gas() {
        assert_eq!(
            validate_action(
                &VMLimitConfig::default(),
                &Action::FunctionCall(FunctionCallAction {
                    method_name: "new".to_string(),
                    args: vec![],
                    gas: 0,
                    deposit: 0,
                }),
            )
            .expect_err("expected an error"),
            ActionsValidationError::FunctionCallZeroAttachedGas,
        );
    }

    #[test]
    fn test_validate_action_valid_transfer() {
        validate_action(
            &VMLimitConfig::default(),
            &Action::Transfer(TransferAction { deposit: 10 }),
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_stake() {
        validate_action(
            &VMLimitConfig::default(),
            &Action::Stake(StakeAction {
                stake: 100,
                public_key: "ed25519:KuTCtARNzxZQ3YvXDeLjx83FDqxv2SdQTSbiq876zR7".parse().unwrap(),
            }),
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_invalid_staking_key() {
        assert_eq!(
            validate_action(
                &VMLimitConfig::default(),
                &Action::Stake(StakeAction {
                    stake: 100,
                    public_key: PublicKey::empty(KeyType::ED25519),
                }),
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
            &VMLimitConfig::default(),
            &Action::AddKey(AddKeyAction {
                public_key: PublicKey::empty(KeyType::ED25519),
                access_key: AccessKey::full_access(),
            }),
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_add_key_function_call() {
        validate_action(
            &VMLimitConfig::default(),
            &Action::AddKey(AddKeyAction {
                public_key: PublicKey::empty(KeyType::ED25519),
                access_key: AccessKey {
                    nonce: 0,
                    permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                        allowance: Some(1000),
                        receiver_id: alice_account(),
                        method_names: vec!["hello".to_string(), "world".to_string()],
                    }),
                },
            }),
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_delete_key() {
        validate_action(
            &VMLimitConfig::default(),
            &Action::DeleteKey(DeleteKeyAction { public_key: PublicKey::empty(KeyType::ED25519) }),
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_action_valid_delete_account() {
        validate_action(
            &VMLimitConfig::default(),
            &Action::DeleteAccount(DeleteAccountAction { beneficiary_id: alice_account() }),
        )
        .expect("valid action");
    }
}
