use crate::config::total_prepaid_gas;
use crate::verifier::ValidateReceiptMode;
use near_crypto::key_conversion::is_valid_staking_key;
use near_primitives::account::AccessKeyPermission;
use near_primitives::action::delegate::VersionedDelegateActionRef;
use near_primitives::action::{
    AddKeyAction, DeployGlobalContractAction, DeterministicStateInitAction,
    GlobalContractIdentifier, UseGlobalContractAction,
};
use near_primitives::errors::ActionsValidationError;
use near_primitives::transaction::{
    Action, DeleteAccountAction, DeployContractAction, FunctionCallAction, StakeAction,
};
use near_primitives::types::{AccountId, Balance, Gas};
use near_primitives::utils::derive_near_deterministic_account_id;
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_vm_runner::logic::LimitConfig;

/// Validates that the number of deploy actions in the given list of actions doesn't exceed the limit.
fn validate_number_of_deploy_actions(
    actions: &[Action],
    max_deploy_actions_per_receipt: u64,
) -> Result<(), ActionsValidationError> {
    let deploy_actions_count = actions
        .iter()
        .filter(|a| matches!(a, Action::DeployContract(_) | Action::DeployGlobalContract(_)))
        .count() as u64;
    if deploy_actions_count > max_deploy_actions_per_receipt {
        Err(ActionsValidationError::TotalNumberOfDeployActionsExceeded {
            number_of_deploy_actions: deploy_actions_count,
            limit: max_deploy_actions_per_receipt,
        })
    } else {
        Ok(())
    }
}

/// Validates given actions in `NewReceipt` mode (the strictest variant). See
/// [`validate_actions_with_mode`] for the full description of checks performed.
pub(crate) fn validate_actions(
    limit_config: &LimitConfig,
    actions: &[Action],
    receiver: &AccountId,
    current_protocol_version: ProtocolVersion,
) -> Result<(), ActionsValidationError> {
    validate_actions_with_mode(
        limit_config,
        actions,
        receiver,
        current_protocol_version,
        ValidateReceiptMode::NewReceipt,
    )
}

/// Validates given actions:
///
/// - Checks limits if applicable.
/// - Checks that the total number of actions doesn't exceed the limit.
/// - Checks that there not other action if Action::Delegate is present.
/// - Validates each individual action.
/// - Checks that the total prepaid gas doesn't exceed the limit.
pub(crate) fn validate_actions_with_mode(
    limit_config: &LimitConfig,
    actions: &[Action],
    receiver: &AccountId,
    current_protocol_version: ProtocolVersion,
    mode: ValidateReceiptMode,
) -> Result<(), ActionsValidationError> {
    if actions.len() as u64 > limit_config.max_actions_per_receipt {
        return Err(ActionsValidationError::TotalNumberOfActionsExceeded {
            total_number_of_actions: actions.len() as u64,
            limit: limit_config.max_actions_per_receipt,
        });
    }

    // Centralized post-quantum gate. Mirrors the tx-admission gate in
    // `check_valid_for_config`, and is load-bearing for actions emitted by
    // contracts via host functions: those actions create new receipts that
    // never go through tx admission, so on a pre-feature protocol they must
    // be rejected here. The exhaustive match in
    // `Action::post_quantum_signatures_required` (including the recursive
    // walk into `Delegate`) forces every future action variant to make an
    // explicit decision at compile time.
    if !ProtocolFeature::PostQuantumSignatures.enabled(current_protocol_version)
        && actions.iter().any(Action::post_quantum_signatures_required)
    {
        return Err(ActionsValidationError::UnsupportedProtocolFeature {
            protocol_feature: "PostQuantumSignatures".to_owned(),
            version: current_protocol_version,
        });
    }

    if mode == ValidateReceiptMode::NewReceipt {
        validate_number_of_deploy_actions(actions, limit_config.max_deploy_actions_per_receipt)?;
    }

    let mut found_delegate_action = false;
    let mut iter = actions.iter().peekable();
    while let Some(action) = iter.next() {
        if let Action::DeleteAccount(_) = action {
            if iter.peek().is_some() {
                return Err(ActionsValidationError::DeleteActionMustBeFinal);
            }
        } else {
            if let Action::Delegate(_) | Action::DelegateV2(_) = action {
                if found_delegate_action {
                    return Err(ActionsValidationError::DelegateActionMustBeOnlyOne);
                }
                found_delegate_action = true;
            }
        }
        validate_action_with_mode(limit_config, action, receiver, current_protocol_version, mode)?;
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

/// Validates a single given action.
/// The `mode` only affects nested validation of `Action::Delegate` payloads
fn validate_action_with_mode(
    limit_config: &LimitConfig,
    action: &Action,
    receiver: &AccountId,
    current_protocol_version: ProtocolVersion,
    mode: ValidateReceiptMode,
) -> Result<(), ActionsValidationError> {
    match action {
        Action::CreateAccount(_) => Ok(()),
        Action::DeployContract(a) => validate_deploy_contract_action(limit_config, a),
        Action::DeployGlobalContract(a) => validate_deploy_global_contract_action(limit_config, a),
        Action::UseGlobalContract(a) => validate_use_global_contract_action(a),
        Action::FunctionCall(a) => {
            validate_function_call_action(limit_config, a, current_protocol_version, mode)
        }
        Action::Transfer(_) => Ok(()),
        Action::Stake(a) => validate_stake_action(a),
        Action::AddKey(a) => validate_add_key_action(limit_config, a, current_protocol_version),
        Action::DeleteKey(_) => Ok(()),
        Action::DeleteAccount(a) => validate_delete_action(a),
        Action::Delegate(a) => validate_delegate_action(
            limit_config,
            (&a.delegate_action).into(),
            receiver,
            current_protocol_version,
            mode,
        ),
        Action::DelegateV2(a) => {
            require_protocol_feature(
                ProtocolFeature::DelegateV2,
                "DelegateV2",
                current_protocol_version,
            )?;
            validate_delegate_action(
                limit_config,
                (&a.delegate_action).into(),
                receiver,
                current_protocol_version,
                mode,
            )
        }
        Action::DeterministicStateInit(a) => {
            validate_deterministic_state_init(limit_config, a, receiver)
        }
        Action::TransferToGasKey(_) => {
            validate_transfer_to_gas_key_action(current_protocol_version)
        }
        Action::WithdrawFromGasKey(_) => {
            validate_withdraw_from_gas_key_action(current_protocol_version)
        }
    }
}

fn validate_delegate_action(
    limit_config: &LimitConfig,
    delegate_action: VersionedDelegateActionRef<'_>,
    receiver: &AccountId,
    current_protocol_version: ProtocolVersion,
    mode: ValidateReceiptMode,
) -> Result<(), ActionsValidationError> {
    let actions = delegate_action.get_actions();
    let inner_receiver =
        if ProtocolFeature::FixDelegatedDeterministicStateInit.enabled(current_protocol_version) {
            // This is the correct receiver id to use for the check.
            delegate_action.receiver_id()
        } else {
            // This is a bug fixed with `FixDelegatedDeterministicStateInit` that
            // validated against the wrong id. This makes it impossible to
            // initialize deterministic accounts from meta transactions.
            // The bug cannot be abused, if someone crafts a state init that passes
            // validation here, it will fail when it is checked as incoming receipt.
            receiver
        };
    validate_actions_with_mode(
        limit_config,
        &actions,
        inner_receiver,
        current_protocol_version,
        mode,
    )?;
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

fn validate_use_global_contract_action(
    action: &UseGlobalContractAction,
) -> Result<(), ActionsValidationError> {
    validate_global_contract_identifier(&action.contract_identifier)?;

    Ok(())
}

/// Validates `FunctionCallAction`. Checks that the method name is non-empty, that its length
/// doesn't exceed the limit, and that the length of the arguments doesn't exceed the limit.
fn validate_function_call_action(
    limit_config: &LimitConfig,
    action: &FunctionCallAction,
    current_protocol_version: ProtocolVersion,
    mode: ValidateReceiptMode,
) -> Result<(), ActionsValidationError> {
    if action.gas == Gas::ZERO {
        return Err(ActionsValidationError::FunctionCallZeroAttachedGas);
    }

    if mode == ValidateReceiptMode::NewReceipt
        && ProtocolFeature::RejectEmptyMethodName.enabled(current_protocol_version)
        && action.method_name.is_empty()
    {
        return Err(ActionsValidationError::FunctionCallEmptyMethodName);
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

/// Validates `AddKeyAction`. Checks validity of the access key permission.
/// If adding a gas key, validates gas key specific constraints.
///
/// Note: ML-DSA-65 keys are gated centrally via
/// `Action::post_quantum_signatures_required`, called both from
/// `validate_actions_with_mode` (covers receipts emitted by contracts) and
/// from `check_valid_for_config` (covers tx admission). An exhaustive match
/// in that predicate guarantees no action variant slips through unchecked.
fn validate_add_key_action(
    limit_config: &LimitConfig,
    action: &AddKeyAction,
    current_protocol_version: ProtocolVersion,
) -> Result<(), ActionsValidationError> {
    validate_access_key_permission(limit_config, &action.access_key.permission)?;

    // If this is a gas key, apply additional gas key validation
    if let Some(gas_key_info) = action.access_key.gas_key_info() {
        require_protocol_feature(ProtocolFeature::GasKeys, "GasKeys", current_protocol_version)?;

        // For gas keys with FunctionCallPermission, allowance must be None
        if let Some(fc) = action.access_key.permission.function_call_permission() {
            if fc.allowance.is_some() {
                return Err(ActionsValidationError::GasKeyFunctionCallAllowanceNotAllowed);
            }
        }

        if gas_key_info.num_nonces == 0
            || gas_key_info.num_nonces > AccessKeyPermission::MAX_NONCES_FOR_GAS_KEY
        {
            return Err(ActionsValidationError::GasKeyInvalidNumNonces {
                requested_nonces: gas_key_info.num_nonces,
                limit: AccessKeyPermission::MAX_NONCES_FOR_GAS_KEY,
            });
        }

        if gas_key_info.balance != Balance::ZERO {
            return Err(ActionsValidationError::AddGasKeyWithNonZeroBalance {
                balance: gas_key_info.balance,
            });
        }
    }

    Ok(())
}

/// Validates `AccessKeyPermission`. If the access key permission is `FunctionCall`, checks that the
/// total number of bytes of the method names doesn't exceed the limit and
/// every method name length doesn't exceed the limit.
fn validate_access_key_permission(
    limit_config: &LimitConfig,
    permission: &AccessKeyPermission,
) -> Result<(), ActionsValidationError> {
    if let Some(fc) = permission.function_call_permission() {
        // Check whether `receiver_id` is a valid account_id. Historically, we
        // allowed arbitrary strings there!
        match limit_config.account_id_validity_rules_version {
            near_primitives_core::config::AccountIdValidityRulesVersion::V0 => (),
            near_primitives_core::config::AccountIdValidityRulesVersion::V1
            | near_primitives_core::config::AccountIdValidityRulesVersion::V2 => {
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

fn validate_delete_action(action: &DeleteAccountAction) -> Result<(), ActionsValidationError> {
    validate_action_account_id(&action.beneficiary_id)?;

    Ok(())
}

fn validate_transfer_to_gas_key_action(
    current_protocol_version: ProtocolVersion,
) -> Result<(), ActionsValidationError> {
    require_protocol_feature(ProtocolFeature::GasKeys, "GasKeys", current_protocol_version)?;

    Ok(())
}

fn validate_withdraw_from_gas_key_action(
    current_protocol_version: ProtocolVersion,
) -> Result<(), ActionsValidationError> {
    require_protocol_feature(ProtocolFeature::GasKeys, "GasKeys", current_protocol_version)?;

    Ok(())
}

fn require_protocol_feature(
    feature: ProtocolFeature,
    feature_name: &str,
    current_protocol_version: ProtocolVersion,
) -> Result<(), ActionsValidationError> {
    if !feature.enabled(current_protocol_version) {
        return Err(ActionsValidationError::UnsupportedProtocolFeature {
            protocol_feature: feature_name.to_owned(),
            version: current_protocol_version,
        });
    }
    Ok(())
}

fn validate_deterministic_state_init(
    limit_config: &LimitConfig,
    action: &DeterministicStateInitAction,
    receiver_id: &AccountId,
) -> Result<(), ActionsValidationError> {
    validate_global_contract_identifier(action.state_init.code())?;

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

fn validate_global_contract_identifier(
    identifier: &GlobalContractIdentifier,
) -> Result<(), ActionsValidationError> {
    if let GlobalContractIdentifier::AccountId(account_id) = &identifier {
        validate_action_account_id(account_id)?;
    }

    Ok(())
}

fn validate_action_account_id(account_id: &AccountId) -> Result<(), ActionsValidationError> {
    AccountId::validate(account_id.as_str()).map_err(|_| {
        ActionsValidationError::InvalidAccountId { account_id: account_id.to_string() }
    })?;

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
    use near_crypto::{KeyType, PublicKey, Signature};
    use near_primitives::account::{AccessKey, FunctionCallPermission};
    use near_primitives::action::GlobalContractDeployMode;
    use near_primitives::action::delegate::{
        DelegateAction, DelegateActionV2, NonDelegateAction, SignedDelegateAction,
        VersionedSignedDelegateAction,
    };
    use near_primitives::deterministic_account_id::{
        DeterministicAccountStateInit, DeterministicAccountStateInitV1,
    };
    use near_primitives::transaction::{
        AddKeyAction, CreateAccountAction, DeleteKeyAction, TransactionNonce, TransferAction,
    };
    use near_primitives::version::PROTOCOL_VERSION;
    use std::collections::BTreeMap;
    use testlib::runtime_utils::alice_account;

    fn test_limit_config() -> LimitConfig {
        let store = near_parameters::RuntimeConfigStore::test();
        store.get_config(PROTOCOL_VERSION).wasm_config.limit_config.clone()
    }

    fn validate_action(
        limit_config: &LimitConfig,
        action: &Action,
        receiver: &AccountId,
        current_protocol_version: ProtocolVersion,
    ) -> Result<(), ActionsValidationError> {
        validate_action_with_mode(
            limit_config,
            action,
            receiver,
            current_protocol_version,
            ValidateReceiptMode::NewReceipt,
        )
    }

    // Group of actions

    #[test]
    fn test_validate_actions_empty() {
        let limit_config = test_limit_config();
        let receiver = "alice.near".parse().unwrap();
        validate_actions(&limit_config, &[], &receiver, PROTOCOL_VERSION).expect("empty actions");
    }

    /// Receipt-level gate: contract-emitted receipts carrying an ML-DSA-65
    /// `AddKey` must be rejected pre-PostQuantumSignatures. The tx-admission
    /// gate in `check_valid_for_config` doesn't cover this path because the
    /// receipt is created by the runtime host functions, not by a user
    /// transaction.
    #[test]
    fn test_validate_actions_ml_dsa_add_key_gated() {
        let limit_config = test_limit_config();
        let receiver: AccountId = "alice.near".parse().unwrap();
        let pq_pubkey = near_crypto::SecretKey::from_seed(KeyType::MLDSA65, "victim").public_key();
        let actions = [Action::AddKey(Box::new(AddKeyAction {
            public_key: pq_pubkey,
            access_key: AccessKey::full_access(),
        }))];

        let pre = ProtocolFeature::PostQuantumSignatures.protocol_version() - 1;
        let post = ProtocolFeature::PostQuantumSignatures.protocol_version();

        assert!(matches!(
            validate_actions(&limit_config, &actions, &receiver, pre),
            Err(ActionsValidationError::UnsupportedProtocolFeature { .. })
        ));
        validate_actions(&limit_config, &actions, &receiver, post)
            .expect("ML-DSA-65 AddKey accepted post-feature");
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
    fn test_validate_actions_num_deploy_actions() {
        let receiver: AccountId = "alice.near".parse().unwrap();
        let deploy_local = || Action::DeployContract(DeployContractAction { code: vec![1; 5] });
        let deploy_global = || {
            Action::DeployGlobalContract(DeployGlobalContractAction {
                code: vec![1; 5].into(),
                deploy_mode: GlobalContractDeployMode::CodeHash,
            })
        };

        let mut limit_config = test_limit_config();
        limit_config.max_deploy_actions_per_receipt = 2;

        // Pure DeployContract over the limit → error.
        assert_eq!(
            validate_actions(
                &limit_config,
                &[deploy_local(), deploy_local(), deploy_local()],
                &receiver,
                PROTOCOL_VERSION,
            )
            .expect_err("expected error"),
            ActionsValidationError::TotalNumberOfDeployActionsExceeded {
                number_of_deploy_actions: 3,
                limit: 2,
            },
        );

        // Pure DeployGlobalContract over the limit → error.
        assert_eq!(
            validate_actions(
                &limit_config,
                &[deploy_global(), deploy_global(), deploy_global()],
                &receiver,
                PROTOCOL_VERSION,
            )
            .expect_err("expected error"),
            ActionsValidationError::TotalNumberOfDeployActionsExceeded {
                number_of_deploy_actions: 3,
                limit: 2,
            },
        );

        // Mixed deploy actions summing over the limit → error.
        assert_eq!(
            validate_actions(
                &limit_config,
                &[deploy_local(), deploy_global(), deploy_local()],
                &receiver,
                PROTOCOL_VERSION,
            )
            .expect_err("expected error"),
            ActionsValidationError::TotalNumberOfDeployActionsExceeded {
                number_of_deploy_actions: 3,
                limit: 2,
            },
        );

        // Equal-to-limit count → ok.
        validate_actions(
            &limit_config,
            &[deploy_local(), deploy_global()],
            &receiver,
            PROTOCOL_VERSION,
        )
        .expect("expected ok");

        // Other action types interleaved don't count toward the limit.
        validate_actions(
            &limit_config,
            &[
                Action::CreateAccount(CreateAccountAction {}),
                deploy_local(),
                Action::CreateAccount(CreateAccountAction {}),
                deploy_global(),
                Action::CreateAccount(CreateAccountAction {}),
            ],
            &receiver,
            PROTOCOL_VERSION,
        )
        .expect("expected ok");

        // Deploys inside a Delegate are checked against the same cap on their own when the
        // verifier recurses through `validate_action` -> `validate_delegate_action` ->
        // `validate_actions`. Outer and inner deploys are not summed: the inner list ends up
        // as a separate receipt and is bounded independently.
        let delegate_with = |inner: Vec<Action>| {
            let actions =
                inner.into_iter().map(|a| NonDelegateAction::try_from(a).unwrap()).collect();
            Action::Delegate(Box::new(SignedDelegateAction {
                delegate_action: DelegateAction {
                    sender_id: "bob.test.near".parse().unwrap(),
                    receiver_id: "token.test.near".parse().unwrap(),
                    actions,
                    nonce: 19000001,
                    max_block_height: 57,
                    public_key: PublicKey::empty(KeyType::ED25519),
                },
                signature: Signature::default(),
            }))
        };

        // Delegate alone whose inner deploys exceed the limit → error (caught by recursion).
        assert_eq!(
            validate_actions(
                &limit_config,
                &[delegate_with(vec![deploy_local(), deploy_global(), deploy_local()])],
                &receiver,
                PROTOCOL_VERSION,
            )
            .expect_err("expected error"),
            ActionsValidationError::TotalNumberOfDeployActionsExceeded {
                number_of_deploy_actions: 3,
                limit: 2,
            },
        );

        // Outer at limit + delegate inner at limit → ok (counts are independent, not summed).
        validate_actions(
            &limit_config,
            &[
                deploy_local(),
                deploy_global(),
                delegate_with(vec![deploy_local(), deploy_global()]),
            ],
            &receiver,
            PROTOCOL_VERSION,
        )
        .expect("expected ok");

        // Delegate carrying only non-deploy actions does not contribute to the count.
        validate_actions(
            &limit_config,
            &[
                deploy_local(),
                deploy_global(),
                delegate_with(vec![Action::CreateAccount(CreateAccountAction {})]),
            ],
            &receiver,
            PROTOCOL_VERSION,
        )
        .expect("expected ok");
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
    fn test_validate_action_invalid_add_key_gas_key_before_protocol_feature() {
        let num_nonces = 10; // Arbitrary number of nonces for testing
        let gas_key = AccessKey::gas_key_full_access(num_nonces);
        let protocol_version = ProtocolFeature::GasKeys.protocol_version() - 1;
        assert_eq!(
            validate_action(
                &test_limit_config(),
                &Action::AddKey(Box::new(AddKeyAction {
                    public_key: PublicKey::empty(KeyType::ED25519),
                    access_key: gas_key,
                })),
                &"alice.near".parse().unwrap(),
                protocol_version,
            ),
            Err(ActionsValidationError::UnsupportedProtocolFeature {
                protocol_feature: "GasKeys".to_owned(),
                version: protocol_version,
            })
        );
    }

    #[test]
    fn test_validate_action_invalid_delegate_v2_before_protocol_feature() {
        let delegate_action = DelegateActionV2 {
            sender_id: alice_account(),
            receiver_id: "bob.near".parse().unwrap(),
            actions: vec![],
            nonce: TransactionNonce::from_nonce_and_index(1, 0),
            max_block_height: 1000,
            public_key: PublicKey::empty(KeyType::ED25519),
        };
        let action = Action::DelegateV2(Box::new(VersionedSignedDelegateAction {
            delegate_action: delegate_action.into(),
            signature: Signature::empty(KeyType::ED25519),
        }));
        let protocol_version = ProtocolFeature::DelegateV2.protocol_version() - 1;
        assert_eq!(
            validate_action(&test_limit_config(), &action, &alice_account(), protocol_version),
            Err(ActionsValidationError::UnsupportedProtocolFeature {
                protocol_feature: "DelegateV2".to_owned(),
                version: protocol_version,
            })
        );
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
            PROTOCOL_VERSION,
            expect![[r#"
                Ok(
                    (),
                )
            "#]],
        );

        // deterministic id but incorrect receiver
        check_validate_state_init(
            "0s1234567890123456789012345678901234567890",
            PROTOCOL_VERSION,
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
            PROTOCOL_VERSION,
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
            PROTOCOL_VERSION,
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
            PROTOCOL_VERSION,
            expect![[r#"
                Ok(
                    (),
                )
            "#]],
        );

        // key and value exactly at limit
        check_validate_state_init(
            make_payload(2_048, 4_194_304),
            PROTOCOL_VERSION,
            expect![[r#"
                Ok(
                    (),
                )
            "#]],
        );

        // key above limit
        check_validate_state_init(
            make_payload(2_049, 4_194_304),
            PROTOCOL_VERSION,
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
            PROTOCOL_VERSION,
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

    #[test]
    fn test_validate_add_gas_key_valid() {
        let limit_config = test_limit_config();
        let num_nonces = 10; // Arbitrary number of nonces for testing
        validate_action(
            &limit_config,
            &Action::AddKey(Box::new(AddKeyAction {
                public_key: PublicKey::empty(KeyType::ED25519),
                access_key: AccessKey::gas_key_full_access(num_nonces),
            })),
            &"alice.near".parse().unwrap(),
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .expect("valid action");
    }

    #[test]
    fn test_validate_add_gas_key_invalid_num_nonces() {
        let limit_config = test_limit_config();
        let account_id: AccountId = "alice.near".parse().unwrap();
        let version = ProtocolFeature::GasKeys.protocol_version();
        // Valid number of nonces is between 1 and MAX_NONCES_FOR_GAS_KEY inclusive.
        // Test 0 nonces and num_nonces greater than the maximum allowed results in an error.
        for num_nonces in [0, AccessKeyPermission::MAX_NONCES_FOR_GAS_KEY + 1] {
            assert_eq!(
                validate_action(
                    &limit_config,
                    &Action::AddKey(Box::new(AddKeyAction {
                        public_key: PublicKey::empty(KeyType::ED25519),
                        access_key: AccessKey::gas_key_full_access(num_nonces),
                    })),
                    &account_id,
                    version,
                )
                .unwrap_err(),
                ActionsValidationError::GasKeyInvalidNumNonces {
                    requested_nonces: num_nonces,
                    limit: AccessKeyPermission::MAX_NONCES_FOR_GAS_KEY
                },
            );
        }
    }

    #[test]
    fn test_validate_add_gas_key_allowance_set() {
        let limit_config = test_limit_config();
        let num_nonces = 10; // Arbitrary number of nonces for testing
        let gas_key = AccessKey::gas_key_function_call(
            num_nonces,
            FunctionCallPermission {
                allowance: Some(Balance::from_yoctonear(1000)),
                receiver_id: "bob.near".parse().unwrap(),
                method_names: vec![],
            },
        );
        assert_eq!(
            validate_action(
                &limit_config,
                &Action::AddKey(Box::new(AddKeyAction {
                    public_key: PublicKey::empty(KeyType::ED25519),
                    access_key: gas_key,
                })),
                &"alice.near".parse().unwrap(),
                ProtocolFeature::GasKeys.protocol_version(),
            )
            .expect_err("expected an error"),
            ActionsValidationError::GasKeyFunctionCallAllowanceNotAllowed
        );
    }

    #[test]
    fn test_validate_add_gas_key_method_name_too_long() {
        let limit_config = test_limit_config();
        let num_nonces = 10; // Arbitrary number of nonces for testing
        let limit_length = limit_config.max_length_method_name;
        let permission = FunctionCallPermission {
            allowance: None,
            receiver_id: "bob.near".parse().unwrap(),
            method_names: vec!["A".repeat(limit_length as usize + 1)],
        };
        assert_eq!(
            validate_action(
                &limit_config,
                &Action::AddKey(Box::new(AddKeyAction {
                    public_key: PublicKey::empty(KeyType::ED25519),
                    access_key: AccessKey::gas_key_function_call(num_nonces, permission),
                })),
                &"alice.near".parse().unwrap(),
                ProtocolFeature::GasKeys.protocol_version(),
            )
            .expect_err("expected an error"),
            ActionsValidationError::AddKeyMethodNameLengthExceeded {
                length: limit_length + 1,
                limit: limit_length
            }
        );
    }
}
