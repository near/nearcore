use crate::logic::mocks::mock_external::MockAction;
use crate::logic::tests::helpers::*;
use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use crate::logic::types::{GlobalContractDeployMode, GlobalContractIdentifier, PromiseResult};
use crate::map;

use near_crypto::PublicKey;
use near_parameters::{ActionCosts, ExtCosts};
use near_primitives_core::config::AccountIdValidityRulesVersion;
use near_primitives_core::deterministic_account_id::{
    DeterministicAccountStateInit, DeterministicAccountStateInitV1,
};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{Balance, Gas, GasWeight};
use near_primitives_core::version::{PROTOCOL_VERSION, ProtocolFeature};

fn test_public_key() -> PublicKey {
    "ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf".parse().unwrap()
}

/// Asserts the common promise_create preamble (CreateReceipt + FunctionCallWeight) in the
/// action log. Returns the remaining actions after the preamble.
#[track_caller]
fn assert_promise_create_preamble<'a>(
    action_log: &'a [MockAction],
    expected_receiver: &str,
) -> &'a [MockAction] {
    assert!(action_log.len() >= 2, "expected at least 2 actions, got {}", action_log.len());
    assert_eq!(
        action_log[0],
        MockAction::CreateReceipt {
            receipt_indices: vec![],
            receiver_id: expected_receiver.parse().unwrap(),
        }
    );
    assert_eq!(
        action_log[1],
        MockAction::FunctionCallWeight {
            receipt_index: 0,
            method_name: b"promise_create".to_vec(),
            args: b"args".to_vec(),
            attached_deposit: Balance::ZERO,
            prepaid_gas: Gas::ZERO,
            gas_weight: GasWeight(0),
        }
    );
    &action_log[2..]
}

#[test]
fn test_promise_results() {
    let promise_results = [
        PromiseResult::Successful(b"test".to_vec().into()),
        PromiseResult::Failed,
        PromiseResult::NotReady,
    ];

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.context.promise_results = promise_results.into();
    let mut logic = logic_builder.build();

    assert_eq!(logic.promise_results_count(), Ok(3), "Total count of registers must be 3");
    assert_eq!(logic.promise_result(0, 0), Ok(1), "Must return code 1 on success");
    assert_eq!(logic.promise_result(1, 0), Ok(2), "Failed promise must return code 2");
    assert_eq!(logic.promise_result(2, 0), Ok(0), "Pending promise must return 0");

    // Only promise with result should write data into register
    logic.assert_read_register(b"test", 0);
}

#[test]
fn test_promise_result_per_byte_gas_fee() {
    const RESULT_SIZE: usize = 100;
    let promise_results = [PromiseResult::Successful([0u8; RESULT_SIZE].into())];

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.context.promise_results = promise_results.into();
    let mut logic = logic_builder.build();

    logic.promise_result(0, 0).expect("promise_result should succeed");

    if ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        assert_costs(map! {
          ExtCosts::base: 1,
          ExtCosts::write_register_base: 1,
        });
    } else {
        assert_costs(map! {
          ExtCosts::base: 1,
          ExtCosts::write_register_base: 1,
          ExtCosts::write_register_byte: RESULT_SIZE as u64,
        });
    }
}

#[test]
fn test_promise_batch_action_function_call() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    let index_ptr = logic.internal_mem_write(&index.to_le_bytes()).ptr;

    promise_batch_action_function_call(&mut logic, 123, 0, Gas::ZERO)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt =
        logic.promise_and(index_ptr, 1u64).expect("should create a non-receipt promise");
    promise_batch_action_function_call(&mut logic, non_receipt, 0, Gas::ZERO)
        .expect_err("shouldn't accept non-receipt promise index");

    promise_batch_action_function_call(&mut logic, index, 0, Gas::ZERO)
        .expect("should add an action to receipt");
    let actions = assert_promise_create_preamble(&logic_builder.ext.action_log, "rick.test");
    assert_eq!(
        actions,
        &[MockAction::FunctionCallWeight {
            receipt_index: 0,
            method_name: b"promise_batch_action".to_vec(),
            args: b"promise_batch_action_args".to_vec(),
            attached_deposit: Balance::ZERO,
            prepaid_gas: Gas::ZERO,
            gas_weight: GasWeight(0),
        }]
    );
}

#[test]
fn test_promise_batch_action_use_global_contract_by_account_id_with_invalid_account() {
    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.account_id_validity_rules_version =
        AccountIdValidityRulesVersion::V2;
    let mut logic = logic_builder.build();
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");

    let invalid_account_id = logic.internal_mem_write(b"not a valid account id");

    let err = logic
        .promise_batch_action_use_global_contract_by_account_id(
            index,
            invalid_account_id.len,
            invalid_account_id.ptr,
        )
        .expect_err("should reject invalid account id when strict validation is enabled");

    assert!(matches!(
        err,
        crate::logic::VMLogicError::HostError(crate::logic::HostError::InvalidAccountId)
    ));
}

#[test]
fn test_promise_batch_action_create_account() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    let index_ptr = logic.internal_mem_write(&index.to_le_bytes()).ptr;

    logic
        .promise_batch_action_create_account(123)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt =
        logic.promise_and(index_ptr, 1u64).expect("should create a non-receipt promise");
    logic
        .promise_batch_action_create_account(non_receipt)
        .expect_err("shouldn't accept non-receipt promise index");
    logic
        .promise_batch_action_create_account(index)
        .expect("should add an action to create account");
    assert_eq!(logic.used_gas().unwrap(), 12578263688564);
    let actions = assert_promise_create_preamble(&logic_builder.ext.action_log, "rick.test");
    assert_eq!(actions, &[MockAction::CreateAccount { receipt_index: 0 }]);
}

#[test]
fn test_promise_batch_action_deploy_contract() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");

    let index_ptr = logic.internal_mem_write(&index.to_le_bytes()).ptr;
    let code = logic.internal_mem_write(b"sample");

    logic
        .promise_batch_action_deploy_contract(123, code.len, code.ptr)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt =
        logic.promise_and(index_ptr, 1u64).expect("should create a non-receipt promise");
    logic
        .promise_batch_action_deploy_contract(non_receipt, code.len, code.ptr)
        .expect_err("shouldn't accept non-receipt promise index");

    logic
        .promise_batch_action_deploy_contract(index, code.len, code.ptr)
        .expect("should add an action to deploy contract");
    assert_eq!(logic.used_gas().unwrap(), 5255774958146);
    let actions = assert_promise_create_preamble(&logic_builder.ext.action_log, "rick.test");
    assert_eq!(
        actions,
        &[MockAction::DeployContract { receipt_index: 0, code: b"sample".to_vec() }]
    );
}

#[test]
fn test_promise_batch_action_deploy_global_contract() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");

    let index_ptr = logic.internal_mem_write(&index.to_le_bytes()).ptr;
    let code = logic.internal_mem_write(b"sample");

    logic
        .promise_batch_action_deploy_global_contract(123, code.len, code.ptr)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt =
        logic.promise_and(index_ptr, 1u64).expect("should create a non-receipt promise");
    logic
        .promise_batch_action_deploy_global_contract(non_receipt, code.len, code.ptr)
        .expect_err("shouldn't accept non-receipt promise index");

    logic
        .promise_batch_action_deploy_global_contract(index, code.len, code.ptr)
        .expect("should add an action to deploy contract");
    assert_eq!(logic.used_gas().unwrap(), 5256154080152);
    let actions = assert_promise_create_preamble(&logic_builder.ext.action_log, "rick.test");
    assert_eq!(
        actions,
        &[MockAction::DeployGlobalContract {
            receipt_index: 0,
            code: b"sample".to_vec(),
            mode: GlobalContractDeployMode::CodeHash,
        }]
    );
}

#[test]
fn test_promise_batch_action_use_global_contract() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");

    let index_ptr = logic.internal_mem_write(&index.to_le_bytes()).ptr;
    let code_hash = logic.internal_mem_write(CryptoHash::hash_bytes(b"arbitrary").as_bytes());
    let malformed_code_hash = logic.internal_mem_write(b"not a valid CryptoHash repr");

    logic
        .promise_batch_action_use_global_contract(123, code_hash.len, code_hash.ptr)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt =
        logic.promise_and(index_ptr, 1u64).expect("should create a non-receipt promise");
    logic
        .promise_batch_action_use_global_contract(non_receipt, code_hash.len, code_hash.ptr)
        .expect_err("shouldn't accept non-receipt promise index");
    logic
        .promise_batch_action_use_global_contract(
            123,
            malformed_code_hash.len,
            malformed_code_hash.ptr,
        )
        .expect_err("shouldn't accept malformed code hash");

    logic
        .promise_batch_action_use_global_contract(index, code_hash.len, code_hash.ptr)
        .expect("should add an action to deploy contract");
    assert_eq!(logic.used_gas().unwrap(), 5262559186522);
    let actions = assert_promise_create_preamble(&logic_builder.ext.action_log, "rick.test");
    assert_eq!(
        actions,
        &[MockAction::UseGlobalContract {
            receipt_index: 0,
            contract_id: GlobalContractIdentifier::CodeHash(CryptoHash::hash_bytes(b"arbitrary")),
        }]
    );
}

#[test]
fn test_promise_batch_action_transfer() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");

    let index_ptr = logic.internal_mem_write(&index.to_le_bytes()).ptr;
    let num_110u128 = logic.internal_mem_write(&110u128.to_le_bytes());
    let num_1u128 = logic.internal_mem_write(&1u128.to_le_bytes());

    logic
        .promise_batch_action_transfer(123, num_110u128.ptr)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt =
        logic.promise_and(index_ptr, 1u64).expect("should create a non-receipt promise");
    logic
        .promise_batch_action_transfer(non_receipt, num_110u128.ptr)
        .expect_err("shouldn't accept non-receipt promise index");

    logic
        .promise_batch_action_transfer(index, num_110u128.ptr)
        .expect("should add an action to transfer money");
    logic.promise_batch_action_transfer(index, num_1u128.ptr).expect_err("not enough money");
    assert_eq!(logic.used_gas().unwrap(), 5349703444787);
    let actions = assert_promise_create_preamble(&logic_builder.ext.action_log, "rick.test");
    assert_eq!(
        actions,
        &[MockAction::Transfer { receipt_index: 0, deposit: Balance::from_yoctonear(110) }]
    );
}

#[test]
fn test_promise_batch_action_transfer_to_gas_key() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    let key = borsh::to_vec(&test_public_key()).unwrap();

    let key = logic.internal_mem_write(&key);
    let index_ptr = logic.internal_mem_write(&index.to_le_bytes()).ptr;
    let num_110u128 = logic.internal_mem_write(&110u128.to_le_bytes());
    let num_1u128 = logic.internal_mem_write(&1u128.to_le_bytes());

    logic
        .promise_batch_action_transfer_to_gas_key(123, key.len, key.ptr, num_110u128.ptr)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt =
        logic.promise_and(index_ptr, 1u64).expect("should create a non-receipt promise");
    logic
        .promise_batch_action_transfer_to_gas_key(non_receipt, key.len, key.ptr, num_110u128.ptr)
        .expect_err("shouldn't accept non-receipt promise index");

    logic
        .promise_batch_action_transfer_to_gas_key(index, key.len, key.ptr, num_110u128.ptr)
        .expect("should add an action to transfer to gas key");
    logic
        .promise_batch_action_transfer_to_gas_key(index, key.len, key.ptr, num_1u128.ptr)
        .expect_err("not enough money");
    let actions = assert_promise_create_preamble(&logic_builder.ext.action_log, "rick.test");
    assert_eq!(
        actions,
        &[MockAction::TransferToGasKey {
            receipt_index: 0,
            public_key: test_public_key(),
            deposit: Balance::from_yoctonear(110),
        }]
    );
}

#[test]
fn test_promise_batch_action_stake() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    let key = borsh::to_vec(&test_public_key()).unwrap();

    let key = logic.internal_mem_write(&key);
    let index_ptr = logic.internal_mem_write(&index.to_le_bytes()).ptr;
    let num_110u128 = logic.internal_mem_write(&110u128.to_le_bytes());

    logic
        .promise_batch_action_stake(123, num_110u128.ptr, key.len, key.ptr)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt =
        logic.promise_and(index_ptr, 1u64).expect("should create a non-receipt promise");
    logic
        .promise_batch_action_stake(non_receipt, num_110u128.ptr, key.len, key.ptr)
        .expect_err("shouldn't accept non-receipt promise index");

    logic
        .promise_batch_action_stake(index, num_110u128.ptr, key.len, key.ptr)
        .expect("should add an action to stake");
    assert_eq!(logic.used_gas().unwrap(), 5138414976215);
    let actions = assert_promise_create_preamble(&logic_builder.ext.action_log, "rick.test");
    assert_eq!(
        actions,
        &[MockAction::Stake {
            receipt_index: 0,
            stake: Balance::from_yoctonear(110),
            public_key: test_public_key(),
        }]
    );
}

#[test]
fn test_promise_batch_action_add_key_with_function_call() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    let index_ptr = logic.internal_mem_write(&index.to_le_bytes()).ptr;
    let key = borsh::to_vec(&test_public_key()).unwrap();
    let nonce = 1;
    let allowance = 999u128;
    let receiver_id = b"sam";
    let method_names = b"foo,bar";

    promise_batch_action_add_key_with_function_call(
        &mut logic,
        123,
        &key,
        nonce,
        allowance,
        receiver_id,
        method_names,
    )
    .expect_err("shouldn't accept non-existent promise index");
    let non_receipt =
        logic.promise_and(index_ptr, 1u64).expect("should create a non-receipt promise");
    promise_batch_action_add_key_with_function_call(
        &mut logic,
        non_receipt,
        &key,
        nonce,
        allowance,
        receiver_id,
        method_names,
    )
    .expect_err("shouldn't accept non-receipt promise index");

    promise_batch_action_add_key_with_function_call(
        &mut logic,
        index,
        &key,
        nonce,
        allowance,
        receiver_id,
        method_names,
    )
    .expect("should add allowance");
    assert_eq!(logic.used_gas().unwrap(), 5126680499695);
    let actions = assert_promise_create_preamble(&logic_builder.ext.action_log, "rick.test");
    assert_eq!(
        actions,
        &[MockAction::AddKeyWithFunctionCall {
            receipt_index: 0,
            public_key: test_public_key(),
            nonce: 1,
            allowance: Some(Balance::from_yoctonear(999)),
            receiver_id: "sam".parse().unwrap(),
            method_names: vec![b"foo".to_vec(), b"bar".to_vec()],
        }]
    );
}

#[test]
fn test_promise_batch_then() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();

    let account_id = b"rick.test";
    let index = promise_create(&mut logic, account_id, 0, 0).expect("should create a promise");

    let account_id = logic.internal_mem_write(&account_id[..]);
    let index_slice = logic.internal_mem_write(&index.to_le_bytes());

    logic
        .promise_batch_then(123, account_id.len, account_id.ptr)
        .expect_err("shouldn't accept non-existent promise index");
    let non_receipt =
        logic.promise_and(index_slice.ptr, 1u64).expect("should create a non-receipt promise");
    logic
        .promise_batch_then(non_receipt, account_id.len, account_id.ptr)
        .expect("should accept non-receipt promise index");

    logic
        .promise_batch_then(index, account_id.len, account_id.ptr)
        .expect("promise batch should run ok");
    assert_eq!(logic.used_gas().unwrap(), 24124999601771);
    let actions = assert_promise_create_preamble(&logic_builder.ext.action_log, "rick.test");
    let expected_receipt = MockAction::CreateReceipt {
        receipt_indices: vec![0],
        receiver_id: "rick.test".parse().unwrap(),
    };
    assert_eq!(actions, &[expected_receipt.clone(), expected_receipt]);
}

#[test]
fn test_promise_batch_action_state_init() {
    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.deterministic_account_ids = true;

    let mut logic = logic_builder.build();

    // Note: Sending to an invalid receiver here, "rick.test" is not a
    // deterministic account id. On the VM level, that won't be caught. But the
    // outgoing receipt will fail validation. Tests for that case are in
    // `runtime/runtime/src/verifier.rs`.
    let receiver = "rick.test";
    let index = promise_batch_create(&mut logic, &receiver).expect("should create a promise");

    // check setup costs are as expected and reset the counter (reset happens inside `assert_costs`)
    assert_costs(map! {
      ExtCosts::base: 1,
      ExtCosts::read_memory_base: 1,
      ExtCosts::read_memory_byte: receiver.len() as u64,
      ExtCosts::utf8_decoding_base: 1,
      ExtCosts::utf8_decoding_byte: receiver.len() as u64,
    });

    let account_id = logic.internal_mem_write(b"global.near");
    let key = logic.internal_mem_write(b"key");
    let value = logic.internal_mem_write(b"value");
    let amount = logic.internal_mem_write(&110u128.to_le_bytes());
    let key2_register = 0;
    let value2_register = 1;
    let key2 = b"key2";
    logic.wrapped_internal_write_register(key2_register, key2).unwrap();
    let value2 = b"value2";
    logic.wrapped_internal_write_register(value2_register, value2).unwrap();
    reset_costs_counter();

    let action_index = logic
        .promise_batch_action_state_init_by_account_id(
            index,
            account_id.len,
            account_id.ptr,
            amount.ptr,
        )
        .expect("should successfully add state init action");

    assert_costs(map! {
      ExtCosts::base: 1,
      ExtCosts::read_memory_base: 2,
      ExtCosts::read_memory_byte: account_id.len + amount.len,
      ExtCosts::utf8_decoding_base: 1,
      ExtCosts::utf8_decoding_byte: account_id.len,
    });

    logic
        .set_state_init_data_entry(index, action_index, key.len, key.ptr, value.len, value.ptr)
        .expect("should successfully add data to state init action");

    assert_costs(map! {
      ExtCosts::base: 1,
      ExtCosts::read_memory_base: 2,
      ExtCosts::read_memory_byte: key.len + value.len,
    });

    // also set by register
    logic
        .set_state_init_data_entry(
            index,
            action_index,
            u64::MAX,
            key2_register,
            u64::MAX,
            value2_register,
        )
        .expect("should successfully add data to state init action");

    assert_costs(map! {
      ExtCosts::base: 1,
      ExtCosts::read_register_base: 2,
      ExtCosts::read_register_byte: (key2.len() + value2.len()) as u64,
    });

    let profile = logic.gas_counter().profile_data();
    assert_eq!(logic.used_gas().unwrap(), 8_586_074_959_513, "unexpected gas usage {profile:?}");

    // Check that all send costs have been charged as expected

    // action_deterministic_state_init
    //    send 3.85 Tgas
    //    exec 4.00 Tgas
    assert_eq!(
        profile.actions_profile[ActionCosts::deterministic_state_init_base].as_gigagas(),
        3850,
        "unexpected action gas usage {profile:?}"
    );
    // action_deterministic_state_init_per_entry
    //    exec 0.20 Tgas
    assert_eq!(
        profile.actions_profile[ActionCosts::deterministic_state_init_entry].as_gigagas(),
        0,
        "unexpected action gas usage {profile:?}"
    );
    // action_deterministic_state_init_per_byte
    //    send 0.000_072 per byte
    //    exec 0.000_070 per byte
    assert_eq!(
        profile.actions_profile[ActionCosts::deterministic_state_init_byte].as_gas(),
        1_296_000_000,
        "unexpected action gas usage {profile:?}"
    );
    // action_receipt_creation
    //    send 0.108 Tgas
    //    exec 0.108 Tgas
    assert_eq!(
        profile.actions_profile[ActionCosts::new_action_receipt].as_gigagas(),
        108,
        "unexpected action gas usage {profile:?}"
    );

    assert_eq!(
        logic_builder.ext.action_log,
        &[
            MockAction::CreateReceipt {
                receipt_indices: vec![],
                receiver_id: "rick.test".parse().unwrap(),
            },
            MockAction::DeterministicStateInit {
                receipt_index: 0,
                state_init: DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
                    code:
                        near_primitives_core::global_contract::GlobalContractIdentifier::AccountId(
                            "global.near".parse().unwrap(),
                        ),
                    data: [
                        (b"key".to_vec(), b"value".to_vec()),
                        (b"key2".to_vec(), b"value2".to_vec()),
                    ]
                    .into(),
                }),
                amount: Balance::from_yoctonear(110),
            },
        ]
    );
}
