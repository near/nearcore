use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::tests::helpers::*;
use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use crate::logic::types::PromiseResult;
use crate::map;

use near_crypto::PublicKey;
use near_parameters::{ActionCosts, ExtCosts};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::Gas;
use serde_json;

fn vm_receipts<'a>(ext: &'a MockedExternal) -> Vec<impl serde::Serialize + 'a> {
    ext.action_log.clone()
}

#[test]
fn test_promise_results() {
    let promise_results = [
        PromiseResult::Successful(b"test".to_vec()),
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
    expect_test::expect![[r#"
        [
          {
            "CreateReceipt": {
              "receipt_indices": [],
              "receiver_id": "rick.test"
            }
          },
          {
            "FunctionCallWeight": {
              "receipt_index": 0,
              "method_name": [
                112,
                114,
                111,
                109,
                105,
                115,
                101,
                95,
                99,
                114,
                101,
                97,
                116,
                101
              ],
              "args": [
                97,
                114,
                103,
                115
              ],
              "attached_deposit": "0",
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          },
          {
            "FunctionCallWeight": {
              "receipt_index": 0,
              "method_name": [
                112,
                114,
                111,
                109,
                105,
                115,
                101,
                95,
                98,
                97,
                116,
                99,
                104,
                95,
                97,
                99,
                116,
                105,
                111,
                110
              ],
              "args": [
                112,
                114,
                111,
                109,
                105,
                115,
                101,
                95,
                98,
                97,
                116,
                99,
                104,
                95,
                97,
                99,
                116,
                105,
                111,
                110,
                95,
                97,
                114,
                103,
                115
              ],
              "attached_deposit": "0",
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          }
        ]"#]]
    .assert_eq(&serde_json::to_string_pretty(&vm_receipts(&logic_builder.ext)).unwrap());
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
    expect_test::expect![[r#"
        [
          {
            "CreateReceipt": {
              "receipt_indices": [],
              "receiver_id": "rick.test"
            }
          },
          {
            "FunctionCallWeight": {
              "receipt_index": 0,
              "method_name": [
                112,
                114,
                111,
                109,
                105,
                115,
                101,
                95,
                99,
                114,
                101,
                97,
                116,
                101
              ],
              "args": [
                97,
                114,
                103,
                115
              ],
              "attached_deposit": "0",
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          },
          {
            "CreateAccount": {
              "receipt_index": 0
            }
          }
        ]"#]]
    .assert_eq(&serde_json::to_string_pretty(&vm_receipts(&logic_builder.ext)).unwrap());
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
    expect_test::expect![[r#"
        [
          {
            "CreateReceipt": {
              "receipt_indices": [],
              "receiver_id": "rick.test"
            }
          },
          {
            "FunctionCallWeight": {
              "receipt_index": 0,
              "method_name": [
                112,
                114,
                111,
                109,
                105,
                115,
                101,
                95,
                99,
                114,
                101,
                97,
                116,
                101
              ],
              "args": [
                97,
                114,
                103,
                115
              ],
              "attached_deposit": "0",
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          },
          {
            "DeployContract": {
              "receipt_index": 0,
              "code": [
                115,
                97,
                109,
                112,
                108,
                101
              ]
            }
          }
        ]"#]]
    .assert_eq(&serde_json::to_string_pretty(&vm_receipts(&logic_builder.ext)).unwrap());
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
    expect_test::expect![[r#"
        [
          {
            "CreateReceipt": {
              "receipt_indices": [],
              "receiver_id": "rick.test"
            }
          },
          {
            "FunctionCallWeight": {
              "receipt_index": 0,
              "method_name": [
                112,
                114,
                111,
                109,
                105,
                115,
                101,
                95,
                99,
                114,
                101,
                97,
                116,
                101
              ],
              "args": [
                97,
                114,
                103,
                115
              ],
              "attached_deposit": "0",
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          },
          {
            "DeployGlobalContract": {
              "receipt_index": 0,
              "code": [
                115,
                97,
                109,
                112,
                108,
                101
              ],
              "mode": "CodeHash"
            }
          }
        ]"#]]
    .assert_eq(&serde_json::to_string_pretty(&vm_receipts(&logic_builder.ext)).unwrap());
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
    expect_test::expect![[r#"
        [
          {
            "CreateReceipt": {
              "receipt_indices": [],
              "receiver_id": "rick.test"
            }
          },
          {
            "FunctionCallWeight": {
              "receipt_index": 0,
              "method_name": [
                112,
                114,
                111,
                109,
                105,
                115,
                101,
                95,
                99,
                114,
                101,
                97,
                116,
                101
              ],
              "args": [
                97,
                114,
                103,
                115
              ],
              "attached_deposit": "0",
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          },
          {
            "UseGlobalContract": {
              "receipt_index": 0,
              "contract_id": {
                "CodeHash": "A6buRpeED4eLGiYvbboigf7WSicK52xwKtdRNFwKcFgv"
              }
            }
          }
        ]"#]]
    .assert_eq(&serde_json::to_string_pretty(&vm_receipts(&logic_builder.ext)).unwrap());
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
    expect_test::expect![[r#"
        [
          {
            "CreateReceipt": {
              "receipt_indices": [],
              "receiver_id": "rick.test"
            }
          },
          {
            "FunctionCallWeight": {
              "receipt_index": 0,
              "method_name": [
                112,
                114,
                111,
                109,
                105,
                115,
                101,
                95,
                99,
                114,
                101,
                97,
                116,
                101
              ],
              "args": [
                97,
                114,
                103,
                115
              ],
              "attached_deposit": "0",
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          },
          {
            "Transfer": {
              "receipt_index": 0,
              "deposit": "110"
            }
          }
        ]"#]]
    .assert_eq(&serde_json::to_string_pretty(&vm_receipts(&logic_builder.ext)).unwrap());
}

#[test]
fn test_promise_batch_action_stake() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    let key = borsh::to_vec(
        &"ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf".parse::<PublicKey>().unwrap(),
    )
    .unwrap();

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
    expect_test::expect![[r#"
        [
          {
            "CreateReceipt": {
              "receipt_indices": [],
              "receiver_id": "rick.test"
            }
          },
          {
            "FunctionCallWeight": {
              "receipt_index": 0,
              "method_name": [
                112,
                114,
                111,
                109,
                105,
                115,
                101,
                95,
                99,
                114,
                101,
                97,
                116,
                101
              ],
              "args": [
                97,
                114,
                103,
                115
              ],
              "attached_deposit": "0",
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          },
          {
            "Stake": {
              "receipt_index": 0,
              "stake": "110",
              "public_key": "ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf"
            }
          }
        ]"#]]
    .assert_eq(&serde_json::to_string_pretty(&vm_receipts(&logic_builder.ext)).unwrap());
}

#[test]
fn test_promise_batch_action_add_key_with_function_call() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    let index_ptr = logic.internal_mem_write(&index.to_le_bytes()).ptr;
    let key = borsh::to_vec(
        &"ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf".parse::<PublicKey>().unwrap(),
    )
    .unwrap();
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
    expect_test::expect![[r#"
        [
          {
            "CreateReceipt": {
              "receipt_indices": [],
              "receiver_id": "rick.test"
            }
          },
          {
            "FunctionCallWeight": {
              "receipt_index": 0,
              "method_name": [
                112,
                114,
                111,
                109,
                105,
                115,
                101,
                95,
                99,
                114,
                101,
                97,
                116,
                101
              ],
              "args": [
                97,
                114,
                103,
                115
              ],
              "attached_deposit": "0",
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          },
          {
            "AddKeyWithFunctionCall": {
              "receipt_index": 0,
              "public_key": "ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf",
              "nonce": 1,
              "allowance": "999",
              "receiver_id": "sam",
              "method_names": [
                [
                  102,
                  111,
                  111
                ],
                [
                  98,
                  97,
                  114
                ]
              ]
            }
          }
        ]"#]]
    .assert_eq(&serde_json::to_string_pretty(&vm_receipts(&logic_builder.ext)).unwrap());
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
    expect_test::expect![[r#"
        [
          {
            "CreateReceipt": {
              "receipt_indices": [],
              "receiver_id": "rick.test"
            }
          },
          {
            "FunctionCallWeight": {
              "receipt_index": 0,
              "method_name": [
                112,
                114,
                111,
                109,
                105,
                115,
                101,
                95,
                99,
                114,
                101,
                97,
                116,
                101
              ],
              "args": [
                97,
                114,
                103,
                115
              ],
              "attached_deposit": "0",
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          },
          {
            "CreateReceipt": {
              "receipt_indices": [
                0
              ],
              "receiver_id": "rick.test"
            }
          },
          {
            "CreateReceipt": {
              "receipt_indices": [
                0
              ],
              "receiver_id": "rick.test"
            }
          }
        ]"#]]
    .assert_eq(&serde_json::to_string_pretty(&vm_receipts(&logic_builder.ext)).unwrap());
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

    let profile = logic.gas_counter().profile_data();
    assert_eq!(logic.used_gas().unwrap(), 8_373_585_814_798, "unexpected gas usage {profile:?}");

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
        576_000_000,
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

    expect_test::expect![[r#"
        [
          {
            "CreateReceipt": {
              "receipt_indices": [],
              "receiver_id": "rick.test"
            }
          },
          {
            "DeterministicStateInit": {
              "receipt_index": 0,
              "state_init": {
                "V1": {
                  "code": {
                    "AccountId": "global.near"
                  },
                  "data": {
                    "a2V5": "dmFsdWU="
                  }
                }
              },
              "amount": "110"
            }
          }
        ]"#]]
    .assert_eq(&serde_json::to_string_pretty(&vm_receipts(&logic_builder.ext)).unwrap());
}
