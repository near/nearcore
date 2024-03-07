use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::tests::helpers::*;
use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use crate::logic::types::PromiseResult;

use near_crypto::PublicKey;
use serde_json;

fn vm_receipts<'a>(ext: &'a MockedExternal) -> Vec<impl serde::Serialize + 'a> {
    ext.action_log.clone()
}

#[test]
fn test_promise_results() {
    let promise_results = vec![
        PromiseResult::Successful(b"test".to_vec()),
        PromiseResult::Failed,
        PromiseResult::NotReady,
    ];

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.promise_results = promise_results;
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

    promise_batch_action_function_call(&mut logic, 123, 0, 0)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt =
        logic.promise_and(index_ptr, 1u64).expect("should create a non-receipt promise");
    promise_batch_action_function_call(&mut logic, non_receipt, 0, 0)
        .expect_err("shouldn't accept non-receipt promise index");

    promise_batch_action_function_call(&mut logic, index, 0, 0)
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
              "attached_deposit": 0,
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
              "attached_deposit": 0,
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
              "attached_deposit": 0,
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
              "attached_deposit": 0,
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
              "attached_deposit": 0,
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          },
          {
            "Transfer": {
              "receipt_index": 0,
              "deposit": 110
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
              "attached_deposit": 0,
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          },
          {
            "Stake": {
              "receipt_index": 0,
              "stake": 110,
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
              "attached_deposit": 0,
              "prepaid_gas": 0,
              "gas_weight": 0
            }
          },
          {
            "AddKeyWithFunctionCall": {
              "receipt_index": 0,
              "public_key": "ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf",
              "nonce": 1,
              "allowance": 999,
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
              "attached_deposit": 0,
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
