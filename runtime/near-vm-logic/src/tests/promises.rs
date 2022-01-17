use crate::tests::fixtures::get_context;
use crate::tests::helpers::*;
use crate::tests::vm_logic_builder::VMLogicBuilder;
use crate::types::PromiseResult;
use serde_json;

#[test]
fn test_promise_results() {
    let mut promise_results = vec![];
    promise_results.push(PromiseResult::Successful(b"test".to_vec()));
    promise_results.push(PromiseResult::Failed);
    promise_results.push(PromiseResult::NotReady);

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.promise_results = promise_results;
    let mut l = logic_builder.build(get_context(vec![], false));

    assert_eq!(l.logic.promise_results_count(l.mem,), Ok(3), "Total count of registers must be 3");
    assert_eq!(l.logic.promise_result(l.mem, 0, 0), Ok(1), "Must return code 1 on success");
    assert_eq!(l.logic.promise_result(l.mem, 1, 0), Ok(2), "Failed promise must return code 2");
    assert_eq!(l.logic.promise_result(l.mem, 2, 0), Ok(0), "Pending promise must return 3");

    let buffer = [0u8; 4];
    l.logic.read_register(l.mem, 0, buffer.as_ptr() as u64).unwrap();
    assert_eq!(&buffer, b"test", "Only promise with result should write data into register");
}

#[test]
fn test_promise_batch_action_function_call() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut l = logic_builder.build(get_context(vec![], false));
    let index = promise_create(&mut l, b"rick.test", 0, 0).expect("should create a promise");

    promise_batch_action_function_call(&mut l, 123, 0, 0)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt = l
        .logic
        .promise_and(l.mem, index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    promise_batch_action_function_call(&mut l, non_receipt, 0, 0)
        .expect_err("shouldn't accept non-receipt promise index");

    promise_batch_action_function_call(&mut l, index, 0, 0)
        .expect("should add an action to receipt");
    let expected = serde_json::json!([
    {
        "receipt_indices":[],
        "receiver_id":"rick.test",
        "actions":[
            {
                "FunctionCall":{
                    "method_name":"promise_create","args":"args","gas":0,"deposit":0}},
            {
                "FunctionCall":{
                    "method_name":"promise_batch_action","args":"promise_batch_action_args","gas":0,"deposit":0}
            }
        ]
    }]);
    assert_eq!(
        &serde_json::to_string(logic_builder.ext.get_receipt_create_calls()).unwrap(),
        &expected.to_string()
    );
}

#[test]
fn test_promise_batch_action_create_account() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut l = logic_builder.build(get_context(vec![], false));
    let index = promise_create(&mut l, b"rick.test", 0, 0).expect("should create a promise");

    l.logic
        .promise_batch_action_create_account(l.mem, 123)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt = l
        .logic
        .promise_and(l.mem, index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    l.logic
        .promise_batch_action_create_account(l.mem, non_receipt)
        .expect_err("shouldn't accept non-receipt promise index");
    l.logic
        .promise_batch_action_create_account(l.mem, index)
        .expect("should add an action to create account");
    assert_eq!(l.logic.used_gas(l.mem).unwrap(), 5077478438564);
    let expected = serde_json::json!([
        {
            "receipt_indices": [],
            "receiver_id": "rick.test",
            "actions": [
            {
                "FunctionCall": {
                "method_name": "promise_create",
                "args": "args",
                "gas": 0,
                "deposit": 0
                }
            },
            "CreateAccount"
            ]
        }
    ]);
    assert_eq!(
        &serde_json::to_string(logic_builder.ext.get_receipt_create_calls()).unwrap(),
        &expected.to_string()
    );
}

#[test]
fn test_promise_batch_action_deploy_contract() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut l = logic_builder.build(get_context(vec![], false));
    let index = promise_create(&mut l, b"rick.test", 0, 0).expect("should create a promise");
    let code = b"sample";

    l.logic
        .promise_batch_action_deploy_contract(l.mem, 123, code.len() as u64, code.as_ptr() as _)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt = l
        .logic
        .promise_and(l.mem, index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    l.logic
        .promise_batch_action_deploy_contract(
            l.mem,
            non_receipt,
            code.len() as u64,
            code.as_ptr() as _,
        )
        .expect_err("shouldn't accept non-receipt promise index");

    l.logic
        .promise_batch_action_deploy_contract(l.mem, index, code.len() as u64, code.as_ptr() as _)
        .expect("should add an action to deploy contract");
    assert_eq!(l.logic.used_gas(l.mem).unwrap(), 5255774958146);
    let expected = serde_json::json!(
      [
        {
        "receipt_indices": [],
        "receiver_id": "rick.test",
        "actions": [
          {
            "FunctionCall": {
              "method_name": "promise_create",
              "args": "args",
              "gas": 0,
              "deposit": 0
            }
          },
          {
            "DeployContract": {
              "code": [
                115,97,109,112,108,101
              ]
            }
          }
        ]
      }
    ]);
    assert_eq!(
        &serde_json::to_string(logic_builder.ext.get_receipt_create_calls()).unwrap(),
        &expected.to_string()
    );
}

#[test]
fn test_promise_batch_action_transfer() {
    let mut context = get_context(vec![], false);
    context.account_balance = 100;
    context.attached_deposit = 10;
    let mut logic_builder = VMLogicBuilder::default();
    let mut l = logic_builder.build(context);
    let index = promise_create(&mut l, b"rick.test", 0, 0).expect("should create a promise");

    l.logic
        .promise_batch_action_transfer(l.mem, 123, 110u128.to_le_bytes().as_ptr() as _)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt = l
        .logic
        .promise_and(l.mem, index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    l.logic
        .promise_batch_action_transfer(l.mem, non_receipt, 110u128.to_le_bytes().as_ptr() as _)
        .expect_err("shouldn't accept non-receipt promise index");

    l.logic
        .promise_batch_action_transfer(l.mem, index, 110u128.to_le_bytes().as_ptr() as _)
        .expect("should add an action to transfer money");
    l.logic
        .promise_batch_action_transfer(l.mem, index, 1u128.to_le_bytes().as_ptr() as _)
        .expect_err("not enough money");
    assert_eq!(l.logic.used_gas(l.mem).unwrap(), 5349703444787);
    let expected = serde_json::json!(
    [
        {
            "receipt_indices": [],
            "receiver_id": "rick.test",
            "actions": [
            {
                "FunctionCall": {
                "method_name": "promise_create",
                "args": "args",
                "gas": 0,
                "deposit": 0
                }
            },
            {
                "Transfer": {
                "deposit": 110
                }
            }
            ]
        }
    ]);
    assert_eq!(
        &serde_json::to_string(logic_builder.ext.get_receipt_create_calls()).unwrap(),
        &expected.to_string()
    );
}

#[test]
fn test_promise_batch_action_stake() {
    let mut context = get_context(vec![], false);
    // And there are 10N in attached balance to the transaction.
    context.account_balance = 100;
    let mut logic_builder = VMLogicBuilder::default();
    let mut l = logic_builder.build(context);
    let index = promise_create(&mut l, b"rick.test", 0, 0).expect("should create a promise");
    let key = b"ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf";

    l.logic
        .promise_batch_action_stake(
            l.mem,
            123,
            110u128.to_le_bytes().as_ptr() as _,
            key.len() as u64,
            key.as_ptr() as _,
        )
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt = l
        .logic
        .promise_and(l.mem, index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    l.logic
        .promise_batch_action_stake(
            l.mem,
            non_receipt,
            110u128.to_le_bytes().as_ptr() as _,
            key.len() as u64,
            key.as_ptr() as _,
        )
        .expect_err("shouldn't accept non-receipt promise index");

    l.logic
        .promise_batch_action_stake(
            l.mem,
            index,
            110u128.to_le_bytes().as_ptr() as _,
            key.len() as u64,
            key.as_ptr() as _,
        )
        .expect("should add an action to stake");
    assert_eq!(l.logic.used_gas(l.mem).unwrap(), 5138631652196);
    let expected = serde_json::json!([
        {
            "receipt_indices": [],
            "receiver_id": "rick.test",
            "actions": [
                {
                    "FunctionCall": {
                        "method_name": "promise_create",
                        "args": "args",
                        "gas": 0,
                        "deposit": 0
                    }
                },
                {
                    "Stake": {
                        "stake": 110,
                        "public_key": "RLb4qQXoZPAFqzZhiLFAcGFPFC7JWcDd8xKvQHHEqLUgDXuQkr2ehKAN28MNGQN9vUZ1qGZ"
                    }
                }
            ]
        }
    ]);
    assert_eq!(
        &serde_json::to_string(logic_builder.ext.get_receipt_create_calls()).unwrap(),
        &expected.to_string()
    );
}

#[test]
fn test_promise_batch_action_add_key_with_function_call() {
    let mut context = get_context(vec![], false);
    context.account_balance = 100;

    let mut logic_builder = VMLogicBuilder::default();
    let mut l = logic_builder.build(context);
    let index = promise_create(&mut l, b"rick.test", 0, 0).expect("should create a promise");
    let key = b"ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf";
    let nonce = 1;
    let allowance = 999u128;
    let receiver_id = b"sam";
    let method_names = b"foo,bar";

    promise_batch_action_add_key_with_function_call(
        &mut l,
        123,
        key,
        nonce,
        allowance,
        receiver_id,
        method_names,
    )
    .expect_err("shouldn't accept non-existent promise index");
    let non_receipt = l
        .logic
        .promise_and(l.mem, index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    promise_batch_action_add_key_with_function_call(
        &mut l,
        non_receipt,
        key,
        nonce,
        allowance,
        receiver_id,
        method_names,
    )
    .expect_err("shouldn't accept non-receipt promise index");

    promise_batch_action_add_key_with_function_call(
        &mut l,
        index,
        key,
        nonce,
        allowance,
        receiver_id,
        method_names,
    )
    .expect("should add allowance");
    assert_eq!(l.logic.used_gas(l.mem).unwrap(), 5126897175676);
    let expected = serde_json::json!(
    [
        {
            "receipt_indices": [],
            "receiver_id": "rick.test",
            "actions": [
                {
                    "FunctionCall": {
                        "method_name": "promise_create",
                        "args": "args",
                        "gas": 0,
                        "deposit": 0
                    }
                },
                {
                    "AddKeyWithFunctionCall": {
                        "public_key": "RLb4qQXoZPAFqzZhiLFAcGFPFC7JWcDd8xKvQHHEqLUgDXuQkr2ehKAN28MNGQN9vUZ1qGZ",
                        "nonce": 1,
                        "allowance": 999,
                        "receiver_id": "sam",
                        "method_names": [
                            "foo",
                            "bar"
                        ]
                    }
                }
            ]
        }
    ]);
    assert_eq!(
        &serde_json::to_string(logic_builder.ext.get_receipt_create_calls()).unwrap(),
        &expected.to_string()
    );
}

#[test]
fn test_promise_batch_then() {
    let mut context = get_context(vec![], false);
    context.account_balance = 100;
    let mut logic_builder = VMLogicBuilder::default();
    let mut l = logic_builder.build(context);

    let account_id = b"rick.test";
    let index = promise_create(&mut l, account_id, 0, 0).expect("should create a promise");

    l.logic
        .promise_batch_then(l.mem, 123, account_id.len() as u64, account_id.as_ptr() as _)
        .expect_err("shouldn't accept non-existent promise index");
    let non_receipt = l
        .logic
        .promise_and(l.mem, index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    l.logic
        .promise_batch_then(l.mem, non_receipt, account_id.len() as u64, account_id.as_ptr() as _)
        .expect("should accept non-receipt promise index");

    l.logic
        .promise_batch_then(l.mem, index, account_id.len() as u64, account_id.as_ptr() as _)
        .expect("promise batch should run ok");
    assert_eq!(l.logic.used_gas(l.mem).unwrap(), 24124999601771);
    let expected = serde_json::json!([
        {
            "receipt_indices": [],
            "receiver_id": "rick.test",
            "actions": [
                {
                    "FunctionCall": {
                        "method_name": "promise_create",
                        "args": "args",
                        "gas": 0,
                        "deposit": 0
                    }
                }
            ]
        },
        {
            "receipt_indices": [
                0
            ],
            "receiver_id": "rick.test",
            "actions": []
        },
        {
            "receipt_indices": [
                0
            ],
            "receiver_id": "rick.test",
            "actions": []
        }
    ]);
    assert_eq!(
        &serde_json::to_string(logic_builder.ext.get_receipt_create_calls()).unwrap(),
        &expected.to_string()
    );
}
