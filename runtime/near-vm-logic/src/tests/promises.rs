use crate::tests::fixtures::get_context;
use crate::tests::helpers::*;
use crate::tests::vm_logic_builder::VMLogicBuilder;
use crate::types::PromiseResult;
use crate::VMLogic;
use borsh::BorshSerialize;
use near_account_id::AccountId;
use near_crypto::PublicKey;
use near_primitives::transaction::Action;
use serde::Serialize;
use serde_json;

#[derive(Serialize)]
struct ReceiptView<'a> {
    receiver_id: &'a AccountId,
    actions: &'a [Action],
}

fn vm_receipts<'a>(logic: &'a VMLogic) -> Vec<ReceiptView<'a>> {
    logic
        .receipt_manager()
        .action_receipts
        .iter()
        .map(|(receiver_id, metadata)| ReceiptView { receiver_id, actions: &metadata.actions })
        .collect()
}

#[test]
fn test_promise_results() {
    let mut promise_results = vec![];
    promise_results.push(PromiseResult::Successful(b"test".to_vec()));
    promise_results.push(PromiseResult::Failed);
    promise_results.push(PromiseResult::NotReady);

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.promise_results = promise_results;
    let mut logic = logic_builder.build(get_context(vec![], false));

    assert_eq!(logic.promise_results_count(), Ok(3), "Total count of registers must be 3");
    assert_eq!(logic.promise_result(0, 0), Ok(1), "Must return code 1 on success");
    assert_eq!(logic.promise_result(1, 0), Ok(2), "Failed promise must return code 2");
    assert_eq!(logic.promise_result(2, 0), Ok(0), "Pending promise must return 3");

    let buffer = [0u8; 4];
    logic.read_register(0, buffer.as_ptr() as u64).unwrap();
    assert_eq!(&buffer, b"test", "Only promise with result should write data into register");
}

#[test]
fn test_promise_batch_action_function_call() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");

    promise_batch_action_function_call(&mut logic, 123, 0, 0)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt = logic
        .promise_and(index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    promise_batch_action_function_call(&mut logic, non_receipt, 0, 0)
        .expect_err("shouldn't accept non-receipt promise index");

    promise_batch_action_function_call(&mut logic, index, 0, 0)
        .expect("should add an action to receipt");
    let expected = serde_json::json!([
    {
        "receiver_id":"rick.test",
        "actions":[
            {
                "FunctionCall":{
                    "method_name":"promise_create","args":"YXJncw==","gas":0,"deposit":"0"}},
            {
                "FunctionCall":{
                    "method_name":"promise_batch_action","args":"cHJvbWlzZV9iYXRjaF9hY3Rpb25fYXJncw==","gas":0,"deposit":"0"}
            }
        ]
    }]);
    assert_eq!(&serde_json::to_string(&vm_receipts(&logic)).unwrap(), &expected.to_string());
}

#[test]
fn test_promise_batch_action_create_account() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");

    logic
        .promise_batch_action_create_account(123)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt = logic
        .promise_and(index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    logic
        .promise_batch_action_create_account(non_receipt)
        .expect_err("shouldn't accept non-receipt promise index");
    logic
        .promise_batch_action_create_account(index)
        .expect("should add an action to create account");
    assert_eq!(logic.used_gas().unwrap(), 5077478438564);
    let expected = serde_json::json!([
        {
            "receiver_id": "rick.test",
            "actions": [
            {
                "FunctionCall": {
                "method_name": "promise_create",
                "args": "YXJncw==",
                "gas": 0,
                "deposit": "0"
                }
            },
            {"CreateAccount": {}}
            ]
        }
    ]);
    assert_eq!(&serde_json::to_string(&vm_receipts(&logic)).unwrap(), &expected.to_string());
}

#[test]
fn test_promise_batch_action_deploy_contract() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    let code = b"sample";

    logic
        .promise_batch_action_deploy_contract(123, code.len() as u64, code.as_ptr() as _)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt = logic
        .promise_and(index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    logic
        .promise_batch_action_deploy_contract(non_receipt, code.len() as u64, code.as_ptr() as _)
        .expect_err("shouldn't accept non-receipt promise index");

    logic
        .promise_batch_action_deploy_contract(index, code.len() as u64, code.as_ptr() as _)
        .expect("should add an action to deploy contract");
    assert_eq!(logic.used_gas().unwrap(), 5255774958146);
    let expected = serde_json::json!(
      [
        {

        "receiver_id": "rick.test",
        "actions": [
          {
            "FunctionCall": {
              "method_name": "promise_create",
              "args": "YXJncw==",
              "gas": 0,
              "deposit": "0"
            }
          },
          {
            "DeployContract": {
              "code": "c2FtcGxl"
            }
          }
        ]
      }
    ]);
    assert_eq!(&serde_json::to_string(&vm_receipts(&logic)).unwrap(), &expected.to_string());
}

#[test]
fn test_promise_batch_action_transfer() {
    let mut context = get_context(vec![], false);
    context.account_balance = 100;
    context.attached_deposit = 10;
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(context);
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");

    logic
        .promise_batch_action_transfer(123, 110u128.to_le_bytes().as_ptr() as _)
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt = logic
        .promise_and(index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    logic
        .promise_batch_action_transfer(non_receipt, 110u128.to_le_bytes().as_ptr() as _)
        .expect_err("shouldn't accept non-receipt promise index");

    logic
        .promise_batch_action_transfer(index, 110u128.to_le_bytes().as_ptr() as _)
        .expect("should add an action to transfer money");
    logic
        .promise_batch_action_transfer(index, 1u128.to_le_bytes().as_ptr() as _)
        .expect_err("not enough money");
    assert_eq!(logic.used_gas().unwrap(), 5349703444787);
    let expected = serde_json::json!(
    [
        {

            "receiver_id": "rick.test",
            "actions": [
            {
                "FunctionCall": {
                "method_name": "promise_create",
                "args": "YXJncw==",
                "gas": 0,
                "deposit": "0"
                }
            },
            {
                "Transfer": {
                "deposit": "110"
                }
            }
            ]
        }
    ]);
    assert_eq!(&serde_json::to_string(&vm_receipts(&logic)).unwrap(), &expected.to_string());
}

#[test]
fn test_promise_batch_action_stake() {
    let mut context = get_context(vec![], false);
    // And there are 10N in attached balance to the transaction.
    context.account_balance = 100;
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(context);
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    let key = "ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf"
        .parse::<PublicKey>()
        .unwrap()
        .try_to_vec()
        .unwrap();

    logic
        .promise_batch_action_stake(
            123,
            110u128.to_le_bytes().as_ptr() as _,
            key.len() as u64,
            key.as_ptr() as _,
        )
        .expect_err("shouldn't accept not existent promise index");
    let non_receipt = logic
        .promise_and(index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    logic
        .promise_batch_action_stake(
            non_receipt,
            110u128.to_le_bytes().as_ptr() as _,
            key.len() as u64,
            key.as_ptr() as _,
        )
        .expect_err("shouldn't accept non-receipt promise index");

    logic
        .promise_batch_action_stake(
            index,
            110u128.to_le_bytes().as_ptr() as _,
            key.len() as u64,
            key.as_ptr() as _,
        )
        .expect("should add an action to stake");
    assert_eq!(logic.used_gas().unwrap(), 5138414976215);
    let expected = serde_json::json!([
        {

            "receiver_id": "rick.test",
            "actions": [
                {
                    "FunctionCall": {
                        "method_name": "promise_create",
                        "args": "YXJncw==",
                        "gas": 0,
                        "deposit": "0"
                    }
                },
                {
                    "Stake": {
                        "stake": "110",
                        "public_key": "ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf"
                    }
                }
            ]
        }
    ]);
    assert_eq!(&serde_json::to_string(&vm_receipts(&logic)).unwrap(), &expected.to_string());
}

#[test]
fn test_promise_batch_action_add_key_with_function_call() {
    let mut context = get_context(vec![], false);
    context.account_balance = 100;

    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(context);
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    let serialized_key = "ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf"
        .parse::<PublicKey>()
        .unwrap()
        .try_to_vec()
        .unwrap();
    let key = &&serialized_key;
    let nonce = 1;
    let allowance = 999u128;
    let receiver_id = b"sam";
    let method_names = b"foo,bar";

    promise_batch_action_add_key_with_function_call(
        &mut logic,
        123,
        key,
        nonce,
        allowance,
        receiver_id,
        method_names,
    )
    .expect_err("shouldn't accept non-existent promise index");
    let non_receipt = logic
        .promise_and(index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    promise_batch_action_add_key_with_function_call(
        &mut logic,
        non_receipt,
        key,
        nonce,
        allowance,
        receiver_id,
        method_names,
    )
    .expect_err("shouldn't accept non-receipt promise index");

    promise_batch_action_add_key_with_function_call(
        &mut logic,
        index,
        key,
        nonce,
        allowance,
        receiver_id,
        method_names,
    )
    .expect("should add allowance");
    assert_eq!(logic.used_gas().unwrap(), 5126680499695);
    let expected = serde_json::json!(
    [
        {
            "receiver_id": "rick.test",
            "actions": [
                {
                    "FunctionCall": {
                        "method_name": "promise_create",
                        "args": "YXJncw==",
                        "gas": 0,
                        "deposit": "0"
                    }
                },
                {
                    "AddKey": {
                        "public_key": "ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf",
                        "access_key": {
                            "nonce": 1,
                            "permission": {
                                "FunctionCall": {
                                    "allowance": "999",
                                    "receiver_id": "sam",
                                    "method_names": [
                                        "foo",
                                        "bar"
                                    ]
                                }
                            }
                        }
                    }
                }
            ]
        }
    ]);
    assert_eq!(&serde_json::to_string(&vm_receipts(&logic)).unwrap(), &expected.to_string());
}

#[test]
fn test_promise_batch_then() {
    let mut context = get_context(vec![], false);
    context.account_balance = 100;
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(context);

    let account_id = b"rick.test";
    let index = promise_create(&mut logic, account_id, 0, 0).expect("should create a promise");

    logic
        .promise_batch_then(123, account_id.len() as u64, account_id.as_ptr() as _)
        .expect_err("shouldn't accept non-existent promise index");
    let non_receipt = logic
        .promise_and(index.to_le_bytes().as_ptr() as _, 1u64)
        .expect("should create a non-receipt promise");
    logic
        .promise_batch_then(non_receipt, account_id.len() as u64, account_id.as_ptr() as _)
        .expect("should accept non-receipt promise index");

    logic
        .promise_batch_then(index, account_id.len() as u64, account_id.as_ptr() as _)
        .expect("promise batch should run ok");
    assert_eq!(logic.used_gas().unwrap(), 24124999601771);
    let expected = serde_json::json!([
        {
            "receiver_id": "rick.test",
            "actions": [
                {
                    "FunctionCall": {
                        "method_name": "promise_create",
                        "args": "YXJncw==",
                        "gas": 0,
                        "deposit": "0"
                    }
                }
            ]
        },
        {
            "receiver_id": "rick.test",
            "actions": []
        },
        {
            "receiver_id": "rick.test",
            "actions": []
        }
    ]);
    assert_eq!(&serde_json::to_string(&vm_receipts(&logic)).unwrap(), &expected.to_string());
}
