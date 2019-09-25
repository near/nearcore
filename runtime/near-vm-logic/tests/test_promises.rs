mod fixtures;
mod helpers;

use crate::fixtures::get_context;
use crate::helpers::*;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{Config, VMLogic};
use serde_json;

#[test]
fn test_promise_results() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![]);
    let config = Config::default();

    let mut promise_results = vec![];
    promise_results.push(PromiseResult::Successful(b"test".to_vec()));
    promise_results.push(PromiseResult::Failed);
    promise_results.push(PromiseResult::NotReady);

    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

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
    let mut ext = MockedExternal::default();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should crate promise");

    promise_batch_action_function_call(&mut logic, 123, 0, 0).expect_err("shouldn't accept wrong promise index");

    promise_batch_action_function_call(&mut logic, index, 0, 0).expect("should add an action to receipt");
    let expected = r#"[{"receipt_indices":[],"receiver_id":"rick.test","actions":[{"FunctionCall":{"method_name":"promise_create","args":"args","gas":0,"deposit":0}},{"FunctionCall":{"method_name":"promise_batch_action","args":"promise_batch_action_args","gas":0,"deposit":0}}]}]"#;
    assert_eq!(&serde_json::to_string(ext.get_receipt_create_calls()).unwrap(), &expected);
}


#[test]
fn test_promise_batch_action_create_account() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should crate promise");

    logic.promise_batch_action_create_account(123).expect_err("shouldn't accept wrong promise index");

    logic.promise_batch_action_create_account(index).expect("should add an action to create account");

    let expected = r#"[{"receipt_indices":[],"receiver_id":"rick.test","actions":[{"FunctionCall":{"method_name":"promise_create","args":"args","gas":0,"deposit":0}},"CreateAccount"]}]"#;
    assert_eq!(&serde_json::to_string(ext.get_receipt_create_calls()).unwrap(), &expected);
}

#[test]
fn test_promise_batch_action_deploy_contract() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should crate promise");
    let code = b"sample";

    logic.promise_batch_action_deploy_contract(123, code.len() as u64, code.as_ptr() as _).expect_err("shouldn't accept wrong promise index");

    logic.promise_batch_action_deploy_contract(index, code.len() as u64, code.as_ptr() as _).expect("should add an action to deploy contract");
    let expected = "[{\"receipt_indices\":[],\"receiver_id\":\"rick.test\",\"actions\":[{\"FunctionCall\":{\"method_name\":\"promise_create\",\"args\":\"args\",\"gas\":0,\"deposit\":0}},{\"DeployContract\":{\"code\":[115,97,109,112,108,101]}}]}]";
    assert_eq!(&serde_json::to_string(ext.get_receipt_create_calls()).unwrap(), &expected);
}

#[test]
fn test_promise_batch_action_transfer() {
    let mut ext = MockedExternal::default();
    let mut context = get_context(vec![]);
    context.account_balance = 100;
    context.attached_deposit = 10;
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should crate promise");

    logic.promise_batch_action_transfer(123, 110u128.to_le_bytes().as_ptr() as _).expect_err("shouldn't accept wrong promise index");

    logic.promise_batch_action_transfer(index, 110u128.to_le_bytes().as_ptr() as _).expect("should add an action to transfer money");
    logic.promise_batch_action_transfer(index, 1u128.to_le_bytes().as_ptr() as _).expect_err("not enough money");
    let expected = "[{\"receipt_indices\":[],\"receiver_id\":\"rick.test\",\"actions\":[{\"FunctionCall\":{\"method_name\":\"promise_create\",\"args\":\"args\",\"gas\":0,\"deposit\":0}},{\"Transfer\":{\"deposit\":110}}]}]";
    assert_eq!(&serde_json::to_string(ext.get_receipt_create_calls()).unwrap(), &expected);
}


#[test]
fn test_promise_batch_action_stake() {
    let mut ext = MockedExternal::default();
    let mut context = get_context(vec![]);
    context.account_balance = 100;
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should crate promise");
    let key = b"ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf";

    logic.promise_batch_action_stake(123, 110u128.to_le_bytes().as_ptr() as _, key.len() as u64, key.as_ptr() as _).expect_err("shouldn't accept wrong promise index");

    logic.promise_batch_action_stake(index, 110u128.to_le_bytes().as_ptr() as _, key.len() as u64, key.as_ptr() as _).expect("should add an action to stake");
    logic.promise_batch_action_stake(index, 1u128.to_le_bytes().as_ptr() as _, key.len() as u64, key.as_ptr() as _).expect_err("not enough money to stake");
    let expected = "[{\"receipt_indices\":[],\"receiver_id\":\"rick.test\",\"actions\":[{\"FunctionCall\":{\"method_name\":\"promise_create\",\"args\":\"args\",\"gas\":0,\"deposit\":0}},{\"Stake\":{\"stake\":110,\"public_key\":\"RLb4qQXoZPAFqzZhiLFAcGFPFC7JWcDd8xKvQHHEqLUgDXuQkr2ehKAN28MNGQN9vUZ1qGZ\"}}]}]";
    assert_eq!(&serde_json::to_string(ext.get_receipt_create_calls()).unwrap(), &expected);
}

#[test]
fn test_promise_batch_action_add_key_with_function_call() {
    let mut ext = MockedExternal::default();
    let mut context = get_context(vec![]);
    context.account_balance = 100;
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should crate promise");
    let key = b"ed25519:5do5nkAEVhL8iteDvXNgxi4pWK78Y7DDadX11ArFNyrf";
    let nonce = 1;
    let allowance = 999u128;
    let receiver_id = b"sam";
    let method_names = b"foo,bar";

    promise_batch_action_add_key_with_function_call(&mut logic, 123, key, nonce, allowance, receiver_id, method_names).expect_err("shouldn't accept wrong promise index");

    promise_batch_action_add_key_with_function_call(&mut logic, index, key, nonce, allowance, receiver_id, method_names).expect("should add allowance");

    let expected = "[{\"receipt_indices\":[],\"receiver_id\":\"rick.test\",\"actions\":[{\"FunctionCall\":{\"method_name\":\"promise_create\",\"args\":\"args\",\"gas\":0,\"deposit\":0}},{\"AddKeyWithFunctionCall\":{\"public_key\":\"RLb4qQXoZPAFqzZhiLFAcGFPFC7JWcDd8xKvQHHEqLUgDXuQkr2ehKAN28MNGQN9vUZ1qGZ\",\"nonce\":1,\"allowance\":999,\"receiver_id\":\"sam\",\"method_names\":[\"foo\",\"bar\"]}}]}]";
    assert_eq!(&serde_json::to_string(ext.get_receipt_create_calls()).unwrap(), &expected);
}

#[test]
fn test_promise_batch_then() {
    let mut ext = MockedExternal::default();
    let mut context = get_context(vec![]);
    context.account_balance = 100;
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    let account_id = b"rick.test";
    let index = promise_create(&mut logic, account_id, 0, 0).expect("should crate promise");

    logic.promise_batch_then(index, account_id.len() as u64, account_id.as_ptr() as _);

    let expected = "[{\"receipt_indices\":[],\"receiver_id\":\"rick.test\",\"actions\":[{\"FunctionCall\":{\"method_name\":\"promise_create\",\"args\":\"args\",\"gas\":0,\"deposit\":0}}]},{\"receipt_indices\":[0],\"receiver_id\":\"rick.test\",\"actions\":[]}]";
    assert_eq!(&serde_json::to_string(ext.get_receipt_create_calls()).unwrap(), &expected);
}
