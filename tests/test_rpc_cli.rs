#[macro_use]
extern crate lazy_static;
extern crate serde_json;
extern crate service;

use service::test_utils::run_test_service;
use std::thread;
use std::process::Command;
use service::rpc::types::ViewAccountResponse;
use serde_json::Value;
use std::process::Output;
use std::borrow::Cow;
use service::rpc::types::CallViewFunctionResponse;

fn test_service_ready() -> bool {
    thread::spawn(|| run_test_service());
    true
}

lazy_static! {
    static ref DEVNET_STARTED: bool = test_service_ready();
}

fn check_result(output: &Output) -> Cow<str> {
    if !output.status.success() {
        panic!("{}", String::from_utf8_lossy(&output.stderr));
    }
    String::from_utf8_lossy(&output.stdout)
}

#[test]
fn test_send_money() {
    if !*DEVNET_STARTED {panic!()}
    let output = Command::new("./scripts/rpc.py")
        .arg("send_money")
        .output()
        .expect("send_money command failed to process");
    let result = check_result(&output);
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);
}

#[test]
fn test_view_account() {
    if !*DEVNET_STARTED {panic!()}
    let output = Command::new("./scripts/rpc.py")
        .arg("view_account")
        .output()
        .expect("view_account command failed to process");
    let result = check_result(&output);
    let _: ViewAccountResponse = serde_json::from_str(&result).unwrap();
}

#[test]
fn test_deploy() {
    if !*DEVNET_STARTED {panic!()}
    let output = Command::new("./scripts/rpc.py")
        .arg("deploy")
        .arg("test_contract_name")
        .arg("core/wasm/runtest/res/wasm_with_mem.wasm")
        .output()
        .expect("deploy command failed to process");
    let result = check_result(&output);
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);
}

#[test]
fn test_schedule_function_call() {
    if !*DEVNET_STARTED {panic!()}
    test_deploy();
    let output = Command::new("./scripts/rpc.py")
        .arg("schedule_function_call")
        .arg("test_contract_name")
        .arg("run_test")
        .output()
        .expect("schedule_function_call command failed to process");
    let result = check_result(&output);
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);
}

#[test]
fn test_call_view_function() {
    if !*DEVNET_STARTED {panic!()}
    test_deploy();
    let output = Command::new("./scripts/rpc.py")
        .arg("call_view_function")
        .arg("test_contract_name")
        .arg("run_test")
        .output()
        .expect("call_view_function command failed to process");
    let result = check_result(&output);
    let _: CallViewFunctionResponse = serde_json::from_str(&result).unwrap();
}
