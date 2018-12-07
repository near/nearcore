//extern crate keystore;
//#[macro_use]
//extern crate lazy_static;
//extern crate node_rpc;
//extern crate primitives;
//extern crate serde_json;
//
//use serde_json::Value;
//use node_rpc::types::{
//    CallViewFunctionResponse, ViewAccountResponse,
//};
//use std::borrow::Cow;
//use std::path::Path;
//use std::process::{Command, Output};
//use std::thread;
//
//const KEY_STORE_PATH: &str = "/tmp/near_key";
//
//fn test_service_ready() -> bool {
//    thread::spawn(|| run_test_service());
//    true
//}
//
//fn get_public_key() -> String {
//    let key_store_path = Path::new(KEY_STORE_PATH);
//    keystore::write_key_file(key_store_path)
//}
//
//lazy_static! {
//    static ref DEVNET_STARTED: bool = test_service_ready();
//    static ref PUBLIC_KEY: String = get_public_key();
//}
//
//fn check_result(output: &Output) -> Cow<str> {
//    if !output.status.success() {
//        panic!("{}", String::from_utf8_lossy(&output.stderr));
//    }
//    String::from_utf8_lossy(&output.stdout)
//}
//
//#[test]
//#[ignore]
//fn test_send_money() {
//    if !*DEVNET_STARTED { panic!() }
//    let output = Command::new("./scripts/rpc.py")
//        .arg("send_money")
//        .arg("-d")
//        .arg(KEY_STORE_PATH)
//        .arg("-k")
//        .arg(&*PUBLIC_KEY)
//        .output()
//        .expect("send_money command failed to process");
//    let result = check_result(&output);
//    let data: Value = serde_json::from_str(&result).unwrap();
//    assert_eq!(data, Value::Null);
//}
//
//#[test]
//#[ignore]
//fn test_view_account() {
//    if !*DEVNET_STARTED { panic!() }
//    let output = Command::new("./scripts/rpc.py")
//        .arg("view_account")
//        .output()
//        .expect("view_account command failed to process");
//    let result = check_result(&output);
//    let _: ViewAccountResponse = serde_json::from_str(&result).unwrap();
//}
//
//#[test]
//#[ignore]
//fn test_deploy() {
//    if !*DEVNET_STARTED { panic!() }
//    let output = Command::new("./scripts/rpc.py")
//        .arg("deploy")
//        .arg("test_contract_name")
//        .arg("core/wasm/runtest/res/wasm_with_mem.wasm")
//        .arg("-d")
//        .arg(KEY_STORE_PATH)
//        .arg("-k")
//        .arg(&*PUBLIC_KEY)
//        .output()
//        .expect("deploy command failed to process");
//    let result = check_result(&output);
//    let data: Value = serde_json::from_str(&result).unwrap();
//    assert_eq!(data, Value::Null);
//}
//
//#[test]
//#[ignore]
//fn test_schedule_function_call() {
//    if !*DEVNET_STARTED { panic!() }
//    test_deploy();
//    let output = Command::new("./scripts/rpc.py")
//        .arg("schedule_function_call")
//        .arg("test_contract_name")
//        .arg("0")
//        .arg("run_test")
//        .arg("-d")
//        .arg(KEY_STORE_PATH)
//        .arg("-k")
//        .arg(&*PUBLIC_KEY)
//        .output()
//        .expect("schedule_function_call command failed to process");
//    let result = check_result(&output);
//    let data: Value = serde_json::from_str(&result).unwrap();
//    assert_eq!(data, Value::Null);
//}
//
//#[test]
//#[ignore]
//fn test_call_view_function() {
//    if !*DEVNET_STARTED { panic!() }
//    test_deploy();
//    let output = Command::new("./scripts/rpc.py")
//        .arg("call_view_function")
//        .arg("test_contract_name")
//        .arg("run_test")
//        .output()
//        .expect("call_view_function command failed to process");
//    let result = check_result(&output);
//    let _: CallViewFunctionResponse = serde_json::from_str(&result).unwrap();
//}
