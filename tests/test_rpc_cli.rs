extern crate devnet;
extern crate keystore;
#[macro_use]
extern crate lazy_static;
extern crate node_rpc;
extern crate rand;
extern crate primitives;
extern crate serde_json;
extern crate core;

use std::borrow::Cow;
use std::panic;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use rand::Rng;
use serde_json::Value;

use node_rpc::types::{
    CallViewFunctionResponse, ViewAccountResponse,
};

const TMP_DIR: &str = "./tmp/test_rpc_cli";
const KEY_STORE_PATH: &str = "./tmp/test_rpc_cli/key_store";

fn test_service_ready() -> bool {
    let mut base_path = Path::new(TMP_DIR).to_owned();
    base_path.push("base");
    if base_path.exists() {
        std::fs::remove_dir_all(base_path.clone()).unwrap();
    }

    thread::spawn(move || { devnet::start_devnet(Some(&base_path)) });
    thread::sleep(Duration::from_secs(1));
    true
}

fn get_public_key() -> String {
    let key_store_path = Path::new(KEY_STORE_PATH);
    keystore::write_key_file(key_store_path)
}

lazy_static! {
    static ref DEVNET_STARTED: bool = test_service_ready();
    static ref PUBLIC_KEY: String = get_public_key();
    static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
}

fn check_result(output: &Output) -> Cow<str> {
    if !output.status.success() {
        panic!("{}", String::from_utf8_lossy(&output.stdout));
    }
    String::from_utf8_lossy(&output.stdout)
}

fn create_account(account_name: &str) -> Output {
    Command::new("./scripts/rpc.py")
        .arg("create_account")
        .arg(account_name)
        .arg("10")
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(&*PUBLIC_KEY)
        .output()
        .expect("create_account command failed to process")
}

fn deploy_contract() -> (Output, String) {
    let buster = rand::thread_rng().gen_range(0, 10000);
    let contract_name = format!("test_contract_{}", buster);
    create_account(contract_name.as_str());
    let output = Command::new("./scripts/rpc.py")
        .arg("deploy")
        .arg(contract_name.as_str())
        .arg("core/wasm/runtest/res/wasm_with_mem.wasm")
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(&*PUBLIC_KEY)
        .output()
        .expect("deploy command failed to process");
    (output, contract_name)
}

macro_rules! test {
    (fn $name:ident() $body:block) => {
        #[test]
        #[ignore]
        fn $name() {
            let guard = $crate::TEST_MUTEX.lock().unwrap();
            if let Err(e) = panic::catch_unwind(|| { $body }) {
                drop(guard);
                panic::resume_unwind(e);
            }
        }
    }
}

fn test_send_money_inner() {
    if !*DEVNET_STARTED { panic!() }
    let output = Command::new("./scripts/rpc.py")
        .arg("send_money")
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(&*PUBLIC_KEY)
        .output()
        .expect("send_money command failed to process");
    let result = check_result(&output);
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);
}

test! { fn test_send_money() { test_send_money_inner() } }

fn test_view_account_inner() {
    if !*DEVNET_STARTED { panic!() }
    let output = Command::new("./scripts/rpc.py")
        .arg("view_account")
        .output()
        .expect("view_account command failed to process");
    let result = check_result(&output);
    let _: ViewAccountResponse = serde_json::from_str(&result).unwrap();
}

test! { fn test_view_account() { test_view_account_inner() } }

fn test_deploy_inner() {
    if !*DEVNET_STARTED { panic!() }
    let (output, _) = deploy_contract();
    let result = check_result(&output);
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);
}

test! { fn test_deploy() { test_deploy_inner() } }

fn test_schedule_function_call_inner() {
    if !*DEVNET_STARTED { panic!() }
    let (_, contract_name) = deploy_contract();
    let output = Command::new("./scripts/rpc.py")
        .arg("schedule_function_call")
        .arg(contract_name)
        .arg("run_test")
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(&*PUBLIC_KEY)
        .output()
        .expect("schedule_function_call command failed to process");
    let result = check_result(&output);
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);
}

test! { fn test_schedule_function_call() { test_schedule_function_call_inner() } }

fn test_call_view_function_inner() {
    if !*DEVNET_STARTED { panic!() }
    let (_, contract_name) = deploy_contract();
    let output = Command::new("./scripts/rpc.py")
        .arg("call_view_function")
        .arg(contract_name)
        .arg("run_test")
        .output()
        .expect("call_view_function command failed to process");

    thread::sleep(Duration::from_secs(1));
    let result = check_result(&output);
    let _: CallViewFunctionResponse = serde_json::from_str(&result).unwrap();
}

test! { fn test_call_view_function() { test_call_view_function_inner() } }

fn test_create_account_inner() {
    if !*DEVNET_STARTED { panic!() }
    let output = create_account("eve");
    let result = check_result(&output);
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);

    let output = Command::new("./scripts/rpc.py")
        .arg("view_account")
        .arg("--account")
        .arg("eve")
        .output()
        .expect("view_account command failed to process");
    let result = check_result(&output);
    let _: ViewAccountResponse = serde_json::from_str(&result).unwrap();
}

test! { fn test_create_account() { test_create_account_inner() } }

fn test_swap_key_inner() {
    if !*DEVNET_STARTED { panic!() }
    let output = Command::new("./scripts/rpc.py")
        .arg("swap_key")
        .arg(&*PUBLIC_KEY)
        .arg(&*PUBLIC_KEY)
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(&*PUBLIC_KEY)
        .output()
        .expect("swap key command failed to process");
    let result = check_result(&output);
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);
}

test! { fn test_swap_key() { test_swap_key_inner() } }
