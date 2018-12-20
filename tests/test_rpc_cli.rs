extern crate core;
extern crate devnet;
#[macro_use]
extern crate lazy_static;
extern crate log;
extern crate node_http;
extern crate primitives;
extern crate rand;
extern crate serde_json;

use std::collections::HashMap;
use std::panic;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use rand::Rng;
use serde_json::Value;

use node_http::types::{
    CallViewFunctionResponse, SignedBeaconBlockResponse, SignedShardBlockResponse,
    ViewAccountResponse, ViewStateResponse,
};
use primitives::signer::write_key_file;

const TMP_DIR: &str = "./tmp/test_rpc_cli";
const KEY_STORE_PATH: &str = "./tmp/test_rpc_cli/key_store";

fn test_service_ready() -> bool {
    let mut base_path = Path::new(TMP_DIR).to_owned();
    base_path.push("base");
    if base_path.exists() {
        std::fs::remove_dir_all(base_path.clone()).unwrap();
    }

    let mut service_config = devnet::ServiceConfig::default();
    service_config.base_path = base_path;
    service_config.log_level = log::LevelFilter::Off;
    thread::spawn(|| { devnet::start_devnet(Some(service_config)) });
    thread::sleep(Duration::from_secs(1));
    true
}

fn get_public_key() -> String {
    let key_store_path = Path::new(KEY_STORE_PATH);
    write_key_file(key_store_path)
}

lazy_static! {
    static ref DEVNET_STARTED: bool = test_service_ready();
    static ref PUBLIC_KEY: String = get_public_key();
    static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
}

fn check_result(output: Output) -> Result<String, String> {
    let result = String::from_utf8_lossy(output.stdout.as_slice());
    if !output.status.success() {
        return Err(result.to_owned().to_string())
    }
    Ok(result.to_owned().to_string())
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

fn view_account(account_name: Option<&str>) -> Output {
    let mut args = vec!["view_account"];
    if let Some(a) = account_name {
        args.push("--account");
        args.push(a);
    };
    Command::new("./scripts/rpc.py")
        .args(args)
        .output()
        .expect("view_account command failed to process")
}

fn deploy_contract() -> Result<(Output, String), String> {
    let buster = rand::thread_rng().gen_range(0, 10000);
    let contract_name = format!("test_contract_{}", buster);
    create_account(contract_name.as_str());
    thread::sleep(Duration::from_millis(500));
    let account = view_account(Some(&contract_name));
    check_result(account)?;

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
    Ok((output, contract_name))
}

macro_rules! test {
    (fn $name:ident() $body:block) => {
        #[test]
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
    let result = check_result(output).unwrap();
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);
}

test! { fn test_send_money() { test_send_money_inner() } }

fn test_view_account_inner() {
    if !*DEVNET_STARTED { panic!() }
    let output = view_account(None);
    let result = check_result(output).unwrap();
    let _: ViewAccountResponse = serde_json::from_str(&result).unwrap();
}

test! { fn test_view_account() { test_view_account_inner() } }

fn test_deploy_inner() {
    if !*DEVNET_STARTED { panic!() }
    let (output, _) = deploy_contract().unwrap();
    let result = check_result(output).unwrap();
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);
}

test! { fn test_deploy() { test_deploy_inner() } }

fn test_schedule_function_call_inner() {
    if !*DEVNET_STARTED { panic!() }
    let (_, contract_name) = deploy_contract().unwrap();
    thread::sleep(Duration::from_millis(500));
    let output = Command::new("./scripts/rpc.py")
        .arg("schedule_function_call")
        .arg(contract_name)
        .arg("run_test")
        .arg("--args")
        .arg("{}")
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(&*PUBLIC_KEY)
        .output()
        .expect("schedule_function_call command failed to process");
    let result = check_result(output).unwrap();
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);
}

test! { fn test_schedule_function_call() { test_schedule_function_call_inner() } }

fn test_call_view_function_inner() {
    if !*DEVNET_STARTED { panic!() }
    let (_, contract_name) = deploy_contract().unwrap();
    let output = Command::new("./scripts/rpc.py")
        .arg("call_view_function")
        .arg(contract_name)
        .arg("run_test")
        .arg("--args")
        .arg("{}")
        .output()
        .expect("call_view_function command failed to process");

    thread::sleep(Duration::from_secs(1));
    let result = check_result(output).unwrap();
    let _: CallViewFunctionResponse = serde_json::from_str(&result).unwrap();
}

test! { fn test_call_view_function() { test_call_view_function_inner() } }

fn test_view_state_inner() {
    if !*DEVNET_STARTED { panic!() }
    let (_, contract_name) = deploy_contract().unwrap();
    let output = Command::new("./scripts/rpc.py")
        .arg("view_state")
        .arg(contract_name)
        .output()
        .expect("view_state command failed to process");
    let result = check_result(output).unwrap();
    let response: ViewStateResponse = serde_json::from_str(&result).unwrap();
    assert_eq!(response.values, HashMap::default());
}

test! { fn test_view_state() { test_view_state_inner() } }

fn test_create_account_inner() {
    if !*DEVNET_STARTED { panic!() }
    let output = create_account("eve");
    let result = check_result(output).unwrap();
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);

    let output = Command::new("./scripts/rpc.py")
        .arg("view_account")
        .arg("--account")
        .arg("eve")
        .output()
        .expect("view_account command failed to process");
    let result = check_result(output).unwrap();
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
    let result = check_result(output).unwrap();
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data, Value::Null);
}

test! { fn test_swap_key() { test_swap_key_inner() } }

fn get_latest_beacon_block() -> SignedBeaconBlockResponse {
    let output = Command::new("./scripts/rpc.py")
        .arg("view_latest_beacon_block")
        .output()
        .expect("view_latest_shard_block command failed to process");
    let result = check_result(output).unwrap();
    serde_json::from_str(&result).unwrap()
}

fn test_view_latest_beacon_block_inner() {
    if !*DEVNET_STARTED { panic!() }
    let _ = get_latest_beacon_block();
}

test! { fn test_view_latest_beacon_block() { test_view_latest_beacon_block_inner() } }

fn test_get_beacon_block_by_hash_inner() {
    if !*DEVNET_STARTED { panic!() }
    let latest_block = get_latest_beacon_block();
    let output = Command::new("./scripts/rpc.py")
        .arg("get_beacon_block_by_hash")
        .arg(String::from(&latest_block.hash))
        .output()
        .expect("view_latest_shard_block command failed to process");
    let result = check_result(output).unwrap();
    let block: SignedBeaconBlockResponse = serde_json::from_str(&result).unwrap();
    assert_eq!(latest_block, block);
}

test! { fn test_get_beacon_block_by_hash() { test_get_beacon_block_by_hash_inner() } }

fn get_latest_shard_block() -> SignedShardBlockResponse {
    let output = Command::new("./scripts/rpc.py")
        .arg("view_latest_shard_block")
        .output()
        .expect("view_latest_shard_block command failed to process");
    let result = check_result(output).unwrap();
    serde_json::from_str(&result).unwrap()
}

fn test_view_latest_shard_block_inner() {
    if !*DEVNET_STARTED { panic!() }
    let _ = get_latest_shard_block();
}

test! { fn test_view_latest_shard_block() { test_view_latest_shard_block_inner() } }

fn test_get_shard_block_by_hash_inner() {
    if !*DEVNET_STARTED { panic!() }
    let latest_block = get_latest_shard_block();
    let output = Command::new("./scripts/rpc.py")
        .arg("get_shard_block_by_hash")
        .arg(String::from(&latest_block.hash))
        .output()
        .expect("view_latest_shard_block command failed to process");
    let result = check_result(output).unwrap();
    let block: SignedShardBlockResponse = serde_json::from_str(&result).unwrap();
    assert_eq!(latest_block, block);
}

test! { fn test_get_shard_block_by_hash() { test_get_shard_block_by_hash_inner() } }
