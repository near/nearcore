extern crate core;
extern crate devnet;
#[macro_use]
extern crate lazy_static;
extern crate log;
extern crate node_http;
extern crate primitives;
extern crate rand;
#[macro_use]
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
    SignedBeaconBlockResponse, SignedShardBlockResponse, SubmitTransactionResponse,
    ViewAccountResponse, ViewStateResponse,
};
use primitives::signer::write_key_file;
use primitives::test_utils::get_key_pair_from_seed;

const TMP_DIR: &str = "./tmp/test_rpc_cli";
const KEY_STORE_PATH: &str = "./tmp/test_rpc_cli/key_store";
const WASM_PATH: &str = "./tests/hello";
const WAIT_FOR_RETRY: u64 = 500;
const MAX_WAIT_FOR_RETRY: u32 = 5;

fn test_service_ready() -> bool {
    let mut base_path = Path::new(TMP_DIR).to_owned();
    base_path.push("base");
    if base_path.exists() {
        std::fs::remove_dir_all(base_path.clone()).unwrap();
    }

    Command::new("sh")
        .arg("-c")
        .arg(&format!("cd {} && npm install && npm run build", WASM_PATH))
        .output()
        .expect("build hello.wasm failed");

    let network_cfg = devnet::NetworkConfig::default();
    let mut client_cfg = devnet::ClientConfig::default();
    client_cfg.base_path = base_path;
    client_cfg.log_level = log::LevelFilter::Off;
    let devnet_cfg = devnet::DevNetConfig { block_period: Duration::from_millis(5) };
    thread::spawn(|| { 
        devnet::start_devnet(Some(network_cfg), Some(client_cfg), Some(devnet_cfg))
    });
    thread::sleep(Duration::from_secs(1));
    true
}

fn get_public_key() -> String {
    let key_store_path = Path::new(KEY_STORE_PATH);
    let (public_key, secret_key) = get_key_pair_from_seed("alice.near");
    write_key_file(key_store_path, public_key, secret_key)
}

lazy_static! {
    static ref DEVNET_STARTED: bool = test_service_ready();
    static ref PUBLIC_KEY: String = get_public_key();
    static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
}

fn wait_for<R>(f: &Fn() -> Result<R, String>) -> Result<R, String> {
    let mut last_result = Err("Not executed".to_string());
    for _ in 0..MAX_WAIT_FOR_RETRY {
        last_result = f();
        if last_result.is_ok() {
            return last_result;
        }
        thread::sleep(Duration::from_millis(WAIT_FOR_RETRY));
    }
    return last_result;
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

fn deploy_contract() -> Result<(String, String), String> {
    let buster = rand::thread_rng().gen_range(0, 10000);
    let contract_name = format!("test_contract_{}", buster);

    Command::new("./scripts/rpc.py")
        .arg("deploy")
        .arg(contract_name.as_str())
        .arg("tests/hello.wasm")
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(&*PUBLIC_KEY)
        .output()
        .expect("deploy command failed to process");

    wait_for(&|| {
        let result = check_result(view_account(Some(&contract_name)));
        result.and_then(|res| {
            let new_account: Value = serde_json::from_str(&res).unwrap();
            if new_account != Value::Null { Ok(()) } else { Err("Account not created".to_string()) }
        })
    }).unwrap();
    let result = wait_for(&|| check_result(view_account(Some(&contract_name))))?;
    Ok((result, contract_name))
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
    let _: SubmitTransactionResponse = serde_json::from_str(&result).unwrap();
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
    let (result, contract_name) = deploy_contract().unwrap();
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data["account_id"], json!(contract_name));
}

test! { fn test_deploy() { test_deploy_inner() } }

#[allow(dead_code)]
fn test_set_get_values_inner() {
    if !*DEVNET_STARTED { panic!() }

    let account: Value = serde_json::from_str(&check_result(view_account(None)).unwrap()).unwrap();

    let (_, contract_name) = deploy_contract().unwrap();
    let output = Command::new("./scripts/rpc.py")
        .arg("schedule_function_call")
        .arg(&contract_name)
        .arg("setValue")
        .arg("--args")
        .arg("{\"value\": \"test\"}")
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(&*PUBLIC_KEY)
        .output()
        .expect("schedule_function_call command failed to process");
    let result = check_result(output).unwrap();
    let _: SubmitTransactionResponse = serde_json::from_str(&result).unwrap();

    // It takes more than two nonce changes for the action to propagate.
    wait_for(&|| {
        let new_account: Value = serde_json::from_str(&check_result(view_account(None))?).unwrap();
        if new_account["nonce"].as_u64().unwrap() > account["nonce"].as_u64().unwrap() + 1 { Ok(()) } else { Err("Nonce didn't change".to_string()) }
    }).unwrap();

    let output = Command::new("./scripts/rpc.py")
        .arg("call_view_function")
        .arg(contract_name)
        .arg("getValue")
        .arg("--args")
        .arg("{}")
        .output()
        .expect("call_view_function command failed to process");

    let result = check_result(output).unwrap();
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data["result"], json!("test"));
}

// TODO(#391): Disabled until the issue is fixed.
// test! { fn test_set_get_values() { test_set_get_values_inner() } }

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
    let output = create_account("eve.near");

    wait_for(&|| {
        let check_result = check_result(view_account(Some("eve.near")));
        check_result.and_then(|res| {
            let new_account: Value = serde_json::from_str(&res).unwrap();
            if new_account != Value::Null { Ok(()) } else { Err("Nonce didn't change".to_string()) }
        })
    }).unwrap();

    let result = check_result(output).unwrap();
    let _: SubmitTransactionResponse = serde_json::from_str(&result).unwrap();

    let output = Command::new("./scripts/rpc.py")
        .arg("view_account")
        .arg("--account")
        .arg("eve.near")
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
    let _: SubmitTransactionResponse = serde_json::from_str(&result).unwrap();
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
