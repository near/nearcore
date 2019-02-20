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
const WAIT_FOR_RETRY: u64 = 1000;
const MAX_WAIT_FOR_RETRY: u32 = 20;

fn install_npm() -> bool {
    let output = Command::new("sh")
        .arg("-c")
        .arg(&format!("cd {} && npm install && npm run build", WASM_PATH))
        .output()
        .expect("build hello.wasm failed");

    check_result(output).unwrap();
    true
}

fn test_service_ready(rpc_port: u16, test_name: &str) -> bool {
    let mut base_path = Path::new(TMP_DIR).to_owned();
    base_path.push("base");
    base_path.push(test_name);
    if base_path.exists() {
        std::fs::remove_dir_all(base_path.clone()).unwrap();
    }

    assert!(*NPM_INSTALLED);

    let mut client_cfg = configs::ClientConfig::default();
    client_cfg.base_path = base_path;
//    client_cfg.log_level = log::LevelFilter::Off;
    let devnet_cfg = configs::DevNetConfig { block_period: Duration::from_millis(5) };
    let rpc_cfg = configs::RPCConfig { rpc_port };
    thread::spawn(|| {
        devnet::start_from_configs(client_cfg, devnet_cfg, rpc_cfg);
    });
    wait_for(&|| get_latest_beacon_block(rpc_port)).unwrap();
    true
}

fn get_public_key() -> String {
    let key_store_path = Path::new(KEY_STORE_PATH);
    let (public_key, secret_key) = get_key_pair_from_seed("alice.near");
    write_key_file(key_store_path, public_key, secret_key)
}

lazy_static! {
    static ref PUBLIC_KEY: String = get_public_key();
    static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
    static ref NPM_INSTALLED: bool = install_npm();
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
    let mut result = String::from_utf8_lossy(output.stdout.as_slice());
    if !output.status.success() {
        if result.is_empty() {
            result = String::from_utf8_lossy(output.stderr.as_slice());
        }
        return Err(result.to_owned().to_string());
    }
    Ok(result.to_owned().to_string())
}

fn create_account(account_name: &str, rpc_port: u16) -> Output {
    let output = Command::new("./scripts/rpc.py")
        .arg("create_account")
        .arg(account_name)
        .arg("10")
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(&*PUBLIC_KEY)
        .arg("-u")
        .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
        .output()
        .expect("create_account command failed to process");

    wait_for(&|| {
        let result = check_result(view_account(Some(account_name), rpc_port));
        result.and_then(|res| {
            let new_account: Value = serde_json::from_str(&res).unwrap();
            if new_account != Value::Null {
                Ok(())
            } else {
                Err("Account not created".to_string())
            }
        })
    })
    .unwrap();

    output
}

fn view_account(account_name: Option<&str>, rpc_port: u16) -> Output {
    if let Some(a) = account_name {
        Command::new("./scripts/rpc.py")
            .arg("view_account")
            .arg("--account")
            .arg(a)
            .arg("-u")
            .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
            .output()
            .expect("view_account command failed to process")
    } else {
        Command::new("./scripts/rpc.py")
            .arg("view_account")
            .arg("-u")
            .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
            .output()
            .expect("view_account command failed to process")
    }
}

fn deploy_contract(rpc_port: u16) -> Result<(String, String), String> {
    let buster = rand::thread_rng().gen_range(0, 10000);
    let contract_name = format!("test_contract_{}", buster);

    let output = create_account(&contract_name, rpc_port);
    check_result(output).unwrap();

    let output = Command::new("./scripts/rpc.py")
        .arg("deploy")
        .arg(contract_name.as_str())
        .arg("tests/hello.wasm")
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(&*PUBLIC_KEY)
        .arg("-u")
        .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
        .output()
        .expect("deploy command failed to process");

    check_result(output).unwrap();

    wait_for(&|| {
        let result = check_result(view_account(Some(&contract_name), rpc_port));
        result.and_then(|res| {
            let new_account: Value = serde_json::from_str(&res).unwrap();
            if new_account != Value::Null {
                Ok(())
            } else {
                Err("Account not created".to_string())
            }
        })
    })
    .unwrap();
    let result = wait_for(&|| check_result(view_account(Some(&contract_name), rpc_port)))?;
    Ok((result, contract_name))
}

#[test]
fn test_send_money() {
    let rpc_port = 3030;
    assert!(test_service_ready(rpc_port, "test_send_money"));
    let buster = rand::thread_rng().gen_range(0, 10000);
    let receiver_name = format!("send_money_test_{}.near", buster);
    create_account(&receiver_name, rpc_port);
    let output = Command::new("./scripts/rpc.py")
        .arg("send_money")
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(&*PUBLIC_KEY)
        .arg("--receiver")
        .arg(&receiver_name)
        .arg("--amount")
        .arg("1")
        .arg("-u")
        .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
        .output()
        .expect("send_money command failed to process");
    let result = check_result(output).unwrap();
    let _: SubmitTransactionResponse = serde_json::from_str(&result).unwrap();

    wait_for(&|| {
        let result = check_result(view_account(Some(&receiver_name), rpc_port));
        result.and_then(|res| match serde_json::from_str::<ViewAccountResponse>(&res) {
            Ok(r) => {
                if r.amount == 11 {
                    Ok(())
                } else {
                    Err(format!("Balance should be 11, is {}", r.amount))
                }
            }
            Err(_) => Err("Account not created".to_string()),
        })
    })
    .unwrap();
}

#[test]
fn test_view_account() {
    let rpc_port = 3031;
    assert!(test_service_ready(rpc_port, "test_view_account"));
    let output = view_account(None, rpc_port);
    let result = check_result(output).unwrap();
    let _: ViewAccountResponse = serde_json::from_str(&result).unwrap();
}

#[test]
fn test_deploy() {
    let rpc_port = 3032;
    assert!(test_service_ready(rpc_port, "test_deploy"));
    let (result, contract_name) = deploy_contract(rpc_port).unwrap();
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data["account_id"], json!(contract_name));
}

#[test]
fn test_set_get_values() {
    let rpc_port = 3033;
    assert!(test_service_ready(rpc_port, "test_set_get_values"));

    let account: Value =
        serde_json::from_str(&check_result(view_account(None, rpc_port)).unwrap()).unwrap();

    let (_, contract_name) = deploy_contract(rpc_port).unwrap();
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
        .arg("-u")
        .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
        .output()
        .expect("schedule_function_call command failed to process");
    let result = check_result(output).unwrap();
    let _: SubmitTransactionResponse = serde_json::from_str(&result).unwrap();

    // TODO(581): Uncomment when it is solved.
    // It takes more than two nonce changes for the action to propagate.
    wait_for(&|| {
        let new_account: Value =
            serde_json::from_str(&check_result(view_account(None, rpc_port))?).unwrap();
        if new_account["nonce"].as_u64().unwrap() > account["nonce"].as_u64().unwrap() + 1 {
            Ok(())
        } else {
            Err("Nonce didn't change".to_string())
        }
    })
    .unwrap();
    // Waiting for nonce to increase seems to be not enough. Additionally, we need to sleep for 5 sec.
    thread::sleep(Duration::from_secs(5));

    let output = Command::new("./scripts/rpc.py")
        .arg("call_view_function")
        .arg(contract_name)
        .arg("getValue")
        .arg("-u")
        .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
        .arg("--args")
        .arg("{}")
        .output()
        .expect("call_view_function command failed to process");

    let result = check_result(output).unwrap();
    let data: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(data["result"], json!("test"));
}

#[test]
fn test_view_state() {
    let rpc_port = 3034;
    assert!(test_service_ready(rpc_port, "test_view_state"));
    let (_, contract_name) = deploy_contract(rpc_port).unwrap();
    let output = Command::new("./scripts/rpc.py")
        .arg("view_state")
        .arg(contract_name)
        .arg("-u")
        .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
        .output()
        .expect("view_state command failed to process");
    let result = check_result(output).unwrap();
    let response: ViewStateResponse = serde_json::from_str(&result).unwrap();
    assert_eq!(response.values, HashMap::default());
}

#[test]
fn test_create_account() {
    let rpc_port = 3035;
    assert!(test_service_ready(rpc_port, "test_create_account"));
    let output = create_account("eve.near", rpc_port);

    wait_for(&|| {
        let check_result = check_result(view_account(Some("eve.near"), rpc_port));
        check_result.and_then(|res| {
            let new_account: Value = serde_json::from_str(&res).unwrap();
            if new_account != Value::Null {
                Ok(())
            } else {
                Err("Nonce didn't change".to_string())
            }
        })
    })
    .unwrap();

    let result = check_result(output).unwrap();
    let _: SubmitTransactionResponse = serde_json::from_str(&result).unwrap();

    let output = Command::new("./scripts/rpc.py")
        .arg("view_account")
        .arg("--account")
        .arg("eve.near")
        .arg("-u")
        .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
        .output()
        .expect("view_account command failed to process");
    let result = check_result(output).unwrap();
    let _: ViewAccountResponse = serde_json::from_str(&result).unwrap();
}

#[test]
fn test_swap_key() {
    let rpc_port = 3036;
    assert!(test_service_ready(rpc_port, "test_swap_key"));
    let output = Command::new("./scripts/rpc.py")
        .arg("swap_key")
        .arg(&*PUBLIC_KEY)
        .arg(&*PUBLIC_KEY)
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(&*PUBLIC_KEY)
        .arg("-u")
        .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
        .output()
        .expect("swap key command failed to process");
    let result = check_result(output).unwrap();
    let _: SubmitTransactionResponse = serde_json::from_str(&result).unwrap();
}

fn get_latest_beacon_block(rpc_port: u16) -> Result<SignedBeaconBlockResponse, String> {
    let output = Command::new("./scripts/rpc.py")
        .arg("view_latest_beacon_block")
        .arg("-u")
        .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
        .output()
        .expect("view_latest_shard_block command failed to process");
    let result = check_result(output)?;
    serde_json::from_str(&result).map_err(|e| format!("{}", e).to_string())
}

#[test]
fn test_view_latest_beacon_block() {
    let rpc_port = 3037;
    assert!(test_service_ready(rpc_port, "test_view_latest_beacon_block"));
    let _ = get_latest_beacon_block(rpc_port).unwrap();
}

#[test]
fn test_get_beacon_block_by_hash() {
    let rpc_port = 3038;
    assert!(test_service_ready(rpc_port, "test_get_beacon_block_by_hash"));
    let latest_block = get_latest_beacon_block(rpc_port).unwrap();
    let output = Command::new("./scripts/rpc.py")
        .arg("get_beacon_block_by_hash")
        .arg(String::from(&latest_block.hash))
        .arg("-u")
        .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
        .output()
        .expect("view_latest_shard_block command failed to process");
    let result = check_result(output).unwrap();
    let block: SignedBeaconBlockResponse = serde_json::from_str(&result).unwrap();
    assert_eq!(latest_block, block);
}

fn get_latest_shard_block(rpc_port: u16) -> SignedShardBlockResponse {
    let output = Command::new("./scripts/rpc.py")
        .arg("view_latest_shard_block")
        .arg("-u")
        .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
        .output()
        .expect("view_latest_shard_block command failed to process");
    let result = check_result(output).unwrap();
    serde_json::from_str(&result).unwrap()
}

#[test]
fn test_view_latest_shard_block() {
    let rpc_port = 3039;
    assert!(test_service_ready(rpc_port, "test_view_latest_shard_block"));
    let _ = get_latest_shard_block(rpc_port);
}

#[test]
fn test_get_shard_block_by_hash() {
    let rpc_port = 3040;
    assert!(test_service_ready(rpc_port, "test_get_shard_block_by_hash"));
    let latest_block = get_latest_shard_block(rpc_port);
    let output = Command::new("./scripts/rpc.py")
        .arg("get_shard_block_by_hash")
        .arg(String::from(&latest_block.hash))
        .arg("-u")
        .arg(format!("http://127.0.0.1:{}/", rpc_port).as_str())
        .output()
        .expect("view_latest_shard_block command failed to process");
    let result = check_result(output).unwrap();
    let block: SignedShardBlockResponse = serde_json::from_str(&result).unwrap();
    assert_eq!(latest_block, block);
}
