use near_workspaces::{Account, types::NearToken};
use wasm_encoder::{Module, reencode::{RoundtripReencoder, utils::parse_core_module}};

#[tokio::main]
async fn main() {
    let worker = near_workspaces::custom("http://127.0.0.1:63963").await.unwrap();
    let root_account = Account::from_file("/tmp/.near/validator_key.json", &worker).unwrap();
    let defuse_wasm = include_bytes!("../defuse.wasm");
    let tm = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let modified_wasm = modify_wasm(defuse_wasm, tm.to_be_bytes().to_vec());
    let account = root_account.create_subaccount(&format!("contract{tm}"))
            .initial_balance(NearToken::from_near(15))
            .transact()
            .await
            .unwrap()
            .unwrap();
    let _ = account.deploy(&modified_wasm).await.unwrap().unwrap();
}

// Modify wasm to only change its hash.
// Allows deploying contract multiple times bypassing contract runtime caching.
fn modify_wasm(wasm_bytes: &[u8], salt_data: Vec<u8>) -> Vec<u8> {
    let mut module = Module::new();
    parse_core_module(
        &mut RoundtripReencoder,
        &mut module,
        wasmparser::Parser::new(0),
        wasm_bytes
    ).unwrap();
    module.section(&wasm_encoder::CustomSection {
        name: "hash-salt".into(),
        data: salt_data.into(),
    });
    let ret  = module.finish();
    ret
}
