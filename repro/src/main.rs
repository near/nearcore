use near_workspaces::{Account, types::NearToken};

#[tokio::main]
async fn main() {
    let worker = near_workspaces::custom("http://127.0.0.1:63963").await.unwrap();
    let root_account = Account::from_file("/tmp/.near/validator_key.json", &worker).unwrap();
    let defuse_wasm = include_bytes!("../defuse.wasm");
    let tm = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let account = root_account.create_subaccount(&format!("contract{tm}"))
            .initial_balance(NearToken::from_near(15))
            .transact()
            .await
            .unwrap()
            .unwrap();
    let _ = account.deploy(defuse_wasm).await.unwrap().unwrap();
}
