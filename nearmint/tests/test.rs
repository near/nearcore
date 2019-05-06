use node_runtime::state_viewer::AccountViewCallResult;
use primitives::rpc::JsonRpcResponse;
use protobuf::Message;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use testlib::test_helpers::wait;

/// Test node that contains the subprocesses of tendermint and nearmint.
/// Used for shutting down the processes gracefully.
struct TestNode {
    tendermint: Child,
    nearmint: Child,
    storage_path: String,
}

impl TestNode {
    fn kill(&mut self) {
        self.tendermint.kill().expect("fail to kill tendermint node");
        self.nearmint.kill().expect("fail to kill nearmint");
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.kill();
        Command::new("tendermint")
            .arg("unsafe_reset_all")
            .output()
            .expect("fail to reset tendermint");
        Command::new("rm")
            .args(&["-rf", &self.storage_path])
            .output()
            .expect("fail to delete test storage");
    }
}

fn start_nearmint(path: &str) -> TestNode {
    let tendermint = Command::new("tendermint")
        .args(&["node", "--rpc.laddr", "tcp://0.0.0.0:3030"])
        .spawn()
        .expect("fail to spawn tendermint");
    let nearmint = Command::new("cargo")
        .args(&["run", "--package", "nearmint", "--", "--base-path", path, "--devnet"])
        .spawn()
        .expect("fail to spawn nearmint");
    wait(
        || {
            let client = reqwest::Client::new();
            let response = client.post("http://127.0.0.1:3030/health").send();
            response.is_ok()
        },
        1000,
        60000,
    );
    thread::sleep(Duration::from_secs(5));
    TestNode { tendermint, nearmint, storage_path: path.to_string() }
}

fn view_account_request(account_id: &str) -> Option<AccountViewCallResult> {
    let client = reqwest::Client::new();
    let mut response = client
        .post("http://127.0.0.1:3030/abci_query")
        .form(&[("path", format!("\"account/{}\"", account_id))])
        .send()
        .unwrap();
    let response: JsonRpcResponse = response.json().expect("cannot decode response");
    response
        .result
        .unwrap()
        .as_object()
        .and_then(|m| m.get("response"))
        .unwrap()
        .as_object()
        .and_then(|m| m.get("value"))
        .and_then(|v| {
            let bytes = base64::decode(v.as_str().unwrap()).unwrap();
            serde_json::from_str::<AccountViewCallResult>(std::str::from_utf8(&bytes).unwrap()).ok()
        })
}

fn submit_tx(tx: near_protos::signed_transaction::SignedTransaction) -> JsonRpcResponse {
    let client = reqwest::Client::new();
    let tx_bytes = tx.write_to_bytes().expect("write to bytes failed");
    let mut response = client
        .post("http://127.0.0.1:3030/broadcast_tx_commit")
        .form(&[("tx", format!("0x{}", hex::encode(&tx_bytes)))])
        .send()
        .unwrap();
    let response: JsonRpcResponse = response.json().expect("cannot decode response");
    response
}

#[cfg(test)]
mod test {
    use super::*;
    use node_runtime::chain_spec::TESTING_INIT_BALANCE;
    use primitives::crypto::signer::InMemorySigner;
    use primitives::hash::hash;
    use primitives::transaction::{
        CreateAccountTransaction, DeployContractTransaction, TransactionBody,
    };
    use testlib::test_helpers::heavy_test;

    #[test]
    fn test_send_tx() {
        heavy_test(|| {
            let storage_path = "tmp/test_send_tx";
            let _test_node = start_nearmint(storage_path);
            let signer = InMemorySigner::from_seed("alice.near", "alice.near");
            let tx: near_protos::signed_transaction::SignedTransaction =
                TransactionBody::send_money(1, "alice.near", "bob.near", 10).sign(&signer).into();
            submit_tx(tx);

            let alice_account = view_account_request("alice.near").unwrap();
            assert_eq!(alice_account.amount, TESTING_INIT_BALANCE - 10);
            let bob_account = view_account_request("bob.near").unwrap();
            assert_eq!(bob_account.amount, TESTING_INIT_BALANCE + 10);
        });
    }

    #[test]
    fn test_create_account() {
        heavy_test(|| {
            let storage_path = "tmp/test_create_account";
            let _test_node = start_nearmint(storage_path);
            let signer = InMemorySigner::from_seed("alice.near", "alice.near");
            let tx: near_protos::signed_transaction::SignedTransaction =
                TransactionBody::CreateAccount(CreateAccountTransaction {
                    nonce: 1,
                    originator: "alice.near".to_string(),
                    new_account_id: "test.near".to_string(),
                    amount: 10,
                    public_key: signer.public_key.0[..].to_vec(),
                })
                .sign(&signer)
                .into();
            submit_tx(tx);

            let alice_account = view_account_request("alice.near").unwrap();
            assert_eq!(alice_account.amount, TESTING_INIT_BALANCE - 10);
            let eve_account = view_account_request("test.near").unwrap();
            assert_eq!(eve_account.amount, 10);
        });
    }

    #[test]
    fn test_deploy_contract() {
        heavy_test(|| {
            let storage_path = "tmp/test_create_account";
            let _test_node = start_nearmint(storage_path);
            let signer = InMemorySigner::from_seed("alice.near", "alice.near");
            let tx: near_protos::signed_transaction::SignedTransaction =
                TransactionBody::CreateAccount(CreateAccountTransaction {
                    nonce: 1,
                    originator: "alice.near".to_string(),
                    new_account_id: "test.near".to_string(),
                    amount: 10,
                    public_key: signer.public_key.0[..].to_vec(),
                })
                .sign(&signer)
                .into();
            submit_tx(tx);

            let wasm_binary: &[u8] = include_bytes!("../../tests/hello.wasm");
            let tx: near_protos::signed_transaction::SignedTransaction =
                TransactionBody::DeployContract(DeployContractTransaction {
                    nonce: 1,
                    contract_id: "test.near".to_string(),
                    wasm_byte_array: wasm_binary.to_vec(),
                })
                .sign(&signer)
                .into();
            submit_tx(tx);
            let eve_account = view_account_request("test.near").unwrap();
            assert_eq!(eve_account.amount, 10);
            assert_eq!(eve_account.code_hash, hash(wasm_binary));
        });
    }
}
