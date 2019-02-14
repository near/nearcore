use std::panic;
use std::path::Path;
use std::process::{Command, Output};
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use configs::chain_spec::read_or_default_chain_spec;
use configs::network::get_peer_id_from_seed;
use configs::ClientConfig;
use configs::NetworkConfig;
use configs::RPCConfig;
use primitives::network::PeerInfo;
use primitives::signer::write_key_file;
use primitives::test_utils::get_key_pair_from_seed;
use std::net::SocketAddr;
use std::path::PathBuf;

fn test_node_ready(
    base_path: PathBuf,
    node_info: PeerInfo,
    rpc_port: u16,
    boot_nodes: Vec<PeerInfo>,
) {
    if base_path.exists() {
        std::fs::remove_dir_all(base_path.clone()).unwrap();
    }

    let client_cfg = ClientConfig {
        base_path,
        account_id: node_info.account_id.unwrap(),
        public_key: None,
        chain_spec: read_or_default_chain_spec(&Some(PathBuf::from(
            "./node/configs/res/testnet_chain.json",
        ))),
        log_level: log::LevelFilter::Off,
    };

    let network_cfg = NetworkConfig {
        listen_addr: node_info.addr,
        peer_id: node_info.id,
        boot_nodes,
        reconnect_delay: Duration::from_millis(50),
        gossip_interval: Duration::from_millis(50),
        gossip_sample_size: 10,
    };

    let rpc_cfg = RPCConfig { rpc_port };
    thread::spawn(|| {
        testnet::start_from_configs(client_cfg, network_cfg, rpc_cfg);
    });
    thread::sleep(Duration::from_secs(1));
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

const TMP_DIR: &str = "./tmp/testnet";
const KEY_STORE_PATH: &str = "./tmp/testnet/key_store";

fn get_public_key() -> String {
    let key_store_path = Path::new(KEY_STORE_PATH);
    let (public_key, secret_key) = get_key_pair_from_seed("alice.near");
    write_key_file(key_store_path, public_key, secret_key)
}

fn start_testnet() {
    // Start boot node.
    let mut base_path = PathBuf::from(TMP_DIR);
    base_path.push("node_alice");
    let alice_info = PeerInfo {
        account_id: Some(String::from("alice.near")),
        id: get_peer_id_from_seed(1),
        addr: SocketAddr::from_str("127.0.0.1:3000").unwrap(),
    };
    test_node_ready(base_path, alice_info.clone(), 3030, vec![]);

    // Start secondary node that boots from the alice node.
    let mut base_path = PathBuf::from(TMP_DIR);
    base_path.push("node_bob");
    let bob_info = PeerInfo {
        account_id: Some(String::from("bob.near")),
        id: get_peer_id_from_seed(2),
        addr: SocketAddr::from_str("127.0.0.1:3001").unwrap(),
    };
    test_node_ready(base_path, bob_info.clone(), 3031, vec![alice_info]);

    // Create an account on alice node.
    Command::new("./scripts/rpc.py")
        .arg("create_account")
        .arg("jason")
        .arg("1")
        .arg("-d")
        .arg(KEY_STORE_PATH)
        .arg("-k")
        .arg(get_public_key())
        .arg("-u")
        .arg("http://127.0.0.1:3030/")
        .output()
        .expect("create_account command failed to process");

    // Wait until this account is present on the bob.near node.
    let view_account = || -> bool {
        let res = Command::new("./scripts/rpc.py")
            .arg("view_account")
            .arg("-a")
            .arg("jason")
            .arg("-u")
            .arg("http://127.0.0.1:3031/")
            .output()
            .expect("view_account command failed to process");
        check_result(res).is_ok()
    };
    wait(view_account, 500, 60000);
}

#[test]
fn test_two_nodes() {
    start_testnet();
}

fn wait<F>(f: F, check_interval_ms: u64, max_wait_ms: u64)
where
    F: Fn() -> bool,
{
    let mut ms_slept = 0;
    while !f() {
        thread::sleep(Duration::from_millis(check_interval_ms));
        ms_slept += check_interval_ms;
        if ms_slept > max_wait_ms {
            panic!("Timed out waiting for the condition");
        }
    }
}
