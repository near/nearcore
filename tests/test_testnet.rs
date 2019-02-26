use std::net::SocketAddr;
use std::panic;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use client::{ChainConsensusBlockBody, Client};
use configs::chain_spec::read_or_default_chain_spec;
use configs::ClientConfig;
use configs::network::get_peer_id_from_seed;
use configs::NetworkConfig;
use configs::RPCConfig;
use primitives::block_traits::SignedBlock;
use primitives::chain::ChainPayload;
use primitives::network::PeerInfo;
use primitives::signer::write_key_file;
use primitives::test_utils::get_key_pair_from_seed;

const TMP_DIR: &str = "./tmp/testnet";
const KEY_STORE_PATH: &str = "./tmp/testnet/key_store";

struct Node {
    pub client: Arc<Client>,
    pub node_info: PeerInfo,
    pub client_cfg: ClientConfig,
    pub network_cfg: NetworkConfig,
    pub rpc_cfg: RPCConfig,
}

impl Node {
    pub fn new(name: &str, account_id: &str, peer_id_seed: u32, addr: &str, rpc_port: u16, boot_nodes: Vec<PeerInfo>) -> Self {
        let node_info = PeerInfo {
            account_id: Some(String::from(account_id)),
            id: get_peer_id_from_seed(peer_id_seed),
            addr: SocketAddr::from_str(addr).unwrap(),
        };
        let mut base_path = PathBuf::from(TMP_DIR);
        base_path.push(name);

        if base_path.exists() {
            std::fs::remove_dir_all(base_path.clone()).unwrap();
        }

        let client_cfg = ClientConfig {
            base_path,
            account_id: String::from(account_id),
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

        let client = Arc::new(Client::new(&client_cfg));
        Node {
            client,
            node_info,
            client_cfg,
            network_cfg,
            rpc_cfg
        }
    }

    pub fn start(&self) {
        let client = self.client.clone();
        let account_id = self.client_cfg.account_id.clone();
        let network_cfg = self.network_cfg.clone();
        let rpc_cfg = self.rpc_cfg.clone();
        thread::spawn(|| {
            testnet::start_from_client(client, account_id, network_cfg, rpc_cfg);
        });
        thread::sleep(Duration::from_secs(1));
    }
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

fn get_public_key() -> String {
    let key_store_path = Path::new(KEY_STORE_PATH);
    let (public_key, secret_key) = get_key_pair_from_seed("alice.near");
    write_key_file(key_store_path, public_key, secret_key)
}

#[test]
fn test_two_nodes() {
    // Create boot node.
    let alice = Node::new("node_alice", "alice.near", 1, "127.0.0.1:3000", 3030, vec![]);
    // Create secondary node that boots from the alice node.
    let bob = Node::new("node_bob", "bob.near", 2, "127.0.0.1:3001", 3031, vec![alice.node_info.clone()]);

    // Start both nodes.
    alice.start();
    bob.start();

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
fn test_two_nodes_sync() {
    let alice = Node::new("node_alice", "alice.near", 1, "127.0.0.1:3000", 3030, vec![]);
    let bob = Node::new("node_bob", "bob.near", 2, "127.0.0.1:3001", 3031, vec![alice.node_info.clone()]);

    let payload = ChainConsensusBlockBody { payload: ChainPayload { transactions: vec![], receipts: vec![] }, beacon_block_index: 1 };
    let (beacon_block, shard_block) = alice.client.produce_block(payload).unwrap();
    alice.client.import_blocks(beacon_block, shard_block);

    alice.start();
    bob.start();

    wait(|| {
        bob.client.shard_chain.chain.best_block().index() == 1
    }, 500, 10000);
}

//#[test]
//fn test_three_nodes_tx_sync() {
//    let alice = Node::new("node_alice", "alice.near", 1, "127.0.0.1:3000", 3030, vec![]);
//    let bob = Node::new("node_bob", "bob.near", 2, "127.0.0.1:3001", 3031, vec![alice.node_info.clone()]);
//    let john = Node::new("node_john", "john.near", 3, "127.0.0.1:3002", 3032, vec![bob.node_info.clone()]);
//
//    let tx = Transaction::new(..);
//    alice.client.shard_chain.send_transaction(tx);
//
//    alice.start();
//    bob.start();
//    john.start();
//
//    wait(|| {
//        bob.client.shard_chain.mempool.contains(tx.hash) && john.client.shard_chain.mempool.contains(tx.hash);
//    }, 500, 10000);
//}