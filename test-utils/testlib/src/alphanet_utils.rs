use std::net::SocketAddr;
use std::panic;
use std::path::PathBuf;
use std::process::Output;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use client::Client;
use configs::chain_spec::{read_or_default_chain_spec, ChainSpec};
use configs::network::get_peer_id_from_seed;
use configs::ClientConfig;
use configs::NetworkConfig;
use configs::RPCConfig;
use primitives::network::{PeerAddr, PeerInfo};
use primitives::signer::InMemorySigner;

use primitives::signer::TransactionSigner;
use primitives::signer::BlockSigner;

const TMP_DIR: &str = "../../tmp/testnet";

pub fn configure_chain_spec() -> ChainSpec {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("../../node/configs/res/testnet_chain.json");
    read_or_default_chain_spec(&Some(d))
}

pub struct Node {
    pub client: Arc<Client>,
    pub node_info: PeerInfo,
    pub client_cfg: ClientConfig,
    pub network_cfg: NetworkConfig,
    pub rpc_cfg: RPCConfig,
}

impl Node {
    pub fn for_test(test_prefix: &str, test_port: u16, account_id: &str, peer_id: u16, boot_nodes: Vec<PeerAddr>, chain_spec: ChainSpec) -> Self {
        let addr = format!("0.0.0.0:{}", test_port + peer_id);
        Self::new(
            &format!("{}_{}", test_prefix, account_id),
            account_id,
            u32::from(peer_id),
            Some(&addr),
            test_port + 1000 + peer_id,
            boot_nodes,
            chain_spec
        )
    }

    /// Create full node that does not accept incoming connections.
    pub fn for_test_passive(test_prefix: &str, test_port: u16, account_id: &str, peer_id: u16, boot_nodes: Vec<PeerAddr>, chain_spec: ChainSpec) -> Self {
        Self::new(
            &format!("{}_{}", test_prefix, account_id),
            account_id,
            u32::from(peer_id),
            None,
            test_port + 1000 + peer_id,
            boot_nodes,
            chain_spec
        )
    }
    pub fn new(
        name: &str,
        account_id: &str,
        peer_id_seed: u32,
        addr: Option<&str>,
        rpc_port: u16,
        boot_nodes: Vec<PeerAddr>,
        chain_spec: ChainSpec,
    ) -> Self {
        let node_info = PeerInfo {
            account_id: Some(String::from(account_id)),
            id: get_peer_id_from_seed(peer_id_seed),
            addr: if addr.is_some() {
                Some(SocketAddr::from_str(addr.unwrap()).unwrap())
            } else {
                None
            },
        };
        let mut base_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        base_path.push(TMP_DIR);
        base_path.push(name);

        if base_path.exists() {
            std::fs::remove_dir_all(base_path.clone()).unwrap();
        }

        let client_cfg = ClientConfig {
            base_path,
            account_id: String::from(account_id),
            public_key: None,
            chain_spec,
            log_level: log::LevelFilter::Info,
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

        let signer = Arc::new(InMemorySigner::from_seed(&account_id, &account_id));
        let client = Arc::new(Client::new_with_signer(&client_cfg, signer));
        Node { client, node_info, client_cfg, network_cfg, rpc_cfg }
    }

    #[inline]
    pub fn signer(&self) -> Arc<InMemorySigner> {
        self.client.signer.clone()
    }

    pub fn start(&self) {
        let client = self.client.clone();
        let account_id = self.client_cfg.account_id.clone();
        let network_cfg = self.network_cfg.clone();
        let rpc_cfg = self.rpc_cfg.clone();
        thread::spawn(|| {
            alphanet::start_from_client(client, Some(account_id), network_cfg, rpc_cfg);
        });
        thread::sleep(Duration::from_secs(1));
    }

    pub fn node_addr(&self) -> PeerAddr {
        let addr = self.network_cfg.listen_addr.expect("Node doesn't have an address");
        PeerAddr::parse(&format!("127.0.0.1:{}/{}", addr.port(), self.network_cfg.peer_id)).expect("Failed to parse")
    }
}

pub fn check_result(output: Output) -> Result<String, String> {
    let mut result = String::from_utf8_lossy(output.stdout.as_slice());
    if !output.status.success() {
        if result.is_empty() {
            result = String::from_utf8_lossy(output.stderr.as_slice());
        }
        return Err(result.to_owned().to_string());
    }
    Ok(result.to_owned().to_string())
}

pub fn wait<F>(f: F, check_interval_ms: u64, max_wait_ms: u64)
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

/// Generates chainspec for running multiple nodes.
pub fn generate_test_chain_spec(account_names: &Vec<String>, balance: u64) -> ChainSpec {
    let genesis_wasm = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
    let mut accounts = vec![];
    let mut initial_authorities = vec![];
    for name in account_names {
        let signer = InMemorySigner::from_seed(name.as_str(), name.as_str());
        accounts.push((name.to_string(), signer.public_key().to_readable(), balance, 10));
        initial_authorities.push((
            name.to_string(),
            signer.public_key().to_readable(),
            signer.bls_public_key().to_readable(),
            50,
        ));
    }
    let num_authorities = account_names.len();
    ChainSpec {
        accounts,
        initial_authorities,
        genesis_wasm,
        beacon_chain_epoch_length: 1,
        beacon_chain_num_seats_per_slot: num_authorities as u64,
        boot_nodes: vec![],
    }
}
