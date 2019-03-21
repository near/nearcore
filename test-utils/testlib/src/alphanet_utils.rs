use std::{fs, panic};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Output};
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use client::Client;
use configs::chain_spec::{AuthorityRotation, ChainSpec, read_or_default_chain_spec, save_chain_spec};
use configs::ClientConfig;
use configs::network::get_peer_id_from_seed;
use configs::NetworkConfig;
use configs::RPCConfig;
use primitives::network::{PeerAddr, PeerInfo};
use primitives::signer::{BlockSigner, InMemorySigner, TransactionSigner};
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, Balance};

const TMP_DIR: &str = "../../tmp/testnet";

pub fn configure_chain_spec() -> ChainSpec {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("../../node/configs/res/poa_testnet_chain.json");
    read_or_default_chain_spec(&Some(d))
}

#[derive(PartialEq, Eq, Debug)]
pub enum NodeType {
    ThreadNode,
    ProcessNode,
}

pub enum ProcessNodeState {
    Stopped,
    Running(Child),
}

pub struct NodeConfig {
    pub node_info: PeerInfo,
    pub client_cfg: ClientConfig,
    pub network_cfg: NetworkConfig,
    pub rpc_cfg: RPCConfig,
    pub peer_id_seed: u32,
    pub node_type: NodeType,
}

impl NodeConfig {
    pub fn node_addr(&self) -> PeerAddr {
        let addr = self.network_cfg.listen_addr.expect("Node doesn't have an address");
        PeerAddr::parse(&format!("127.0.0.1:{}/{}", addr.port(), self.network_cfg.peer_id)).expect("Failed to parse")
    }
}

pub trait Node {
    fn config(&self) -> &NodeConfig;

    fn node_type(&self) -> NodeType;

    fn start(&mut self);

    fn view_balance(&self, account_id: &AccountId) -> Result<Balance, String>;

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String>;

    fn get_account_nonce(&self, account_id: &AccountId) -> Option<u64>;

    fn signer(&self) -> Arc<InMemorySigner>;

    fn as_process_mut(&mut self) -> &mut ProcessNode;

    fn as_thread_mut(&mut self) -> &mut ThreadNode;
}

impl Node {
    pub fn new(config: NodeConfig) -> Box<dyn Node> {
        match config.node_type {
            NodeType::ThreadNode => Box::new(ThreadNode::new(config)),
            NodeType::ProcessNode => Box::new(ProcessNode::new(config)),
        }
    }
}

pub struct ThreadNode {
    pub config: NodeConfig,
    pub client: Arc<Client>,
}

pub struct ProcessNode {
    pub config: NodeConfig,
    pub state: ProcessNodeState,
}

impl Node for ProcessNode {
    fn config(&self) -> &NodeConfig {
        &self.config
    }

    fn node_type(&self) -> NodeType {
        NodeType::ProcessNode
    }

    fn start(&mut self) {
        match self.state {
            ProcessNodeState::Stopped => {
                let child = self.get_start_node_command().spawn().expect("start node command failed");
                self.state = ProcessNodeState::Running(child);
                thread::sleep(Duration::from_secs(10));
            }
            ProcessNodeState::Running(_) => panic!("Node is already running"),
        }
    }

    fn view_balance(&self, _account_id: &String) -> Result<u64, String> {
        unimplemented!()
    }

    fn add_transaction(&self, _transaction: SignedTransaction) -> Result<(), String> {
        unimplemented!()
    }

    fn get_account_nonce(&self, _account_id: &String) -> Option<u64> {
        unimplemented!()
    }

    fn signer(&self) -> Arc<InMemorySigner> {
        let account_id = &self.config().client_cfg.account_id;
        Arc::new(InMemorySigner::from_seed(account_id, account_id))
    }

    fn as_process_mut(&mut self) -> &mut ProcessNode {
        self
    }

    fn as_thread_mut(&mut self) -> &mut ThreadNode {
        unimplemented!()
    }
}

impl Node for ThreadNode {
    fn config(&self) -> &NodeConfig {
        &self.config
    }

    fn node_type(&self) -> NodeType {
        NodeType::ThreadNode
    }

    fn start(&mut self) {
        let client = self.client.clone();
        let account_id = self.config().client_cfg.account_id.clone();
        let network_cfg = self.config().network_cfg.clone();
        let rpc_cfg = self.config().rpc_cfg.clone();
        thread::spawn(|| {
            alphanet::start_from_client(client, Some(account_id), network_cfg, rpc_cfg);
        });
        thread::sleep(Duration::from_secs(1));
    }

    fn view_balance(&self, account_id: &String) -> Result<u64, String> {
        let mut state_update = self.client.shard_client.get_state_update();
        self.client
            .shard_client
            .trie_viewer
            .view_account(&mut state_update, account_id)
            .map(|x| x.amount)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        self.client
            .shard_client
            .pool
            .add_transaction(
                transaction,
            )
    }

    fn get_account_nonce(&self, account_id: &String) -> Option<u64> {
        self.client
            .shard_client
            .get_account_nonce(account_id.clone())
    }

    fn signer(&self) -> Arc<InMemorySigner> {
        self.client.signer.clone()
    }

    fn as_process_mut(&mut self) -> &mut ProcessNode {
        unimplemented!()
    }

    fn as_thread_mut(&mut self) -> &mut ThreadNode {
        self
    }
}

impl ThreadNode {
    /// Side effects: create storage, open database, lock database
    pub fn new(config: NodeConfig) -> ThreadNode {
        let account_id = &config.client_cfg.account_id;
        let signer = Arc::new(InMemorySigner::from_seed(account_id, account_id));
        let client = Arc::new(Client::new_with_signer(&config.client_cfg, signer));
        ThreadNode { config, client }
    }
}

impl ProcessNode {
    /// Side effect: reset_storage
    pub fn new(config: NodeConfig) -> ProcessNode {
        let result = ProcessNode {
            config,
            state: ProcessNodeState::Stopped,
        };
        result.reset_storage();
        result
    }

    pub fn kill(&mut self) {
        match self.state {
            ProcessNodeState::Running(ref mut child) => {
                child.kill().expect("kill failed");
                thread::sleep(Duration::from_secs(1));
                self.state = ProcessNodeState::Stopped;
            }
            ProcessNodeState::Stopped => panic!("Invalid state"),
        }
    }

    /// Clear storage directory and run keygen
    pub fn reset_storage(&self) {
        let keygen_path = self.config().client_cfg.base_path.join("storage/keystore");
        Command::new("rm").args(&["-r", self.config().client_cfg.base_path.to_str().unwrap()]).spawn().unwrap().wait().unwrap();
        Command::new("cargo").args(&[
            "run",
            "--package", "keystore",
            "--",
            "keygen",
            "--test-seed", self.config().client_cfg.account_id.as_str(),
            "-p", keygen_path.to_str().unwrap(),
        ]).spawn().expect("keygen command failed").wait().expect("keygen command failed");
    }

    /// Side effect: writes chain spec file
    pub fn get_start_node_command(&self) -> Command {
        let account_name = self.config().client_cfg.account_id.clone();
        let pubkey = InMemorySigner::from_seed(&account_name, &account_name).public_key;
        let chain_spec = &self.config().client_cfg.chain_spec;
        let chain_spec_path = self.config().client_cfg.base_path.join("chain_spec.json");
        if !self.config().client_cfg.base_path.exists() {
            fs::create_dir_all(&self.config().client_cfg.base_path).unwrap();
        }
        save_chain_spec(&chain_spec_path, chain_spec.clone());

        let mut start_node_command = Command::new("cargo");
        start_node_command.args(&[
            "run",
            "--",
            "--rpc_port", format!("{}", self.config().rpc_cfg.rpc_port).as_str(),
            "--base-path", self.config().client_cfg.base_path.to_str().unwrap(),
            "--test-network-key-seed", format!("{}", self.config().peer_id_seed).as_str(),
            "--chain-spec-file", chain_spec_path.to_str().unwrap(),
            "-a", account_name.as_str(),
            "-k", format!("{}", pubkey).as_str(),
        ]);
        if let Some(ref addr) = self.config().node_info.addr {
            start_node_command.args(&["--addr", format!("{}", addr).as_str()]);
        }
        if !self.config().network_cfg.boot_nodes.is_empty() {
            let boot_node = format!("{}", self.config().network_cfg.boot_nodes[0]);
            start_node_command.args(&["--boot-nodes", boot_node.as_str()]);
        }
        start_node_command
    }
}

impl Drop for ProcessNode {
    fn drop(&mut self) {
        match self.state {
            ProcessNodeState::Running(ref mut child) => {
                child.kill().unwrap();
            }
            ProcessNodeState::Stopped => {}
        }
    }
}

impl NodeConfig {
    pub fn for_test(test_prefix: &str, test_port: u16, account_id: &str, peer_id: u16, boot_nodes: Vec<PeerAddr>, chain_spec: ChainSpec) -> Self {
        let addr = format!("0.0.0.0:{}", test_port + peer_id);
        Self::new(
            &format!("{}_{}", test_prefix, account_id),
            account_id,
            u32::from(peer_id),
            Some(&addr),
            test_port + 1000 + peer_id,
            boot_nodes,
            chain_spec,
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
            chain_spec,
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

        let node_type = NodeType::ThreadNode;

        NodeConfig { node_info, client_cfg, network_cfg, rpc_cfg, peer_id_seed, node_type }
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
            println!("BBBB Slept {}; max_wait_ms {}", ms_slept, max_wait_ms);
            panic!("Timed out waiting for the condition");
        }
    }
}

/// Generates chainspec for running multiple nodes.
pub fn generate_poa_test_chain_spec(account_names: &Vec<String>, balance: u64) -> ChainSpec {
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
    ChainSpec {
        accounts,
        initial_authorities,
        genesis_wasm,
        authority_rotation: AuthorityRotation::ProofOfAuthority,
        boot_nodes: vec![],
    }
}

// Create some nodes
pub fn create_nodes(num_nodes: usize, test_prefix: &str, test_port: u16) -> (u64, Vec<String>, Vec<NodeConfig>) {
    let init_balance = 1_000_000_000;
    let mut account_names = vec![];
    for i in 0..num_nodes {
        account_names.push(format!("near.{}", i));
    }
    let chain_spec = generate_poa_test_chain_spec(&account_names, init_balance);
    let mut nodes = vec![];
    let mut boot_nodes = vec![];
    // Launch nodes in a chain, such that X+1 node boots from X node.
    for (i, account_name) in account_names.iter().enumerate() {
        let node = NodeConfig::for_test(
            test_prefix,
            test_port,
            account_name.as_str(),
            i as u16 + 1,
            boot_nodes,
            chain_spec.clone(),
        );
        boot_nodes = vec![node.node_addr()];
        nodes.push(node);
    }
    (init_balance, account_names, nodes)
}

pub fn sample_two_nodes(num_nodes: usize) -> (usize, usize) {
    let i = rand::random::<usize>() % num_nodes;
    // Should be a different node.
    let mut j = rand::random::<usize>() % (num_nodes - 1);
    if j >= i {
        j += 1;
    }
    (i, j)
}

/// Sample a node for sending a transaction/checking balance
pub fn sample_queryable_node(nodes: &Vec<Box<Node>>) -> usize {
    let num_nodes = nodes.len();
    let mut k = rand::random::<usize>() % num_nodes;
    while nodes[k].node_type() == NodeType::ProcessNode {
        k = rand::random::<usize>() % num_nodes;
    }
    k
}