use std::fs;
use std::net::SocketAddr;
use std::panic;
use std::path::PathBuf;
use std::process::{Child, Command, Output};
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use log::error;

use client::Client;
use configs::chain_spec::{AuthorityRotation, ChainSpec, DefaultIdType, TESTING_INIT_BALANCE};
use configs::network::get_peer_id_from_seed;
use configs::ClientConfig;
use configs::NetworkConfig;
use configs::RPCConfig;
use network::proxy::ProxyHandler;
use primitives::network::{PeerAddr, PeerInfo};
use primitives::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, Balance};
use tokio_utils::ShutdownableThread;

use crate::node_user::{NodeUser, RpcNodeUser, ThreadNodeUser};

const TMP_DIR: &str = "../../tmp/testnet";
pub const TEST_BLOCK_FETCH_LIMIT: u64 = 5;

pub fn configure_chain_spec() -> ChainSpec {
    ChainSpec::default_poa()
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

pub enum ThreadNodeState {
    Stopped,
    Running(ShutdownableThread),
}

pub struct NodeConfig {
    pub node_info: PeerInfo,
    pub client_cfg: ClientConfig,
    pub network_cfg: NetworkConfig,
    pub rpc_cfg: RPCConfig,
    pub peer_id_seed: u32,
    pub node_type: NodeType,
    pub proxy_handlers: Vec<Arc<ProxyHandler>>,
}

impl NodeConfig {
    pub fn node_addr(&self) -> PeerAddr {
        let addr = self.network_cfg.listen_addr.expect("Node doesn't have an address");
        PeerAddr::parse(&format!("127.0.0.1:{}/{}", addr.port(), self.network_cfg.peer_id))
            .expect("Failed to parse")
    }
}

pub trait Node {
    fn config(&self) -> &NodeConfig;

    fn node_type(&self) -> NodeType;

    fn start(&mut self);

    fn kill(&mut self);

    fn view_balance(&self, account_id: &AccountId) -> Result<Balance, String> {
        self.user().view_balance(account_id)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        self.user().add_transaction(transaction)
    }

    fn get_account_nonce(&self, account_id: &String) -> Option<u64> {
        self.user().get_account_nonce(account_id)
    }

    fn signer(&self) -> Arc<InMemorySigner>;

    fn as_process_mut(&mut self) -> &mut ProcessNode;

    fn as_thread_mut(&mut self) -> &mut ThreadNode;

    fn is_running(&self) -> bool;

    fn rpc_user(&self) -> RpcNodeUser {
        RpcNodeUser::new(self.config().rpc_cfg.rpc_port)
    }

    fn user(&self) -> Box<dyn NodeUser>;
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
    pub state: ThreadNodeState,
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
                let child =
                    self.get_start_node_command().spawn().expect("start node command failed");
                self.state = ProcessNodeState::Running(child);
                thread::sleep(Duration::from_secs(3));
            }
            ProcessNodeState::Running(_) => panic!("Node is already running"),
        }
    }

    fn kill(&mut self) {
        match self.state {
            ProcessNodeState::Running(ref mut child) => {
                child.kill().expect("kill failed");
                thread::sleep(Duration::from_secs(1));
                self.state = ProcessNodeState::Stopped;
            }
            ProcessNodeState::Stopped => panic!("Invalid state"),
        }
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

    fn is_running(&self) -> bool {
        match self.state {
            ProcessNodeState::Stopped => false,
            ProcessNodeState::Running(_) => true,
        }
    }

    fn user(&self) -> Box<NodeUser> {
        Box::new(self.rpc_user())
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
        let client_cfg = self.config().client_cfg.clone();
        let proxy_handlers = self.config().proxy_handlers.clone();
        let handle = alphanet::start_from_client(
            client,
            Some(account_id),
            network_cfg,
            rpc_cfg,
            client_cfg,
            proxy_handlers,
        );
        self.state = ThreadNodeState::Running(handle);
        thread::sleep(Duration::from_secs(1));
    }

    fn kill(&mut self) {
        let state = std::mem::replace(&mut self.state, ThreadNodeState::Stopped);
        match state {
            ThreadNodeState::Stopped => panic!("Node is not running"),
            ThreadNodeState::Running(handle) => {
                handle.shutdown();
            }
        }
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

    fn is_running(&self) -> bool {
        match self.state {
            ThreadNodeState::Stopped => false,
            ThreadNodeState::Running(_) => true,
        }
    }

    fn user(&self) -> Box<dyn NodeUser> {
        Box::new(ThreadNodeUser::new(self.client.clone()))
    }
}

impl ThreadNode {
    /// Side effects: create storage, open database, lock database
    pub fn new(config: NodeConfig) -> ThreadNode {
        let account_id = &config.client_cfg.account_id;
        let signer = Arc::new(InMemorySigner::from_seed(account_id, account_id));
        let client = Arc::new(Client::new_with_signer(&config.client_cfg, signer));
        let state = ThreadNodeState::Stopped;
        ThreadNode { config, client, state }
    }
}

impl ProcessNode {
    /// Side effect: reset_storage
    pub fn new(config: NodeConfig) -> ProcessNode {
        let result = ProcessNode { config, state: ProcessNodeState::Stopped };
        result.reset_storage();
        result
    }

    /// Clear storage directory and run keygen
    pub fn reset_storage(&self) {
        let keygen_path = self.config().client_cfg.base_path.join("storage/keystore");
        Command::new("rm")
            .args(&["-r", self.config().client_cfg.base_path.to_str().unwrap()])
            .spawn()
            .unwrap()
            .wait()
            .unwrap();
        Command::new("cargo")
            .args(&[
                "run",
                "--package",
                "keystore",
                "--",
                "keygen",
                "--test-seed",
                self.config().client_cfg.account_id.as_str(),
                "-p",
                keygen_path.to_str().unwrap(),
            ])
            .spawn()
            .expect("keygen command failed")
            .wait()
            .expect("keygen command failed");
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
        chain_spec.write_to_file(&chain_spec_path);

        let mut start_node_command = Command::new("cargo");
        start_node_command.args(&[
            "run",
            "--",
            "--rpc_port",
            format!("{}", self.config().rpc_cfg.rpc_port).as_str(),
            "--base-path",
            self.config().client_cfg.base_path.to_str().unwrap(),
            "--test-network-key-seed",
            format!("{}", self.config().peer_id_seed).as_str(),
            "--chain-spec-file",
            chain_spec_path.to_str().unwrap(),
            "-a",
            account_name.as_str(),
            "-k",
            format!("{}", pubkey).as_str(),
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
                let _ = child.kill().map_err(|_| error!("child process died"));
            }
            ProcessNodeState::Stopped => {}
        }
    }
}

impl NodeConfig {
    pub fn for_test(
        test_prefix: &str,
        test_port: u16,
        account_id: &str,
        peer_id: u16,
        boot_nodes: Vec<PeerAddr>,
        chain_spec: ChainSpec,
        block_fetch_limit: u64,
        proxy_handlers: Vec<Arc<ProxyHandler>>,
    ) -> Self {
        let addr = format!("0.0.0.0:{}", test_port + peer_id);
        Self::new(
            &format!("{}_{}", test_prefix, account_id),
            account_id,
            u32::from(peer_id),
            Some(&addr),
            test_port + 1000 + peer_id,
            boot_nodes,
            chain_spec,
            block_fetch_limit,
            proxy_handlers,
        )
    }

    /// Create full node that does not accept incoming connections.
    pub fn for_test_passive(
        test_prefix: &str,
        test_port: u16,
        account_id: &str,
        peer_id: u16,
        boot_nodes: Vec<PeerAddr>,
        chain_spec: ChainSpec,
        block_fetch_limit: u64,
        proxy_handlers: Vec<Arc<ProxyHandler>>,
    ) -> Self {
        Self::new(
            &format!("{}_{}", test_prefix, account_id),
            account_id,
            u32::from(peer_id),
            None,
            test_port + 1000 + peer_id,
            boot_nodes,
            chain_spec,
            block_fetch_limit,
            proxy_handlers,
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
        block_fetch_limit: u64,
        proxy_handlers: Vec<Arc<ProxyHandler>>,
    ) -> Self {
        let node_info = PeerInfo {
            account_id: Some(String::from(account_id)),
            id: get_peer_id_from_seed(Some(peer_id_seed)),
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
            block_fetch_limit,
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
            proxy_handlers: vec![],
        };

        let rpc_cfg = RPCConfig { rpc_port };

        let node_type = NodeType::ThreadNode;

        NodeConfig {
            node_info,
            client_cfg,
            network_cfg,
            rpc_cfg,
            peer_id_seed,
            node_type,
            proxy_handlers,
        }
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

// Create some nodes
pub fn create_nodes(
    num_nodes: usize,
    test_prefix: &str,
    test_port: u16,
    block_fetch_limit: u64,
    proxy_handlers: Vec<Arc<ProxyHandler>>,
) -> (u64, Vec<String>, Vec<NodeConfig>) {
    let (chain_spec, _) = ChainSpec::testing_spec(
        DefaultIdType::Enumerated,
        num_nodes,
        num_nodes,
        AuthorityRotation::ProofOfAuthority,
    );
    let account_names: Vec<_> = chain_spec.accounts.iter().map(|acc| acc.0.clone()).collect();
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
            block_fetch_limit,
            proxy_handlers.clone(),
        );
        boot_nodes = vec![node.node_addr()];
        nodes.push(node);
    }
    (TESTING_INIT_BALANCE, account_names, nodes)
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
    while !nodes[k].is_running() {
        k = rand::random::<usize>() % num_nodes;
    }
    k
}

/// TODO it makes sense to have three types of wait checks:
/// Wait until sufficient number of nodes is caught up (> 2/3). This can be checked by looking at the block indices and verifying that the blocks are produced;
/// Wait until a certain node is caught up and participating in a consensus. Check first-layer BLS signatures;
/// Wait until all nodes are more-or-less caught up. Check that the max_block_index - min_block_index < threshold;
///
pub fn wait_for_catchup(nodes: &Vec<Box<Node>>) {
    wait(
        || {
            let tips: Vec<_> = nodes
                .iter()
                .filter(|node| node.is_running())
                .map(|node| node.user().get_best_block_index())
                .collect();
            tips.iter().min() == tips.iter().max()
        },
        1000,
        10000,
    );
}
