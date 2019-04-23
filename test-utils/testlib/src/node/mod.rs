use std::net::SocketAddr;
use std::panic;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use node_runtime::chain_spec::{AuthorityRotation, ChainSpec, DefaultIdType, TESTING_INIT_BALANCE};
use configs::network::get_peer_id_from_seed;
use configs::ClientConfig;
use configs::NetworkConfig;
use configs::RPCConfig;
use network::proxy::ProxyHandler;
use primitives::crypto::signer::InMemorySigner;
use primitives::network::{PeerAddr, PeerInfo};
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, Balance};

use crate::user::{AsyncUser, User};
use std::sync::RwLock;
pub mod thread_node;
pub use thread_node::ThreadNode;
pub mod process_node;
use crate::node::remote_node::RemoteNode;
pub use process_node::ProcessNode;
pub mod remote_node;
pub mod runtime_node;
pub use runtime_node::RuntimeNode;
pub mod shard_client_node;
pub use shard_client_node::ShardClientNode;

const TMP_DIR: &str = "../../tmp/testnet";
pub const TEST_BLOCK_FETCH_LIMIT: u64 = 5;
pub const TEST_BLOCK_MAX_SIZE: u32 = 1000;

pub fn configure_chain_spec() -> ChainSpec {
    ChainSpec::default_poa()
}

/// Config that can be used to start a node or connect to an existing node.
#[allow(clippy::large_enum_variant)]
pub enum NodeConfig {
    /// A complete node with network, RPC, client, consensus and all tasks running in a thead.
    /// Should be the default choice for the tests, since it provides the most control through the
    /// internal access.
    Thread(LocalNodeConfig),
    /// A complete noe running in a subprocess. Can be started and stopped, but besides that all
    /// interactions are limited to what is exposed through RPC.
    Process(LocalNodeConfig),
    /// A node running remotely, which we cannot start or stop, but can communicate with via RPC.
    Remote { addr: SocketAddr, signer: Arc<InMemorySigner> },
    /// A node with only runtime and state that is used to run runtime tests
    Runtime { account_id: AccountId },
    /// A node with shard client that has chain spec and mempool, which allows for more integrated
    /// testing without network
    ShardClient { client_cfg: ClientConfig },
}

impl NodeConfig {
    /// Get `<ip>:port/<peer_id>` that other nodes can use for booting from this node.
    pub fn boot_addr(&self) -> PeerAddr {
        match self {
            NodeConfig::Thread(local_cfg) | NodeConfig::Process(local_cfg) => {
                let addr = local_cfg.network_cfg.listen_addr.expect("Node doesn't have an address");
                PeerAddr::parse(&format!(
                    "127.0.0.1:{}/{}",
                    addr.port(),
                    local_cfg.network_cfg.peer_id
                ))
                .expect("Failed to parse")
            }
            _ => unimplemented!(),
        }
    }
}

/// Config of a node running either inside a thread or inside a subprocess.
pub struct LocalNodeConfig {
    // TODO(#847): Clean-up LocalNodeConfig by making it different for thread and process node, because
    // they are using different fields for initialization.
    pub node_info: PeerInfo,
    pub client_cfg: ClientConfig,
    pub network_cfg: NetworkConfig,
    pub rpc_cfg: RPCConfig,
    pub peer_id_seed: u32,
    pub proxy_handlers: Vec<Arc<ProxyHandler>>,
}

pub trait Node: Send + Sync {
    fn account_id(&self) -> Option<&AccountId>;

    fn start(&mut self);

    fn kill(&mut self);

    fn view_balance(&self, account_id: &AccountId) -> Result<Balance, String> {
        self.user().view_balance(account_id)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        self.user().add_transaction(transaction)
    }

    fn get_account_nonce(&self, account_id: &AccountId) -> Option<u64> {
        self.user().get_account_nonce(account_id)
    }

    fn signer(&self) -> Arc<InMemorySigner>;

    fn as_process_ref(&self) -> &ProcessNode {
        unimplemented!()
    }

    fn as_process_mut(&mut self) -> &mut ProcessNode {
        unimplemented!()
    }

    fn as_thread_ref(&self) -> &ThreadNode {
        unimplemented!()
    }

    fn as_thread_mut(&mut self) -> &mut ThreadNode {
        unimplemented!()
    }

    fn is_running(&self) -> bool;

    fn user(&self) -> Box<dyn User>;

    fn async_user(&self) -> Box<dyn AsyncUser> {
        unimplemented!()
    }
}

impl Node {
    pub fn new_sharable(config: NodeConfig) -> Arc<RwLock<dyn Node>> {
        match config {
            NodeConfig::Thread(local_cfg) => Arc::new(RwLock::new(ThreadNode::new(local_cfg))),
            NodeConfig::Process(local_cfg) => Arc::new(RwLock::new(ProcessNode::new(local_cfg))),
            NodeConfig::Remote { addr, signer } => {
                Arc::new(RwLock::new(RemoteNode::new(addr, signer)))
            }
            NodeConfig::Runtime { account_id } => {
                Arc::new(RwLock::new(RuntimeNode::new(&account_id)))
            }
            NodeConfig::ShardClient { client_cfg } => {
                Arc::new(RwLock::new(ShardClientNode::new(client_cfg)))
            }
        }
    }

    pub fn new(config: NodeConfig) -> Box<dyn Node> {
        match config {
            NodeConfig::Thread(local_cfg) => Box::new(ThreadNode::new(local_cfg)),
            NodeConfig::Process(local_cfg) => Box::new(ProcessNode::new(local_cfg)),
            NodeConfig::Remote { addr, signer } => Box::new(RemoteNode::new(addr, signer)),
            NodeConfig::Runtime { account_id } => Box::new(RuntimeNode::new(&account_id)),
            NodeConfig::ShardClient { client_cfg } => Box::new(ShardClientNode::new(client_cfg)),
        }
    }
}

impl NodeConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn for_test(
        test_prefix: &str,
        test_port: u16,
        account_id: &str,
        peer_id: u16,
        boot_nodes: Vec<PeerAddr>,
        chain_spec: ChainSpec,
        block_fetch_limit: u64,
        block_size_limit: u32,
        proxy_handlers: Vec<Arc<ProxyHandler>>,
    ) -> Self {
        let addr = format!("0.0.0.0:{}", test_port + peer_id);
        Self::new(
            &format!("{}_{}", test_prefix, account_id),
            Some(account_id),
            u32::from(peer_id),
            Some(&addr),
            test_port + 1000 + peer_id,
            boot_nodes,
            chain_spec,
            block_fetch_limit,
            block_size_limit,
            proxy_handlers,
        )
    }

    /// Create full node that does not accept incoming connections.
    #[allow(clippy::too_many_arguments)]
    pub fn for_test_passive(
        test_prefix: &str,
        test_port: u16,
        account_id: Option<&str>,
        peer_id: u16,
        boot_nodes: Vec<PeerAddr>,
        chain_spec: ChainSpec,
        block_fetch_limit: u64,
        block_size_limit: u32,
        proxy_handlers: Vec<Arc<ProxyHandler>>,
    ) -> Self {
        Self::new(
            &format!("{}_{}", test_prefix, peer_id),
            account_id,
            u32::from(peer_id),
            None,
            test_port + 1000 + peer_id,
            boot_nodes,
            chain_spec,
            block_fetch_limit,
            block_size_limit,
            proxy_handlers,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: &str,
        account_id: Option<&str>,
        peer_id_seed: u32,
        addr: Option<&str>,
        rpc_port: u16,
        boot_nodes: Vec<PeerAddr>,
        chain_spec: ChainSpec,
        block_fetch_limit: u64,
        block_size_limit: u32,
        proxy_handlers: Vec<Arc<ProxyHandler>>,
    ) -> Self {
        let node_info = PeerInfo {
            account_id: account_id.map(String::from),
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
            account_id: account_id.map(String::from),
            public_key: None,
            block_fetch_limit,
            block_size_limit,
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

        NodeConfig::Thread(LocalNodeConfig {
            node_info,
            client_cfg,
            network_cfg,
            rpc_cfg,
            peer_id_seed,
            proxy_handlers,
        })
    }
}

/// Create configs for nodes running in a thread. Can use either Enumerated
/// or Named for id type.
pub fn create_nodes_with_id_type(
    num_nodes: usize,
    test_prefix: &str,
    test_port: u16,
    block_fetch_limit: u64,
    block_size_limit: u32,
    proxy_handlers: Vec<Arc<ProxyHandler>>,
    node_id_type: DefaultIdType,
) -> (u64, Vec<String>, Vec<NodeConfig>) {
    let (chain_spec, _) = ChainSpec::testing_spec(
        node_id_type,
        num_nodes,
        num_nodes,
        AuthorityRotation::ProofOfAuthority,
    );
    let account_names: Vec<_> =
        chain_spec.initial_authorities.iter().map(|acc| acc.0.clone()).collect();
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
            block_size_limit,
            proxy_handlers.clone(),
        );
        boot_nodes = vec![node.boot_addr()];
        nodes.push(node);
    }
    (TESTING_INIT_BALANCE, account_names, nodes)
}

/// Create configs for nodes running in a thread.
pub fn create_nodes(
    num_nodes: usize,
    test_prefix: &str,
    test_port: u16,
    block_fetch_limit: u64,
    block_size_limit: u32,
    proxy_handlers: Vec<Arc<ProxyHandler>>,
) -> (u64, Vec<String>, Vec<NodeConfig>) {
    create_nodes_with_id_type(
        num_nodes,
        test_prefix,
        test_port,
        block_fetch_limit,
        block_size_limit,
        proxy_handlers,
        DefaultIdType::Enumerated,
    )
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
pub fn sample_queryable_node(nodes: &[Arc<RwLock<Node>>]) -> usize {
    let num_nodes = nodes.len();
    let mut k = rand::random::<usize>() % num_nodes;
    while !nodes[k].read().unwrap().is_running() {
        k = rand::random::<usize>() % num_nodes;
    }
    k
}
