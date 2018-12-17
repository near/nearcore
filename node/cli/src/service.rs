use std::cmp;
use std::env;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use env_logger::Builder;
use futures::future;
use futures::sync::mpsc::{channel, Receiver, Sender};
use parking_lot::{Mutex, RwLock};

use beacon::types::{BeaconBlockChain, SignedBeaconBlock, SignedBeaconBlockHeader};
use beacon_chain_handler;
use beacon_chain_handler::producer::ChainConsensusBlockBody;
use chain::SignedBlock;
use chain_spec;
use consensus::adapters;
use log;
use network;
use network::protocol::{Protocol, ProtocolConfig};
use node_rpc;
use node_runtime::{state_viewer::StateDbViewer, Runtime};
use primitives::signer::InMemorySigner;
use primitives::types::ChainPayload;
use primitives::types::{Gossip, ReceiptTransaction, SignedTransaction};
use shard::{ShardBlockChain, SignedShardBlock};
use storage;
use storage::{StateDb, Storage};
use tokio;

fn get_storage(base_path: &Path) -> Arc<Storage> {
    let mut storage_path = base_path.to_owned();
    storage_path.push("storage/db");
    match fs::canonicalize(storage_path.clone()) {
        Ok(path) => info!("Opening storage database at {:?}", path),
        _ => info!("Could not resolve {:?} path", storage_path),
    };
    Arc::new(storage::open_database(&storage_path.to_string_lossy()))
}

fn spawn_rpc_server_task(
    transactions_tx: Sender<SignedTransaction>,
    rpc_port: Option<u16>,
    shard_chain: Arc<ShardBlockChain>,
    state_db: Arc<StateDb>,
) {
    let state_db_viewer = StateDbViewer::new(shard_chain, state_db);
    let rpc_impl = node_rpc::api::RpcImpl::new(state_db_viewer, transactions_tx);
    let rpc_handler = node_rpc::api::get_handler(rpc_impl);
    let rpc_port = rpc_port.unwrap_or(DEFAULT_P2P_PORT);
    let rpc_addr = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), rpc_port));
    let server = node_rpc::server::get_server(rpc_handler, rpc_addr);
    tokio::spawn(future::lazy(|| {
        server.wait();
        Ok(())
    }));
}

fn spawn_network_tasks(
    p2p_port: Option<u16>,
    boot_nodes: Vec<String>,
    test_node_index: Option<u32>,
    beacon_chain: Arc<BeaconBlockChain>,
    beacon_block_tx: Sender<SignedBeaconBlock>,
    transactions_tx: Sender<SignedTransaction>,
    receipts_tx: Sender<ReceiptTransaction>,
    gossip_tx: Sender<Gossip<ChainPayload>>,
    gossip_rx: Receiver<Gossip<ChainPayload>>,
) {
    let (net_messages_tx, net_messages_rx) = channel(1024);
    let protocol_config = ProtocolConfig::default();
    let protocol = Protocol::<_, SignedBeaconBlockHeader>::new(
        protocol_config,
        beacon_chain,
        beacon_block_tx,
        transactions_tx,
        receipts_tx,
        net_messages_tx.clone(),
        gossip_tx,
    );
    let mut network_config = network::service::NetworkConfiguration::new();
    network_config.boot_nodes = boot_nodes;
    let p2p_port = p2p_port.unwrap_or(DEFAULT_P2P_PORT);
    network_config.listen_addresses =
        vec![network::service::get_multiaddr(Ipv4Addr::UNSPECIFIED, p2p_port)];

    network_config.use_secret =
        test_node_index.map(network::service::get_test_secret_from_node_index);

    let network_service = network::service::new_network_service(&protocol_config, network_config);
    network::service::spawn_network_tasks(
        Arc::new(Mutex::new(network_service)),
        protocol,
        net_messages_rx,
    );

    // Spawn task sending gossips to the network.
    adapters::gossip_to_network_message::spawn_task(gossip_rx, net_messages_tx.clone());
}

fn configure_logging(log_level: log::LevelFilter) {
    let internal_targets = vec!["network", "producer", "runtime", "service", "near-rpc"];
    let mut builder = Builder::from_default_env();
    internal_targets.iter().for_each(|internal_targets| {
        builder.filter(Some(internal_targets), log_level);
    });

    let other_log_level = cmp::min(log_level, log::LevelFilter::Info);
    builder.filter(None, other_log_level);

    if let Ok(lvl) = env::var("RUST_LOG") {
        builder.parse(&lvl);
    }
    builder.init();
}

pub const DEFAULT_BASE_PATH: &str = ".";
pub const DEFAULT_LOG_LEVEL: log::LevelFilter = log::LevelFilter::Info;
pub const DEFAULT_P2P_PORT: u16 = 30333;
pub const DEFAULT_RPC_PORT: u16 = 3030;

pub struct ServiceConfig {
    pub base_path: PathBuf,
    pub chain_spec_path: Option<PathBuf>,
    pub log_level: log::LevelFilter,
    pub rpc_port: u16,

    // Network configuration
    pub p2p_port: u16,
    pub boot_nodes: Vec<String>,
    pub test_node_index: Option<u32>,
}

impl Default for ServiceConfig {
    fn default() -> ServiceConfig {
        ServiceConfig {
            base_path: PathBuf::from(DEFAULT_BASE_PATH),
            chain_spec_path: None,
            log_level: DEFAULT_LOG_LEVEL,
            rpc_port: DEFAULT_RPC_PORT,
            p2p_port: DEFAULT_P2P_PORT,
            boot_nodes: vec![],
            test_node_index: None,
        }
    }
}

pub fn start_service<S>(config: ServiceConfig, spawn_consensus_task_fn: S)
where
    S: Fn(
            Receiver<ChainPayload>,
            Sender<ChainConsensusBlockBody>,
            Receiver<Gossip<ChainPayload>>,
            Sender<Gossip<ChainPayload>>,
        ) -> ()
        + Send
        + Sync
        + 'static,
{
    configure_logging(config.log_level);

    // Create shared-state objects.
    let storage = get_storage(&config.base_path);
    let chain_spec = chain_spec::read_or_default_chain_spec(&config.chain_spec_path);

    let state_db = Arc::new(StateDb::new(storage.clone()));
    let runtime = Arc::new(RwLock::new(Runtime::new(state_db.clone())));
    let genesis_root = runtime.write().apply_genesis_state(
        &chain_spec.accounts,
        &chain_spec.genesis_wasm,
        &chain_spec.initial_authorities,
    );

    let shard_genesis = SignedShardBlock::genesis(genesis_root);
    let genesis = SignedBeaconBlock::genesis(shard_genesis.block_hash());
    let shard_chain = Arc::new(ShardBlockChain::new(shard_genesis, storage.clone()));
    let beacon_chain = Arc::new(BeaconBlockChain::new(genesis, storage.clone()));
    let signer = Arc::new(InMemorySigner::default());

    tokio::run(future::lazy(move || {
        // TODO: TxFlow should be listening on these transactions.
        let (transactions_tx, transactions_rx) = channel(1024);
        spawn_rpc_server_task(
            transactions_tx.clone(),
            Some(config.rpc_port),
            shard_chain.clone(),
            state_db.clone(),
        );

        // Create a task that consumes the consensuses
        // and produces the beacon chain blocks.
        let (beacon_block_consensus_body_tx, beacon_block_consensus_body_rx) = channel(1024);
        beacon_chain_handler::producer::spawn_block_producer(
            beacon_chain.clone(),
            shard_chain.clone(),
            runtime.clone(),
            signer.clone(),
            state_db.clone(),
            beacon_block_consensus_body_rx,
        );

        // Create task that can import beacon chain blocks from other peers.
        let (beacon_block_tx, beacon_block_rx) = channel(1024);
        beacon_chain_handler::importer::spawn_block_importer(
            beacon_chain.clone(),
            shard_chain.clone(),
            runtime.clone(),
            state_db.clone(),
            beacon_block_rx,
        );

        // Spawn protocol and the network_task.
        // Note, that network and RPC are using the same channels
        // to send transactions and receipts for processing.
        let (receipts_tx, receipts_rx) = channel(1024);
        let (inc_gossip_tx, inc_gossip_rx) = channel(1024);
        let (out_gossip_tx, out_gossip_rx) = channel(1024);
        spawn_network_tasks(
            Some(config.p2p_port),
            config.boot_nodes,
            config.test_node_index,
            beacon_chain.clone(),
            beacon_block_tx.clone(),
            transactions_tx.clone(),
            receipts_tx.clone(),
            inc_gossip_tx.clone(),
            out_gossip_rx,
        );

        // Spawn consensus tasks.
        let (payload_tx, payload_rx) = channel(1024);
        adapters::signed_transaction_to_payload::spawn_task(transactions_rx, payload_tx.clone());
        adapters::receipt_transaction_to_payload::spawn_task(receipts_rx, payload_tx.clone());

        spawn_consensus_task_fn(
            payload_rx,
            beacon_block_consensus_body_tx,
            inc_gossip_rx,
            out_gossip_tx,
        );
        Ok(())
    }));
}
