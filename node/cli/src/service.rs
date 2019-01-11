use std::cmp;
use std::collections::HashMap;
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

use beacon::authority::{Authority, AuthorityStake};
use beacon::types::{BeaconBlockChain, SignedBeaconBlock, SignedBeaconBlockHeader};
use beacon_chain_handler;
use beacon_chain_handler::authority_handler::{spawn_authority_task, AuthorityHandler};
use beacon_chain_handler::producer::ChainConsensusBlockBody;
use chain::SignedBlock;
use chain_spec;
use consensus::adapters;
use network::protocol::{Protocol, ProtocolConfig};
use node_http::api::HttpApi;
use node_runtime::{state_viewer::StateDbViewer, Runtime};
use primitives::signer::InMemorySigner;
use primitives::traits::Signer;
use primitives::types::{
    AccountId, ChainPayload, Gossip, ReceiptTransaction, SignedTransaction, UID,
};
use shard::{ShardBlockChain, SignedShardBlock};
use storage::{StateDb, Storage};
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use txflow::txflow_task::Control;

const STORAGE_PATH: &str = "storage/db";
const NETWORK_CONFIG_PATH: &str = "storage";
const KEY_STORE_PATH: &str = "storage/keystore";

fn get_storage(base_path: &Path) -> Arc<Storage> {
    let mut storage_path = base_path.to_owned();
    storage_path.push(STORAGE_PATH);
    match fs::canonicalize(storage_path.clone()) {
        Ok(path) => info!("Opening storage database at {:?}", path),
        _ => info!("Could not resolve {:?} path", storage_path),
    };
    Arc::new(storage::open_database(&storage_path.to_string_lossy()))
}

fn spawn_rpc_server_task(
    transactions_tx: Sender<SignedTransaction>,
    rpc_port: Option<u16>,
    shard_chain: &Arc<ShardBlockChain>,
    state_db: Arc<StateDb>,
    beacon_chain: Arc<BeaconBlockChain>,
) {
    let state_db_viewer = StateDbViewer::new(shard_chain.clone(), state_db);
    let rpc_port = rpc_port.unwrap_or(DEFAULT_P2P_PORT);
    let http_addr = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), rpc_port));
    let http_api =
        HttpApi::new(state_db_viewer, transactions_tx, beacon_chain, shard_chain.clone());
    node_http::server::spawn_server(http_api, http_addr);
}

fn spawn_network_tasks(
    account_id: Option<AccountId>,
    base_path: &Path,
    p2p_port: Option<u16>,
    boot_nodes: Vec<String>,
    test_network_key_seed: Option<u32>,
    beacon_chain: Arc<BeaconBlockChain>,
    beacon_block_tx: Sender<SignedBeaconBlock>,
    transactions_tx: Sender<SignedTransaction>,
    receipts_tx: Sender<ReceiptTransaction>,
    inc_gossip_tx: Sender<Gossip<ChainPayload>>,
    out_gossip_rx: Receiver<Gossip<ChainPayload>>,
    beacon_block_rx: Receiver<SignedBeaconBlock>,
    authority_rx: Receiver<HashMap<UID, AuthorityStake>>,
) {
    let (net_messages_tx, net_messages_rx) = channel(1024);
    let protocol_config = ProtocolConfig::new_with_default_id(account_id);
    let protocol = Protocol::<_, SignedBeaconBlockHeader>::new(
        protocol_config.clone(),
        beacon_chain,
        beacon_block_tx,
        transactions_tx,
        receipts_tx,
        net_messages_tx.clone(),
        inc_gossip_tx,
    );
    let mut network_config = network::service::NetworkConfiguration::new();
    let mut network_config_path = base_path.to_owned();
    network_config_path.push(NETWORK_CONFIG_PATH);
    network_config.net_config_path = Some(network_config_path.to_string_lossy().to_string());
    network_config.boot_nodes = boot_nodes;
    let p2p_port = p2p_port.unwrap_or(DEFAULT_P2P_PORT);
    network_config.listen_addresses =
        vec![network::service::get_multiaddr(Ipv4Addr::UNSPECIFIED, p2p_port)];

    network_config.use_secret =
        test_network_key_seed.map(network::service::get_test_secret_from_network_key_seed);

    let network_service = network::service::new_network_service(&protocol_config, network_config);
    network::service::spawn_network_tasks(
        Arc::new(Mutex::new(network_service)),
        protocol,
        net_messages_rx,
        beacon_block_rx,
        authority_rx,
        out_gossip_rx,
    );
}

fn configure_logging(log_level: log::LevelFilter) {
    let internal_targets = vec!["network", "producer", "runtime", "service", "near-rpc", "wasm"];
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
    pub account_id: AccountId,
    pub public_key: Option<String>,
    pub chain_spec_path: Option<PathBuf>,
    pub log_level: log::LevelFilter,
    pub rpc_port: u16,

    // Network configuration
    pub p2p_port: u16,
    pub boot_nodes: Vec<String>,
    pub test_network_key_seed: Option<u32>,
}

impl Default for ServiceConfig {
    fn default() -> ServiceConfig {
        ServiceConfig {
            base_path: PathBuf::from(DEFAULT_BASE_PATH),
            account_id: String::from("alice"),
            public_key: None,
            chain_spec_path: None,
            log_level: DEFAULT_LOG_LEVEL,
            rpc_port: DEFAULT_RPC_PORT,
            p2p_port: DEFAULT_P2P_PORT,
            boot_nodes: vec![],
            test_network_key_seed: None,
        }
    }
}

pub fn start_service<S>(config: ServiceConfig, spawn_consensus_task_fn: S)
where
    S: Fn(
            Receiver<Gossip<ChainPayload>>,
            Receiver<ChainPayload>,
            Sender<Gossip<ChainPayload>>,
            Receiver<Control<BeaconWitnessSelector>>,
            Sender<ChainConsensusBlockBody>,
        ) -> ()
        + Send
        + Sync
        + 'static,
{
    // Create shared-state objects.
    let storage = get_storage(&config.base_path);
    let chain_spec = chain_spec::read_or_default_chain_spec(&config.chain_spec_path);
    let boot_nodes = if chain_spec.boot_nodes.is_empty() {
        config.boot_nodes.clone()
    } else {
        if !config.boot_nodes.is_empty() {
            // TODO(#222): return an error here instead of panicking
            panic!("boot nodes cannot be specified when chain spec has boot nodes");
        } else {
            chain_spec.boot_nodes.clone()
        }
    };

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

    let mut key_file_path = config.base_path.to_path_buf();
    key_file_path.push(KEY_STORE_PATH);
    let signer = Arc::new(InMemorySigner::from_key_file(
        config.account_id.clone(),
        key_file_path.as_path(),
        config.public_key.clone(),
    ));
    let authority_config = chain_spec::get_authority_config(&chain_spec);
    let authority = Arc::new(RwLock::new(Authority::new(authority_config, &beacon_chain)));
    let authority_handler = AuthorityHandler::new(authority.clone(), config.account_id.clone());

    configure_logging(config.log_level);
    tokio::run(future::lazy(move || {
        // TODO: TxFlow should be listening on these transactions.
        let (transactions_tx, transactions_rx) = channel(1024);
        spawn_rpc_server_task(
            transactions_tx.clone(),
            Some(config.rpc_port),
            &shard_chain.clone(),
            state_db.clone(),
            beacon_chain.clone(),
        );

        // Create a task that receives new blocks from importer/producer
        // and send the authority information to consensus
        let (new_block_tx, new_block_rx) = channel(1024);
        let (authority_tx, authority_rx) = channel(1024);
        let (consensus_control_tx, consensus_control_rx) = channel(1024);
        spawn_authority_task(authority_handler, new_block_rx, authority_tx, consensus_control_tx);

        // Create a task that consumes the consensuses
        // and produces the beacon chain blocks.
        let (beacon_block_consensus_body_tx, beacon_block_consensus_body_rx) = channel(1024);
        let (beacon_block_announce_tx, beacon_block_announce_rx) = channel(1024);
        beacon_chain_handler::producer::spawn_block_producer(
            beacon_chain.clone(),
            shard_chain.clone(),
            runtime.clone(),
            signer.clone(),
            state_db.clone(),
            authority.clone(),
            beacon_block_consensus_body_rx,
            beacon_block_announce_tx,
            new_block_tx.clone(),
        );

        // Create task that can import beacon chain blocks from other peers.
        let (beacon_block_tx, beacon_block_rx) = channel(1024);
        beacon_chain_handler::importer::spawn_block_importer(
            beacon_chain.clone(),
            shard_chain.clone(),
            runtime.clone(),
            state_db.clone(),
            beacon_block_rx,
            new_block_tx,
        );

        // Spawn protocol and the network_task.
        // Note, that network and RPC are using the same channels
        // to send transactions and receipts for processing.
        let (receipts_tx, receipts_rx) = channel(1024);
        let (inc_gossip_tx, inc_gossip_rx) = channel(1024);
        let (out_gossip_tx, out_gossip_rx) = channel(1024);
        spawn_network_tasks(
            Some(signer.account_id()),
            &config.base_path,
            Some(config.p2p_port),
            boot_nodes,
            config.test_network_key_seed,
            beacon_chain.clone(),
            beacon_block_tx.clone(),
            transactions_tx.clone(),
            receipts_tx.clone(),
            inc_gossip_tx.clone(),
            out_gossip_rx,
            beacon_block_announce_rx,
            authority_rx,
        );

        // Spawn consensus tasks.
        let (payload_tx, payload_rx) = channel(1024);
        adapters::signed_transaction_to_payload::spawn_task(transactions_rx, payload_tx.clone());
        adapters::receipt_transaction_to_payload::spawn_task(receipts_rx, payload_tx.clone());

        spawn_consensus_task_fn(
            inc_gossip_rx,
            payload_rx,
            out_gossip_tx,
            consensus_control_rx,
            beacon_block_consensus_body_tx,
        );
        Ok(())
    }));
}
