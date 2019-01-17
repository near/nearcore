use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;

use futures::future;
use futures::sync::mpsc::{channel, Receiver, Sender};
use parking_lot::Mutex;

use crate::chain_spec;
use beacon::authority::AuthorityStake;
use beacon::types::SignedBeaconBlock;
use beacon_chain_handler;
use client::{Client, ClientConfig};
use consensus::{adapters, passthrough};
use network::protocol::{Protocol, ProtocolConfig};
use node_http::api::HttpApi;
use primitives::traits::Signer;
use primitives::types::{
    AccountId, Gossip, UID,
};
use transaction::{ChainPayload, Transaction};
use txflow::txflow_task;
use std::time::Duration;

const NETWORK_CONFIG_PATH: &str = "storage";

fn spawn_rpc_server_task(
    transactions_tx: Sender<Transaction>,
    rpc_port: Option<u16>,
    client: Arc<Client>,
) {
    let rpc_port = rpc_port.unwrap_or(DEFAULT_P2P_PORT);
    let http_addr = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), rpc_port));
    let http_api = HttpApi::new(client, transactions_tx);
    node_http::server::spawn_server(http_api, http_addr);
}

fn spawn_network_tasks(
    account_id: Option<AccountId>,
    base_path: &Path,
    p2p_port: Option<u16>,
    boot_nodes: Vec<String>,
    test_network_key_seed: Option<u32>,
    client: Arc<Client>,
    beacon_block_tx: Sender<SignedBeaconBlock>,
    transactions_tx: Sender<Transaction>,
    inc_gossip_tx: Sender<Gossip<ChainPayload>>,
    out_gossip_rx: Receiver<Gossip<ChainPayload>>,
    beacon_block_rx: Receiver<SignedBeaconBlock>,
    authority_rx: Receiver<HashMap<UID, AuthorityStake>>,
) {
    let (net_messages_tx, net_messages_rx) = channel(1024);
    let protocol_config = ProtocolConfig::new_with_default_id(account_id);
    let protocol = Protocol::new(
        protocol_config.clone(),
        client,
        beacon_block_tx,
        transactions_tx,
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

pub const DEFAULT_P2P_PORT: u16 = 30333;
pub const DEFAULT_RPC_PORT: u16 = 3030;

pub struct NetworkConfig {
    pub rpc_port: u16,

    // Network configuration
    pub p2p_port: u16,
    pub boot_nodes: Vec<String>,
    pub test_network_key_seed: Option<u32>,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            rpc_port: DEFAULT_RPC_PORT,
            p2p_port: DEFAULT_P2P_PORT,
            boot_nodes: vec![],
            test_network_key_seed: None,
        }
    }
}

pub struct DevNetConfig {
    /// how often devnet produces blocks
    pub block_period: Duration
}

impl Default for DevNetConfig {
    fn default() -> Self {
        DevNetConfig {
            block_period: Duration::from_millis(100)
        }
    }
}

pub fn start_service(
    network_cfg: NetworkConfig,
    client_cfg: ClientConfig,
    devnet_cfg: Option<DevNetConfig>,
) {
    let chain_spec = chain_spec::read_or_default_chain_spec(&client_cfg.chain_spec_path);
    let boot_nodes = if chain_spec.boot_nodes.is_empty() {
        network_cfg.boot_nodes.clone()
    } else {
        if !network_cfg.boot_nodes.is_empty() {
            // TODO(#222): return an error here instead of panicking
            panic!("boot nodes cannot be specified when chain spec has boot nodes");
        } else {
            chain_spec.boot_nodes.clone()
        }
    };

    let client = Arc::new(Client::new(&client_cfg, &chain_spec));
    tokio::run(future::lazy(move || {
        // TODO: TxFlow should be listening on these transactions.
        let (transactions_tx, transactions_rx) = channel(1024);
        spawn_rpc_server_task(
            transactions_tx.clone(),
            Some(network_cfg.rpc_port),
            client.clone(),
        );

        // Create a task that receives new blocks from importer/producer
        // and send the authority information to consensus
        let (authority_tx, authority_rx) = channel(1024);
        let (consensus_control_tx, consensus_control_rx) = channel(1024);

        // Create a task that consumes the consensuses
        // and produces the beacon chain blocks.
        let (beacon_block_consensus_body_tx, beacon_block_consensus_body_rx) = channel(1024);
        let (beacon_block_announce_tx, beacon_block_announce_rx) = channel(1024);
        // Block producer is also responsible for re-submitting receipts from the previous block
        // into the next block.
        beacon_chain_handler::producer::spawn_block_producer(
            client.clone(),
            beacon_block_consensus_body_rx,
            beacon_block_announce_tx,
            transactions_tx.clone(),
            authority_tx,
            consensus_control_tx,
        );

        // Create task that can import beacon chain blocks from other peers.
        let (beacon_block_tx, _) = channel(1024);
        // TODO: Re-enable once we have correct shard block fetching and announcement.

        // Spawn protocol and the network_task.
        // Note, that network and RPC are using the same channels
        // to send transactions and receipts for processing.
        let (inc_gossip_tx, inc_gossip_rx) = channel(1024);
        let (out_gossip_tx, out_gossip_rx) = channel(1024);
        spawn_network_tasks(
            Some(client.signer.account_id()),
            &client_cfg.base_path,
            Some(network_cfg.p2p_port),
            boot_nodes,
            network_cfg.test_network_key_seed,
            client.clone(),
            beacon_block_tx.clone(),
            transactions_tx.clone(),
            inc_gossip_tx.clone(),
            out_gossip_rx,
            beacon_block_announce_rx,
            authority_rx,
        );

        // Spawn consensus tasks.
        let (payload_tx, payload_rx) = channel(1024);
        adapters::transaction_to_payload::spawn_task(transactions_rx, payload_tx.clone());

        if let Some(devnet_cfg) = devnet_cfg {
            passthrough::spawn_consensus(
                payload_rx,
                consensus_control_rx,
                beacon_block_consensus_body_tx,
                devnet_cfg.block_period,
            );
        } else {
            txflow_task::spawn_task(
                inc_gossip_rx,
                payload_rx,
                out_gossip_tx,
                consensus_control_rx,
                beacon_block_consensus_body_tx
            );
        }
        Ok(())
    }));
}
