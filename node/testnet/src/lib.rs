//! Starts TestNet either from args or the provided configs.
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use futures::future;
use futures::sync::mpsc::{channel, Receiver, Sender};
use parking_lot::Mutex;

use beacon::types::SignedBeaconBlock;
use client::Client;
use configs::{get_testnet_configs, ClientConfig, NetworkConfig, RPCConfig};
use consensus::adapters::transaction_to_payload;
use network::protocol::{Protocol, ProtocolConfig};
use primitives::types::{AccountId, Gossip};
use shard::SignedShardBlock;
use transaction::{ChainPayload, Transaction};
use txflow::txflow_task;

pub fn start() {
    let (client_cfg, network_cfg, rpc_cfg) = get_testnet_configs();
    start_from_configs(client_cfg, network_cfg, rpc_cfg);
}

fn start_from_configs(client_cfg: ClientConfig, network_cfg: NetworkConfig, rpc_cfg: RPCConfig) {
    let client = Arc::new(Client::new(&client_cfg));
    tokio::run(future::lazy(move || {
        // TODO: TxFlow should be listening on these transactions.
        let (transactions_tx, transactions_rx) = channel(1024);
        spawn_rpc_server_task(transactions_tx.clone(), &rpc_cfg, client.clone());

        let (consensus_control_tx, consensus_control_rx) = channel(1024);

        // Create a task that consumes the consensuses
        // and produces the beacon chain blocks.
        let (beacon_block_consensus_body_tx, beacon_block_consensus_body_rx) = channel(1024);
        let (outgoing_block_tx, outgoing_block_rx) = channel(1024);
        // Block producer is also responsible for re-submitting receipts from the previous block
        // into the next block.
        coroutines::producer::spawn_block_producer(
            client.clone(),
            beacon_block_consensus_body_rx,
            outgoing_block_tx,
            transactions_tx.clone(),
            consensus_control_tx,
        );

        // Create task that can import beacon chain blocks from other peers.
        let (incoming_block_tx, incominb_block_rx) = channel(1024);
        coroutines::importer::spawn_block_importer(client.clone(), incominb_block_rx);

        // Spawn the network tasks.
        // Note, that network and RPC are using the same channels
        // to send transactions and receipts for processing.
        let (inc_gossip_tx, inc_gossip_rx) = channel(1024);
        let (out_gossip_tx, out_gossip_rx) = channel(1024);
        spawn_network_tasks(
            client_cfg.account_id,
            network_cfg,
            client.clone(),
            transactions_tx.clone(),
            inc_gossip_tx.clone(),
            out_gossip_rx,
            incoming_block_tx,
            outgoing_block_rx,
        );

        // Spawn consensus tasks.
        let (payload_tx, payload_rx) = channel(1024);
        transaction_to_payload::spawn_task(transactions_rx, payload_tx.clone());
        txflow_task::spawn_task(
            inc_gossip_rx,
            payload_rx,
            out_gossip_tx,
            consensus_control_rx,
            beacon_block_consensus_body_tx,
        );
        Ok(())
    }));
}

fn spawn_rpc_server_task(
    transactions_tx: Sender<Transaction>,
    rpc_config: &RPCConfig,
    client: Arc<Client>,
) {
    let http_addr = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), rpc_config.rpc_port));
    let http_api = node_http::api::HttpApi::new(client, transactions_tx);
    node_http::server::spawn_server(http_api, http_addr);
}

fn spawn_network_tasks(
    account_id: AccountId,
    network_cfg: NetworkConfig,
    client: Arc<Client>,
    transactions_tx: Sender<Transaction>,
    inc_gossip_tx: Sender<Gossip<ChainPayload>>,
    out_gossip_rx: Receiver<Gossip<ChainPayload>>,
    incoming_block_tx: Sender<(SignedBeaconBlock, SignedShardBlock)>,
    outgoing_block_rx: Receiver<(SignedBeaconBlock, SignedShardBlock)>,
) {
    let (net_messages_tx, net_messages_rx) = channel(1024);
    let protocol_config = ProtocolConfig::new_with_default_id(Some(account_id));
    let protocol = Protocol::new(
        protocol_config.clone(),
        client,
        incoming_block_tx,
        transactions_tx,
        net_messages_tx.clone(),
        inc_gossip_tx,
    );

    let network_service = network::service::new_network_service(&protocol_config, network_cfg);
    network::service::spawn_network_tasks(
        Arc::new(Mutex::new(network_service)),
        protocol,
        net_messages_rx,
        outgoing_block_rx,
        out_gossip_rx,
    );
}
