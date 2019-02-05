//! Starts TestNet either from args or the provided configs.
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use futures::future;
use futures::sync::mpsc::{channel, Sender};

use client::Client;
use configs::{get_testnet_configs, ClientConfig, NetworkConfig, RPCConfig};
use consensus::adapters::transaction_to_payload;
use chain::ChainPayload;
use transaction::SignedTransaction;
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
        let (receipts_tx, receipts_rx) = channel(1024);
        spawn_rpc_server_task(transactions_tx, &rpc_cfg, client.clone());

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
            receipts_tx.clone(),
            consensus_control_tx,
        );

        // Create task that can import beacon chain blocks from other peers.
        let (incoming_block_tx, incoming_block_rx) = channel(1024);
        coroutines::importer::spawn_block_importer(client.clone(), incoming_block_rx);

        // Spawn the network tasks.
        // Note, that network and RPC are using the same channels
        // to send transactions and receipts for processing.
        let (inc_gossip_tx, inc_gossip_rx) = channel(1024);
        let (out_gossip_tx, out_gossip_rx) = channel(1024);
        network::spawn_network(
            Some(client_cfg.account_id),
            network_cfg,
            client.clone(),
            inc_gossip_tx,
            out_gossip_rx,
            incoming_block_tx,
            outgoing_block_rx,
        );

        // Spawn consensus tasks.
        let (payload_tx, payload_rx) = channel(1024);
        transaction_to_payload::spawn_task(
            transactions_rx,
            |t| ChainPayload { transactions: vec![t], receipts: vec![] },
            payload_tx.clone()
        );
        transaction_to_payload::spawn_task(
            receipts_rx,
            |r| ChainPayload { transactions: vec![], receipts: vec![r] },
            payload_tx.clone()
        );
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
    transactions_tx: Sender<SignedTransaction>,
    rpc_config: &RPCConfig,
    client: Arc<Client>,
) {
    let http_addr = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), rpc_config.rpc_port));
    let http_api = node_http::api::HttpApi::new(client, transactions_tx);
    node_http::server::spawn_server(http_api, http_addr);
}
