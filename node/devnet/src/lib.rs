//! Starts DevNet either from args or the provided configs.
use std::sync::Arc;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use futures::sync::mpsc::{channel, Sender};
use futures::future;

use configs::{get_devnet_configs, ClientConfig, DevNetConfig, RPCConfig};
use client::Client;
use consensus::adapters::transaction_to_payload;
use consensus::passthrough::spawn_consensus;
use transaction::Transaction;

pub fn start() {
    let (client_cfg, devnet_cfg, rpc_cfg) = get_devnet_configs();
    start_from_configs(client_cfg, devnet_cfg, rpc_cfg);
}

pub fn start_from_configs(client_cfg: ClientConfig, devnet_cfg: DevNetConfig, rpc_cfg: RPCConfig) {
    let client = Arc::new(Client::new(&client_cfg));
    tokio::run(future::lazy(move || {
        // TODO: TxFlow should be listening on these transactions.
        let (transactions_tx, transactions_rx) = channel(1024);
        spawn_rpc_server_task(transactions_tx.clone(), &rpc_cfg, client.clone());

        // Create a task that receives new blocks from importer/producer
        // and send the authority information to consensus
        let (consensus_control_tx, consensus_control_rx) = channel(1024);

        // Create a task that consumes the consensuses
        // and produces the beacon chain blocks.
        let (beacon_block_consensus_body_tx, beacon_block_consensus_body_rx) = channel(1024);
        let (outgoing_block_tx, _) = channel(1024);
        // Block producer is also responsible for re-submitting receipts from the previous block
        // into the next block.
        coroutines::producer::spawn_block_producer(
            client.clone(),
            beacon_block_consensus_body_rx,
            outgoing_block_tx,
            transactions_tx.clone(),
            consensus_control_tx,
        );

        // Spawn consensus tasks.
        let (payload_tx, payload_rx) = channel(1024);
        transaction_to_payload::spawn_task(transactions_rx, payload_tx.clone());

        spawn_consensus(
            payload_rx,
            consensus_control_rx,
            beacon_block_consensus_body_tx,
            client.beacon_chain.chain.best_index(),
            devnet_cfg.block_period,
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
