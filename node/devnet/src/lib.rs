//! Starts DevNet either from args or the provided configs.
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use futures::sync::mpsc::{channel, Receiver};
use futures::{future, Future, Stream};
use log::error;

use client::Client;
use configs::{get_devnet_configs, ClientConfig, DevNetConfig, RPCConfig};
use consensus::passthrough::spawn_consensus;
use primitives::chain::ReceiptBlock;

pub fn start() {
    let (client_cfg, devnet_cfg, rpc_cfg) = get_devnet_configs();
    start_from_configs(client_cfg, devnet_cfg, rpc_cfg);
}

pub fn start_from_configs(client_cfg: ClientConfig, devnet_cfg: DevNetConfig, rpc_cfg: RPCConfig) {
    let client = Arc::new(Client::new(&client_cfg));
    start_from_client(client, devnet_cfg, rpc_cfg);
}

pub fn start_from_client(client: Arc<Client>, devnet_cfg: DevNetConfig, rpc_cfg: RPCConfig) {
    let node_task = future::lazy(move || {
        spawn_rpc_server_task(client.clone(), &rpc_cfg);

        // Create a task that receives new blocks from importer/producer
        // and send the authority information to consensus
        let (consensus_tx, consensus_rx) = channel(1024);
        let (mempool_control_tx, mempool_control_rx) = channel(1024);
        let (out_block_tx, out_block_rx) = channel(1024);

        // Block producer is also responsible for re-submitting receipts from the previous block
        // into the next block.
        coroutines::ns_producer::spawn_block_producer(
            client.clone(),
            consensus_rx,
            mempool_control_tx,
            out_block_tx
        );

        // Spawn consensus tasks.
        spawn_consensus(client.clone(), consensus_tx, mempool_control_rx, devnet_cfg.block_period);

        // Spawn empty block announcement task.
        tokio::spawn(out_block_rx.for_each(|(_, _)| { future::ok(())}));
        Ok(())
    });

    tokio::run(node_task);
}

fn spawn_rpc_server_task(client: Arc<Client>, rpc_config: &RPCConfig) {
    let http_addr = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), rpc_config.rpc_port));
    let http_api = node_http::api::HttpApi::new(client);
    node_http::server::spawn_server(http_api, http_addr);
}

fn spawn_receipt_task(client: Arc<Client>, receipt_rx: Receiver<ReceiptBlock>) {
    let task = receipt_rx
        .for_each(move |receipt| {
            if let Err(e) = client.shard_client.pool.add_receipt(receipt) {
                error!("Failed to add receipt: {}", e);
            }
            Ok(())
        })
        .map_err(|e| error!("Error receiving receipts: {:?}", e));

    tokio::spawn(task);
}
