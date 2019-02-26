extern crate env_logger;
#[macro_use]
extern crate log;
extern crate serde;
extern crate serde_derive;

use futures::future::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc;

use client::Client;
use configs::{ClientConfig, NetworkConfig, RPCConfig};
use network::nightshade_protocol::spawn_consensus_network;
use nightshade::nightshade_task::{spawn_nightshade_task, Control};
use std::sync::Arc;

mod control_builder;

pub fn start_from_configs(
    client_cfg: ClientConfig,
    network_cfg: NetworkConfig,
    _rpc_cfg: RPCConfig,
) {
    let client = Arc::new(Client::new(&client_cfg));
    let node_task = futures::lazy(move || {
        // Create control channel and send kick-off reset signal.
        let (control_tx, control_rx) = mpsc::channel(1024);
        let start_task = control_tx
            .clone()
            .send(control_builder::get_control(&client, 0))
            .map(|_| ())
            .map_err(|e| error!("Error sending control {:?}", e));
        tokio::spawn(start_task);

        // Launch Nightshade task
        let (inc_gossip_tx, inc_gossip_rx) = mpsc::channel(1024);
        let (out_gossip_tx, out_gossip_rx) = mpsc::channel(1024);
        let (consensus_tx, consensus_rx) = mpsc::channel(1024);

        spawn_nightshade_task(inc_gossip_rx, out_gossip_tx, consensus_tx, control_rx);
        // Spawn the network tasks.
        // Note, that network and RPC are using the same channels
        // to send transactions and receipts for processing.
        spawn_consensus_network(
            Some(client_cfg.account_id),
            network_cfg,
            client.clone(),
            inc_gossip_tx,
            out_gossip_rx,
        );

        // Wait for consensus is achieved and send stop signal.
        let commit_task = consensus_rx.for_each(move |_outcome| {
            let stop_task = control_tx.clone()
                .send(Control::Stop)
                .map(|_| ())
                .map_err(|e| error!("Error sending stop signal: {:?}", e));
            tokio::spawn(stop_task);
            Ok(())

            // TODO: Add block (with the evidence) to the chain, broadcast action and move onto next block.
        });

        tokio::spawn(commit_task);

        Ok(())
    });

    tokio::run(node_task);
}
