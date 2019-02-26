extern crate env_logger;
#[macro_use]
extern crate log;
extern crate serde;
extern crate serde_derive;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::future::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc;
use tokio::timer::Delay;

use chain::ChainPayload;
use configs::{ClientConfig, get_alphanet_configs, NetworkConfig, RPCConfig};
use network::nightshade_protocol::{spawn_consensus_network, start_peer};
use nightshade::nightshade_task::{Control, spawn_nightshade_task};
use primitives::aggregate_signature::{BlsPublicKey, BlsSecretKey};
use primitives::network::PeerInfo;
use primitives::signature::{PublicKey, SecretKey};
use primitives::types::AccountId;
use std::sync::Arc;
use client::Client;

pub fn start() {
    let (client_cfg, network_cfg, rpc_cfg) = get_alphanet_configs();
    start_from_configs(client_cfg, network_cfg, rpc_cfg);
}

pub fn start_from_configs(client_cfg: ClientConfig, network_cfg: NetworkConfig, rpc_cfg: RPCConfig) {
    let client = Arc::new(Client::new(&client_cfg));
    let node_task = futures::lazy(move || {
        let (transactions_tx, transactions_rx) = mpsc::channel(1024);
        let (receipts_tx, receipts_rx) = mpsc::channel(1024);

        // Launch rpc server
        spawn_rpc_server_task(transactions_tx, &rpc_cfg, client.clone());

        // Create control channel and send kick-off reset signal.
        let (control_tx, control_rx) = mpsc::channel(1024);
        let start_task = control_tx
            .clone()
            .send(Control::Reset(payload))
            .map(|_| ())
            .map_err(|e| error!("Error sending control {:?}", e));
        tokio::spawn(start_task);

        // Launch Nightshade task
        let (inc_gossip_tx, inc_gossip_rx) = mpsc::channel(1024);
        let (out_gossip_tx, out_gossip_rx) = mpsc::channel(1024);
        let (consensus_tx, consensus_rx) = mpsc::channel(1024);

        spawn_nightshade_task(
            inc_gossip_rx,
            out_gossip_tx,
            consensus_tx,
            control_rx,
        );

        // Create a task that consumes the consensuses and produces the beacon chain blocks.
        let (beacon_block_consensus_body_tx, beacon_block_consensus_body_rx) = mpsc::channel(1024);
        let (outgoing_block_tx, outgoing_block_rx) = mpsc::channel(1024);
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
        let (incoming_block_tx, incoming_block_rx) = mpsc::channel(1024);
        coroutines::importer::spawn_block_importer(client.clone(), incoming_block_rx);

        // Spawn the network tasks.
        // Note, that network and RPC are using the same channels
        // to send transactions and receipts for processing.
        let (inc_gossip_tx, inc_gossip_rx) = mpsc::channel(1024);
        let (out_gossip_tx, out_gossip_rx) = mpsc::channel(1024);
        network::spawn_network(
            Some(client_cfg.account_id),
            network_cfg,
            client.clone(),
            inc_gossip_tx,
            out_gossip_rx,
            incoming_block_tx,
            outgoing_block_rx,
        );

        // Witness selector must handle authority positions in the consensu
        // Start protocol. Connect consensus channels with network channels. Encode + Decode messages/gossips
        let mut auth_map = HashMap::new();
        authorities.drain(..).enumerate().for_each(|(authority, account_id)| {
            auth_map.insert(authority, account_id);
        });

        let account_id = authorities[authority].clone();
        spawn_consensus_network(Some(account_id), network_cfg, inc_gossip_tx, out_gossip_rx, auth_map.clone());

        // Wait for consensus is achieved and send stop signal.
        let commit_task = consensus_rx.for_each(|_outcome| {
            stop_task = control_tx.send(Control::Stop)
                .map(|_| ())
                .map_err(|e| error!("Error sending stop signal: {:?}", e));
            tokio::spawn(stop_task);

            // TODO: Add block (with the evidence) to the chain, broadcast action and move onto next block.
        });

        tokio::spawn(commit_task);

        Ok(())
    });

    tokio::run(node_task);
}

#[cfg(test)]
mod tests {
    use std::cmp;
    use std::env;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::thread;

    use env_logger::Builder;

    use primitives::aggregate_signature::get_bls_key_pair;
    use primitives::hash::hash_struct;
    use primitives::network::PeerInfo;
    use primitives::signature::get_key_pair;

    fn configure_logging(log_level: log::LevelFilter) {
        let internal_targets = vec!["nightshade", "alphanet"];
        let mut builder = Builder::from_default_env();
        internal_targets.iter().for_each(|internal_targets| {
            builder.filter(Some(internal_targets), log_level);
        });

        let other_log_level = cmp::min(log_level, log::LevelFilter::Info);
        builder.filter(None, other_log_level);

        if let Ok(lvl) = env::var("RUST_LOG") {
            builder.parse(&lvl);
        }
        if let Err(e) = builder.try_init() {
            warn!(target: "client", "Failed to reinitialize the log level {}", e);
        }
    }

    #[test]
    fn test_alphanet() {
        run_many_nodes(2);
    }
}
