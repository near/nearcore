extern crate env_logger;
#[macro_use]
extern crate log;
extern crate serde;
extern crate serde_derive;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::future::Future;
use futures::sink::Sink;
use futures::sync::mpsc;
use tokio::timer::Delay;

use chain::ChainPayload;
use network::nightshade_protocol::{spawn_consensus_network, start_peer};
use nightshade::nightshade_task::{spawn_nightshade_task, Control};
use primitives::aggregate_signature::{BlsPublicKey, BlsSecretKey};
use primitives::network::PeerInfo;
use primitives::signature::{PublicKey, SecretKey};
use primitives::types::AccountId;

// TODO: Explain this function and their arguments
fn run_node(
    authority: usize,
    num_authorities: usize,
    mut authorities: Vec<AccountId>,
    boot_nodes: Vec<PeerInfo>,
    public_keys: Vec<PublicKey>,
    secret_key: SecretKey,
    bls_public_keys: Vec<BlsPublicKey>,
    bls_secret_key: BlsSecretKey,
) {
    let node_task = futures::lazy(move || {
        info!(target: "alphanet", "node {}: Main task", authority);

        // 1. Initialize peer manager

        let account_id = authorities[authority].clone();

        // 2. Initialize NightshadeTask. Create proposals.

        // TODO: Each participant should propose different chain payloads
        let payload: ChainPayload = ChainPayload { transactions: vec![], receipts: vec![] };

        let (inc_gossip_tx, inc_gossip_rx) = mpsc::channel(1024);
        let (out_gossip_tx, out_gossip_rx) = mpsc::channel(1024);
        let (consensus_tx, _consensus_rx) = mpsc::channel(1024);

        // Create control channel and send kick-off reset signal.
        // Nightshade task should end alone after more than 2/3 of authorities have committed.
        let (control_tx, control_rx) = mpsc::channel(1024);
        let start_task = control_tx
            .clone()
            .send(Control::Reset(payload))
            .map(|_| ())
            .map_err(|e| error!("Error sending control {:?}", e));
        tokio::spawn(start_task);
        spawn_nightshade_task(
            authority,
            num_authorities,
            public_keys,
            secret_key,
            bls_public_keys,
            bls_secret_key,
            inc_gossip_rx,
            out_gossip_tx,
            consensus_tx,
            control_rx,
        );

        // 3. Start protocol. Connect consensus channels with network channels. Encode + Decode messages/gossips
        let mut auth_map = HashMap::new();
        authorities.drain(..).enumerate().for_each(|(authority, account_id)| {
            auth_map.insert(authority, account_id);
        });

        spawn_consensus_network(pm, inc_msg_rx, inc_gossip_tx, out_gossip_rx, auth_map.clone());

        // 4. Wait for consensus is achieved and send stop signal (or a maximum number of time)

        // TODO: Step 4

        // Workaround: keep control_tx and send stop signal after some long fixed delay (10min)
        let stop_task = Delay::new(Instant::now() + Duration::from_secs(600)).then(|_| {
            control_tx
                .send(Control::Stop)
                .map(|_| ())
                .map_err(|e| error!("Error sending stop signal: {:?}", e))
        });

        tokio::spawn(stop_task);

        Ok(())
    });

    tokio::run(node_task);
}

#[cfg(test)]
mod tests {
    use crate::run_node;
    use env_logger::Builder;
    use primitives::aggregate_signature::get_bls_key_pair;
    use primitives::hash::hash_struct;
    use primitives::network::PeerInfo;
    use primitives::signature::get_key_pair;
    use std::cmp;
    use std::env;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::thread;

    fn run_many_nodes(num_authorities: usize) {
        configure_logging(log::LevelFilter::Debug);

        let (public_keys, secret_keys): (Vec<_>, Vec<_>) =
            (0..num_authorities).map(|_| get_key_pair()).unzip();
        let (bls_public_keys, bls_secret_keys): (Vec<_>, Vec<_>) =
            (0..num_authorities).map(|_| get_bls_key_pair()).unzip();
        let accounts_id: Vec<_> = (0..num_authorities).map(|a| a.to_string()).collect();

        let mut threads = vec![];

        for a in 0..num_authorities {
            let mut boot_nodes = vec![];

            if a != 0 {
                boot_nodes.push(PeerInfo {
                    id: hash_struct(&(0 as usize)),
                    addr: SocketAddr::from_str("127.0.0.1:3000").unwrap(),
                    account_id: None,
                });
            }

            let authority = a.clone();
            let accounts_id1 = accounts_id.clone();
            let public_keys1 = public_keys.clone();
            let secret_key1 = secret_keys[a].clone();
            let bls_public_keys1 = bls_public_keys.clone();
            let bls_secret_key1 = bls_secret_keys[a].clone();

            let task = thread::spawn(move || {
                run_node(
                    authority,
                    num_authorities,
                    accounts_id1,
                    boot_nodes,
                    public_keys1,
                    secret_key1,
                    bls_public_keys1,
                    bls_secret_key1,
                );
            });

            threads.push(task);
        }

        // Wait for all task to finish
        for task in threads.drain(..) {
            let _ = task.join();
        }
    }

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
