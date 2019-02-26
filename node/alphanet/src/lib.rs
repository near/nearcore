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
use coroutines::ns_control_builder::get_control;
use coroutines::ns_producer::spawn_block_producer;
use network::nightshade_protocol::spawn_consensus_network;
use nightshade::nightshade_task::{spawn_nightshade_task, Control};
use std::sync::Arc;

pub fn start_from_configs(
    client_cfg: ClientConfig,
    network_cfg: NetworkConfig,
    _rpc_cfg: RPCConfig,
) {
    let client = Arc::new(Client::new(&client_cfg));
    let node_task = futures::lazy(move || {
        // Create control channel and send kick-off reset signal.
        let (control_tx, control_rx) = mpsc::channel(1024);

        // Launch Nightshade task
        let (inc_gossip_tx, inc_gossip_rx) = mpsc::channel(1024);
        let (out_gossip_tx, out_gossip_rx) = mpsc::channel(1024);
        let (consensus_tx, consensus_rx) = mpsc::channel(1024);

        spawn_nightshade_task(inc_gossip_rx, out_gossip_tx, consensus_tx, control_rx);
        let start_task = control_tx
            .clone()
            .send(get_control(&client, 1))
            .map(|_| ())
            .map_err(|e| error!("Error sending control {:?}", e));
        tokio::spawn(start_task);
        // Spawn the network tasks.
        spawn_consensus_network(
            Some(client_cfg.account_id),
            network_cfg,
            client.clone(),
            inc_gossip_tx,
            out_gossip_rx,
        );

        spawn_block_producer(client.clone(), consensus_rx, control_tx);

        Ok(())
    });

    tokio::run(node_task);
}

#[cfg(test)]
mod tests {
    use configs::chain_spec::read_or_default_chain_spec;
    use configs::network::get_peer_id_from_seed;
    use configs::ClientConfig;
    use configs::NetworkConfig;
    use configs::RPCConfig;
    use primitives::network::PeerInfo;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::thread;
    use std::time::Duration;
    use std::str::FromStr;
    use crate::start_from_configs;

    const TMP_DIR: &str = "./tmp/alphanet";

    fn test_node_ready(
        base_path: PathBuf,
        node_info: PeerInfo,
        rpc_port: u16,
        boot_nodes: Vec<PeerInfo>,
    ) {
        if base_path.exists() {
            std::fs::remove_dir_all(base_path.clone()).unwrap();
        }

        let client_cfg = ClientConfig {
            base_path,
            account_id: node_info.account_id.unwrap(),
            public_key: None,
            chain_spec: read_or_default_chain_spec(&Some(PathBuf::from(
                "./node/configs/res/testnet_chain.json",
            ))),
            log_level: log::LevelFilter::Off,
        };

        let network_cfg = NetworkConfig {
            listen_addr: node_info.addr,
            peer_id: node_info.id,
            boot_nodes,
            reconnect_delay: Duration::from_millis(50),
            gossip_interval: Duration::from_millis(50),
            gossip_sample_size: 10,
        };

        let rpc_cfg = RPCConfig { rpc_port };
        thread::spawn(|| {
            start_from_configs(client_cfg, network_cfg, rpc_cfg);
        });
        thread::sleep(Duration::from_secs(1));
    }

    #[test]
    fn two_nodes() {
        let mut base_path = PathBuf::from(TMP_DIR);
        base_path.push("node_alice");
        let alice_info = PeerInfo {
            account_id: Some(String::from("alice.near")),
            id: get_peer_id_from_seed(1),
            addr: SocketAddr::from_str("127.0.0.1:3000").unwrap(),
        };
        test_node_ready(base_path, alice_info.clone(), 3030, vec![]);

        // Start secondary node that boots from the alice node.
        let mut base_path = PathBuf::from(TMP_DIR);
        base_path.push("node_bob");
        let bob_info = PeerInfo {
            account_id: Some(String::from("bob.near")),
            id: get_peer_id_from_seed(2),
            addr: SocketAddr::from_str("127.0.0.1:3001").unwrap(),
        };
        test_node_ready(base_path, bob_info.clone(), 3031, vec![alice_info]);
        thread::sleep(Duration::from_secs(60));
    }
}
