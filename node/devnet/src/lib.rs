//! Starts DevNet either from args or the provided configs.
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use futures::future;
use futures::stream::Stream;
use futures::sync::mpsc::channel;

use client::Client;
use configs::{ClientConfig, DevNetConfig, get_devnet_configs, RPCConfig};
use consensus::passthrough::spawn_consensus;
use coroutines::client_task::ClientTask;

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
        let (control_tx, control_rx) = channel(1024);

        let (_, retrieve_payload_rx) = channel(1024);
        let (out_payload_gossip_tx, inc_payload_gossip_rx) = channel(1014);
        let (payload_request_tx, _) = channel(1024);
        let (_, payload_response_rx) = channel(1024);
        let (_, inc_block_rx) = channel(1024);
        let (out_block_tx, out_block_rx) = channel(1024);
        let (_, inc_chain_state_rx) = channel(1024);
        let (out_block_fetch_tx, _) = channel(1024);

        // Gossip interval is currently not used.
        let gossip_interval = Duration::from_secs(1);
        ClientTask::new(
            client.clone(),
            inc_block_rx,
            out_block_tx,
            consensus_rx,
            control_tx,
            retrieve_payload_rx,
            payload_request_tx,
            payload_response_rx,
            inc_payload_gossip_rx,
            out_payload_gossip_tx,
            inc_chain_state_rx,
            out_block_fetch_tx,
            gossip_interval,
        )
        .spawn();

        // Spawn consensus tasks.
        spawn_consensus(client.clone(), consensus_tx, control_rx, devnet_cfg.block_period);

        // Spawn tasks to consume not used channels.
        tokio::spawn(out_block_rx.for_each(move |_| { future::ok(()) }));
        Ok(())
    });

    tokio::run(node_task);
}

fn spawn_rpc_server_task(client: Arc<Client>, rpc_config: &RPCConfig) {
    let http_addr = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), rpc_config.rpc_port));
    let http_api = node_http::api::HttpApi::new(client);
    node_http::server::spawn_server(http_api, http_addr);
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::thread;
    use std::time::Duration;

    use primitives::signer::InMemorySigner;
    use primitives::transaction::TransactionBody;
    use testlib::alphanet_utils::wait;

    use super::*;

    const TMP_DIR: &str = "../../tmp/devnet";

    #[test]
    fn test_devnet_produce_blocks() {
        let mut base_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        base_path.push(TMP_DIR);
        base_path.push("test_devnet_produce_blocks");
        if base_path.exists() {
            std::fs::remove_dir_all(base_path.clone()).unwrap();
        }

        let mut client_cfg = configs::ClientConfig::default();
        client_cfg.base_path = base_path;
        client_cfg.log_level = log::LevelFilter::Info;
        let devnet_cfg = configs::DevNetConfig { block_period: Duration::from_millis(5) };
        let rpc_cfg = configs::RPCConfig::default();

        let client = Arc::new(Client::new(&client_cfg));
        let client1 = client.clone();
        thread::spawn(|| {
            start_from_client(client1, devnet_cfg, rpc_cfg);
        });

        let signer = Arc::new(InMemorySigner::from_seed("alice.near", "alice.near"));
        client
            .shard_client
            .pool
            .add_transaction(
                TransactionBody::send_money(1, "alice.near", "bob.near", 10).sign(signer.clone()),
            )
            .unwrap();
        wait(|| client.shard_client.chain.best_index() >= 2, 50, 10000);

        // Check that transaction and it's receipt were included.
        let mut state_update = client.shard_client.get_state_update();
        assert_eq!(
            client
                .shard_client
                .trie_viewer
                .view_account(&mut state_update, &"alice.near".to_string())
                .unwrap()
                .amount,
            9999990
        );
        assert_eq!(
            client
                .shard_client
                .trie_viewer
                .view_account(&mut state_update, &"bob.near".to_string())
                .unwrap()
                .amount,
            110
        );
        assert!(client.shard_client.pool.is_empty());
    }
}
