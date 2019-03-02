extern crate env_logger;
extern crate serde;
extern crate serde_derive;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use futures::sync::mpsc::channel;

use client::Client;
use configs::{get_testnet_configs, ClientConfig, NetworkConfig, RPCConfig};
use coroutines::importer::spawn_block_importer;
use coroutines::ns_producer::spawn_block_producer;
use mempool::pool_task::spawn_pool;
use network::spawn_network;
use nightshade::nightshade_task::spawn_nightshade_task;
use primitives::types::AccountId;

pub mod testing_utils;

pub fn start() {
    let (client_cfg, network_cfg, rpc_cfg) = get_testnet_configs();
    start_from_configs(client_cfg, network_cfg, rpc_cfg);
}

pub fn start_from_configs(
    client_cfg: ClientConfig,
    network_cfg: NetworkConfig,
    rpc_cfg: RPCConfig,
) {
    let client = Arc::new(Client::new(&client_cfg));
    start_from_client(client, Some(client_cfg.account_id), network_cfg, rpc_cfg)
}

pub fn start_from_client(
    client: Arc<Client>,
    account_id: Option<AccountId>,
    network_cfg: NetworkConfig,
    rpc_cfg: RPCConfig,
) {
    let node_task = futures::lazy(move || {
        spawn_rpc_server_task(client.clone(), &rpc_cfg);

        // Create all the consensus channels.
        let (inc_gossip_tx, inc_gossip_rx) = channel(1024);
        let (out_gossip_tx, out_gossip_rx) = channel(1024);
        let (consensus_tx, consensus_rx) = channel(1024);
        let (control_tx, control_rx) = channel(1024);

        // Launch tx gossip / payload sync.
        let (retrieve_payload_tx, retrieve_payload_rx) = channel(1024);
        let (payload_announce_tx, payload_announce_rx) = channel(1014);
        let (payload_request_tx, payload_request_rx) = channel(1024);
        let (payload_response_tx, payload_response_rx) = channel(1024);
        let (mempool_control_tx, mempool_control_rx) = channel(1024);
        spawn_pool(
            client.shard_client.pool.clone(),
            mempool_control_rx,
            control_tx,
            retrieve_payload_rx,
            payload_announce_tx,
            payload_request_tx,
            payload_response_rx,
            network_cfg.gossip_interval,
        );

        // Launch block syncing / importing.
        let (inc_block_tx, inc_block_rx) = channel(1024);
        let (out_block_tx, out_block_rx) = channel(1024);
        spawn_block_importer(client.clone(), inc_block_rx, mempool_control_tx.clone());

        // Launch block producer.
        spawn_block_producer(
            client.clone(),
            consensus_rx,
            mempool_control_tx,
            out_block_tx,
        );

        // Launch Nightshade task.
        spawn_nightshade_task(
            inc_gossip_rx,
            out_gossip_tx,
            consensus_tx,
            control_rx,
            retrieve_payload_tx,
        );

        // Launch Network task.
        spawn_network(
            client.clone(),
            account_id,
            network_cfg,
            inc_gossip_tx,
            out_gossip_rx,
            inc_block_tx,
            out_block_rx,
            payload_announce_rx,
            payload_request_rx,
            payload_response_tx,
        );

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
    use client::BlockProductionResult;
    use primitives::block_traits::SignedBlock;
    use primitives::chain::ChainPayload;
    use primitives::transaction::TransactionBody;

    use crate::testing_utils::{configure_chain_spec, wait, Node};

    /// Creates two nodes, one boot node and secondary node booting from it.
    /// Waits until they produce block with transfer money tx.
    #[test]
    fn two_nodes() {
        let chain_spec = configure_chain_spec();
        let alice = Node::new(
            "t1_alice",
            "alice.near",
            1,
            "127.0.0.1:3000",
            3030,
            vec![],
            chain_spec.clone(),
        );
        let bob = Node::new(
            "t1_bob",
            "bob.near",
            2,
            "127.0.0.1:3001",
            3031,
            vec![alice.node_info.clone()],
            chain_spec,
        );

        alice
            .client
            .shard_client
            .pool
            .add_transaction(
                TransactionBody::send_money(1, "alice.near", "bob.near", 10)
                    .sign(&alice.secret_key),
            )
            .unwrap();

        alice.start();
        bob.start();

        // Wait until alice and bob produce at least one block.
        wait(
            || {
                alice.client.shard_client.chain.best_block().index() >= 2
                    && bob.client.shard_client.chain.best_block().index() >= 2
            },
            500,
            80000,
        );

        // Check that transaction and it's receipt were included.
        let mut state_update = alice.client.shard_client.get_state_update();
        assert_eq!(
            alice
                .client
                .shard_client
                .trie_viewer
                .view_account(&mut state_update, &"alice.near".to_string())
                .unwrap()
                .amount,
            9999990
        );
        assert_eq!(
            alice
                .client
                .shard_client
                .trie_viewer
                .view_account(&mut state_update, &"bob.near".to_string())
                .unwrap()
                .amount,
            110
        );
    }

    /// Creates three nodes, two are authorities, first authority node is ahead on blocks.
    /// Wait until the second authority syncs and then build a block on top.
    /// Check that third node got the same state.
    #[test]
    #[ignore]
    fn test_three_nodes_sync() {
        let chain_spec = configure_chain_spec();
        let alice = Node::new(
            "t2_alice",
            "alice.near",
            1,
            "127.0.0.1:3002",
            3032,
            vec![],
            chain_spec.clone(),
        );
        let bob = Node::new(
            "t2_bob",
            "bob.near",
            2,
            "127.0.0.1:3003",
            3033,
            vec![alice.node_info.clone()],
            chain_spec.clone(),
        );
        let charlie = Node::new(
            "t2_charlie",
            "charlie.near",
            3,
            "127.0.0.1:3004",
            3034,
            vec![bob.node_info.clone()],
            chain_spec,
        );

        let (beacon_block, shard_block) =
            match alice.client.try_produce_block(1, ChainPayload::default()) {
                BlockProductionResult::Success(beacon_block, shard_block) => {
                    (beacon_block, shard_block)
                }
                _ => panic!("Should produce block"),
            };
        alice.client.try_import_blocks(beacon_block, shard_block);

        bob.client
            .shard_client
            .pool
            .add_transaction(
                TransactionBody::send_money(1, "alice.near", "bob.near", 10)
                    .sign(&alice.secret_key),
            )
            .unwrap();

        alice.start();
        bob.start();
        charlie.start();

        wait(|| {
            charlie.client.shard_client.chain.best_block().index() >= 3
        }, 500, 10000);

        // Check that non-authority synced into the same state.
        let mut state_update = charlie.client.shard_client.get_state_update();
        assert_eq!(
            charlie
                .client
                .shard_client
                .trie_viewer
                .view_account(&mut state_update, &"bob.near".to_string())
                .unwrap()
                .amount,
            110
        );
    }
}
