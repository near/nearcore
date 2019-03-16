extern crate env_logger;
extern crate serde;
extern crate serde_derive;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use futures::sync::mpsc::channel;

use client::Client;
use configs::{get_alphanet_configs, ClientConfig, NetworkConfig, RPCConfig};
use coroutines::client_task::ClientTask;
use network::spawn_network;
use nightshade::nightshade_task::spawn_nightshade_task;
use primitives::types::AccountId;

pub fn start() {
    let (client_cfg, network_cfg, rpc_cfg) = get_alphanet_configs();
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
        let (inc_payload_gossip_tx, inc_payload_gossip_rx) = channel(1024);
        let (out_payload_gossip_tx, out_payload_gossip_rx) = channel(1024);
        let (consensus_tx, consensus_rx) = channel(1024);
        let (control_tx, control_rx) = channel(1024);

        // Launch tx gossip / payload sync.
        let (retrieve_payload_tx, retrieve_payload_rx) = channel(1024);
        let (payload_request_tx, payload_request_rx) = channel(1024);
        let (payload_response_tx, payload_response_rx) = channel(1024);

        // Launch block syncing / importing.
        let (inc_block_tx, inc_block_rx) = channel(1024);
        let (out_block_tx, out_block_rx) = channel(1024);
        let (inc_chain_state_tx, inc_chain_state_rx) = channel(1024);
        let (out_block_fetch_tx, out_block_fetch_rx) = channel(1024);

        // Launch Client task.
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
            network_cfg.gossip_interval,
        )
        .spawn();

        // Launch Nightshade task.
        spawn_nightshade_task(
            client.signer.clone(),
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
            payload_request_rx,
            payload_response_tx,
            inc_payload_gossip_tx,
            out_payload_gossip_rx,
            inc_chain_state_tx,
            out_block_fetch_rx,
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

    use testlib::alphanet_utils::{configure_chain_spec, wait, Node};

    /// Creates two nodes, one boot node and secondary node booting from it.
    /// Waits until they produce block with transfer money tx.
    #[test]
    fn two_nodes() {
        let (test_prefix, test_port) = ("two_nodes", 7000);
        let chain_spec = configure_chain_spec();
        let alice = Node::for_test(
            test_prefix,
            test_port,
            "alice.near",
            1,
            vec![],
            chain_spec.clone(),
        );
        let bob = Node::for_test(
            test_prefix,
            test_port,
            "bob.near",
            2,
            vec![alice.node_addr()],
            chain_spec,
        );
        alice
            .client
            .shard_client
            .pool
            .add_transaction(
                TransactionBody::send_money(1, "alice.near", "bob.near", 10).sign(alice.signer()),
            )
            .unwrap();

        alice.start();
        bob.start();

        // Wait until alice and bob produce at least one block.
        wait(
            || {
                alice.client.shard_client.chain.best_index() >= 2
                    && bob.client.shard_client.chain.best_index() >= 2
            },
            500,
            600000,
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
    fn test_three_nodes_sync() {
        let (test_prefix, test_port) = ("three_nodes_sync", 7010);
        let chain_spec = configure_chain_spec();
        let alice = Node::for_test(
            test_prefix,
            test_port,
            "alice.near",
            1,
            vec![],
            chain_spec.clone(),
        );
        let bob = Node::for_test(
            test_prefix,
            test_port,
            "bob.near",
            2,
            vec![alice.node_addr()],
            chain_spec.clone(),
        );
        let charlie = Node::for_test_passive(
            test_prefix,
            test_port,
            "charlie.near",
            3,
            vec![bob.node_addr()],
            chain_spec,
        );

        let (mut beacon_block, mut shard_block) =
            match alice.client.try_produce_block(1, ChainPayload::default()) {
                BlockProductionResult::Success(beacon_block, shard_block) => {
                    (*beacon_block, *shard_block)
                }
                _ => panic!("Should produce block"),
            };
        // Sign by bob to make this blocks valid.
        beacon_block.add_signature(&beacon_block.sign(bob.signer()), 1);
        shard_block.add_signature(&shard_block.sign(bob.signer()), 1);
        alice.client.try_import_blocks(beacon_block, shard_block);

        bob
            .client
            .shard_client
            .pool
            .add_transaction(
                TransactionBody::send_money(1, "alice.near", "bob.near", 10).sign(alice.signer()),
            )
            .unwrap();

        alice.start();
        bob.start();
        charlie.start();

        wait(|| charlie.client.shard_client.chain.best_index() >= 3, 500, 60000);

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

    /// Creates two nodes, first authority node is ahead on blocks.
    /// Post a transaction on the second authority.
    /// Wait until the second authority syncs and check that transaction is applied.
    #[test]
    fn test_late_transaction() {
        let (test_prefix, test_port) = ("late_transaction", 7020);
        let chain_spec = configure_chain_spec();
        let alice = Node::for_test(
            test_prefix,
            test_port,
            "alice.near",
            1,
            vec![],
            chain_spec.clone(),
        );
        let bob = Node::for_test(
            test_prefix,
            test_port,
            "bob.near",
            2,
            vec![alice.node_addr()],
            chain_spec.clone(),
        );
        let (mut beacon_block, mut shard_block) =
            match alice.client.try_produce_block(1, ChainPayload::default()) {
                BlockProductionResult::Success(beacon_block, shard_block) => {
                    (*beacon_block, *shard_block)
                }
                _ => panic!("Should produce block"),
            };
        // Sign by bob to make this blocks valid.
        beacon_block.add_signature(&beacon_block.sign(bob.signer()), 1);
        shard_block.add_signature(&shard_block.sign(bob.signer()), 1);
        alice.client.try_import_blocks(beacon_block, shard_block);

        bob.client
            .shard_client
            .pool
            .add_transaction(
                TransactionBody::send_money(1, "alice.near", "bob.near", 10)
                    .sign(alice.signer()),
            )
            .unwrap();

        alice.start();
        bob.start();

        wait(|| {
            alice.client.shard_client.chain.best_index() >= 3
        }, 500, 60000);

        // Check that non-authority synced into the same state.
        let mut state_update = alice.client.shard_client.get_state_update();
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

    /// Creates two authority nodes, run them for 10 blocks.
    /// Two non-authorities join later and must catch up.
    #[test]
    fn test_new_nodes_catchup() {
        let (test_prefix, test_port) = ("new_node_catchup", 7030);
        let chain_spec = configure_chain_spec();
        let alice = Node::for_test(
            test_prefix,
            test_port,
            "alice.near",
            1,
            vec![],
            chain_spec.clone(),
        );
        let bob = Node::for_test(
            test_prefix,
            test_port,
            "bob.near",
            2,
            vec![alice.node_addr()],
            chain_spec.clone(),
        );
        let charlie = Node::for_test(
            test_prefix,
            test_port,
            "charlie.near",
            3,
            vec![bob.node_addr()],
            chain_spec.clone(),
        );
        let dan = Node::for_test(
            test_prefix,
            test_port,
            "dan.near",
            4,
            vec![charlie.node_addr()],
            chain_spec,
        );

        alice.start();
        bob.start();

        wait(|| alice.client.shard_client.chain.best_index() >= 2, 500, 60000);

        charlie.start();
        dan.start();
        wait(|| charlie.client.shard_client.chain.best_index() >= 2, 500, 60000);
        wait(|| dan.client.shard_client.chain.best_index() >= 2, 500, 60000);

    }
}
