extern crate env_logger;
extern crate serde;
extern crate serde_derive;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use futures::sync::mpsc::channel;

use client::Client;
use configs::{get_alphanet_configs, ClientConfig, NetworkConfig, RPCConfig};
use coroutines::client_task::ClientTask;
use network::proxy::ProxyHandler;
use network::spawn_network;
use nightshade::nightshade_task::spawn_nightshade_task;
use primitives::crypto::signer::{AccountSigner, BLSSigner, EDSigner, InMemorySigner};
use tokio_utils::ShutdownableThread;

const KEY_STORE_PATH: &str = "storage/keystore";

pub fn start() {
    let (client_cfg, network_cfg, rpc_cfg) = get_alphanet_configs();
    let handle = start_from_configs(client_cfg, network_cfg, rpc_cfg);
    handle.wait_sigint_and_shutdown();
}

pub fn start_from_configs(
    client_cfg: ClientConfig,
    network_cfg: NetworkConfig,
    rpc_cfg: RPCConfig,
) -> ShutdownableThread {
    let signer = match client_cfg.account_id.clone() {
        Some(account_id) => {
            let mut key_file_path = client_cfg.base_path.to_path_buf();
            key_file_path.push(KEY_STORE_PATH);
            Some(Arc::new(InMemorySigner::from_key_file(
                account_id,
                key_file_path.as_path(),
                client_cfg.public_key.clone(),
            )))
        }
        None => None,
    };
    let client = Arc::new(Client::new(&client_cfg, signer));
    // Use empty pipeline to launch nodes on production.
    let proxy_handlers: Vec<Arc<ProxyHandler>> = vec![];
    start_from_client(client, network_cfg, rpc_cfg, client_cfg, proxy_handlers)
}

pub fn start_from_client<T: AccountSigner + BLSSigner + EDSigner + Clone + 'static>(
    client: Arc<Client<T>>,
    network_cfg: NetworkConfig,
    rpc_cfg: RPCConfig,
    client_cfg: ClientConfig,
    proxy_handlers: Vec<Arc<ProxyHandler>>,
) -> ShutdownableThread {
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
        let (inc_final_signatures_tx, inc_final_signatures_rx) = channel(1024);
        let (out_final_signatures_tx, out_final_signatures_rx) = channel(1024);
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
            out_final_signatures_tx,
            inc_final_signatures_rx,
            inc_payload_gossip_rx,
            out_payload_gossip_tx,
            inc_chain_state_rx,
            out_block_fetch_tx,
            network_cfg.gossip_interval,
        )
        .spawn();

        // Launch Nightshade task, if this client has block signer available.
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
            client.account_id(),
            network_cfg,
            client_cfg,
            inc_gossip_tx,
            out_gossip_rx,
            inc_block_tx,
            out_block_rx,
            payload_request_rx,
            payload_response_tx,
            inc_final_signatures_tx,
            out_final_signatures_rx,
            inc_payload_gossip_tx,
            out_payload_gossip_rx,
            inc_chain_state_tx,
            out_block_fetch_rx,
            proxy_handlers,
        );

        Ok(())
    });

    ShutdownableThread::start(node_task)
}

fn spawn_rpc_server_task<T: Send + Sync + 'static>(client: Arc<Client<T>>, rpc_config: &RPCConfig) {
    let http_addr = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), rpc_config.rpc_port));
    let http_api = node_http::api::HttpApi::new(client);
    node_http::server::spawn_server(http_api, http_addr);
}

#[cfg(test)]
mod tests {
    use primitives::block_traits::SignedBlock;
    use primitives::chain::ChainPayload;
    use primitives::test_utils::TestSignedBlock;
    use primitives::transaction::TransactionBody;
    use testlib::node::{
        configure_chain_spec, Node, NodeConfig, TEST_BLOCK_FETCH_LIMIT,
    };
    use testlib::test_helpers::wait;

    /// Creates two nodes, one boot node and secondary node booting from it.
    /// Waits until they produce block with transfer money tx.
    #[test]
    fn two_nodes() {
        let (test_prefix, test_port) = ("two_nodes", 7000);
        let chain_spec = configure_chain_spec();
        let money_to_send = 10;
        let init_balance = chain_spec.accounts[0].2;
        let alice = NodeConfig::for_test(
            test_prefix,
            test_port,
            "alice.near",
            1,
            vec![],
            chain_spec.clone(),
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );

        let bob = NodeConfig::for_test(
            test_prefix,
            test_port,
            "bob.near",
            2,
            vec![alice.boot_addr()],
            chain_spec,
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );
        let mut alice = Node::new(alice);
        let mut bob = Node::new(bob);

        alice
            .add_transaction(
                TransactionBody::send_money(1, "alice.near", "bob.near", money_to_send)
                    .sign(&*alice.signer()),
            )
            .unwrap();

        alice.start();
        bob.start();

        // Wait until alice and bob produce at least one block.
        wait(
            || {
                alice.as_thread_ref().client.shard_client.chain.best_index() >= 3
                    && bob.as_thread_ref().client.shard_client.chain.best_index() >= 3
            },
            500,
            600000,
        );

        // Check that transaction and it's receipt were included.
        assert_eq!(
            alice.view_balance(&"alice.near".to_string()).unwrap(),
            init_balance - money_to_send
        );
        assert_eq!(
            alice.view_balance(&"bob.near".to_string()).unwrap(),
            init_balance + money_to_send
        );
    }

    /// Creates three nodes, two are authorities, first authority node is ahead on blocks.
    /// Wait until the second authority syncs and then build a block on top.
    /// Check that third node got the same state.
    #[test]
    fn test_three_nodes_sync() {
        let (test_prefix, test_port) = ("three_nodes_sync", 7010);
        let chain_spec = configure_chain_spec();
        let money_to_send = 10;
        let init_balance = chain_spec.accounts[0].2;
        let alice = NodeConfig::for_test(
            test_prefix,
            test_port,
            "alice.near",
            1,
            vec![],
            chain_spec.clone(),
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );
        let bob = NodeConfig::for_test(
            test_prefix,
            test_port,
            "bob.near",
            2,
            vec![alice.boot_addr()],
            chain_spec.clone(),
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );
        let charlie = NodeConfig::for_test_passive(
            test_prefix,
            test_port,
            None,
            3,
            vec![bob.boot_addr()],
            chain_spec.clone(),
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );

        let mut alice = Node::new(alice);
        let mut bob = Node::new(bob);
        let mut charlie = Node::new(charlie);

        let (mut beacon_block, mut shard_block, shard_extra) =
            alice.as_thread_mut().client.prepare_block(ChainPayload::default());
        // Sign by alice & bob to make this blocks valid.
        let (_, authorities) =
            alice.as_thread_mut().client.get_uid_to_authority_map(beacon_block.index());
        let signers = vec![alice.signer(), bob.signer()];
        beacon_block.sign_all(&authorities, &signers);
        shard_block.sign_all(&authorities, &signers);
        alice.as_thread_mut().client.try_import_produced(beacon_block, shard_block, shard_extra);

        bob.as_thread_mut()
            .add_transaction(
                TransactionBody::send_money(1, "alice.near", "bob.near", money_to_send)
                    .sign(&*alice.signer()),
            )
            .unwrap();

        alice.start();
        bob.start();
        charlie.start();

        wait(|| charlie.as_thread_ref().client.shard_client.chain.best_index() >= 4, 500, 60000);

        // Check that non-authority synced into the same state.
        assert_eq!(
            charlie.view_balance(&"bob.near".to_string()).unwrap(),
            init_balance + money_to_send
        );
    }

    /// Creates two nodes, first authority node is ahead on blocks.
    /// Post a transaction on the second authority.
    /// Wait until the second authority syncs and check that transaction is applied.
    #[test]
    fn test_late_transaction() {
        let (test_prefix, test_port) = ("late_transaction", 7020);
        let chain_spec = configure_chain_spec();
        let money_to_send = 10;
        let init_balance = chain_spec.accounts[0].2;
        let alice = NodeConfig::for_test(
            test_prefix,
            test_port,
            "alice.near",
            1,
            vec![],
            chain_spec.clone(),
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );
        let bob = NodeConfig::for_test(
            test_prefix,
            test_port,
            "bob.near",
            2,
            vec![alice.boot_addr()],
            chain_spec.clone(),
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );
        let mut alice = Node::new(alice);
        let mut bob = Node::new(bob);
        let (mut beacon_block, mut shard_block, shard_extra) =
            alice.as_thread_mut().client.prepare_block(ChainPayload::default());
        // Sign by alice & bob to make this blocks valid.
        let (_, authorities) =
            alice.as_thread_mut().client.get_uid_to_authority_map(beacon_block.index());
        let signers = vec![alice.signer(), bob.signer()];
        beacon_block.sign_all(&authorities, &signers);
        shard_block.sign_all(&authorities, &signers);
        alice.as_thread_mut().client.try_import_produced(beacon_block, shard_block, shard_extra);

        bob.add_transaction(
            TransactionBody::send_money(1, "alice.near", "bob.near", money_to_send)
                .sign(&*alice.signer()),
        )
        .unwrap();

        alice.start();
        bob.start();

        wait(|| alice.as_thread_ref().client.shard_client.chain.best_index() >= 4, 500, 60000);

        // Check that non-authority synced into the same state.
        assert_eq!(
            alice.view_balance(&"bob.near".to_string()).unwrap(),
            init_balance + money_to_send
        );
    }

    /// Creates two authority nodes, run them for 10 blocks.
    /// Two non-authorities join later and must catch up.
    #[test]
    fn test_new_nodes_catchup() {
        let (test_prefix, test_port) = ("new_node_catchup", 7030);
        let chain_spec = configure_chain_spec();
        let alice = NodeConfig::for_test(
            test_prefix,
            test_port,
            "alice.near",
            1,
            vec![],
            chain_spec.clone(),
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );
        let bob = NodeConfig::for_test(
            test_prefix,
            test_port,
            "bob.near",
            2,
            vec![alice.boot_addr()],
            chain_spec.clone(),
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );
        let charlie = NodeConfig::for_test(
            test_prefix,
            test_port,
            "charlie.near",
            3,
            vec![bob.boot_addr()],
            chain_spec.clone(),
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );
        let dan = NodeConfig::for_test(
            test_prefix,
            test_port,
            "dan.near",
            4,
            vec![charlie.boot_addr()],
            chain_spec,
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );
        let mut alice = Node::new(alice);
        let mut bob = Node::new(bob);
        let mut charlie = Node::new(charlie);
        let mut dan = Node::new(dan);

        alice.start();
        bob.start();

        wait(|| alice.as_thread_ref().client.shard_client.chain.best_index() >= 2, 500, 60000);

        charlie.start();
        dan.start();
        wait(|| charlie.as_thread_ref().client.shard_client.chain.best_index() >= 2, 500, 60000);
        wait(|| dan.as_thread_ref().client.shard_client.chain.best_index() >= 2, 500, 60000);
    }

    #[test]
    /// One node produces 500 blocks and the other node starts and tries to catch up.
    /// Check that the catchup works and after the catchup, they can produce blocks.
    fn test_node_sync() {
        let (test_prefix, test_port) = ("new_node_sync", 7040);
        let chain_spec = configure_chain_spec();
        let alice = NodeConfig::for_test(
            test_prefix,
            test_port,
            "alice.near",
            1,
            vec![],
            chain_spec.clone(),
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );
        let bob = NodeConfig::for_test(
            test_prefix,
            test_port,
            "bob.near",
            2,
            vec![alice.boot_addr()],
            chain_spec.clone(),
            TEST_BLOCK_FETCH_LIMIT,
            vec![],
        );
        let mut alice = Node::new(alice);
        let mut bob = Node::new(bob);
        let alice_client = alice.as_thread_mut().client.clone();
        let bob_client = bob.as_thread_mut().client.clone();
        for i in 0..100 {
            let transaction = TransactionBody::send_money(i + 1, "alice.near", "bob.near", 1)
                .sign(&*alice.signer());
            let payload = ChainPayload::new(vec![transaction], vec![]);
            let (mut beacon_block, mut shard_block, shard_extra) =
                alice_client.prepare_block(payload);
            // Sign by alice & bob to make this blocks valid.
            let (_, authorities) = alice_client.get_uid_to_authority_map(beacon_block.index());
            let signers = vec![alice.signer(), bob.signer()];
            beacon_block.sign_all(&authorities, &signers);
            shard_block.sign_all(&authorities, &signers);
            alice_client.try_import_produced(beacon_block, shard_block, shard_extra);
        }
        assert_eq!(alice_client.shard_client.chain.best_index(), 100);

        alice.start();
        bob.start();

        wait(|| bob_client.shard_client.chain.best_index() >= 101, 1000, 600000);
    }
}
