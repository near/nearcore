extern crate env_logger;
extern crate serde;
extern crate serde_derive;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use futures::sync::mpsc::channel;

use client::Client;
use configs::{get_alphanet_configs, ClientConfig, NetworkConfig, RPCConfig};
use coroutines::importer::spawn_block_importer;
use coroutines::ns_producer::spawn_block_producer;
use mempool::pool_task::spawn_pool;
use network::spawn_network;
use nightshade::nightshade_task::spawn_nightshade_task;
use primitives::types::AccountId;

pub mod testing_utils;

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
        spawn_block_producer(client.clone(), consensus_rx, mempool_control_tx, out_block_tx);

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
    use configs::ChainSpec;
    use primitives::signer::BlockSigner;
    use primitives::signer::InMemorySigner;
    use primitives::signer::TransactionSigner;
    use std::thread;
    use std::time::Duration;

    /// Creates two nodes, one boot node and secondary node booting from it.
    /// Waits until they produce block with transfer money tx.
    #[test]
    fn two_nodes() {
        let chain_spec = configure_chain_spec();
        let alice = Node::new(
            "t1_alice",
            "alice.near",
            1,
            Some("127.0.0.1:3000"),
            3030,
            vec![],
            chain_spec.clone(),
        );
        let bob = Node::new(
            "t1_bob",
            "bob.near",
            2,
            Some("127.0.0.1:3001"),
            3031,
            vec![alice.node_info.clone()],
            chain_spec,
        );
        let alice_signer = alice.signer();
        let bob_signer = bob.signer();
        println!(
            "Alice pk={:?}, bls pk={:?}",
            alice_signer.public_key.to_readable(),
            alice_signer.bls_public_key.to_readable()
        );
        println!(
            "Bob pk={:?}, bls pk={:?}",
            bob_signer.public_key.to_readable(),
            bob_signer.bls_public_key.to_readable()
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
                alice.client.shard_client.chain.best_block().index() >= 2
                    && bob.client.shard_client.chain.best_block().index() >= 2
            },
            500,
            60000,
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
        let chain_spec = configure_chain_spec();
        let alice = Node::new(
            "t2_alice",
            "alice.near",
            1,
            Some("127.0.0.1:3002"),
            3032,
            vec![],
            chain_spec.clone(),
        );
        let bob = Node::new(
            "t2_bob",
            "bob.near",
            2,
            Some("127.0.0.1:3003"),
            3033,
            vec![alice.node_info.clone()],
            chain_spec.clone(),
        );
        let charlie = Node::new(
            "t2_charlie",
            "charlie.near",
            3,
            None,
            3034,
            vec![bob.node_info.clone()],
            chain_spec,
        );

        let (mut beacon_block, mut shard_block) =
            match alice.client.try_produce_block(1, ChainPayload::default()) {
                BlockProductionResult::Success(beacon_block, shard_block) => {
                    (beacon_block, shard_block)
                }
                _ => panic!("Should produce block"),
            };
        // Sign by bob to make this blocks valid.
        beacon_block.add_signature(&beacon_block.sign(bob.signer()), 1);
        shard_block.add_signature(&shard_block.sign(bob.signer()), 1);
        alice.client.try_import_blocks(beacon_block, shard_block);

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
        charlie.start();

        wait(|| charlie.client.shard_client.chain.best_block().index() >= 3, 500, 10000);

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

    #[test]
    fn test_multiple_nodes() {
        // Modify the following two variables to run more nodes or to exercise them for multiple
        // trials.
        let num_nodes = 2;
        let num_trials = 10;

        let init_balance = 1_000_000_000;
        let mut account_names = vec![];
        let mut node_names = vec![];
        for i in 0..num_nodes {
            account_names.push(format!("near.{}", i));
            node_names.push(format!("node_{}", i));
        }
        let chain_spec = generate_test_chain_spec(&account_names, init_balance);

        let mut nodes = vec![];
        let mut boot_nodes = vec![];
        // Launch nodes in a chain, such that X+1 node boots from X node.
        for i in 0..num_nodes {
            let node = Node::new(
                node_names[i].as_str(),
                account_names[i].as_str(),
                i as u32 + 1,
                Some(format!("127.0.0.1:{}", 3000 + i).as_str()),
                3030 + i as u16,
                boot_nodes,
                chain_spec.clone(),
            );
            boot_nodes = vec![node.node_info.clone()];
            node.start();
            nodes.push(node);
        }
        //        thread::sleep(Duration::from_secs(10));

        // Execute N trials. In each trial we submit a transaction to a random node i, that sends
        // 1 token to a random node j. Then we wait for the balance change to propagate by checking
        // the balance of j on node k.
        let mut expected_balances = vec![init_balance; num_nodes];
        let mut nonces = vec![1; num_nodes];
        let trial_duration = 10000;
        for trial in 0..num_trials {
            println!("TRIAL #{}", trial);
            let i = rand::random::<usize>() % num_nodes;
            // Should be a different node.
            let mut j = rand::random::<usize>() % (num_nodes - 1);
            if j >= i {
                j += 1;
            }
            for k in 0..num_nodes {
                nodes[k]
                    .client
                    .shard_client
                    .pool
                    .add_transaction(
                        TransactionBody::send_money(
                            nonces[i],
                            account_names[i].as_str(),
                            account_names[j].as_str(),
                            1,
                        )
                        .sign(nodes[i].signer()),
                    )
                    .unwrap();
            }
            nonces[i] += 1;
            expected_balances[i] -= 1;
            expected_balances[j] += 1;

            wait(
                || {
                    let mut state_update = nodes[j].client.shard_client.get_state_update();
                    let amt = nodes[j]
                        .client
                        .shard_client
                        .trie_viewer
                        .view_account(&mut state_update, &account_names[j])
                        .unwrap()
                        .amount;
                    expected_balances[j] == amt
                },
                1000,
                trial_duration,
            );
        }
    }

    pub fn generate_test_chain_spec(account_names: &Vec<String>, balance: u64) -> ChainSpec {
        let genesis_wasm =
            include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
        let mut accounts = vec![];
        let mut initial_authorities = vec![];
        for name in account_names {
            let signer = InMemorySigner::from_seed(name.as_str(), name.as_str());
            accounts.push((name.to_string(), signer.public_key().to_readable(), balance, 10));
            initial_authorities.push((
                name.to_string(),
                signer.public_key().to_readable(),
                signer.bls_public_key().to_readable(),
                50,
            ));
        }
        let num_authorities = account_names.len();
        ChainSpec {
            accounts,
            initial_authorities,
            genesis_wasm,
            beacon_chain_epoch_length: 1,
            beacon_chain_num_seats_per_slot: num_authorities as u64,
            boot_nodes: vec![],
        }
    }
}
