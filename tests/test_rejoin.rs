//! Tests whether nodes can leave and rejoin the consensus.
#[cfg(test)]
#[cfg(feature = "expensive_tests")]
mod test {
    use std::process::Command;
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::Duration;

    use near_chain_configs::Genesis;
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::types::AccountId;
    use node_runtime::config::RuntimeConfig;
    use testlib::node::{create_nodes, sample_queryable_node, sample_two_nodes, Node, NodeConfig};
    use testlib::test_helpers::{heavy_test, wait, wait_for_catchup};

    fn warmup() {
        Command::new("cargo")
            .args(&["build", "-p", "neard"])
            .spawn()
            .expect("warmup failed")
            .wait()
            .unwrap();
    }

    // DISCLAIMER. These tests are very heavy and somehow manage to interfere with each other.
    // If you add multiple tests and they start failing consider splitting it into several *.rs files
    // to ensure they are not run in parallel.

    fn send_transaction(
        nodes: &Vec<Arc<RwLock<dyn Node>>>,
        account_names: &Vec<AccountId>,
        nonces: &Vec<u64>,
        from: usize,
        to: usize,
    ) {
        let k = sample_queryable_node(nodes);
        nodes[k]
            .read()
            .unwrap()
            .add_transaction(SignedTransaction::send_money(
                nonces[from],
                account_names[from].clone(),
                account_names[to].clone(),
                &*nodes[from].read().unwrap().signer(),
                1000,
                nodes[k].read().unwrap().user().get_best_block_hash().unwrap(),
            ))
            .unwrap();
    }

    fn test_kill_1(num_nodes: usize, num_trials: usize, two_shards: bool, test_prefix: &str) {
        warmup();
        // Start all nodes, crash node#2, proceed, restart node #2 but crash node #3
        let crash1 = 2;
        let crash2 = 3;
        let nodes = create_nodes(num_nodes, test_prefix);
        // Convert all the thread nodes into processes (otherwise killing doesn't free up the port)
        // Disable fees (so that we can validate exact balances)
        // Add one more shard, so that the chunk production schedule doesn't get perfectly aligned
        //   with the block production schedule (otherwise chunk from 0 would always be included in
        //   the block by 2, and thus when 2 is killed, 0 won't be able to have their tx through)
        let nodes: Vec<_> = nodes
            .into_iter()
            .map(|node_cfg| {
                // Disable fees
                if let NodeConfig::Thread(cfg) = node_cfg {
                    let mut new_cfg = cfg;

                    let Genesis { config, records, .. } = &*new_cfg.genesis;
                    let mut config = config.clone();
                    config.runtime_config = RuntimeConfig::free();
                    config.num_block_producer_seats = 17;
                    config.num_block_producer_seats_per_shard =
                        if two_shards { vec![8, 9] } else { vec![17] };
                    new_cfg.genesis = Arc::new(Genesis::new(config, records.clone()));
                    NodeConfig::Process(new_cfg)
                } else {
                    unreachable!()
                }
            })
            .collect();

        let nodes: Vec<_> = nodes.into_iter().map(|cfg| Node::new_sharable(cfg)).collect();
        let account_names: Vec<_> =
            nodes.iter().map(|node| node.read().unwrap().account_id().unwrap()).collect();

        for i in 0..num_nodes {
            nodes[i].write().unwrap().start();
        }

        let mut expected_balances = vec![0; num_nodes];
        let mut nonces = vec![1; num_nodes];
        for i in 0..num_nodes {
            nonces[i] = nodes[i]
                .read()
                .unwrap()
                .get_access_key_nonce_for_signer(&account_names[i])
                .unwrap()
                + 1;
            let account = nodes[0].read().unwrap().view_account(&account_names[i]).unwrap();
            expected_balances[i] = account.amount;
        }
        let trial_duration = 90000;
        for trial in 0..num_trials {
            println!("TRIAL #{}", trial);
            if trial % 10 == 3 {
                println!("Killing node {}", crash1);
                nodes[crash1].write().unwrap().kill();
                // Need to wait enough time for others to remove the killed node as a peer, so
                // that future transactions don't get routed to them
                thread::sleep(Duration::from_secs(10));
            }
            if trial % 10 == 6 {
                println!("Restarting node {}", crash1);
                nodes[crash1].write().unwrap().start();
                wait_for_catchup(&nodes);
                println!("Killing node {}", crash2);
                nodes[crash2].write().unwrap().kill();
                // Need to wait enough time for others to remove the killed node as a peer, so
                // that future transactions don't get routed to them
                thread::sleep(Duration::from_secs(10));
            }
            if trial % 10 == 9 {
                println!("Restarting node {}", crash2);
                nodes[crash2].write().unwrap().start();
            }

            let (i, j) = sample_two_nodes(num_nodes);
            send_transaction(&nodes, &account_names, &nonces, i, j);
            nonces[i] += 1;
            expected_balances[i] -= 1000;
            expected_balances[j] += 1000;
            let t = sample_queryable_node(&nodes);
            wait(
                || {
                    let amt = nodes[t].read().unwrap().view_balance(&account_names[j]).unwrap();
                    expected_balances[j] <= amt
                },
                1000,
                trial_duration,
            );
        }
    }

    fn test_kill_2(num_nodes: usize, num_trials: usize, test_prefix: &str) {
        warmup();
        // Start all nodes, crash nodes 2 and 3, restart node 2, proceed, restart node 3
        let (crash1, crash2) = (2, 3);
        let nodes = create_nodes(num_nodes, test_prefix);

        // Convert some of the thread nodes into processes.
        let nodes: Vec<_> = nodes
            .into_iter()
            .map(|node_cfg| {
                if rand::random::<bool>() {
                    if let NodeConfig::Thread(cfg) = node_cfg {
                        NodeConfig::Process(cfg)
                    } else {
                        unimplemented!()
                    }
                } else {
                    node_cfg
                }
            })
            .collect();
        let nodes: Vec<Arc<RwLock<dyn Node>>> =
            nodes.into_iter().map(|cfg| Node::new_sharable(cfg)).collect();
        let account_names: Vec<_> =
            nodes.iter().map(|node| node.read().unwrap().account_id().unwrap()).collect();

        for i in 0..num_nodes {
            nodes[i].write().unwrap().start();
        }

        let mut expected_balances = vec![0; num_nodes];
        let mut nonces = vec![1; num_nodes];
        for i in 0..num_nodes {
            let account = nodes[i].read().unwrap().view_account(&account_names[i]).unwrap();
            nonces[i] = 1 + nodes[i]
                .read()
                .unwrap()
                .get_access_key_nonce_for_signer(&account_names[i])
                .unwrap();
            expected_balances[i] = account.amount;
        }

        let trial_duration = 20000;
        for trial in 0..num_trials {
            println!("TRIAL #{}", trial);
            let (i, j) = sample_two_nodes(num_nodes);
            if trial % 5 == 2 {
                // Here we kill two nodes, make sure transactions stop going through,
                // then restart one of the nodes
                println!("Killing nodes {}, {}", crash1, crash2);
                nodes[crash1].write().unwrap().kill();
                nodes[crash2].write().unwrap().kill();

                send_transaction(&nodes, &account_names, &nonces, i, j);
                thread::sleep(Duration::from_secs(2));
                let t = sample_queryable_node(&nodes);
                assert_eq!(
                    nodes[t].read().unwrap().view_balance(&account_names[j]).unwrap(),
                    expected_balances[j]
                );

                println!("Restarting node {}", crash1);
                nodes[crash1].write().unwrap().start();
            } else {
                send_transaction(&nodes, &account_names, &nonces, i, j);
                if trial % 5 == 4 {
                    // Restart the second of the nodes killed earlier
                    println!("Restarting node {}", crash2);
                    nodes[crash2].write().unwrap().start();
                }
            }
            nonces[i] += 1;
            expected_balances[i] -= 100;
            expected_balances[j] += 100;
            let t = sample_queryable_node(&nodes);
            wait(
                || {
                    let amt = nodes[t].read().unwrap().view_balance(&account_names[j]).unwrap();
                    expected_balances[j] <= amt
                },
                1000,
                trial_duration,
            );
        }
    }

    #[test]
    fn test_4_20_kill1() {
        heavy_test(|| test_kill_1(4, 10, false, "4_10_kill1"));
    }

    #[test]
    fn test_4_20_kill1_two_shards() {
        heavy_test(|| test_kill_1(4, 10, true, "4_10_kill1"));
    }

    #[test]
    #[ignore]
    fn test_4_20_kill2() {
        heavy_test(|| test_kill_2(4, 5, "4_10_kill2"));
    }
}
