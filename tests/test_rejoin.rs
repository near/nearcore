//! Tests whether nodes can leave and rejoin the consensus.
#[cfg(test)]
mod test {
    use std::process::Command;
    use std::thread;
    use std::time::Duration;

    use network::proxy::benchmark::BenchmarkHandler;
    use network::proxy::ProxyHandler;
    use primitives::transaction::TransactionBody;
    use primitives::types::AccountId;
    use std::sync::{Arc, RwLock};
    use testlib::node::{
        create_nodes, sample_queryable_node, sample_two_nodes, Node, NodeConfig,
        TEST_BLOCK_FETCH_LIMIT, TEST_BLOCK_MAX_SIZE,
    };
    use testlib::test_helpers::{heavy_test, wait, wait_for_catchup};

    fn warmup() {
        Command::new("cargo").args(&["build"]).spawn().expect("warmup failed").wait().unwrap();
    }

    // DISCLAIMER. These tests are very heavy and somehow manage to interfere with each other.
    // If you add multiple tests and they start failing consider splitting it into several *.rs files
    // to ensure they are not run in parallel.

    fn send_transaction(
        nodes: &Vec<Arc<RwLock<Node>>>,
        account_names: &Vec<AccountId>,
        nonces: &Vec<u64>,
        from: usize,
        to: usize,
    ) {
        let k = sample_queryable_node(nodes);
        nodes[k]
            .read()
            .unwrap()
            .add_transaction(
                TransactionBody::send_money(
                    nonces[from],
                    account_names[from].as_str(),
                    account_names[to].as_str(),
                    1,
                )
                .sign(&*nodes[from].read().unwrap().signer()),
            )
            .unwrap();
    }

    fn test_kill_1(num_nodes: usize, num_trials: usize, test_prefix: &str, test_port: u16) {
        warmup();
        let proxy_handlers: Vec<Arc<ProxyHandler>> = vec![Arc::new(BenchmarkHandler::new())];
        // Start all nodes, crash node#2, proceed, restart node #2 but crash node #3
        let crash1 = 2;
        let crash2 = 3;
        let (init_balance, account_names, nodes) = create_nodes(
            num_nodes,
            test_prefix,
            test_port,
            TEST_BLOCK_FETCH_LIMIT,
            TEST_BLOCK_MAX_SIZE,
            proxy_handlers,
        );
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

        let nodes: Vec<_> = nodes.into_iter().map(|cfg| Node::new_sharable(cfg)).collect();

        for i in 0..num_nodes {
            nodes[i].write().unwrap().start();
        }

        let mut expected_balances = vec![init_balance; num_nodes];
        let mut nonces = vec![1; num_nodes];
        let trial_duration = 60000;
        for trial in 0..num_trials {
            println!("TRIAL #{}", trial);
            if trial % 10 == 3 {
                println!("Killing node {}", crash1);
                nodes[crash1].write().unwrap().kill();
                thread::sleep(Duration::from_secs(2));
            }
            if trial % 10 == 6 {
                println!("Restarting node {}", crash1);
                nodes[crash1].write().unwrap().start();
                wait_for_catchup(&nodes);
                println!("Killing node {}", crash2);
                nodes[crash2].write().unwrap().kill();
            }
            if trial % 10 == 9 {
                println!("Restarting node {}", crash2);
                nodes[crash2].write().unwrap().start();
            }

            let (i, j) = sample_two_nodes(num_nodes);
            send_transaction(&nodes, &account_names, &nonces, i, j);
            nonces[i] += 1;
            expected_balances[i] -= 1;
            expected_balances[j] += 1;
            let t = sample_queryable_node(&nodes);
            wait(
                || {
                    let amt = nodes[t].read().unwrap().view_balance(&account_names[j]).unwrap();
                    expected_balances[j] == amt
                },
                1000,
                trial_duration,
            );
        }
    }

    fn test_kill_2(num_nodes: usize, num_trials: usize, test_prefix: &str, test_port: u16) {
        warmup();
        // Start all nodes, crash nodes 2 and 3, restart node 2, proceed, restart node 3
        let (crash1, crash2) = (2, 3);
        let (init_balance, account_names, nodes) = create_nodes(
            num_nodes,
            test_prefix,
            test_port,
            TEST_BLOCK_FETCH_LIMIT,
            TEST_BLOCK_MAX_SIZE,
            vec![],
        );

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
        let nodes: Vec<Arc<RwLock<Node>>> =
            nodes.into_iter().map(|cfg| Node::new_sharable(cfg)).collect();

        for i in 0..num_nodes {
            nodes[i].write().unwrap().start();
        }

        let mut expected_balances = vec![init_balance; num_nodes];
        let mut nonces = vec![1; num_nodes];
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
            expected_balances[i] -= 1;
            expected_balances[j] += 1;
            let t = sample_queryable_node(&nodes);
            wait(
                || {
                    let amt = nodes[t].read().unwrap().view_balance(&account_names[j]).unwrap();
                    expected_balances[j] == amt
                },
                1000,
                trial_duration,
            );
        }
    }

    #[test]
    fn test_4_20_kill1() {
        heavy_test(|| test_kill_1(4, 10, "4_10_kill1", 3200));
    }

    #[test]
    #[ignore]
    fn test_4_20_kill2() {
        heavy_test(|| test_kill_2(4, 5, "4_10_kill2", 3300));
    }
}
