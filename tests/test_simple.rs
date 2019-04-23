///! Simply starts and runs TestNet for a while.
#[cfg(feature = "old_tests")]
#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use network::proxy::benchmark::BenchmarkHandler;
    use network::proxy::ProxyHandler;
    use primitives::transaction::TransactionBody;
    use testlib::node::{
        create_nodes, sample_two_nodes, Node, TEST_BLOCK_FETCH_LIMIT, TEST_BLOCK_MAX_SIZE,
    };
    use testlib::test_helpers::{heavy_test, wait};

    fn run_multiple_nodes(num_nodes: usize, num_trials: usize, test_prefix: &str, test_port: u16) {
        // Add proxy handlers to the pipeline.
        let proxy_handlers: Vec<Arc<ProxyHandler>> = vec![Arc::new(BenchmarkHandler::new())];

        let (init_balance, account_names, mut nodes) = create_nodes(
            num_nodes,
            test_prefix,
            test_port,
            TEST_BLOCK_FETCH_LIMIT,
            TEST_BLOCK_MAX_SIZE,
            proxy_handlers,
        );

        let nodes: Vec<_> = nodes.drain(..).map(|cfg| Node::new_sharable(cfg)).collect();
        for i in 0..num_nodes {
            nodes[i].write().unwrap().start();
        }

        // Execute N trials. In each trial we submit a transaction to a random node i, that sends
        // 1 token to a random node j. We send transaction to node Then we wait for the balance change to propagate by checking
        // the balance of j on node k.
        let mut expected_balances = vec![init_balance; num_nodes];
        let trial_duration = 60000;
        for trial in 0..num_trials {
            println!("TRIAL #{}", trial);
            let (i, j) = sample_two_nodes(num_nodes);
            let (k, r) = sample_two_nodes(num_nodes);
            let nonce =
                nodes[i].read().unwrap().get_account_nonce(&account_names[i]).unwrap_or_default()
                    + 1;
            let transaction = TransactionBody::send_money(
                nonce,
                account_names[i].as_str(),
                account_names[j].as_str(),
                1,
            )
            .sign(&*nodes[i].read().unwrap().signer());
            nodes[k].read().unwrap().add_transaction(transaction).unwrap();
            expected_balances[i] -= 1;
            expected_balances[j] += 1;

            wait(
                || {
                    expected_balances[j]
                        == nodes[r].read().unwrap().view_balance(&account_names[j]).unwrap()
                },
                1000,
                trial_duration,
            );
            thread::sleep(Duration::from_millis(500));
        }
    }

    #[test]
    fn test_4_10_multiple_nodes() {
        heavy_test(|| run_multiple_nodes(4, 10, "4_10", 3200));
    }

    #[test]
    fn test_7_10_multiple_nodes() {
        heavy_test(|| run_multiple_nodes(7, 10, "7_10", 3300));
    }
}
