#[cfg(feature = "old_tests")]
#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use network::proxy::predicate::FnProxyHandler;
    use network::proxy::ProxyHandler;
    use primitives::transaction::TransactionBody;
    use testlib::node::{
        create_nodes, sample_two_nodes, Node, TEST_BLOCK_FETCH_LIMIT, TEST_BLOCK_MAX_SIZE,
    };
    use testlib::test_helpers::wait;

    fn run_multiple_nodes(num_nodes: usize, num_trials: usize, test_prefix: &str, test_port: u16) {
        // Add proxy handlers to the pipeline.

        // Custom handler example
        let fn_proxy_handler = Arc::new(FnProxyHandler::new(|package| {
            // Logic here...

            // Return None to dismiss this channel,
            // or Some(package) for passing to the next handler. Note that returned package don't need to be the
            // same as the received package.
            Some(package)
        }));

        let proxy_handlers: Vec<Arc<ProxyHandler>> = vec![fn_proxy_handler];

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

        thread::sleep(Duration::from_millis(1000));

        // Execute N trials. In each trial we submit a transaction to a random node i, that sends
        // 1 token to a random node j. We send transaction to node Then we wait for the balance change to propagate by checking
        // the balance of j on node k.
        let mut expected_balances = vec![init_balance; num_nodes];
        let trial_duration = 10000;
        for _ in 0..num_trials {
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

    /// Similar to `test_alphanet::test_4_10_multiple_nodes` to show custom proxy handlers usage.
    #[test]
    fn test_custom_handler() {
        run_multiple_nodes(3, 1, "custom_handler", 3200);
    }
}
