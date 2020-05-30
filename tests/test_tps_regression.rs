//! Measures the input and the output transactions-per-seconds, compares it with the expected tps,
//! and verifies that the output tps is not much different from the input tps (makes sure there is
//! no choking on transactions). The input tps -- is how fast the nodes can be accepting
//! transactions. The output tps -- is how fast the nodes propagate transactions into the blocks.
#[cfg(any(feature = "expensive_tests", feature = "regression_tests"))]
#[cfg(test)]
mod test {
    use std::io::stdout;
    use std::io::Write;
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::{Duration, Instant};

    use near_primitives::transaction::SignedTransaction;
    use testlib::node::{create_nodes, sample_queryable_node, sample_two_nodes, Node};
    use testlib::test_helpers::heavy_test;

    /// Creates and sends a random transaction.
    /// Args:
    /// `nodes`: node to submit to;
    /// `nonces`: tracker of the nonces for the corresponding accounts.
    /// `submitted_transactions`: (number of transactions, when these transactions were submitted).
    fn send_transaction(
        nodes: Vec<Arc<RwLock<dyn Node>>>,
        nonces: Arc<RwLock<Vec<u64>>>,
        submitted_transactions: Arc<RwLock<Vec<(u64, Instant)>>>,
    ) {
        let (money_sender, money_receiver) = sample_two_nodes(nodes.len());
        let tx_receiver = sample_queryable_node(&nodes);
        // Update nonces.
        let mut nonces = nonces.write().unwrap();
        nonces[money_sender] += 1;
        let nonce = nonces[money_sender];

        let sender_acc = nodes[money_sender].read().unwrap().account_id().unwrap();
        let receiver_acc = nodes[money_receiver].read().unwrap().account_id().unwrap();
        let transaction = SignedTransaction::send_money(
            nonce,
            sender_acc,
            receiver_acc,
            &*nodes[money_sender].read().unwrap().signer(),
            1,
            nodes[money_sender].read().unwrap().user().get_best_block_hash().unwrap(),
        );
        nodes[tx_receiver].read().unwrap().add_transaction(transaction).unwrap();
        submitted_transactions.write().unwrap().push((1, Instant::now()));
    }

    /// Iterates over the given records of transactions, bucketizes them, and computes the tps.
    /// Can be used for debugging this test.
    fn bucketize_tps(
        recorded_transactions: &Arc<RwLock<Vec<(u64, Instant)>>>,
        bucket_size: Duration,
    ) -> Vec<u64> {
        let mut bucket_start = recorded_transactions.read().unwrap()[0].1;
        let mut buckets = vec![0u64];
        for b in &*recorded_transactions.read().unwrap() {
            if bucket_start + bucket_size >= b.1 {
                *buckets.last_mut().unwrap() += b.0;
            } else {
                *buckets.last_mut().unwrap() /= bucket_size.as_secs() as u64;
                buckets.push(b.0);
                bucket_start += Duration::from_nanos(
                    ((b.1 - bucket_start).as_nanos() as u64) / (bucket_size.as_nanos() as u64)
                        * (bucket_size.as_nanos() as u64),
                );
            }
        }
        // Pop the last bucket because it might be incomplete.
        buckets
    }

    /// Creates a network of nodes and submits a large number of transactions to them.
    /// Args:
    /// * `num_nodes`: number of nodes to create;
    /// * `tps`: transactions-per-second rate with which we submit transactions at even intervals;
    /// * `target_tps`: the target output transactions-per-seconds of the network;
    /// * `timeout`: how long this test should run.
    fn run_multiple_nodes(
        num_nodes: usize,
        tps: usize,
        target_tps: usize,
        timeout: Duration,
        test_prefix: &str,
    ) {
        let nodes = create_nodes(num_nodes, test_prefix);

        let nodes: Vec<Arc<RwLock<dyn Node>>> =
            nodes.into_iter().map(|cfg| Node::new_sharable(cfg)).collect();
        for i in 0..num_nodes {
            nodes[i].write().unwrap().start();
        }

        // Collection that stores #num of transactions -> when these transaction were submitted.
        let submitted_transactions = Arc::new(RwLock::new(vec![]));

        // Create thread that submits transactions with high tps.
        let transaction_handler = {
            // Delay between transactions.
            let tx_delay =
                Duration::from_nanos((Duration::from_secs(1).as_nanos() as u64) / (tps as u64));
            let timeout = Instant::now() + timeout;
            let nodes = nodes.to_vec();
            let submitted_transactions = submitted_transactions.clone();

            thread::spawn(move || {
                let nonces = vec![0u64; nodes.len()];
                let nonces = Arc::new(RwLock::new(nonces));
                while Instant::now() < timeout {
                    {
                        let nodes = nodes.to_vec();
                        let nonces = nonces.clone();
                        let submitted_transactions = submitted_transactions.clone();
                        thread::spawn(move || {
                            send_transaction(nodes, nonces, submitted_transactions)
                        });
                    }
                    thread::sleep(tx_delay);
                }
            })
        };

        // Delay between checking the nodes.
        let check_delay = Duration::from_millis(100);
        // Collection that stores #num of transactions in a block -> when this block was observed.
        let observed_transactions = Arc::new(RwLock::new(vec![]));

        // Create thread that observes new blocks and counts new transactions in them.
        let observer_handler = {
            let timeout = Instant::now() + timeout;
            let observed_transactions = observed_transactions.clone();
            thread::spawn(move || {
                let mut prev_ind = 0;
                while Instant::now() < timeout {
                    // Get random node.
                    let node = &nodes[sample_queryable_node(&nodes)];
                    if let Some(new_ind) = node.read().unwrap().user().get_best_height() {
                        if new_ind > prev_ind {
                            let blocks = ((prev_ind + 1)..=new_ind)
                                .map(|idx| node.read().unwrap().user().get_block(idx).unwrap())
                                .collect::<Vec<_>>();
                            for b in &blocks {
                                let _gas_used = b.chunks.iter().fold(0, |acc, chunk| {
                                    if chunk.height_included == b.header.height {
                                        acc + chunk.gas_used
                                    } else {
                                        acc
                                    }
                                });
                                observed_transactions.write().unwrap().push((1, Instant::now()));
                            }
                            prev_ind = new_ind;
                        }
                    }
                    thread::sleep(check_delay);
                }
            })
        };
        transaction_handler.join().unwrap();
        observer_handler.join().unwrap();

        let bucket_size = Duration::from_secs(10);

        let bucketed_submitted_xacts = bucketize_tps(&submitted_transactions, bucket_size);
        let mut bucketed_observed_xacts = bucketize_tps(&observed_transactions, bucket_size);

        let _ = stdout().write(
            format!("Submitted transactions tps: {:?}; ", bucketed_submitted_xacts).as_bytes(),
        );
        let _ = stdout()
            .write(format!("Observed transactions tps: {:?}", bucketed_observed_xacts).as_bytes());
        let _ = stdout().flush();

        // Test that the network does not choke. The choke can be observed when the number of submitted
        // transactions is not approx. the same the number of observed.

        let submitted_num: f64 =
            submitted_transactions.read().unwrap().iter().map(|(n, _)| *n as f64).sum();
        let observed_num: f64 =
            observed_transactions.read().unwrap().iter().map(|(n, _)| *n as f64).sum();
        // The difference is within 20%.
        assert!((submitted_num - observed_num).abs() < f64::max(submitted_num, observed_num) * 0.2);

        // Also verify that the median tps is within 20% of the target. We use median to discard
        // anomalies that happens when nodes start and stop.
        bucketed_observed_xacts.sort();
        let median = bucketed_observed_xacts[bucketed_observed_xacts.len() / 2];
        assert!((target_tps as f64) * 0.8 < (median as f64));
    }

    #[test]
    fn test_highload() {
        // Run 4 nodes with 20 input tps and check the output tps to be 20.
        heavy_test(|| run_multiple_nodes(4, 20, 20, Duration::from_secs(120), "4_20"));
    }
}
