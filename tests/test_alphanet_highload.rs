use std::thread;
use std::time::{Duration, Instant};

use network::proxy::benchmark::BenchmarkHandler;
use network::proxy::ProxyHandler;
use primitives::hash::CryptoHash;
use primitives::transaction::TransactionBody;
use std::collections::{HashSet, HashMap};
use std::iter::Inspect;
use std::sync::{Arc, RwLock};
use testlib::alphanet_utils::{
    create_nodes, sample_queryable_node, sample_two_nodes, wait, Node, TEST_BLOCK_FETCH_LIMIT,
};
use testlib::test_locks::heavy_test;
use node_http::types::GetBlocksByIndexRequest;

/// Creates and sends a random transaction.
/// Args:
/// `nodes`: node to submit to;
/// `nonces`: tracker of the nonces for the corresponding accounts.
fn send_transaction(nodes: &Vec<Arc<RwLock<dyn Node>>>, nonces: &mut Vec<u64>) {
    let (money_sender, money_receiver) = sample_two_nodes(nodes.len());
    let tx_receiver = sample_queryable_node(nodes);
    // Update nonces.
    nonces[money_sender] += 1;
    let nonce = nonces[money_sender];

    let sender_acc = nodes[money_sender].read().unwrap().account_id().clone();
    let receiver_acc = nodes[money_receiver].read().unwrap().account_id().clone();
    let transaction = TransactionBody::send_money(nonce, sender_acc.as_str(), receiver_acc.as_str(), 1)
        .sign(nodes[money_sender].read().unwrap().signer());
    nodes[tx_receiver].read().unwrap().add_transaction(transaction).unwrap();
}

/// Creates a network of nodes and submits a large number of transactions to them.
/// Args:
/// * `num_nodes`: number of nodes to create;
/// * `tps`: transactions-per-second rate with which we submit transactions at even intervals;
/// * `timeout`: how long this test should run.
fn run_multiple_nodes(
    num_nodes: usize,
    tps: usize,
    timeout: Duration,
    test_prefix: &str,
    test_port: u16,
) {
    // Add proxy handlers to the pipeline.
    let proxy_handlers: Vec<Arc<ProxyHandler>> = vec![Arc::new(BenchmarkHandler::new())];

    let (init_balance, account_names, mut nodes) =
        create_nodes(num_nodes, test_prefix, test_port, TEST_BLOCK_FETCH_LIMIT, proxy_handlers);

    let nodes: Vec<Arc<RwLock<dyn Node>>> = nodes.drain(..).map(|cfg| Node::new(cfg)).collect();
    for i in 0..num_nodes {
        nodes[i].write().unwrap().start();
    }

    // Create thread that submits transactions with high tps.
    let transaction_handler = {
        // Delay between transactions.
        let tx_delay =
            Duration::from_nanos((Duration::from_secs(1).as_nanos() as u64) / (tps as u64));
        let timeout = Instant::now() + timeout;
        let nodes = nodes.to_vec();
        thread::spawn(move || {
            let mut nonces = vec![0; nodes.len()];
            while Instant::now() < timeout {
                send_transaction(&nodes, &mut nonces);
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
                let new_ind = node.read().unwrap().user().get_best_block_index();
                if new_ind > prev_ind {
                    let blocks = node.read().unwrap().user().get_shard_blocks_by_index(GetBlocksByIndexRequest {
                        start: Some(prev_ind + 1),
                        limit: Some(new_ind)
                    }).unwrap();
                    for b in &blocks.blocks {
                        observed_transactions.write().unwrap().push((
                            b.body.transactions.len(),
                            Instant::now()));
                    }
                    prev_ind = new_ind;
                }
                thread::sleep(check_delay);
            }
        })
    };

    transaction_handler.join().unwrap();
    observer_handler.join().unwrap();
}

#[test]
fn test_highload() {
    heavy_test(|| run_multiple_nodes(4, 1, Duration::from_secs(10), "4_10", 3300));
}
