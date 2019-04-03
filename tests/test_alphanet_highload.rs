use std::thread;
use std::time::Duration;

use network::proxy::benchmark::BenchmarkHandler;
use network::proxy::ProxyHandler;
use primitives::hash::CryptoHash;
use primitives::transaction::TransactionBody;
use serde_json::map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::RwLock;
use testlib::alphanet_utils::{create_nodes, sample_queryable_node, sample_two_nodes, wait, Node};
use testlib::test_locks::heavy_test;
use std::time::Instant;

/// Create and submit some transaction, returning its hash. Nonces are kept in a separate collection.
fn submit_transaction(
    nodes: &Vec<Arc<RwLock<dyn Node>>>,
    nonces: &mut HashMap<usize, u64>,
) -> CryptoHash {
    let (money_sender, money_receiver) = sample_two_nodes(nodes.len());
    let tx_sender = sample_queryable_node(nodes);
    // Get and increase nonce.
    let nonce = *nonces.entry(money_sender).and_modify(|e| *e += 1).or_insert(1);

    let money_sender = &nodes[money_sender];
    let money_receiver = &nodes[money_receiver];
    let tx_sender = &nodes[tx_sender];
    let money_to_send = 1;

    let transaction = TransactionBody::send_money(
        nonce,
        money_sender.read().unwrap().account_id().as_str(),
        money_receiver.read().unwrap().account_id().as_str(),
        money_to_send,
    )
    .sign(money_sender.read().unwrap().signer());
    let hash = transaction.get_hash();
    tx_sender.read().unwrap().add_transaction(transaction).unwrap();
    hash
}

/// Launch several nodes, then start submitting transactions with high frequency. Measure the
/// throughput by checking the published blocks. Does not differentiate failing and passing
/// transactions. Can be used to determine top tps.
fn run_multiple_nodes(
    num_nodes: usize,
    running_time: Duration,
    tps: usize,
    test_prefix: &str,
    test_port: u16,
) {
    // Add proxy handlers to the pipeline.
    let proxy_handlers = vec![];

    let (init_balance, account_names, mut nodes) =
        create_nodes(num_nodes, test_prefix, test_port, proxy_handlers);

    let nodes: Vec<_> = nodes.drain(..).map(Node::new).collect();
    for node in &nodes {
        node.write().unwrap().start();
    }

    // Transactions that were submitted but were not accepted yet into the block.
    let pending_transactions = Arc::new(RwLock::new(HashSet::new()));

    // Send transactions in a loop.
    let tx_sender_handle = {
        let transaction_delay = Duration::from_millis((Duration::from_secs(1).as_millis() / (tps as u128)) as u64);
        let end = Instant::now() + running_time;
        let pending_transactions = pending_transactions.clone();
        let nodes = nodes.clone();
        thread::spawn(move || {
            let mut nonces = HashMap::new();

            while Instant::now() <= end {
                let tx_hash = submit_transaction(&nodes, &mut nonces);
                pending_transactions.write().unwrap().insert(tx_hash);
                thread::sleep(transaction_delay);
            }
        })
    };

    // Monitor when the most recent block changes and update the pending transactions.
    // Also record finalized transactions.
//    let finalized_transactions = Arc::new(RwLock::new(HashSet::new()));
    let tx_monitor_handle = {
        let end = Instant::now() + running_time;
        thread::spawn(move || {

        })
    };

    tx_sender_handle.join().unwrap();
    tx_monitor_handle.join().unwrap();
}

#[test]
fn test_4_10_multiple_nodes() {
    heavy_test(|| run_multiple_nodes(4, Duration::from_secs(60), 1000, "4_10", 3300));
}
