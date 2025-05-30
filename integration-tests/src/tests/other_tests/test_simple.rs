//! Simply starts and runs testnet for a while.
use crate::node::{Node, create_nodes, sample_two_nodes};
use crate::utils::test_helpers::{heavy_test, wait};
use near_async::time::Clock;
use near_o11y::testonly::init_integration_logger;
use near_primitives::transaction::SignedTransaction;
use std::time::Duration;

fn run_multiple_nodes(num_nodes: usize, num_trials: usize, test_prefix: &str) {
    init_integration_logger();

    let nodes = create_nodes(num_nodes, test_prefix);
    let nodes: Vec<_> = nodes.into_iter().map(|cfg| <dyn Node>::new_sharable(cfg)).collect();
    let account_names: Vec<_> =
        nodes.iter().map(|node| node.read().account_id().unwrap()).collect();

    for i in 0..num_nodes {
        nodes[i].write().start();
    }

    // waiting for nodes to be synced
    let started = Clock::real().now();
    loop {
        if started.elapsed() > Duration::from_secs(10) {
            panic!("nodes are not synced in 10s");
        }
        let all_synced =
            nodes.iter().all(|node| node.read().view_account(&account_names[0]).is_ok());
        if all_synced {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Execute N trials. In each trial we submit a transaction to a random node i, that sends
    // 1 token to a random node j. We send transaction to node Then we wait for the balance change to propagate by checking
    // the balance of j on node k.
    let trial_duration = 60_000;
    let amount_to_send = 100 * 10u128.pow(24);
    for trial in 0..num_trials {
        println!("TRIAL #{}", trial);
        let (i, j) = sample_two_nodes(num_nodes);
        let (k, r) = sample_two_nodes(num_nodes);
        let nonce_i = nodes[i].read().get_access_key_nonce_for_signer(&account_names[i]).unwrap();
        let account_j = nodes[k].read().view_account(&account_names[j]).unwrap();
        let transaction = SignedTransaction::send_money(
            nonce_i + 1,
            account_names[i].clone(),
            account_names[j].clone(),
            &*nodes[i].read().signer(),
            amount_to_send,
            nodes[k].read().user().get_best_block_hash().unwrap(),
        );
        nodes[k].read().add_transaction(transaction).unwrap();

        wait(
            || {
                account_j.amount
                    < nodes[r].read().view_balance(&account_names[j]).unwrap()
                        - amount_to_send * 9 / 10
            },
            100,
            trial_duration,
        );
    }
}

#[test]
fn ultra_slow_test_2_10_multiple_nodes() {
    heavy_test(|| run_multiple_nodes(2, 10, "2_10"));
}

#[test]
fn ultra_slow_test_4_10_multiple_nodes() {
    heavy_test(|| run_multiple_nodes(4, 10, "4_10"));
}

#[test]
fn ultra_slow_test_7_10_multiple_nodes() {
    heavy_test(|| run_multiple_nodes(7, 10, "7_10"));
}
