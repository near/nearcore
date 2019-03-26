use std::thread;
use std::time::Duration;

use primitives::transaction::TransactionBody;
use testlib::alphanet_utils::create_nodes;
use testlib::alphanet_utils::sample_two_nodes;
use testlib::alphanet_utils::wait;
use testlib::alphanet_utils::Node;

fn run_multiple_nodes(num_nodes: usize, num_trials: usize, test_prefix: &str, test_port: u16) {
    let (init_balance, account_names, mut nodes) = create_nodes(num_nodes, test_prefix, test_port);

    let mut nodes: Vec<Box<Node>> = nodes.drain(..).map(|cfg| Node::new(cfg)).collect();
    for i in 0..num_nodes {
        nodes[i].start();
    }

    // Execute N trials. In each trial we submit a transaction to a random node i, that sends
    // 1 token to a random node j. We send transaction to node Then we wait for the balance change to propagate by checking
    // the balance of j on node k.
    let mut expected_balances = vec![init_balance; num_nodes];
    let trial_duration = 10000;
    for trial in 0..num_trials {
        println!("TRIAL #{}", trial);
        let (i, j) = sample_two_nodes(num_nodes);
        let (k, r) = sample_two_nodes(num_nodes);
        let nonce = nodes[i].get_account_nonce(&account_names[i]).unwrap_or_default() + 1;
        let transaction = TransactionBody::send_money(
            nonce,
            account_names[i].as_str(),
            account_names[j].as_str(),
            1,
        )
        .sign(nodes[i].signer());
        nodes[k].add_transaction(transaction).unwrap();
        expected_balances[i] -= 1;
        expected_balances[j] += 1;

        wait(
            || expected_balances[j] == nodes[r].view_balance(&account_names[j]).unwrap(),
            1000,
            trial_duration,
        );
        thread::sleep(Duration::from_millis(500));
    }
}

#[test]
fn test_4_10_multiple_nodes() {
    run_multiple_nodes(4, 10, "4_10", 3200);
}

// TODO(#718)
//#[test]
//fn test_7_10_multiple_nodes() {
//    run_multiple_nodes(7, 10, "7_10", 3300);
//}
