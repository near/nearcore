use testlib::alphanet_utils::generate_test_chain_spec;
use testlib::alphanet_utils::wait;
use testlib::alphanet_utils::Node;
use primitives::transaction::TransactionBody;

fn sample_two_nodes(num_nodes: usize) -> (usize, usize) {
    let i = rand::random::<usize>() % num_nodes;
    // Should be a different node.
    let mut j = rand::random::<usize>() % (num_nodes - 1);
    if j >= i {
        j += 1;
    }
    (i, j)
}

fn run_multiple_nodes(num_nodes: usize, num_trials: usize, test_prefix: &str, test_port: u16) {
    let init_balance = 1_000_000_000;
    let mut account_names = vec![];
    for i in 0..num_nodes {
        account_names.push(format!("near.{}", i));
    }
    let chain_spec = generate_test_chain_spec(&account_names, init_balance);

    let mut nodes = vec![];
    let mut boot_nodes = vec![];
    // Launch nodes in a chain, such that X+1 node boots from X node.
    for i in 0..num_nodes {
        let node = Node::for_test(
            test_prefix,
            test_port,
            account_names[i].as_str(),
            i as u16 + 1,
            boot_nodes,
            chain_spec.clone()
        );
        boot_nodes = vec![node.node_addr()];
        node.start();
        nodes.push(node);
    }
    //        thread::sleep(Duration::from_secs(10));

    // Execute N trials. In each trial we submit a transaction to a random node i, that sends
    // 1 token to a random node j. We send transaction to node Then we wait for the balance change to propagate by checking
    // the balance of j on node k.
    let mut expected_balances = vec![init_balance; num_nodes];
    let trial_duration = 10000;
    for trial in 0..num_trials {
        println!("TRIAL #{}", trial);
        let (i, j) = sample_two_nodes(num_nodes);
        let (k, r) = sample_two_nodes(num_nodes);
        let nonce = nodes[i]
                .client
                .shard_client
                .get_account_nonce(account_names[i].clone())
                .unwrap_or_default()
                + 1;
        nodes[k]
            .client
            .shard_client
            .pool
            .add_transaction(
                TransactionBody::send_money(
                    nonce,
                    account_names[i].as_str(),
                    account_names[j].as_str(),
                    1,
                )
                .sign(nodes[i].signer()),
            )
            .unwrap();
        expected_balances[i] -= 1;
        expected_balances[j] += 1;

        wait(
            || {
                let mut state_update = nodes[r].client.shard_client.get_state_update();
                let amt = nodes[r]
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

// DISCLAIMER. These tests are very heavy and somehow manage to interfere with each other.
// If you add multiple tests and they start failing consider splitting it into several *.rs files
// to ensure they are not run in parallel.


#[test]
fn test_4_10_multiple_nodes() {
    run_multiple_nodes(4, 10, "4_10", 3200);
}

// TODO(#718)
//#[test]
//fn test_7_10_multiple_nodes() {
//    run_multiple_nodes(7, 10, "7_10", 3300);
//}
