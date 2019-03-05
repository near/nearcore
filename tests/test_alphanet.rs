use alphanet::testing_utils::Node;
use primitives::transaction::TransactionBody;
use alphanet::testing_utils::wait;
use alphanet::testing_utils::generate_test_chain_spec;

fn run_multiple_nodes(num_nodes: usize, num_trials: usize) {
    let init_balance = 1_000_000_000;
    let mut account_names = vec![];
    let mut node_names = vec![];
    for i in 0..num_nodes {
        account_names.push(format!("near.{}", i));
        node_names.push(format!("node_{}", i));
    }
    let chain_spec = generate_test_chain_spec(&account_names, init_balance);

    let mut nodes = vec![];
    let mut boot_nodes = vec![];
    // Launch nodes in a chain, such that X+1 node boots from X node.
    for i in 0..num_nodes {
        let node = Node::new(
            node_names[i].as_str(),
            account_names[i].as_str(),
            i as u32 + 1,
            Some(format!("127.0.0.1:{}", 3000 + i).as_str()),
            3030 + i as u16,
            boot_nodes,
            chain_spec.clone(),
        );
        boot_nodes = vec![node.node_info.clone()];
        node.start();
        nodes.push(node);
    }
    //        thread::sleep(Duration::from_secs(10));

    // Execute N trials. In each trial we submit a transaction to a random node i, that sends
    // 1 token to a random node j. Then we wait for the balance change to propagate by checking
    // the balance of j on node k.
    let mut expected_balances = vec![init_balance; num_nodes];
    let mut nonces = vec![1; num_nodes];
    let trial_duration = 10000;
    for trial in 0..num_trials {
        println!("TRIAL #{}", trial);
        let i = rand::random::<usize>() % num_nodes;
        // Should be a different node.
        let mut j = rand::random::<usize>() % (num_nodes - 1);
        if j >= i {
            j += 1;
        }
        for k in 0..num_nodes {
            nodes[k]
                .client
                .shard_client
                .pool
                .add_transaction(
                    TransactionBody::send_money(
                        nonces[i],
                        account_names[i].as_str(),
                        account_names[j].as_str(),
                        1,
                    )
                    .sign(nodes[i].signer()),
                )
                .unwrap();
        }
        nonces[i] += 1;
        expected_balances[i] -= 1;
        expected_balances[j] += 1;

        wait(
            || {
                let mut state_update = nodes[j].client.shard_client.get_state_update();
                let amt = nodes[j]
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
fn test_multiple_nodes() {
    run_multiple_nodes(4, 10);
}

/// This test should work after (#667) is fixed.
#[test]
#[ignore]
fn test_multiple_nodes_10() {
    run_multiple_nodes(10, 1);
}


