use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use primitives::transaction::TransactionBody;
use testlib::alphanet_utils::{create_nodes, NodeType, ProcessNode, Node};
use testlib::alphanet_utils::NodeConfig;
use testlib::alphanet_utils::sample_two_nodes;
use testlib::alphanet_utils::wait;

fn warmup() {
    Command::new("cargo").args(&["build"]).spawn().expect("warmup failed").wait();
}

// DISCLAIMER. These tests are very heavy and somehow manage to interfere with each other.
// If you add multiple tests and they start failing consider splitting it into several *.rs files
// to ensure they are not run in parallel.

fn test_command_line(num_nodes: usize, num_trials: usize, test_prefix: &str, test_port: u16) {
    warmup();
    // Start all nodes, crash node#2, proceed, restart node #2
    let node_to_crash = 2;
    let (init_balance, account_names, mut nodes) = create_nodes(num_nodes, test_prefix, test_port);
    nodes[node_to_crash].node_type = NodeType::ProcessNode;

    let mut nodes: Vec<Box<Node>> = nodes.drain(..).map(|cfg| Node::new(cfg)).collect();

    for i in 0..num_nodes {
        nodes[i].start();
    }

    let mut expected_balances = vec![init_balance; num_nodes];
    let mut nonces = vec![1; num_nodes];
    let trial_duration = 10000;
    for trial in 0..num_trials {
        println!("TRIAL #{}", trial);
        if trial == num_trials / 3 {
            println!("Killing node {}", node_to_crash);
            nodes[node_to_crash].as_process_mut().kill();
            thread::sleep(Duration::from_secs(2));
        }
        if trial == num_trials * 2 / 3 {
            println!("Restarting node {}", node_to_crash);
            nodes[node_to_crash].start();
        }
        let (i, j) = sample_two_nodes(num_nodes);
        let (mut k, mut t) = sample_two_nodes(num_nodes);
        while k == node_to_crash || t == node_to_crash {
            let (x, y) = sample_two_nodes(num_nodes);
            k = x;
            t = y;
        }
        nodes[k]
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
        nonces[i] += 1;
        expected_balances[i] -= 1;
        expected_balances[j] += 1;

        wait(
            || {
                let amt = nodes[t]
                    .view_balance(&account_names[j])
                    .unwrap();
                expected_balances[j] == amt
            },
            1000,
            trial_duration,
        );
    }
}


#[test]
fn test_4_20_command_line() {
    test_command_line(4, 20, "4_20", 3300);
}
