/// This module does end-to-end testing of the testnet by spinning up multiple nodes and
/// exercising them in different scenarios.
/// Note: tests get executed in parallel, so use different ports / names.

use std::panic;
use std::process::Command;

use alphanet::testing_utils::{check_result, configure_chain_spec, Node, wait};
use client::BlockProductionResult;
use primitives::chain::ChainPayload;
use primitives::block_traits::SignedBlock;

#[test]
#[ignore]
fn test_two_nodes() {
    let chain_spec = configure_chain_spec();
    // Create boot node.
    let alice = Node::new("t1_alice", "alice.near", 1, "127.0.0.1:3000", 3030, vec![], chain_spec.clone());
    // Create secondary node that boots from the alice node.
    let bob = Node::new("t1_bob", "bob.near", 2, "127.0.0.1:3001", 3031, vec![alice.node_info.clone()], chain_spec);

    // Start both nodes.
    alice.start();
    bob.start();

    // Create an account on alice node.
    Command::new("pynear")
        .arg("create_account")
        .arg("jason")
        .arg("1")
        .arg("-u")
        .arg("http://127.0.0.1:3030/")
        .output()
        .expect("create_account command failed to process");

    // Wait until this account is present on the bob.near node.
    let view_account = || -> bool {
        let res = Command::new("pynear")
            .arg("view_account")
            .arg("-a")
            .arg("jason")
            .arg("-u")
            .arg("http://127.0.0.1:3031/")
            .output()
            .expect("view_account command failed to process");
        check_result(res).is_ok()
    };
    wait(view_account, 500, 60000);
}

#[test]
#[ignore]
fn test_two_nodes_sync() {
    let chain_spec = configure_chain_spec();
    let alice = Node::new("t2_alice", "alice.near", 1, "127.0.0.1:3002", 3032, vec![], chain_spec.clone());
    let bob = Node::new("t2_bob", "bob.near", 2, "127.0.0.1:3003", 3033, vec![alice.node_info.clone()], chain_spec);

    let payload = ChainPayload { transactions: vec![], receipts: vec![] };
    let (beacon_block, shard_block) = match alice.client.try_produce_block(1, payload) {
        BlockProductionResult::Success(beacon_block, shard_block) => (beacon_block, shard_block),
        _ => panic!("Should produce block"),
    };
    alice.client.try_import_blocks(beacon_block, shard_block);

    alice.start();
    bob.start();

    wait(|| {
        bob.client.shard_client.chain.best_block().index() == 1
    }, 500, 10000);
}
