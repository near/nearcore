/// This module does end-to-end testing of the testnet by spinning up multiple nodes and
/// exercising them in different scenarios.
/// Note: tests get executed in parallel, so use different ports / names.

use std::net::SocketAddr;
use std::panic;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use client::{ChainConsensusBlockBody, Client, BlockProductionResult};
use configs::chain_spec::{ChainSpec, read_or_default_chain_spec};
use configs::ClientConfig;
use configs::network::get_peer_id_from_seed;
use configs::NetworkConfig;
use configs::RPCConfig;
use primitives::block_traits::SignedBlock;
use primitives::chain::ChainPayload;
use primitives::network::PeerInfo;
use primitives::signer::write_key_file;
use primitives::test_utils::get_key_pair_from_seed;

use alphanet::testing_utils::{Node, configure_chain_spec, wait, check_result};

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

    let payload = ChainConsensusBlockBody { payload: ChainPayload { transactions: vec![], receipts: vec![] }, beacon_block_index: 1 };
    let (beacon_block, shard_block) = match alice.client.try_produce_block(payload) {
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
