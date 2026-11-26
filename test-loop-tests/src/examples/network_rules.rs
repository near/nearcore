//! Network rules: what a node does with the requests it sends, switched on and off while the
//! test runs.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::network_rules::{Action, NetworkRequestsMatcher};
use near_async::time::Duration;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::stateless_validation::ChunkProductionKey;
use parking_lot::Mutex;
use std::sync::Arc;

fn approvals() -> NetworkRequestsMatcher {
    NetworkRequestsMatcher::new(|request| matches!(request, NetworkRequests::Approval { .. }))
}

/// A rule that counts the approvals reaching it: a rule sees a request after the rules added
/// before it had their say.
fn count_approvals(counter: &Arc<Mutex<u64>>) -> Action {
    let counter = counter.clone();
    Action::Observe(Box::new(move |_| *counter.lock() += 1))
}

/// Node 0 drops the approvals it sends from now on; the rule counts what it dropped, and once
/// it is lifted the approvals go out again.
#[test]
fn test_drop_messages_from_node() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();

    let dropped = env.node_network(0).drop_messages(approvals());
    let passed = Arc::new(Mutex::new(0));
    env.node_network(0).add_rule(approvals(), count_approvals(&passed));

    env.node_runner(1).run_for_number_of_blocks(5);
    assert!(dropped.matched() > 0, "node 0 sent no approval to drop");
    assert_eq!(*passed.lock(), 0, "a dropped approval went on to the next rule");

    dropped.lift();
    env.node_runner(1).run_for_number_of_blocks(5);
    assert!(*passed.lock() > 0, "node 0 sent no approval once the drop was lifted");
}

/// Node 0 sends its approvals a second late. A request that goes out later skips the rules on
/// its way, so the rule after the delay never sees one, and the chain goes on regardless.
#[test]
fn test_delay_messages_from_node() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();

    let delayed = env.node_network(0).add_rule(approvals(), Action::delay(Duration::seconds(1)));
    let passed = Arc::new(Mutex::new(0));
    env.node_network(0).add_rule(approvals(), count_approvals(&passed));

    env.node_runner(1).run_for_number_of_blocks(10);
    assert!(delayed.matched() > 0, "node 0 sent no approval to delay");
    assert_eq!(*passed.lock(), 0, "a delayed approval went through the rules again on its way out");
}

/// Node 0 delays its approvals, and an observer added after the delay still sees every one
/// of them: `observe_messages` reports what the node sent ahead of its rules, where a
/// rule added after the delay never sees a delayed request.
#[test]
fn test_observe_messages_from_node() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();

    let delayed = env.node_network(0).add_rule(approvals(), Action::delay(Duration::seconds(1)));
    let observed = Arc::new(Mutex::new(0));
    let observed_clone = observed.clone();
    env.node_network(0).observe_messages(approvals(), move |_| *observed_clone.lock() += 1);

    env.node_runner(1).run_for_number_of_blocks(10);
    assert!(delayed.matched() > 0, "node 0 sent no approval to delay");
    assert_eq!(*observed.lock(), delayed.matched(), "the observer missed a delayed approval");
}

/// Every node drops the parts of one shard's chunk at one height: the parts the producer
/// pushes, the ones owners forward and the requests and responses for them. The block
/// producer of that height never gets the chunk, so the block lacks it.
#[test]
fn test_chunk_parts_rule() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).num_shards(2).build();

    // A height whose block producer is not the shard's chunk producer, which would have the
    // chunk without any part reaching it.
    let start_height = env.node(0).head().height + 3;
    let (shard_id, shard_index, missing_height) = {
        let node = env.node(0);
        let epoch_manager = node.client().epoch_manager.clone();
        let epoch_id = node.head().epoch_id;
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_id = shard_layout.shard_ids().next().unwrap();
        let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
        let missing_height = (start_height..start_height + 10)
            .find(|&height| {
                let block_producer = epoch_manager.get_block_producer(&epoch_id, height).unwrap();
                let key = ChunkProductionKey { shard_id, epoch_id, height_created: height };
                let chunk_producer = epoch_manager.get_chunk_producer_info(&key).unwrap();
                &block_producer != chunk_producer.account_id()
            })
            .expect("the producers rotate within ten heights");
        (shard_id, shard_index, missing_height)
    };

    let rules: Vec<_> = (0..env.node_datas.len())
        .map(|index| {
            env.node_network(index).add_chunk_parts_rule(
                move |chunk| {
                    chunk.shard_id() == shard_id && chunk.height_created() == missing_height
                },
                Action::Drop,
            )
        })
        .collect();

    env.node_runner(0).run_until_head_height(missing_height + 2);
    assert!(rules.iter().any(|rule| rule.matched() > 0), "no part of the chunk was sent");
    let node = env.node(0);
    let mask = |height| {
        node.client().chain.get_block_by_height(height).unwrap().header().chunk_mask().to_vec()
    };
    let mut expected = vec![true, true];
    expected[shard_index] = false;
    assert_eq!(mask(missing_height), expected);
    assert_eq!(mask(missing_height + 1), vec![true, true]);
}
