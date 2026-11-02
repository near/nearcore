use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use crate::utils::gaps::producer_outage;
use crate::utils::node::TimedOut;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;

#[test]
fn test_restart_node() {
    init_test_logger();

    // 4 validators with equal stake means that with one unavailable node
    // the chain can still make progress
    let epoch_length = 4;
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .epoch_length(epoch_length)
        .gc_num_epochs_to_keep(20)
        .build();

    let stable_node_idx = 1;
    let restart_node_data = &env.node_datas[0];
    let restart_account = restart_node_data.account_id.clone();
    let restart_identifier = restart_node_data.identifier.clone();

    let kill_height = 2 * epoch_length;
    env.runner_for_account(&restart_account).run_until_head_height(kill_height);

    let killed_node_state = env.kill_node(&restart_identifier);

    let restart_height = kill_height + 2 * epoch_length;
    env.node_runner(stable_node_idx).run_until_head_height(restart_height);

    let new_node_identifier = format!("{}-restart", restart_identifier);
    env.restart_node(&new_node_identifier, killed_node_state);

    assert_eq!(env.node_for_account(&restart_account).head().height, kill_height);

    // Give a few blocks for the restarted node to catch up
    env.node_runner(stable_node_idx).run_for_number_of_blocks(5);

    assert_eq!(env.node_for_account(&restart_account).head().height, env.node(1).head().height);
}

/// A killed node keeps its entry but no longer counts as alive, waiting on it times out
/// without taking the test down, and the restarted node is a new, live entry.
#[test]
fn test_killed_node_is_not_alive() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();
    env.node_runner(0).run_for_number_of_blocks(3);

    let identifier = env.node_datas[0].identifier.clone();
    let killed_state = env.kill_node(&identifier);
    assert!(!env.is_alive(0));
    assert_eq!(env.live_node_indices(), vec![1, 2, 3]);

    // The dead node processes nothing, so its head stays put while the others go on; a
    // deadline that passes is reported instead of panicking, and the loop stays usable.
    let height = env.node(0).head().height;
    let timeout = Duration::seconds(5);
    let waited = env.node_runner(0).try_run_until(|node| node.head().height > height, timeout);
    assert_eq!(waited, Err(TimedOut { waited: timeout }));
    assert!(env.node(1).head().height > height, "the live nodes did not go on");

    let restarted = env.restart_node(&format!("{identifier}-restart"), killed_state);
    assert!(env.is_alive(restarted));
    assert_eq!(env.live_node_indices(), vec![1, 2, 3, restarted]);
    let head = env.node(1).head().height;
    env.node_runner(restarted).run_until_head_height(head);
}

/// `producer_outage` accounts for every gap a killed producer leaves: across the kill,
/// the restart and the catch-up, each height without a block is one of its skipped slots
/// and each block lacking a chunk lacks one it owed, so a test that allows exactly the
/// outage still catches any other gap.
#[test]
fn test_producer_outage_accounts_for_the_gaps() {
    init_test_logger();
    let mut env =
        TestLoopBuilder::new().validators(4, 0).epoch_length(10).gc_num_epochs_to_keep(20).build();
    let identifier = env.node_datas[0].identifier.clone();
    let account_id = env.node_datas[0].account_id.clone();

    let kill_height = 8;
    env.node_runner(0).run_until_head_height(kill_height);
    let killed_state = env.kill_node(&identifier);
    let restart_height = kill_height + 16;
    env.node_runner(1).run_until_head_height(restart_height);
    env.restart_node(&format!("{identifier}-restart"), killed_state);
    env.node_runner(1).run_for_number_of_blocks(12);

    let node = env.node(1);
    let outage = producer_outage(&node, &account_id, kill_height + 1, Some(restart_height));
    assert!(
        !outage.skipped_heights.is_empty(),
        "sixteen blocks down with four producers skips at least one slot"
    );
    let chain = &node.client().chain;
    let epoch_manager = node.client().epoch_manager.as_ref();
    for height in kill_height + 1..=node.head().height {
        let Ok(block) = chain.get_block_by_height(height) else {
            assert!(
                outage.skipped_heights.contains(&height),
                "height {height} was skipped for a reason other than the outage"
            );
            continue;
        };
        let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();
        for shard_id in shard_layout.shard_ids() {
            let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
            assert!(
                block.chunks()[shard_index].is_new_chunk(height)
                    || outage.missing_chunks.contains(&(shard_id, height)),
                "the chunk of shard {shard_id} at height {height} is missing for a reason \
                 other than the outage"
            );
        }
    }
}

#[test]
fn test_add_node() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(4, 0).gc_num_epochs_to_keep(20).build();

    // Let the chain progress for a few blocks
    env.node_runner(0).run_for_number_of_blocks(10);

    // Add a new non-validator tracking node
    let identifier = "new_node";
    let new_account_id = create_account_id(identifier);
    let new_node_state = env.node_state_builder().account_id(&new_account_id).build();
    env.add_node(identifier, new_node_state);

    // Let the network run so the new node can catch up
    env.node_runner(0).run_for_number_of_blocks(5);

    // Verify the new node synced to the same height as a validator
    assert_eq!(env.node_for_account(&new_account_id).head().height, env.node(0).head().height);
}
