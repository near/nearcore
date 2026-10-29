//! Task delays: how long a node's background tasks take, switched on and off while the test
//! runs. See `src/setup/task_delays.rs`. The example slows one of four equal validators: the
//! other three hold enough stake for the chain to go on without it, so the slowed node falls
//! behind the chain and catches up once the delay is lifted.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::task_delays::task_names;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;

/// The task that applies a block's chunks on the slowed node: the client's before spice, the
/// chunk executor's under it.
fn apply_chunks_task() -> &'static str {
    if cfg!(feature = "protocol_feature_spice") {
        task_names::SPICE_APPLY_CHUNKS
    } else {
        task_names::APPLY_CHUNKS_NORMAL
    }
}

/// Node 0 takes three seconds to apply each block's chunks, longer than a block takes, so its
/// head falls behind the chain; once the delay is lifted it catches up.
#[test]
fn test_delay_task_on_node() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();

    let slowed = env.delay_task(0, apply_chunks_task(), Duration::seconds(3));
    env.node_runner(1).run_for_number_of_blocks(5);
    assert!(slowed.matched() > 0, "node 0 applied no chunks while slowed");
    let (slow_head, head) = (env.node(0).head().height, env.node(1).head().height);
    assert!(slow_head < head, "node 0 kept up at height {slow_head} although it was slowed");

    let applied = slowed.matched();
    slowed.lift();
    env.node_runner(0).run_until_head_height(head);
    assert_eq!(slowed.matched(), applied, "a lifted delay applied again");
}
