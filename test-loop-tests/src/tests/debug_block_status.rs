use crate::setup::builder::TestLoopBuilder;
use near_async::messaging::Handler;
use near_client_primitives::debug::{
    DebugBlockStatusQuery, DebugBlocksStartingMode, DebugStatus, DebugStatusResponse,
};
use near_o11y::testonly::init_test_logger;

#[test]
fn test_debug_block_status_clamps_starting_height_to_header_head() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().epoch_length(5).build();
    env.node_runner(0).run_until_new_epoch();
    let header_head = env.node(0).client().chain.header_head().unwrap().height;

    let handle = env.node_datas[0].client_sender.actor_handle();
    let client_actor = env.test_loop.data.get_mut(&handle);

    // Boundary values of the u64 range, not just a height one past the head.
    // Genesis height reports nothing, since the range below it is empty.
    for (starting_height, expected_heights) in [
        (header_head + 1, vec![header_head]),
        (1 << 63, vec![header_head]),
        (u64::MAX, vec![header_head]),
        (0, vec![]),
    ] {
        let query = DebugBlockStatusQuery {
            starting_height: Some(starting_height),
            mode: DebugBlocksStartingMode::All,
            num_blocks: 1,
        };
        let DebugStatusResponse::BlockStatus(data) =
            client_actor.handle(DebugStatus::BlockStatus(query)).unwrap()
        else {
            panic!("expected BlockStatus response");
        };
        let heights = data
            .blocks
            .iter()
            .map(|block| block.block_height)
            .chain(data.missed_heights.iter().map(|missed| missed.block_height))
            .collect::<Vec<_>>();
        assert_eq!(heights, expected_heights, "starting_height {starting_height}");
    }
}
