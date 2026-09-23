use crate::setup::builder::TestLoopBuilder;
use near_async::messaging::Handler;
use near_client::client_actor::ClientActor;
use near_client_primitives::debug::{DebugStatus, DebugStatusResponse, EpochInfoView};
use near_o11y::testonly::init_test_logger;

const EPOCH_LENGTH: u64 = 5;

fn get_recent_epochs(client_actor: &mut ClientActor, request: DebugStatus) -> Vec<EpochInfoView> {
    match client_actor.handle(request).unwrap() {
        DebugStatusResponse::EpochInfo(epochs) => epochs,
        other => panic!("expected EpochInfo response, got {other:?}"),
    }
}

/// The `EpochInfoLight` debug request must return the same recent-epoch list as
/// `EpochInfo`, only with the expensive per-validator `validator_info` omitted.
#[test]
fn test_debug_epoch_info_light_omits_validator_info() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().epoch_length(EPOCH_LENGTH).build();
    // Advance past the first epoch so the recent-epoch list contains at least one
    // finalized epoch whose `validator_info` is populated in the full response.
    env.node_runner(0).run_until_new_epoch();
    env.node_runner(0).run_until_new_epoch();

    let handle = env.node_datas[0].client_sender.actor_handle();
    let client_actor = env.test_loop.data.get_mut(&handle);
    let full = get_recent_epochs(client_actor, DebugStatus::EpochInfo(None));
    let lite = get_recent_epochs(client_actor, DebugStatus::EpochInfoLight(None));

    assert_eq!(full.len(), lite.len(), "lite response must list the same epochs as the full one");

    for (full_epoch, lite_epoch) in full.iter().zip(lite.iter()) {
        // Everything except validator_info must be identical between the two.
        assert_eq!(full_epoch.epoch_id, lite_epoch.epoch_id);
        assert_eq!(full_epoch.epoch_height, lite_epoch.epoch_height);
        assert_eq!(full_epoch.height, lite_epoch.height);
        assert_eq!(full_epoch.protocol_version, lite_epoch.protocol_version);
        assert_eq!(full_epoch.first_block, lite_epoch.first_block);
        assert_eq!(full_epoch.block_producers.len(), lite_epoch.block_producers.len());
        assert_eq!(full_epoch.chunk_producers, lite_epoch.chunk_producers);
        assert_eq!(full_epoch.chunk_validators, lite_epoch.chunk_validators);
        assert_eq!(full_epoch.shards_size_and_parts, lite_epoch.shards_size_and_parts);

        // The lite response never carries validator_info.
        assert!(
            lite_epoch.validator_info.is_none(),
            "lite epoch {} must not include validator_info",
            lite_epoch.epoch_id
        );
    }

    // Sanity check that the full response actually does compute validator_info for a
    // finalized epoch, otherwise the assertions above would pass vacuously. The "next
    // epoch" entry (the first one) legitimately has no validator_info, so look past it.
    let full_has_validator_info = full.iter().any(|epoch| {
        epoch.validator_info.as_ref().is_some_and(|info| !info.current_validators.is_empty())
    });
    assert!(full_has_validator_info, "full response should populate validator_info");
}
