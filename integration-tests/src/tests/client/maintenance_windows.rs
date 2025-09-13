use near_async::time::Clock;
use near_client_primitives::types::GetMaintenanceWindows;
use near_o11y::testonly::init_test_logger;

use crate::env::setup::setup_no_network;
use near_async::ActorSystem;
use near_async::messaging::CanSendAsync;

/// get maintenance window from view client
#[tokio::test]
async fn test_get_maintenance_windows_for_validator() {
    init_test_logger();
    let actor_system = ActorSystem::new();
    let actor_handles = setup_no_network(
        Clock::real(),
        actor_system.clone(),
        vec!["test".parse().unwrap(), "other".parse().unwrap()],
        "other".parse().unwrap(),
        true,
        true,
    );
    let res = actor_handles
        .view_client_actor
        .send_async(GetMaintenanceWindows { account_id: "test".parse().unwrap() })
        .await;

    // With test setup we get the following:
    //
    // shard_ids: [ShardId(0)]
    // #0
    // block producer: other
    // chunk producer for shard 0: test
    // #1
    // block producer: test
    // chunk producer for shard 0: test
    // #2
    // block producer: test
    // chunk producer for shard 0: other
    // #3
    // block producer: other
    // chunk producer for shard 0: other
    // #4
    // block producer: test
    // chunk producer for shard 0: test
    // #5
    // block producer: other
    // chunk producer for shard 0: other
    // #6
    // block producer: other
    // chunk producer for shard 0: test
    // #7
    // block producer: test
    // chunk producer for shard 0: other
    // #8
    // block producer: test
    // chunk producer for shard 0: other
    // #9
    // block producer: test
    // chunk producer for shard 0: other
    //
    // Maintenance heights are heights where account not a block or chunk producer
    assert_eq!(res.unwrap().unwrap(), [3..4, 5..6]);
    actor_system.stop();
}

#[tokio::test]
async fn test_get_maintenance_windows_for_not_validator() {
    init_test_logger();
    let actor_system = ActorSystem::new();
    let actor_handles = setup_no_network(
        Clock::real(),
        actor_system.clone(),
        vec!["test".parse().unwrap()],
        "other".parse().unwrap(),
        true,
        true,
    );
    let res = actor_handles
        .view_client_actor
        .send_async(GetMaintenanceWindows { account_id: "alice".parse().unwrap() })
        .await;
    assert_eq!(res.unwrap().unwrap(), vec![0..10]);
    actor_system.stop();
}
