use actix::System;
use futures::{FutureExt, future};
use near_actix_test_utils::run_actix;
use near_async::time::Clock;
use near_client::test_utils::setup_no_network;
use near_client_primitives::types::GetMaintenanceWindows;

use near_o11y::WithSpanContextExt;
use near_o11y::testonly::init_test_logger;

/// get maintenance window from view client
#[test]
fn test_get_maintenance_windows_for_validator() {
    init_test_logger();
    run_actix(async {
        let actor_handles = setup_no_network(
            Clock::real(),
            vec!["test".parse().unwrap(), "other".parse().unwrap()],
            "other".parse().unwrap(),
            true,
            true,
        );
        let actor = actor_handles.view_client_actor.send(
            GetMaintenanceWindows { account_id: "test".parse().unwrap() }.with_span_context(),
        );

        // shard_ids: [ShardId(0)]
        // #0
        // block producer: test
        // chunk producer for shard 0: other
        // #1
        // block producer: other
        // chunk producer for shard 0: other
        // #2
        // block producer: other
        // chunk producer for shard 0: test
        // #3
        // block producer: test
        // chunk producer for shard 0: test
        // #4
        // block producer: other
        // chunk producer for shard 0: other
        // #5
        // block producer: test
        // chunk producer for shard 0: test
        // #6
        // block producer: test
        // chunk producer for shard 0: other
        // #7
        // block producer: other
        // chunk producer for shard 0: test
        // #8
        // block producer: other
        // chunk producer for shard 0: test
        // #9
        // block producer: other
        // chunk producer for shard 0: test
        //
        // Maintenance heights (heights where it's not a block or chunk producer) for test: 1, 4
        let actor = actor.then(|res| {
            assert_eq!(res.unwrap().unwrap(), [1..2, 4..5]);
            System::current().stop();
            future::ready(())
        });
        actix::spawn(actor);
    });
}

#[test]
fn test_get_maintenance_windows_for_not_validator() {
    init_test_logger();
    run_actix(async {
        let actor_handles = setup_no_network(
            Clock::real(),
            vec!["test".parse().unwrap()],
            "other".parse().unwrap(),
            true,
            true,
        );
        let actor = actor_handles.view_client_actor.send(
            GetMaintenanceWindows { account_id: "alice".parse().unwrap() }.with_span_context(),
        );
        let actor = actor.then(|res| {
            assert_eq!(res.unwrap().unwrap(), vec![0..10]);
            System::current().stop();
            future::ready(())
        });
        actix::spawn(actor);
    });
}
