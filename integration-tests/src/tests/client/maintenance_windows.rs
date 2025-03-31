use actix::System;
use futures::{FutureExt, future};
use near_actix_test_utils::run_actix;
use near_async::time::Clock;
use near_client_primitives::types::GetMaintenanceWindows;
use near_o11y::WithSpanContextExt;
use near_o11y::testonly::init_test_logger;

use crate::env::setup::setup_no_network;

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
        let actor = actor.then(|res| {
            assert_eq!(res.unwrap().unwrap(), [3..4, 5..6]);
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
