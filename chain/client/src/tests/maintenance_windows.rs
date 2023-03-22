use crate::test_utils::setup_no_network;
use actix::System;
use futures::{future, FutureExt};
use near_actix_test_utils::run_actix;
use near_client_primitives::types::GetMaintenanceWindows;

use near_o11y::testonly::init_test_logger;
use near_o11y::WithSpanContextExt;

/// get maintenance window from view client
#[test]
fn test_get_maintenance_windows_for_validator() {
    init_test_logger();
    run_actix(async {
        let actor_handles = setup_no_network(
            vec!["test".parse().unwrap(), "other".parse().unwrap()],
            "other".parse().unwrap(),
            true,
            true,
        );
        let actor = actor_handles.view_client_actor.send(
            GetMaintenanceWindows { account_id: "test".parse().unwrap() }.with_span_context(),
        );

        // block_height: 0 bp: test         cps: [AccountId("test")]
        // block_height: 1 bp: validator    cps: [AccountId("validator")]
        // block_height: 2 bp: test         cps: [AccountId("test")]
        // block_height: 3 bp: validator    cps: [AccountId("validator")]
        // block_height: 4 bp: test         cps: [AccountId("test")]
        // block_height: 5 bp: validator    cps: [AccountId("validator")]
        // block_height: 6 bp: test         cps: [AccountId("test")]
        // block_height: 7 bp: validator    cps: [AccountId("validator")]
        // block_height: 8 bp: test         cps: [AccountId("test")]
        // block_height: 9 bp: validator    cps: [AccountId("validator")]
        let actor = actor.then(|res| {
            assert_eq!(res.unwrap().unwrap(), vec![1..2, 3..4, 5..6, 7..8, 9..10]);
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
        let actor_handles =
            setup_no_network(vec!["test".parse().unwrap()], "other".parse().unwrap(), true, true);
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
