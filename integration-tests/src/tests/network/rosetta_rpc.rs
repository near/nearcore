use actix::System;
use near_actix_test_utils::run_actix;
use near_async::time::Clock;
use near_parameters::{RuntimeConfig, RuntimeConfigView};

use crate::env::setup::setup_no_network;

#[test]
fn test_convert_block_changes_to_transactions() {
    run_actix(async {
        let runtime_config: RuntimeConfigView = RuntimeConfig::test().into();
        let actor_handles = setup_no_network(
            Clock::real(),
            vec!["test".parse().unwrap()],
            "other".parse().unwrap(),
            true,
            false,
        );
        near_rosetta_rpc::test::test_convert_block_changes_to_transactions(
            &actor_handles.view_client_actor,
            &runtime_config,
        )
        .await;
        System::current().stop();
    });
}
