use crate::env::setup::setup_no_network;
use near_async::ActorSystem;
use near_async::time::Clock;
use near_parameters::{RuntimeConfig, RuntimeConfigView};

#[tokio::test]
async fn test_convert_block_changes_to_transactions() {
    let actor_system = ActorSystem::new();
    let runtime_config: RuntimeConfigView = RuntimeConfig::test().into();
    let actor_handles = setup_no_network(
        Clock::real(),
        actor_system.clone(),
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
    near_rosetta_rpc::test::test_gas_key_changes_to_transactions(
        &actor_handles.view_client_actor,
        &runtime_config,
    )
    .await;
    near_rosetta_rpc::test::test_stake_gas_prepayment(
        &actor_handles.view_client_actor,
        &runtime_config,
    )
    .await;
    near_rosetta_rpc::test::test_function_call_deposit_separation(
        &actor_handles.view_client_actor,
        &runtime_config,
    )
    .await;
    near_rosetta_rpc::test::test_execution_status_propagation(
        &actor_handles.view_client_actor,
        &runtime_config,
    )
    .await;
    actor_system.stop();
}
