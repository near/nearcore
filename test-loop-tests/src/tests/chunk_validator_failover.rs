use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_validator_id;
use crate::utils::contract_distribution::assert_all_chunk_endorsements_received;
use crate::utils::validators::get_epoch_all_validators;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};

/// Tests chunk validator key failover: when a chunk-only validator's key is
/// moved to a backup node mid-epoch, the backup node picks up endorsement
/// duties and the original validator account is NOT kicked out.
#[test]
fn test_chunk_validator_failover() {
    init_test_logger();

    let epoch_length: u64 = 10;
    let killed_validator = create_validator_id(3);

    let mut env =
        TestLoopBuilder::new().validators(1, 3).enable_rpc().epoch_length(epoch_length).build();

    // Wait until 1/3 into epoch 2.
    let failover_height = epoch_length + epoch_length / 3;
    env.validator_runner().run_until_head_height(failover_height);

    // Kill validator3 and the rpc (backup) node. Restart the backup node
    // with validator3's account_id so it receives validator3's key and the
    // network routing table directs state witnesses to it.
    env.kill_node(killed_validator.as_str());
    let mut backup_state = env.kill_node("rpc");
    backup_state.account_id = killed_validator.clone();
    env.restart_node("failover", backup_state);

    // Run past the next epoch boundary so endorsement stats are evaluated.
    let end_height = epoch_length * 3;
    env.validator_runner().run_until_head_height(end_height);

    // Verify all chunk endorsements received soon after failover.
    // Spice uses a different endorsement mechanism (core statements), so the
    // traditional endorsement bitmap in block headers is empty.
    if !ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
        assert_all_chunk_endorsements_received(&env, failover_height + 2, end_height);
    }

    // Verify no validators were kicked out.
    let client = env.validator().client();
    let tip = client.chain.head().unwrap();
    let epoch_info = client.epoch_manager.get_epoch_info(&tip.epoch_id).unwrap();
    let kickouts = epoch_info.validator_kickout();
    assert!(kickouts.is_empty(), "no validators should be kicked out, got: {:?}", kickouts);

    // Verify the killed validator is still active (not kicked out).
    let validators = get_epoch_all_validators(client);
    assert!(
        validators.contains(&killed_validator.to_string()),
        "{killed_validator} must still be a validator",
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
