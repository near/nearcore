use std::sync::Arc;

use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, Balance};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::validators::get_epoch_all_validators;

/// Tests chunk validator key failover: when a chunk-only validator's key is
/// moved to a backup node mid-epoch, the backup node begins endorsing
/// immediately and the original validator account is NOT kicked out.
#[test]
fn slow_test_chunk_validator_failover() {
    init_test_logger();

    let epoch_length = 30;
    let block_producer = "block_producer";
    let chunk_validators = &["chunk_validator0", "chunk_validator1", "chunk_validator2"];
    let failover_node = "failover";

    let validators_spec = ValidatorsSpec::desired_roles(&[block_producer], chunk_validators);

    let clients: Vec<AccountId> = [block_producer]
        .iter()
        .chain(chunk_validators.iter())
        .chain([&failover_node])
        .map(|s| s.parse().unwrap())
        .collect();

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&clients, Balance::from_near(1_000_000))
        .build();

    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .kickouts_standard_80_percent()
        .build_store_for_genesis_protocol_version();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    let block_producer_id: AccountId = block_producer.parse().unwrap();
    let killed_validator = chunk_validators[2];
    let failover_id: AccountId = failover_node.parse().unwrap();

    // Wait until 1/3 into epoch 2.
    let failover_height = epoch_length + epoch_length / 3;
    env.runner_for_account(&block_producer_id).run_until_head_height(failover_height as u64);

    // Kill chunk_validator2 and swap its key to the failover node.
    env.kill_node(killed_validator);

    let signer = create_test_signer(killed_validator);
    env.node_for_account(&failover_id).client().validator_signer.update(Some(Arc::new(signer)));

    // Run past two more epoch boundaries so the endorsement stats are checked.
    env.runner_for_account(&block_producer_id).run_until_head_height(epoch_length as u64 * 3);

    // Verify no validators were kicked out.
    let client = env.node_for_account(&block_producer_id).client();
    let tip = client.chain.head().unwrap();
    let epoch_info = client.epoch_manager.get_epoch_info(&tip.epoch_id).unwrap();
    let kickouts = epoch_info.validator_kickout();
    assert!(kickouts.is_empty(), "no validators should be kicked out, got: {:?}", kickouts);

    // Verify all validators are still active.
    let validators = get_epoch_all_validators(client);
    assert!(
        validators.contains(&killed_validator.to_string()),
        "{killed_validator} must still be a validator",
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
