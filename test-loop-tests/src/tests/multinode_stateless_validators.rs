use std::collections::HashMap;

use itertools::Itertools;
use near_async::messaging::Handler;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::{GetValidatorInfo, ViewClientActorInner};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, EpochId, EpochReference};
use near_primitives::views::{CurrentEpochValidatorInfo, EpochValidatorInfo};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::transactions::execute_money_transfers;

const NUM_ACCOUNTS: usize = 20;
const NUM_SHARDS: u64 = 4;
const EPOCH_LENGTH: u64 = 12;

const NUM_BLOCK_AND_CHUNK_PRODUCERS: usize = 4;
const NUM_CHUNK_VALIDATORS_ONLY: usize = 4;
const NUM_VALIDATORS: usize = NUM_BLOCK_AND_CHUNK_PRODUCERS + NUM_CHUNK_VALIDATORS_ONLY;

#[test]
fn slow_test_stateless_validators_with_multi_test_loop() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let accounts = (0..NUM_ACCOUNTS)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();

    // All block_and_chunk_producers will be both block and chunk validators.
    let block_and_chunk_producers =
        (0..NUM_BLOCK_AND_CHUNK_PRODUCERS).map(|idx| accounts[idx].as_str()).collect_vec();
    // These are the accounts that are only chunk validators, but not block/chunk producers.
    let chunk_validators_only = (NUM_BLOCK_AND_CHUNK_PRODUCERS..NUM_VALIDATORS)
        .map(|idx| accounts[idx].as_str())
        .collect_vec();
    let clients = accounts.iter().take(NUM_VALIDATORS).cloned().collect_vec();

    let shard_layout = ShardLayout::simple_v1(&["account3", "account5", "account7"]);
    let validators_spec =
        ValidatorsSpec::desired_roles(&block_and_chunk_producers, &chunk_validators_only);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .genesis_height(10000)
        .transaction_validity_period(1000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let TestLoopEnv { mut test_loop, node_datas, shared_state } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    // Capture the initial validator info in the first epoch.
    let client_handle = node_datas[0].client_sender.actor_handle();
    let chain = &test_loop.data.get(&client_handle).client.chain;
    let initial_epoch_id = chain.head().unwrap().epoch_id;

    let non_validator_accounts = accounts.iter().skip(NUM_VALIDATORS).cloned().collect_vec();
    execute_money_transfers(&mut test_loop, &node_datas, &non_validator_accounts).unwrap();

    // Capture the id of the epoch we will check for the correct validator information in assert_validator_info.
    let prev_epoch_id = test_loop.data.get(&client_handle).client.chain.head().unwrap().epoch_id;
    assert_ne!(prev_epoch_id, initial_epoch_id);

    // Run the chain until it transitions to a different epoch then prev_epoch_id.
    test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().epoch_id
                != prev_epoch_id
        },
        Duration::seconds(EPOCH_LENGTH as i64),
    );

    // Check the validator information for the epoch with the prev_epoch_id.
    let view_client_handle = node_datas[0].view_client_sender.actor_handle();
    assert_validator_info(
        test_loop.data.get_mut(&view_client_handle),
        prev_epoch_id,
        initial_epoch_id,
        accounts.clone(),
    );

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Returns the CurrentEpochValidatorInfo for each validator account for the given epoch id.
fn get_validator_info(
    view_client: &mut ViewClientActorInner,
    epoch_id: EpochId,
) -> HashMap<AccountId, CurrentEpochValidatorInfo> {
    let validator_info: EpochValidatorInfo = view_client
        .handle(GetValidatorInfo { epoch_reference: EpochReference::EpochId(epoch_id) })
        .unwrap();
    validator_info.current_validators.iter().map(|v| (v.account_id.clone(), v.clone())).collect()
}

/// Asserts the following:
/// 1. Block and chunk producers produce block and chunks and also validate chunk witnesses.
/// 2. Chunk validators only validate chunk witnesses.
/// 3. Stake of both the block/chunk producers and chunk validators increase (due to rewards).
/// TODO: Assert on the specific reward amount, currently it only checks that some amount is rewarded.
fn assert_validator_info(
    view_client: &mut ViewClientActorInner,
    epoch_id: EpochId,
    initial_epoch_id: EpochId,
    accounts: Vec<AccountId>,
) {
    let validator_to_info = get_validator_info(view_client, epoch_id);
    let initial_validator_to_info = get_validator_info(view_client, initial_epoch_id);

    // Check that block/chunk producers generate blocks/chunks and also endorse chunk witnesses.
    for idx in 0..NUM_BLOCK_AND_CHUNK_PRODUCERS {
        let account = &accounts[idx];
        let validator_info = validator_to_info.get(account).unwrap();
        assert!(validator_info.num_produced_blocks > 0);
        assert!(validator_info.num_produced_blocks <= validator_info.num_expected_blocks);
        assert!(validator_info.num_expected_blocks < EPOCH_LENGTH);

        assert!(0 < validator_info.num_produced_chunks);
        assert!(validator_info.num_produced_chunks <= validator_info.num_expected_chunks);
        assert!(validator_info.num_expected_chunks < EPOCH_LENGTH * NUM_SHARDS);

        assert!(validator_info.num_produced_endorsements > 0);
        assert!(
            validator_info.num_produced_endorsements <= validator_info.num_expected_endorsements
        );
        assert!(validator_info.num_expected_endorsements <= EPOCH_LENGTH * NUM_SHARDS);

        let initial_validator_info = initial_validator_to_info.get(account).unwrap();
        assert!(initial_validator_info.stake < validator_info.stake);
    }
    // Check chunk validators only endorse chunk witnesses.
    for idx in NUM_BLOCK_AND_CHUNK_PRODUCERS..NUM_VALIDATORS {
        let account = &accounts[idx];
        let validator_info = validator_to_info.get(account).unwrap();
        assert_eq!(validator_info.num_expected_blocks, 0);
        assert_eq!(validator_info.num_expected_chunks, 0);
        assert_eq!(validator_info.num_produced_blocks, 0);
        assert_eq!(validator_info.num_produced_chunks, 0);

        assert!(validator_info.num_produced_endorsements > 0);
        assert!(
            validator_info.num_produced_endorsements <= validator_info.num_expected_endorsements
        );
        assert!(validator_info.num_expected_endorsements <= EPOCH_LENGTH * NUM_SHARDS);

        let initial_validator_info = initial_validator_to_info.get(account).unwrap();
        assert!(initial_validator_info.stake < validator_info.stake);
    }
}
