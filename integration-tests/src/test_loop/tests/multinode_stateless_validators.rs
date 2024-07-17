use std::collections::HashMap;

use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{AccountId, EpochId, ValidatorInfoIdentifier};
use near_primitives::version::ProtocolFeature::StatelessValidation;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::CurrentEpochValidatorInfo;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::transactions::execute_money_transfers;
use crate::test_loop::utils::ONE_NEAR;

const NUM_ACCOUNTS: usize = 20;
const NUM_SHARDS: u64 = 4;
const EPOCH_LENGTH: u64 = 12;

const NUM_BLOCK_AND_CHUNK_PRODUCERS: usize = 4;
const NUM_CHUNK_VALIDATORS_ONLY: usize = 4;
const NUM_VALIDATORS: usize = NUM_BLOCK_AND_CHUNK_PRODUCERS + NUM_CHUNK_VALIDATORS_ONLY;

#[test]
fn test_stateless_validators_with_multi_test_loop() {
    if !StatelessValidation.enabled(PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
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

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(10000)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        .transaction_validity_period(1000)
        .epoch_length(EPOCH_LENGTH)
        .validators_desired_roles(&block_and_chunk_producers, &chunk_validators_only)
        .shuffle_shard_assignment_for_chunk_producers(true);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } =
        builder.genesis(genesis).clients(clients).build();

    // Capture the initial validator info in the first epoch.
    let client_handle = node_datas[0].client_sender.actor_handle();
    let chain = &test_loop.data.get(&client_handle).client.chain;
    let initial_epoch_id = chain.head().unwrap().epoch_id;

    let non_validator_accounts = accounts.iter().skip(NUM_VALIDATORS).cloned().collect_vec();
    execute_money_transfers(&mut test_loop, &node_datas, &non_validator_accounts);

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
    assert_validator_info(
        &test_loop.data.get(&client_handle).client,
        prev_epoch_id,
        initial_epoch_id,
        &accounts,
    );

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Returns the CurrentEpochValidatorInfo for each validator account for the given epoch id.
fn get_current_validators(
    client: &Client,
    epoch_id: EpochId,
) -> HashMap<AccountId, CurrentEpochValidatorInfo> {
    client
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::EpochId(epoch_id))
        .unwrap()
        .current_validators
        .iter()
        .map(|v| (v.account_id.clone(), v.clone()))
        .collect()
}

/// Asserts the following:
/// 1. Block and chunk producers produce block and chunks and also validate chunk witnesses.
/// 2. Chunk validators only validate chunk witnesses.
/// 3. Stake of both the block/chunk producers and chunk validators increase (due to rewards).
/// TODO: Assert on the specific reward amount, currently it only checks that some amount is rewarded.
fn assert_validator_info(
    client: &Client,
    epoch_id: EpochId,
    initial_epoch_id: EpochId,
    accounts: &Vec<AccountId>,
) {
    let validator_to_info = get_current_validators(client, epoch_id);
    let initial_validator_to_info = get_current_validators(client, initial_epoch_id);

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
