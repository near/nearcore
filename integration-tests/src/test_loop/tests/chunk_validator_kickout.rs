use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::validators::get_epoch_all_validators;
use crate::test_loop::utils::ONE_NEAR;
use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;

const NUM_ACCOUNTS: usize = 8;
const NUM_PRODUCER_ACCOUNTS: usize = 6;

fn create_accounts() -> Vec<AccountId> {
    (0..NUM_ACCOUNTS).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>()
}

enum TestCase {
    /// Drop chunks validated by the account.
    DropChunksValidatedBy(AccountId),
    /// Drop endorsements from the account.
    DropEndorsementsFrom(AccountId),
}

impl TestCase {
    fn selected_account(&self) -> &AccountId {
        match self {
            TestCase::DropChunksValidatedBy(account_id) => account_id,
            TestCase::DropEndorsementsFrom(account_id) => account_id,
        }
    }
}

fn run_test_chunk_validator_kickout(accounts: Vec<AccountId>, test_case: TestCase) {
    init_test_logger();
    let initial_balance = 10000 * ONE_NEAR;
    let epoch_length = 10;
    let clients = accounts.iter().cloned().collect_vec();
    let accounts_str = accounts.iter().map(|a| a.as_str()).collect_vec();
    let (block_and_chunk_producers, chunk_validators_only) =
        accounts_str.split_at(NUM_PRODUCER_ACCOUNTS);

    let builder = match &test_case {
        TestCase::DropChunksValidatedBy(account_id) => TestLoopBuilder::new()
            // Drop only chunks validated by `account_id`.
            // By how our endorsement stats are computed, this will count as this
            // validator validating zero chunks.
            .drop_chunks_validated_by(account_id.as_str()),
        TestCase::DropEndorsementsFrom(account_id) => TestLoopBuilder::new()
            // Drop only endorsements for chunks validated by `account_id`.
            .drop_endorsements_from(account_id.as_str()),
    };

    let num_validator_mandates_per_shard = match &test_case {
        // Target giving one mandate to each chunk validator, which results in
        // every chunk validator validating only one shard in most cases.
        // As a result, when we drop the chunk, we also zero-out all the endorsements
        // of the corresponding chunk validator.
        TestCase::DropChunksValidatedBy(_) => 1,
        // Target giving a large number of mandates to each chunk validator, so that if we drop all the
        // endorsements from one of the validators, this will not result in missing any chunks.
        TestCase::DropEndorsementsFrom(_) => 16,
    };

    // Only chunk validator-only node can be kicked out for low endorsement stats.
    let account_to_kickout =
        if chunk_validators_only.contains(&test_case.selected_account().as_str()) {
            Some(test_case.selected_account())
        } else {
            None
        };

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .shard_layout_simple_v1(&["account2", "account4", "account6"])
        .epoch_length(epoch_length)
        // Select 6 block&chunk producers and 2 chunk validators.
        .validators_desired_roles(block_and_chunk_producers, chunk_validators_only)
        // Set up config to kick out only chunk validators for low performance.
        .kickouts_for_chunk_validators_only()
        .target_validator_mandates_per_shard(num_validator_mandates_per_shard);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis_and_epoch_config_store = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = builder
        .genesis_and_epoch_config_store(genesis_and_epoch_config_store)
        .clients(clients)
        .build();

    // Run chain until our targeted chunk validator is (not) kicked out.
    let client_handle = node_datas[0].client_sender.actor_handle();
    let initial_validators = get_epoch_all_validators(&test_loop.data.get(&client_handle).client);
    assert_eq!(initial_validators.len(), NUM_ACCOUNTS);
    assert!(initial_validators.contains(&test_case.selected_account().to_string()));
    let success_condition = |test_loop_data: &mut TestLoopData| -> bool {
        let client = &test_loop_data.get(&client_handle).client;
        let tip = client.chain.head().unwrap();

        // Check the number of missed chunks for each test case.
        let block = client.chain.get_block(&tip.last_block_hash).unwrap();
        let num_missed_chunks = block.header().chunk_mask().iter().filter(|c| !**c).count();
        match &test_case {
            TestCase::DropChunksValidatedBy(_) => assert!(num_missed_chunks <= 1,
                "At most one chunk must be missed when dropping chunks validated by the selected account"),
            TestCase::DropEndorsementsFrom(_) => assert_eq!(num_missed_chunks, 0,
                "No chunk must be missed when dropping endorsements from the selected account"),
        }

        let validators = get_epoch_all_validators(client);
        let epoch_height =
            client.epoch_manager.get_epoch_height_from_prev_block(&tip.prev_block_hash).unwrap();
        if let Some(account_id) = &account_to_kickout {
            assert!(epoch_height < 4);
            return if validators.len() == NUM_ACCOUNTS - 1 {
                assert!(!validators.contains(&account_id.to_string()));
                true
            } else {
                false
            };
        } else {
            assert_eq!(validators.len(), NUM_ACCOUNTS, "No kickouts are expected");
            epoch_height >= 4
        }
    };

    test_loop.run_until(
        success_condition,
        // Timeout at producing 5 epochs, approximately.
        Duration::seconds((5 * epoch_length) as i64),
    );

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Checks that chunk validator with low endorsement stats is kicked out when the chunks it would validate are all dropped.
#[test]
fn test_chunk_validator_kicked_out_when_chunks_dropped() {
    let accounts = create_accounts();
    let test_case = TestCase::DropChunksValidatedBy(accounts[NUM_PRODUCER_ACCOUNTS + 1].clone());
    run_test_chunk_validator_kickout(accounts, test_case);
}

/// Checks that block producer with low chunk endorsement stats is not kicked out when the chunks it would validate are all dropped.
#[test]
fn test_block_producer_not_kicked_out_when_chunks_dropped() {
    let accounts = create_accounts();
    let test_case = TestCase::DropChunksValidatedBy(accounts[NUM_PRODUCER_ACCOUNTS - 1].clone());
    run_test_chunk_validator_kickout(accounts, test_case);
}

/// Checks that chunk validator with low endorsement stats is kicked out when the endorsements it generates are all dropped.
#[test]
fn test_chunk_validator_kicked_out_when_endorsements_dropped() {
    let accounts = create_accounts();
    let test_case = TestCase::DropEndorsementsFrom(accounts[NUM_PRODUCER_ACCOUNTS + 1].clone());
    run_test_chunk_validator_kickout(accounts, test_case);
}

/// Checks that block producer with low chunk endorsement stats is not kicked out when the endorsements it generates are all dropped.
#[test]
fn test_block_producer_not_kicked_out_when_endorsements_dropped() {
    let accounts = create_accounts();
    let test_case = TestCase::DropEndorsementsFrom(accounts[NUM_PRODUCER_ACCOUNTS - 1].clone());
    run_test_chunk_validator_kickout(accounts, test_case);
}
