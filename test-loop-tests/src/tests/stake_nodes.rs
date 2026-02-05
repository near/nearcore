use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_chain_configs::test_utils::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near_o11y::testonly::init_test_logger;
use near_primitives::num_rational::Rational32;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountInfo, Balance};
use primitive_types::U256;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_validator_ids;
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::{get_shared_block_hash, run_tx, run_txs_parallel};
use crate::utils::validators::get_epoch_all_validators;

fn get_validators_sorted(client: &near_client::Client) -> Vec<String> {
    get_epoch_all_validators(client).into_iter().sorted().collect()
}

/// Runs one validator network, sends staking transaction for the second node and
/// waits until it becomes a validator.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_stake_nodes() {
    init_test_logger();

    let accounts = create_validator_ids(2);
    let epoch_length = 10;

    let validators = vec![AccountInfo {
        account_id: accounts[0].clone(),
        public_key: create_test_signer(accounts[0].as_str()).public_key(),
        amount: TESTING_INIT_STAKE,
    }];
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(ValidatorsSpec::raw(validators, 2, 2, 2))
        .add_user_accounts_simple(&accounts, TESTING_INIT_BALANCE)
        .max_inflation_rate(Rational32::new(0, 1))
        .build();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(accounts.clone())
        .build()
        .warmup();

    // Submit stake transaction from accounts[1]
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let tx = SignedTransaction::stake(
        1,
        accounts[1].clone(),
        &create_user_test_signer(&accounts[1]),
        TESTING_INIT_STAKE,
        create_test_signer(accounts[1].as_str()).public_key(),
        block_hash,
    );
    run_tx(&mut env.test_loop, &accounts[0], tx, &env.node_datas, Duration::seconds(30));

    let client_handle = env.node_datas[0].client_sender.actor_handle();
    let expected: Vec<String> = vec!["validator0".to_string(), "validator1".to_string()];

    env.test_loop.run_until(
        |test_loop_data| {
            let client = &test_loop_data.get(&client_handle).client;
            get_validators_sorted(client) == expected
        },
        Duration::seconds(60),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Starts 4 nodes with 4 validators. Nodes 0-1 reduce stake to ~1 NEAR.
/// Waits until only nodes 2-3 are validators (kickout due to low stake).
/// Verifies locked == 0 for kicked nodes and locked == TESTING_INIT_STAKE for remaining.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_validator_kickout() {
    init_test_logger();

    let accounts = create_validator_ids(4);
    let epoch_length = 15;

    let validators: Vec<AccountInfo> = accounts
        .iter()
        .map(|account| AccountInfo {
            account_id: account.clone(),
            public_key: create_test_signer(account.as_str()).public_key(),
            amount: TESTING_INIT_STAKE,
        })
        .collect();
    // Use very high minimum_stake_divisor to allow low stakes
    // and zero inflation to avoid reward accumulation affecting locked amounts.
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(ValidatorsSpec::raw(validators, 4, 4, 4))
        .add_user_accounts_simple(&accounts, TESTING_INIT_BALANCE)
        .max_inflation_rate(Rational32::new(0, 1))
        .minimum_stake_divisor(1_000_000_000)
        .minimum_stake_ratio(Rational32::new(1, 100))
        .build();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(accounts.clone())
        .track_all_shards()
        .build()
        .warmup();

    // Submit reduced stake transactions for nodes 0 and 1
    let mut rng = StdRng::seed_from_u64(0);
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let txs: Vec<SignedTransaction> = (0..2)
        .map(|i| {
            let stake = Balance::from_near(1)
                .checked_add(Balance::from_yoctonear(rng.gen_range(1u128..100u128)))
                .unwrap();
            SignedTransaction::stake(
                1,
                accounts[i].clone(),
                &create_user_test_signer(&accounts[i]),
                stake,
                create_test_signer(accounts[i].as_str()).public_key(),
                block_hash,
            )
        })
        .collect();
    run_txs_parallel(&mut env.test_loop, txs, &env.node_datas, Duration::seconds(30));

    let node = TestLoopNode::from(&env.node_datas[0]);
    let expected: Vec<String> = vec!["validator2".to_string(), "validator3".to_string()];

    env.test_loop.run_until(
        |test_loop_data| {
            let client = node.client(test_loop_data);
            if get_validators_sorted(client) != expected {
                return false;
            }
            // Also wait for kicked nodes' locked amounts to return to zero
            for i in 0..2 {
                let view = node.view_account_query(test_loop_data, &accounts[i]);
                if !view.locked.is_zero() {
                    return false;
                }
            }
            true
        },
        Duration::seconds(120),
    );

    // Verify kicked nodes have locked == 0 and stake returned to balance
    let expected_balance = TESTING_INIT_BALANCE.checked_add(TESTING_INIT_STAKE).unwrap();
    for i in 0..2 {
        let view = node.view_account_query(&env.test_loop.data, &accounts[i]);
        assert!(view.locked.is_zero(), "kicked node {i}");
        assert_eq!(view.amount, expected_balance, "kicked node {i}");
    }

    // Verify remaining validators have locked == TESTING_INIT_STAKE
    // Note: Genesis builder sets amount separately from validator stake (not deducted)
    for i in 2..4 {
        let view = node.view_account_query(&env.test_loop.data, &accounts[i]);
        assert_eq!(view.locked, TESTING_INIT_STAKE, "remaining validator {i}");
        assert_eq!(view.amount, TESTING_INIT_BALANCE, "remaining validator {i}");
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Starts 4 nodes, genesis has 2 validator seats.
/// Node1 unstakes, Node2 stakes.
/// Submit the transactions via Node1 and Node2.
/// Poll validators until you see the change of validator assignments.
/// Afterwards check that `locked` amount on accounts Node1 and Node2 are 0 and TESTING_INIT_STAKE.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_validator_join() {
    init_test_logger();

    let accounts = create_validator_ids(4);
    let epoch_length = 30;

    let validators: Vec<AccountInfo> = accounts[..2]
        .iter()
        .map(|account| AccountInfo {
            account_id: account.clone(),
            public_key: create_test_signer(account.as_str()).public_key(),
            amount: TESTING_INIT_STAKE,
        })
        .collect();
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(ValidatorsSpec::raw(validators, 2, 2, 2))
        .add_user_accounts_simple(&accounts, TESTING_INIT_BALANCE)
        .max_inflation_rate(Rational32::new(0, 1))
        .build();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(accounts.clone())
        .track_all_shards()
        .build()
        .warmup();

    // Node1 unstakes, Node2 stakes
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let txs = vec![
        SignedTransaction::stake(
            1,
            accounts[1].clone(),
            &create_user_test_signer(&accounts[1]),
            Balance::ZERO,
            create_test_signer(accounts[1].as_str()).public_key(),
            block_hash,
        ),
        SignedTransaction::stake(
            1,
            accounts[2].clone(),
            &create_user_test_signer(&accounts[2]),
            TESTING_INIT_STAKE,
            create_test_signer(accounts[2].as_str()).public_key(),
            block_hash,
        ),
    ];
    run_txs_parallel(&mut env.test_loop, txs, &env.node_datas, Duration::seconds(30));

    let node = TestLoopNode::from(&env.node_datas[0]);
    let expected: Vec<String> = vec!["validator0".to_string(), "validator2".to_string()];

    env.test_loop.run_until(
        |test_loop_data| {
            let client = node.client(test_loop_data);
            if get_validators_sorted(client) != expected {
                return false;
            }
            // Wait for node1's locked amount to return to zero
            let view = node.view_account_query(test_loop_data, &accounts[1]);
            if !view.locked.is_zero() {
                return false;
            }
            // Wait for node2's locked amount to equal TESTING_INIT_STAKE
            let view = node.view_account_query(test_loop_data, &accounts[2]);
            view.locked == TESTING_INIT_STAKE
        },
        Duration::seconds(120),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Checks that during the first epoch, total_supply matches total_supply in genesis.
/// Checks that during the second epoch, total_supply matches the expected inflation rate.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_inflation() {
    init_test_logger();

    let accounts = create_validator_ids(1);
    let epoch_length: u64 = 10;

    let validators = vec![AccountInfo {
        account_id: accounts[0].clone(),
        public_key: create_test_signer(accounts[0].as_str()).public_key(),
        amount: TESTING_INIT_STAKE,
    }];
    let max_inflation_rate = Rational32::new(1, 10);
    let protocol_reward_rate = Rational32::new(1, 10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .genesis_height(0)
        .epoch_length(epoch_length)
        .validators_spec(ValidatorsSpec::raw(validators, 1, 1, 1))
        .add_user_accounts_simple(&accounts, TESTING_INIT_BALANCE)
        .max_inflation_rate(max_inflation_rate)
        .protocol_reward_rate(protocol_reward_rate)
        .gas_prices(Balance::from_yoctonear(100_000_000), Balance::from_yoctonear(100_000_000))
        .build();
    let initial_total_supply = genesis.config.total_supply;

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(accounts)
        .build()
        .warmup();

    let client_handle = env.node_datas[0].client_sender.actor_handle();

    // Wait until we're in epoch 1 (height 1 to epoch_length)
    env.test_loop.run_until(
        |test_loop_data| {
            let client = &test_loop_data.get(&client_handle).client;
            let head = client.chain.head().unwrap();
            head.height >= 2 && head.height <= epoch_length
        },
        Duration::seconds(30),
    );

    // Check total_supply unchanged in epoch 1
    {
        let client = &env.test_loop.data.get(&client_handle).client;
        let head = client.chain.head().unwrap();
        let block = client.chain.get_block(&head.last_block_hash).unwrap();
        assert_eq!(
            block.header().total_supply(),
            initial_total_supply,
            "total supply should be unchanged in epoch 1"
        );
    }

    // Wait until we're in epoch 2 (height > epoch_length)
    env.test_loop.run_until(
        |test_loop_data| {
            let client = &test_loop_data.get(&client_handle).client;
            let head = client.chain.head().unwrap();
            head.height > epoch_length && head.height < epoch_length * 2
        },
        Duration::seconds(60),
    );

    // Check total_supply matches expected inflation in epoch 2
    {
        let client = &env.test_loop.data.get(&client_handle).client;
        let head = client.chain.head().unwrap();
        let block = client.chain.get_block(&head.last_block_hash).unwrap();

        // Get timestamps for inflation calculation
        let genesis_block = client.chain.genesis_block();
        let epoch_end_block = client.chain.get_block_by_height(epoch_length).unwrap();
        let genesis_timestamp = genesis_block.header().raw_timestamp();
        let epoch_end_timestamp = epoch_end_block.header().raw_timestamp();

        let base_reward = (U256::from(initial_total_supply.as_yoctonear())
            * U256::from(epoch_end_timestamp - genesis_timestamp)
            * U256::from(*max_inflation_rate.numer() as u64)
            / (U256::from(10u64.pow(9) * 365 * 24 * 60 * 60)
                * U256::from(*max_inflation_rate.denom() as u64)))
        .as_u128();
        // To match rounding, split into protocol reward and validator reward.
        // Protocol reward is one tenth of the base reward, while validator reward is the remainder.
        // There's only one validator so the second part of the computation is easier.
        // The validator rewards depend on its uptime; in other words, the more blocks, chunks and endorsements
        // it produces the bigger is the reward.
        // In this test the validator produces 10 blocks out 10, 9 chunks out of 10 and 9 endorsements out of 10.
        // Then there's a formula to translate 28/30 successes to a 10/27 reward multiplier
        // (using min_online_threshold=9/10 and max_online_threshold=99/100).
        //
        // For additional details check: chain/epoch-manager/src/reward_calculator.rs or
        // https://nomicon.io/Economics/Economics.html#validator-rewards-calculation
        let protocol_reward = base_reward / 10;
        let validator_reward = base_reward - protocol_reward;
        // Chunk endorsement ratio 9/10 is mapped to 1 so the reward multiplier becomes 20/27.
        let inflation = protocol_reward + validator_reward * 20 / 27;

        assert_eq!(
            block.header().total_supply(),
            initial_total_supply.checked_add(Balance::from_yoctonear(inflation)).unwrap(),
            "total supply should match expected inflation in epoch 2"
        );
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
