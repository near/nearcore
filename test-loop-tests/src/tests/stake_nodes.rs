use super::spice_utils::delay_endorsements_propagation;
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{
    create_validator_ids, create_validators_spec, validators_spec_clients,
};
use crate::utils::transactions::{get_shared_block_hash, run_tx, run_txs_parallel};
use crate::utils::validators::get_epoch_all_validators_sorted;
use near_async::messaging::Handler;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_chain_configs::test_utils::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near_client::{GetStateChanges, GetValidatorInfo, Query};
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, StakeAction};
use near_primitives::num_rational::Rational32;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockId, BlockReference, EpochReference, ShardId,
};
use near_primitives::views::{
    QueryRequest, QueryResponseKind, StateChangeCauseView, StateChangeValueView,
    StateChangesRequestView,
};
use primitive_types::U256;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
/// Runs one validator network, sends staking transaction for the second node and
/// waits until it becomes a validator.
#[test]
fn test_stake_nodes() {
    let epoch_length = 10;
    let execution_delay = 0;
    test_stake_nodes_impl(epoch_length, execution_delay);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_stake_nodes_delayed_execution() {
    let epoch_length = 10;
    let execution_delay = 4;
    test_stake_nodes_impl(epoch_length, execution_delay);
}

fn test_stake_nodes_impl(epoch_length: u64, execution_delay: u64) {
    init_test_logger();

    let accounts = create_validator_ids(2);

    // Build validators with explicit stake amounts for precise balance assertions
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
        .delay_warmup()
        .build();
    if execution_delay > 0 {
        delay_endorsements_propagation(&mut env, execution_delay);
    }
    let mut env = env.warmup();

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

    let expected: Vec<String> = vec!["validator0".to_string(), "validator1".to_string()];

    env.node_runner(0).run_until(
        |node| get_epoch_all_validators_sorted(node.client()) == expected,
        Duration::seconds(60),
    );
}

/// Starts 4 nodes with 4 validators. Nodes 0-1 reduce stake to ~1 NEAR.
/// Waits until only nodes 2-3 are validators (kickout due to low stake).
/// Verifies locked == 0 for kicked nodes and locked == TESTING_INIT_STAKE for remaining.
#[test]
fn test_validator_kickout() {
    let epoch_length = 15;
    let execution_delay = 0;
    test_validator_kickout_impl(epoch_length, execution_delay);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validator_kickout_delayed_execution() {
    let epoch_length = 15;
    let execution_delay = 4;
    test_validator_kickout_impl(epoch_length, execution_delay);
}

fn test_validator_kickout_impl(epoch_length: u64, execution_delay: u64) {
    init_test_logger();

    let accounts = create_validator_ids(4);

    // Build validators with explicit stake amounts for precise balance assertions
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
        .delay_warmup()
        .build();
    if execution_delay > 0 {
        delay_endorsements_propagation(&mut env, execution_delay);
    }
    let mut env = env.warmup();

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

    let expected: Vec<String> = vec!["validator2".to_string(), "validator3".to_string()];

    env.validator_runner().run_until(
        |node| {
            if get_epoch_all_validators_sorted(node.client()) != expected {
                return false;
            }
            // Also wait for kicked nodes' locked amounts to return to zero
            for i in 0..2 {
                let view = node.view_account_query(&accounts[i]).unwrap();
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
        let view = env.validator().view_account_query(&accounts[i]).unwrap();
        assert!(view.locked.is_zero(), "kicked node {i}");
        assert_eq!(view.amount, expected_balance, "kicked node {i}");
    }

    // Verify remaining validators have locked == TESTING_INIT_STAKE
    // Note: Genesis builder sets amount separately from validator stake (not deducted)
    for i in 2..4 {
        let view = env.validator().view_account_query(&accounts[i]).unwrap();
        assert_eq!(view.locked, TESTING_INIT_STAKE, "remaining validator {i}");
        assert_eq!(view.amount, TESTING_INIT_BALANCE, "remaining validator {i}");
    }
}

/// Starts 4 nodes, genesis has 2 validator seats.
/// Node1 unstakes, Node2 stakes.
/// Submit the transactions via Node1 and Node2.
/// Poll validators until you see the change of validator assignments.
/// Afterwards check that `locked` amount on accounts Node1 and Node2 are 0 and TESTING_INIT_STAKE.
#[test]
fn test_validator_join() {
    let epoch_length = 30;
    let execution_delay = 0;
    test_validator_join_impl(epoch_length, execution_delay);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validator_join_delayed_execution() {
    let epoch_length = 30;
    let execution_delay = 4;
    test_validator_join_impl(epoch_length, execution_delay);
}

fn test_validator_join_impl(epoch_length: u64, execution_delay: u64) {
    init_test_logger();

    let accounts = create_validator_ids(4);

    // Build validators with explicit stake amounts for precise balance assertions
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
        .delay_warmup()
        .build();
    if execution_delay > 0 {
        delay_endorsements_propagation(&mut env, execution_delay);
    }
    let mut env = env.warmup();

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

    let expected: Vec<String> = vec!["validator0".to_string(), "validator2".to_string()];

    env.node_runner(0).run_until(
        |node| {
            if get_epoch_all_validators_sorted(node.client()) != expected {
                return false;
            }
            // Wait for node1's locked amount to return to zero
            let view = node.view_account_query(&accounts[1]).unwrap();
            if !view.locked.is_zero() {
                return false;
            }
            // Wait for node2's locked amount to equal TESTING_INIT_STAKE
            let view = node.view_account_query(&accounts[2]).unwrap();
            view.locked == TESTING_INIT_STAKE
        },
        Duration::seconds(120),
    );
}

/// Tests the full validator lifecycle: a non-validator stakes to join the validator set,
/// then unstakes to leave it. Verifies that the validator set changes happen in the
/// expected epoch windows and that locked amounts are correct throughout.
#[test]
fn test_staking_join_and_leave() {
    test_staking_join_and_leave_impl(0);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_staking_join_and_leave_delayed_execution() {
    test_staking_join_and_leave_impl(4);
}

/// `execution_delay` is the number of blocks by which chunk endorsement propagation
/// is delayed; 0 means normal execution, >0 triggers delayed (spice) execution.
fn test_staking_join_and_leave_impl(execution_delay: u64) {
    init_test_logger();

    let epoch_length = 10;
    let accounts = create_validator_ids(3);
    let validator_key = create_test_signer(accounts[2].as_str()).public_key();

    // Set up 2 initial validators; the third account starts as a non-validator.
    let validators: Vec<AccountInfo> = accounts[..2]
        .iter()
        .map(|account| AccountInfo {
            account_id: account.clone(),
            public_key: create_test_signer(account.as_str()).public_key(),
            amount: TESTING_INIT_STAKE,
        })
        .collect();
    let mut env = TestLoopBuilder::new()
        .validators_spec(ValidatorsSpec::raw(validators, 2, 2, 2))
        .add_non_validator_client(&accounts[2])
        .add_user_account(&accounts[2], TESTING_INIT_BALANCE)
        .epoch_length(epoch_length)
        .max_inflation_rate(Rational32::new(0, 1))
        .track_all_shards()
        .delay_warmup()
        .build();
    if execution_delay > 0 {
        delay_endorsements_propagation(&mut env, execution_delay);
    }
    let mut env = env.warmup();

    // Verify initial validator set.
    let initial_validators: Vec<String> = accounts[..2].iter().map(|a| a.to_string()).collect();
    assert_eq!(get_epoch_all_validators_sorted(env.node(0).client()), initial_validators,);

    // Stake for validator2 to join the validator set.
    let stake_tx = env.node(0).tx_from_actions(
        &accounts[2],
        &accounts[2],
        vec![Action::Stake(Box::new(StakeAction {
            stake: TESTING_INIT_STAKE,
            public_key: validator_key.clone(),
        }))],
    );
    env.node_runner(0).run_tx(stake_tx, Duration::seconds(5));

    // Wait for validator2 to join.
    let all_validators: Vec<String> = accounts.iter().map(|a| a.to_string()).collect();
    env.node_runner(0).run_until(
        |node| get_epoch_all_validators_sorted(node.client()) == all_validators,
        Duration::seconds(30),
    );

    // Staking tx in epoch 0 → validator joins in epoch 2.
    // With delayed execution, tx inclusion shifts so skip the height check.
    if execution_delay == 0 {
        let join_height = env.node(0).head().height;
        assert!(
            join_height > 2 * epoch_length && join_height <= 3 * epoch_length,
            "validator2 should join in epoch 2, but head height is {join_height}"
        );
    }

    // Verify validator2 has its stake locked.
    let view = env.node(0).view_account_query(&accounts[2]).unwrap();
    assert_eq!(view.locked, TESTING_INIT_STAKE);

    // Unstake validator2.
    let unstake_tx = env.node(0).tx_from_actions(
        &accounts[2],
        &accounts[2],
        vec![Action::Stake(Box::new(StakeAction {
            stake: Balance::ZERO,
            public_key: validator_key,
        }))],
    );
    env.node_runner(0).run_tx(unstake_tx, Duration::seconds(5));

    // Wait for validator2 to leave the validator set.
    env.node_runner(0).run_until(
        |node| get_epoch_all_validators_sorted(node.client()) == initial_validators,
        Duration::seconds(30),
    );

    // Unstaking tx in epoch 2 → validator removed from set in epoch 4.
    if execution_delay == 0 {
        let leave_height = env.node(0).head().height;
        assert!(
            leave_height > 4 * epoch_length && leave_height <= 5 * epoch_length,
            "validator2 should leave in epoch 4, but head height is {leave_height}"
        );
    }

    // Wait for locked amount to return to zero (happens one epoch after removal).
    env.node_runner(0).run_until(
        |node| node.view_account_query(&accounts[2]).unwrap().locked.is_zero(),
        Duration::seconds(30),
    );

    // Verify stake was fully returned to balance.
    let view = env.node(0).view_account_query(&accounts[2]).unwrap();
    assert!(view.locked.is_zero());
    assert_eq!(view.amount, TESTING_INIT_BALANCE);
}

/// Verifies that a validator who unstakes and then re-stakes in an uncertified chunk
/// near the epoch boundary does NOT have their stake returned. The re-stake proposal
/// from the uncertified chunk should be picked up by get_uncertified_validator_proposals.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_uncertified_restake_prevents_stake_return() {
    init_test_logger();

    let epoch_length: u64 = 10;
    let endorsement_delay: u64 = 4;
    let unstaker_idx = 0;
    let validators_spec = create_validators_spec(4, 1);
    let accounts = validators_spec_clients(&validators_spec);
    let unstaker = accounts[unstaker_idx].clone();
    let unstaker_key = create_test_signer(unstaker.as_str()).public_key();

    let mut env = TestLoopBuilder::new()
        .validators_spec(validators_spec)
        .epoch_length(epoch_length)
        .add_user_accounts(&accounts, TESTING_INIT_BALANCE)
        .max_inflation_rate(Rational32::new(0, 1))
        .config_modifier(move |config, idx| {
            // TODO(spice): Here, we force unstaker to retain memtrie for the
            // unloaded shard. Memtrie retention needs to be fixed for spice to
            // wait until certification of the last block of the prior epoch.
            if idx == unstaker_idx {
                config.tracked_shards_config = TrackedShardsConfig::AllShards;
            }
        })
        .delay_warmup()
        .build();
    delay_endorsements_propagation(&mut env, endorsement_delay);
    let mut env = env.warmup();

    let genesis_height = env.node(unstaker_idx).client().chain.genesis().height();
    let initial_stake = env.node(unstaker_idx).view_account_query(&unstaker).unwrap().locked;

    // Submit unstake (stake = 0) in the first epoch.
    let unstake_tx = env.node(unstaker_idx).tx_from_actions(
        &unstaker,
        &unstaker,
        vec![Action::Stake(Box::new(StakeAction {
            stake: Balance::ZERO,
            public_key: unstaker_key.clone(),
        }))],
    );
    env.node_runner(unstaker_idx).run_tx(unstake_tx, Duration::seconds(30));

    // Advance to a few blocks before the E4->E5 boundary where stake return would
    // happen. Unstake in E0 -> active in E0,E1 -> inactive from E2 -> 3-epoch window
    // covers E2,E3,E4 with 0 stake -> return at E4->E5 boundary.
    let epoch_boundary = genesis_height + 5 * epoch_length;
    let restake_submit_height = epoch_boundary - 3;
    // The restake must land within endorsement_delay of the epoch boundary
    // to make sure the premise of the test holds.
    assert!(restake_submit_height + endorsement_delay > epoch_boundary);
    env.node_runner(unstaker_idx).run_until_head_height(restake_submit_height);

    // Submit re-stake near the end of E4. With endorsement delay, this tx will
    // land in an uncertified chunk, and its stake proposal should be picked up
    // by get_uncertified_validator_proposals at the epoch boundary.
    let restake_tx = env.node(unstaker_idx).tx_from_actions(
        &unstaker,
        &unstaker,
        vec![Action::Stake(Box::new(StakeAction {
            stake: initial_stake,
            public_key: unstaker_key,
        }))],
    );
    env.node(unstaker_idx).submit_tx(restake_tx);

    // Run past the epoch boundary, plus extra blocks for execution to certify.
    env.node_runner(unstaker_idx).run_until_head_height(epoch_boundary + endorsement_delay + 2);

    // Verify the re-stake proposal landed in an uncertified chunk at the epoch boundary.
    let client = env.node(unstaker_idx).client();
    let last_block_hash =
        client.chain.chain_store.get_block_hash_by_height(epoch_boundary - 1).unwrap();
    let uncertified_proposals = client
        .chain
        .spice_core_reader
        .get_uncertified_validator_proposals(&last_block_hash, ShardId::new(0))
        .unwrap();
    assert!(
        uncertified_proposals.iter().any(|p| p.account_id() == &unstaker),
        "re-stake proposal should be in uncertified chunks, got: {:?}",
        uncertified_proposals
    );

    // Verify that the unstaker's locked balance was NOT returned.
    let account = env.node(unstaker_idx).view_account_query(&unstaker).unwrap();
    assert_eq!(account.locked, initial_stake, "stake should NOT have been returned");
}

/// Checks that during the first epoch, total_supply matches total_supply in genesis.
/// Checks that during the second epoch, total_supply matches the expected inflation rate.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_inflation() {
    init_test_logger();

    let epoch_length: u64 = 10;
    let validators_spec = create_validators_spec(1, 0);
    let accounts = validators_spec_clients(&validators_spec);

    let max_inflation_rate = Rational32::new(1, 10);
    let protocol_reward_rate = Rational32::new(1, 10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .genesis_height(0)
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
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
        .build();

    // Check total_supply unchanged in epoch 1
    {
        let node = env.node(0);
        let client = node.client();
        let head = client.chain.head().unwrap();
        let block = client.chain.get_block(&head.last_block_hash).unwrap();
        assert_eq!(
            block.header().total_supply(),
            initial_total_supply,
            "total supply should be unchanged in epoch 1"
        );
    }

    // Advance to epoch 2
    env.node_runner(0).run_until_new_epoch();

    // Check total_supply matches expected inflation in epoch 2
    {
        let node = env.node(0);
        let client = node.client();
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
}

/// Verifies that `validator_reward_paid_prev_epoch` returned by the
/// validators RPC matches the per-account reward deltas from the on-chain
/// `ValidatorAccountsUpdate` state changes at the epoch boundary.
#[test]
fn test_validator_reward_in_get_validator_info() {
    init_test_logger();

    let validators_spec = create_validators_spec(2, 0);
    let accounts = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .protocol_reward_rate(Rational32::new(1, 10))
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(accounts.clone())
        .build();

    env.node_runner(0).run_until_new_epoch();
    let boundary_block = env.node(0).client().chain.get_head_block().unwrap();
    env.node_runner(0).run_until_new_epoch();

    let mut all_ids = accounts;
    all_ids.push("near".parse().unwrap()); // protocol treasury
    let boundary_hash = *boundary_block.hash();
    let prev_hash = *boundary_block.header().prev_hash();

    let mut node = env.node_mut(0);
    let view_client = node.view_client_actor();

    // Query pre-reward balances at the block before the epoch boundary.
    let mut pre_balances: HashMap<AccountId, Balance> = HashMap::new();
    for id in &all_ids {
        let query = Query::new(
            BlockReference::from(BlockId::Hash(prev_hash)),
            QueryRequest::ViewAccount { account_id: id.clone() },
        );
        if let Ok(response) = view_client.handle_query(query) {
            if let QueryResponseKind::ViewAccount(view) = response.kind {
                pre_balances.insert(id.clone(), view.locked.checked_add(view.amount).unwrap());
            }
        }
    }

    // Compute expected per-account rewards from ValidatorAccountsUpdate
    // state changes: reward = post_balance - pre_balance.
    let state_changes = view_client
        .handle(GetStateChanges {
            block_hash: boundary_hash,
            state_changes_request: StateChangesRequestView::AccountChanges { account_ids: all_ids },
        })
        .unwrap();
    let mut expected: HashMap<AccountId, Balance> = HashMap::new();
    for change in &state_changes {
        if matches!(&change.cause, StateChangeCauseView::ValidatorAccountsUpdate) {
            if let StateChangeValueView::AccountUpdate { account_id, account } = &change.value {
                let post = account.locked.checked_add(account.amount).unwrap();
                let pre = pre_balances.get(account_id).copied().unwrap_or(Balance::ZERO);
                expected.insert(account_id.clone(), post.checked_sub(pre).unwrap());
            }
        }
    }
    assert!(!expected.is_empty(), "should find ValidatorAccountsUpdate changes");

    let info =
        view_client.handle(GetValidatorInfo { epoch_reference: EpochReference::Latest }).unwrap();
    assert_eq!(info.validator_reward_paid_prev_epoch, expected);
}
