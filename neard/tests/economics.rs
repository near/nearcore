/// Test economic edge cases.
use std::path::Path;
use std::sync::Arc;

use num_rational::Rational;

use near_chain::{ChainGenesis, Provenance, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_logger_utils::init_integration_logger;
use near_primitives::transaction::SignedTransaction;
use near_primitives::version::ENABLE_INFLATION_PROTOCOL_VERSION;
use near_store::test_utils::create_test_store;
use neard::config::{GenesisExt, TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use testlib::fees_utils::FeeHelper;

fn setup_env(f: &mut dyn FnMut(&mut Genesis) -> ()) -> (TestEnv, FeeHelper) {
    init_integration_logger();
    let store1 = create_test_store();
    let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
    f(&mut genesis);
    let fee_helper = FeeHelper::new(
        genesis.config.runtime_config.transaction_costs.clone(),
        genesis.config.min_gas_price,
    );
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        store1,
        &genesis,
        vec![],
        vec![],
    ))];
    let env = TestEnv::new_with_runtime(ChainGenesis::from(&genesis), 1, 1, runtimes);
    (env, fee_helper)
}

/// Debug tool to show current balances.
#[allow(dead_code)]
fn print_accounts(env: &mut TestEnv) {
    println!(
        "{:?}",
        ["near", "test0", "test1"]
            .iter()
            .map(|account_id| {
                let account = env.query_account(account_id.to_string());
                (account_id, account.amount, account.locked)
            })
            .collect::<Vec<_>>()
    );
}

fn calc_total_supply(env: &mut TestEnv) -> u128 {
    ["near", "test0", "test1"]
        .iter()
        .map(|account_id| {
            let account = env.query_account(account_id.to_string());
            account.amount + account.locked
        })
        .sum()
}

/// Test that node mints and burns tokens correctly with fees and epoch rewards.
/// This combines Client & NightshadeRuntime to also test EpochManager.
#[test]
fn test_burn_mint() {
    let (mut env, fee_helper) = setup_env(&mut |mut genesis| {
        genesis.config.epoch_length = 2;
        genesis.config.num_blocks_per_year = 2;
        genesis.config.protocol_reward_rate = Rational::new_raw(1, 10);
        genesis.config.max_inflation_rate = Rational::new_raw(1, 10);
        genesis.config.chunk_producer_kickout_threshold = 30;
        genesis.config.online_min_threshold = Rational::new_raw(0, 1);
        genesis.config.online_max_threshold = Rational::new_raw(1, 1);
    });
    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");
    let initial_total_supply = env.chain_genesis.total_supply;
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    env.clients[0].process_tx(
        SignedTransaction::send_money(
            1,
            "test0".to_string(),
            "test1".to_string(),
            &signer,
            1000,
            genesis_hash,
        ),
        false,
        false,
    );
    let near_balance = env.query_balance("near".to_string());
    assert_eq!(calc_total_supply(&mut env), initial_total_supply);
    for i in 1..6 {
        env.produce_block(0, i);
        // TODO: include receipts into total supply calculations and check with total supply in the next block?
        //        print_accounts(&mut env);
        //        assert_eq!(
        //            calc_total_supply(&mut env),
        //            env.clients[0].chain.get_block_by_height(i + 1).unwrap().header.total_supply()
        //        );
    }

    // Block 3: epoch ends, it gets it's 10% of total supply - transfer cost.
    let block3 = env.clients[0].chain.get_block_by_height(3).unwrap().clone();
    // We burn half of the cost when tx executed and the other half in the next block for the receipt processing.
    let half_transfer_cost = fee_helper.transfer_cost() / 2;
    assert_eq!(
        block3.header().total_supply(),
        // supply + 1% of protocol rewards + 3/4 * 9% of validator rewards.
        initial_total_supply * 10775 / 10000 - half_transfer_cost
    );
    assert_eq!(block3.chunks()[0].balance_burnt(), half_transfer_cost);
    // Block 4: subtract 2nd part of transfer.
    let block4 = env.clients[0].chain.get_block_by_height(4).unwrap().clone();
    assert_eq!(block4.header().total_supply(), block3.header().total_supply() - half_transfer_cost);
    assert_eq!(block4.chunks()[0].balance_burnt(), half_transfer_cost);
    // Check that Protocol Treasury account got it's 1% as well.
    assert_eq!(
        env.query_balance("near".to_string()),
        near_balance + initial_total_supply * 1 / 100
    );
    // Block 5: reward from previous block.
    let block5 = env.clients[0].chain.get_block_by_height(5).unwrap().clone();
    assert_eq!(
        block5.header().total_supply(),
        // previous supply + 10%
        block4.header().total_supply() * 110 / 100
    );
}

#[test]
fn test_enable_inflation() {
    let epoch_length = 10;
    let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.max_inflation_rate = Rational::from_integer(0);
    genesis.config.protocol_reward_rate = Rational::from_integer(0);
    genesis.config.protocol_version = ENABLE_INFLATION_PROTOCOL_VERSION - 1;
    let chain_genesis = ChainGenesis::from(&genesis);
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        create_test_store(),
        &genesis,
        vec![],
        vec![],
    ))];
    let mut env = TestEnv::new_with_runtime(chain_genesis, 1, 1, runtimes);
    // initially there is no inflation
    for i in 1..31 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::NONE);
        assert_eq!(block.header().total_supply(), genesis.config.total_supply);
    }

    let test_account = env.query_account("test0".to_string());
    assert_eq!(test_account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(test_account.locked, TESTING_INIT_STAKE);
    for i in 31..33 {
        env.produce_block(0, i);
    }
    let test_account = env.query_account("test0".to_string());
    let expected_max_inflation_rate = Rational::new_raw(1, 20);
    let expected_epoch_total_reward = *expected_max_inflation_rate.numer() as u128
        * genesis.config.total_supply
        * u128::from(genesis.config.epoch_length)
        / (genesis.config.num_blocks_per_year as u128
            * *expected_max_inflation_rate.denom() as u128);
    let expected_epoch_treasury = expected_epoch_total_reward / 10;
    let expected_account_reward = expected_epoch_total_reward - expected_epoch_treasury;
    assert_eq!(test_account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(test_account.locked, TESTING_INIT_STAKE + expected_account_reward);
    let treasury_account = env.query_account("near".to_string());
    assert_eq!(treasury_account.amount, TESTING_INIT_BALANCE + expected_epoch_treasury);
    let last_block_header = env.clients[0].chain.head_header().unwrap();
    assert_eq!(
        last_block_header.total_supply(),
        genesis.config.total_supply + expected_epoch_total_reward
    );
}
