/// Test economic edge cases.
use std::path::Path;
use std::sync::Arc;

use num_rational::Rational;

use near_chain::{ChainGenesis, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_logger_utils::init_integration_logger;
use near_primitives::transaction::SignedTransaction;
use near_store::test_utils::create_test_store;
use neard::config::GenesisExt;
use testlib::fees_utils::FeeHelper;

use primitive_types::U256;

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
        None,
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
    let epoch_total_reward = {
        let block0 = env.clients[0].chain.get_block_by_height(0).unwrap().clone();
        let block2 = env.clients[0].chain.get_block_by_height(2).unwrap().clone();
        let duration = block2.header().raw_timestamp() - block0.header().raw_timestamp();
        (U256::from(initial_total_supply) * U256::from(duration)
            / U256::from(10u128.pow(9) * 24 * 60 * 60 * 365 * 10))
        .as_u128()
    };
    assert_eq!(
        block3.header().total_supply(),
        // supply + 1% of protocol rewards + 3/4 * 9% of validator rewards.
        initial_total_supply + epoch_total_reward * 775 / 1000 - half_transfer_cost
    );
    assert_eq!(block3.chunks()[0].balance_burnt(), half_transfer_cost);
    // Block 4: subtract 2nd part of transfer.
    let block4 = env.clients[0].chain.get_block_by_height(4).unwrap().clone();
    assert_eq!(block4.header().total_supply(), block3.header().total_supply() - half_transfer_cost);
    assert_eq!(block4.chunks()[0].balance_burnt(), half_transfer_cost);
    // Check that Protocol Treasury account got it's 1% as well.
    assert_eq!(env.query_balance("near".to_string()), near_balance + epoch_total_reward / 10);
    // Block 5: reward from previous block.
    let block5 = env.clients[0].chain.get_block_by_height(5).unwrap().clone();
    let prev_total_supply = block4.header().total_supply();
    let block2 = env.clients[0].chain.get_block_by_height(2).unwrap().clone();
    let epoch_total_reward = (U256::from(prev_total_supply)
        * U256::from(block4.header().raw_timestamp() - block2.header().raw_timestamp())
        / U256::from(10u128.pow(9) * 24 * 60 * 60 * 365 * 10))
    .as_u128();
    assert_eq!(block5.header().total_supply(), prev_total_supply + epoch_total_reward);
}
