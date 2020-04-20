use std::path::Path;
use std::sync::Arc;

use near_chain::{ChainGenesis, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::test_utils::init_integration_logger;
use near_primitives::transaction::SignedTransaction;
use near_store::test_utils::create_test_store;
use neard::config::GenesisExt;
use num_rational::Rational;
use testlib::fees_utils::FeeHelper;

/// Test that node mints and burns tokens correctly as blocks become full.
/// This combines Client & NightshadeRuntime to also test EpochManager.
#[test]
fn test_burn_mint() {
    init_integration_logger();
    let store1 = create_test_store();
    let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
    genesis.config.epoch_length = 2;
    genesis.config.num_blocks_per_year = 2;
    genesis.config.protocol_reward_rate = Rational::new_raw(1, 10);
    genesis.config.max_inflation_rate = Rational::new_raw(1, 10);
    let initial_total_supply = genesis.config.total_supply;
    let fee_helper = FeeHelper::new(
        genesis.config.runtime_config.transaction_costs.clone(),
        genesis.config.min_gas_price,
    );
    let arc_genesis = Arc::new(genesis);
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        store1,
        arc_genesis.clone(),
        vec![],
        vec![],
    ))];
    let mut env = TestEnv::new_with_runtime(ChainGenesis::from(&arc_genesis), 1, 1, runtimes);
    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");
    let genesis_hash = env.clients[0].chain.genesis().hash();
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
    );
    let near_balance = env.query_balance("near".to_string());
    env.produce_block(0, 1);
    env.produce_block(0, 2);
    env.produce_block(0, 3);
    env.produce_block(0, 4);

    // Block 3: epoch ends, it gets it's 10% of total supply - transfer cost.
    let block3 = env.clients[0].chain.get_block_by_height(3).unwrap().clone();
    // We burn half of the cost when tx executed and the other half in the next block for the receipt processing.
    let half_transfer_cost = fee_helper.transfer_cost() / 2;
    assert_eq!(
        block3.header.inner_rest.total_supply,
        initial_total_supply * 110 / 100 - half_transfer_cost
    );
    assert_eq!(block3.chunks[0].inner.balance_burnt, half_transfer_cost);
    // Block 4: subtract 2nd part of transfer.
    let block4 = env.clients[0].chain.get_block_by_height(4).unwrap().clone();
    assert_eq!(
        block4.header.inner_rest.total_supply,
        block3.header.inner_rest.total_supply - half_transfer_cost
    );
    assert_eq!(block4.chunks[0].inner.balance_burnt, half_transfer_cost);
    // Check that PROTOCOL account got it's 1% as well.
    assert_eq!(
        env.query_balance("near".to_string()),
        near_balance + initial_total_supply * 1 / 100
    );
}
