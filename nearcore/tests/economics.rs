/// Test economic edge cases.
use std::path::Path;

use near_client::ProcessTxResponse;
use near_epoch_manager::EpochManager;
use num_rational::Ratio;

use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_integration_logger;
use near_primitives::transaction::SignedTransaction;
use near_store::{genesis::initialize_genesis_state, test_utils::create_test_store};
use nearcore::{config::GenesisExt, NightshadeRuntime};
use testlib::fees_utils::FeeHelper;

use near_primitives::types::EpochId;
use primitive_types::U256;

fn build_genesis() -> Genesis {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = 2;
    genesis.config.num_blocks_per_year = 2;
    genesis.config.protocol_reward_rate = Ratio::new_raw(1, 10);
    genesis.config.max_inflation_rate = Ratio::new_raw(1, 10);
    genesis.config.chunk_producer_kickout_threshold = 30;
    genesis.config.online_min_threshold = Ratio::new_raw(0, 1);
    genesis.config.online_max_threshold = Ratio::new_raw(1, 1);
    genesis
}

fn setup_env(genesis: &Genesis) -> TestEnv {
    init_integration_logger();
    let store = create_test_store();
    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
    let runtime = NightshadeRuntime::test(
        Path::new("."),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );
    TestEnv::builder(ChainGenesis::new(&genesis))
        .stores(vec![store])
        .epoch_managers(vec![epoch_manager])
        .runtimes(vec![runtime])
        .build()
}

fn calc_total_supply(env: &mut TestEnv) -> u128 {
    ["near", "test0", "test1"]
        .iter()
        .map(|account_id| {
            let account = env.query_account(account_id.parse().unwrap());
            account.amount + account.locked
        })
        .sum()
}

/// Test that node mints and burns tokens correctly with fees and epoch rewards.
/// This combines Client & NightshadeRuntime to also test EpochManager.
#[test]
fn test_burn_mint() {
    let genesis = build_genesis();
    let mut env = setup_env(&genesis);
    let transaction_costs = env.clients[0]
        .runtime_adapter
        .get_protocol_config(&EpochId::default())
        .unwrap()
        .runtime_config
        .fees;
    let fee_helper = FeeHelper::new(transaction_costs, genesis.config.min_gas_price);
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let initial_total_supply = env.chain_genesis.total_supply;
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    assert_eq!(
        env.clients[0].process_tx(
            SignedTransaction::send_money(
                1,
                "test0".parse().unwrap(),
                "test1".parse().unwrap(),
                &signer,
                1000,
                genesis_hash,
            ),
            false,
            false,
        ),
        ProcessTxResponse::ValidTx
    );
    let near_balance = env.query_balance("near".parse().unwrap());
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
    let block3 = env.clients[0].chain.get_block_by_height(3).unwrap();
    // We burn half of the cost when tx executed and the other half in the next block for the receipt processing.
    let half_transfer_cost = fee_helper.transfer_cost() / 2;
    let epoch_total_reward = {
        let block0 = env.clients[0].chain.get_block_by_height(0).unwrap();
        let block2 = env.clients[0].chain.get_block_by_height(2).unwrap();
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
    let block4 = env.clients[0].chain.get_block_by_height(4).unwrap();
    assert_eq!(block4.header().total_supply(), block3.header().total_supply() - half_transfer_cost);
    assert_eq!(block4.chunks()[0].balance_burnt(), half_transfer_cost);
    // Check that Protocol Treasury account got it's 1% as well.
    assert_eq!(env.query_balance("near".parse().unwrap()), near_balance + epoch_total_reward / 10);
    // Block 5: reward from previous block.
    let block5 = env.clients[0].chain.get_block_by_height(5).unwrap();
    let prev_total_supply = block4.header().total_supply();
    let block2 = env.clients[0].chain.get_block_by_height(2).unwrap();
    let epoch_total_reward = (U256::from(prev_total_supply)
        * U256::from(block4.header().raw_timestamp() - block2.header().raw_timestamp())
        / U256::from(10u128.pow(9) * 24 * 60 * 60 * 365 * 10))
    .as_u128();
    assert_eq!(block5.header().total_supply(), prev_total_supply + epoch_total_reward);
}
