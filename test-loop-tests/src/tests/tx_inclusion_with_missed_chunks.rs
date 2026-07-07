use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::utils::run_for_number_of_blocks;
use crate::utils::transactions::{TransactionRunner, execute_tx};
use assert_matches::assert_matches;
use itertools::Itertools;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfig;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockHeightDelta};
use near_primitives::views::FinalExecutionStatus;

/// Regression test for the active `max_congestion_missed_chunks = 125` runtime
/// parameter: a tx targeting a shard with several consecutive missed chunks
/// must still be accepted, since the missed-chunk count stays well below the
/// threshold..
#[test]
// N/A under spice: missing chunks are applied as empty new chunks, so the
// missed-chunk threshold being tested here never accrues.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_tx_inclusion_with_missed_chunks() {
    init_test_logger();

    let epoch_length: BlockHeightDelta = 20;
    let num_missed_chunks: usize = 10;

    // 2 producers, 2 validators, 1 rpc node, 4 shards, 20 accounts.
    let num_clients = 5;
    let num_producers = 2;
    let num_validators = 2;
    let num_rpc = 1;
    let accounts =
        (0..20).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let initial_balance = Balance::from_near(10000);
    let clients = accounts.iter().take(num_clients).cloned().collect_vec();

    let boundary_accounts = ["account3", "account5", "account7"];
    let boundary_accounts = boundary_accounts.iter().map(|a| a.parse().unwrap()).collect();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);

    // split the clients into producers, validators, and rpc nodes
    let tmp = clients.clone();
    let (producers, tmp) = tmp.split_at(num_producers);
    let (validators, tmp) = tmp.split_at(num_validators);
    let (rpcs, tmp) = tmp.split_at(num_rpc);
    assert!(tmp.is_empty());

    let producers = producers.iter().map(|account| account.as_str()).collect_vec();
    let validators = validators.iter().map(|account| account.as_str()).collect_vec();
    let validators_spec = ValidatorsSpec::desired_roles(&producers, &validators);
    let [rpc_id] = rpcs else { panic!("Expected exactly one rpc node") };

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, initial_balance)
        .genesis_height(10000)
        .build();

    // Sanity-check: the active threshold is large enough that
    // `num_missed_chunks` consecutive misses won't mark the shard stuck.
    let max_missed = RuntimeConfig::test().congestion_control_config.max_congestion_missed_chunks;
    assert!(num_missed_chunks < max_missed as usize);

    let env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build();

    // account4 lives in the second shard (boundary "account3" separates 0–2 from 4+).
    let shard_layout = &env.shared_state.genesis.config.shard_layout;
    let target_shard_id = shard_layout.account_id_to_shard_id(&"account4".parse().unwrap());

    // Drop `num_missed_chunks` consecutive chunks for the target shard.
    let drop_map = (0..epoch_length).map(|h| (h as usize) >= num_missed_chunks).collect_vec();
    let dropped = [(target_shard_id, drop_map)].into_iter().collect();
    let mut env = env.drop(DropCondition::ChunksProducedByHeight(dropped));

    run_for_number_of_blocks(&mut env, rpc_id, num_missed_chunks + 2);

    // Send a tx targeting the stuck shard; it should be accepted because
    // 10 missed chunks is well below the 125 threshold.
    let tx = env.node_for_account(rpc_id).tx_send_money(
        &"account0".parse().unwrap(),
        &"account4".parse().unwrap(),
        Balance::from_near(1),
    );
    let client_actor = env.test_loop.data.get(&env.node_datas[0].client_sender.actor_handle());
    let block_time = client_actor.client.config.max_block_production_delay.get();
    let tx_outcome = execute_tx(
        &mut env.test_loop,
        rpc_id,
        TransactionRunner::new(tx, false),
        &env.node_datas,
        block_time * 5,
    )
    .unwrap();
    assert_matches!(tx_outcome.status, FinalExecutionStatus::SuccessValue(_));
}
