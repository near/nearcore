use crate::setup::builder::TestLoopBuilder;
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::execute_money_transfers;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance};

/// Runs chain with sequence of chunks with empty state changes, long enough to
/// cover 5 epochs which is default GC period.
/// After that, it checks that memtrie for the shard can be loaded.
/// This is a repro for #11583 where flat storage head was not moved at all at
/// this scenario, so chain data related to that block was garbage collected,
/// and loading memtrie failed because of missing `ChunkExtra` with desired
/// state root.
#[test]
fn test_load_memtrie_after_empty_chunks() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let num_accounts = 3;
    let num_clients = 2;
    let epoch_length = 5;
    // Set 2 shards, first of which doesn't have any validators.
    let boundary_accounts = ["account1"].iter().map(|a| a.parse().unwrap()).collect();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
    let accounts = (num_accounts - num_clients..num_accounts)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    let client_accounts = accounts.iter().take(num_clients).cloned().collect_vec();
    let validators_spec = ValidatorsSpec::desired_roles(
        &client_accounts.iter().map(|t| t.as_str()).collect_vec(),
        &[],
    );

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .genesis_height(10000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(client_accounts)
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();

    // Make sure the chain progresses for several epochs.
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().height
                > 10000 + epoch_length * 10
        },
        Duration::seconds(10),
    );

    // Find client currently tracking shard with index 0.
    let shard_uid = shard_layout.shard_uids().next().unwrap();
    let shard_id = shard_uid.shard_id();
    let tracked_shards_per_node = env
        .node_datas
        .iter()
        .map(|node_data| TestLoopNode { data: &env.test_loop.data, node_data }.tracked_shards())
        .collect_vec();
    tracing::info!(?tracked_shards_per_node, "current tracked shards");
    let idx = tracked_shards_per_node
        .iter()
        .enumerate()
        .find_map(|(idx, shards)| if shards.contains(&shard_id) { Some(idx) } else { None })
        .expect("Not found any client tracking shard 0");

    // Unload memtrie and load it back, check that it doesn't panic.
    let client =
        TestLoopNode { data: &env.test_loop.data, node_data: &env.node_datas[idx] }.client();
    client.runtime_adapter.get_tries().unload_memtrie(&shard_uid);
    client
        .runtime_adapter
        .get_tries()
        .load_memtrie(&shard_uid, None, true)
        .expect("Couldn't load memtrie");
}
