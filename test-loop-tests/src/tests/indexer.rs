use near_async::futures::FutureSpawnerExt;
use near_async::time::Duration;
use near_indexer::{AwaitForNodeSyncedEnum, IndexerConfig, SyncModeEnum, start};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::Finality;
use near_store::StoreConfig;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::state::NodeExecutionData;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::node::TestLoopNode;

#[test]
fn test_indexer_basic() {
    init_test_logger();

    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(ShardLayout::single_shard())
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let node_data = &env.node_datas[0];
    let node = TestLoopNode::for_account(&env.node_datas, &node_data.account_id);
    let start_block_height = 1;
    let indexer_config = IndexerConfig {
        home_dir: NodeExecutionData::homedir(&env.shared_state.tempdir, &node_data.identifier),
        sync_mode: SyncModeEnum::BlockHeight(start_block_height),
        await_for_node_synced: AwaitForNodeSyncedEnum::StreamWhileSyncing,
        finality: Finality::None,
        validate_genesis: false,
    };

    let shard_tracker = node.client(env.test_loop_data()).shard_tracker.clone();
    let store_config =
        StoreConfig { path: Some(indexer_config.home_dir.clone()), ..Default::default() };
    let (sender, mut receiver) = tokio::sync::mpsc::channel(100);
    let future = start(
        node_data.view_client_sender.clone().into(),
        node_data.client_sender.clone().into(),
        shard_tracker,
        indexer_config,
        store_config,
        sender,
        env.test_loop.clock(),
    );
    env.test_loop.future_spawner("Indexer").spawn("main indexer loop", future);
    let last_block_height = 10;
    let mut block_heights_received = vec![];
    env.test_loop.run_until(
        |_| {
            if let Ok(msg) = receiver.try_recv() {
                let height = msg.block.header.height;
                block_heights_received.push(height);
                height == last_block_height
            } else {
                false
            }
        },
        Duration::seconds(20),
    );

    assert_eq!(
        block_heights_received,
        (start_block_height..=last_block_height).collect::<Vec<_>>()
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
