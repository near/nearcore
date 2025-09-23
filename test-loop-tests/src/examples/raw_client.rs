use near_async::messaging::{IntoMultiSender, IntoSender, LateBoundSender, noop};
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain::ChainGenesis;
use near_chain::spice_core::CoreStatementsProcessor;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_chain_configs::test_utils::TestClientConfigParams;
use near_chain_configs::{ClientConfig, MutableConfigValue, TrackedShardsConfig};
use near_chunks::shards_manager_actor::ShardsManagerActor;
use near_client::client_actor::ClientActorInner;
use near_client::sync_jobs_actor::SyncJobsActor;
use near_client::{AsyncComputationMultiSpawner, Client};
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_o11y::testonly::init_test_logger;
use near_primitives::network::PeerId;

use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_test_signer;
use near_primitives::version::get_protocol_upgrade_schedule;
use near_store::adapter::StoreAdapter;

use crate::utils::account::{create_account_ids, create_validators_spec, validators_spec_clients};
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use nearcore::NightshadeRuntime;
use std::path::Path;
use std::sync::Arc;

/// min block production time in milliseconds
pub const MIN_BLOCK_PROD_TIME: Duration = Duration::milliseconds(100);
/// max block production time in milliseconds
pub const MAX_BLOCK_PROD_TIME: Duration = Duration::milliseconds(200);

// Demonstrates raw set up for single Client to be used with test loop
#[test]
fn test_raw_client_test_loop_setup() {
    init_test_logger();
    let mut test_loop = TestLoopV2::new();

    let client_config = ClientConfig::test(TestClientConfigParams {
        skip_sync_wait: true,
        min_block_prod_time: MIN_BLOCK_PROD_TIME.whole_milliseconds() as u64,
        max_block_prod_time: MAX_BLOCK_PROD_TIME.whole_milliseconds() as u64,
        num_block_producer_seats: 4,
        archive: false,
        save_trie_changes: true,
        state_sync_enabled: false,
    });

    let validators_spec = create_validators_spec(1, 0);
    let validator_id = validators_spec_clients(&validators_spec).remove(0);

    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&test_loop.clock())
        .shard_layout(ShardLayout::multi_shard_custom(create_account_ids(["account1"]).to_vec(), 1))
        .validators_spec(validators_spec)
        .build();
    let store = create_test_store();
    initialize_genesis_state(store.clone(), &genesis, None);

    let chain_genesis = ChainGenesis::new(&genesis.config);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime_adapter = NightshadeRuntime::test(
        Path::new("."),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );
    let validator_signer = MutableConfigValue::new(
        Some(Arc::new(create_test_signer(validator_id.as_str()))),
        "validator_signer",
    );
    let shard_tracker = ShardTracker::new(
        TrackedShardsConfig::AllShards,
        epoch_manager.clone(),
        validator_signer.clone(),
    );

    let shards_manager_adapter = LateBoundSender::new();
    let sync_jobs_adapter = LateBoundSender::new();
    let client_adapter = LateBoundSender::new();

    let sync_jobs_actor = SyncJobsActor::new(client_adapter.as_multi_sender());

    let protocol_upgrade_schedule = get_protocol_upgrade_schedule(&chain_genesis.chain_id);
    let multi_spawner = AsyncComputationMultiSpawner::all_custom(Arc::new(
        test_loop.async_computation_spawner("node0", |_| Duration::milliseconds(80)),
    ));
    let client = Client::new(
        test_loop.clock(),
        client_config,
        chain_genesis,
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime_adapter,
        noop().into_multi_sender(),
        shards_manager_adapter.as_sender(),
        validator_signer.clone(),
        true,
        [0; 32],
        None,
        multi_spawner,
        noop().into_multi_sender(),
        noop().into_multi_sender(),
        Arc::new(test_loop.future_spawner("node0")),
        noop().into_multi_sender(),
        client_adapter.as_multi_sender(),
        noop().into_multi_sender(),
        protocol_upgrade_schedule,
        CoreStatementsProcessor::new_with_noop_senders(store.chain_store(), epoch_manager.clone()),
    )
    .unwrap();

    let head = client.chain.head().unwrap();
    let header_head = client.chain.header_head().unwrap();
    let shards_manager = ShardsManagerActor::new(
        test_loop.clock(),
        validator_signer,
        epoch_manager.clone(),
        epoch_manager,
        shard_tracker,
        noop().into_sender(),
        client_adapter.as_sender(),
        store.chunk_store(),
        <_>::clone(&head),
        <_>::clone(&header_head),
        Duration::milliseconds(100),
    );

    let client_actor = ClientActorInner::new(
        test_loop.clock(),
        client,
        PeerId::random(),
        noop().into_multi_sender(),
        noop().into_sender(),
        None,
        Default::default(),
        None,
        sync_jobs_adapter.as_multi_sender(),
        noop().into_sender(),
        noop().into_sender(),
        noop().into_sender(),
    )
    .unwrap();

    test_loop.data.register_actor("node0", sync_jobs_actor, Some(sync_jobs_adapter));
    test_loop.data.register_actor("node0", shards_manager, Some(shards_manager_adapter));
    test_loop.data.register_actor("node0", client_actor, Some(client_adapter));

    test_loop.run_for(Duration::seconds(10));
    test_loop.shutdown_and_drain_remaining_events(Duration::seconds(1));
}
