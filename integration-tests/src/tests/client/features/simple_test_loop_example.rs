use near_async::messaging::{noop, IntoMultiSender, IntoSender, LateBoundSender};
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain::chunks_store::ReadOnlyChunksStore;
use near_chain::ChainGenesis;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_chain_configs::{ClientConfig, MutableConfigValue};
use near_chunks::shards_manager_actor::ShardsManagerActor;
use near_client::client_actor::ClientActorInner;
use near_client::sync_jobs_actor::SyncJobsActor;
use near_client::test_utils::{MAX_BLOCK_PROD_TIME, MIN_BLOCK_PROD_TIME};
use near_client::{Client, SyncAdapter};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_o11y::testonly::init_test_logger;
use near_primitives::network::PeerId;

use near_primitives::test_utils::create_test_signer;
use near_primitives::types::AccountId;

use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use nearcore::NightshadeRuntime;
use std::path::Path;
use std::sync::{Arc, RwLock};

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

#[test]
fn test_client_with_simple_test_loop() {
    init_test_logger();
    let mut test_loop = TestLoopV2::new();

    let client_config = ClientConfig::test(
        true,
        MIN_BLOCK_PROD_TIME.whole_milliseconds() as u64,
        MAX_BLOCK_PROD_TIME.whole_milliseconds() as u64,
        4,
        false,
        true,
        false,
        false,
    );
    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&test_loop.clock())
        .protocol_version_latest()
        .genesis_height(10000)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        .transaction_validity_period(1000)
        .epoch_length(10)
        .validators_desired_roles(&["account0"], &[])
        .shuffle_shard_assignment_for_chunk_producers(true);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let store = create_test_store();
    initialize_genesis_state(store.clone(), &genesis, None);

    let chain_genesis = ChainGenesis::new(&genesis.config);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
    let shard_tracker = ShardTracker::new(TrackedConfig::AllShards, epoch_manager.clone());
    let runtime_adapter = NightshadeRuntime::test(
        Path::new("."),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );
    let validator_signer = MutableConfigValue::new(
        Some(Arc::new(create_test_signer(accounts[0].as_str()))),
        "validator_signer",
    );

    let shards_manager_adapter = LateBoundSender::new();
    let sync_jobs_adapter = LateBoundSender::new();
    let client_adapter = LateBoundSender::new();

    let sync_jobs_actor = SyncJobsActor::new(client_adapter.as_multi_sender());

    let state_sync_adapter = Arc::new(RwLock::new(SyncAdapter::new(
        client_adapter.as_sender(),
        noop().into_sender(),
        SyncAdapter::actix_actor_maker(),
    )));

    let client = Client::new(
        test_loop.clock(),
        client_config.clone(),
        chain_genesis,
        epoch_manager.clone(),
        shard_tracker.clone(),
        state_sync_adapter,
        runtime_adapter,
        noop().into_multi_sender(),
        shards_manager_adapter.as_sender(),
        validator_signer.clone(),
        true,
        [0; 32],
        None,
        Arc::new(test_loop.async_computation_spawner(|_| Duration::milliseconds(80))),
        noop().into_multi_sender(),
    )
    .unwrap();

    let shards_manager = ShardsManagerActor::new(
        test_loop.clock(),
        validator_signer,
        epoch_manager,
        shard_tracker,
        noop().into_sender(),
        client_adapter.as_sender(),
        ReadOnlyChunksStore::new(store),
        client.chain.head().unwrap(),
        client.chain.header_head().unwrap(),
        Duration::milliseconds(100),
    );

    let client_actor = ClientActorInner::new(
        test_loop.clock(),
        client,
        client_adapter.as_multi_sender(),
        client_config,
        PeerId::random(),
        noop().into_multi_sender(),
        noop().into_sender(),
        None,
        Default::default(),
        None,
        sync_jobs_adapter.as_multi_sender(),
        Box::new(test_loop.future_spawner()),
    )
    .unwrap();

    test_loop.register_actor(sync_jobs_actor, Some(sync_jobs_adapter));
    test_loop.register_actor(shards_manager, Some(shards_manager_adapter));
    test_loop.register_actor(client_actor, Some(client_adapter));

    test_loop.run_for(Duration::seconds(10));
    test_loop.shutdown_and_drain_remaining_events(Duration::seconds(1));
}
