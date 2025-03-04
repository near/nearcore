use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use near_async::messaging::{IntoMultiSender, IntoSender, LateBoundSender, noop};
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain::ChainGenesis;
use near_chain::resharding::resharding_actor::ReshardingActor;
use near_chain::state_snapshot_actor::{
    SnapshotCallbacks, StateSnapshotActor, get_delete_snapshot_callback, get_make_snapshot_callback,
};
use near_chain::types::RuntimeAdapter;
use near_chain_configs::{ClientConfig, Genesis, MutableConfigValue};
use near_chunks::shards_manager_actor::ShardsManagerActor;
use near_client::client_actor::ClientActorInner;
use near_client::gc_actor::GCActor;
use near_client::sync_jobs_actor::SyncJobsActor;
use near_client::{Client, PartialWitnessActor, ViewClientActorInner};
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_network::test_loop::{TestLoopNetworkSharedState, TestLoopPeerManagerActor};
use near_parameters::RuntimeConfigStore;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::network::PeerId;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::AccountId;
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_store::adapter::StoreAdapter;
use near_store::config::StateSnapshotType;
use near_store::{Store, StoreConfig, TrieConfig};
use near_vm_runner::{ContractRuntimeCache, FilesystemContractRuntimeCache};
use nearcore::NightshadeRuntime;
use nearcore::state_sync::StateSyncDumper;

use crate::setup::env::{ClientToShardsManagerSender, TestData, TestLoopChunksStorage};

pub struct NodeHandle {
    account_id: AccountId,
    client_config: ClientConfig,
    store: Store,
    split_store: Option<Store>,
    homedir: PathBuf,
}

pub struct SharedState {
    genesis: Genesis,
    epoch_config_store: EpochConfigStore,
    runtime_config_store: Option<RuntimeConfigStore>,
    network_shared_state: TestLoopNetworkSharedState,
    upgrade_schedule: ProtocolUpgradeVotingSchedule,
    chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
    load_memtries_for_tracked_shards: bool,
}

pub fn setup_client(
    identifier: &str,
    test_loop: &mut TestLoopV2,
    node_handle: NodeHandle,
    shared_state: &SharedState,
) -> TestData {
    let NodeHandle { account_id, client_config, store, split_store, homedir } = node_handle;
    let SharedState {
        genesis,
        epoch_config_store,
        runtime_config_store,
        network_shared_state,
        upgrade_schedule,
        chunks_storage,
        load_memtries_for_tracked_shards,
    } = shared_state;

    let client_adapter = LateBoundSender::new();
    let network_adapter = LateBoundSender::new();
    let state_snapshot_adapter = LateBoundSender::new();
    let partial_witness_adapter = LateBoundSender::new();
    let sync_jobs_adapter = LateBoundSender::new();
    let resharding_sender = LateBoundSender::new();

    let store_config = StoreConfig {
        path: Some(homedir.clone()),
        load_memtries_for_tracked_shards: *load_memtries_for_tracked_shards,
        ..Default::default()
    };

    let sync_jobs_actor = SyncJobsActor::new(client_adapter.as_multi_sender());
    let chain_genesis = ChainGenesis::new(&genesis.config);
    let epoch_manager = EpochManager::new_arc_handle_from_epoch_config_store(
        store.clone(),
        &genesis.config,
        epoch_config_store.clone(),
    );
    let shard_tracker =
        ShardTracker::new(TrackedConfig::from_config(&client_config), epoch_manager.clone());

    let contract_cache = FilesystemContractRuntimeCache::test().expect("filesystem contract cache");
    let runtime_adapter = NightshadeRuntime::test_with_trie_config(
        &homedir,
        store.clone(),
        ContractRuntimeCache::handle(&contract_cache),
        &genesis.config,
        epoch_manager.clone(),
        runtime_config_store.clone(),
        TrieConfig::from_store_config(&store_config),
        StateSnapshotType::EveryEpoch,
        client_config.gc.gc_num_epochs_to_keep,
    );

    let state_snapshot = StateSnapshotActor::new(
        runtime_adapter.get_flat_storage_manager(),
        network_adapter.as_multi_sender(),
        runtime_adapter.get_tries(),
    );

    let delete_snapshot_callback =
        get_delete_snapshot_callback(state_snapshot_adapter.as_multi_sender());
    let make_snapshot_callback = get_make_snapshot_callback(
        state_snapshot_adapter.as_multi_sender(),
        runtime_adapter.get_flat_storage_manager(),
    );
    let snapshot_callbacks = SnapshotCallbacks { make_snapshot_callback, delete_snapshot_callback };

    let validator_signer =
        MutableConfigValue::new(Some(Arc::new(create_test_signer(identifier))), "validator_signer");

    let shards_manager_adapter = LateBoundSender::new();
    let client_to_shards_manager_sender = Arc::new(ClientToShardsManagerSender {
        sender: shards_manager_adapter.clone(),
        chunks_storage: chunks_storage.clone(),
    });

    // Generate a PeerId. It doesn't matter what this is. We're just making it based on
    // the account ID, so that it is stable across multiple runs in the same test.
    let peer_id = PeerId::new(create_test_signer(identifier).public_key());

    let client = Client::new(
        test_loop.clock(),
        client_config.clone(),
        chain_genesis.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime_adapter.clone(),
        network_adapter.as_multi_sender(),
        client_to_shards_manager_sender.as_sender(),
        validator_signer.clone(),
        true,
        [0; 32],
        Some(snapshot_callbacks),
        Arc::new(test_loop.async_computation_spawner(identifier, |_| Duration::milliseconds(80))),
        partial_witness_adapter.as_multi_sender(),
        resharding_sender.as_multi_sender(),
        Arc::new(test_loop.future_spawner(identifier)),
        client_adapter.as_multi_sender(),
        client_adapter.as_multi_sender(),
        upgrade_schedule.clone(),
    )
    .unwrap();

    // If this is an archival node and split storage is initialized, then create view-specific
    // versions of EpochManager, ShardTracker and RuntimeAdapter and use them to initialize the
    // ViewClientActorInner. Otherwise, we use the regular versions created above.
    let (view_epoch_manager, view_shard_tracker, view_runtime_adapter) = if let Some(split_store) =
        &split_store
    {
        let view_epoch_manager = EpochManager::new_arc_handle_from_epoch_config_store(
            split_store.clone(),
            &genesis.config,
            epoch_config_store.clone(),
        );
        let view_shard_tracker =
            ShardTracker::new(TrackedConfig::from_config(&client_config), epoch_manager.clone());
        let view_runtime_adapter = NightshadeRuntime::test_with_trie_config(
            &homedir,
            split_store.clone(),
            ContractRuntimeCache::handle(&contract_cache),
            &genesis.config,
            view_epoch_manager.clone(),
            runtime_config_store.clone(),
            TrieConfig::from_store_config(&store_config),
            StateSnapshotType::EveryEpoch,
            client_config.gc.gc_num_epochs_to_keep,
        );
        (view_epoch_manager, view_shard_tracker, view_runtime_adapter)
    } else {
        (epoch_manager.clone(), shard_tracker.clone(), runtime_adapter.clone())
    };
    let view_client_actor = ViewClientActorInner::new(
        test_loop.clock(),
        validator_signer.clone(),
        chain_genesis.clone(),
        view_epoch_manager.clone(),
        view_shard_tracker,
        view_runtime_adapter,
        network_adapter.as_multi_sender(),
        client_config.clone(),
        near_client::adversarial::Controls::default(),
    )
    .unwrap();

    let shards_manager = ShardsManagerActor::new(
        test_loop.clock(),
        validator_signer.clone(),
        epoch_manager.clone(),
        view_epoch_manager,
        shard_tracker.clone(),
        network_adapter.as_sender(),
        client_adapter.as_sender(),
        store.chunk_store(),
        client.chain.head().unwrap(),
        client.chain.header_head().unwrap(),
        Duration::milliseconds(100),
    );

    let client_actor = ClientActorInner::new(
        test_loop.clock(),
        client,
        peer_id.clone(),
        network_adapter.as_multi_sender(),
        noop().into_sender(),
        None,
        Default::default(),
        None,
        sync_jobs_adapter.as_multi_sender(),
    )
    .unwrap();

    let partial_witness_actor = PartialWitnessActor::new(
        test_loop.clock(),
        network_adapter.as_multi_sender(),
        client_adapter.as_multi_sender(),
        validator_signer.clone(),
        epoch_manager.clone(),
        runtime_adapter.clone(),
        Arc::new(test_loop.async_computation_spawner(identifier, |_| Duration::milliseconds(80))),
        Arc::new(test_loop.async_computation_spawner(identifier, |_| Duration::milliseconds(80))),
    );

    let peer_manager_actor = TestLoopPeerManagerActor::new(
        test_loop.clock(),
        &account_id,
        network_shared_state,
        Arc::new(test_loop.future_spawner(identifier)),
    );

    let gc_actor = GCActor::new(
        runtime_adapter.store().clone(),
        &chain_genesis,
        runtime_adapter.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        validator_signer.clone(),
        client_config.gc.clone(),
        client_config.archive,
    );
    // We don't send messages to `GCActor` so adapter is not needed.
    test_loop.data.register_actor(identifier, gc_actor, None);

    let resharding_actor = ReshardingActor::new(runtime_adapter.store().clone(), &chain_genesis);

    let state_sync_dumper = StateSyncDumper {
        clock: test_loop.clock(),
        client_config,
        chain_genesis,
        epoch_manager: epoch_manager.clone(),
        shard_tracker,
        runtime: runtime_adapter,
        validator: validator_signer,
        future_spawner: Arc::new(test_loop.future_spawner(identifier)),
        handle: None,
    };
    let state_sync_dumper_handle = test_loop.data.register_data(state_sync_dumper);

    let client_sender =
        test_loop.data.register_actor(identifier, client_actor, Some(client_adapter));
    let view_client_sender = test_loop.data.register_actor(identifier, view_client_actor, None);
    let shards_manager_sender =
        test_loop.data.register_actor(identifier, shards_manager, Some(shards_manager_adapter));
    let partial_witness_sender = test_loop.data.register_actor(
        identifier,
        partial_witness_actor,
        Some(partial_witness_adapter),
    );
    test_loop.data.register_actor(identifier, sync_jobs_actor, Some(sync_jobs_adapter));
    test_loop.data.register_actor(identifier, state_snapshot, Some(state_snapshot_adapter));
    test_loop.data.register_actor(identifier, resharding_actor, Some(resharding_sender));

    // State sync dumper is not an Actor, handle starting separately.
    let state_sync_dumper_handle_clone = state_sync_dumper_handle.clone();
    test_loop.send_adhoc_event("start_state_sync_dumper".to_owned(), move |test_loop_data| {
        test_loop_data.get_mut(&state_sync_dumper_handle_clone).start().unwrap();
    });

    // TODO
    for condition in &self.drop_condition_kinds {
        register_drop_condition(
            &mut peer_manager_actor,
            self.chunks_storage.clone(),
            epoch_manager.clone(),
            condition,
        );
    }

    let peer_manager_sender =
        test_loop.data.register_actor(identifier, peer_manager_actor, Some(network_adapter));

    let data = TestData {
        identifier: identifier.to_string(),
        account_id,
        peer_id,
        client_sender,
        view_client_sender,
        shards_manager_sender,
        partial_witness_sender,
        peer_manager_sender,
        state_sync_dumper_handle,
    };

    // Add the client to the network shared state before returning data
    network_shared_state.add_client(&data);
    data
}
