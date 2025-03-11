use itertools::Itertools;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::TempDir;

use near_async::messaging::{IntoMultiSender, IntoSender, LateBoundSender, noop};
use near_async::test_loop::TestLoopV2;
use near_async::time::{Clock, Duration};
use near_chain::ChainGenesis;
use near_chain::runtime::NightshadeRuntime;
use near_chain::state_snapshot_actor::{
    SnapshotCallbacks, StateSnapshotActor, get_delete_snapshot_callback, get_make_snapshot_callback,
};
use near_chain::types::RuntimeAdapter;
use near_chain_configs::{
    ClientConfig, DumpConfig, ExternalStorageConfig, ExternalStorageLocation, Genesis,
    MutableConfigValue, StateSyncConfig, SyncConfig,
};
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
use near_primitives::types::{AccountId, ShardId};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::PROTOCOL_UPGRADE_SCHEDULE;
use near_store::adapter::StoreAdapter;
use near_store::config::StateSnapshotType;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::{create_test_split_store, create_test_store};
use near_store::{Store, StoreConfig, TrieConfig};
use near_vm_runner::{ContractRuntimeCache, FilesystemContractRuntimeCache};
use nearcore::state_sync::StateSyncDumper;

use super::drop_condition::ClientToShardsManagerSender;
use super::env::TestLoopEnv;
use super::state::{NodeState, SharedState, TestData};
use near_chain::resharding::resharding_actor::ReshardingActor;

pub(crate) struct TestLoopBuilder {
    test_loop: TestLoopV2,
    genesis: Option<Genesis>,
    epoch_config_store: Option<EpochConfigStore>,
    clients: Vec<AccountId>,
    /// Overrides the stores; rather than constructing fresh new stores, use
    /// the provided ones (to test with existing data).
    /// Each element in the vector is (hot_store, split_store).
    stores_override: Option<Vec<(Store, Option<Store>)>>,
    /// Overrides the directory used for test loop shared data; rather than
    /// constructing fresh new tempdir, use the provided one (to test with
    /// existing data from a previous test loop run).
    test_loop_data_dir: Option<TempDir>,
    /// Accounts whose clients should be configured as an archival node.
    /// This should be a subset of the accounts in the `clients` list.
    archival_clients: HashSet<AccountId>,
    /// Number of latest epochs to keep before garbage collecting associated data.
    gc_num_epochs_to_keep: Option<u64>,
    /// The store of runtime configurations to be passed into runtime adapters.
    runtime_config_store: Option<RuntimeConfigStore>,
    /// Custom function to change the configs before constructing each client.
    config_modifier: Option<Box<dyn Fn(&mut ClientConfig, usize)>>,
    /// Whether to do the warmup or not. See `skip_warmup` for more details.
    warmup_pending: Arc<AtomicBool>,
    /// Whether all nodes must track all shards.
    track_all_shards: bool,
    /// Whether to load mem tries for the tracked shards.
    load_memtries_for_tracked_shards: bool,
    /// Upgrade schedule which determines when the clients start voting for new protocol versions.
    upgrade_schedule: ProtocolUpgradeVotingSchedule,
}

impl TestLoopBuilder {
    pub(crate) fn new() -> Self {
        Self {
            test_loop: TestLoopV2::new(),
            genesis: None,
            epoch_config_store: None,
            clients: vec![],
            stores_override: None,
            test_loop_data_dir: None,
            archival_clients: HashSet::new(),
            gc_num_epochs_to_keep: None,
            runtime_config_store: None,
            config_modifier: None,
            warmup_pending: Arc::new(AtomicBool::new(true)),
            track_all_shards: false,
            load_memtries_for_tracked_shards: true,
            upgrade_schedule: PROTOCOL_UPGRADE_SCHEDULE.clone(),
        }
    }

    // Creates TestLoop-compatible genesis builder
    pub(crate) fn new_genesis_builder() -> TestGenesisBuilder {
        TestGenesisBuilder::new()
            .genesis_time_from_clock(&near_async::time::FakeClock::default().clock())
    }

    /// Get the clock for the test loop.
    pub(crate) fn clock(&self) -> Clock {
        self.test_loop.clock()
    }

    /// Set the genesis configuration for the test loop.
    pub(crate) fn genesis(mut self, genesis: Genesis) -> Self {
        self.genesis = Some(genesis);
        self
    }

    pub(crate) fn epoch_config_store(mut self, epoch_config_store: EpochConfigStore) -> Self {
        self.epoch_config_store = Some(epoch_config_store);
        self
    }

    pub(crate) fn runtime_config_store(mut self, runtime_config_store: RuntimeConfigStore) -> Self {
        self.runtime_config_store = Some(runtime_config_store);
        self
    }

    /// Set the clients for the test loop.
    pub(crate) fn clients(mut self, clients: Vec<AccountId>) -> Self {
        self.clients = clients;
        self
    }

    /// Uses the provided stores instead of generating new ones.
    /// Each element in the vector is (hot_store, split_store).
    pub fn stores_override(mut self, stores: Vec<(Store, Option<Store>)>) -> Self {
        self.stores_override = Some(stores);
        self
    }

    /// Like stores_override, but all cold stores are None.
    pub fn stores_override_hot_only(mut self, stores: Vec<Store>) -> Self {
        self.stores_override = Some(stores.into_iter().map(|store| (store, None)).collect());
        self
    }

    /// Set the accounts whose clients should be configured as archival nodes in the test loop.
    /// These accounts should be a subset of the accounts provided to the `clients` method.
    pub(crate) fn archival_clients(mut self, clients: HashSet<AccountId>) -> Self {
        self.archival_clients = clients;
        self
    }

    pub(crate) fn gc_num_epochs_to_keep(mut self, num_epochs: u64) -> Self {
        self.gc_num_epochs_to_keep = Some(num_epochs);
        self
    }

    /// Custom function to change the configs before constructing each client.
    pub fn config_modifier(
        mut self,
        modifier: impl Fn(&mut ClientConfig, usize) + 'static,
    ) -> Self {
        self.config_modifier = Some(Box::new(modifier));
        self
    }

    /// Do not automatically warmup the chain. Start from genesis instead.
    /// Note that this can cause unexpected issues, as the chain behaves
    /// somewhat differently (and correctly so) at genesis. So only skip
    /// warmup if you are interested in the behavior of starting from genesis.
    pub fn skip_warmup(self) -> Self {
        self.warmup_pending.store(false, Ordering::Relaxed);
        self
    }

    pub fn track_all_shards(mut self) -> Self {
        self.track_all_shards = true;
        self
    }

    pub fn load_memtries_for_tracked_shards(mut self, load_memtries: bool) -> Self {
        self.load_memtries_for_tracked_shards = load_memtries;
        self
    }

    /// Overrides the tempdir (which contains state dump, etc.) instead
    /// of creating a new one.
    pub fn test_loop_data_dir(mut self, dir: TempDir) -> Self {
        self.test_loop_data_dir = Some(dir);
        self
    }

    pub fn protocol_upgrade_schedule(mut self, schedule: ProtocolUpgradeVotingSchedule) -> Self {
        self.upgrade_schedule = schedule;
        self
    }

    /// Build the test loop environment.
    pub(crate) fn build(self) -> TestLoopEnv {
        self.ensure_genesis()
            .ensure_epoch_config_store()
            .ensure_clients()
            .ensure_tempdir()
            .build_impl()
    }

    fn ensure_genesis(self) -> Self {
        assert!(self.genesis.is_some(), "Genesis must be provided to the test loop");
        self
    }

    fn ensure_epoch_config_store(self) -> Self {
        assert!(self.epoch_config_store.is_some(), "EpochConfigStore must be provided");
        self
    }

    fn ensure_clients(self) -> Self {
        assert!(!self.clients.is_empty(), "Clients must be provided to the test loop");
        assert!(
            self.archival_clients.is_subset(&HashSet::from_iter(self.clients.iter().cloned())),
            "Archival accounts must be subset of the clients"
        );
        self
    }

    fn ensure_tempdir(mut self) -> Self {
        self.test_loop_data_dir.get_or_insert_with(|| tempfile::tempdir().unwrap());
        self
    }

    fn build_impl(mut self) -> TestLoopEnv {
        let warmup_pending = self.warmup_pending.clone();
        self.test_loop.send_adhoc_event("warmup_pending".into(), move |_| {
            assert!(
                !warmup_pending.load(Ordering::Relaxed),
                "Warmup is pending! Call env.warmup() or builder.skip_warmup()"
            );
        });

        let node_states =
            (0..self.clients.len()).map(|idx| self.setup_node_state(idx)).collect_vec();
        let (mut test_loop, shared_state) = self.setup_shared_state();
        let datas = node_states
            .into_iter()
            .map(|node_state| {
                let account_id = node_state.account_id.clone();
                setup_client(account_id.as_str(), &mut test_loop, node_state, &shared_state)
            })
            .collect_vec();

        TestLoopEnv { test_loop, node_datas: datas, shared_state }
    }

    fn setup_shared_state(self) -> (TestLoopV2, SharedState) {
        let shared_state = SharedState {
            genesis: self.genesis.unwrap(),
            tempdir: self.test_loop_data_dir.unwrap(),
            epoch_config_store: self.epoch_config_store.unwrap(),
            runtime_config_store: self.runtime_config_store,
            network_shared_state: TestLoopNetworkSharedState::new(),
            upgrade_schedule: self.upgrade_schedule,
            chunks_storage: Default::default(),
            drop_conditions: Default::default(),
            load_memtries_for_tracked_shards: self.load_memtries_for_tracked_shards,
            warmup_pending: self.warmup_pending,
        };
        (self.test_loop, shared_state)
    }

    fn setup_node_state(&mut self, idx: usize) -> NodeState {
        let account_id = self.clients[idx].clone();
        let client_config = self.setup_client_config(idx);
        let (store, split_store) = self.setup_store(idx);
        NodeState { account_id, client_config, store, split_store }
    }

    fn setup_client_config(&mut self, idx: usize) -> ClientConfig {
        let account_id = &self.clients[idx];
        let is_archival = self.archival_clients.contains(account_id);

        let genesis = self.genesis.as_ref().unwrap();
        let tempdir = self.test_loop_data_dir.as_ref().unwrap();
        let mut client_config = ClientConfig::test(true, 600, 2000, 4, is_archival, true, false);
        client_config.epoch_length = genesis.config.epoch_length;
        client_config.max_block_wait_delay = Duration::seconds(6);
        client_config.state_sync_enabled = true;
        client_config.state_sync_external_timeout = Duration::milliseconds(100);
        client_config.state_sync_p2p_timeout = Duration::milliseconds(100);
        client_config.state_sync_retry_backoff = Duration::milliseconds(100);
        client_config.state_sync_external_backoff = Duration::milliseconds(100);
        if let Some(num_epochs) = self.gc_num_epochs_to_keep {
            client_config.gc.gc_num_epochs_to_keep = num_epochs;
        }
        let external_storage_location =
            ExternalStorageLocation::Filesystem { root_dir: tempdir.path().join("state_sync") };
        client_config.state_sync = StateSyncConfig {
            dump: Some(DumpConfig {
                iteration_delay: Some(Duration::seconds(1)),
                location: external_storage_location.clone(),
                credentials_file: None,
                restart_dump_for_shards: None,
            }),
            sync: SyncConfig::ExternalStorage(ExternalStorageConfig {
                location: external_storage_location,
                num_concurrent_requests: 1,
                num_concurrent_requests_during_catchup: 1,
                // We go straight to storage here because the network layer basically
                // doesn't exist in testloop. We could mock a bunch of stuff to make
                // the clients transfer state parts "peer to peer" but we wouldn't really
                // gain anything over having them dump parts to a tempdir.
                external_storage_fallback_threshold: 0,
            }),
        };

        // Configure tracked shards.
        // * single shard tracking for validators
        // * all shard tracking for non-validators (RPCs and archival)
        let is_validator =
            genesis.config.validators.iter().any(|validator| validator.account_id == *account_id);
        if is_validator && !self.track_all_shards {
            client_config.tracked_shards = Vec::new();
        } else {
            client_config.tracked_shards = vec![ShardId::new(666)];
        }

        if let Some(config_modifier) = &self.config_modifier {
            config_modifier(&mut client_config, idx);
        }

        client_config
    }

    fn setup_store(&mut self, idx: usize) -> (Store, Option<Store>) {
        let account_id = &self.clients[idx];
        let is_archival = self.archival_clients.contains(account_id);

        let (store, split_store) = if let Some(stores_override) = &self.stores_override {
            stores_override[idx].clone()
        } else if is_archival {
            let (hot_store, split_store) = create_test_split_store();
            (hot_store, Some(split_store))
        } else {
            (create_test_store(), None)
        };

        let genesis = self.genesis.as_ref().unwrap();
        initialize_genesis_state(store.clone(), genesis, None);
        (store, split_store)
    }
}

pub fn setup_client(
    identifier: &str,
    test_loop: &mut TestLoopV2,
    node_handle: NodeState,
    shared_state: &SharedState,
) -> TestData {
    let NodeState { account_id, client_config, store, split_store } = node_handle;
    let SharedState {
        genesis,
        tempdir,
        epoch_config_store,
        runtime_config_store,
        network_shared_state,
        upgrade_schedule,
        chunks_storage,
        drop_conditions,
        load_memtries_for_tracked_shards,
        ..
    } = shared_state;

    let client_adapter = LateBoundSender::new();
    let network_adapter = LateBoundSender::new();
    let state_snapshot_adapter = LateBoundSender::new();
    let partial_witness_adapter = LateBoundSender::new();
    let sync_jobs_adapter = LateBoundSender::new();
    let resharding_sender = LateBoundSender::new();

    let homedir = tempdir.path().join(format!("{}", identifier));
    std::fs::create_dir_all(&homedir).expect("Unable to create homedir");

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

    let validator_signer = MutableConfigValue::new(
        Some(Arc::new(create_test_signer(account_id.as_str()))),
        "validator_signer",
    );

    let shards_manager_adapter = LateBoundSender::new();
    let client_to_shards_manager_sender = Arc::new(ClientToShardsManagerSender {
        sender: shards_manager_adapter.clone(),
        chunks_storage: chunks_storage.clone(),
    });

    // Generate a PeerId. This is used to identify the client in the network.
    // Make sure this is the same as the account_id of the client to redirect the network messages properly.
    let peer_id = PeerId::new(create_test_signer(account_id.as_str()).public_key());

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
        epoch_manager,
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
    // Note that this can potentially overwrite an existing client with the same account_id
    // and all new messages would be redirected to the new client.
    network_shared_state.add_client(&data);

    // Register all accumulated drop conditions
    for condition in drop_conditions {
        data.register_drop_condition(&mut test_loop.data, chunks_storage.clone(), condition);
    }

    data
}
