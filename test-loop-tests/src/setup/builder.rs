use itertools::Itertools;
use near_chain_configs::test_genesis::{
    TestEpochConfigBuilder, TestGenesisBuilder, ValidatorsSpec,
};
use near_chain_configs::test_utils::TestClientConfigParams;
use near_primitives::shard_layout::ShardLayout;
use near_store::archive::cloud_storage::config::test_cloud_archival_config;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::TempDir;

use near_async::test_loop::TestLoopV2;
use near_async::time::{Clock, Duration};
use near_chain_configs::{
    ClientConfig, DumpConfig, ExternalStorageConfig, ExternalStorageLocation, Genesis,
    StateSyncConfig, SyncConfig, TrackedShardsConfig,
};
use near_parameters::RuntimeConfigStore;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::types::{AccountId, NumShards};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::get_protocol_upgrade_schedule;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::{TestNodeStorage, create_test_node_storage};

use crate::utils::account::{
    create_validators_spec, validators_spec_clients, validators_spec_clients_with_rpc,
};
use crate::utils::peer_manager_actor::{TestLoopNetworkSharedState, UnreachableActor};

use super::env::TestLoopEnv;
use super::setup::setup_client;
use super::state::{NodeSetupState, SharedState};

pub(crate) const MIN_BLOCK_PROD_TIME: u64 = 600;

pub(crate) struct TestLoopBuilder {
    test_loop: TestLoopV2,
    genesis: Option<Genesis>,
    epoch_config_store: Option<EpochConfigStore>,
    clients: Vec<AccountId>,
    /// Overrides the directory used for test loop shared data; rather than
    /// constructing fresh new tempdir, use the provided one (to test with
    /// existing data from a previous test loop run).
    test_loop_data_dir: TempDir,
    /// Accounts whose clients should be configured as cold DB split store archival node.
    /// This should be a subset of the accounts in the `clients` list.
    cold_storage_archival_clients: HashSet<AccountId>,
    /// Accounts whose clients should be configured as a cloud archival node.
    /// This should be a subset of the accounts in the `clients` list.
    cloud_storage_archival_clients: HashSet<AccountId>,
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
    /// If not explicitly set, the chain_id from genesis determines the schedule.
    upgrade_schedule: Option<ProtocolUpgradeVotingSchedule>,

    validators_spec: Option<ValidatorsSpec>,
    enable_rpc: bool,
    shard_layout: Option<ShardLayout>,
}

impl TestLoopBuilder {
    pub(crate) fn new() -> Self {
        Self {
            test_loop: TestLoopV2::new(),
            genesis: None,
            epoch_config_store: None,
            clients: vec![],
            test_loop_data_dir: tempfile::tempdir().unwrap(),
            cold_storage_archival_clients: HashSet::new(),
            cloud_storage_archival_clients: HashSet::new(),
            gc_num_epochs_to_keep: None,
            runtime_config_store: None,
            config_modifier: None,
            warmup_pending: Arc::new(AtomicBool::new(true)),
            track_all_shards: false,
            load_memtries_for_tracked_shards: true,
            upgrade_schedule: None,
            validators_spec: None,
            enable_rpc: false,
            shard_layout: None,
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

    pub(crate) fn epoch_config_store_from_genesis(self) -> Self {
        // noop, this is a default behavior now, to be removed
        self
    }

    pub(crate) fn runtime_config_store(mut self, runtime_config_store: RuntimeConfigStore) -> Self {
        self.runtime_config_store = Some(runtime_config_store);
        self
    }

    pub(crate) fn chunk_producer_per_shard(self) -> Self {
        let shard_layout = self.shard_layout.as_ref().expect("shard layout should be set");
        let num_block_and_chunk_producers = shard_layout.num_shards() as usize;
        self.validators(num_block_and_chunk_producers, 0)
    }

    pub(crate) fn validators(
        self,
        num_block_and_chunk_producers: usize,
        num_chunk_validators_only: usize,
    ) -> Self {
        self.validators_spec(create_validators_spec(
            num_block_and_chunk_producers,
            num_chunk_validators_only,
        ))
    }

    pub(crate) fn enable_rpc(mut self) -> Self {
        self.enable_rpc = true;
        self
    }

    pub(crate) fn validators_spec(mut self, spec: ValidatorsSpec) -> Self {
        self.validators_spec = Some(spec);
        self
    }

    /// Set the clients for the test loop.
    pub(crate) fn clients(mut self, clients: Vec<AccountId>) -> Self {
        self.clients = clients;
        self
    }

    pub fn num_shards(self, num_shards: usize) -> Self {
        self.shard_layout(ShardLayout::multi_shard(num_shards as NumShards, 1))
    }

    pub fn shard_layout(mut self, shard_layout: ShardLayout) -> Self {
        self.shard_layout = Some(shard_layout);
        self
    }

    /// Set the accounts whose clients should be configured as cold DB split store archival nodes in the test loop.
    /// These accounts should be a subset of the accounts provided to the `clients` method.
    pub(crate) fn cold_storage_archival_clients(mut self, clients: HashSet<AccountId>) -> Self {
        self.cold_storage_archival_clients = clients;
        self
    }

    /// Set the accounts whose clients should be configured as cloud archival nodes in the test loop.
    /// These accounts should be a subset of the accounts provided to the `clients` method.
    pub(crate) fn cloud_storage_archival_clients(mut self, clients: HashSet<AccountId>) -> Self {
        self.cloud_storage_archival_clients = clients;
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
        assert!(self.config_modifier.is_none());
        self.config_modifier = Some(Box::new(modifier));
        self
    }

    /// Do not automatically warmup the chain. Start from genesis instead.
    /// Note that this can cause unexpected issues, as the chain behaves
    /// somewhat differently (and correctly so) at genesis. So only skip
    /// warmup if you are interested in the behavior of starting from genesis.
    #[allow(dead_code)]
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

    pub fn protocol_upgrade_schedule(mut self, schedule: ProtocolUpgradeVotingSchedule) -> Self {
        self.upgrade_schedule = Some(schedule);
        self
    }

    /// Build the test loop environment.
    pub(crate) fn build(self) -> TestLoopEnv {
        self.ensure_compatible()
            .ensure_validators_spec()
            .ensure_genesis()
            .ensure_epoch_config_store()
            .ensure_clients()
            .build_impl()
    }

    /// Check that the builder configuration is consistent.
    /// There are two supported API paths:
    /// - New API: use `.validators()`, `.num_shards()`, `.enable_rpc()` etc to
    ///   auto-derive genesis and clients.
    /// - Old API: manually provide genesis and clients via `.genesis()` and `.clients()`.
    /// Mixing the two is not allowed.
    fn ensure_compatible(self) -> Self {
        let uses_new_api =
            self.validators_spec.is_some() || self.shard_layout.is_some() || self.enable_rpc;
        let has_genesis = self.genesis.is_some();
        let has_clients = !self.clients.is_empty();

        assert!(
            !uses_new_api || !has_genesis,
            "builder API (.validators(), .num_shards(), .enable_rpc()) is incompatible \
             with manually provided genesis"
        );
        assert!(
            !uses_new_api || !has_clients,
            "builder API (.validators(), .num_shards(), .enable_rpc()) is incompatible \
             with manually provided clients"
        );
        assert!(
            has_genesis == has_clients,
            "genesis and clients must be provided together; \
             either provide both or use the builder API to auto-derive them"
        );
        self
    }

    fn ensure_validators_spec(mut self) -> Self {
        if self.validators_spec.is_none() {
            self.validators_spec = Some(default_validators_spec());
        }
        self
    }

    fn ensure_genesis(mut self) -> Self {
        if self.genesis.is_none() {
            let validators_spec = self.validators_spec.clone().unwrap();
            let mut genesis_builder = Self::new_genesis_builder().validators_spec(validators_spec);
            if let Some(shard_layout) = &self.shard_layout {
                genesis_builder = genesis_builder.shard_layout(shard_layout.clone());
            }
            self.genesis = Some(genesis_builder.build());
        }
        self
    }

    fn ensure_epoch_config_store(mut self) -> Self {
        if self.epoch_config_store.is_none() {
            self.epoch_config_store = Some(TestEpochConfigBuilder::build_store_from_genesis(
                self.genesis.as_ref().unwrap(),
            ));
        }
        self
    }

    fn ensure_clients(mut self) -> Self {
        if self.clients.is_empty() {
            let validators_spec = self.validators_spec.as_ref().unwrap();
            self.clients = if self.enable_rpc {
                validators_spec_clients_with_rpc(validators_spec)
            } else {
                validators_spec_clients(validators_spec)
            };
        }
        assert!(
            self.cold_storage_archival_clients
                .is_subset(&HashSet::from_iter(self.clients.iter().cloned())),
            "Archival accounts must be subset of the clients"
        );
        self
    }

    fn build_impl(self) -> TestLoopEnv {
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

    fn setup_shared_state(mut self) -> (TestLoopV2, SharedState) {
        let unreachable_actor_sender =
            self.test_loop.data.register_actor("UnreachableActor", UnreachableActor {}, None);
        self.test_loop.event_denylist().lock().push("UnreachableActor".to_string());

        let genesis = self.genesis.unwrap();
        let upgrade_schedule = self
            .upgrade_schedule
            .unwrap_or_else(|| get_protocol_upgrade_schedule(&genesis.config.chain_id));
        let shared_state = SharedState {
            genesis: genesis,
            tempdir: self.test_loop_data_dir,
            epoch_config_store: self.epoch_config_store.unwrap(),
            runtime_config_store: self.runtime_config_store,
            network_shared_state: TestLoopNetworkSharedState::new(unreachable_actor_sender),
            upgrade_schedule,
            chunks_storage: Default::default(),
            drop_conditions: Default::default(),
            load_memtries_for_tracked_shards: self.load_memtries_for_tracked_shards,
            warmup_pending: self.warmup_pending,
        };
        (self.test_loop, shared_state)
    }

    fn setup_node_state(&self, idx: usize) -> NodeSetupState {
        let account_id = self.clients[idx].clone();
        let genesis = self.genesis.as_ref().unwrap();
        let enable_cold_storage = self.cold_storage_archival_clients.contains(&account_id);
        let enable_cloud_storage = self.cloud_storage_archival_clients.contains(&account_id);
        let config_modifier = |client_config: &mut ClientConfig| {
            if let Some(num_epochs) = self.gc_num_epochs_to_keep {
                client_config.gc.gc_num_epochs_to_keep = num_epochs;
            }

            // Configure tracked shards.
            // * single shard tracking for validators
            // * all shard tracking for non-validators (RPCs and archival)
            let is_validator = genesis.config.validators.iter().any(|v| v.account_id == account_id);
            client_config.tracked_shards_config = if is_validator && !self.track_all_shards {
                TrackedShardsConfig::NoShards
            } else {
                TrackedShardsConfig::AllShards
            };

            if let Some(config_modifier) = &self.config_modifier {
                config_modifier(client_config, idx);
            }
        };
        let tempdir_path = self.test_loop_data_dir.path().to_path_buf();
        NodeStateBuilder::new(genesis.clone(), tempdir_path)
            .account_id(account_id.clone())
            .cold_storage(enable_cold_storage)
            .cloud_storage(enable_cloud_storage)
            .config_modifier(config_modifier)
            .build()
    }
}

fn default_validators_spec() -> ValidatorsSpec {
    create_validators_spec(1, 0)
}

pub struct NodeStateBuilder<'a> {
    genesis: Genesis,
    tempdir_path: PathBuf,

    account_id: Option<AccountId>,
    enable_cold_storage: bool,
    enable_cloud_storage: bool,
    config_modifier: Option<Box<dyn Fn(&mut ClientConfig) + 'a>>,
}

impl<'a> NodeStateBuilder<'a> {
    pub fn new(genesis: Genesis, tempdir_path: PathBuf) -> Self {
        Self {
            genesis,
            tempdir_path,
            account_id: None,
            enable_cold_storage: false,
            enable_cloud_storage: false,
            config_modifier: None,
        }
    }

    pub fn account_id(mut self, account_id: AccountId) -> Self {
        self.account_id = Some(account_id);
        self
    }

    pub fn cold_storage(mut self, enable_cold_storage: bool) -> Self {
        self.enable_cold_storage = enable_cold_storage;
        self
    }

    pub fn cloud_storage(mut self, enable_cloud_storage: bool) -> Self {
        self.enable_cloud_storage = enable_cloud_storage;
        self
    }

    pub fn config_modifier(mut self, modifier: impl Fn(&mut ClientConfig) + 'a) -> Self {
        self.config_modifier = Some(Box::new(modifier));
        self
    }

    pub fn build(self) -> NodeSetupState {
        let client_config = self.create_client_config();
        let storage = self.setup_storage(client_config.chain_id.clone());
        let account_id = self.account_id.unwrap();
        NodeSetupState { account_id, client_config, storage }
    }

    fn create_client_config(&self) -> ClientConfig {
        let archive = self.enable_cold_storage || self.enable_cloud_storage;

        let mut client_config = ClientConfig::test(TestClientConfigParams {
            skip_sync_wait: true,
            min_block_prod_time: MIN_BLOCK_PROD_TIME,
            max_block_prod_time: 2000,
            num_block_producer_seats: 4,
            archive,
            state_sync_enabled: false,
        });
        client_config.epoch_length = self.genesis.config.epoch_length;
        client_config.max_block_wait_delay = Duration::seconds(6);
        client_config.state_sync_external_timeout = Duration::milliseconds(100);
        client_config.state_sync_p2p_timeout = Duration::milliseconds(100);
        client_config.state_sync_retry_backoff = Duration::milliseconds(100);
        client_config.state_sync_external_backoff = Duration::milliseconds(100);

        if !archive {
            client_config.state_sync_enabled = true;
            // Testloop does not handle decentralized state sync network messages.
            // Instead, parts are dumped into a tempdir that mocks a centralized state sync bucket.
            client_config.state_sync = default_testloop_state_sync_config(&self.tempdir_path);
        }

        if let Some(config_modifier) = &self.config_modifier {
            config_modifier(&mut client_config);
        }

        if client_config.cloud_archival_writer.is_some() {
            client_config.state_sync_enabled = true;
            let cloud_archival_config = test_cloud_archival_config(&self.tempdir_path);
            let mut dump_config: DumpConfig = cloud_archival_config.into_default_dump_config();
            dump_config.iteration_delay = Some(Duration::seconds(1));
            client_config.state_sync.dump = Some(dump_config);
        }
        client_config
    }

    fn setup_storage(&self, chain_id: String) -> TestNodeStorage {
        let home_dir = Some(self.tempdir_path.clone());
        let storage = create_test_node_storage(
            self.enable_cold_storage,
            self.enable_cloud_storage,
            home_dir,
            Some(chain_id),
        );
        initialize_genesis_state(storage.hot_store.clone(), &self.genesis, None);
        storage
    }
}

fn default_testloop_state_sync_config(tempdir: &PathBuf) -> StateSyncConfig {
    let external_storage_location =
        ExternalStorageLocation::Filesystem { root_dir: tempdir.join("state_sync") };
    StateSyncConfig {
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
        concurrency: Default::default(),
        parts_compression_lvl: Default::default(),
    }
}
