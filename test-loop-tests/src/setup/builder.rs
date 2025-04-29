use itertools::Itertools;
use near_chain_configs::test_genesis::TestGenesisBuilder;
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
use near_primitives::types::AccountId;
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::get_protocol_upgrade_schedule;
use near_store::Store;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::{create_test_split_store, create_test_store};

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
    /// If not explicitly set, the chain_id from genesis determines the schedule.
    upgrade_schedule: Option<ProtocolUpgradeVotingSchedule>,
}

impl TestLoopBuilder {
    pub(crate) fn new() -> Self {
        Self {
            test_loop: TestLoopV2::new(),
            genesis: None,
            epoch_config_store: None,
            clients: vec![],
            test_loop_data_dir: tempfile::tempdir().unwrap(),
            archival_clients: HashSet::new(),
            gc_num_epochs_to_keep: None,
            runtime_config_store: None,
            config_modifier: None,
            warmup_pending: Arc::new(AtomicBool::new(true)),
            track_all_shards: false,
            load_memtries_for_tracked_shards: true,
            upgrade_schedule: None,
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
        self.ensure_genesis().ensure_epoch_config_store().ensure_clients().build_impl()
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
        self.test_loop.remove_events_with_identifier("UnreachableActor");

        let upgrade_schedule = self.upgrade_schedule.unwrap_or_else(|| {
            get_protocol_upgrade_schedule(&self.genesis.as_ref().unwrap().config.chain_id)
        });
        let shared_state = SharedState {
            genesis: self.genesis.unwrap(),
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
        let is_archival = self.archival_clients.contains(&account_id);
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
            .archive(is_archival)
            .config_modifier(config_modifier)
            .build()
    }
}

pub struct NodeStateBuilder<'a> {
    genesis: Genesis,
    tempdir_path: PathBuf,

    account_id: Option<AccountId>,
    archive: bool,
    config_modifier: Option<Box<dyn Fn(&mut ClientConfig) + 'a>>,
}

impl<'a> NodeStateBuilder<'a> {
    pub fn new(genesis: Genesis, tempdir_path: PathBuf) -> Self {
        Self { genesis, tempdir_path, account_id: None, archive: false, config_modifier: None }
    }

    pub fn account_id(mut self, account_id: AccountId) -> Self {
        self.account_id = Some(account_id);
        self
    }

    pub fn archive(mut self, archive: bool) -> Self {
        self.archive = archive;
        self
    }

    pub fn config_modifier(mut self, modifier: impl Fn(&mut ClientConfig) + 'a) -> Self {
        self.config_modifier = Some(Box::new(modifier));
        self
    }

    pub fn build(self) -> NodeSetupState {
        let (store, split_store) = self.setup_store();
        let account_id = self.account_id.unwrap();

        let mut client_config =
            ClientConfig::test(true, MIN_BLOCK_PROD_TIME, 2000, 4, self.archive, true, false);
        client_config.epoch_length = self.genesis.config.epoch_length;
        client_config.max_block_wait_delay = Duration::seconds(6);
        client_config.state_sync_enabled = true;
        client_config.state_sync_external_timeout = Duration::milliseconds(100);
        client_config.state_sync_p2p_timeout = Duration::milliseconds(100);
        client_config.state_sync_retry_backoff = Duration::milliseconds(100);
        client_config.state_sync_external_backoff = Duration::milliseconds(100);
        let external_storage_location =
            ExternalStorageLocation::Filesystem { root_dir: self.tempdir_path.join("state_sync") };
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

        if let Some(config_modifier) = self.config_modifier {
            config_modifier(&mut client_config);
        }

        NodeSetupState { account_id, client_config, store, split_store }
    }

    fn setup_store(&self) -> (Store, Option<Store>) {
        let (store, split_store) = if self.archive {
            let (hot_store, split_store) = create_test_split_store();
            (hot_store, Some(split_store))
        } else {
            (create_test_store(), None)
        };

        initialize_genesis_state(store.clone(), &self.genesis, None);
        (store, split_store)
    }
}
