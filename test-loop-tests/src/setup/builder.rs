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
use near_primitives::gas::Gas;
use near_primitives::types::{AccountId, Balance, BlockHeight, NumBlocks, NumShards};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::{ProtocolVersion, get_protocol_upgrade_schedule};
use near_primitives_core::num_rational::Rational32;
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
    setup_config: SetupConfig,
    epoch_config_store: Option<EpochConfigStore>,
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
}

impl TestLoopBuilder {
    pub(crate) fn new() -> Self {
        Self {
            test_loop: TestLoopV2::new(),
            setup_config: SetupConfig::Undecided,
            epoch_config_store: None,
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

    // -- Old API methods (transition to Manual) --

    /// Set the genesis configuration for the test loop.
    pub(crate) fn genesis(mut self, genesis: Genesis) -> Self {
        let (genesis_slot, _) = self.setup_config.ensure_manual();
        assert!(genesis_slot.is_none(), "genesis is already set");
        *genesis_slot = Some(genesis);
        self
    }

    /// Set the clients for the test loop.
    pub(crate) fn clients(mut self, clients: Vec<AccountId>) -> Self {
        let (_, clients_slot) = self.setup_config.ensure_manual();
        assert!(clients_slot.is_empty(), "clients are already set");
        *clients_slot = clients;
        self
    }

    // -- New API methods (transition to Auto) --

    pub(crate) fn chunk_producer_per_shard(mut self) -> Self {
        let auto = self.setup_config.ensure_auto();
        let num_block_and_chunk_producers =
            auto.shard_layout.as_ref().expect("shard layout should be set").num_shards() as usize;
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
        let auto = self.setup_config.ensure_auto();
        assert!(!auto.enable_rpc, "enable_rpc is already set");
        auto.enable_rpc = true;
        self
    }

    pub(crate) fn validators_spec(mut self, spec: ValidatorsSpec) -> Self {
        let auto = self.setup_config.ensure_auto();
        assert!(auto.validators_spec.is_none(), "validators_spec is already set");
        auto.validators_spec = Some(spec);
        self
    }

    pub fn num_shards(self, num_shards: usize) -> Self {
        self.shard_layout(ShardLayout::multi_shard(num_shards as NumShards, 1))
    }

    pub fn shard_layout(mut self, layout: ShardLayout) -> Self {
        let auto = self.setup_config.ensure_auto();
        assert!(auto.shard_layout.is_none(), "shard_layout is already set");
        auto.shard_layout = Some(layout);
        self
    }

    pub fn epoch_length(mut self, epoch_length: u64) -> Self {
        let auto = self.setup_config.ensure_auto();
        assert!(auto.epoch_length.is_none(), "epoch_length is already set");
        auto.epoch_length = Some(epoch_length);
        self
    }

    pub fn gas_limit(mut self, gas_limit: Gas) -> Self {
        let auto = self.setup_config.ensure_auto();
        assert!(auto.gas_limit.is_none(), "gas_limit is already set");
        auto.gas_limit = Some(gas_limit);
        self
    }

    pub fn protocol_version(mut self, protocol_version: ProtocolVersion) -> Self {
        let auto = self.setup_config.ensure_auto();
        assert!(auto.protocol_version.is_none(), "protocol_version is already set");
        auto.protocol_version = Some(protocol_version);
        self
    }

    pub fn genesis_height(mut self, genesis_height: BlockHeight) -> Self {
        let auto = self.setup_config.ensure_auto();
        assert!(auto.genesis_height.is_none(), "genesis_height is already set");
        auto.genesis_height = Some(genesis_height);
        self
    }

    pub fn transaction_validity_period(mut self, transaction_validity_period: NumBlocks) -> Self {
        let auto = self.setup_config.ensure_auto();
        assert!(
            auto.transaction_validity_period.is_none(),
            "transaction_validity_period is already set"
        );
        auto.transaction_validity_period = Some(transaction_validity_period);
        self
    }

    pub fn max_inflation_rate(mut self, max_inflation_rate: Rational32) -> Self {
        let auto = self.setup_config.ensure_auto();
        assert!(auto.max_inflation_rate.is_none(), "max_inflation_rate is already set");
        auto.max_inflation_rate = Some(max_inflation_rate);
        self
    }

    pub fn minimum_stake_ratio(mut self, minimum_stake_ratio: Rational32) -> Self {
        let auto = self.setup_config.ensure_auto();
        assert!(auto.minimum_stake_ratio.is_none(), "minimum_stake_ratio is already set");
        auto.minimum_stake_ratio = Some(minimum_stake_ratio);
        self
    }

    pub fn gas_prices(mut self, min: Balance, max: Balance) -> Self {
        let auto = self.setup_config.ensure_auto();
        assert!(auto.gas_prices.is_none(), "gas_prices is already set");
        auto.gas_prices = Some((min, max));
        self
    }

    pub fn add_user_account(mut self, account_id: &AccountId, initial_balance: Balance) -> Self {
        let auto = self.setup_config.ensure_auto();
        auto.user_accounts.push((account_id.clone(), initial_balance));
        self
    }

    pub fn add_user_accounts(mut self, accounts: &[AccountId], initial_balance: Balance) -> Self {
        let auto = self.setup_config.ensure_auto();
        for account_id in accounts {
            auto.user_accounts.push((account_id.clone(), initial_balance));
        }
        self
    }

    // -- Shared methods (work with both API paths) --

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

    // -- Build --

    /// Build the test loop environment.
    pub(crate) fn build(mut self) -> TestLoopEnv {
        let (genesis, clients) = self.resolve_setup_config();
        self.build_impl(genesis, clients)
    }

    fn resolve_setup_config(&mut self) -> (Genesis, Vec<AccountId>) {
        let setup_config = std::mem::replace(&mut self.setup_config, SetupConfig::Undecided);
        setup_config.resolve()
    }

    fn ensure_epoch_config_store(&mut self, genesis: &Genesis) {
        if self.epoch_config_store.is_none() {
            self.epoch_config_store =
                Some(TestEpochConfigBuilder::build_store_from_genesis(genesis));
        }
    }

    fn build_impl(mut self, genesis: Genesis, clients: Vec<AccountId>) -> TestLoopEnv {
        self.ensure_epoch_config_store(&genesis);

        assert!(
            self.cold_storage_archival_clients
                .is_subset(&HashSet::from_iter(clients.iter().cloned())),
            "Archival accounts must be subset of the clients"
        );

        let warmup_pending = self.warmup_pending.clone();
        self.test_loop.send_adhoc_event("warmup_pending".into(), move |_| {
            assert!(
                !warmup_pending.load(Ordering::Relaxed),
                "Warmup is pending! Call env.warmup() or builder.skip_warmup()"
            );
        });

        let node_states = (0..clients.len())
            .map(|idx| self.setup_node_state(idx, &genesis, &clients))
            .collect_vec();
        let (mut test_loop, shared_state) = self.setup_shared_state(genesis);
        let datas = node_states
            .into_iter()
            .map(|node_state| {
                let account_id = node_state.account_id.clone();
                setup_client(account_id.as_str(), &mut test_loop, node_state, &shared_state)
            })
            .collect_vec();

        TestLoopEnv { test_loop, node_datas: datas, shared_state }
    }

    fn setup_shared_state(mut self, genesis: Genesis) -> (TestLoopV2, SharedState) {
        let unreachable_actor_sender =
            self.test_loop.data.register_actor("UnreachableActor", UnreachableActor {}, None);
        self.test_loop.event_denylist().lock().push("UnreachableActor".to_string());

        let upgrade_schedule = self
            .upgrade_schedule
            .unwrap_or_else(|| get_protocol_upgrade_schedule(&genesis.config.chain_id));
        let shared_state = SharedState {
            genesis,
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

    fn setup_node_state(
        &self,
        idx: usize,
        genesis: &Genesis,
        clients: &[AccountId],
    ) -> NodeSetupState {
        let account_id = clients[idx].clone();
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

/// Determines how genesis and clients are configured.
enum SetupConfig {
    /// Initial state before any setup methods are called.
    /// Will be resolved to `Auto` with defaults at build time.
    Undecided,
    /// New API: builder auto-derives genesis and clients from high-level topology.
    Auto(AutoSetupConfig),
    /// Old API: manually provided genesis and clients.
    Manual { genesis: Option<Genesis>, clients: Vec<AccountId> },
}

/// Data for auto-derived setup (new API).
struct AutoSetupConfig {
    validators_spec: Option<ValidatorsSpec>,
    enable_rpc: bool,
    shard_layout: Option<ShardLayout>,
    user_accounts: Vec<(AccountId, Balance)>,
    epoch_length: Option<u64>,
    gas_limit: Option<Gas>,
    protocol_version: Option<ProtocolVersion>,
    genesis_height: Option<BlockHeight>,
    transaction_validity_period: Option<NumBlocks>,
    max_inflation_rate: Option<Rational32>,
    minimum_stake_ratio: Option<Rational32>,
    gas_prices: Option<(Balance, Balance)>,
}

impl SetupConfig {
    /// Transitions `Undecided` to `Auto` with defaults, or returns
    /// existing `Auto` data. Panics if `Manual`.
    fn ensure_auto(&mut self) -> &mut AutoSetupConfig {
        if matches!(self, SetupConfig::Undecided) {
            *self = SetupConfig::Auto(AutoSetupConfig::new());
        }
        match self {
            SetupConfig::Auto(data) => data,
            SetupConfig::Manual { .. } => {
                panic!("cannot use genesis builder API when genesis/clients are manually provided")
            }
            SetupConfig::Undecided => unreachable!(),
        }
    }

    /// Transitions `Undecided` to `Manual`, or returns existing `Manual`
    /// fields. Panics if `Auto`.
    fn ensure_manual(&mut self) -> (&mut Option<Genesis>, &mut Vec<AccountId>) {
        if matches!(self, SetupConfig::Undecided) {
            *self = SetupConfig::Manual { genesis: None, clients: vec![] };
        }
        match self {
            SetupConfig::Manual { genesis, clients } => (genesis, clients),
            SetupConfig::Auto { .. } => {
                panic!("cannot manually provide genesis/clients when using genesis builder API")
            }
            SetupConfig::Undecided => unreachable!(),
        }
    }

    fn resolve(self) -> (Genesis, Vec<AccountId>) {
        match self {
            SetupConfig::Undecided => AutoSetupConfig::new().resolve(),
            SetupConfig::Auto(auto) => auto.resolve(),
            SetupConfig::Manual { genesis, clients } => {
                let genesis = genesis.expect("genesis must be provided with manual setup");
                assert!(!clients.is_empty(), "clients must be provided with manual setup");
                (genesis, clients)
            }
        }
    }
}

impl AutoSetupConfig {
    fn new() -> Self {
        Self {
            validators_spec: None,
            enable_rpc: false,
            shard_layout: None,
            user_accounts: vec![],
            epoch_length: None,
            gas_limit: None,
            protocol_version: None,
            genesis_height: None,
            transaction_validity_period: None,
            max_inflation_rate: None,
            minimum_stake_ratio: None,
            gas_prices: None,
        }
    }

    fn resolve(self) -> (Genesis, Vec<AccountId>) {
        let validators_spec = self.validators_spec.unwrap_or_else(|| create_validators_spec(1, 0));
        let mut genesis_builder =
            TestLoopBuilder::new_genesis_builder().validators_spec(validators_spec.clone());
        if let Some(shard_layout) = self.shard_layout {
            genesis_builder = genesis_builder.shard_layout(shard_layout);
        }
        if let Some(epoch_length) = self.epoch_length {
            genesis_builder = genesis_builder.epoch_length(epoch_length);
        }
        if let Some(gas_limit) = self.gas_limit {
            genesis_builder = genesis_builder.gas_limit(gas_limit);
        }
        if let Some(protocol_version) = self.protocol_version {
            genesis_builder = genesis_builder.protocol_version(protocol_version);
        }
        if let Some(genesis_height) = self.genesis_height {
            genesis_builder = genesis_builder.genesis_height(genesis_height);
        }
        if let Some(transaction_validity_period) = self.transaction_validity_period {
            genesis_builder =
                genesis_builder.transaction_validity_period(transaction_validity_period);
        }
        if let Some(max_inflation_rate) = self.max_inflation_rate {
            genesis_builder = genesis_builder.max_inflation_rate(max_inflation_rate);
        }
        if let Some(minimum_stake_ratio) = self.minimum_stake_ratio {
            genesis_builder = genesis_builder.minimum_stake_ratio(minimum_stake_ratio);
        }
        if let Some((min, max)) = self.gas_prices {
            genesis_builder = genesis_builder.gas_prices(min, max);
        }
        for (account_id, balance) in self.user_accounts {
            genesis_builder = genesis_builder.add_user_account_simple(account_id, balance);
        }
        let genesis = genesis_builder.build();
        let clients = if self.enable_rpc {
            validators_spec_clients_with_rpc(&validators_spec)
        } else {
            validators_spec_clients(&validators_spec)
        };
        (genesis, clients)
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
