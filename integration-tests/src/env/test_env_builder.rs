use actix_rt::System;
use itertools::{Itertools, multizip};
use near_async::messaging::{IntoMultiSender, IntoSender, noop};
use near_async::time::Clock;
use near_chain::state_snapshot_actor::SnapshotCallbacks;
use near_chain::types::RuntimeAdapter;
use near_chain::{Block, ChainGenesis};
use near_chain_configs::{Genesis, GenesisConfig, TrackedShardsConfig};
use near_chunks::test_utils::MockClientAdapterForShardsManager;
use near_client::Client;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManager, EpochManagerHandle};
use near_network::test_utils::MockPeerManagerAdapter;
use near_parameters::RuntimeConfigStore;
use near_primitives::epoch_info::RngSeed;
use near_primitives::epoch_manager::{AllEpochConfigTestOverrides, EpochConfig, EpochConfigStore};
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, ShardIndex};
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use near_store::{NodeStorage, ShardUId, Store, StoreConfig, TrieConfig};
use near_vm_runner::{ContractRuntimeCache, FilesystemContractRuntimeCache};
use nearcore::NightshadeRuntime;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::utils::mock_partial_witness_adapter::MockPartialWitnessAdapter;

use super::setup::{
    TEST_SEED, setup_client_with_runtime, setup_synchronous_shards_manager,
    setup_tx_request_handler,
};
use super::test_env::{AccountIndices, TestEnv};

/// A builder for the TestEnv structure.
pub struct TestEnvBuilder {
    clock: Option<Clock>,
    genesis_config: GenesisConfig,
    genesis: Option<Genesis>,
    epoch_config_store: Option<EpochConfigStore>,
    clients: Vec<AccountId>,
    validators: Vec<AccountId>,
    home_dirs: Option<Vec<PathBuf>>,
    stores: Option<Vec<Store>>,
    contract_caches: Option<Vec<Box<dyn ContractRuntimeCache>>>,
    epoch_managers: Option<Vec<Arc<EpochManagerHandle>>>,
    shard_trackers: Option<Vec<ShardTracker>>,
    runtimes: Option<Vec<Arc<NightshadeRuntime>>>,
    network_adapters: Option<Vec<Arc<MockPeerManagerAdapter>>>,
    // random seed to be inject in each client according to AccountId
    // if not set, a default constant TEST_SEED will be injected
    seeds: HashMap<AccountId, RngSeed>,
    archive: bool,
    save_trie_changes: bool,
    state_snapshot_enabled: bool,
}

/// Builder for the [`TestEnv`] structure.
impl TestEnvBuilder {
    /// Constructs a new builder.
    pub(crate) fn new(genesis_config: GenesisConfig) -> Self {
        if let None = System::try_current() {
            let _ = System::new();
        }
        let clients = Self::make_accounts(1);
        let validators = clients.clone();
        let seeds: HashMap<AccountId, RngSeed> = HashMap::with_capacity(1);
        Self {
            clock: None,
            genesis_config,
            genesis: None,
            epoch_config_store: None,
            clients,
            validators,
            home_dirs: None,
            stores: None,
            contract_caches: None,
            epoch_managers: None,
            shard_trackers: None,
            runtimes: None,
            network_adapters: None,
            seeds,
            archive: false,
            save_trie_changes: true,
            state_snapshot_enabled: false,
        }
    }

    pub(crate) fn from_genesis(genesis: Genesis) -> Self {
        let mut env = Self::new(genesis.config.clone());
        env.genesis = Some(genesis);
        env
    }

    pub fn clock(mut self, clock: Clock) -> Self {
        assert!(self.clock.is_none(), "Cannot set clock twice");
        self.clock = Some(clock);
        self
    }

    /// Sets list of client [`AccountId`]s to the one provided.  Panics if the
    /// vector is empty.
    pub fn clients(mut self, clients: Vec<AccountId>) -> Self {
        assert!(!clients.is_empty());
        assert!(self.stores.is_none(), "Cannot set clients after stores");
        assert!(self.epoch_managers.is_none(), "Cannot set clients after epoch_managers");
        assert!(self.shard_trackers.is_none(), "Cannot set clients after shard_trackers");
        assert!(self.runtimes.is_none(), "Cannot set clients after runtimes");
        assert!(self.network_adapters.is_none(), "Cannot set clients after network_adapters");
        self.clients = clients;
        self
    }

    pub fn epoch_config_store(mut self, epoch_config_store: EpochConfigStore) -> Self {
        assert!(self.epoch_config_store.is_none(), "Cannot set epoch_config_store twice");
        self.epoch_config_store = Some(epoch_config_store);
        self
    }

    /// Sets random seed for each client according to the provided HashMap.
    pub fn clients_random_seeds(mut self, seeds: HashMap<AccountId, RngSeed>) -> Self {
        self.seeds = seeds;
        self
    }

    /// Sets number of clients to given one.  To get [`AccountId`] used by the
    /// validator associated with the client the [`TestEnv::get_client_id`]
    /// method can be used.  Tests should not rely on any particular format of
    /// account identifiers used by the builder.  Panics if `num` is zero.
    pub fn clients_count(self, num: usize) -> Self {
        self.clients(Self::make_accounts(num))
    }

    pub fn num_clients(&self) -> usize {
        self.clients.len()
    }

    /// Sets list of validator [`AccountId`]s to the one provided.  Panics if
    /// the vector is empty.
    pub fn validators(mut self, validators: Vec<AccountId>) -> Self {
        assert!(!validators.is_empty());
        assert!(self.epoch_managers.is_none(), "Cannot set validators after epoch_managers");
        self.validators = validators;
        self
    }

    /// Sets number of validator seats to given one.  To get [`AccountId`] used
    /// in the test environment the `validators` field of the built [`TestEnv`]
    /// object can be used.  Tests should not rely on any particular format of
    /// account identifiers used by the builder.  Panics if `num` is zero.
    pub fn validator_seats(self, num: usize) -> Self {
        self.validators(Self::make_accounts(num))
    }

    fn ensure_home_dirs(mut self) -> Self {
        if self.home_dirs.is_none() {
            let home_dirs = (0..self.clients.len())
                .map(|_| {
                    let temp_dir = tempfile::tempdir().unwrap();
                    temp_dir.into_path()
                })
                .collect_vec();
            self.home_dirs = Some(home_dirs)
        }
        self
    }

    /// Overrides the stores that are used to create epoch managers and runtimes.
    pub fn stores(mut self, stores: Vec<Store>) -> Self {
        assert_eq!(stores.len(), self.clients.len());
        assert!(self.stores.is_none(), "Cannot override twice");
        assert!(self.epoch_managers.is_none(), "Cannot override store after epoch_managers");
        assert!(self.runtimes.is_none(), "Cannot override store after runtimes");
        self.stores = Some(stores);
        self
    }

    pub fn contract_caches<C: ContractRuntimeCache>(
        mut self,
        caches: impl IntoIterator<Item = C>,
    ) -> Self {
        assert!(self.contract_caches.is_none(), "Cannot override twice");
        self.contract_caches = Some(caches.into_iter().map(|c| c.handle()).collect());
        assert_eq!(self.contract_caches.as_ref().unwrap().len(), self.clients.len());
        self
    }

    pub fn real_stores(self) -> Self {
        let ret = self.ensure_home_dirs();
        let stores = ret
            .home_dirs
            .as_ref()
            .unwrap()
            .iter()
            .map(|home_dir| {
                // The max number of open files across all RocksDB instances is INT_MAX i.e. 65,535
                // The default value of max_open_files is 10,000 which only allows up to 6 RocksDB
                // instance to open at a time. This is problematic in testing resharding. To overcome
                // this limit, we set the max_open_files config to 1000.
                let mut store_config = StoreConfig::default();
                store_config.max_open_files = 1000;
                NodeStorage::opener(home_dir.as_path(), &store_config, None)
                    .open()
                    .unwrap()
                    .get_hot_store()
            })
            .collect_vec();
        ret.stores(stores)
    }

    /// Internal impl to make sure the stores are initialized.
    fn ensure_stores(self) -> Self {
        let ret = if self.stores.is_some() {
            self
        } else {
            let num_clients = self.clients.len();
            self.stores((0..num_clients).map(|_| create_test_store()).collect())
        };

        if let Some(genesis) = &ret.genesis {
            for store in ret.stores.as_ref().unwrap() {
                initialize_genesis_state(store.clone(), &genesis, None);
            }
        }
        ret
    }

    fn ensure_contract_caches(self) -> Self {
        if self.contract_caches.is_some() {
            return self;
        }
        let count = self.clients.len();
        self.contract_caches((0..count).map(|_| FilesystemContractRuntimeCache::test().unwrap()))
    }

    /// Specifies custom EpochManagerHandle for each client.  This allows us to
    /// construct [`TestEnv`] with a custom implementation.
    ///
    /// The vector must have the same number of elements as they are clients
    /// (one by default).  If that does not hold, [`Self::build`] method will
    /// panic.
    pub fn epoch_managers(mut self, epoch_managers: Vec<Arc<EpochManagerHandle>>) -> Self {
        assert_eq!(epoch_managers.len(), self.clients.len());
        assert!(self.epoch_managers.is_none(), "Cannot override twice");
        assert!(
            self.shard_trackers.is_none(),
            "Cannot override epoch_managers after shard_trackers"
        );
        assert!(self.runtimes.is_none(), "Cannot override epoch_managers after runtimes");
        self.epoch_managers = Some(epoch_managers);
        self
    }

    /// Constructs real EpochManager implementations for each instance.
    pub fn epoch_managers_with_test_overrides(
        self,
        test_overrides: AllEpochConfigTestOverrides,
    ) -> Self {
        let ret = self.ensure_stores();

        // TODO(#11265): consider initializing epoch config separately as it
        // should be decoupled from the genesis config.
        // However, there are a lot of tests which only initialize genesis.
        let mut base_epoch_config: EpochConfig = (&ret.genesis_config).into();
        if let Some(block_producer_kickout_threshold) =
            test_overrides.block_producer_kickout_threshold
        {
            base_epoch_config.block_producer_kickout_threshold = block_producer_kickout_threshold;
        }
        if let Some(chunk_producer_kickout_threshold) =
            test_overrides.chunk_producer_kickout_threshold
        {
            base_epoch_config.chunk_producer_kickout_threshold = chunk_producer_kickout_threshold;
        }
        let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(vec![(
            ret.genesis_config.protocol_version,
            Arc::new(base_epoch_config),
        )]));

        let epoch_managers = (0..ret.clients.len())
            .map(|i| {
                EpochManager::new_arc_handle_from_epoch_config_store(
                    ret.stores.as_ref().unwrap()[i].clone(),
                    &ret.genesis_config,
                    epoch_config_store.clone(),
                )
            })
            .collect();
        ret.epoch_managers(epoch_managers)
    }

    /// Internal impl to make sure EpochManagers are initialized.
    fn ensure_epoch_managers(self) -> Self {
        let ret = self.ensure_stores();
        if ret.epoch_managers.is_some() {
            return ret;
        }
        if let Some(epoch_config_store) = &ret.epoch_config_store {
            let epoch_managers = (0..ret.clients.len())
                .map(|i| {
                    EpochManager::new_arc_handle_from_epoch_config_store(
                        ret.stores.as_ref().unwrap()[i].clone(),
                        &ret.genesis_config,
                        epoch_config_store.clone(),
                    )
                })
                .collect();
            return ret.epoch_managers(epoch_managers);
        }
        ret.epoch_managers_with_test_overrides(AllEpochConfigTestOverrides::default())
    }

    /// Visible for extension methods in integration-tests.
    pub fn internal_initialize_nightshade_runtimes(
        self,
        runtime_configs: Vec<RuntimeConfigStore>,
        trie_configs: Vec<TrieConfig>,
        nightshade_runtime_creator: impl Fn(
            PathBuf,
            Store,
            Box<dyn ContractRuntimeCache>,
            Arc<EpochManagerHandle>,
            RuntimeConfigStore,
            TrieConfig,
        ) -> Arc<NightshadeRuntime>,
    ) -> Self {
        let builder = self
            .ensure_home_dirs()
            .ensure_epoch_managers()
            .ensure_stores()
            .ensure_contract_caches();
        let home_dirs = builder.home_dirs.clone().unwrap();
        let stores = builder.stores.clone().unwrap();
        let contract_caches =
            builder.contract_caches.as_ref().unwrap().iter().map(|c| c.handle()).collect_vec();
        let epoch_managers = builder.epoch_managers.clone().unwrap();
        let runtimes = multizip((
            home_dirs,
            stores,
            contract_caches,
            epoch_managers,
            runtime_configs,
            trie_configs,
        ))
        .map(|(home_dir, store, contract_cache, epoch_manager, runtime_config, trie_config)| {
            nightshade_runtime_creator(
                home_dir,
                store,
                contract_cache,
                epoch_manager,
                runtime_config,
                trie_config,
            )
        })
        .collect();
        builder.runtimes(runtimes)
    }

    /// Specifies custom ShardTracker for each client.  This allows us to
    /// construct [`TestEnv`] with a custom implementation.
    pub fn shard_trackers(mut self, shard_trackers: Vec<ShardTracker>) -> Self {
        assert_eq!(shard_trackers.len(), self.clients.len());
        assert!(self.shard_trackers.is_none(), "Cannot override twice");
        self.shard_trackers = Some(shard_trackers);
        self
    }

    /// Constructs ShardTracker that tracks all shards for each instance.
    ///
    /// Note that in order to track *NO* shards, just don't override shard_trackers.
    pub fn track_all_shards(self) -> Self {
        let ret = self.ensure_epoch_managers();
        let shard_trackers = ret
            .epoch_managers
            .as_ref()
            .unwrap()
            .iter()
            .map(|epoch_manager| {
                ShardTracker::new(TrackedShardsConfig::AllShards, epoch_manager.clone())
            })
            .collect();
        ret.shard_trackers(shard_trackers)
    }

    /// Calls track_all_shards only if the given boolean is true.
    pub fn maybe_track_all_shards(self, track_all_shards: bool) -> Self {
        if track_all_shards { self.track_all_shards() } else { self }
    }

    /// Internal impl to make sure ShardTrackers are initialized.
    fn ensure_shard_trackers(self) -> Self {
        let ret = self.ensure_epoch_managers();
        if ret.shard_trackers.is_some() {
            return ret;
        }
        let shard_trackers = ret
            .epoch_managers
            .as_ref()
            .unwrap()
            .iter()
            .map(|epoch_manager| {
                ShardTracker::new(TrackedShardsConfig::new_empty(), epoch_manager.clone())
            })
            .collect();
        ret.shard_trackers(shard_trackers)
    }

    /// Specifies custom NightshadeRuntime for each client.  This allows us to
    /// construct [`TestEnv`] with a custom implementation.
    pub fn runtimes(mut self, runtimes: Vec<Arc<NightshadeRuntime>>) -> Self {
        assert_eq!(runtimes.len(), self.clients.len());
        assert!(self.runtimes.is_none(), "Cannot override twice");
        self.runtimes = Some(runtimes);
        self
    }

    /// Internal impl to make sure runtimes are initialized.
    fn ensure_runtimes(self) -> Self {
        let ret = self.ensure_epoch_managers();
        if ret.runtimes.is_some() {
            return ret;
        }
        let runtimes = (0..ret.clients.len())
            .map(|i| {
                let store = ret.stores.as_ref().unwrap()[i].clone();
                let epoch_manager = ret.epoch_managers.as_ref().unwrap()[i].clone();
                if ret.genesis.is_none() {
                    // In order to use EpochManagerHandle and NightshadeRuntime stores have to be initialized with genesis
                    // which in default case we can ensure only if builder has information about genesis.
                    panic!("NightshadeRuntime cannot be created automatically in testenv without genesis")
                }
                return NightshadeRuntime::test(
                    Path::new("."),
                    store,
                    &ret.genesis_config,
                    epoch_manager,
                );
            })
            .collect();
        ret.runtimes(runtimes)
    }

    /// Specifies custom network adaptors for each client.
    ///
    /// The vector must have the same number of elements as they are clients
    /// (one by default).  If that does not hold, [`Self::build`] method will
    /// panic.
    pub fn network_adapters(mut self, adapters: Vec<Arc<MockPeerManagerAdapter>>) -> Self {
        self.network_adapters = Some(adapters);
        self
    }

    /// Internal impl to make sure network adapters are initialized.
    fn ensure_network_adapters(self) -> Self {
        if self.network_adapters.is_some() {
            self
        } else {
            let num_clients = self.clients.len();
            self.network_adapters((0..num_clients).map(|_| Arc::new(Default::default())).collect())
        }
    }

    pub fn archive(mut self, archive: bool) -> Self {
        self.archive = archive;
        self
    }

    pub fn save_trie_changes(mut self, save_trie_changes: bool) -> Self {
        self.save_trie_changes = save_trie_changes;
        self
    }

    /// Constructs new `TestEnv` structure.
    ///
    /// If no clients were configured (either through count or vector) one
    /// client is created.  Similarly, if no validator seats were configured,
    /// one seat is configured.
    ///
    /// Panics if `runtime_adapters` or `network_adapters` methods were used and
    /// the length of the vectors passed to them did not equal number of
    /// configured clients.
    pub fn build(self) -> TestEnv {
        self.ensure_shard_trackers().ensure_runtimes().ensure_network_adapters().build_impl()
    }

    fn build_impl(self) -> TestEnv {
        let clock = self.clock.unwrap_or_else(|| Clock::real());
        let chain_genesis = ChainGenesis::new(&self.genesis_config);
        let client_accounts = self.clients.clone();
        let num_clients = client_accounts.len();
        let validators = self.validators;
        let num_validators = validators.len();
        let seeds = self.seeds;
        let epoch_managers = self.epoch_managers.unwrap();
        let shard_trackers = self.shard_trackers.unwrap();
        let runtimes = self.runtimes.unwrap();
        let network_adapters = self.network_adapters.unwrap();
        let client_adapters = (0..num_clients)
            .map(|_| Arc::new(MockClientAdapterForShardsManager::default()))
            .collect_vec();
        let partial_witness_adapters =
            (0..num_clients).map(|_| MockPartialWitnessAdapter::default()).collect_vec();
        let shards_manager_adapters = (0..num_clients)
            .map(|i| {
                let clock = clock.clone();
                let epoch_manager = epoch_managers[i].clone();
                let shard_tracker = shard_trackers[i].clone();
                let runtime = runtimes[i].clone();
                let network_adapter = network_adapters[i].clone();
                let client_adapter = client_adapters[i].clone();
                setup_synchronous_shards_manager(
                    clock,
                    Some(client_accounts[i].clone()),
                    client_adapter.as_sender(),
                    network_adapter.as_multi_sender(),
                    epoch_manager,
                    shard_tracker,
                    runtime,
                    &chain_genesis,
                )
            })
            .collect_vec();
        let clients: Vec<Client> = (0..num_clients)
                .map(|i| {
                    let account_id = client_accounts[i].clone();
                    let network_adapter = network_adapters[i].clone();
                    let partial_witness_adapter = partial_witness_adapters[i].clone();
                    let shards_manager_adapter = shards_manager_adapters[i].clone();
                    let epoch_manager = epoch_managers[i].clone();
                    let shard_tracker = shard_trackers[i].clone();
                    let runtime = runtimes[i].clone();
                    let rng_seed = match seeds.get(&account_id) {
                        Some(seed) => *seed,
                        None => TEST_SEED,
                    };
                    let tries = runtime.get_tries();
                    let make_snapshot_callback = Arc::new(move |_min_chunk_prev_height, _epoch_height, shard_uids: Vec<(ShardIndex, ShardUId)>, block: Block| {
                        let prev_block_hash = *block.header().prev_hash();
                        tracing::info!(target: "state_snapshot", ?prev_block_hash, "make_snapshot_callback");
                        tries.delete_state_snapshot();
                        tries.create_state_snapshot(prev_block_hash, &shard_uids, &block).unwrap();
                    });
                    let tries = runtime.get_tries();
                    let delete_snapshot_callback = Arc::new(move || {
                        tracing::info!(target: "state_snapshot", "delete_snapshot_callback");
                        tries.delete_state_snapshot();
                    });
                    let snapshot_callbacks = SnapshotCallbacks {
                        make_snapshot_callback,
                        delete_snapshot_callback,
                    };
                    let validator_signer = Arc::new(create_test_signer(client_accounts[i].as_str()));
                    setup_client_with_runtime(
                        clock.clone(),
                        u64::try_from(num_validators).unwrap(),
                        false,
                        network_adapter.as_multi_sender(),
                        shards_manager_adapter,
                        chain_genesis.clone(),
                        epoch_manager,
                        shard_tracker,
                        runtime,
                        rng_seed,
                        self.archive,
                        self.save_trie_changes,
                        Some(snapshot_callbacks),
                        partial_witness_adapter.into_multi_sender(),
                        validator_signer,
                        noop().into_multi_sender(),
                    )
                })
                .collect();

        let tx_request_handlers = (0..num_clients)
            .map(|i| {
                setup_tx_request_handler(
                    chain_genesis.clone(),
                    &clients[i],
                    epoch_managers[i].clone(),
                    shard_trackers[i].clone(),
                    runtimes[i].clone(),
                    network_adapters[i].clone().as_multi_sender(),
                )
            })
            .collect();

        TestEnv {
            clock,
            chain_genesis,
            validators,
            network_adapters,
            client_adapters,
            partial_witness_adapters,
            shards_manager_adapters,
            clients,
            rpc_handlers: tx_request_handlers,
            account_indices: AccountIndices(
                self.clients
                    .into_iter()
                    .enumerate()
                    .map(|(index, client)| (client, index))
                    .collect(),
            ),
            paused_blocks: Default::default(),
            seeds,
            archive: self.archive,
            save_trie_changes: self.save_trie_changes,
        }
    }

    pub fn make_accounts(count: usize) -> Vec<AccountId> {
        (0..count).map(|i| format!("test{}", i).parse().unwrap()).collect()
    }

    pub fn use_state_snapshots(mut self) -> Self {
        assert!(self.runtimes.is_none(), "Set up snapshot config before runtimes");
        self.state_snapshot_enabled = true;
        self
    }

    pub fn state_snapshot_enabled(&self) -> bool {
        self.state_snapshot_enabled
    }
}
