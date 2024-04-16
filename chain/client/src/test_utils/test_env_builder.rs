use crate::StateWitnessDistributionActions;

use super::setup::{setup_client_with_runtime, setup_synchronous_shards_manager};
use super::state_witness_distribution_mock::SynchronousStateWitnessDistributionAdapter;
use super::test_env::TestEnv;
use super::{AccountIndices, TEST_SEED};
use actix_rt::System;
use itertools::{multizip, Itertools};
use near_async::messaging::{IntoMultiSender, IntoSender};
use near_async::time::Clock;
use near_chain::state_snapshot_actor::SnapshotCallbacks;
use near_chain::test_utils::{KeyValueRuntime, MockEpochManager, ValidatorSchedule};
use near_chain::types::RuntimeAdapter;
use near_chain::ChainGenesis;
use near_chain_configs::GenesisConfig;
use near_chunks::test_utils::MockClientAdapterForShardsManager;
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_network::test_utils::MockPeerManagerAdapter;
use near_parameters::RuntimeConfigStore;
use near_primitives::epoch_manager::{AllEpochConfigTestOverrides, RngSeed};
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, NumShards};
use near_store::config::StateSnapshotType;
use near_store::test_utils::create_test_store;
use near_store::{NodeStorage, ShardUId, Store, StoreConfig, TrieConfig};
use near_vm_runner::{ContractRuntimeCache, FilesystemContractRuntimeCache};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(derive_more::From, Clone)]
enum EpochManagerKind {
    Mock(Arc<MockEpochManager>),
    Handle(Arc<EpochManagerHandle>),
}

impl EpochManagerKind {
    pub fn into_adapter(self) -> Arc<dyn EpochManagerAdapter> {
        match self {
            Self::Mock(mock) => mock,
            Self::Handle(handle) => handle,
        }
    }
}

/// A builder for the TestEnv structure.
pub struct TestEnvBuilder {
    clock: Option<Clock>,
    genesis_config: GenesisConfig,
    clients: Vec<AccountId>,
    validators: Vec<AccountId>,
    home_dirs: Option<Vec<PathBuf>>,
    stores: Option<Vec<Store>>,
    contract_caches: Option<Vec<Box<dyn ContractRuntimeCache>>>,
    epoch_managers: Option<Vec<EpochManagerKind>>,
    shard_trackers: Option<Vec<ShardTracker>>,
    runtimes: Option<Vec<Arc<dyn RuntimeAdapter>>>,
    network_adapters: Option<Vec<Arc<MockPeerManagerAdapter>>>,
    num_shards: Option<NumShards>,
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
            clients,
            validators,
            home_dirs: None,
            stores: None,
            contract_caches: None,
            epoch_managers: None,
            shard_trackers: None,
            runtimes: None,
            network_adapters: None,
            num_shards: None,
            seeds,
            archive: false,
            save_trie_changes: true,
            state_snapshot_enabled: false,
        }
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
                // The default value of max_open_files is 10,000 which only allows upto 6 RocksDB
                // instance to open at a time. This is problematic in testing resharding. To overcome
                // this limit, we set the max_open_files config to 1000.
                let mut store_config = StoreConfig::default();
                store_config.max_open_files = 1000;
                NodeStorage::opener(home_dir.as_path(), false, &store_config, None)
                    .open()
                    .unwrap()
                    .get_hot_store()
            })
            .collect_vec();
        ret.stores(stores)
    }

    /// Internal impl to make sure the stores are initialized.
    fn ensure_stores(self) -> Self {
        if self.stores.is_some() {
            self
        } else {
            let num_clients = self.clients.len();
            self.stores((0..num_clients).map(|_| create_test_store()).collect())
        }
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
            self.num_shards.is_none(),
            "Cannot set both num_shards and epoch_managers at the same time"
        );
        assert!(
            self.shard_trackers.is_none(),
            "Cannot override epoch_managers after shard_trackers"
        );
        assert!(self.runtimes.is_none(), "Cannot override epoch_managers after runtimes");
        self.epoch_managers =
            Some(epoch_managers.into_iter().map(|epoch_manager| epoch_manager.into()).collect());
        self
    }

    /// Constructs real EpochManager implementations for each instance.
    pub fn epoch_managers_with_test_overrides(
        self,
        test_overrides: Option<AllEpochConfigTestOverrides>,
    ) -> Self {
        assert!(
            self.num_shards.is_none(),
            "Cannot set both num_shards and epoch_managers at the same time"
        );
        let ret = self.ensure_stores();
        let epoch_managers = (0..ret.clients.len())
            .map(|i| {
                EpochManager::new_arc_handle_with_test_overrides(
                    ret.stores.as_ref().unwrap()[i].clone(),
                    &ret.genesis_config,
                    test_overrides.clone(),
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
        ret.epoch_managers_with_test_overrides(None)
    }

    /// Constructs MockEpochManager implementations for each instance.
    pub fn mock_epoch_managers(self) -> Self {
        assert!(self.epoch_managers.is_none(), "Cannot override twice");
        let mut ret = self.ensure_stores();
        let epoch_managers: Vec<EpochManagerKind> = (0..ret.clients.len())
            .map(|i| {
                let vs = ValidatorSchedule::new_with_shards(ret.num_shards.unwrap_or(1))
                    .block_producers_per_epoch(vec![ret.validators.clone()]);
                MockEpochManager::new_with_validators(
                    ret.stores.as_ref().unwrap()[i].clone(),
                    vs,
                    ret.genesis_config.epoch_length,
                )
                .into()
            })
            .collect();
        assert!(
            ret.shard_trackers.is_none(),
            "Cannot override shard_trackers without overriding epoch_managers"
        );
        assert!(
            ret.runtimes.is_none(),
            "Cannot override runtimes without overriding epoch_managers"
        );
        ret.epoch_managers = Some(epoch_managers);
        ret
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
        ) -> Arc<dyn RuntimeAdapter>,
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
            let epoch_manager = match epoch_manager {
                EpochManagerKind::Mock(_) => {
                    panic!("NightshadeRuntime can only be instantiated with EpochManagerHandle")
                }
                EpochManagerKind::Handle(handle) => handle,
            };
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
                ShardTracker::new(TrackedConfig::AllShards, epoch_manager.clone().into_adapter())
            })
            .collect();
        ret.shard_trackers(shard_trackers)
    }

    /// Calls track_all_shards only if the given boolean is true.
    pub fn maybe_track_all_shards(self, track_all_shards: bool) -> Self {
        if track_all_shards {
            self.track_all_shards()
        } else {
            self
        }
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
                ShardTracker::new(TrackedConfig::new_empty(), epoch_manager.clone().into_adapter())
            })
            .collect();
        ret.shard_trackers(shard_trackers)
    }

    /// Specifies custom RuntimeAdapter for each client.  This allows us to
    /// construct [`TestEnv`] with a custom implementation.
    pub fn runtimes(mut self, runtimes: Vec<Arc<dyn RuntimeAdapter>>) -> Self {
        assert_eq!(runtimes.len(), self.clients.len());
        assert!(self.runtimes.is_none(), "Cannot override twice");
        self.runtimes = Some(runtimes);
        self
    }

    /// Internal impl to make sure runtimes are initialized.
    fn ensure_runtimes(self) -> Self {
        let state_snapshot_enabled = self.state_snapshot_enabled;
        let ret = self.ensure_epoch_managers();
        if ret.runtimes.is_some() {
            return ret;
        }
        assert!(
                !state_snapshot_enabled,
                "State snapshot is not supported with KeyValueRuntime. Consider adding nightshade_runtimes"
            );
        let runtimes = (0..ret.clients.len())
            .map(|i| {
                let epoch_manager = match &ret.epoch_managers.as_ref().unwrap()[i] {
                    EpochManagerKind::Mock(mock) => mock.as_ref(),
                    EpochManagerKind::Handle(_) => {
                        panic!("Can only default construct KeyValueRuntime with MockEpochManager")
                    }
                };
                KeyValueRuntime::new(ret.stores.as_ref().unwrap()[i].clone(), epoch_manager)
                    as Arc<dyn RuntimeAdapter>
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

    pub fn num_shards(mut self, num_shards: NumShards) -> Self {
        assert!(
            self.epoch_managers.is_none(),
            "Cannot set both num_shards and epoch_managers at the same time"
        );
        self.num_shards = Some(num_shards);
        self
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
        let clients = self.clients.clone();
        let num_clients = clients.len();
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
                    Some(clients[i].clone()),
                    client_adapter.as_sender(),
                    network_adapter.as_multi_sender(),
                    epoch_manager.into_adapter(),
                    shard_tracker,
                    runtime,
                    &chain_genesis,
                )
            })
            .collect_vec();
        let clients = (0..num_clients)
                .map(|i| {
                    let account_id = clients[i].clone();
                    let network_adapter = network_adapters[i].clone();
                    let shards_manager_adapter = shards_manager_adapters[i].clone();
                    let epoch_manager = epoch_managers[i].clone();
                    let shard_tracker = shard_trackers[i].clone();
                    let runtime = runtimes[i].clone();
                    let rng_seed = match seeds.get(&account_id) {
                        Some(seed) => *seed,
                        None => TEST_SEED,
                    };
                    let tries = runtime.get_tries();
                    let make_snapshot_callback = Arc::new(move |prev_block_hash, _epoch_height, shard_uids: Vec<ShardUId>, block| {
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
                    let validator_signer = Arc::new(create_test_signer(clients[i].as_str()));
                    let state_witness_distribution_adapter = SynchronousStateWitnessDistributionAdapter::new(StateWitnessDistributionActions::new(
                        clock.clone(),
                        network_adapters[i].clone().as_multi_sender(),
                        validator_signer.clone(),
                    ));
                    setup_client_with_runtime(
                        clock.clone(),
                        u64::try_from(num_validators).unwrap(),
                        false,
                        network_adapter.as_multi_sender(),
                        shards_manager_adapter,
                        chain_genesis.clone(),
                        epoch_manager.into_adapter(),
                        shard_tracker,
                        runtime,
                        rng_seed,
                        self.archive,
                        self.save_trie_changes,
                        Some(snapshot_callbacks),
                        state_witness_distribution_adapter.into_multi_sender(),
                        validator_signer,
                    )
                })
                .collect();

        TestEnv {
            clock,
            chain_genesis,
            validators,
            network_adapters,
            client_adapters,
            shards_manager_adapters,
            clients,
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

    fn make_accounts(count: usize) -> Vec<AccountId> {
        (0..count).map(|i| format!("test{}", i).parse().unwrap()).collect()
    }

    pub fn use_state_snapshots(mut self) -> Self {
        assert!(self.runtimes.is_none(), "Set up snapshot config before runtimes");
        self.state_snapshot_enabled = true;
        self
    }

    pub fn state_snapshot_type(&self) -> StateSnapshotType {
        if self.state_snapshot_enabled {
            StateSnapshotType::EveryEpoch
        } else {
            StateSnapshotType::ForReshardingOnly
        }
    }
}
