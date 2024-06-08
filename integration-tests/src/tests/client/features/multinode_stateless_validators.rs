use itertools::Itertools;
use near_async::futures::FutureSpawner;
use near_async::messaging::{
    noop, IntoMultiSender, IntoSender, LateBoundSender, SendAsync, Sender,
};
use near_async::test_loop::data::{TestLoopData, TestLoopDataHandle};
use near_async::test_loop::sender::TestLoopSender;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain::chunks_store::ReadOnlyChunksStore;
use near_chain::state_snapshot_actor::{
    get_delete_snapshot_callback, get_make_snapshot_callback, SnapshotCallbacks, StateSnapshotActor,
};
use near_chain::types::RuntimeAdapter;
use near_chain::ChainGenesis;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_chain_configs::{
    ClientConfig, DumpConfig, ExternalStorageConfig, ExternalStorageLocation, StateSyncConfig,
    SyncConfig,
};
use near_chunks::shards_manager_actor::ShardsManagerActor;
use near_client::client_actor::ClientActorInner;
use near_client::sync_jobs_actor::SyncJobsActor;
use near_client::test_utils::test_loop::sync_actor::test_loop_sync_actor_maker;
use near_client::test_utils::test_loop::ClientQueries;
use near_client::{Client, PartialWitnessActor, SyncAdapter};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_network::client::ProcessTxRequest;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::state_witness::PartialWitnessSenderForNetwork;
use near_network::test_loop::{ClientSenderForTestLoopNetwork, TestLoopPeerManagerActor};
use near_o11y::testonly::init_test_logger;
use near_primitives::network::PeerId;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId, ValidatorInfoIdentifier};
use near_primitives::version::ProtocolFeature::StatelessValidationV0;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::CurrentEpochValidatorInfo;
use near_store::config::StateSnapshotType;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use near_store::{StoreConfig, TrieConfig};
use near_vm_runner::ContractRuntimeCache;
use near_vm_runner::FilesystemContractRuntimeCache;
use nearcore::state_sync::StateSyncDumper;
use nearcore::NightshadeRuntime;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

const NUM_ACCOUNTS: usize = 20;
const NUM_SHARDS: u64 = 4;
const EPOCH_LENGTH: u64 = 12;
const NETWORK_DELAY: Duration = Duration::milliseconds(10);

const NUM_BLOCK_AND_CHUNK_PRODUCERS: usize = 4;
const NUM_CHUNK_VALIDATORS_ONLY: usize = 4;
const NUM_VALIDATORS: usize = NUM_BLOCK_AND_CHUNK_PRODUCERS + NUM_CHUNK_VALIDATORS_ONLY;

struct TestData {
    pub account_id: AccountId,
    pub client_sender: TestLoopSender<ClientActorInner>,
    pub shards_manager_sender: TestLoopSender<ShardsManagerActor>,
    pub partial_witness_sender: TestLoopSender<PartialWitnessActor>,
    pub state_sync_dumper_handle: TestLoopDataHandle<StateSyncDumper>,
    pub network_adapter: Arc<LateBoundSender<TestLoopSender<TestLoopPeerManagerActor>>>,
}

impl From<&TestData> for AccountId {
    fn from(data: &TestData) -> AccountId {
        data.account_id.clone()
    }
}

impl From<&TestData> for ClientSenderForTestLoopNetwork {
    fn from(data: &TestData) -> ClientSenderForTestLoopNetwork {
        data.client_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&TestData> for PartialWitnessSenderForNetwork {
    fn from(data: &TestData) -> PartialWitnessSenderForNetwork {
        data.partial_witness_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&TestData> for Sender<ShardsManagerRequestFromNetwork> {
    fn from(data: &TestData) -> Sender<ShardsManagerRequestFromNetwork> {
        data.shards_manager_sender.clone().with_delay(NETWORK_DELAY).into_sender()
    }
}

#[test]
fn test_stateless_validators_with_multi_test_loop() {
    if !StatelessValidationV0.enabled(PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    init_test_logger();
    let mut test_loop = TestLoopV2::new();

    let initial_balance = 10000 * ONE_NEAR;
    let accounts = (0..NUM_ACCOUNTS)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();

    // All block_and_chunk_producers will be both block and chunk validators.
    let block_and_chunk_producers =
        (0..NUM_BLOCK_AND_CHUNK_PRODUCERS).map(|idx| accounts[idx].as_str()).collect::<Vec<_>>();
    // These are the accounts that are only chunk validators, but not block/chunk producers.
    let chunk_validators_only = (NUM_BLOCK_AND_CHUNK_PRODUCERS..NUM_VALIDATORS)
        .map(|idx| accounts[idx].as_str())
        .collect::<Vec<_>>();

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&test_loop.clock())
        .protocol_version_latest()
        .genesis_height(10000)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        .transaction_validity_period(1000)
        .epoch_length(EPOCH_LENGTH)
        .validators_desired_roles(&block_and_chunk_producers, &chunk_validators_only)
        .shuffle_shard_assignment_for_chunk_producers(true);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let tempdir = tempfile::tempdir().unwrap();
    let mut node_datas = Vec::new();
    for idx in 0..NUM_VALIDATORS {
        let client_adapter = LateBoundSender::new();
        let network_adapter = LateBoundSender::new();
        let state_snapshot_adapter = LateBoundSender::new();
        let shards_manager_adapter = LateBoundSender::new();
        let partial_witness_adapter = LateBoundSender::new();
        let sync_jobs_adapter = LateBoundSender::new();

        let mut client_config = ClientConfig::test(true, 600, 2000, 4, false, true, false, false);
        client_config.max_block_wait_delay = Duration::seconds(6);
        client_config.state_sync_enabled = true;
        client_config.state_sync_timeout = Duration::milliseconds(100);
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
            }),
        };
        client_config.tracked_shards = Vec::new();

        let homedir = tempdir.path().join(format!("{}", idx));
        std::fs::create_dir_all(&homedir).expect("Unable to create homedir");

        let store_config = StoreConfig {
            path: Some(homedir.clone()),
            load_mem_tries_for_tracked_shards: true,
            ..Default::default()
        };
        let store = create_test_store();
        initialize_genesis_state(store.clone(), &genesis, None);

        let sync_jobs_actor = SyncJobsActor::new(client_adapter.as_multi_sender());
        let chain_genesis = ChainGenesis::new(&genesis.config);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let shard_tracker =
            ShardTracker::new(TrackedConfig::from_config(&client_config), epoch_manager.clone());

        let state_sync_adapter = Arc::new(RwLock::new(SyncAdapter::new(
            client_adapter.as_sender(),
            network_adapter.as_sender(),
            test_loop_sync_actor_maker(test_loop.sender()),
        )));
        let contract_cache = FilesystemContractRuntimeCache::new(&homedir, None::<&str>)
            .expect("filesystem contract cache")
            .handle();
        let runtime_adapter = NightshadeRuntime::test_with_trie_config(
            &homedir,
            store.clone(),
            contract_cache,
            &genesis.config,
            epoch_manager.clone(),
            None,
            TrieConfig::from_store_config(&store_config),
            StateSnapshotType::EveryEpoch,
        );

        let state_snapshot = StateSnapshotActor::new(
            runtime_adapter.get_flat_storage_manager(),
            network_adapter.as_multi_sender(),
            runtime_adapter.get_tries(),
            state_snapshot_adapter.as_multi_sender(),
        );

        let delete_snapshot_callback =
            get_delete_snapshot_callback(state_snapshot_adapter.as_multi_sender());
        let make_snapshot_callback = get_make_snapshot_callback(
            state_snapshot_adapter.as_multi_sender(),
            runtime_adapter.get_flat_storage_manager(),
        );
        let snapshot_callbacks =
            SnapshotCallbacks { make_snapshot_callback, delete_snapshot_callback };

        let validator_signer = Arc::new(create_test_signer(accounts[idx].as_str()));
        let client = Client::new(
            test_loop.clock(),
            client_config.clone(),
            chain_genesis.clone(),
            epoch_manager.clone(),
            shard_tracker.clone(),
            state_sync_adapter,
            runtime_adapter.clone(),
            network_adapter.as_multi_sender(),
            shards_manager_adapter.as_sender(),
            Some(validator_signer.clone()),
            true,
            [0; 32],
            Some(snapshot_callbacks),
            Arc::new(test_loop.async_computation_spawner(|_| Duration::milliseconds(80))),
            partial_witness_adapter.as_multi_sender(),
        )
        .unwrap();

        let shards_manager = ShardsManagerActor::new(
            test_loop.clock(),
            Some(accounts[idx].clone()),
            epoch_manager.clone(),
            shard_tracker.clone(),
            network_adapter.as_sender(),
            client_adapter.as_sender(),
            ReadOnlyChunksStore::new(store.clone()),
            client.chain.head().unwrap(),
            client.chain.header_head().unwrap(),
            Duration::milliseconds(100),
        );

        let client_actor = ClientActorInner::new(
            test_loop.clock(),
            client,
            client_adapter.as_multi_sender(),
            client_config.clone(),
            PeerId::random(),
            network_adapter.as_multi_sender(),
            None,
            noop().into_sender(),
            None,
            Default::default(),
            None,
            sync_jobs_adapter.as_multi_sender(),
            Box::new(test_loop.future_spawner()),
        )
        .unwrap();

        let partial_witness_actions = PartialWitnessActor::new(
            test_loop.clock(),
            network_adapter.as_multi_sender(),
            client_adapter.as_multi_sender(),
            validator_signer,
            epoch_manager.clone(),
            store,
        );

        let future_spawner = test_loop.future_spawner();
        let state_sync_dumper = StateSyncDumper {
            clock: test_loop.clock(),
            client_config,
            chain_genesis,
            epoch_manager,
            shard_tracker,
            runtime: runtime_adapter,
            account_id: Some(accounts[idx].clone()),
            dump_future_runner: Box::new(move |future| {
                future_spawner.spawn_boxed("state_sync_dumper", future);
                Box::new(|| {})
            }),
            handle: None,
        };
        let state_sync_dumper_handle = test_loop.data.register_data(state_sync_dumper);

        let client_sender = test_loop.register_actor(client_actor, Some(client_adapter));
        let shards_manager_sender =
            test_loop.register_actor(shards_manager, Some(shards_manager_adapter));
        let partial_witness_sender =
            test_loop.register_actor(partial_witness_actions, Some(partial_witness_adapter));
        test_loop.register_actor(sync_jobs_actor, Some(sync_jobs_adapter));
        test_loop.register_actor(state_snapshot, Some(state_snapshot_adapter));

        let data = TestData {
            account_id: accounts[idx].clone(),
            client_sender,
            shards_manager_sender,
            partial_witness_sender,
            state_sync_dumper_handle,
            network_adapter,
        };
        node_datas.push(data);
    }

    // Bootstrap the test by starting the components.
    for idx in 0..NUM_VALIDATORS {
        let state_sync_dumper_handle = node_datas[idx].state_sync_dumper_handle.clone();
        test_loop.send_adhoc_event("start_state_sync_dumper".to_owned(), move |test_loop_data| {
            test_loop_data.get_mut(&state_sync_dumper_handle).start().unwrap();
        });
    }

    for idx in 0..NUM_VALIDATORS {
        let peer_manager_actor =
            TestLoopPeerManagerActor::new(test_loop.clock(), &accounts[idx], &node_datas);
        test_loop.register_actor(peer_manager_actor, Some(node_datas[idx].network_adapter.clone()));
    }

    // Give it some condition to stop running at. Here we run the test until the first client
    // reaches height 10003, with a timeout of 5sec (failing if it doesn't reach 10003 in time).
    let client_handle = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data| {
            let client_actor = test_loop_data.get(&client_handle);
            client_actor.client.chain.head().unwrap().height == 10003
        },
        Duration::seconds(5),
    );
    for idx in 0..NUM_VALIDATORS {
        let client_handle = node_datas[idx].client_sender.actor_handle();
        let event = move |test_loop_data: &mut TestLoopData| {
            let client_actor = test_loop_data.get(&client_handle);
            let block = client_actor.client.chain.get_block_by_height(10002).unwrap();
            assert_eq!(block.header().chunk_mask(), &(0..NUM_SHARDS).map(|_| true).collect_vec());
        };
        test_loop.send_adhoc_event("assertions".to_owned(), Box::new(event));
    }
    test_loop.run_instant();

    // Capture the initial validator info in the first epoch.
    let chain = &test_loop.data.get(&client_handle).client.chain;
    let initial_epoch_id = chain.head().unwrap().epoch_id;
    let mut balances = accounts
        .iter()
        .cloned()
        .map(|account| (account, initial_balance))
        .collect::<HashMap<_, _>>();
    let anchor_hash = *chain.get_block_by_height(10002).unwrap().hash();

    // Run send-money transactions between "non-validator" accounts.
    for i in NUM_VALIDATORS..NUM_ACCOUNTS {
        let amount = ONE_NEAR * (i as u128 + 1);
        let tx = SignedTransaction::send_money(
            1,
            accounts[i].clone(),
            accounts[(i + 1) % NUM_ACCOUNTS].clone(),
            &create_user_test_signer(&accounts[i]),
            amount,
            anchor_hash,
        );
        *balances.get_mut(&accounts[i]).unwrap() -= amount;
        *balances.get_mut(&accounts[(i + 1) % NUM_ACCOUNTS]).unwrap() += amount;
        let _ = node_datas[i % NUM_VALIDATORS]
            .client_sender
            .clone()
            .with_delay(Duration::milliseconds(300 * i as i64))
            .send_async(ProcessTxRequest {
                transaction: tx,
                is_forwarded: false,
                check_only: false,
            });
    }

    // Run the chain some time to allow transactions be processed.
    test_loop.run_for(Duration::seconds(20));

    // Capture the id of the epoch we will check for the correct validator information in assert_validator_info.
    let prev_epoch_id = test_loop.data.get(&client_handle).client.chain.head().unwrap().epoch_id;
    assert_ne!(prev_epoch_id, initial_epoch_id);

    // Run the chain until it transitions to a different epoch then prev_epoch_id.
    test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().epoch_id
                != prev_epoch_id
        },
        Duration::seconds(EPOCH_LENGTH as i64),
    );

    // Check that the balances are correct.
    let clients = node_datas
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    for i in NUM_VALIDATORS..NUM_ACCOUNTS {
        let account = &accounts[i];
        assert_eq!(
            clients.query_balance(account),
            *balances.get(account).unwrap(),
            "Account balance mismatch for account {}",
            account
        );
    }

    for idx in 0..NUM_VALIDATORS {
        test_loop.data.get_mut(&node_datas[idx].state_sync_dumper_handle).stop();
    }

    // Check the validator information for the epoch with the prev_epoch_id.
    assert_validator_info(
        &test_loop.data.get(&client_handle).client,
        prev_epoch_id,
        initial_epoch_id,
        &accounts,
    );

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    test_loop.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Returns the CurrentEpochValidatorInfo for each validator account for the given epoch id.
fn get_current_validators(
    client: &Client,
    epoch_id: EpochId,
) -> HashMap<AccountId, CurrentEpochValidatorInfo> {
    client
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::EpochId(epoch_id))
        .unwrap()
        .current_validators
        .iter()
        .map(|v| (v.account_id.clone(), v.clone()))
        .collect()
}

/// Asserts the following:
/// 1. Block and chunk producers produce block and chunks and also validate chunk witnesses.
/// 2. Chunk validators only validate chunk witnesses.
/// 3. Stake of both the block/chunk producers and chunk validators increase (due to rewards).
/// TODO: Assert on the specific reward amount, currently it only checks that some amount is rewarded.
fn assert_validator_info(
    client: &Client,
    epoch_id: EpochId,
    initial_epoch_id: EpochId,
    accounts: &Vec<AccountId>,
) {
    let validator_to_info = get_current_validators(client, epoch_id);
    let initial_validator_to_info = get_current_validators(client, initial_epoch_id);

    // Check that block/chunk producers generate blocks/chunks and also endorse chunk witnesses.
    for idx in 0..NUM_BLOCK_AND_CHUNK_PRODUCERS {
        let account = &accounts[idx];
        let validator_info = validator_to_info.get(account).unwrap();
        assert!(validator_info.num_produced_blocks > 0);
        assert!(validator_info.num_produced_blocks <= validator_info.num_expected_blocks);
        assert!(validator_info.num_expected_blocks < EPOCH_LENGTH);

        assert!(0 < validator_info.num_produced_chunks);
        assert!(validator_info.num_produced_chunks <= validator_info.num_expected_chunks);
        assert!(validator_info.num_expected_chunks < EPOCH_LENGTH * NUM_SHARDS);

        assert!(validator_info.num_produced_endorsements > 0);
        assert!(
            validator_info.num_produced_endorsements <= validator_info.num_expected_endorsements
        );
        assert!(validator_info.num_expected_endorsements <= EPOCH_LENGTH * NUM_SHARDS);

        let initial_validator_info = initial_validator_to_info.get(account).unwrap();
        assert!(initial_validator_info.stake < validator_info.stake);
    }
    // Check chunk validators only endorse chunk witnesses.
    for idx in NUM_BLOCK_AND_CHUNK_PRODUCERS..NUM_VALIDATORS {
        let account = &accounts[idx];
        let validator_info = validator_to_info.get(account).unwrap();
        assert_eq!(validator_info.num_expected_blocks, 0);
        assert_eq!(validator_info.num_expected_chunks, 0);
        assert_eq!(validator_info.num_produced_blocks, 0);
        assert_eq!(validator_info.num_produced_chunks, 0);

        assert!(validator_info.num_produced_endorsements > 0);
        assert!(
            validator_info.num_produced_endorsements <= validator_info.num_expected_endorsements
        );
        assert!(validator_info.num_expected_endorsements <= EPOCH_LENGTH * NUM_SHARDS);

        let initial_validator_info = initial_validator_to_info.get(account).unwrap();
        assert!(initial_validator_info.stake < validator_info.stake);
    }
}
