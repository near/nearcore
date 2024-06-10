use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::futures::FutureSpawner;
use near_async::messaging::{noop, IntoMultiSender, IntoSender, MessageWithCallback, SendAsync};
use near_async::test_loop::adhoc::{handle_adhoc_events, AdhocEvent, AdhocEventSender};
use near_async::test_loop::event_handler::ignore_events;
use near_async::test_loop::futures::{
    drive_async_computations, drive_futures, TestLoopAsyncComputationEvent,
    TestLoopDelayedActionEvent, TestLoopTask,
};
use near_async::test_loop::TestLoopBuilder;
use near_async::time::Duration;
use near_chain::chunks_store::ReadOnlyChunksStore;
use near_chain::state_snapshot_actor::{
    get_delete_snapshot_callback, get_make_snapshot_callback, SnapshotCallbacks,
    StateSnapshotActor, StateSnapshotSenderForClient, StateSnapshotSenderForClientMessage,
    StateSnapshotSenderForStateSnapshot, StateSnapshotSenderForStateSnapshotMessage,
};
use near_chain::test_utils::test_loop::{
    forward_state_snapshot_messages_from_client,
    forward_state_snapshot_messages_from_state_snapshot,
};
use near_chain::types::RuntimeAdapter;
use near_chain::ChainGenesis;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_chain_configs::{
    ClientConfig, DumpConfig, ExternalStorageConfig, ExternalStorageLocation, StateSyncConfig,
    SyncConfig,
};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::client::ShardsManagerResponse;
use near_chunks::shards_manager_actor::ShardsManagerActor;
use near_chunks::test_loop::{
    forward_client_request_to_shards_manager, forward_network_request_to_shards_manager,
    route_shards_manager_network_messages,
};
use near_client::client_actor::{
    ClientActorInner, ClientSenderForClientMessage, ClientSenderForPartialWitnessMessage,
    SyncJobsSenderForClientMessage,
};
use near_client::sync::sync_actor::SyncActor;
use near_client::sync_jobs_actor::{ClientSenderForSyncJobsMessage, SyncJobsActor};
use near_client::test_utils::test_loop::client_actor::{
    forward_client_messages_from_client_to_client_actor,
    forward_client_messages_from_network_to_client_actor,
    forward_client_messages_from_shards_manager, forward_client_messages_from_sync_adapter,
    forward_client_messages_from_sync_jobs_to_client_actor,
};
use near_client::test_utils::test_loop::partial_witness_actor::{
    forward_messages_from_client_to_partial_witness_actor,
    forward_messages_from_network_to_partial_witness_actor,
};
use near_client::test_utils::test_loop::sync_actor::{
    forward_sync_actor_messages_from_client, forward_sync_actor_messages_from_network,
    test_loop_sync_actor_maker, TestSyncActors,
};
use near_client::test_utils::test_loop::sync_jobs_actor::forward_messages_from_client_to_sync_jobs_actor;
use near_client::test_utils::test_loop::{
    forward_messages_from_partial_witness_actor_to_client,
    print_basic_client_info_before_each_event,
};
use near_client::test_utils::test_loop::{route_network_messages_to_client, ClientQueries};
use near_client::{
    Client, PartialWitnessActor, PartialWitnessSenderForClientMessage, SyncAdapter, SyncMessage,
};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_network::client::{
    ClientSenderForNetwork, ClientSenderForNetworkMessage, ProcessTxRequest,
};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::state_sync::StateSyncResponse;
use near_network::state_witness::PartialWitnessSenderForNetworkMessage;
use near_network::types::{PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo};
use near_primitives::network::PeerId;
use near_primitives::shard_layout::ShardUId;
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
use std::sync::{Arc, Mutex, RwLock};

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct TestData {
    pub dummy: (),
    pub account: AccountId,
    pub client: ClientActorInner,
    pub sync_jobs: SyncJobsActor,
    pub shards_manager: ShardsManagerActor,
    pub partial_witness: PartialWitnessActor,
    pub sync_actors: TestSyncActors,
    pub state_sync_dumper: StateSyncDumper,
    pub state_snapshot: StateSnapshotActor,
}

impl AsMut<TestData> for TestData {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

impl AsRef<Client> for TestData {
    fn as_ref(&self) -> &Client {
        &self.client.client
    }
}

#[derive(EnumTryInto, Debug, EnumFrom)]
#[allow(clippy::large_enum_variant)]
enum TestEvent {
    /// Allows futures to be spawn and executed.
    Task(Arc<TestLoopTask>),
    /// Allows adhoc events to be used for the test (only used inside this file).
    Adhoc(AdhocEvent<TestData>),
    /// Allows asynchronous computation (chunk application, stateless validation, etc.).
    AsyncComputation(TestLoopAsyncComputationEvent),

    /// Allows delayed actions to be posted, as if ClientActor scheduled them, e.g. timers.
    ClientDelayedActions(TestLoopDelayedActionEvent<ClientActorInner>),
    /// Allows delayed actions to be posted, as if ShardsManagerActor scheduled them, e.g. timers.
    ShardsManagerDelayedActions(TestLoopDelayedActionEvent<ShardsManagerActor>),
    /// Allows delayed actions to be posted, as if SyncJobsActor scheduled them, e.g. timers.
    SyncJobsDelayedActions(TestLoopDelayedActionEvent<SyncJobsActor>),

    /// Message that the network layer sends to the client.
    ClientEventFromNetwork(ClientSenderForNetworkMessage),
    /// Message that the client sends to the client itself.
    ClientEventFromClient(ClientSenderForClientMessage),
    /// Message that the SyncJobs component sends to the client.
    ClientEventFromSyncJobs(ClientSenderForSyncJobsMessage),
    /// Message that the ShardsManager component sends to the client.
    ClientEventFromShardsManager(ShardsManagerResponse),
    /// Message that the state sync adapter sends to the client.
    ClientEventFromStateSyncAdapter(SyncMessage),

    /// Message that the client sends to the SyncJobs component.
    SyncJobsEventFromClient(SyncJobsSenderForClientMessage),

    /// Message that the client sends to the SyncActor component.
    SyncActorEventFromClient((ShardUId, SyncMessage)),
    /// Message that the network sends to the SyncActor component.
    SyncActorEventFromNetwork((ShardUId, StateSyncResponse)),

    /// Message that the client sends to the ShardsManager component.
    ShardsManagerRequestFromClient(ShardsManagerRequestFromClient),
    /// Message that the network layer sends to the ShardsManager component.
    ShardsManagerRequestFromNetwork(ShardsManagerRequestFromNetwork),

    /// Message that the client sends to StateSnapshotActor.
    StateSnapshotRequestFromClient(StateSnapshotSenderForClientMessage),
    /// Message that the StateSnapshotActor sends to itself.
    StateSnapshotRequestFromStateSnapshot(StateSnapshotSenderForStateSnapshotMessage),

    /// Outgoing network message that is sent by any of the components of this node.
    OutgoingNetworkMessage(PeerManagerMessageRequest),
    /// Same as OutgoingNetworkMessage, but of the variant that requests a response.
    OutgoingNetworkMessageForResult(
        MessageWithCallback<PeerManagerMessageRequest, PeerManagerMessageResponse>,
    ),
    /// Calls to the network component to set chain info.
    SetChainInfo(SetChainInfo),
    /// Message from Client to PartialWitnessActor.
    PartialWitnessSenderForClient(PartialWitnessSenderForClientMessage),
    /// Message from Network to PartialWitnessActor.
    PartialWitnessSenderForNetwork(PartialWitnessSenderForNetworkMessage),
    /// Message from PartialWitnessActor to Client.
    ClientSenderForPartialWitness(ClientSenderForPartialWitnessMessage),
}

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

const NUM_ACCOUNTS: usize = 20;
const NUM_SHARDS: u64 = 4;
const EPOCH_LENGTH: u64 = 12;
const NETWORK_DELAY: Duration = Duration::milliseconds(10);

const NUM_BLOCK_AND_CHUNK_PRODUCERS: usize = 4;
const NUM_CHUNK_VALIDATORS_ONLY: usize = 4;
const NUM_VALIDATORS: usize = NUM_BLOCK_AND_CHUNK_PRODUCERS + NUM_CHUNK_VALIDATORS_ONLY;

#[test]
fn test_stateless_validators_with_multi_test_loop() {
    if !StatelessValidationV0.enabled(PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let builder = TestLoopBuilder::<(usize, TestEvent)>::new();

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
        .genesis_time_from_clock(&builder.clock())
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
    let mut datas = Vec::new();
    for idx in 0..NUM_VALIDATORS {
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

        let sync_jobs_actor = SyncJobsActor::new(
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<ClientSenderForSyncJobsMessage, _>(),
        );
        let chain_genesis = ChainGenesis::new(&genesis.config);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let shard_tracker =
            ShardTracker::new(TrackedConfig::from_config(&client_config), epoch_manager.clone());

        let sync_actors = Arc::new(Mutex::new(HashMap::<ShardUId, SyncActor>::new()));
        let state_sync_adapter = Arc::new(RwLock::new(SyncAdapter::new(
            builder.sender().for_index(idx).into_sender(),
            builder.sender().for_index(idx).into_sender(),
            test_loop_sync_actor_maker(builder.sender().for_index(idx), sync_actors.clone()),
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
            builder.sender().for_index(idx).into_multi_sender(),
            runtime_adapter.get_tries(),
            builder.sender().for_index(idx).into_wrapped_multi_sender::<StateSnapshotSenderForStateSnapshotMessage, StateSnapshotSenderForStateSnapshot>(),
        );

        let delete_snapshot_callback = get_delete_snapshot_callback(
            builder.sender().for_index(idx).into_wrapped_multi_sender::<StateSnapshotSenderForClientMessage, StateSnapshotSenderForClient>(),
        );
        let make_snapshot_callback = get_make_snapshot_callback(
            builder.sender().for_index(idx).into_wrapped_multi_sender::<StateSnapshotSenderForClientMessage, StateSnapshotSenderForClient>(),
            runtime_adapter.get_flat_storage_manager(),
        );
        let snapshot_callbacks =
            SnapshotCallbacks { make_snapshot_callback, delete_snapshot_callback };

        let validator_signer = Arc::new(create_test_signer(accounts[idx].as_str()));
        let client = Client::new(
            builder.clock(),
            client_config.clone(),
            chain_genesis.clone(),
            epoch_manager.clone(),
            shard_tracker.clone(),
            state_sync_adapter,
            runtime_adapter.clone(),
            builder.sender().for_index(idx).into_multi_sender(),
            builder.sender().for_index(idx).into_sender(),
            Some(validator_signer.clone()),
            true,
            [0; 32],
            Some(snapshot_callbacks),
            Arc::new(
                builder
                    .sender()
                    .for_index(idx)
                    .into_async_computation_spawner(|_| Duration::milliseconds(80)),
            ),
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<PartialWitnessSenderForClientMessage, _>(),
        )
        .unwrap();

        let shards_manager = ShardsManagerActor::new(
            builder.clock(),
            Some(accounts[idx].clone()),
            epoch_manager.clone(),
            shard_tracker.clone(),
            builder.sender().for_index(idx).into_sender(),
            builder.sender().for_index(idx).into_sender(),
            ReadOnlyChunksStore::new(store.clone()),
            client.chain.head().unwrap(),
            client.chain.header_head().unwrap(),
            Duration::milliseconds(100),
        );

        let client_actor = ClientActorInner::new(
            builder.clock(),
            client,
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<ClientSenderForClientMessage, _>(),
            client_config.clone(),
            PeerId::random(),
            builder.sender().for_index(idx).into_multi_sender(),
            None,
            noop().into_sender(),
            None,
            Default::default(),
            None,
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<SyncJobsSenderForClientMessage, _>(),
            Box::new(builder.sender().for_index(idx).into_future_spawner()),
        )
        .unwrap();

        let partial_witness_actions = PartialWitnessActor::new(
            builder.clock(),
            builder.sender().for_index(idx).into_multi_sender(),
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<ClientSenderForPartialWitnessMessage, _>(),
            validator_signer,
            epoch_manager.clone(),
            store,
        );

        let future_spawner = builder.sender().for_index(idx).into_future_spawner();
        let state_sync_dumper = StateSyncDumper {
            clock: builder.clock(),
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

        let data = TestData {
            dummy: (),
            account: accounts[idx].clone(),
            client: client_actor,
            sync_jobs: sync_jobs_actor,
            shards_manager,
            partial_witness: partial_witness_actions,
            sync_actors,
            state_sync_dumper,
            state_snapshot,
        };
        datas.push(data);
    }

    let mut test = builder.build(datas);
    for idx in 0..NUM_VALIDATORS {
        // Handlers that do nothing but print some information.
        test.register_handler(print_basic_client_info_before_each_event(Some(idx)).for_index(idx));

        // Futures, adhoc events, async computations.
        test.register_handler(drive_futures().widen().for_index(idx));
        test.register_handler(handle_adhoc_events::<TestData>().widen().for_index(idx));
        test.register_handler(drive_async_computations().widen().for_index(idx));

        // Delayed actions.
        test.register_delayed_action_handler_for_index::<ClientActorInner>(idx);
        test.register_delayed_action_handler_for_index::<ShardsManagerActor>(idx);

        // Messages to the client.
        test.register_handler(
            forward_client_messages_from_network_to_client_actor().widen().for_index(idx),
        );
        test.register_handler(
            forward_client_messages_from_client_to_client_actor().widen().for_index(idx),
        );
        test.register_handler(
            forward_client_messages_from_sync_jobs_to_client_actor().widen().for_index(idx),
        );
        test.register_handler(forward_client_messages_from_shards_manager().widen().for_index(idx));
        test.register_handler(
            forward_messages_from_partial_witness_actor_to_client().widen().for_index(idx),
        );
        test.register_handler(forward_client_messages_from_sync_adapter().widen().for_index(idx));

        // Messages to the SyncJobs component.
        test.register_handler(
            forward_messages_from_client_to_sync_jobs_actor(
                test.sender().for_index(idx).into_delayed_action_runner(test.shutting_down()),
            )
            .widen()
            .for_index(idx),
        );

        // Messages to the SyncActor component.
        test.register_handler(forward_sync_actor_messages_from_client().widen().for_index(idx));
        test.register_handler(forward_sync_actor_messages_from_network().widen().for_index(idx));

        // Messages to the ShardsManager component.
        test.register_handler(forward_client_request_to_shards_manager().widen().for_index(idx));
        test.register_handler(forward_network_request_to_shards_manager().widen().for_index(idx));

        // Messages to the StateSnapshotActor component.
        test.register_handler(
            forward_state_snapshot_messages_from_state_snapshot().widen().for_index(idx),
        );
        test.register_handler(forward_state_snapshot_messages_from_client().widen().for_index(idx));

        // Messages to the network layer; multi-node messages are handled below.
        test.register_handler(ignore_events::<SetChainInfo>().widen().for_index(idx));

        // Messages to PartialWitnessActor.
        test.register_handler(
            forward_messages_from_client_to_partial_witness_actor().widen().for_index(idx),
        );
        test.register_handler(
            forward_messages_from_network_to_partial_witness_actor().widen().for_index(idx),
        );
    }
    // Handles network routing. Outgoing messages are handled by emitting incoming messages to the
    // appropriate component of the appropriate node index.
    test.register_handler(route_network_messages_to_client(test.sender(), NETWORK_DELAY));
    test.register_handler(route_shards_manager_network_messages(
        test.sender(),
        test.clock(),
        NETWORK_DELAY,
    ));

    // Bootstrap the test by starting the components.
    // We use adhoc events for these, just so that the visualizer can see these as events rather
    // than happening outside of the TestLoop framework. Other than that, we could also just remove
    // the send_adhoc_event part and the test would still work.
    for idx in 0..NUM_VALIDATORS {
        let sender = test.sender().for_index(idx);
        let shutting_down = test.shutting_down();
        test.sender().for_index(idx).send_adhoc_event("start_client", move |data| {
            data.client.start(&mut sender.into_delayed_action_runner(shutting_down));
        });

        let sender = test.sender().for_index(idx);
        let shutting_down = test.shutting_down();
        test.sender().for_index(idx).send_adhoc_event("start_shards_manager", move |data| {
            data.shards_manager.periodically_resend_chunk_requests(
                &mut sender.into_delayed_action_runner(shutting_down),
            );
        });

        test.sender().for_index(idx).send_adhoc_event("start_state_sync_dumper", move |data| {
            data.state_sync_dumper.start().unwrap();
        });
    }

    // Give it some condition to stop running at. Here we run the test until the first client
    // reaches height 10003, with a timeout of 5sec (failing if it doesn't reach 10003 in time).
    test.run_until(
        |data| data[0].client.client.chain.head().unwrap().height == 10003,
        Duration::seconds(5),
    );
    for idx in 0..NUM_VALIDATORS {
        test.sender().for_index(idx).send_adhoc_event("assertions", |data| {
            let chain = &data.client.client.chain;
            let block = chain.get_block_by_height(10002).unwrap();
            assert_eq!(
                block.header().chunk_mask(),
                &(0..NUM_SHARDS).map(|_| true).collect::<Vec<_>>()
            );
        })
    }
    test.run_instant();

    // Capture the initial validator info in the first epoch.
    let initial_epoch_id = test.data[0].client.client.chain.head().unwrap().epoch_id;

    let mut balances = accounts
        .iter()
        .cloned()
        .map(|account| (account, initial_balance))
        .collect::<HashMap<_, _>>();

    let anchor_hash = *test.data[0].client.client.chain.get_block_by_height(10002).unwrap().hash();

    // Run send-money transactions between "non-validator" accounts.
    for i in NUM_VALIDATORS..NUM_ACCOUNTS {
        let amount = ONE_NEAR * (i as u128 + 1);
        let tx = SignedTransaction::send_money(
            1,
            accounts[i].clone(),
            accounts[(i + 1) % NUM_ACCOUNTS].clone(),
            &create_user_test_signer(&accounts[i]).into(),
            amount,
            anchor_hash,
        );
        *balances.get_mut(&accounts[i]).unwrap() -= amount;
        *balances.get_mut(&accounts[(i + 1) % NUM_ACCOUNTS]).unwrap() += amount;
        drop(
            test.sender()
                .for_index(i % NUM_VALIDATORS)
                .with_additional_delay(Duration::milliseconds(300 * i as i64))
                .into_wrapped_multi_sender::<ClientSenderForNetworkMessage, ClientSenderForNetwork>(
                )
                .send_async(ProcessTxRequest {
                    transaction: tx,
                    is_forwarded: false,
                    check_only: false,
                }),
        );
    }

    // Run the chain some time to allow transactions be processed.
    test.run_for(Duration::seconds(20));

    // Capture the id of the epoch we will check for the correct validator information in assert_validator_info.
    let prev_epoch_id = test.data[0].client.client.chain.head().unwrap().epoch_id;
    assert_ne!(prev_epoch_id, initial_epoch_id);

    // Run the chain until it transitions to a different epoch then prev_epoch_id.
    test.run_until(
        |data| data[0].client.client.chain.head().unwrap().epoch_id != prev_epoch_id,
        Duration::seconds(EPOCH_LENGTH as i64),
    );

    // Check that the balances are correct.
    for i in NUM_VALIDATORS..NUM_ACCOUNTS {
        let account = &accounts[i];
        assert_eq!(
            test.data.query_balance(account),
            *balances.get(account).unwrap(),
            "Account balance mismatch for account {}",
            account
        );
    }

    for idx in 0..NUM_VALIDATORS {
        test.data[idx].state_sync_dumper.stop();
    }

    // Check the validator information for the epoch with the prev_epoch_id.
    assert_validator_info(&test.data[0].client.client, prev_epoch_id, initial_epoch_id, &accounts);

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    test.shutdown_and_drain_remaining_events(Duration::seconds(20));
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
