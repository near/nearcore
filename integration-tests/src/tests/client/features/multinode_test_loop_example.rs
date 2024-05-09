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
use near_chain_configs::{
    ClientConfig, DumpConfig, ExternalStorageConfig, ExternalStorageLocation, Genesis,
    GenesisConfig, GenesisRecords, StateSyncConfig, SyncConfig,
};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::client::ShardsManagerResponse;
use near_chunks::shards_manager_actor::ShardsManagerActor;
use near_chunks::test_loop::{
    forward_client_request_to_shards_manager, forward_network_request_to_shards_manager,
    route_shards_manager_network_messages,
};
use near_client::client_actions::{
    ClientActorInner, ClientSenderForClientMessage, ClientSenderForPartialWitnessMessage,
    SyncJobsSenderForClientMessage,
};
use near_client::sync::sync_actor::SyncActor;
use near_client::sync_jobs_actor::{ClientSenderForSyncJobsMessage, SyncJobsActor};
use near_client::test_utils::test_loop::client_actions::{
    forward_client_messages_from_client_to_client_actions,
    forward_client_messages_from_network_to_client_actions,
    forward_client_messages_from_shards_manager, forward_client_messages_from_sync_adapter,
    forward_client_messages_from_sync_jobs_to_client_actions,
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
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::state_record::StateRecord;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, AccountInfo};
use near_primitives::utils::from_timestamp;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives_core::account::{AccessKey, Account};
use near_primitives_core::hash::CryptoHash;
use near_store::config::StateSnapshotType;
use near_store::genesis::initialize_genesis_state;
use near_store::{NodeStorage, StoreConfig, TrieConfig};
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

#[test]
fn test_client_with_multi_test_loop() {
    const NUM_CLIENTS: usize = 4;
    const NETWORK_DELAY: Duration = Duration::milliseconds(10);
    let builder = TestLoopBuilder::<(usize, TestEvent)>::new();

    let validator_stake = 1000000 * ONE_NEAR;
    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();

    // TODO: Make some builder for genesis.
    let mut genesis_config = GenesisConfig {
        genesis_time: from_timestamp(builder.clock().now_utc().unix_timestamp_nanos() as u64),
        protocol_version: PROTOCOL_VERSION,
        genesis_height: 10000,
        shard_layout: ShardLayout::v1(
            vec!["account3", "account5", "account7"]
                .into_iter()
                .map(|a| a.parse().unwrap())
                .collect(),
            None,
            1,
        ),
        min_gas_price: 0,
        max_gas_price: 0,
        gas_limit: 100000000000000,
        transaction_validity_period: 1000,
        validators: (0..NUM_CLIENTS)
            .map(|idx| AccountInfo {
                account_id: accounts[idx].clone(),
                amount: validator_stake,
                public_key: create_test_signer(accounts[idx].as_str()).public_key(),
            })
            .collect(),
        epoch_length: 10,
        protocol_treasury_account: accounts[NUM_CLIENTS].clone(),
        num_block_producer_seats: 4,
        minimum_validators_per_shard: 1,
        num_block_producer_seats_per_shard: vec![0, 0, 0, 0], // ignored
        shuffle_shard_assignment_for_chunk_producers: true,
        ..Default::default()
    };
    let mut records = Vec::new();
    for (i, account) in accounts.iter().enumerate() {
        // The staked amount must be consistent with validators from genesis.
        let staked = if i < NUM_CLIENTS { validator_stake } else { 0 };
        records.push(StateRecord::Account {
            account_id: account.clone(),
            account: Account::new(
                initial_balance,
                staked,
                0,
                CryptoHash::default(),
                0,
                PROTOCOL_VERSION,
            ),
        });
        records.push(StateRecord::AccessKey {
            account_id: account.clone(),
            public_key: create_user_test_signer(&account).public_key,
            access_key: AccessKey::full_access(),
        });
        // The total supply must be correct to pass validation.
        genesis_config.total_supply += initial_balance + staked;
    }
    let genesis = Genesis::new(genesis_config, GenesisRecords(records)).unwrap();

    let tempdir = tempfile::tempdir().unwrap();
    let mut datas = Vec::new();
    for idx in 0..NUM_CLIENTS {
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
            max_open_files: 1000,
            ..Default::default()
        };
        let opener = NodeStorage::opener(&homedir, false, &store_config, None);
        let store = opener.open().unwrap().get_hot_store();
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
            ReadOnlyChunksStore::new(store),
            client.chain.head().unwrap(),
            client.chain.header_head().unwrap(),
            Duration::milliseconds(100),
        );

        let client_actions = ClientActorInner::new(
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
            client: client_actions,
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
    for idx in 0..NUM_CLIENTS {
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
            forward_client_messages_from_network_to_client_actions().widen().for_index(idx),
        );
        test.register_handler(
            forward_client_messages_from_client_to_client_actions().widen().for_index(idx),
        );
        test.register_handler(
            forward_client_messages_from_sync_jobs_to_client_actions().widen().for_index(idx),
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
    for idx in 0..NUM_CLIENTS {
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
    for idx in 0..NUM_CLIENTS {
        test.sender().for_index(idx).send_adhoc_event("assertions", |data| {
            let chain = &data.client.client.chain;
            let block = chain.get_block_by_height(10002).unwrap();
            assert_eq!(
                block.header().chunk_mask(),
                &(0..NUM_CLIENTS).map(|_| true).collect::<Vec<_>>()
            );
        })
    }
    test.run_instant();

    let first_epoch_tracked_shards = test.data.tracked_shards_for_each_client();
    tracing::info!("First epoch tracked shards: {:?}", first_epoch_tracked_shards);

    let mut balances = accounts
        .iter()
        .cloned()
        .map(|account| (account, initial_balance))
        .collect::<HashMap<_, _>>();

    let anchor_hash = *test.data[0].client.client.chain.get_block_by_height(10002).unwrap().hash();
    for i in 0..accounts.len() {
        let amount = ONE_NEAR * (i as u128 + 1);
        let tx = SignedTransaction::send_money(
            1,
            accounts[i].clone(),
            accounts[(i + 1) % accounts.len()].clone(),
            &create_user_test_signer(&accounts[i]),
            amount,
            anchor_hash,
        );
        *balances.get_mut(&accounts[i]).unwrap() -= amount;
        *balances.get_mut(&accounts[(i + 1) % accounts.len()]).unwrap() += amount;
        drop(
            test.sender()
                .for_index(i % NUM_CLIENTS)
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

    // Give plenty of time for these transactions to complete.
    test.run_for(Duration::seconds(40));

    // Make sure the chain progresses for several epochs.
    test.run_until(
        |data| data[0].client.client.chain.head().unwrap().height > 10050,
        Duration::seconds(10),
    );

    for account in &accounts {
        assert_eq!(
            test.data.query_balance(account),
            *balances.get(account).unwrap(),
            "Account balance mismatch for account {}",
            account
        );
    }

    let later_epoch_tracked_shards = test.data.tracked_shards_for_each_client();
    tracing::info!("Later epoch tracked shards: {:?}", later_epoch_tracked_shards);
    assert_ne!(first_epoch_tracked_shards, later_epoch_tracked_shards);

    for idx in 0..NUM_CLIENTS {
        test.data[idx].state_sync_dumper.stop();
    }

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    test.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
