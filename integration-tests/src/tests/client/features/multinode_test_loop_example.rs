use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::futures::FutureSpawner;
use near_async::messaging::{noop, IntoSender, MessageWithCallback, SendAsync};
use near_async::test_loop::adhoc::AdhocEvent;
use near_async::test_loop::futures::{
    TestLoopAsyncComputationEvent, TestLoopDelayedActionEvent, TestLoopTask,
};
use near_async::time::Duration;
use near_async::v2::{self, AdhocEventSender, LoopData, LoopStream};
use near_chain::chunks_store::ReadOnlyChunksStore;
use near_chain::state_snapshot_actor::{
    get_delete_snapshot_callback, get_make_snapshot_callback, SnapshotCallbacks,
    StateSnapshotActor, StateSnapshotSenderForClientMessage,
    StateSnapshotSenderForStateSnapshotMessage,
};
use near_chain::test_utils::test_loop::{
    loop_state_snapshot_actor_builder, LoopStateSnapshotActor,
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
    handle_shards_manager_network_routing, loop_shards_manager_actor_builder,
    LoopShardsManagerActor,
};
use near_client::client_actor::{
    ClientActorInner, ClientSenderForClientMessage, ClientSenderForPartialWitnessMessage,
    SyncJobsSenderForClientMessage,
};
use near_client::sync_jobs_actor::{ClientSenderForSyncJobsMessage, SyncJobsActor};
use near_client::test_utils::test_loop::client_actor::{
    loop_client_actor_builder, LoopClientActor,
};
use near_client::test_utils::test_loop::partial_witness_actor::{
    loop_partial_witness_actor_builder, LoopPartialWitnessActor,
};
use near_client::test_utils::test_loop::sync_actor::{
    loop_sync_actor_builder, LoopSyncActor, TestSyncActors,
};
use near_client::test_utils::test_loop::sync_jobs_actor::{
    loop_sync_jobs_actor_builder, LoopSyncJobsActor,
};
use near_client::test_utils::test_loop::ClientQueries;
use near_client::test_utils::test_loop::{
    handle_client_network_routing, handle_partial_witness_network_routing, ClientQueriesV2,
};
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
use near_network::types::{
    PeerManagerAdapter, PeerManagerAdapterMessage, PeerManagerMessageRequest,
    PeerManagerMessageResponse, SetChainInfo,
};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::shard_layout::ShardUId;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
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

struct TestActors {
    account_id: AccountId,
    client: LoopClientActor,
    sync_jobs: LoopSyncJobsActor,
    shards_manager: LoopShardsManagerActor,
    partial_witness: LoopPartialWitnessActor,
    sync_actors: LoopSyncActor,
    state_snapshot: LoopStateSnapshotActor,
    state_sync_dumper: LoopData<StateSyncDumper>,
    network_stream: LoopStream<PeerManagerAdapterMessage>,
}

#[test]
fn test_client_with_multi_test_loop() {
    const NUM_CLIENTS: usize = 4;
    const NETWORK_DELAY: Duration = Duration::milliseconds(10);
    let mut test = v2::TestLoop::new();

    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&test.clock())
        .protocol_version_latest()
        .genesis_height(10000)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        .transaction_validity_period(1000)
        .epoch_length(10)
        .validators_desired_roles(
            &(0..NUM_CLIENTS).map(|idx| accounts[idx].as_str()).collect::<Vec<_>>(),
            &[],
        )
        .shuffle_shard_assignment_for_chunk_producers(true);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let tempdir = tempfile::tempdir().unwrap();
    let mut actors = Vec::new();
    let mut client_sender_by_account = HashMap::new();
    let mut shards_manager_sender_by_account = HashMap::new();
    let mut partial_witness_sender_by_account = HashMap::new();
    let mut client_queries = ClientQueriesV2::new();

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
            ..Default::default()
        };
        let store = create_test_store();
        initialize_genesis_state(store.clone(), &genesis, None);

        let chain_genesis = ChainGenesis::new(&genesis.config);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let shard_tracker =
            ShardTracker::new(TrackedConfig::from_config(&client_config), epoch_manager.clone());

        let future_spawner = test.new_future_spawner();
        let async_computation_spawner =
            test.new_async_computation_spawner(|_| Duration::milliseconds(80));

        let network_stream = test.new_stream::<PeerManagerAdapterMessage>();
        let network_adapter = network_stream
            .delay_sender()
            .into_wrapped_multi_sender::<PeerManagerAdapterMessage, PeerManagerAdapter>();
        let sync_actor_builder = loop_sync_actor_builder(&mut test);
        let client_actor_builder = loop_client_actor_builder(&mut test);
        let shards_manager_actor_builder = loop_shards_manager_actor_builder(&mut test);
        let state_snapshot_actor_builder = loop_state_snapshot_actor_builder(&mut test);
        let partial_witness_actor_builder = loop_partial_witness_actor_builder(&mut test);
        let sync_jobs_actor_builder = loop_sync_jobs_actor_builder(&mut test);

        // Ignore SetChainInfo.
        network_stream.handle0(&mut test, |msg| {
            if let PeerManagerAdapterMessage::_set_chain_info_sender(_) = msg {
                Ok(())
            } else {
                Err(msg)
            }
        });

        let sync_jobs_actor = SyncJobsActor::new(client_actor_builder.from_sync_jobs.clone());

        let state_sync_adapter = Arc::new(RwLock::new(SyncAdapter::new(
            client_actor_builder.from_sync_adapter.clone(),
            network_adapter.request_sender.clone(),
            sync_actor_builder.sync_actor_maker(),
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
            network_adapter.clone(),
            runtime_adapter.get_tries(),
            state_snapshot_actor_builder.from_state_snapshot.clone(),
        );

        let delete_snapshot_callback =
            get_delete_snapshot_callback(state_snapshot_actor_builder.from_client.clone());
        let make_snapshot_callback = get_make_snapshot_callback(
            state_snapshot_actor_builder.from_client.clone(),
            runtime_adapter.get_flat_storage_manager(),
        );
        let snapshot_callbacks =
            SnapshotCallbacks { make_snapshot_callback, delete_snapshot_callback };

        let validator_signer = Arc::new(create_test_signer(accounts[idx].as_str()));
        let client = Client::new(
            test.clock(),
            client_config.clone(),
            chain_genesis.clone(),
            epoch_manager.clone(),
            shard_tracker.clone(),
            state_sync_adapter,
            runtime_adapter.clone(),
            network_adapter.clone(),
            shards_manager_actor_builder.from_client.clone(),
            Some(validator_signer.clone()),
            true,
            [0; 32],
            Some(snapshot_callbacks),
            Arc::new(async_computation_spawner.clone()),
            partial_witness_actor_builder.from_client.clone(),
        )
        .unwrap();

        let shards_manager = ShardsManagerActor::new(
            test.clock(),
            Some(accounts[idx].clone()),
            epoch_manager.clone(),
            shard_tracker.clone(),
            network_adapter.request_sender.clone(),
            client_actor_builder.from_shards_manager.clone(),
            ReadOnlyChunksStore::new(store.clone()),
            client.chain.head().unwrap(),
            client.chain.header_head().unwrap(),
            Duration::milliseconds(100),
        );

        let client_actor = ClientActorInner::new(
            test.clock(),
            client,
            client_actor_builder.from_client.clone(),
            client_config.clone(),
            PeerId::random(),
            network_adapter.clone(),
            None,
            noop().into_sender(),
            None,
            Default::default(),
            None,
            sync_jobs_actor_builder.from_client.clone(),
            Box::new(future_spawner.clone()),
        )
        .unwrap();

        let partial_witness_actions = PartialWitnessActor::new(
            test.clock(),
            network_adapter.clone(),
            client_actor_builder.from_partial_witness.clone(),
            validator_signer,
            epoch_manager.clone(),
            store,
        );

        let future_spawner = future_spawner.clone();
        let state_sync_dumper = StateSyncDumper {
            clock: test.clock(),
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

        client_sender_by_account
            .insert(accounts[idx].clone(), client_actor_builder.from_network_stream.delay_sender());
        shards_manager_sender_by_account.insert(
            accounts[idx].clone(),
            shards_manager_actor_builder.from_network_stream.delay_sender(),
        );
        partial_witness_sender_by_account.insert(
            accounts[idx].clone(),
            partial_witness_actor_builder.from_network_stream.delay_sender(),
        );

        let client = client_actor_builder.build(&mut test, client_actor);
        client_queries.add_client(client.actor, accounts[idx].clone());
        actors.push(TestActors {
            account_id: accounts[idx].clone(),
            client,
            sync_jobs: sync_jobs_actor_builder.build(&mut test, sync_jobs_actor),
            shards_manager: shards_manager_actor_builder.build(&mut test, shards_manager),
            partial_witness: partial_witness_actor_builder
                .build(&mut test, partial_witness_actions),
            sync_actors: sync_actor_builder.build(&mut test),
            state_snapshot: state_snapshot_actor_builder.build(&mut test, state_snapshot),
            state_sync_dumper: test.add_data(state_sync_dumper),
            network_stream,
        });
    }

    // TODO: how to do this in v2?
    // for idx in 0..NUM_CLIENTS {
    //     // Handlers that do nothing but print some information.
    //     test.register_handler(print_basic_client_info_before_each_event(Some(idx)).for_index(idx));
    // }

    // Handles network routing. Outgoing messages are handled by emitting incoming messages to the
    // appropriate component of the appropriate node index.
    let route_back_lookup = test.add_data(HashMap::<CryptoHash, AccountId>::new());
    for actors in &actors {
        handle_client_network_routing(
            &actors.network_stream,
            &actors.account_id,
            &mut test,
            &client_sender_by_account,
            NETWORK_DELAY,
        );
        handle_shards_manager_network_routing(
            &actors.network_stream,
            &actors.account_id,
            &mut test,
            &shards_manager_sender_by_account,
            NETWORK_DELAY,
            route_back_lookup,
        );
        handle_partial_witness_network_routing(
            &actors.network_stream,
            &actors.account_id,
            &mut test,
            &partial_witness_sender_by_account,
            NETWORK_DELAY,
        );
    }

    let adhoc_senders = (0..NUM_CLIENTS).map(|_| test.new_adhoc_sender()).collect::<Vec<_>>();

    // Bootstrap the test by starting the components.
    // We use adhoc events for these, just so that the visualizer can see these as events rather
    // than happening outside of the TestLoop framework. Other than that, we could also just remove
    // the send_adhoc_event part and the test would still work.
    for idx in 0..NUM_CLIENTS {
        let client = actors[idx].client.clone();
        adhoc_senders[idx].send_adhoc_event("start_client", move |data| {
            data.get_mut(client.actor).start(&mut client.delayed_action_runner.clone());
        });
        let shards_manager = actors[idx].shards_manager.clone();
        adhoc_senders[idx].send_adhoc_event("start_shards_manager", move |data| {
            data.get_mut(shards_manager.actor).periodically_resend_chunk_requests(
                &mut shards_manager.delayed_action_runner.clone(),
            );
        });
        let state_sync_dumper = actors[idx].state_sync_dumper.clone();
        adhoc_senders[idx].send_adhoc_event("start_state_sync_dumper", move |data| {
            data.get_mut(state_sync_dumper).start().unwrap();
        });
    }

    // Give it some condition to stop running at. Here we run the test until the first client
    // reaches height 10003, with a timeout of 5sec (failing if it doesn't reach 10003 in time).
    let client0 = actors[0].client.actor;
    test.run_until(
        |data| data.get(client0).client.chain.head().unwrap().height == 10003,
        Duration::seconds(5),
    );
    for idx in 0..NUM_CLIENTS {
        let client = actors[idx].client.clone();
        adhoc_senders[idx].send_adhoc_event("assertions", move |data| {
            let chain = &data.get(client.actor).client.chain;
            let block = chain.get_block_by_height(10002).unwrap();
            assert_eq!(
                block.header().chunk_mask(),
                &(0..NUM_CLIENTS).map(|_| true).collect::<Vec<_>>()
            );
        })
    }
    test.run_instant();

    let first_epoch_tracked_shards = client_queries.with(&test).tracked_shards_for_each_client();
    tracing::info!("First epoch tracked shards: {:?}", first_epoch_tracked_shards);

    let mut balances = accounts
        .iter()
        .cloned()
        .map(|account| (account, initial_balance))
        .collect::<HashMap<_, _>>();

    let anchor_hash = *test.data(client0).client.chain.get_block_by_height(10002).unwrap().hash();
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
            client_sender_by_account[&accounts[i % NUM_CLIENTS]]
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
        |data| data.get(client0).client.chain.head().unwrap().height > 10050,
        Duration::seconds(10),
    );

    for account in &accounts {
        assert_eq!(
            client_queries.with(&test).query_balance(account),
            *balances.get(account).unwrap(),
            "Account balance mismatch for account {}",
            account
        );
    }

    let later_epoch_tracked_shards = client_queries.with(&test).tracked_shards_for_each_client();
    tracing::info!("Later epoch tracked shards: {:?}", later_epoch_tracked_shards);
    assert_ne!(first_epoch_tracked_shards, later_epoch_tracked_shards);

    for idx in 0..NUM_CLIENTS {
        test.data_mut(actors[idx].state_sync_dumper).stop();
    }

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    test.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
