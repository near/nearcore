use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::messaging::{noop, IntoMultiSender, IntoSender};
use near_async::test_loop::adhoc::{handle_adhoc_events, AdhocEvent, AdhocEventSender};
use near_async::test_loop::futures::{
    drive_async_computations, drive_futures, TestLoopAsyncComputationEvent,
    TestLoopDelayedActionEvent, TestLoopTask,
};
use near_async::test_loop::TestLoopBuilder;
use near_async::time::Duration;
use near_chain::chunks_store::ReadOnlyChunksStore;
use near_chain::ChainGenesis;
use near_chain_configs::{ClientConfig, Genesis, GenesisConfig, GenesisRecords};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::client::ShardsManagerResponse;
use near_chunks::test_loop::forward_client_request_to_shards_manager;
use near_chunks::ShardsManager;
use near_client::client_actions::{
    ClientActions, ClientSenderForClientMessage, SyncJobsSenderForClientMessage,
};
use near_client::sync_jobs_actions::{
    ClientSenderForSyncJobsMessage, SyncJobsActions, SyncJobsSenderForSyncJobsMessage,
};
use near_client::test_utils::client_actions_test_utils::{
    forward_client_messages_from_client_to_client_actions,
    forward_client_messages_from_shards_manager,
    forward_client_messages_from_sync_jobs_to_client_actions,
};
use near_client::test_utils::sync_jobs_test_utils::forward_sync_jobs_messages_from_client_to_sync_jobs_actions;
use near_client::test_utils::{MAX_BLOCK_PROD_TIME, MIN_BLOCK_PROD_TIME};
use near_client::{Client, SyncAdapter, SyncMessage};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_network::client::ClientSenderForNetworkMessage;
use near_primitives::network::PeerId;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::types::{AccountId, AccountInfo};
use near_primitives::utils::from_timestamp;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives_core::account::{AccessKey, Account};
use near_primitives_core::hash::CryptoHash;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use nearcore::NightshadeRuntime;
use std::path::Path;
use std::sync::{Arc, RwLock};

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct TestData {
    pub dummy: (),
    pub client: ClientActions,
    pub sync_jobs: SyncJobsActions,
    pub shards_manager: ShardsManager,
}

impl AsMut<TestData> for TestData {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

#[derive(EnumTryInto, Debug, EnumFrom)]
#[allow(clippy::large_enum_variant)]
enum TestEvent {
    Task(Arc<TestLoopTask>),
    Adhoc(AdhocEvent<TestData>),
    AsyncComputation(TestLoopAsyncComputationEvent),
    ClientDelayedActions(TestLoopDelayedActionEvent<ClientActions>),
    ClientEventFromNetwork(ClientSenderForNetworkMessage),
    ClientEventFromClient(ClientSenderForClientMessage),
    ClientEventFromSyncJobs(ClientSenderForSyncJobsMessage),
    ClientEventFromShardsManager(ShardsManagerResponse),
    SyncJobsEventFromClient(SyncJobsSenderForClientMessage),
    SyncJobsEventFromSyncJobs(SyncJobsSenderForSyncJobsMessage),
    ShardsManagerRequestFromClient(ShardsManagerRequestFromClient),
    ClientEventFromStateSyncAdapter(SyncMessage),
}

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

#[test]
fn test_client_with_simple_test_loop() {
    let builder = TestLoopBuilder::<TestEvent>::new();
    let sync_jobs_actions = SyncJobsActions::new(
        builder.sender().into_wrapped_multi_sender::<ClientSenderForSyncJobsMessage, _>(),
        builder.sender().into_wrapped_multi_sender::<SyncJobsSenderForSyncJobsMessage, _>(),
    );
    let client_config = ClientConfig::test(
        true,
        MIN_BLOCK_PROD_TIME.whole_milliseconds() as u64,
        MAX_BLOCK_PROD_TIME.whole_milliseconds() as u64,
        4,
        false,
        true,
        false,
        false,
    );
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
        validators: vec![AccountInfo {
            account_id: accounts[0].clone(),
            amount: validator_stake,
            public_key: create_test_signer(accounts[0].as_str()).public_key(),
        }],
        epoch_length: 10,
        protocol_treasury_account: accounts[2].clone(),
        num_block_producer_seats: 1,
        minimum_validators_per_shard: 1,
        num_block_producer_seats_per_shard: vec![1, 1, 1, 1],
        ..Default::default()
    };
    let mut records = Vec::new();
    for (i, account) in accounts.iter().enumerate() {
        // The staked amount must be consistent with validators from genesis.
        let staked = if i < 1 { validator_stake } else { 0 };
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

    let store = create_test_store();
    let genesis = Genesis::new(genesis_config, GenesisRecords(records)).unwrap();
    initialize_genesis_state(store.clone(), &genesis, None);

    let chain_genesis = ChainGenesis::new(&genesis.config);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
    let shard_tracker = ShardTracker::new(TrackedConfig::AllShards, epoch_manager.clone());
    let state_sync_adapter = Arc::new(RwLock::new(SyncAdapter::new(
        builder.sender().into_sender(),
        noop().into_sender(),
        SyncAdapter::actix_actor_maker(),
    )));
    let runtime_adapter = NightshadeRuntime::test(
        Path::new("."),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );

    let client = Client::new(
        builder.clock(),
        client_config.clone(),
        chain_genesis,
        epoch_manager.clone(),
        shard_tracker.clone(),
        state_sync_adapter,
        runtime_adapter,
        noop().into_multi_sender(),
        builder.sender().into_sender(),
        Some(Arc::new(create_test_signer(accounts[0].as_str()))),
        true,
        [0; 32],
        None,
        Arc::new(builder.sender().into_async_computation_spawner(|_| Duration::milliseconds(80))),
    )
    .unwrap();

    let shards_manager = ShardsManager::new(
        builder.clock(),
        Some(accounts[0].clone()),
        epoch_manager,
        shard_tracker,
        noop().into_sender(),
        builder.sender().into_sender(),
        ReadOnlyChunksStore::new(store),
        client.chain.head().unwrap(),
        client.chain.header_head().unwrap(),
    );

    let client_actions = ClientActions::new(
        builder.clock(),
        client,
        builder.sender().into_wrapped_multi_sender::<ClientSenderForClientMessage, _>(),
        client_config,
        PeerId::random(),
        noop().into_multi_sender(),
        None,
        noop().into_sender(),
        None,
        Default::default(),
        None,
        builder.sender().into_wrapped_multi_sender::<SyncJobsSenderForClientMessage, _>(),
        Box::new(builder.sender().into_future_spawner()),
    )
    .unwrap();

    let data = TestData {
        dummy: (),
        client: client_actions,
        sync_jobs: sync_jobs_actions,
        shards_manager,
    };

    let mut test = builder.build(data);
    test.register_handler(forward_client_messages_from_client_to_client_actions().widen());
    test.register_handler(forward_client_messages_from_sync_jobs_to_client_actions().widen());
    test.register_handler(forward_client_messages_from_shards_manager().widen());
    test.register_handler(
        forward_sync_jobs_messages_from_client_to_sync_jobs_actions(
            test.sender().into_future_spawner(),
        )
        .widen(),
    );
    test.register_handler(drive_futures().widen());
    test.register_handler(handle_adhoc_events::<TestData>().widen());
    test.register_handler(drive_async_computations().widen());
    test.register_delayed_action_handler::<ClientActions>();
    test.register_handler(forward_client_request_to_shards_manager().widen());
    // TODO: handle additional events.

    let mut delayed_runner =
        test.sender().into_delayed_action_runner::<ClientActions>(test.shutting_down());
    test.sender().send_adhoc_event("start_client", move |data| {
        data.client.start(&mut delayed_runner);
    });
    test.run_for(Duration::seconds(10));
    test.shutdown_and_drain_remaining_events(Duration::seconds(1));
}
