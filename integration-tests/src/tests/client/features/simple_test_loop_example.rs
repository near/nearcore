use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::futures::DelayedActionRunnerExt;
use near_async::messaging::{noop, IntoMultiSender, IntoSender, MessageWithCallback};
use near_async::test_loop::event_handler::ignore_events;
use near_async::test_loop::futures::{
    drive_delayed_action_runners, drive_futures, TestLoopDelayedActionEvent, TestLoopTask,
};
use near_async::test_loop::TestLoopBuilder;
use near_chain::ChainGenesis;
use near_chain_configs::{ClientConfig, Genesis, GenesisConfig, GenesisRecords};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_client::client_actions::{
    ClientActions, ClientSenderForClientMessage, SyncJobsSenderForClientMessage,
};
use near_client::sync_jobs_actions::{
    ClientSenderForSyncJobsMessage, SyncJobsActions, SyncJobsSenderForSyncJobsMessage,
};
use near_client::test_utils::client_actions_test_utils::{
    forward_client_messages_from_client_to_client_actions,
    forward_client_messages_from_sync_jobs_to_client_actions,
};
use near_client::test_utils::sync_jobs_test_utils::forward_sync_jobs_messages_from_client_to_sync_jobs_actions;
use near_client::test_utils::{MAX_BLOCK_PROD_TIME, MIN_BLOCK_PROD_TIME};
use near_client::{Client, SyncAdapter, SyncMessage};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_network::client::ClientSenderForNetworkMessage;
use near_network::types::{PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo};
use near_primitives::network::PeerId;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::types::{AccountId, AccountInfo};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives_core::account::{AccessKey, Account};
use near_primitives_core::hash::CryptoHash;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use nearcore::NightshadeRuntime;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct TestData {
    pub dummy: (),
    pub client: ClientActions,
    pub sync_jobs: SyncJobsActions,
}

#[derive(EnumTryInto, Debug, EnumFrom)]
#[allow(clippy::large_enum_variant)]
enum TestEvent {
    Task(Arc<TestLoopTask>),
    ClientDelayedActions(TestLoopDelayedActionEvent<ClientActions>),
    ClientEventFromNetwork(ClientSenderForNetworkMessage),
    ClientEventFromClient(ClientSenderForClientMessage),
    ClientEventFromSyncJobs(ClientSenderForSyncJobsMessage),
    SyncJobsEventFromClient(SyncJobsSenderForClientMessage),
    SyncJobsEventFromSyncJobs(SyncJobsSenderForSyncJobsMessage),
    ShardsManagerRequestFromClient(ShardsManagerRequestFromClient),
    ClientEventFromStateSyncAdapter(SyncMessage),
    NetworkMessage(PeerManagerMessageRequest),
    NetworkMessageForResponse(
        MessageWithCallback<PeerManagerMessageRequest, PeerManagerMessageResponse>,
    ),
    NetworkSetChainInfo(SetChainInfo),
}

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

// TODO(robin-near): Complete this test so that it will actually run a chain.
// TODO(robin-near): Make this a multi-node test.
// TODO(robin-near): Make the network layer send messages.
#[test]
fn test_client_with_simple_test_loop() {
    let builder = TestLoopBuilder::<TestEvent>::new();
    let sync_jobs_actions = SyncJobsActions::new(
        builder.wrapped_multi_sender::<ClientSenderForSyncJobsMessage, _>(),
        builder.wrapped_multi_sender::<SyncJobsSenderForSyncJobsMessage, _>(),
    );
    let client_config = ClientConfig::test(
        true,
        MIN_BLOCK_PROD_TIME.as_nanos() as u64,
        MAX_BLOCK_PROD_TIME.as_nanos() as u64,
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
    let mut genesis_config = GenesisConfig {
        // Use the latest protocol version. Otherwise, the version may be too
        // old that e.g. blocks don't even store previous heights.
        protocol_version: PROTOCOL_VERSION,
        // Some arbitrary starting height. Doesn't matter.
        genesis_height: 10000,
        // We'll test with 4 shards. This can be any number, but we want to test
        // the case when some shards are loaded into memory and others are not.
        // We pick the boundaries so that each shard would get some transactions.
        shard_layout: ShardLayout::v1(
            vec!["account3", "account5", "account7"]
                .into_iter()
                .map(|a| a.parse().unwrap())
                .collect(),
            None,
            1,
        ),
        // We're going to send NEAR between accounts and then assert at the end
        // that these transactions have been processed correctly, so here we set
        // the gas price to 0 so that we don't have to calculate gas cost.
        min_gas_price: 0,
        max_gas_price: 0,
        // Set the block gas limit high enough so we don't have to worry about
        // transactions being throttled.
        gas_limit: 100000000000000,
        // Set the validity period high enough so even if a transaction gets
        // included a few blocks later it won't be rejected.
        transaction_validity_period: 1000,
        // Make two validators. In this test we don't care about validators but
        // the TestEnv framework works best if all clients are validators. So
        // since we are using two clients, make two validators.
        validators: vec![
            AccountInfo {
                account_id: accounts[0].clone(),
                amount: validator_stake,
                public_key: create_test_signer(accounts[0].as_str()).public_key(),
            },
            AccountInfo {
                account_id: accounts[1].clone(),
                amount: validator_stake,
                public_key: create_test_signer(accounts[1].as_str()).public_key(),
            },
        ],
        // We don't care about epoch transitions in this test, and epoch
        // transitions means validator selection, which can kick out validators
        // (due to our test purposefully skipping blocks to create forks), and
        // that's annoying to deal with. So set this to a high value to stay
        // within a single epoch.
        epoch_length: 10000,
        // The genesis requires this, so set it to something arbitrary.
        protocol_treasury_account: accounts[2].clone(),
        // Simply make all validators block producers.
        num_block_producer_seats: 2,
        // Make all validators produce chunks for all shards.
        minimum_validators_per_shard: 2,
        // Even though not used for the most recent protocol version,
        // this must still have the same length as the number of shards,
        // or else the genesis fails validation.
        num_block_producer_seats_per_shard: vec![2, 2, 2, 2],
        ..Default::default()
    };
    // We'll now create the initial records. We'll set up 100 accounts, each
    // with some initial balance. We'll add an access key to each account so
    // we can send transactions from them.
    let mut records = Vec::new();
    for (i, account) in accounts.iter().enumerate() {
        // The staked amount must be consistent with validators from genesis.
        let staked = if i < 2 { validator_stake } else { 0 };
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
        builder.sender().into_sender(),
    )));
    let runtime_adapter =
        NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());

    let client = Client::new(
        client_config.clone(),
        chain_genesis,
        epoch_manager,
        shard_tracker,
        state_sync_adapter,
        runtime_adapter,
        builder.sender().into_multi_sender(),
        builder.sender().into_sender(),
        None,
        true,
        [0; 32],
        None,
    )
    .unwrap();

    let client_actions = ClientActions::new(
        builder.clock(),
        client,
        builder.wrapped_multi_sender::<ClientSenderForClientMessage, _>(),
        client_config,
        PeerId::random(),
        builder.sender().into_multi_sender(),
        None,
        noop().into_sender(),
        None,
        Default::default(),
        None,
        builder.wrapped_multi_sender::<SyncJobsSenderForClientMessage, _>(),
        Box::new(builder.future_spawner()),
    )
    .unwrap();

    let data = TestData { dummy: (), client: client_actions, sync_jobs: sync_jobs_actions };

    let mut test = builder.build(data);
    test.register_handler(forward_client_messages_from_client_to_client_actions().widen());
    test.register_handler(forward_client_messages_from_sync_jobs_to_client_actions().widen());
    test.register_handler(
        forward_sync_jobs_messages_from_client_to_sync_jobs_actions(test.future_spawner()).widen(),
    );
    test.register_handler(drive_futures().widen());
    test.register_handler(drive_delayed_action_runners::<ClientActions>().widen());
    test.register_handler(ignore_events::<SetChainInfo>().widen());
    // TODO: handle additional events.

    test.delayed_action_runner::<ClientActions>().run_later(
        "start_client",
        Duration::ZERO,
        |client, runner| {
            client.start(runner);
        },
    );
    test.run_for(near_async::time::Duration::seconds(1));
}
