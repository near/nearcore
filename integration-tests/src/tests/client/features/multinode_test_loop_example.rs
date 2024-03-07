use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::messaging::{noop, IntoMultiSender, IntoSender, MessageWithCallback, SendAsync};
use near_async::test_loop::adhoc::{handle_adhoc_events, AdhocEvent, AdhocEventSender};
use near_async::test_loop::event_handler::{
    ignore_events, LoopEventHandler, LoopHandlerContext, TryIntoOrSelf,
};
use near_async::test_loop::futures::{
    drive_delayed_action_runners, drive_futures, TestLoopDelayedActionEvent, TestLoopTask,
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
    forward_client_messages_from_network_to_client_actions,
    forward_client_messages_from_shards_manager,
    forward_client_messages_from_sync_jobs_to_client_actions,
};
use near_client::test_utils::sync_jobs_test_utils::forward_sync_jobs_messages_from_client_to_sync_jobs_actions;
use near_client::{Client, SyncAdapter, SyncMessage};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_network::client::{
    BlockApproval, BlockResponse, ClientSenderForNetwork, ClientSenderForNetworkMessage,
};
use near_network::test_loop::SupportsRoutingLookup;
use near_network::types::{
    NetworkRequests, PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo,
};
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

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct TestData {
    pub dummy: (),
    pub account: AccountId,
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
    ClientDelayedActions(TestLoopDelayedActionEvent<ClientActions>),
    ClientEventFromNetwork(ClientSenderForNetworkMessage),
    ClientEventFromClient(ClientSenderForClientMessage),
    ClientEventFromSyncJobs(ClientSenderForSyncJobsMessage),
    ClientEventFromShardsManager(ShardsManagerResponse),
    SyncJobsEventFromClient(SyncJobsSenderForClientMessage),
    SyncJobsEventFromSyncJobs(SyncJobsSenderForSyncJobsMessage),
    ShardsManagerRequestFromClient(ShardsManagerRequestFromClient),
    ClientEventFromStateSyncAdapter(SyncMessage),
    NetworkMessage(PeerManagerMessageRequest),
    NetworkMessageForResult(
        MessageWithCallback<PeerManagerMessageRequest, PeerManagerMessageResponse>,
    ),
    SetChainInfo(SetChainInfo),
}

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

// TODO(robin-near): Complete this test so that it will actually run a chain.
// TODO(robin-near): Make this a multi-node test.
// TODO(robin-near): Make the network layer send messages.
#[test]
fn test_client_with_multi_test_loop() {
    const NUM_CLIENTS: usize = 4;
    let builder = TestLoopBuilder::<(usize, TestEvent)>::new();

    let validator_stake = 1000000 * ONE_NEAR;
    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();

    // TODO: Make some builder for genesis.
    let mut genesis_config = GenesisConfig {
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
        num_block_producer_seats_per_shard: vec![4, 4, 4, 4],
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

    let mut datas = Vec::new();
    for idx in 0..NUM_CLIENTS {
        let mut client_config = ClientConfig::test(true, 600, 2000, 4, false, true, false, false);
        client_config.max_block_wait_delay = Duration::seconds(6);
        let store = create_test_store();
        initialize_genesis_state(store.clone(), &genesis, None);

        let sync_jobs_actions = SyncJobsActions::new(
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<ClientSenderForSyncJobsMessage, _>(),
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<SyncJobsSenderForSyncJobsMessage, _>(),
        );
        let chain_genesis = ChainGenesis::new(&genesis.config);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let shard_tracker = ShardTracker::new(TrackedConfig::AllShards, epoch_manager.clone());
        let state_sync_adapter = Arc::new(RwLock::new(SyncAdapter::new(
            builder.sender().for_index(idx).into_sender(),
            builder.sender().for_index(idx).into_sender(),
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
            builder.sender().for_index(idx).into_multi_sender(),
            builder.sender().for_index(idx).into_sender(),
            Some(Arc::new(create_test_signer(accounts[idx].as_str()))),
            true,
            [0; 32],
            None,
        )
        .unwrap();

        let shards_manager = ShardsManager::new(
            builder.clock(),
            Some(accounts[idx].clone()),
            epoch_manager,
            shard_tracker,
            builder.sender().for_index(idx).into_sender(),
            builder.sender().for_index(idx).into_sender(),
            ReadOnlyChunksStore::new(store),
            client.chain.head().unwrap(),
            client.chain.header_head().unwrap(),
        );

        let client_actions = ClientActions::new(
            builder.clock(),
            client,
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<ClientSenderForClientMessage, _>(),
            client_config,
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

        let data = TestData {
            dummy: (),
            account: accounts[idx].clone(),
            client: client_actions,
            sync_jobs: sync_jobs_actions,
            shards_manager,
        };
        datas.push(data);
    }

    let mut test = builder.build(datas);
    for idx in 0..NUM_CLIENTS {
        test.register_handler(handle_adhoc_events::<TestData>().widen().for_index(idx));
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
            forward_sync_jobs_messages_from_client_to_sync_jobs_actions(
                test.sender().for_index(idx).into_future_spawner(),
            )
            .widen()
            .for_index(idx),
        );
        test.register_handler(drive_futures().widen().for_index(idx));
        test.register_handler(
            drive_delayed_action_runners::<ClientActions>().widen().for_index(idx),
        );
        test.register_handler(forward_client_request_to_shards_manager().widen().for_index(idx));
        test.register_handler(ignore_events::<SetChainInfo>().widen().for_index(idx));
    }
    test.register_handler(route_network_messages_to_client(Duration::milliseconds(10)));

    for idx in 0..NUM_CLIENTS {
        let mut delayed_action_runner = test.sender().for_index(idx).into_delayed_action_runner();
        test.sender().for_index(idx).send_adhoc_event("start_client", move |data| {
            data.client.start(&mut delayed_action_runner);
        });
    }
    test.run_for(Duration::seconds(10));
}

/// Handles outgoing network messages, and turns them into incoming client messages.
pub fn route_network_messages_to_client<
    Data: SupportsRoutingLookup,
    Event: TryIntoOrSelf<PeerManagerMessageRequest>
        + From<PeerManagerMessageRequest>
        + From<ClientSenderForNetworkMessage>,
>(
    network_delay: Duration,
) -> LoopEventHandler<Data, (usize, Event)> {
    // let mut route_back_lookup: HashMap<CryptoHash, usize> = HashMap::new();
    // let mut next_hash: u64 = 0;
    LoopEventHandler::new(
        move |event: (usize, Event),
              data: &mut Data,
              context: &LoopHandlerContext<(usize, Event)>| {
            let (idx, event) = event;
            let message = event.try_into_or_self().map_err(|event| (idx, event.into()))?;
            let PeerManagerMessageRequest::NetworkRequests(request) = message else {
                return Err((idx, message.into()));
            };

            let client_senders = (0..data.num_accounts())
                .map(|idx| {
                    context
                        .sender
                        .with_additional_delay(network_delay)
                        .for_index(idx)
                        .into_wrapped_multi_sender::<ClientSenderForNetworkMessage, ClientSenderForNetwork>()
                })
                .collect::<Vec<_>>();

            match request {
                NetworkRequests::Block { block } => {
                    for other_idx in 0..data.num_accounts() {
                        if other_idx != idx {
                            drop(client_senders[other_idx].send_async(BlockResponse {
                                block: block.clone(),
                                peer_id: PeerId::random(),
                                was_requested: false,
                            }));
                        }
                    }
                }
                NetworkRequests::Approval { approval_message } => {
                    let other_idx = data.index_for_account(&approval_message.target);
                    drop(
                        client_senders[other_idx]
                            .send_async(BlockApproval(approval_message.approval, PeerId::random())),
                    );
                }
                // TODO: Support more network message types as we expand the test.
                _ => return Err((idx, PeerManagerMessageRequest::NetworkRequests(request).into())),
            }

            Ok(())
        },
    )
}
