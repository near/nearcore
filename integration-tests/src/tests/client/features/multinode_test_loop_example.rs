use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::messaging::{noop, IntoMultiSender, IntoSender, MessageWithCallback, SendAsync};
use near_async::test_loop::adhoc::{handle_adhoc_events, AdhocEvent, AdhocEventSender};
use near_async::test_loop::event_handler::{
    ignore_events, LoopEventHandler, LoopHandlerContext, TryIntoOrSelf,
};
use near_async::test_loop::futures::{
    drive_async_computations, drive_delayed_action_runners, drive_futures,
    TestLoopAsyncComputationEvent, TestLoopDelayedActionEvent, TestLoopTask,
};
use near_async::test_loop::TestLoopBuilder;
use near_async::time::Duration;
use near_chain::chunks_store::ReadOnlyChunksStore;
use near_chain::ChainGenesis;
use near_chain_configs::{ClientConfig, Genesis, GenesisConfig, GenesisRecords};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::client::ShardsManagerResponse;
use near_chunks::test_loop::{
    forward_client_request_to_shards_manager, forward_network_request_to_shards_manager,
    route_shards_manager_network_messages,
};
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
    BlockApproval, BlockResponse, ChunkEndorsementMessage, ChunkStateWitnessMessage,
    ClientSenderForNetwork, ClientSenderForNetworkMessage, ProcessTxRequest,
};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::test_loop::SupportsRoutingLookup;
use near_network::types::{
    NetworkRequests, PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo,
};
use near_primitives::network::PeerId;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, AccountInfo};
use near_primitives::utils::from_timestamp;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{QueryRequest, QueryResponseKind};
use near_primitives_core::account::{AccessKey, Account};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::Balance;
use near_store::config::StateSnapshotType;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use near_store::TrieConfig;
use nearcore::NightshadeRuntime;
use std::collections::HashMap;
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
    ClientDelayedActions(TestLoopDelayedActionEvent<ClientActions>),
    /// Allows delayed actions to be posted, as if ShardsManagerActor scheduled them, e.g. timers.
    ShardsManagerDelayedActions(TestLoopDelayedActionEvent<ShardsManager>),

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
    /// Message that the SyncJobs component sends to itself.
    SyncJobsEventFromSyncJobs(SyncJobsSenderForSyncJobsMessage),

    /// Message that the client sends to the ShardsManager component.
    ShardsManagerRequestFromClient(ShardsManagerRequestFromClient),
    /// Message that the network layer sends to the ShardsManager component.
    ShardsManagerRequestFromNetwork(ShardsManagerRequestFromNetwork),

    /// Outgoing network message that is sent by any of the components of this node.
    OutgoingNetworkMessage(PeerManagerMessageRequest),
    /// Same as OutgoingNetworkMessage, but of the variant that requests a response.
    OutgoingNetworkMessageForResult(
        MessageWithCallback<PeerManagerMessageRequest, PeerManagerMessageResponse>,
    ),
    /// Calls to the network component to set chain info.
    SetChainInfo(SetChainInfo),
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
        let runtime_adapter = NightshadeRuntime::test_with_trie_config(
            Path::new("."),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
            TrieConfig { load_mem_tries_for_all_shards: true, ..Default::default() },
            StateSnapshotType::ForReshardingOnly,
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
            Arc::new(
                builder
                    .sender()
                    .for_index(idx)
                    .into_async_computation_spawner(|_| Duration::milliseconds(80)),
            ),
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
        // Futures, adhoc events, async computations.
        test.register_handler(drive_futures().widen().for_index(idx));
        test.register_handler(handle_adhoc_events::<TestData>().widen().for_index(idx));
        test.register_handler(drive_async_computations().widen().for_index(idx));

        // Delayed actions.
        test.register_handler(
            drive_delayed_action_runners::<ClientActions>().widen().for_index(idx),
        );
        test.register_handler(
            drive_delayed_action_runners::<ShardsManager>().widen().for_index(idx),
        );

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
        // TODO: handle state sync adapter -> client.

        // Messages to the SyncJobs component.
        test.register_handler(
            forward_sync_jobs_messages_from_client_to_sync_jobs_actions(
                test.sender().for_index(idx).into_future_spawner(),
            )
            .widen()
            .for_index(idx),
        );
        // TODO: handle SyncJobs -> SyncJobs.

        // Messages to the ShardsManager component.
        test.register_handler(forward_client_request_to_shards_manager().widen().for_index(idx));
        test.register_handler(forward_network_request_to_shards_manager().widen().for_index(idx));

        // Messages to the network layer; multi-node messages are handled below.
        test.register_handler(ignore_events::<SetChainInfo>().widen().for_index(idx));
    }
    // Handles network routing. Outgoing messages are handled by emitting incoming messages to the
    // appropriate component of the appropriate node index.
    test.register_handler(route_network_messages_to_client(NETWORK_DELAY));
    test.register_handler(route_shards_manager_network_messages(NETWORK_DELAY));

    // Bootstrap the test by starting the components.
    // We use adhoc events for these, just so that the visualizer can see these as events rather
    // than happening outside of the TestLoop framework. Other than that, we could also just remove
    // the send_adhoc_event part and the test would still work.
    for idx in 0..NUM_CLIENTS {
        let sender = test.sender().for_index(idx);
        test.sender().for_index(idx).send_adhoc_event("start_client", move |data| {
            data.client.start(&mut sender.into_delayed_action_runner());
        });

        let sender = test.sender().for_index(idx);
        test.sender().for_index(idx).send_adhoc_event("start_shards_manager", move |data| {
            data.shards_manager.periodically_resend_chunk_requests(
                &mut sender.into_delayed_action_runner(),
                Duration::milliseconds(100),
            );
        })
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

    let mut balances = accounts
        .iter()
        .cloned()
        .map(|account| (account, initial_balance))
        .collect::<HashMap<_, _>>();
    for i in 0..accounts.len() {
        let amount = ONE_NEAR * (i as u128 + 1);
        let tx = SignedTransaction::send_money(
            1,
            accounts[i].clone(),
            accounts[(i + 1) % accounts.len()].clone(),
            &create_user_test_signer(&accounts[i]),
            amount,
            *test.data[0].client.client.chain.get_block_by_height(10002).unwrap().hash(),
        );
        *balances.get_mut(&accounts[i]).unwrap() -= amount;
        *balances.get_mut(&accounts[(i + 1) % accounts.len()]).unwrap() += amount;
        drop(
            test.sender()
                .for_index(i % NUM_CLIENTS)
                .into_wrapped_multi_sender::<ClientSenderForNetworkMessage, ClientSenderForNetwork>(
                )
                .send_async(ProcessTxRequest {
                    transaction: tx,
                    is_forwarded: false,
                    check_only: false,
                }),
        );
    }
    test.run_until(
        |data| data[0].client.client.chain.head().unwrap().height == 10008,
        Duration::seconds(8),
    );

    for account in &accounts {
        assert_eq!(
            test.data.query_balance(account),
            *balances.get(account).unwrap(),
            "Account balance mismatch for account {}",
            account
        );
    }

    // Give the test a chance to finish off remaining important events in the event loop, which can
    // be important for properly shutting down the nodes.
    test.finish_remaining_events(Duration::seconds(1));
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
                    if other_idx != idx {
                        drop(client_senders[other_idx].send_async(BlockApproval(
                            approval_message.approval,
                            PeerId::random(),
                        )));
                    } else {
                        tracing::warn!("Dropping message to self");
                    }
                }
                NetworkRequests::ForwardTx(account, transaction) => {
                    let other_idx = data.index_for_account(&account);
                    if other_idx != idx {
                        drop(client_senders[other_idx].send_async(ProcessTxRequest {
                            transaction,
                            is_forwarded: true,
                            check_only: false,
                        }))
                    } else {
                        tracing::warn!("Dropping message to self");
                    }
                }
                NetworkRequests::ChunkEndorsement(target, endorsement) => {
                    let other_idx = data.index_for_account(&target);
                    if other_idx != idx {
                        drop(
                            client_senders[other_idx]
                                .send_async(ChunkEndorsementMessage(endorsement)),
                        );
                    } else {
                        tracing::warn!("Dropping message to self");
                    }
                }
                NetworkRequests::ChunkStateWitness(targets, witness) => {
                    let other_idxes = targets
                        .iter()
                        .map(|account| data.index_for_account(account))
                        .collect::<Vec<_>>();
                    for other_idx in &other_idxes {
                        if *other_idx != idx {
                            drop(
                                client_senders[*other_idx]
                                    .send_async(ChunkStateWitnessMessage(witness.clone())),
                            );
                        } else {
                            tracing::warn!(
                                "ChunkStateWitness asked to send to nodes {:?}, but {} is ourselves, so skipping that",
                                other_idxes, idx);
                        }
                    }
                }
                // TODO: Support more network message types as we expand the test.
                _ => return Err((idx, PeerManagerMessageRequest::NetworkRequests(request).into())),
            }

            Ok(())
        },
    )
}

// TODO: This would be a good starting point for turning this into a test util.
trait ClientQueries {
    fn query_balance(&self, account: &AccountId) -> Balance;
}

impl<Data: AsRef<Client> + AsRef<AccountId>> ClientQueries for Vec<Data> {
    fn query_balance(&self, account_id: &AccountId) -> Balance {
        let client: &Client = self[0].as_ref();
        let head = client.chain.head().unwrap();
        let last_block = client.chain.get_block(&head.last_block_hash).unwrap();
        let shard_id =
            client.epoch_manager.account_id_to_shard_id(&account_id, &head.epoch_id).unwrap();
        let shard_uid = client.epoch_manager.shard_id_to_uid(shard_id, &head.epoch_id).unwrap();
        let last_chunk_header = &last_block.chunks()[shard_id as usize];

        for i in 0..self.len() {
            let client: &Client = self[i].as_ref();
            let tracks_shard = client
                .epoch_manager
                .cares_about_shard_from_prev_block(
                    &head.prev_block_hash,
                    &self[i].as_ref(),
                    shard_id,
                )
                .unwrap();
            if tracks_shard {
                let response = client
                    .runtime_adapter
                    .query(
                        shard_uid,
                        &last_chunk_header.prev_state_root(),
                        last_block.header().height(),
                        last_block.header().raw_timestamp(),
                        last_block.header().prev_hash(),
                        last_block.header().hash(),
                        last_block.header().epoch_id(),
                        &QueryRequest::ViewAccount { account_id: account_id.clone() },
                    )
                    .unwrap();
                match response.kind {
                    QueryResponseKind::ViewAccount(account_view) => return account_view.amount,
                    _ => panic!("Wrong return value"),
                }
            }
        }
        panic!("No client tracks shard {}", shard_id);
    }
}
