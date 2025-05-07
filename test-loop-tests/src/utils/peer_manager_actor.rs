use std::collections::{HashMap, HashSet, hash_map};
use std::sync::Arc;

use itertools::Itertools;
use near_async::actix::ActixResult;
use near_async::futures::{DelayedActionRunnerExt as _, FutureSpawner, FutureSpawnerExt};
use near_async::messaging::{
    Actor, AsyncSender, CanSend, Handler, IntoMultiSender as _, IntoSender as _, SendAsync, Sender,
};
use near_async::test_loop::sender::TestLoopSender;
use near_async::time::{Clock, Duration};
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::BlockHeader;
use near_client::{BlockApproval, BlockResponse, SetNetworkInfo};
use near_network::client::{
    BlockHeadersRequest, BlockHeadersResponse, BlockRequest, ChunkEndorsementMessage,
    EpochSyncRequestMessage, EpochSyncResponseMessage, OptimisticBlockMessage, ProcessTxRequest,
    ProcessTxResponse,
};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::state_witness::{
    ChunkContractAccessesMessage, ChunkStateWitnessAckMessage, ContractCodeRequestMessage,
    ContractCodeResponseMessage, PartialEncodedContractDeploysMessage,
    PartialEncodedStateWitnessForwardMessage, PartialEncodedStateWitnessMessage,
    PartialWitnessSenderForNetwork,
};
use near_network::types::{
    HighestHeightPeerInfo, NetworkInfo, NetworkRequests, NetworkResponses, PeerInfo,
    PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo, StateSyncEvent,
    Tier3Request,
};
use near_primitives::genesis::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use parking_lot::{Mutex, MutexGuard};

/// Subset of ClientSenderForNetwork required for the TestLoop network.
/// We skip over the message handlers from view client.
#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ClientSenderForTestLoopNetwork {
    pub block: AsyncSender<BlockResponse, ()>,
    pub block_headers: AsyncSender<BlockHeadersResponse, ActixResult<BlockHeadersResponse>>,
    pub block_approval: AsyncSender<BlockApproval, ()>,
    pub epoch_sync_request: Sender<EpochSyncRequestMessage>,
    pub epoch_sync_response: Sender<EpochSyncResponseMessage>,
    pub optimistic_block_receiver: Sender<OptimisticBlockMessage>,
    pub network_info: AsyncSender<SetNetworkInfo, ()>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct TxRequestHandleSenderForTestLoopNetwork {
    pub transaction: AsyncSender<ProcessTxRequest, ProcessTxResponse>,
    pub chunk_endorsement: AsyncSender<ChunkEndorsementMessage, ()>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ViewClientSenderForTestLoopNetwork {
    pub block_headers_request: AsyncSender<BlockHeadersRequest, ActixResult<BlockHeadersRequest>>,
    pub block_request: AsyncSender<BlockRequest, ActixResult<BlockRequest>>,
}

/// This message is used to allow TestLoopPeerManagerActor to construct NetworkInfo for each
/// client.
#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct TestLoopNetworkBlockInfo {
    pub peer: PeerInfo,
    pub block_header: BlockHeader,
}

type NetworkRequestHandler = Box<dyn Fn(NetworkRequests) -> Option<NetworkRequests>>;

/// A custom actor for the TestLoop framework that can be used to send network messages across clients
/// in a multi-node test.
///
/// This actor has a set of handlers to handle PeerManagerMessageRequest messages. We have a set of
/// default handlers that handle messages sent to client, partial_witness actor, and shards_manager.
/// It is possible to override these handlers by registering a new handler using the
/// `register_override_handler()` method.
///
/// The signature of the handler is `dyn Fn(NetworkRequests) -> Option<NetworkRequests>`.
/// If the handler returns None, it means that the message was handled and no further processing is
/// required. If the handler returns Some(request), it means that the message was not handled and
/// the request should be passed to the next handler in the chain.
///
/// It's possible for a handler to modify the data in request and return it. This can be useful for
/// simulating things like malicious actors where we can modify the data in the request.
///
/// In case no handler is able to handle the request, the actor will panic.
///
/// NOTE: To make the override functionality work with the default handlers, the handlers are tried in
/// reverse order.
///
/// Examples of custom handlers
/// - Override handler to skip sending messages to or from a specific client.
/// - Override handler to simulate more network delays.
/// - Override handler to modify data and simulate malicious behavior.
pub struct TestLoopPeerManagerActor {
    handlers: Vec<NetworkRequestHandler>,

    client_sender: ClientSenderForTestLoopNetwork,
    genesis_id: GenesisId,
    last_block_headers: HashMap<PeerInfo, BlockHeader>,
}

impl Actor for TestLoopPeerManagerActor {
    fn start_actor(&mut self, ctx: &mut dyn near_async::futures::DelayedActionRunner<Self>) {
        const PUSH_NETWORK_INFO_PERIOD: Duration = Duration::milliseconds(100);
        self.push_network_info(ctx, PUSH_NETWORK_INFO_PERIOD);
    }
}

impl Handler<TestLoopNetworkBlockInfo> for TestLoopPeerManagerActor {
    fn handle(&mut self, msg: TestLoopNetworkBlockInfo) {
        match self.last_block_headers.entry(msg.peer) {
            hash_map::Entry::Occupied(entry) => {
                if entry.get().height() <= msg.block_header.height() {
                    *entry.into_mut() = msg.block_header;
                }
            }
            hash_map::Entry::Vacant(entry) => {
                entry.insert(msg.block_header);
            }
        }
    }
}

impl TestLoopPeerManagerActor {
    /// Create a new TestLoopPeerManagerActor with default handlers for client, partial_witness, and shards_manager.
    /// Note that we should be able to access the senders for these actors from the data type.
    pub fn new(
        clock: Clock,
        account_id: &AccountId,
        shared_state: &TestLoopNetworkSharedState,
        client_sender: ClientSenderForTestLoopNetwork,
        genesis_id: GenesisId,
        future_spawner: Arc<dyn FutureSpawner>,
    ) -> Self {
        let handlers = vec![
            network_message_to_client_handler(&account_id, shared_state.clone()),
            network_message_to_view_client_handler(
                account_id.clone(),
                shared_state.clone(),
                future_spawner,
            ),
            network_message_to_partial_witness_handler(&account_id, shared_state.clone()),
            network_message_to_shards_manager_handler(clock, &account_id, shared_state.clone()),
            network_message_to_state_snapshot_handler(),
        ];
        Self { handlers, client_sender, genesis_id, last_block_headers: HashMap::new() }
    }

    /// Register a new handler to override the default handlers.
    pub fn register_override_handler(&mut self, handler: NetworkRequestHandler) {
        // We add the handler to the end of the list and while processing the request, we iterate
        // over the handlers in reverse order.
        self.handlers.push(handler);
    }

    fn push_network_info(
        &self,
        ctx: &mut dyn near_async::futures::DelayedActionRunner<Self>,
        interval: Duration,
    ) {
        // Some tests (especially the ones having to do with sync) need NetworkInfo to be up to
        // date to work properly. That's why we're sending it periodically here.
        let future = self.client_sender.send_async(SetNetworkInfo(NetworkInfo {
            highest_height_peers: self
                .last_block_headers
                .iter()
                .map(|(peer_info, header)| HighestHeightPeerInfo {
                    archival: false,
                    genesis_id: self.genesis_id.clone(),
                    highest_block_hash: *header.hash(),
                    highest_block_height: header.height(),
                    tracked_shards: vec![],
                    peer_info: peer_info.clone(),
                })
                .collect(),
            ..NetworkInfo::default()
        }));
        drop(future);

        ctx.run_later("TestLoopPeerManagerActor::push_network_info", interval, move |act, ctx| {
            act.push_network_info(ctx, interval);
        });
    }
}

/// Shared state across all the network actors. It handles the mapping between AccountId,
/// PeerId, and the route back CryptoHash, so that individual network actors can do
/// routing.
#[derive(Clone)]
pub struct TestLoopNetworkSharedState(Arc<Mutex<TestLoopNetworkSharedStateInner>>);

struct TestLoopNetworkSharedStateInner {
    account_to_peer_id: HashMap<AccountId, PeerId>,
    senders: HashMap<PeerId, Arc<OneClientSenders>>,
    // Everything sent using these senders should be dropped.
    drop_events_senders: Arc<OneClientSenders>,
    route_back: HashMap<CryptoHash, PeerId>,
    disallowed_peer_links: HashMap<PeerId, HashSet<PeerId>>,
}

/// Senders available for the networking layer, for one node in the test loop.
struct OneClientSenders {
    client_sender: ClientSenderForTestLoopNetwork,
    view_client_sender: ViewClientSenderForTestLoopNetwork,
    rpc_handler_sender: TxRequestHandleSenderForTestLoopNetwork,
    partial_witness_sender: PartialWitnessSenderForNetwork,
    shards_manager_sender: Sender<ShardsManagerRequestFromNetwork>,
    peer_manager_sender: Sender<TestLoopNetworkBlockInfo>,
}

/// This actor can be used in situations when we don't expect any events to reach it.
/// For example when we want to simulate broken link between peers and drop all events sent
/// between.
pub struct UnreachableActor {}

impl<M: actix::Message> Handler<M> for UnreachableActor
where
    M::Result: std::marker::Send,
{
    fn handle(&mut self, _msg: M) -> M::Result {
        unreachable!("All events for this actor shouldn't be processed");
    }
}

impl Actor for UnreachableActor {}

fn to_drop_events_senders(s: TestLoopSender<UnreachableActor>) -> Arc<OneClientSenders> {
    Arc::new(OneClientSenders {
        client_sender: s.clone().into_multi_sender(),
        view_client_sender: s.clone().into_multi_sender(),
        rpc_handler_sender: s.clone().into_multi_sender(),
        partial_witness_sender: s.clone().into_multi_sender(),
        shards_manager_sender: s.clone().into_sender(),
        peer_manager_sender: s.into_sender(),
    })
}

impl TestLoopNetworkSharedState {
    pub fn new(unreachable_actor_sender: TestLoopSender<UnreachableActor>) -> Self {
        let inner = TestLoopNetworkSharedStateInner {
            account_to_peer_id: HashMap::new(),
            senders: HashMap::new(),
            drop_events_senders: to_drop_events_senders(unreachable_actor_sender),
            route_back: HashMap::new(),
            disallowed_peer_links: HashMap::new(),
        };
        Self(Arc::new(Mutex::new(inner)))
    }

    pub fn add_client<'a, D>(&self, data: &'a D)
    where
        AccountId: From<&'a D>,
        PeerId: From<&'a D>,
        ClientSenderForTestLoopNetwork: From<&'a D>,
        ViewClientSenderForTestLoopNetwork: From<&'a D>,
        TxRequestHandleSenderForTestLoopNetwork: From<&'a D>,
        PartialWitnessSenderForNetwork: From<&'a D>,
        Sender<ShardsManagerRequestFromNetwork>: From<&'a D>,
        Sender<TestLoopNetworkBlockInfo>: From<&'a D>,
    {
        let account_id = AccountId::from(data);
        let peer_id = PeerId::from(data);

        let mut guard = self.0.lock();
        guard.account_to_peer_id.insert(account_id, peer_id.clone());
        guard.senders.insert(
            peer_id,
            Arc::new(OneClientSenders {
                client_sender: ClientSenderForTestLoopNetwork::from(data),
                view_client_sender: ViewClientSenderForTestLoopNetwork::from(data),
                rpc_handler_sender: TxRequestHandleSenderForTestLoopNetwork::from(data),
                partial_witness_sender: PartialWitnessSenderForNetwork::from(data),
                shards_manager_sender: Sender::<ShardsManagerRequestFromNetwork>::from(data),
                peer_manager_sender: Sender::<TestLoopNetworkBlockInfo>::from(data),
            }),
        );
    }

    /// Stops processing of requests from `from` peer to `to` peer.
    pub fn disallow_requests(&self, from: PeerId, to: PeerId) {
        let mut guard = self.0.lock();
        guard.disallowed_peer_links.entry(from).or_default().insert(to);
    }

    /// Allows processing of requests between all peers.
    pub fn allow_all_requests(&self) {
        let mut guard = self.0.lock();
        guard.disallowed_peer_links = HashMap::new();
    }

    fn account_to_peer_id(&self, account_id: &AccountId) -> PeerId {
        let guard = self.0.lock();
        guard.account_to_peer_id.get(account_id).unwrap().clone()
    }

    fn is_peer_link_disallowed(
        guard: &MutexGuard<TestLoopNetworkSharedStateInner>,
        from: &PeerId,
        to: &PeerId,
    ) -> bool {
        guard.disallowed_peer_links.get(from).and_then(|blocklist| blocklist.get(to)).is_some()
    }
    fn senders_for_account(
        &self,
        origin: &AccountId,
        account_id: &AccountId,
    ) -> Arc<OneClientSenders> {
        let guard = self.0.lock();
        let origin_peer_id = &guard.account_to_peer_id[origin];
        let peer_id = &guard.account_to_peer_id[account_id];
        if Self::is_peer_link_disallowed(&guard, origin_peer_id, peer_id) {
            return guard.drop_events_senders.clone();
        }
        guard.senders.get(peer_id).unwrap().clone()
    }

    fn senders_for_peer(&self, origin: &PeerId, peer_id: &PeerId) -> Arc<OneClientSenders> {
        let guard = self.0.lock();
        if Self::is_peer_link_disallowed(&guard, origin, peer_id) {
            return guard.drop_events_senders.clone();
        }
        guard.senders.get(peer_id).unwrap().clone()
    }

    fn generate_route_back(&self, peer_id: &PeerId) -> CryptoHash {
        let mut guard = self.0.lock();
        let route_id = CryptoHash::hash_borsh(guard.route_back.len());
        guard.route_back.insert(route_id, peer_id.clone());
        route_id
    }

    fn senders_for_route_back(
        &self,
        origin: &AccountId,
        route_back: &CryptoHash,
    ) -> Arc<OneClientSenders> {
        let guard = self.0.lock();
        let origin_peer_id = &guard.account_to_peer_id[origin];
        let peer_id = guard.route_back.get(route_back).unwrap();
        if Self::is_peer_link_disallowed(&guard, origin_peer_id, peer_id) {
            return guard.drop_events_senders.clone();
        }
        guard.senders.get(peer_id).unwrap().clone()
    }

    fn accounts(&self) -> Vec<AccountId> {
        let guard = self.0.lock();
        let account_ids = guard.account_to_peer_id.keys().cloned().collect_vec();
        account_ids
    }
}

impl Handler<SetChainInfo> for TestLoopPeerManagerActor {
    fn handle(&mut self, _msg: SetChainInfo) {}
}

impl Handler<StateSyncEvent> for TestLoopPeerManagerActor {
    fn handle(&mut self, _msg: StateSyncEvent) {}
}

impl Handler<Tier3Request> for TestLoopPeerManagerActor {
    fn handle(&mut self, _msg: Tier3Request) {}
}

impl Handler<PeerManagerMessageRequest> for TestLoopPeerManagerActor {
    fn handle(&mut self, msg: PeerManagerMessageRequest) -> PeerManagerMessageResponse {
        let PeerManagerMessageRequest::NetworkRequests(request) = msg else {
            panic!("Unexpected message: {:?}", msg);
        };

        // Iterate over the handlers in reverse order to allow for overriding the default handlers.
        let mut request = Some(request);
        for handler in self.handlers.iter().rev() {
            if let Some(new_request) = handler(request.take().unwrap()) {
                request = Some(new_request);
            } else {
                // Some handler was successfully able to handle the request.
                return PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse);
            }
        }
        // If no handler was able to handle the request, panic.
        panic!("Unhandled request: {:?}", request);
    }
}

fn network_message_to_client_handler(
    my_account_id: &AccountId,
    shared_state: TestLoopNetworkSharedState,
) -> NetworkRequestHandler {
    let my_account_id = my_account_id.clone();
    Box::new(move |request| match request {
        NetworkRequests::Block { block } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            for account_id in shared_state.accounts() {
                if account_id == my_account_id {
                    continue;
                }

                let senders = shared_state.senders_for_account(&my_account_id, &account_id);

                let future = senders.client_sender.send_async(BlockResponse {
                    block: block.clone(),
                    peer_id: my_peer_id.clone(),
                    was_requested: false,
                });
                drop(future);

                senders.peer_manager_sender.send(TestLoopNetworkBlockInfo {
                    peer: PeerInfo {
                        id: my_peer_id.clone(),
                        addr: None,
                        account_id: Some(my_account_id.clone()),
                    },
                    block_header: block.header().clone(),
                });
            }
            None
        }
        NetworkRequests::OptimisticBlock { optimistic_block } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            for account_id in shared_state.accounts() {
                if account_id != my_account_id {
                    let _ = shared_state
                        .senders_for_account(&my_account_id, &account_id)
                        .client_sender
                        .send(OptimisticBlockMessage {
                            optimistic_block: optimistic_block.clone(),
                            from_peer: my_peer_id.clone(),
                        });
                }
            }
            None
        }
        NetworkRequests::Approval { approval_message } => {
            assert_ne!(
                approval_message.target, my_account_id,
                "Sending message to self not supported."
            );
            let future = shared_state
                .senders_for_account(&my_account_id, &approval_message.target)
                .client_sender
                .send_async(BlockApproval(approval_message.approval, PeerId::random()));
            drop(future);
            None
        }
        NetworkRequests::ForwardTx(account, transaction) => {
            assert_ne!(account, my_account_id, "Sending message to self not supported.");
            let future = shared_state
                .senders_for_account(&my_account_id, &account)
                .rpc_handler_sender
                .send_async(ProcessTxRequest {
                    transaction,
                    is_forwarded: true,
                    check_only: false,
                });
            drop(future);
            None
        }
        NetworkRequests::ChunkEndorsement(target, endorsement) => {
            let future = shared_state
                .senders_for_account(&my_account_id, &target)
                .rpc_handler_sender
                .send_async(ChunkEndorsementMessage(endorsement));
            drop(future);
            None
        }
        NetworkRequests::EpochSyncRequest { peer_id } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            assert_ne!(peer_id, my_peer_id, "Sending message to self not supported.");
            shared_state
                .senders_for_peer(&my_peer_id, &peer_id)
                .client_sender
                .send(EpochSyncRequestMessage { from_peer: my_peer_id });
            None
        }
        NetworkRequests::EpochSyncResponse { peer_id, proof } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            shared_state
                .senders_for_peer(&my_peer_id, &peer_id)
                .client_sender
                .send(EpochSyncResponseMessage { from_peer: my_peer_id, proof });
            None
        }
        NetworkRequests::StateRequestPart { .. } => None,
        _ => Some(request),
    })
}

fn network_message_to_view_client_handler(
    my_account_id: AccountId,
    shared_state: TestLoopNetworkSharedState,
    future_spawner: Arc<dyn FutureSpawner>,
) -> NetworkRequestHandler {
    Box::new(move |request| match request {
        NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            let responder =
                shared_state.senders_for_peer(&peer_id, &my_peer_id).client_sender.clone();
            let future = shared_state
                .senders_for_peer(&my_peer_id, &peer_id)
                .view_client_sender
                .send_async(BlockHeadersRequest(hashes));
            future_spawner.spawn("wait for ViewClient to handle BlockHeadersRequest", async move {
                let response = future.await.unwrap().unwrap();
                let future = responder.send_async(BlockHeadersResponse(response, peer_id));
                drop(future);
            });
            None
        }
        NetworkRequests::BlockRequest { hash, peer_id } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            let responder =
                shared_state.senders_for_peer(&peer_id, &my_peer_id).client_sender.clone();
            let future = shared_state
                .senders_for_peer(&my_peer_id, &peer_id)
                .view_client_sender
                .send_async(BlockRequest(hash));
            future_spawner.spawn("wait for ViewClient to handle BlockRequest", async move {
                let response = *future.await.unwrap().unwrap_or_else(|| {
                    panic!("Expect block with {hash} to be available on {peer_id}")
                });
                let future = responder.send_async(BlockResponse {
                    block: response,
                    peer_id,
                    was_requested: true,
                });
                drop(future);
            });
            None
        }
        _ => Some(request),
    })
}

fn network_message_to_partial_witness_handler(
    my_account_id: &AccountId,
    shared_state: TestLoopNetworkSharedState,
) -> NetworkRequestHandler {
    let my_account_id = my_account_id.clone();
    Box::new(move |request| match request {
        NetworkRequests::ChunkStateWitnessAck(target, witness_ack) => {
            assert_ne!(target, my_account_id, "Sending message to self not supported.");
            shared_state
                .senders_for_account(&my_account_id, &target)
                .partial_witness_sender
                .send(ChunkStateWitnessAckMessage(witness_ack));
            None
        }

        NetworkRequests::PartialEncodedStateWitness(validator_witness_tuple) => {
            for (target, partial_witness) in validator_witness_tuple {
                shared_state
                    .senders_for_account(&my_account_id, &target)
                    .partial_witness_sender
                    .send(PartialEncodedStateWitnessMessage(partial_witness));
            }
            None
        }
        NetworkRequests::PartialEncodedStateWitnessForward(chunk_validators, partial_witness) => {
            for target in chunk_validators {
                shared_state
                    .senders_for_account(&my_account_id, &target)
                    .partial_witness_sender
                    .send(PartialEncodedStateWitnessForwardMessage(partial_witness.clone()));
            }
            None
        }
        NetworkRequests::ChunkContractAccesses(chunk_validators, accesses) => {
            for target in chunk_validators {
                shared_state
                    .senders_for_account(&my_account_id, &target)
                    .partial_witness_sender
                    .send(ChunkContractAccessesMessage(accesses.clone()));
            }
            None
        }
        NetworkRequests::ContractCodeRequest(target, request) => {
            shared_state
                .senders_for_account(&my_account_id, &target)
                .partial_witness_sender
                .send(ContractCodeRequestMessage(request));
            None
        }
        NetworkRequests::ContractCodeResponse(target, response) => {
            shared_state
                .senders_for_account(&my_account_id, &target)
                .partial_witness_sender
                .send(ContractCodeResponseMessage(response));
            None
        }
        NetworkRequests::PartialEncodedContractDeploys(accounts, deploys) => {
            for account in accounts {
                shared_state
                    .senders_for_account(&my_account_id, &account)
                    .partial_witness_sender
                    .send(PartialEncodedContractDeploysMessage(deploys.clone()));
            }
            None
        }
        _ => Some(request),
    })
}

fn network_message_to_state_snapshot_handler() -> NetworkRequestHandler {
    Box::new(move |request| match request {
        NetworkRequests::SnapshotHostInfo { .. } => None,
        _ => Some(request),
    })
}

fn network_message_to_shards_manager_handler(
    clock: Clock,
    my_account_id: &AccountId,
    shared_state: TestLoopNetworkSharedState,
) -> NetworkRequestHandler {
    let my_account_id = my_account_id.clone();
    Box::new(move |request| match request {
        NetworkRequests::PartialEncodedChunkRequest { target, request, .. } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            let route_back = shared_state.generate_route_back(&my_peer_id);
            let target = target.account_id.unwrap();
            assert!(target != my_account_id, "Sending message to self not supported.");
            shared_state.senders_for_account(&my_account_id, &target).shards_manager_sender.send(
                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                    partial_encoded_chunk_request: request,
                    route_back,
                },
            );
            None
        }
        NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
            // Use route_back information to send the response back to the correct client.
            shared_state
                .senders_for_route_back(&my_account_id, &route_back)
                .shards_manager_sender
                .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                    partial_encoded_chunk_response: response,
                    received_time: clock.now(),
                });
            None
        }
        NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
            assert!(account_id != my_account_id, "Sending message to self not supported.");
            shared_state
                .senders_for_account(&my_account_id, &account_id)
                .shards_manager_sender
                .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                    partial_encoded_chunk.into(),
                ));
            None
        }
        NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
            assert!(account_id != my_account_id, "Sending message to self not supported.");
            shared_state
                .senders_for_account(&my_account_id, &account_id)
                .shards_manager_sender
                .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(forward));
            None
        }
        _ => Some(request),
    })
}
