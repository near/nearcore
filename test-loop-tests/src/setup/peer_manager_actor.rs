use itertools::Itertools;
use near_async::futures::{DelayedActionRunnerExt as _, FutureSpawner, FutureSpawnerExt};
use near_async::messaging::{
    Actor, AsyncSender, CanSend, CanSendAsync, Handler, IntoMultiSender, IntoSender, Sender,
};
use near_async::test_loop::sender::TestLoopSender;
use near_async::time::{Clock, Duration};
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::{Block, BlockHeader};
use near_client::spice_data_distributor_actor::SpiceDistributorOutgoingReceipts;
use near_client::{BlockApproval, BlockResponse, SetNetworkInfo};
use near_network::PeerManagerActor;
use near_network::client::{
    BlockHeadersRequest, BlockHeadersResponse, BlockRequest, ChunkEndorsementMessage,
    EpochSyncRequestMessage, EpochSyncResponseMessage, OptimisticBlockMessage, ProcessTxRequest,
    ProcessTxResponse, SpiceChunkEndorsementMessage,
};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::spice_data_distribution::{
    SpiceChunkContractAccessesMessage, SpiceContractCodeRequestMessage,
    SpiceContractCodeResponseMessage, SpiceIncomingPartialData, SpicePartialDataRequest,
};
use near_network::state_witness::{
    ChunkContractAccessesMessage, ChunkStateWitnessAckMessage, ContractCodeRequestMessage,
    ContractCodeResponseMessage, PartialEncodedContractDeploysMessage,
    PartialEncodedStateWitnessForwardMessage, PartialEncodedStateWitnessMessage,
    PartialWitnessSenderForNetwork,
};
use near_network::types::{
    HighestHeightPeerInfo, NetworkInfo, NetworkRequests, NetworkResponses, PeerInfo,
    PeerManagerMessageRequest, PeerManagerMessageResponse, PeerMessage, ReasonForBan, SetChainInfo,
    StateSyncEvent, Tier3Request,
};
use near_o11y::span_wrapped_msg::{SpanWrapped, SpanWrappedMessageExt};
use near_primitives::genesis::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use parking_lot::{Mutex, MutexGuard};
use std::collections::{HashMap, HashSet, hash_map};
use std::sync::Arc;

/// Subset of ClientSenderForNetwork required for the TestLoop network.
/// We skip over the message handlers from view client.
#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ClientSenderForTestLoopNetwork {
    pub block: AsyncSender<SpanWrapped<BlockResponse>, ()>,
    pub block_headers: AsyncSender<SpanWrapped<BlockHeadersResponse>, Result<(), ReasonForBan>>,
    pub block_approval: AsyncSender<SpanWrapped<BlockApproval>, ()>,
    pub epoch_sync_request: Sender<EpochSyncRequestMessage>,
    pub epoch_sync_response: Sender<EpochSyncResponseMessage>,
    pub optimistic_block_receiver: Sender<SpanWrapped<OptimisticBlockMessage>>,
    pub network_info: AsyncSender<SpanWrapped<SetNetworkInfo>, ()>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct TxRequestHandleSenderForTestLoopNetwork {
    pub transaction: AsyncSender<ProcessTxRequest, ProcessTxResponse>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ChunkEndorsementSenderForTestLoopNetwork {
    pub chunk_endorsement: AsyncSender<ChunkEndorsementMessage, ()>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ViewClientSenderForTestLoopNetwork {
    pub block_headers_request: AsyncSender<BlockHeadersRequest, Option<Vec<Arc<BlockHeader>>>>,
    pub block_request: AsyncSender<BlockRequest, Option<Arc<Block>>>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct SpiceDataDistributorSenderForTestLoopNetwork {
    pub receipts: Sender<SpiceDistributorOutgoingReceipts>,
    pub incoming_data: Sender<SpiceIncomingPartialData>,
    pub data_requests: Sender<SpicePartialDataRequest>,
    pub contract_accesses: Sender<SpiceChunkContractAccessesMessage>,
    pub contract_code_request: Sender<SpiceContractCodeRequestMessage>,
    pub contract_code_response: Sender<SpiceContractCodeResponseMessage>,
}

/// This message is used to allow TestLoopPeerManagerActor to construct NetworkInfo for each
/// client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestLoopNetworkBlockInfo {
    pub peer: PeerInfo,
    pub block_header: BlockHeader,
}

pub type NetworkRequestHandler = Box<dyn Fn(NetworkRequests) -> Option<NetworkRequests>>;

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
#[allow(dead_code)]
pub struct TestLoopPeerManagerActor {
    handlers: Vec<NetworkRequestHandler>,

    /// Production PeerManagerActor for routing via real handle_msg_network_requests.
    /// Override handlers run first; unhandled requests fall through to this actor.
    production_actor: PeerManagerActor,

    /// NetworkState from the production actor, stored separately for access
    /// to account_announcements (pre-population with account→peer mappings).
    pub(crate) network_state: Arc<near_network::types::NetworkState>,

    client_sender: ClientSenderForTestLoopNetwork,
    shared_state: TestLoopNetworkSharedState,
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

#[allow(dead_code)]
impl TestLoopPeerManagerActor {
    /// Create a new TestLoopPeerManagerActor that delegates to a production
    /// PeerManagerActor for routing. Override handlers run first; unhandled
    /// requests are forwarded to the production actor's
    /// `handle_msg_network_requests`.
    pub fn new(
        production_actor: PeerManagerActor,
        network_state: Arc<near_network::types::NetworkState>,
        shared_state: &TestLoopNetworkSharedState,
        client_sender: ClientSenderForTestLoopNetwork,
        genesis_id: GenesisId,
    ) -> Self {
        Self {
            handlers: vec![],
            production_actor,
            network_state,
            client_sender,
            shared_state: shared_state.clone(),
            genesis_id,
            last_block_headers: HashMap::new(),
        }
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
        let future = self.client_sender.send_async(
            SetNetworkInfo(NetworkInfo {
                highest_height_peers: self
                    .last_block_headers
                    .iter()
                    .map(|(peer_info, header)| HighestHeightPeerInfo {
                        archival: self.shared_state.is_peer_archival(&peer_info.id),
                        genesis_id: self.genesis_id.clone(),
                        highest_block_hash: *header.hash(),
                        highest_block_height: header.height(),
                        tracked_shards: vec![],
                        peer_info: peer_info.clone(),
                    })
                    .collect(),
                ..NetworkInfo::default()
            })
            .span_wrap(),
        );
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

/// Filter function type for transport-level message interception.
/// Receives (from_peer, to_peer, message) and returns:
/// - `Some(msg)` to continue delivery (possibly with a modified message)
/// - `None` to drop the message silently
pub type TransportMessageFilter =
    Arc<dyn Fn(&PeerId, &PeerId, &PeerMessage) -> Option<PeerMessage> + Send + Sync>;

struct TestLoopNetworkSharedStateInner {
    account_to_peer_id: HashMap<AccountId, PeerId>,
    senders: HashMap<PeerId, Arc<OneClientSenders>>,
    // Everything sent using these senders should be dropped.
    drop_events_senders: Arc<OneClientSenders>,
    route_back: HashMap<CryptoHash, PeerId>,
    disallowed_peer_links: HashMap<PeerId, HashSet<PeerId>>,
    archival_peer_ids: HashSet<PeerId>,
    /// Per-node NetworkState instances, used by TestLoopTransport to record
    /// route_back entries when forwarding routed messages between nodes.
    network_states: HashMap<PeerId, Arc<near_network::types::NetworkState>>,
    /// Transport-level message filters applied before delivery. Each filter
    /// receives (from, to, msg) and returns `Some(msg)` to continue or `None`
    /// to drop. Multiple filters are applied in registration order;
    /// short-circuits on the first `None`.
    message_filters: Vec<TransportMessageFilter>,
}

/// Senders available for the networking layer, for one node in the test loop.
pub(crate) struct OneClientSenders {
    pub(crate) client_sender: ClientSenderForTestLoopNetwork,
    pub(crate) view_client_sender: ViewClientSenderForTestLoopNetwork,
    pub(crate) rpc_handler_sender: TxRequestHandleSenderForTestLoopNetwork,
    pub(crate) chunk_endorsement_handler_sender: ChunkEndorsementSenderForTestLoopNetwork,
    pub(crate) partial_witness_sender: PartialWitnessSenderForNetwork,
    pub(crate) shards_manager_sender: Sender<ShardsManagerRequestFromNetwork>,
    pub(crate) spice_data_distributor_actor: SpiceDataDistributorSenderForTestLoopNetwork,
    pub(crate) spice_core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
}

/// This actor can be used in situations when we don't expect any events to reach it.
/// For example when we want to simulate broken link between peers and drop all events sent
/// between.
pub struct UnreachableActor {}

impl<M, R> Handler<M, R> for UnreachableActor
where
    M: Send + 'static,
    R: Send,
{
    fn handle(&mut self, _msg: M) -> R {
        unreachable!("All events for this actor shouldn't be processed");
    }
}

impl Actor for UnreachableActor {}

fn to_drop_events_senders(s: TestLoopSender<UnreachableActor>) -> Arc<OneClientSenders> {
    Arc::new(OneClientSenders {
        client_sender: s.clone().into_multi_sender(),
        view_client_sender: s.clone().into_multi_sender(),
        rpc_handler_sender: s.clone().into_multi_sender(),
        chunk_endorsement_handler_sender: s.clone().into_multi_sender(),
        partial_witness_sender: s.clone().into_multi_sender(),
        shards_manager_sender: s.clone().into_sender(),
        spice_data_distributor_actor: s.clone().into_multi_sender(),
        spice_core_writer_sender: s.into_sender(),
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
            archival_peer_ids: HashSet::new(),
            network_states: HashMap::new(),
            message_filters: Vec::new(),
        };
        Self(Arc::new(Mutex::new(inner)))
    }

    /// Register a transport-level message filter. Filters are applied in
    /// registration order before each message delivery. Each filter receives
    /// `(from_peer, to_peer, &msg)` and returns `Some(msg)` to continue
    /// (possibly with a modified message) or `None` to drop silently.
    /// Short-circuits on the first `None`.
    pub fn register_message_filter(
        &self,
        filter: impl Fn(&PeerId, &PeerId, &PeerMessage) -> Option<PeerMessage> + Send + Sync + 'static,
    ) {
        self.0.lock().message_filters.push(Arc::new(filter));
    }

    /// Register a pre-wrapped `TransportMessageFilter` (Arc-wrapped closure).
    pub fn register_message_filter_arc(&self, filter: TransportMessageFilter) {
        self.0.lock().message_filters.push(filter);
    }

    /// Apply all registered message filters to a message. Returns `Some(msg)`
    /// if delivery should proceed, `None` if any filter dropped the message.
    pub(crate) fn apply_message_filters(
        &self,
        from: &PeerId,
        to: &PeerId,
        msg: PeerMessage,
    ) -> Option<PeerMessage> {
        let guard = self.0.lock();
        let mut msg = msg;
        for filter in &guard.message_filters {
            match filter(from, to, &msg) {
                Some(new_msg) => msg = new_msg,
                None => return None,
            }
        }
        Some(msg)
    }

    pub fn add_client<'a, D>(&self, data: &'a D)
    where
        AccountId: From<&'a D>,
        PeerId: From<&'a D>,
        ClientSenderForTestLoopNetwork: From<&'a D>,
        ViewClientSenderForTestLoopNetwork: From<&'a D>,
        TxRequestHandleSenderForTestLoopNetwork: From<&'a D>,
        ChunkEndorsementSenderForTestLoopNetwork: From<&'a D>,
        PartialWitnessSenderForNetwork: From<&'a D>,
        Sender<ShardsManagerRequestFromNetwork>: From<&'a D>,
        SpiceDataDistributorSenderForTestLoopNetwork: From<&'a D>,
        Sender<SpiceChunkEndorsementMessage>: From<&'a D>,
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
                chunk_endorsement_handler_sender: ChunkEndorsementSenderForTestLoopNetwork::from(
                    data,
                ),
                partial_witness_sender: PartialWitnessSenderForNetwork::from(data),
                shards_manager_sender: Sender::<ShardsManagerRequestFromNetwork>::from(data),
                spice_data_distributor_actor: SpiceDataDistributorSenderForTestLoopNetwork::from(
                    data,
                ),
                spice_core_writer_sender: Sender::<SpiceChunkEndorsementMessage>::from(data),
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

    pub(crate) fn account_to_peer_id(&self, account_id: &AccountId) -> PeerId {
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

    /// Returns `true` if messages are allowed from `from` to `to`.
    pub(crate) fn is_link_allowed(&self, from: &PeerId, to: &PeerId) -> bool {
        let guard = self.0.lock();
        !Self::is_peer_link_disallowed(&guard, from, to)
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

    pub(crate) fn senders_for_peer(
        &self,
        origin: &PeerId,
        peer_id: &PeerId,
    ) -> Arc<OneClientSenders> {
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

    pub fn mark_archival(&self, peer_id: &PeerId) {
        self.0.lock().archival_peer_ids.insert(peer_id.clone());
    }

    /// Register a node's NetworkState for route_back recording in TestLoopTransport.
    pub(crate) fn register_network_state(
        &self,
        peer_id: &PeerId,
        state: Arc<near_network::types::NetworkState>,
    ) {
        self.0.lock().network_states.insert(peer_id.clone(), state);
    }

    /// Get a node's NetworkState by peer_id.
    pub(crate) fn network_state_for_peer(
        &self,
        peer_id: &PeerId,
    ) -> Option<Arc<near_network::types::NetworkState>> {
        self.0.lock().network_states.get(peer_id).cloned()
    }

    fn is_peer_archival(&self, peer_id: &PeerId) -> bool {
        self.0.lock().archival_peer_ids.contains(peer_id)
    }

    pub(crate) fn all_peer_ids(&self) -> Vec<PeerId> {
        let guard = self.0.lock();
        guard.senders.keys().cloned().collect_vec()
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
    fn handle(&mut self, msg: PeerManagerMessageRequest) {
        Handler::<PeerManagerMessageRequest, PeerManagerMessageResponse>::handle(self, msg);
    }
}

impl Handler<PeerManagerMessageRequest, PeerManagerMessageResponse> for TestLoopPeerManagerActor {
    fn handle(&mut self, msg: PeerManagerMessageRequest) -> PeerManagerMessageResponse {
        let PeerManagerMessageRequest::NetworkRequests(request) = msg else {
            // Non-NetworkRequests variants (AdvertiseTier1Proxies, OutboundTcpConnect, etc.)
            // are irrelevant in testloop. Delegate to the production actor.
            return Handler::<PeerManagerMessageRequest, PeerManagerMessageResponse>::handle(
                &mut self.production_actor,
                msg,
            );
        };

        // Iterate over the override handlers in reverse order.
        let mut request = Some(request);
        for handler in self.handlers.iter().rev() {
            if let Some(new_request) = handler(request.take().unwrap()) {
                request = Some(new_request);
            } else {
                // Some handler was successfully able to handle the request.
                return PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse);
            }
        }

        // No override handler handled the request — delegate to the production
        // PeerManagerActor which uses real routing via NetworkState.
        let request = request.unwrap();
        Handler::<PeerManagerMessageRequest, PeerManagerMessageResponse>::handle(
            &mut self.production_actor,
            PeerManagerMessageRequest::NetworkRequests(request),
        )
    }
}

#[allow(dead_code)] // Will be removed in iteration 13 (old mock cleanup)
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

                let future = senders.client_sender.send_async(
                    BlockResponse {
                        block: block.clone(),
                        peer_id: my_peer_id.clone(),
                        was_requested: false,
                    }
                    .span_wrap(),
                );
                drop(future);
            }
            None
        }
        NetworkRequests::OptimisticBlock { chunk_producers, optimistic_block } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            for account_id in shared_state.accounts() {
                if !chunk_producers.contains(&account_id) {
                    continue;
                }
                let msg = OptimisticBlockMessage {
                    optimistic_block: optimistic_block.clone(),
                    from_peer: my_peer_id.clone(),
                }
                .span_wrap();
                let _ = shared_state
                    .senders_for_account(&my_account_id, &account_id)
                    .client_sender
                    .send(msg);
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
                .send_async(BlockApproval(approval_message.approval, PeerId::random()).span_wrap());
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
                .chunk_endorsement_handler_sender
                .send_async(ChunkEndorsementMessage(endorsement));
            drop(future);
            None
        }
        NetworkRequests::SpiceChunkEndorsement(target, endorsement) => {
            shared_state
                .senders_for_account(&my_account_id, &target)
                .spice_core_writer_sender
                .send(SpiceChunkEndorsementMessage(endorsement));
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
        NetworkRequests::BanPeer { peer_id, ban_reason } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            tracing::debug!(
                target: "test_loop",
                account = %my_account_id,
                ?peer_id,
                ?ban_reason,
                "test loop banning peer"
            );
            shared_state.disallow_requests(my_peer_id.clone(), peer_id.clone());
            shared_state.disallow_requests(peer_id, my_peer_id);
            None
        }
        NetworkRequests::StateRequestHeader { .. } => None,
        NetworkRequests::StateRequestPart { .. } => None,
        _ => Some(request),
    })
}

#[allow(dead_code)] // Will be removed in iteration 13 (old mock cleanup)
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
                let future =
                    responder.send_async(BlockHeadersResponse(response, peer_id).span_wrap());
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
                let Some(response) = future.await.unwrap() else {
                    // The peer may have GC'd this block. In production, the
                    // requester would simply not receive a response and retry
                    // with another peer. Mimic that by silently dropping.
                    return;
                };
                let future = responder.send_async(
                    BlockResponse { block: response, peer_id, was_requested: true }.span_wrap(),
                );
                drop(future);
            });
            None
        }
        _ => Some(request),
    })
}

#[allow(dead_code)] // Will be removed in iteration 13 (old mock cleanup)
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

#[allow(dead_code)] // Will be removed in iteration 13 (old mock cleanup)
fn network_message_to_state_snapshot_handler() -> NetworkRequestHandler {
    Box::new(move |request| match request {
        NetworkRequests::SnapshotHostEvent { .. } => None,
        _ => Some(request),
    })
}

#[allow(dead_code)] // Will be removed in iteration 13 (old mock cleanup)
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

#[allow(dead_code)] // Will be removed in iteration 13 (old mock cleanup)
fn network_message_to_spice_data_distributor_handler(
    my_account_id: &AccountId,
    shared_state: TestLoopNetworkSharedState,
) -> NetworkRequestHandler {
    let my_account_id = my_account_id.clone();
    Box::new(move |request| match request {
        NetworkRequests::SpicePartialData { partial_data, recipients } => {
            for account_id in recipients {
                assert!(account_id != my_account_id, "Sending message to self not supported.");
                shared_state
                    .senders_for_account(&my_account_id, &account_id)
                    .spice_data_distributor_actor
                    .send(SpiceIncomingPartialData { data: partial_data.clone() });
            }
            None
        }
        NetworkRequests::SpicePartialDataRequest { producer, request } => {
            assert!(producer != my_account_id, "Sending message to self not supported.");
            shared_state
                .senders_for_account(&my_account_id, &producer)
                .spice_data_distributor_actor
                .send(request);
            None
        }
        NetworkRequests::SpiceChunkContractAccesses(targets, accesses) => {
            for target in targets {
                shared_state
                    .senders_for_account(&my_account_id, &target)
                    .spice_data_distributor_actor
                    .send(SpiceChunkContractAccessesMessage(accesses.clone()));
            }
            None
        }
        NetworkRequests::SpiceContractCodeRequest(target, request) => {
            shared_state
                .senders_for_account(&my_account_id, &target)
                .spice_data_distributor_actor
                .send(SpiceContractCodeRequestMessage(request));
            None
        }
        NetworkRequests::SpiceContractCodeResponse(target, response) => {
            shared_state
                .senders_for_account(&my_account_id, &target)
                .spice_data_distributor_actor
                .send(SpiceContractCodeResponseMessage(response));
            None
        }
        _ => Some(request),
    })
}
