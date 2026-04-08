use itertools::Itertools;
use near_async::messaging::{AsyncSender, Handler, IntoMultiSender, IntoSender, Sender};
use near_async::test_loop::sender::TestLoopSender;
use near_async::{MultiSend, MultiSenderFrom};
use near_client::spice_data_distributor_actor::SpiceDistributorOutgoingReceipts;
use near_client::{BlockApproval, BlockResponse, SetNetworkInfo};
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
use near_network::state_witness::PartialWitnessSenderForNetwork;
use near_network::types::PeerMessage;
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use parking_lot::{Mutex, MutexGuard};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Subset of ClientSenderForNetwork required for the TestLoop network.
/// We skip over the message handlers from view client.
#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ClientSenderForTestLoopNetwork {
    pub block: AsyncSender<SpanWrapped<BlockResponse>, ()>,
    pub block_headers: AsyncSender<
        SpanWrapped<BlockHeadersResponse>,
        Result<(), near_network::types::ReasonForBan>,
    >,
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
    pub block_headers_request:
        AsyncSender<BlockHeadersRequest, Option<Vec<Arc<near_chain::BlockHeader>>>>,
    pub block_request: AsyncSender<BlockRequest, Option<Arc<near_chain::Block>>>,
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
/// Most fields are used by drop_events_senders (UnreachableActor) for network
/// partition simulation. The client_sender field is also read directly in
/// network_dispatch.rs for BlockRequest/BlockHeadersRequest response routing.
#[allow(dead_code)]
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

impl near_async::messaging::Actor for UnreachableActor {}

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

    pub(crate) fn all_peer_ids(&self) -> Vec<PeerId> {
        let guard = self.0.lock();
        guard.senders.keys().cloned().collect_vec()
    }
}
