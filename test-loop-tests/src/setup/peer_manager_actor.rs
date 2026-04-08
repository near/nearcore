use itertools::Itertools;
use near_network::types::PeerMessage;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use parking_lot::{Mutex, MutexGuard};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Shared state across all the network actors. It handles the mapping between AccountId,
/// PeerId, and network state lookups so that TestLoopTransport can route messages.
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

impl TestLoopNetworkSharedState {
    pub fn new() -> Self {
        let inner = TestLoopNetworkSharedStateInner {
            account_to_peer_id: HashMap::new(),
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

    /// Register a node's account→peer_id mapping.
    pub fn add_peer(&self, account_id: AccountId, peer_id: PeerId) {
        self.0.lock().account_to_peer_id.insert(account_id, peer_id);
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
        guard.account_to_peer_id.values().cloned().collect_vec()
    }
}
