use near_network::types::PeerMessage;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use parking_lot::{Mutex, MutexGuard};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Filter function for transport-level message interception.
/// Receives (from_peer, to_peer, message) and returns:
/// - `Some(msg)` to continue delivery (possibly modified)
/// - `None` to drop the message silently
pub type TransportMessageFilter =
    Arc<dyn Fn(&PeerId, &PeerId, &PeerMessage) -> Option<PeerMessage> + Send + Sync>;

/// Shared state across all the network actors. It handles the mapping between AccountId,
/// PeerId, and the route back CryptoHash, so that individual network actors can do
/// routing.
#[derive(Clone)]
pub struct TestLoopNetworkSharedState(Arc<Mutex<TestLoopNetworkSharedStateInner>>);

struct TestLoopNetworkSharedStateInner {
    account_to_peer_id: HashMap<AccountId, PeerId>,
    disallowed_peer_links: HashMap<PeerId, HashSet<PeerId>>,
    archival_peer_ids: HashSet<PeerId>,
    message_filters: Vec<TransportMessageFilter>,
}

impl TestLoopNetworkSharedState {
    pub fn new() -> Self {
        let inner = TestLoopNetworkSharedStateInner {
            account_to_peer_id: HashMap::new(),
            disallowed_peer_links: HashMap::new(),
            archival_peer_ids: HashSet::new(),
            message_filters: Vec::new(),
        };
        Self(Arc::new(Mutex::new(inner)))
    }

    pub fn add_client<'a, D>(&self, data: &'a D)
    where
        AccountId: From<&'a D>,
        PeerId: From<&'a D>,
    {
        let account_id = AccountId::from(data);
        let peer_id = PeerId::from(data);

        let mut guard = self.0.lock();
        guard.account_to_peer_id.insert(account_id, peer_id);
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

    /// Returns true if the link from `from` to `to` is allowed (not partitioned).
    pub fn is_link_allowed(&self, from: &PeerId, to: &PeerId) -> bool {
        let guard = self.0.lock();
        !Self::is_peer_link_disallowed(&guard, from, to)
    }

    pub fn mark_archival(&self, peer_id: &PeerId) {
        self.0.lock().archival_peer_ids.insert(peer_id.clone());
    }

    /// Register a transport-level message filter.
    pub fn register_message_filter(
        &self,
        filter: impl Fn(&PeerId, &PeerId, &PeerMessage) -> Option<PeerMessage> + Send + Sync + 'static,
    ) {
        self.0.lock().message_filters.push(Arc::new(filter));
    }

    /// Register a pre-built Arc filter.
    #[allow(dead_code)]
    pub fn register_message_filter_arc(&self, filter: TransportMessageFilter) {
        self.0.lock().message_filters.push(filter);
    }

    /// Apply all filters in order. Returns None if any filter drops the message.
    pub fn apply_message_filters(
        &self,
        from: &PeerId,
        to: &PeerId,
        msg: PeerMessage,
    ) -> Option<PeerMessage> {
        let guard = self.0.lock();
        let mut current = msg;
        for filter in &guard.message_filters {
            match filter(from, to, &current) {
                Some(m) => current = m,
                None => return None,
            }
        }
        Some(current)
    }
}
