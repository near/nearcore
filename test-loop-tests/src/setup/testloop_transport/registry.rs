use super::transport::TestLoopTransport;
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// Cross-node lookup for testloop transports. Populated as nodes start
/// (via `setup_client`) and updated by `kill_node` / `restart_node`.
#[derive(Clone, Default)]
pub(crate) struct TestLoopNodeRegistry {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    nodes: Mutex<HashMap<PeerId, Arc<TestLoopTransport>>>,
}

impl TestLoopNodeRegistry {
    pub(crate) fn register(&self, peer_id: PeerId, transport: Arc<TestLoopTransport>) {
        self.inner.nodes.lock().insert(peer_id, transport);
    }

    pub(crate) fn unregister(&self, peer_id: &PeerId) {
        self.inner.nodes.lock().remove(peer_id);
    }

    pub(crate) fn get(&self, peer_id: &PeerId) -> Option<Arc<TestLoopTransport>> {
        self.inner.nodes.lock().get(peer_id).cloned()
    }

    /// True if the peer is currently registered (i.e. not killed).
    /// Used by `restart_node` to filter out dead nodes from re-seed.
    pub(crate) fn contains(&self, peer_id: &PeerId) -> bool {
        self.inner.nodes.lock().contains_key(peer_id)
    }
}
