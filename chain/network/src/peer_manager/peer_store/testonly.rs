use crate::types::{KnownPeerState};

impl super::PeerStore {
    pub fn dump(&self) -> Vec<KnownPeerState> {
        self.0.lock().peer_states.values().cloned().collect()
    }
}
