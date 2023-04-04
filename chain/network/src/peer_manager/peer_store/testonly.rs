use crate::types::KnownPeerState;

impl super::PeerStore {
    pub fn dump(&self) -> Vec<KnownPeerState> {
        self.0.lock().peer_states.iter().map(|(_, v)| v.clone()).collect()
    }
}
