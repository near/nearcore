use crate::types::ConnectionInfo;
use near_primitives::network::PeerId;

impl super::ConnectionStore {
    pub(crate) fn contains_outbound_connection(&self, peer_id: &PeerId) -> bool {
        return self.0.load().contains_outbound(&peer_id);
    }

    pub(crate) fn insert_outbound_connections(&self, outbound: Vec<ConnectionInfo>) {
        self.0.update(|mut inner| {
            inner.insert_outbound(outbound);
            ((), inner)
        });
    }
}
