use crate::types::ConnectionInfo;

impl super::ConnectionStore {
    pub(crate) fn insert_outbound_connections(&self, outbound: Vec<ConnectionInfo>) {
        self.0.update(|mut inner| {
            inner.insert_outbound(outbound);
            ((), inner)
        });
    }
}
