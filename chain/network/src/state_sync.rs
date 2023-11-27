use crate::network_protocol::StateResponseInfo;

/// State sync interface for network.
/// The implementation uses try_send to avoid blocking.
pub trait StateSync: Send + Sync + 'static {
    fn send(&self, info: StateResponseInfo);
}

pub struct Noop;
impl StateSync for Noop {
    fn send(&self, _info: StateResponseInfo) {}
}
