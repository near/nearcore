use near_async::time;
use near_network::types::PeerMessage;
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use std::sync::Arc;

/// Filter applied to messages between two specific peers. Returns
/// `Some(msg)` to continue delivery (possibly modified), `None` to
/// drop. Filters chain in registration order; first `None` wins.
pub type TransportMessageFilter =
    Arc<dyn Fn(&PeerId, &PeerId, &PeerMessage) -> Option<PeerMessage> + Send + Sync>;

/// Predicate for an additional delay applied to matching messages.
pub type DelayPredicate = Arc<dyn Fn(&PeerId, &PeerId, &PeerMessage) -> bool + Send + Sync>;

/// Test-controlled hooks: filters and delays. Cloneable so `TestLoopEnv`
/// can hand it to the builder, the registry, and to test code.
#[derive(Clone, Default)]
#[allow(dead_code)]
pub struct TestLoopNetworkSharedStateV2 {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    filters: Mutex<Vec<TransportMessageFilter>>,
    delays: Mutex<Vec<(DelayPredicate, time::Duration)>>,
}

#[allow(dead_code)]
impl TestLoopNetworkSharedStateV2 {
    pub fn register_message_filter(&self, f: TransportMessageFilter) {
        self.inner.filters.lock().push(f);
    }

    pub fn add_message_delay(&self, predicate: DelayPredicate, duration: time::Duration) {
        self.inner.delays.lock().push((predicate, duration));
    }

    /// Apply all filters in order. Short-circuit on first `None`.
    /// Lock is released before the (owned) message is returned.
    pub(super) fn apply_filters(
        &self,
        from: &PeerId,
        to: &PeerId,
        msg: &PeerMessage,
    ) -> Option<PeerMessage> {
        let filters = self.inner.filters.lock();
        let mut current = msg.clone();
        for f in filters.iter() {
            current = f(from, to, &current)?;
        }
        Some(current)
    }

    /// Maximum delay across matching predicates. Base NETWORK_DELAY is
    /// added by the caller.
    pub(super) fn compute_extra_delay(
        &self,
        from: &PeerId,
        to: &PeerId,
        msg: &PeerMessage,
    ) -> time::Duration {
        let delays = self.inner.delays.lock();
        delays
            .iter()
            .filter(|(pred, _)| pred(from, to, msg))
            .map(|(_, d)| *d)
            .max()
            .unwrap_or(time::Duration::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_crypto::{KeyType, SecretKey};
    use near_network::types::{Disconnect, PeerMessage};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn peer_id(seed: &str) -> PeerId {
        PeerId::new(SecretKey::from_seed(KeyType::ED25519, seed).public_key())
    }

    fn dummy_msg() -> PeerMessage {
        PeerMessage::Disconnect(Disconnect { remove_from_connection_store: false })
    }

    #[test]
    fn filters_chain_in_order_and_short_circuit_on_none() {
        let shared = TestLoopNetworkSharedStateV2::default();
        let order: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));

        let order_a = order.clone();
        shared.register_message_filter(Arc::new(move |_, _, m| {
            order_a.lock().push(1);
            Some(m.clone())
        }));
        let order_b = order.clone();
        shared.register_message_filter(Arc::new(move |_, _, _| {
            order_b.lock().push(2);
            None
        }));
        // Should never be called: filter 2 returns None.
        let order_c = order.clone();
        shared.register_message_filter(Arc::new(move |_, _, m| {
            order_c.lock().push(3);
            Some(m.clone())
        }));

        let result = shared.apply_filters(&peer_id("a"), &peer_id("b"), &dummy_msg());
        assert!(result.is_none());
        assert_eq!(*order.lock(), vec![1, 2]);
    }

    #[test]
    fn filter_modifies_message_passed_to_next_filter() {
        let shared = TestLoopNetworkSharedStateV2::default();
        let observed = Arc::new(AtomicUsize::new(0));

        // First filter swaps Disconnect's flag.
        shared.register_message_filter(Arc::new(|_, _, m| match m {
            PeerMessage::Disconnect(d) => {
                Some(PeerMessage::Disconnect(near_network::types::Disconnect {
                    remove_from_connection_store: !d.remove_from_connection_store,
                }))
            }
            other => Some(other.clone()),
        }));

        let observed_clone = observed.clone();
        shared.register_message_filter(Arc::new(move |_, _, m| {
            if let PeerMessage::Disconnect(d) = m {
                if d.remove_from_connection_store {
                    observed_clone.fetch_add(1, Ordering::SeqCst);
                }
            }
            Some(m.clone())
        }));

        let _ = shared.apply_filters(&peer_id("a"), &peer_id("b"), &dummy_msg());
        assert_eq!(observed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn delays_take_max_not_sum() {
        let shared = TestLoopNetworkSharedStateV2::default();
        shared.add_message_delay(Arc::new(|_, _, _| true), time::Duration::milliseconds(100));
        shared.add_message_delay(Arc::new(|_, _, _| true), time::Duration::milliseconds(50));
        shared.add_message_delay(Arc::new(|_, _, _| true), time::Duration::milliseconds(200));
        let d = shared.compute_extra_delay(&peer_id("a"), &peer_id("b"), &dummy_msg());
        assert_eq!(d, time::Duration::milliseconds(200));
    }

    #[test]
    fn no_matching_predicate_returns_zero() {
        let shared = TestLoopNetworkSharedStateV2::default();
        shared.add_message_delay(Arc::new(|_, _, _| false), time::Duration::milliseconds(100));
        let d = shared.compute_extra_delay(&peer_id("a"), &peer_id("b"), &dummy_msg());
        assert_eq!(d, time::Duration::ZERO);
    }
}
