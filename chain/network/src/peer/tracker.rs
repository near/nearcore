use crate::peer::transfer_stats::TransferStats;
use near_async::time;
use near_primitives::hash::CryptoHash;

/// Maximum number of requests and responses to track.
const MAX_TRACK_SIZE: usize = 30;

/// Internal structure to keep a circular queue within a tracker with unique hashes.
struct CircularUniqueQueue {
    v: Vec<CryptoHash>,
    index: usize,
    limit: usize,
}

impl CircularUniqueQueue {
    fn new(limit: usize) -> Self {
        assert!(limit > 0);
        Self { v: Vec::with_capacity(limit), index: 0, limit }
    }

    fn contains(&self, hash: &CryptoHash) -> bool {
        self.v.contains(hash)
    }

    /// Pushes an element if it's not in the queue already. The queue will pop the oldest element.
    fn push(&mut self, hash: CryptoHash) {
        if !self.contains(&hash) {
            if self.v.len() < self.limit {
                self.v.push(hash);
            } else {
                self.v[self.index] = hash;
                self.index += 1;
                if self.index == self.limit {
                    self.index = 0;
                }
            }
        }
    }
}

/// Keeps track of requests and received hashes of transactions and blocks.
/// Also keeps track of number of bytes sent and received from this peer to prevent abuse.
pub(crate) struct Tracker {
    /// Bytes we've sent.
    pub(crate) sent_bytes: TransferStats,
    /// Bytes we've received.
    pub(crate) received_bytes: TransferStats,
    /// Sent requests.
    requested: CircularUniqueQueue,
    /// Received elements.
    received: CircularUniqueQueue,
}

impl Default for Tracker {
    fn default() -> Self {
        Tracker {
            sent_bytes: TransferStats::default(),
            received_bytes: TransferStats::default(),
            requested: CircularUniqueQueue::new(MAX_TRACK_SIZE),
            received: CircularUniqueQueue::new(MAX_TRACK_SIZE),
        }
    }
}

impl Tracker {
    pub(crate) fn increment_received(&mut self, clock: &time::Clock, size: u64) {
        self.received_bytes.record(clock, size);
    }

    pub(crate) fn increment_sent(&mut self, clock: &time::Clock, size: u64) {
        self.sent_bytes.record(clock, size);
    }

    // TODO: uncomment this once we add a new message type to sync block height
    /*
    pub(crate) fn has_received(&self, hash: &CryptoHash) -> bool {
        self.received.contains(hash)
    }
     */

    pub(crate) fn push_received(&mut self, hash: CryptoHash) {
        self.received.push(hash);
    }

    pub(crate) fn has_request(&self, hash: &CryptoHash) -> bool {
        self.requested.contains(hash)
    }

    pub(crate) fn push_request(&mut self, hash: CryptoHash) {
        self.requested.push(hash);
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::hash;

    use super::*;

    #[test]
    #[should_panic]
    fn test_circular_queue_zero_capacity() {
        let _ = CircularUniqueQueue::new(0);
    }

    #[test]
    fn test_circular_queue_empty_queue() {
        let q = CircularUniqueQueue::new(5);

        assert!(!q.contains(&hash(&[0])));
    }

    #[test]
    fn test_circular_queue_partially_full_queue() {
        let mut q = CircularUniqueQueue::new(5);
        for i in 1..=3 {
            q.push(hash(&[i]));
        }

        for i in 1..=3 {
            assert!(q.contains(&hash(&[i])));
        }
    }

    #[test]
    fn test_circular_queue_full_queue() {
        let mut q = CircularUniqueQueue::new(5);
        for i in 1..=5 {
            q.push(hash(&[i]));
        }

        for i in 1..=5 {
            assert!(q.contains(&hash(&[i])));
        }
    }

    #[test]
    fn test_circular_queue_over_full_queue() {
        let mut q = CircularUniqueQueue::new(5);
        for i in 1..=7 {
            q.push(hash(&[i]));
        }

        for i in 1..=2 {
            assert!(!q.contains(&hash(&[i])));
        }
        for i in 3..=7 {
            assert!(q.contains(&hash(&[i])));
        }
    }

    #[test]
    fn test_circular_queue_similar_inputs() {
        let mut q = CircularUniqueQueue::new(5);
        q.push(hash(&[5]));
        for _ in 0..3 {
            for i in 1..=3 {
                for _ in 0..5 {
                    q.push(hash(&[i]));
                }
            }
        }
        for i in 1..=3 {
            assert!(q.contains(&hash(&[i])));
        }
        assert!(q.contains(&hash(&[5])));
    }
}
