use core::str;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use parking_lot::RwLock;

/// InstrumentedQueue keeps track of the number of pending messages of each type in the queue.
pub struct InstrumentedQueue {
    pending: RwLock<HashMap<String, AtomicU64>>,
}

impl InstrumentedQueue {
    pub fn new(_queue_name: &str) -> Arc<Self> {
        let queue = Arc::new(Self { pending: RwLock::new(HashMap::new()) });
        queue
    }

    pub fn enqueue(&self, message_type: &str) {
        {
            // Try just with read lock first, to avoid write lock in the common case.
            let pending = self.pending.read();
            if let Some(counter) = pending.get(message_type) {
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return;
            }
        }
        // Use write lock if the message type is not found.
        let mut pending = self.pending.write();
        let counter = pending.entry(message_type.to_string()).or_insert_with(|| AtomicU64::new(0));
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn dequeue(&self, message_type: &str) {
        let pending = self.pending.read();
        if let Some(counter) = pending.get(message_type) {
            counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn get_pending_events(&self) -> HashMap<String, u64> {
        let pending = self.pending.read();
        pending
            .iter()
            .map(|(k, v)| (k.clone(), v.load(std::sync::atomic::Ordering::Relaxed)))
            .collect()
    }
}
