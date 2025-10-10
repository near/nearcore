use core::str;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use parking_lot::RwLock;
use serde::Serialize;

use crate::instrumentation::metrics::QUEUE_PENDING_MESSAGES;
use prometheus::core::GenericGauge;

pub struct InstrumentedQueue {
    pending: RwLock<HashMap<String, AtomicU64>>,
    pending_messages_gauge: GenericGauge<prometheus::core::AtomicI64>,
}

impl InstrumentedQueue {
    pub fn new(queue_name: &str) -> Arc<Self> {
        let queue = Arc::new(Self {
            pending: RwLock::new(HashMap::new()),
            pending_messages_gauge: QUEUE_PENDING_MESSAGES.with_label_values(&[queue_name]),
        });
        queue
    }

    pub fn enqueue(&self, message_type: &str) {
        {
            // Try just with read lock first, to avoid write lock in the common case.
            let pending = self.pending.read();
            if let Some(counter) = pending.get(message_type) {
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.pending_messages_gauge.inc();
                return;
            }
        }
        // Use write lock if the message type is not found.
        let mut pending = self.pending.write();
        let counter = pending.entry(message_type.to_string()).or_insert_with(|| AtomicU64::new(0));
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.pending_messages_gauge.inc();
    }

    pub fn dequeue(&self, message_type: &str) {
        let pending = self.pending.read();
        if let Some(counter) = pending.get(message_type) {
            counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            self.pending_messages_gauge.dec();
        }
    }

    pub fn get_pending_counts(&self) -> HashMap<String, u64> {
        let pending = self.pending.read();
        pending
            .iter()
            .map(|(k, v)| (k.clone(), v.load(std::sync::atomic::Ordering::Relaxed)))
            .collect()
    }
}

#[derive(Serialize, Debug)]
pub struct InstrumentedQueueView {
    pub pending_counts: HashMap<String, u64>,
}

impl InstrumentedQueue {
    pub fn to_view(&self) -> InstrumentedQueueView {
        let pending_counts = self.get_pending_counts();
        InstrumentedQueueView { pending_counts }
    }
}
