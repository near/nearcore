use core::str;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock, Weak};

use parking_lot::RwLock;
use serde::Serialize;

pub struct AllQueues {
    queues: RwLock<HashMap<String, Weak<InstrumentedQueue>>>,
}

pub static ALL_QUEUES: LazyLock<AllQueues> =
    LazyLock::new(|| AllQueues { queues: RwLock::new(HashMap::new()) });

pub struct InstrumentedQueue {
    pending: RwLock<HashMap<String, AtomicU64>>,
}

impl InstrumentedQueue {
    pub fn register_new(queue_name: &str) -> Arc<InstrumentedQueue> {
        let mut queues = ALL_QUEUES.queues.write();
        if let Some(queue) = queues.get(queue_name).and_then(|w| w.upgrade()) {
            return queue;
        }
        let queue = Arc::new(InstrumentedQueue { pending: RwLock::new(HashMap::new()) });
        queues.insert(queue_name.to_string(), Arc::downgrade(&queue));
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

    pub fn get_pending_counts(&self) -> HashMap<String, u64> {
        let pending = self.pending.read();
        pending
            .iter()
            .map(|(k, v)| (k.clone(), v.load(std::sync::atomic::Ordering::Relaxed)))
            .collect()
    }
}

#[derive(Serialize, Debug)]
pub struct AllQueuesView {
    pub queues: HashMap<String, InstrumentedQueueView>,
}

impl AllQueues {
    pub fn to_view(&self) -> AllQueuesView {
        let queues = self.queues.read();
        let mut result = HashMap::new();
        for (name, weak_queue) in queues.iter() {
            if let Some(queue) = weak_queue.upgrade() {
                result.insert(name.clone(), queue.to_view());
            }
        }
        AllQueuesView { queues: result }
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
