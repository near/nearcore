use crate::{GGas, Model, Queue, ShardId};
use std::collections::HashMap;
use std::iter::Sum;

#[derive(Debug, Clone, Default)]
pub struct ShardQueueLengths {
    /// Number of transactions waiting and not processed.
    pub unprocessed_incoming_transactions: u64,
    /// Receipts in the mailbox.
    pub incoming_receipts: QueueStats,
    /// Receipts in all internal queues plus the mailbox.
    pub queued_receipts: QueueStats,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct QueueStats {
    /// Number of messages.
    pub num: u64,
    /// Sum of messages sizes in bytes.
    pub size: u64,
    /// Sum of attached gas for all messages in the queue.
    pub gas: GGas,
}

impl Model {
    /// Current queue lengths per shard.
    pub fn queue_lengths(&self) -> HashMap<ShardId, ShardQueueLengths> {
        let mut out = HashMap::new();
        for shard in self.shard_ids.clone() {
            let unprocessed_incoming_transactions =
                self.queues.incoming_transactions(shard).len() as u64;
            let incoming_receipts = self.queues.incoming_receipts(shard).stats();
            let total_shard_receipts: QueueStats =
                self.queues.shard_queues(shard).map(|q| q.stats()).sum();

            let shard_stats = ShardQueueLengths {
                unprocessed_incoming_transactions,
                incoming_receipts,
                queued_receipts: total_shard_receipts,
            };
            out.insert(shard, shard_stats);
        }
        out
    }

    /// Current max queue length stats.
    pub fn max_queue_length(&self) -> ShardQueueLengths {
        let mut out = ShardQueueLengths::default();
        for q in self.queue_lengths().values() {
            out = out.max_component_wise(q);
        }
        out
    }
}

impl Queue {
    pub fn stats(&self) -> QueueStats {
        QueueStats { num: self.len() as u64, size: self.size(), gas: self.attached_gas() }
    }
}

impl ShardQueueLengths {
    pub fn max_component_wise(&self, rhs: &Self) -> Self {
        Self {
            unprocessed_incoming_transactions: self
                .unprocessed_incoming_transactions
                .max(rhs.unprocessed_incoming_transactions),
            incoming_receipts: self.incoming_receipts.max_component_wise(rhs.incoming_receipts),
            queued_receipts: self.queued_receipts.max_component_wise(rhs.queued_receipts),
        }
    }
}

impl QueueStats {
    pub fn max_component_wise(&self, rhs: Self) -> Self {
        Self {
            num: self.num.max(rhs.num),
            size: self.size.max(rhs.size),
            gas: self.gas.max(rhs.gas),
        }
    }
}

impl std::ops::Add for QueueStats {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self { num: self.num + rhs.num, size: self.size + rhs.size, gas: self.gas + rhs.gas }
    }
}

impl std::ops::Sub for QueueStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self { num: self.num - rhs.num, size: self.size - rhs.size, gas: self.gas - rhs.gas }
    }
}

impl Sum for QueueStats {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.reduce(std::ops::Add::add).unwrap_or_default()
    }
}
