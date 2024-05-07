use crate::strategy::QueueFactory;
use crate::{Queue, ShardId, TransactionId};
use std::collections::{HashMap, VecDeque};

/// A bag of all queues in the system, bundled in a single struct.
pub struct QueueBundle {
    receipt_queues: Vec<Queue>,
    transaction_queues: HashMap<ShardId, VecDeque<TransactionId>>,

    /// Maps shards to their implicit incoming receipts queue.
    shard_mailbox: HashMap<ShardId, QueueId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueueId(usize);

impl QueueBundle {
    pub fn new(shards: &[ShardId]) -> Self {
        let mut this = Self {
            receipt_queues: Default::default(),
            transaction_queues: Default::default(),
            shard_mailbox: Default::default(),
        };

        for &shard in shards {
            let mailbox = this.new_queue(shard, "mailbox");
            this.shard_mailbox.insert(shard, mailbox);
            this.transaction_queues.insert(shard, VecDeque::new());
        }

        this
    }

    pub fn new_queue(&mut self, shard_id: ShardId, name: &str) -> QueueId {
        let id = self.receipt_queues.len();
        self.receipt_queues.push(Queue::new(shard_id, name));
        QueueId(id)
    }

    pub fn queue(&self, id: QueueId) -> &Queue {
        &self.receipt_queues[id.0]
    }

    pub fn queue_mut(&mut self, id: QueueId) -> &mut Queue {
        &mut self.receipt_queues[id.0]
    }

    pub fn incoming_receipts(&self, shard_id: ShardId) -> &Queue {
        self.queue(self.shard_mailbox[&shard_id])
    }

    pub fn incoming_receipts_mut(&mut self, shard_id: ShardId) -> &mut Queue {
        self.queue_mut(self.shard_mailbox[&shard_id])
    }

    pub fn incoming_transactions(&self, shard_id: ShardId) -> &VecDeque<TransactionId> {
        self.transaction_queues
            .get(&shard_id)
            .expect("transaction queue should exist for all shards")
    }

    pub fn incoming_transactions_mut(&mut self, shard_id: ShardId) -> &mut VecDeque<TransactionId> {
        self.transaction_queues
            .get_mut(&shard_id)
            .expect("transaction queue should exist for all shards")
    }

    pub fn shard_queues(&self, shard_id: ShardId) -> impl Iterator<Item = &Queue> {
        self.receipt_queues.iter().filter(move |q| q.shard() == shard_id)
    }
}

impl QueueFactory for QueueBundle {
    fn register_queue(&mut self, shard_id: ShardId, name: &str) -> QueueId {
        self.new_queue(shard_id, name)
    }
}
