use super::Receipt;
use super::ShardId;
use std::collections::VecDeque;

pub struct Queue {
    shard: ShardId,
    messages: VecDeque<Receipt>,
}

impl Queue {
    pub fn new(shard: ShardId) -> Self {
        Self { shard, messages: VecDeque::new() }
    }

    pub fn size(&self) -> u64 {
        self.messages.iter().map(|receipt| receipt.size).sum()
    }

    pub fn shard(&self) -> ShardId {
        self.shard
    }
}

impl std::ops::Deref for Queue {
    type Target = VecDeque<Receipt>;

    fn deref(&self) -> &Self::Target {
        &self.messages
    }
}

impl std::ops::DerefMut for Queue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.messages
    }
}
