use super::Receipt;
use super::ShardId;
use std::collections::VecDeque;

pub struct Queue {
    shard: ShardId,
    name: String,
    messages: VecDeque<Receipt>,
}

impl Queue {
    pub fn new(shard: ShardId, name: &str) -> Self {
        Self { shard, name: name.to_string(), messages: VecDeque::new() }
    }

    pub fn size(&self) -> u64 {
        self.messages.iter().map(|receipt| receipt.size).sum()
    }

    pub fn attached_gas(&self) -> u64 {
        self.messages.iter().map(|receipt| receipt.attached_gas).sum()
    }

    pub fn shard(&self) -> ShardId {
        self.shard
    }

    pub fn name(&self) -> &String {
        &self.name
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
