use slab::Slab;

use near_primitives::hash::CryptoHash;

use crate::trie::iterator::TrieItem;
use crate::trie::{TrieNode, TrieNodeWithSize, ValueHandle};
use crate::{StorageError, Trie, TrieIterator};

pub enum NodeOrValue {
    Value(Option<Vec<u8>>),
    Node(Option<TrieNodeWithSize>),
}

impl NodeOrValue {
    fn is_some(&self) -> bool {
        match self {
            NodeOrValue::Value(Some(_)) => true,
            NodeOrValue::Node(Some(_)) => true,
            _ => false,
        }
    }

    fn is_none(&self) -> bool {
        !self.is_some()
    }

    fn is_value(&self) -> bool {
        match self {
            NodeOrValue::Value(_) => true,
            _ => false,
        }
    }

    fn is_node(&self) -> bool {
        match self {
            NodeOrValue::Node(_) => true,
            _ => false,
        }
    }

    fn none_node() -> Self {
        Self::Node(None)
    }

    fn none_value() -> Self {
        Self::Value(None)
    }
}

struct QueueItem {
    hash: CryptoHash,
    value: NodeOrValue,
    next: usize,
    prev: usize,
}

impl QueueItem {
    fn value(hash: CryptoHash) -> Self {
        Self { hash, value: NodeOrValue::none_value(), next: 0, prev: 0 }
    }

    fn node(hash: CryptoHash) -> Self {
        Self { hash, value: NodeOrValue::none_node(), next: 0, prev: 0 }
    }
}

pub struct TrieBatchingIterator<'a> {
    queue: IteratorQueue,
    trie: &'a Trie,
    iterator: TrieIterator<'a>,
    batch_size: usize,
}

impl<'a> TrieBatchingIterator<'a> {
    pub fn new(trie: &'a Trie, root: &CryptoHash) -> Result<Self, StorageError> {
        Self::with_batch_size(trie, root, 100)
    }

    pub fn with_batch_size(
        trie: &'a Trie,
        root: &CryptoHash,
        batch_size: usize,
    ) -> Result<Self, StorageError> {
        let batch_size = std::cmp::max(batch_size, 1);
        let batch_size = std::cmp::min(batch_size, 10000);
        let mut queue = IteratorQueue::new();
        queue.push_back(QueueItem::node(*root));
        let iterator = TrieIterator::new(trie, root)?;
        Ok(Self { queue, iterator, batch_size, trie })
    }

    fn process_step(&mut self) -> Result<(), StorageError> {
        let queue = &mut self.queue;
        let mut items = Vec::new();
        let mut current = queue.next(queue.head);
        if queue.get(current).value.is_some() {
            return Ok(());
        }
        for _ in 0..self.batch_size {
            if current == queue.tail {
                break;
            }
            let item = queue.get(current);
            if item.value.is_none() {
                items.push(current);
            }
            current = item.next;
        }
        let queries = items.iter().map(|index| queue.get(*index).hash).collect::<Vec<_>>();
        self.trie.storage.prefetch(&queries)?;
        for index in items {
            let mut trie_node = None;
            {
                let item = queue.get_mut(index);
                let hash = item.hash;
                let value = &mut item.value;
                assert!(value.is_none());
                match value {
                    NodeOrValue::Value(None) => {
                        let bytes = self.trie.storage.retrieve_raw_bytes(&hash)?;
                        *value = NodeOrValue::Value(Some(bytes));
                    }
                    NodeOrValue::Node(None) => {
                        let node = self.trie.retrieve_node(&hash)?;
                        trie_node = Some(node.node.clone());
                        *value = NodeOrValue::Node(Some(node));
                    }
                    _ => {}
                }
            }
            if let Some(trie_node) = trie_node {
                match trie_node {
                    TrieNode::Empty => {}
                    TrieNode::Leaf(_, val) => {
                        let hash = match val {
                            ValueHandle::InMemory(_) => unreachable!(),
                            ValueHandle::HashAndSize(_, hash) => hash,
                        };
                        queue.insert_after(index, QueueItem::value(hash));
                    }
                    TrieNode::Branch(children, value) => {
                        let mut index = index;
                        if let Some(val) = value {
                            let hash = match val {
                                ValueHandle::InMemory(_) => unreachable!(),
                                ValueHandle::HashAndSize(_, hash) => hash,
                            };
                            index = queue.insert_after(index, QueueItem::value(hash));
                        }
                        for i in 0..16 {
                            if let Some(handle) = &children[i] {
                                let hash = *handle.unwrap_hash();
                                index = queue.insert_after(index, QueueItem::node(hash));
                            }
                        }
                    }
                    TrieNode::Extension(_, handle) => {
                        let hash = *handle.unwrap_hash();
                        queue.insert_after(index, QueueItem::node(hash));
                    }
                }
                queue.delete(index);
            }
        }
        Ok(())
    }
}

impl<'a> Iterator for TrieBatchingIterator<'a> {
    type Item = TrieItem;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.queue.is_empty() {
            let need_step = self.queue.get(self.queue.next(self.queue.head)).value.is_none();
            if need_step {
                if let Err(e) = self.process_step() {
                    return Some(Err(e));
                }
            } else {
                let item = match &self.queue.get(self.queue.front()).value {
                    NodeOrValue::Value(Some(val)) => Some(val.clone()),
                    _ => None,
                };
                self.queue.pop_front();
                if let Some(_value1) = item {
                    let item = self.iterator.next();
                    if let Some(item) = item {
                        return Some(item);
                    } else {
                        break;
                    }
                }
            }
        }
        None
    }
}

struct IteratorQueue {
    items: Slab<QueueItem>,
    head: usize,
    tail: usize,
}

type QueueItemRef = usize;

impl IteratorQueue {
    fn new() -> Self {
        let mut items = Slab::new();
        let head = items.insert(QueueItem::value(CryptoHash::default()));
        let tail = items.insert(QueueItem::value(CryptoHash::default()));
        items.get_mut(head).unwrap().next = tail;
        items.get_mut(tail).unwrap().prev = head;
        Self { items, head, tail }
    }

    fn delete(&mut self, index: QueueItemRef) {
        let item = self.items.remove(index);
        let next = item.next;
        let prev = item.prev;
        self.items.get_mut(next).unwrap().prev = prev;
        self.items.get_mut(prev).unwrap().next = next;
    }

    fn get(&self, index: QueueItemRef) -> &QueueItem {
        self.items.get(index).unwrap()
    }

    fn get_mut(&mut self, index: QueueItemRef) -> &mut QueueItem {
        self.items.get_mut(index).unwrap()
    }

    fn next(&self, index: QueueItemRef) -> QueueItemRef {
        self.items.get(index).unwrap().next
    }

    fn prev(&self, index: QueueItemRef) -> QueueItemRef {
        self.items.get(index).unwrap().prev
    }

    fn insert_after(&mut self, prev: QueueItemRef, item: QueueItem) -> QueueItemRef {
        let index = self.items.insert(item);
        let next = self.get(prev).next;
        self.items.get_mut(prev).unwrap().next = index;
        self.items.get_mut(next).unwrap().prev = index;
        let item = self.items.get_mut(index).unwrap();
        item.next = next;
        item.prev = prev;
        index
    }

    fn pop_front(&mut self) {
        let index = self.next(self.head);
        if index != self.tail {
            self.delete(index)
        }
    }

    fn front(&self) -> QueueItemRef {
        self.next(self.head)
    }

    fn push_back(&mut self, item: QueueItem) -> QueueItemRef {
        let prev = self.items.get(self.tail).unwrap().prev;
        self.insert_after(prev, item)
    }

    fn is_empty(&self) -> bool {
        self.items.get(self.head).unwrap().next == self.tail
    }
}

impl Trie {
    pub fn for_each_1<F>(&self, root_hash: &CryptoHash, mut f: F) -> Result<(), StorageError>
    where
        F: FnMut((Vec<u8>, Vec<u8>)),
    {
        let iterator = self.iter(root_hash)?;
        for item in iterator {
            f(item?);
        }
        Ok(())
    }

    pub fn for_each_2<F>(&self, root_hash: &CryptoHash, mut f: F) -> Result<(), StorageError>
    where
        F: FnMut((Vec<u8>, Vec<u8>)),
    {
        let iterator = TrieBatchingIterator::new(self, root_hash)?;
        for item in iterator {
            f(item?);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::test_utils::{create_tries, gen_changes, simplify_changes, test_populate_trie};
    use crate::Trie;

    #[test]
    fn test_for_each() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let tries = create_tries();
            let trie = tries.get_trie_for_shard(0);
            let trie_changes = gen_changes(&mut rng, 100);
            let trie_changes = simplify_changes(&trie_changes);

            let mut map = BTreeMap::new();
            for (key, value) in trie_changes.iter() {
                if let Some(value) = value {
                    map.insert(key.clone(), value.clone());
                }
            }
            let state_root =
                test_populate_trie(&tries, &Trie::empty_root(), 0, trie_changes.clone());

            let mut keyvalues1 = Vec::new();
            trie.for_each_2(&state_root, |(key, value)| {
                keyvalues1.push((key, value));
            })
            .unwrap();
            let keyvalues2 = map.into_iter().collect::<Vec<_>>();

            assert_eq!(keyvalues1, keyvalues2);
        }
    }
}
