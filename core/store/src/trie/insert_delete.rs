use super::mem::updating::{
    GenericNodeOrIndex, GenericTrieUpdate, GenericUpdatedTrieNode, UpdatedTrieStorageNodeWithSize,
};
use super::TrieRefcountDeltaMap;
use crate::trie::{
    Children, RawTrieNode, RawTrieNodeWithSize, StorageHandle, StorageValueHandle, ValueHandle,
};
use crate::{StorageError, Trie, TrieChanges};
use borsh::BorshSerialize;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state::ValueRef;

pub(crate) struct NodesStorage<'a> {
    pub(crate) nodes: Vec<Option<UpdatedTrieStorageNodeWithSize>>,
    pub(crate) values: Vec<Option<Vec<u8>>>,
    pub(crate) refcount_changes: TrieRefcountDeltaMap,
    pub(crate) trie: &'a Trie,
}

const INVALID_STORAGE_HANDLE: &str = "invalid storage handle";

/// Local mutable storage that owns node objects.
impl<'a> NodesStorage<'a> {
    pub fn new(trie: &'a Trie) -> NodesStorage<'a> {
        NodesStorage {
            nodes: Vec::new(),
            refcount_changes: TrieRefcountDeltaMap::new(),
            values: Vec::new(),
            trie,
        }
    }

    pub(crate) fn store(&mut self, node: UpdatedTrieStorageNodeWithSize) -> StorageHandle {
        self.nodes.push(Some(node));
        StorageHandle(self.nodes.len() - 1)
    }

    pub(crate) fn value_ref(&self, handle: StorageValueHandle) -> &[u8] {
        self.values
            .get(handle.0)
            .expect(INVALID_STORAGE_HANDLE)
            .as_ref()
            .expect(INVALID_STORAGE_HANDLE)
    }
}

enum FlattenNodesCrumb {
    Entering,
    AtChild(Box<Children>, u8),
    Exiting,
}

impl Trie {
    #[tracing::instrument(level = "debug", target = "store::trie", "Trie::flatten_nodes", skip_all)]
    pub(crate) fn flatten_nodes(
        old_root: &CryptoHash,
        memory: NodesStorage,
        node: usize,
    ) -> Result<TrieChanges, StorageError> {
        let mut stack: Vec<(usize, FlattenNodesCrumb)> = Vec::new();
        stack.push((node, FlattenNodesCrumb::Entering));
        let mut last_hash = CryptoHash::default();
        let mut buffer: Vec<u8> = Vec::new();
        let mut memory = memory;
        'outer: while let Some((node, position)) = stack.pop() {
            let node_with_size = memory.get_node_ref(node);
            let memory_usage = node_with_size.memory_usage;
            let raw_node = match &node_with_size.node {
                GenericUpdatedTrieNode::Empty => {
                    last_hash = Trie::EMPTY_ROOT;
                    continue;
                }
                GenericUpdatedTrieNode::Branch { children, value } => match position {
                    FlattenNodesCrumb::Entering => {
                        stack.push((node, FlattenNodesCrumb::AtChild(Default::default(), 0)));
                        continue;
                    }
                    FlattenNodesCrumb::AtChild(mut new_children, mut i) => {
                        if i > 0 && children[(i - 1) as usize].is_some() {
                            new_children[i - 1] = Some(last_hash);
                        }
                        while i < 16 {
                            match &children[i as usize] {
                                Some(GenericNodeOrIndex::Updated(handle)) => {
                                    stack.push((
                                        node,
                                        FlattenNodesCrumb::AtChild(new_children, i + 1),
                                    ));
                                    stack.push((*handle, FlattenNodesCrumb::Entering));
                                    continue 'outer;
                                }
                                Some(GenericNodeOrIndex::Old(hash)) => {
                                    new_children[i] = Some(*hash);
                                }
                                None => {}
                            }
                            i += 1;
                        }
                        let new_value =
                            (*value).map(|value| Trie::flatten_value(&mut memory, value));
                        RawTrieNode::branch(*new_children, new_value)
                    }
                    FlattenNodesCrumb::Exiting => unreachable!(),
                },
                GenericUpdatedTrieNode::Extension { extension, child } => match position {
                    FlattenNodesCrumb::Entering => match child {
                        GenericNodeOrIndex::Updated(child) => {
                            stack.push((node, FlattenNodesCrumb::Exiting));
                            stack.push((*child, FlattenNodesCrumb::Entering));
                            continue;
                        }
                        GenericNodeOrIndex::Old(hash) => {
                            RawTrieNode::Extension(extension.to_vec(), *hash)
                        }
                    },
                    FlattenNodesCrumb::Exiting => {
                        RawTrieNode::Extension(extension.to_vec(), last_hash)
                    }
                    _ => unreachable!(),
                },
                GenericUpdatedTrieNode::Leaf { extension, value } => {
                    let key = extension.to_vec();
                    let value = *value;
                    let value = Trie::flatten_value(&mut memory, value);
                    RawTrieNode::Leaf(key, value)
                }
            };
            let raw_node_with_size = RawTrieNodeWithSize { node: raw_node, memory_usage };
            raw_node_with_size.serialize(&mut buffer).unwrap();
            let key = hash(&buffer);

            memory.refcount_changes.add(key, buffer.clone(), 1);
            buffer.clear();
            last_hash = key;
        }
        let (insertions, deletions) = memory.refcount_changes.into_changes();
        Ok(TrieChanges {
            old_root: *old_root,
            new_root: last_hash,
            insertions,
            deletions,
            mem_trie_changes: None,
        })
    }

    fn flatten_value(memory: &mut NodesStorage, value: ValueHandle) -> ValueRef {
        match value {
            ValueHandle::InMemory(value_handle) => {
                let value = memory.value_ref(value_handle).to_vec();
                let value_length = value.len() as u32;
                let value_hash = hash(&value);
                memory.refcount_changes.add(value_hash, value, 1);
                ValueRef { length: value_length, hash: value_hash }
            }
            ValueHandle::HashAndSize(value) => value,
        }
    }
}
