use std::collections::HashMap;

use borsh::BorshSerialize;

use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state::ValueRef;

use crate::trie::nibble_slice::NibbleSlice;
use crate::trie::{
    Children, NodeHandle, RawTrieNode, RawTrieNodeWithSize, StorageHandle, StorageValueHandle,
    TrieNode, TrieNodeWithSize, ValueHandle,
};
use crate::{StorageError, Trie, TrieChanges};

pub(crate) struct NodesStorage {
    nodes: Vec<Option<TrieNodeWithSize>>,
    values: Vec<Option<Vec<u8>>>,
    pub(crate) refcount_changes: HashMap<CryptoHash, (Vec<u8>, i32)>,
}

const INVALID_STORAGE_HANDLE: &str = "invalid storage handle";

/// Local mutable storage that owns node objects.
impl NodesStorage {
    pub fn new() -> NodesStorage {
        NodesStorage { nodes: Vec::new(), refcount_changes: HashMap::new(), values: Vec::new() }
    }

    fn destroy(&mut self, handle: StorageHandle) -> TrieNodeWithSize {
        self.nodes
            .get_mut(handle.0)
            .expect(INVALID_STORAGE_HANDLE)
            .take()
            .expect(INVALID_STORAGE_HANDLE)
    }

    pub fn node_ref(&self, handle: StorageHandle) -> &TrieNodeWithSize {
        self.nodes
            .get(handle.0)
            .expect(INVALID_STORAGE_HANDLE)
            .as_ref()
            .expect(INVALID_STORAGE_HANDLE)
    }

    fn node_mut(&mut self, handle: StorageHandle) -> &mut TrieNodeWithSize {
        self.nodes
            .get_mut(handle.0)
            .expect(INVALID_STORAGE_HANDLE)
            .as_mut()
            .expect(INVALID_STORAGE_HANDLE)
    }

    pub(crate) fn store(&mut self, node: TrieNodeWithSize) -> StorageHandle {
        self.nodes.push(Some(node));
        StorageHandle(self.nodes.len() - 1)
    }

    pub(crate) fn store_value(&mut self, value: Vec<u8>) -> StorageValueHandle {
        self.values.push(Some(value));
        StorageValueHandle(self.values.len() - 1)
    }

    pub(crate) fn value_ref(&self, handle: StorageValueHandle) -> &[u8] {
        self.values
            .get(handle.0)
            .expect(INVALID_STORAGE_HANDLE)
            .as_ref()
            .expect(INVALID_STORAGE_HANDLE)
    }

    fn store_at(&mut self, handle: StorageHandle, node: TrieNodeWithSize) {
        debug_assert!(self.nodes.get(handle.0).expect(INVALID_STORAGE_HANDLE).is_none());
        self.nodes[handle.0] = Some(node);
    }
}

enum FlattenNodesCrumb {
    Entering,
    AtChild(Box<Children>, u8),
    Exiting,
}

impl Trie {
    /// Allowed to mutate nodes in NodesStorage.
    /// Insert while holding StorageHandles to NodesStorage is unsafe
    pub(crate) fn insert(
        &self,
        memory: &mut NodesStorage,
        node: StorageHandle,
        partial: NibbleSlice<'_>,
        value: Vec<u8>,
    ) -> Result<StorageHandle, StorageError> {
        let root_handle = node;
        let mut handle = node;
        let mut value = Some(value);
        let mut partial = partial;
        let mut path = Vec::new();
        loop {
            path.push(handle);
            let TrieNodeWithSize { node, memory_usage } = memory.destroy(handle);
            let children_memory_usage = memory_usage - node.memory_usage_direct(memory);
            match node {
                TrieNode::Empty => {
                    let value_handle = memory.store_value(value.take().unwrap());
                    let leaf_node = TrieNode::Leaf(
                        partial.encoded(true).into_vec(),
                        ValueHandle::InMemory(value_handle),
                    );
                    let memory_usage = leaf_node.memory_usage_direct(memory);
                    memory.store_at(handle, TrieNodeWithSize { node: leaf_node, memory_usage });
                    break;
                }
                TrieNode::Branch(mut children, existing_value) => {
                    // If the key ends here, store the value in branch's value.
                    if partial.is_empty() {
                        if let Some(value) = &existing_value {
                            self.delete_value(memory, value)?;
                        }
                        let value_handle = memory.store_value(value.take().unwrap());
                        let new_node =
                            TrieNode::Branch(children, Some(ValueHandle::InMemory(value_handle)));
                        let new_memory_usage =
                            children_memory_usage + new_node.memory_usage_direct(memory);
                        memory.store_at(handle, TrieNodeWithSize::new(new_node, new_memory_usage));
                        break;
                    } else {
                        let child = &mut children[partial.at(0)];
                        let new_handle = match child.take() {
                            Some(NodeHandle::Hash(hash)) => {
                                self.move_node_to_mutable(memory, &hash)?
                            }
                            Some(NodeHandle::InMemory(handle)) => handle,
                            None => memory.store(TrieNodeWithSize::empty()),
                        };
                        *child = Some(NodeHandle::InMemory(new_handle));
                        Trie::calc_memory_usage_and_store(
                            memory,
                            handle,
                            children_memory_usage,
                            TrieNode::Branch(children, existing_value),
                            Some(new_handle),
                        );
                        handle = new_handle;
                        partial = partial.mid(1);
                        continue;
                    }
                }
                TrieNode::Leaf(key, existing_value) => {
                    let existing_key = NibbleSlice::from_encoded(&key).0;
                    let common_prefix = partial.common_prefix(&existing_key);
                    if common_prefix == existing_key.len() && common_prefix == partial.len() {
                        // Equivalent leaf.
                        self.delete_value(memory, &existing_value)?;
                        let value_handle = memory.store_value(value.take().unwrap());
                        let node = TrieNode::Leaf(key, ValueHandle::InMemory(value_handle));
                        let memory_usage = node.memory_usage_direct(memory);
                        memory.store_at(handle, TrieNodeWithSize { node, memory_usage });
                        break;
                    } else if common_prefix == 0 {
                        let mut children = Default::default();
                        let children_memory_usage;
                        let branch_node = if existing_key.is_empty() {
                            children_memory_usage = 0;
                            TrieNode::Branch(children, Some(existing_value))
                        } else {
                            let idx = existing_key.at(0);
                            let new_leaf = TrieNode::Leaf(
                                existing_key.mid(1).encoded(true).into_vec(),
                                existing_value,
                            );
                            let memory_usage = new_leaf.memory_usage_direct(memory);
                            children_memory_usage = memory_usage;
                            let handle =
                                memory.store(TrieNodeWithSize { node: new_leaf, memory_usage });
                            children[idx] = Some(NodeHandle::InMemory(handle));
                            TrieNode::Branch(children, None)
                        };
                        let memory_usage =
                            branch_node.memory_usage_direct(memory) + children_memory_usage;
                        memory
                            .store_at(handle, TrieNodeWithSize { node: branch_node, memory_usage });
                        path.pop();
                        continue;
                    } else if common_prefix == existing_key.len() {
                        let branch_node =
                            TrieNode::Branch(Default::default(), Some(existing_value));
                        let memory_usage = branch_node.memory_usage_direct(memory);
                        let child =
                            memory.store(TrieNodeWithSize { node: branch_node, memory_usage });
                        let new_node = TrieNode::Extension(
                            existing_key.encoded(false).into_vec(),
                            NodeHandle::InMemory(child),
                        );
                        let memory_usage = new_node.memory_usage_direct(memory);
                        memory.store_at(handle, TrieNodeWithSize { node: new_node, memory_usage });
                        handle = child;
                        partial = partial.mid(common_prefix);
                        continue;
                    } else {
                        // Partially shared prefix: convert to leaf and call recursively to add a branch.
                        let leaf_node = TrieNode::Leaf(
                            existing_key.mid(common_prefix).encoded(true).into_vec(),
                            existing_value,
                        );
                        let leaf_memory_usage = leaf_node.memory_usage_direct(memory);
                        let child =
                            memory.store(TrieNodeWithSize::new(leaf_node, leaf_memory_usage));
                        let node = TrieNode::Extension(
                            partial.encoded_leftmost(common_prefix, false).into_vec(),
                            NodeHandle::InMemory(child),
                        );
                        let mem = node.memory_usage_direct(memory);
                        memory.store_at(handle, TrieNodeWithSize::new(node, mem));
                        handle = child;
                        partial = partial.mid(common_prefix);
                        continue;
                    }
                }
                TrieNode::Extension(key, child) => {
                    let existing_key = NibbleSlice::from_encoded(&key).0;
                    let common_prefix = partial.common_prefix(&existing_key);
                    if common_prefix == 0 {
                        let idx = existing_key.at(0);
                        let child_memory_usage;
                        let child = if existing_key.len() == 1 {
                            child_memory_usage = children_memory_usage;
                            child
                        } else {
                            let child = TrieNode::Extension(
                                existing_key.mid(1).encoded(false).into_vec(),
                                child,
                            );
                            child_memory_usage =
                                children_memory_usage + child.memory_usage_direct(memory);
                            NodeHandle::InMemory(
                                memory.store(TrieNodeWithSize::new(child, child_memory_usage)),
                            )
                        };
                        let mut children = Box::<Children<_>>::default();
                        children[idx] = Some(child);
                        let branch_node = TrieNode::Branch(children, None);
                        let memory_usage =
                            branch_node.memory_usage_direct(memory) + child_memory_usage;
                        memory.store_at(handle, TrieNodeWithSize::new(branch_node, memory_usage));
                        path.pop();
                        continue;
                    } else if common_prefix == existing_key.len() {
                        let child = match child {
                            NodeHandle::Hash(hash) => self.move_node_to_mutable(memory, &hash)?,
                            NodeHandle::InMemory(handle) => handle,
                        };
                        let node = TrieNode::Extension(key, NodeHandle::InMemory(child));
                        let memory_usage = node.memory_usage_direct(memory);
                        memory.store_at(handle, TrieNodeWithSize::new(node, memory_usage));
                        handle = child;
                        partial = partial.mid(common_prefix);
                        continue;
                    } else {
                        // Partially shared prefix: covert to shorter extension and recursively add a branch.
                        let child_node = TrieNode::Extension(
                            existing_key.mid(common_prefix).encoded(false).into_vec(),
                            child,
                        );
                        let child_memory_usage =
                            children_memory_usage + child_node.memory_usage_direct(memory);
                        let child =
                            memory.store(TrieNodeWithSize::new(child_node, child_memory_usage));
                        let node = TrieNode::Extension(
                            existing_key.encoded_leftmost(common_prefix, false).into_vec(),
                            NodeHandle::InMemory(child),
                        );
                        let memory_usage = node.memory_usage_direct(memory);
                        memory.store_at(handle, TrieNodeWithSize::new(node, memory_usage));
                        handle = child;
                        partial = partial.mid(common_prefix);
                        continue;
                    }
                }
            }
        }
        for i in (0..path.len() - 1).rev() {
            let node = path.get(i).unwrap();
            let child = path.get(i + 1).unwrap();
            let child_memory_usage = memory.node_ref(*child).memory_usage;
            memory.node_mut(*node).memory_usage += child_memory_usage;
        }
        Ok(root_handle)
    }

    /// On insert/delete, we want to recompute subtree sizes without touching nodes that aren't on
    /// the path of the key inserted/deleted. This is relevant because reducing storage reads
    /// saves time and makes fraud proofs smaller.
    ///
    /// Memory usage is recalculated in two steps:
    /// 1. go down the trie, modify the node and subtract the next child on the path from memory usage
    /// 2. go up the path and add new child's memory usage
    fn calc_memory_usage_and_store(
        memory: &mut NodesStorage,
        handle: StorageHandle,
        children_memory_usage: u64,
        new_node: TrieNode,
        old_child: Option<StorageHandle>,
    ) {
        let new_memory_usage = children_memory_usage + new_node.memory_usage_direct(memory)
            - old_child.map(|child| memory.node_ref(child).memory_usage()).unwrap_or_default();
        memory.store_at(handle, TrieNodeWithSize::new(new_node, new_memory_usage));
    }

    /// Deletes a node from the trie which has key = `partial` given root node.
    /// Returns (new root node or `None` if this was the node to delete, was it updated).
    /// While deleting keeps track of all the removed / updated nodes in `death_row`.
    pub(crate) fn delete(
        &self,
        memory: &mut NodesStorage,
        node: StorageHandle,
        partial: NibbleSlice<'_>,
    ) -> Result<StorageHandle, StorageError> {
        let mut handle = node;
        let mut partial = partial;
        let root_node = handle;
        let mut path: Vec<StorageHandle> = Vec::new();
        loop {
            path.push(handle);
            let TrieNodeWithSize { node, memory_usage } = memory.destroy(handle);
            let children_memory_usage = memory_usage - node.memory_usage_direct(memory);
            match node {
                TrieNode::Empty => {
                    memory.store_at(handle, TrieNodeWithSize::empty());
                    break;
                }
                TrieNode::Leaf(key, value) => {
                    if NibbleSlice::from_encoded(&key).0 == partial {
                        self.delete_value(memory, &value)?;
                        memory.store_at(handle, TrieNodeWithSize::empty());
                        break;
                    } else {
                        let leaf_node = TrieNode::Leaf(key, value);
                        let memory_usage = leaf_node.memory_usage_direct(memory);
                        memory.store_at(handle, TrieNodeWithSize::new(leaf_node, memory_usage));
                        break;
                    }
                }
                TrieNode::Branch(mut children, value) => {
                    if partial.is_empty() {
                        if let Some(value) = &value {
                            self.delete_value(memory, value)?;
                        }
                        if children.iter().count() == 0 {
                            memory.store_at(handle, TrieNodeWithSize::empty());
                        } else {
                            Trie::calc_memory_usage_and_store(
                                memory,
                                handle,
                                children_memory_usage,
                                TrieNode::Branch(children, None),
                                None,
                            );
                        }
                        break;
                    } else {
                        let child = &mut children[partial.at(0)];
                        if let Some(node_or_hash) = child.take() {
                            let new_handle = match node_or_hash {
                                NodeHandle::Hash(hash) => {
                                    self.move_node_to_mutable(memory, &hash)?
                                }
                                NodeHandle::InMemory(handle) => handle,
                            };
                            *child = Some(NodeHandle::InMemory(new_handle));
                            Trie::calc_memory_usage_and_store(
                                memory,
                                handle,
                                children_memory_usage,
                                TrieNode::Branch(children, value),
                                Some(new_handle),
                            );
                            handle = new_handle;
                            partial = partial.mid(1);
                            continue;
                        } else {
                            memory.store_at(
                                handle,
                                TrieNodeWithSize::new(
                                    TrieNode::Branch(children, value),
                                    memory_usage,
                                ),
                            );
                            break;
                        }
                    }
                }
                TrieNode::Extension(key, child) => {
                    let (common_prefix, existing_len) = {
                        let existing_key = NibbleSlice::from_encoded(&key).0;
                        (existing_key.common_prefix(&partial), existing_key.len())
                    };
                    if common_prefix == existing_len {
                        let child = match child {
                            NodeHandle::Hash(hash) => self.move_node_to_mutable(memory, &hash)?,
                            NodeHandle::InMemory(node) => node,
                        };
                        Trie::calc_memory_usage_and_store(
                            memory,
                            handle,
                            children_memory_usage,
                            TrieNode::Extension(key, NodeHandle::InMemory(child)),
                            Some(child),
                        );
                        partial = partial.mid(existing_len);
                        handle = child;
                        continue;
                    } else {
                        memory.store_at(
                            handle,
                            TrieNodeWithSize::new(TrieNode::Extension(key, child), memory_usage),
                        );
                        break;
                    }
                }
            }
        }
        self.fix_nodes(memory, path)?;
        Ok(root_node)
    }

    fn fix_nodes(
        &self,
        memory: &mut NodesStorage,
        path: Vec<StorageHandle>,
    ) -> Result<(), StorageError> {
        let mut child_memory_usage = 0;
        for handle in path.into_iter().rev() {
            let TrieNodeWithSize { node, memory_usage } = memory.destroy(handle);
            let memory_usage = memory_usage + child_memory_usage;
            match node {
                TrieNode::Empty => {
                    memory.store_at(handle, TrieNodeWithSize::empty());
                }
                TrieNode::Leaf(key, value) => {
                    memory.store_at(
                        handle,
                        TrieNodeWithSize::new(TrieNode::Leaf(key, value), memory_usage),
                    );
                }
                TrieNode::Branch(mut children, value) => {
                    for child in children.0.iter_mut() {
                        if let Some(NodeHandle::InMemory(h)) = child {
                            if let TrieNode::Empty = memory.node_ref(*h).node {
                                *child = None
                            }
                        }
                    }
                    let num_children = children.iter().count();
                    if num_children == 0 {
                        if let Some(value) = value {
                            let empty = NibbleSlice::new(&[]).encoded(true).into_vec();
                            let leaf_node = TrieNode::Leaf(empty, value);
                            let memory_usage = leaf_node.memory_usage_direct(memory);
                            memory.store_at(handle, TrieNodeWithSize::new(leaf_node, memory_usage));
                        } else {
                            memory.store_at(handle, TrieNodeWithSize::empty());
                        }
                    } else if num_children == 1 && value.is_none() {
                        // Branch with one child becomes extension
                        // Extension followed by leaf becomes leaf
                        // Extension followed by extension becomes extension
                        let idx = children.iter().next().unwrap().0;
                        let child = children[idx].take().unwrap();
                        let key = NibbleSlice::new(&[(idx << 4) as u8])
                            .encoded_leftmost(1, false)
                            .into_vec();
                        self.fix_extension_node(memory, handle, key, child)?;
                    } else {
                        let node =
                            TrieNodeWithSize::new(TrieNode::Branch(children, value), memory_usage);
                        memory.store_at(handle, node);
                    }
                }
                TrieNode::Extension(key, child) => {
                    self.fix_extension_node(memory, handle, key, child)?;
                }
            }
            child_memory_usage = memory.node_ref(handle).memory_usage;
        }
        Ok(())
    }

    fn fix_extension_node(
        &self,
        memory: &mut NodesStorage,
        handle: StorageHandle,
        key: Vec<u8>,
        child: NodeHandle,
    ) -> Result<(), StorageError> {
        let child = match child {
            NodeHandle::Hash(hash) => self.move_node_to_mutable(memory, &hash)?,
            NodeHandle::InMemory(h) => h,
        };
        let TrieNodeWithSize { node, memory_usage } = memory.destroy(child);
        let child_child_memory_usage = memory_usage - node.memory_usage_direct(memory);
        match node {
            TrieNode::Empty => {
                memory.store_at(handle, TrieNodeWithSize::empty());
            }
            TrieNode::Leaf(child_key, value) => {
                let key = NibbleSlice::from_encoded(&key)
                    .0
                    .merge_encoded(&NibbleSlice::from_encoded(&child_key).0, true)
                    .into_vec();
                let new_node = TrieNode::Leaf(key, value);
                let memory_usage = new_node.memory_usage_direct(memory);
                memory.store_at(handle, TrieNodeWithSize::new(new_node, memory_usage));
            }
            TrieNode::Branch(children, value) => {
                memory.store_at(
                    child,
                    TrieNodeWithSize::new(TrieNode::Branch(children, value), memory_usage),
                );
                let new_node = TrieNode::Extension(key, NodeHandle::InMemory(child));
                let memory_usage = memory_usage + new_node.memory_usage_direct(memory);
                memory.store_at(handle, TrieNodeWithSize::new(new_node, memory_usage));
            }
            TrieNode::Extension(child_key, child_child) => {
                let key = NibbleSlice::from_encoded(&key)
                    .0
                    .merge_encoded(&NibbleSlice::from_encoded(&child_key).0, false)
                    .into_vec();
                let new_node = TrieNode::Extension(key, child_child);
                let memory_usage = new_node.memory_usage_direct(memory) + child_child_memory_usage;
                memory.store_at(handle, TrieNodeWithSize::new(new_node, memory_usage));
            }
        }
        Ok(())
    }

    pub(crate) fn flatten_nodes(
        old_root: &CryptoHash,
        memory: NodesStorage,
        node: StorageHandle,
    ) -> Result<TrieChanges, StorageError> {
        let mut stack: Vec<(StorageHandle, FlattenNodesCrumb)> = Vec::new();
        stack.push((node, FlattenNodesCrumb::Entering));
        let mut last_hash = CryptoHash::default();
        let mut buffer: Vec<u8> = Vec::new();
        let mut memory = memory;
        'outer: while let Some((node, position)) = stack.pop() {
            let node_with_size = memory.node_ref(node);
            let memory_usage = node_with_size.memory_usage;
            let raw_node = match &node_with_size.node {
                TrieNode::Empty => {
                    last_hash = Trie::EMPTY_ROOT;
                    continue;
                }
                TrieNode::Branch(children, value) => match position {
                    FlattenNodesCrumb::Entering => {
                        stack.push((node, FlattenNodesCrumb::AtChild(Default::default(), 0)));
                        continue;
                    }
                    FlattenNodesCrumb::AtChild(mut new_children, mut i) => {
                        if i > 0 && children[i - 1].is_some() {
                            new_children[i - 1] = Some(last_hash);
                        }
                        while i < 16 {
                            match children[i].clone() {
                                Some(NodeHandle::InMemory(handle)) => {
                                    stack.push((
                                        node,
                                        FlattenNodesCrumb::AtChild(new_children, i + 1),
                                    ));
                                    stack.push((handle, FlattenNodesCrumb::Entering));
                                    continue 'outer;
                                }
                                Some(NodeHandle::Hash(hash)) => new_children[i] = Some(hash),
                                None => {}
                            }
                            i += 1;
                        }
                        let new_value =
                            value.clone().map(|value| Trie::flatten_value(&mut memory, value));
                        RawTrieNode::branch(*new_children, new_value)
                    }
                    FlattenNodesCrumb::Exiting => unreachable!(),
                },
                TrieNode::Extension(key, child) => match position {
                    FlattenNodesCrumb::Entering => match child {
                        NodeHandle::InMemory(child) => {
                            stack.push((node, FlattenNodesCrumb::Exiting));
                            stack.push((*child, FlattenNodesCrumb::Entering));
                            continue;
                        }
                        NodeHandle::Hash(hash) => RawTrieNode::Extension(key.clone(), *hash),
                    },
                    FlattenNodesCrumb::Exiting => RawTrieNode::Extension(key.clone(), last_hash),
                    _ => unreachable!(),
                },
                TrieNode::Leaf(key, value) => {
                    let key = key.clone();
                    let value = value.clone();
                    let value = Trie::flatten_value(&mut memory, value);
                    RawTrieNode::Leaf(key, value)
                }
            };
            let raw_node_with_size = RawTrieNodeWithSize { node: raw_node, memory_usage };
            raw_node_with_size.serialize(&mut buffer).unwrap();
            let key = hash(&buffer);

            let (_value, rc) =
                memory.refcount_changes.entry(key).or_insert_with(|| (buffer.clone(), 0));
            *rc += 1;
            buffer.clear();
            last_hash = key;
        }
        let (insertions, deletions) =
            Trie::convert_to_insertions_and_deletions(memory.refcount_changes);
        Ok(TrieChanges { old_root: *old_root, new_root: last_hash, insertions, deletions })
    }

    fn flatten_value(memory: &mut NodesStorage, value: ValueHandle) -> ValueRef {
        match value {
            ValueHandle::InMemory(value_handle) => {
                let value = memory.value_ref(value_handle).to_vec();
                let value_length = value.len() as u32;
                let value_hash = hash(&value);
                let (_value, rc) =
                    memory.refcount_changes.entry(value_hash).or_insert_with(|| (value, 0));
                *rc += 1;
                ValueRef { length: value_length, hash: value_hash }
            }
            ValueHandle::HashAndSize(value) => value,
        }
    }
}
