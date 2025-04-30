use super::ops::interface::{
    GenericNodeOrIndex, GenericTrieNode, GenericTrieNodeWithSize, GenericTrieUpdate,
    GenericTrieValue, GenericUpdatedTrieNode, GenericUpdatedTrieNodeWithSize, UpdatedNodeId,
};
use super::{
    AccessOptions, Children, RawTrieNode, RawTrieNodeWithSize, StorageHandle, StorageValueHandle,
    Trie, TrieChanges, TrieRefcountDeltaMap, ValueHandle,
};
use borsh::BorshSerialize;
use near_primitives::errors::StorageError;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::state::ValueRef;

const INVALID_STORAGE_HANDLE: &str = "invalid storage handle";

pub(crate) type TrieStorageNodePtr = CryptoHash;

pub(crate) type TrieStorageNode = GenericTrieNode<TrieStorageNodePtr, ValueHandle>;

impl TrieStorageNode {
    fn new_branch(children: Children, value: Option<ValueRef>) -> Self {
        let children = Box::new(children.0);
        let value = value.map(ValueHandle::HashAndSize);
        Self::Branch { children, value }
    }

    /// Conversion from the node just read from trie storage.
    pub fn from_raw_trie_node(node: RawTrieNode) -> Self {
        match node {
            RawTrieNode::Leaf(extension, value) => Self::Leaf {
                extension: extension.into_boxed_slice(),
                value: ValueHandle::HashAndSize(value),
            },
            RawTrieNode::BranchNoValue(children) => Self::new_branch(children, None),
            RawTrieNode::BranchWithValue(value, children) => {
                Self::new_branch(children, Some(value))
            }
            RawTrieNode::Extension(extension, child) => {
                Self::Extension { extension: extension.into_boxed_slice(), child }
            }
        }
    }
}

pub(crate) type TrieStorageNodeWithSize = GenericTrieNodeWithSize<TrieStorageNodePtr, ValueHandle>;

pub(crate) type UpdatedTrieStorageNodeWithSize =
    GenericUpdatedTrieNodeWithSize<TrieStorageNodePtr, ValueHandle>;

impl TrieStorageNodeWithSize {
    pub fn from_raw_trie_node_with_size(node: RawTrieNodeWithSize) -> Self {
        Self {
            node: TrieStorageNode::from_raw_trie_node(node.node),
            memory_usage: node.memory_usage,
        }
    }
}

pub(crate) struct TrieStorageUpdate<'a> {
    pub(crate) nodes: Vec<Option<UpdatedTrieStorageNodeWithSize>>,
    pub(crate) values: Vec<Option<Vec<u8>>>,
    pub(crate) refcount_changes: TrieRefcountDeltaMap,
    pub(crate) trie: &'a Trie,
}

/// Local mutable storage that owns node objects.
impl<'a> TrieStorageUpdate<'a> {
    pub fn new(trie: &'a Trie) -> TrieStorageUpdate<'a> {
        TrieStorageUpdate {
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

impl<'a> GenericTrieUpdate<'a, TrieStorageNodePtr, ValueHandle> for TrieStorageUpdate<'a> {
    fn ensure_updated(
        &mut self,
        node: GenericNodeOrIndex<TrieStorageNodePtr>,
        opts: AccessOptions,
    ) -> Result<UpdatedNodeId, StorageError> {
        match node {
            GenericNodeOrIndex::Old(node_hash) => {
                self.trie.move_node_to_mutable(self, &node_hash, opts).map(|handle| handle.0)
            }
            GenericNodeOrIndex::Updated(node_id) => Ok(node_id),
        }
    }

    fn take_node(&mut self, index: UpdatedNodeId) -> UpdatedTrieStorageNodeWithSize {
        self.nodes
            .get_mut(index)
            .expect(INVALID_STORAGE_HANDLE)
            .take()
            .expect(INVALID_STORAGE_HANDLE)
    }

    fn place_node_at(&mut self, index: UpdatedNodeId, node: UpdatedTrieStorageNodeWithSize) {
        debug_assert!(self.nodes.get(index).expect(INVALID_STORAGE_HANDLE).is_none());
        self.nodes[index] = Some(node);
    }

    fn place_node(&mut self, node: UpdatedTrieStorageNodeWithSize) -> UpdatedNodeId {
        let index = self.nodes.len();
        self.nodes.push(Some(node));
        index
    }

    fn get_node_ref(&self, index: UpdatedNodeId) -> &UpdatedTrieStorageNodeWithSize {
        self.nodes.get(index).expect(INVALID_STORAGE_HANDLE).as_ref().expect(INVALID_STORAGE_HANDLE)
    }

    fn store_value(&mut self, value: GenericTrieValue) -> ValueHandle {
        let GenericTrieValue::MemtrieAndDisk(value) = value else {
            unimplemented!(
                "NodesStorage for Trie doesn't support value {value:?} \
                because disk updates must be generated."
            );
        };

        let value_len = value.len();
        self.values.push(Some(value));
        ValueHandle::InMemory(StorageValueHandle(self.values.len() - 1, value_len))
    }

    fn delete_value(&mut self, value: ValueHandle) -> Result<(), StorageError> {
        match value {
            ValueHandle::HashAndSize(value) => {
                // Note that we don't need to read the actual value to remove it.
                self.refcount_changes.subtract(value.hash, 1);
            }
            ValueHandle::InMemory(_) => {
                // Do nothing. Values which were just inserted were not
                // refcounted yet.
            }
        }
        Ok(())
    }
}

enum FlattenNodesCrumb {
    Entering,
    AtChild(Box<Children>, u8),
    Exiting,
}

impl TrieStorageUpdate<'_> {
    #[tracing::instrument(level = "debug", target = "store::trie", "Trie::flatten_nodes", skip_all)]
    pub(crate) fn flatten_nodes(
        mut self,
        old_root: &CryptoHash,
        node: usize,
    ) -> Result<TrieChanges, StorageError> {
        let mut stack: Vec<(usize, FlattenNodesCrumb)> = Vec::new();
        stack.push((node, FlattenNodesCrumb::Entering));
        let mut last_hash = CryptoHash::default();
        let mut buffer: Vec<u8> = Vec::new();
        'outer: while let Some((node, position)) = stack.pop() {
            let node_with_size = self.get_node_ref(node);
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
                        let new_value = (*value).map(|value| self.flatten_value(value));
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
                    let value = self.flatten_value(value);
                    RawTrieNode::Leaf(key, value)
                }
            };
            let raw_node_with_size = RawTrieNodeWithSize { node: raw_node, memory_usage };
            raw_node_with_size.serialize(&mut buffer).unwrap();
            let key = hash(&buffer);

            self.refcount_changes.add(key, buffer.clone(), 1);
            buffer.clear();
            last_hash = key;
        }
        let (insertions, deletions) = self.refcount_changes.into_changes();
        Ok(TrieChanges {
            old_root: *old_root,
            new_root: last_hash,
            insertions,
            deletions,
            memtrie_changes: None,
            children_memtrie_changes: Default::default(),
        })
    }

    fn flatten_value(&mut self, value: ValueHandle) -> ValueRef {
        match value {
            ValueHandle::InMemory(value_handle) => {
                let value = self.value_ref(value_handle).to_vec();
                let value_length = value.len() as u32;
                let value_hash = hash(&value);
                self.refcount_changes.add(value_hash, value, 1);
                ValueRef { length: value_length, hash: value_hash }
            }
            ValueHandle::HashAndSize(value) => value,
        }
    }
}
