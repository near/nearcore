use super::interface::{
    GenericNodeOrIndex, GenericTrieUpdate, GenericTrieValue, GenericUpdatedTrieNode,
    GenericUpdatedTrieNodeWithSize, HasValueLength, UpdatedNodeId,
};
use super::squash::GenericTrieUpdateSquash;
use crate::NibbleSlice;
use crate::trie::AccessOptions;
use near_primitives::errors::StorageError;

pub(crate) trait GenericTrieUpdateInsertDelete<'a, N, V>:
    GenericTrieUpdateSquash<'a, N, V>
where
    N: std::fmt::Debug,
    V: std::fmt::Debug + HasValueLength,
{
    fn calc_memory_usage_and_store(
        &mut self,
        node_id: UpdatedNodeId,
        children_memory_usage: u64,
        new_node: GenericUpdatedTrieNode<N, V>,
        old_child: Option<UpdatedNodeId>,
    ) {
        let new_memory_usage =
            children_memory_usage.saturating_add(new_node.memory_usage_direct()).saturating_sub(
                old_child.map(|child| self.get_node_ref(child).memory_usage).unwrap_or_default(),
            );
        self.place_node_at(
            node_id,
            GenericUpdatedTrieNodeWithSize { node: new_node, memory_usage: new_memory_usage },
        );
    }

    /// Insertion logic. We descend from the root down to whatever node corresponds to
    /// the inserted value. We would need to split, modify, or transform nodes along
    /// the way to achieve that. This takes care of refcounting changes for existing
    /// nodes as well as values, but will not yet increment refcount for any newly
    /// created nodes - that's done at the end.
    fn generic_insert(
        &mut self,
        mut node_id: UpdatedNodeId,
        key: &[u8],
        value: GenericTrieValue,
        opts: AccessOptions,
    ) -> Result<(), StorageError> {
        let mut partial = NibbleSlice::new(key);
        // Path to the key being inserted.
        // Needed to recompute memory usages in the end.
        let mut path = Vec::new();

        loop {
            path.push(node_id);
            // Take out the current node; we'd have to change it no matter what.
            let GenericUpdatedTrieNodeWithSize { node, memory_usage } = self.take_node(node_id);
            let children_memory_usage = memory_usage.saturating_sub(node.memory_usage_direct());

            match node {
                GenericUpdatedTrieNode::Empty => {
                    // There was no node here, create a new leaf.
                    let value_handle = self.store_value(value);
                    let node = GenericUpdatedTrieNode::Leaf {
                        extension: partial.encoded(true).into_vec().into_boxed_slice(),
                        value: value_handle,
                    };
                    let memory_usage = node.memory_usage_direct();
                    self.place_node_at(
                        node_id,
                        GenericUpdatedTrieNodeWithSize { node, memory_usage },
                    );
                    break;
                }
                GenericUpdatedTrieNode::Branch { children, value: old_value } => {
                    if partial.is_empty() {
                        // This branch node is exactly where the value should be added.
                        if let Some(value) = old_value {
                            self.delete_value(value)?;
                        }
                        let value_handle = self.store_value(value);
                        let node =
                            GenericUpdatedTrieNode::Branch { children, value: Some(value_handle) };
                        let memory_usage = children_memory_usage + node.memory_usage_direct();
                        self.place_node_at(
                            node_id,
                            GenericUpdatedTrieNodeWithSize { node, memory_usage },
                        );
                        break;
                    } else {
                        // Continue descending into the branch, possibly adding a new child.
                        let mut new_children = children;
                        let child = &mut new_children[partial.at(0) as usize];
                        let new_node_id = match child.take() {
                            Some(node_id) => self.ensure_updated(node_id, opts)?,
                            None => self.place_node(GenericUpdatedTrieNodeWithSize::empty()),
                        };
                        *child = Some(GenericNodeOrIndex::Updated(new_node_id));
                        self.calc_memory_usage_and_store(
                            node_id,
                            children_memory_usage,
                            GenericUpdatedTrieNode::Branch {
                                children: new_children,
                                value: old_value,
                            },
                            Some(new_node_id),
                        );
                        node_id = new_node_id;
                        partial = partial.mid(1);
                        continue;
                    }
                }
                GenericUpdatedTrieNode::Leaf { extension, value: old_value } => {
                    let existing_key = NibbleSlice::from_encoded(&extension).0;
                    let common_prefix = partial.common_prefix(&existing_key);
                    if common_prefix == existing_key.len() && common_prefix == partial.len() {
                        // We're at the exact leaf. Rewrite the value at this leaf.
                        self.delete_value(old_value)?;
                        let value_handle = self.store_value(value);
                        let node = GenericUpdatedTrieNode::Leaf { extension, value: value_handle };
                        let memory_usage = node.memory_usage_direct();
                        self.place_node_at(
                            node_id,
                            GenericUpdatedTrieNodeWithSize { node, memory_usage },
                        );
                        break;
                    } else if common_prefix == 0 {
                        // Convert the leaf to an equivalent branch. We are not adding
                        // the new branch yet; that will be done in the next iteration.
                        let mut children = Box::<[_; 16]>::default();
                        let children_memory_usage;
                        let branch_node = if existing_key.is_empty() {
                            // Existing key being empty means the old value now lives at the branch.
                            children_memory_usage = 0;
                            GenericUpdatedTrieNode::Branch { children, value: Some(old_value) }
                        } else {
                            let branch_idx = existing_key.at(0) as usize;
                            let new_extension = existing_key.mid(1).encoded(true).into_vec();
                            let new_node = GenericUpdatedTrieNode::Leaf {
                                extension: new_extension.into_boxed_slice(),
                                value: old_value,
                            };
                            let memory_usage = new_node.memory_usage_direct();
                            children_memory_usage = memory_usage;
                            let new_node_id = self.place_node(GenericUpdatedTrieNodeWithSize {
                                node: new_node,
                                memory_usage,
                            });
                            children[branch_idx] = Some(GenericNodeOrIndex::Updated(new_node_id));
                            GenericUpdatedTrieNode::Branch { children, value: None }
                        };
                        let memory_usage =
                            branch_node.memory_usage_direct() + children_memory_usage;
                        self.place_node_at(
                            node_id,
                            GenericUpdatedTrieNodeWithSize { node: branch_node, memory_usage },
                        );
                        path.pop();
                        continue;
                    } else {
                        // Split this leaf into an extension plus a leaf, and descend into the leaf.
                        let leaf_node = GenericUpdatedTrieNode::Leaf {
                            extension: existing_key
                                .mid(common_prefix)
                                .encoded(true)
                                .into_vec()
                                .into_boxed_slice(),
                            value: old_value,
                        };
                        let leaf_memory_usage = leaf_node.memory_usage_direct();
                        let leaf_node_id = self.place_node(GenericUpdatedTrieNodeWithSize {
                            node: leaf_node,
                            memory_usage: leaf_memory_usage,
                        });
                        let extension_node = GenericUpdatedTrieNode::Extension {
                            extension: partial
                                .encoded_leftmost(common_prefix, false)
                                .into_vec()
                                .into_boxed_slice(),
                            child: GenericNodeOrIndex::Updated(leaf_node_id),
                        };
                        let extension_memory_usage = extension_node.memory_usage_direct();
                        self.place_node_at(
                            node_id,
                            GenericUpdatedTrieNodeWithSize {
                                node: extension_node,
                                memory_usage: extension_memory_usage,
                            },
                        );
                        node_id = leaf_node_id;
                        partial = partial.mid(common_prefix);
                        continue;
                    }
                }
                GenericUpdatedTrieNode::Extension { extension, child: old_child, .. } => {
                    let existing_key = NibbleSlice::from_encoded(&extension).0;
                    let common_prefix = partial.common_prefix(&existing_key);
                    if common_prefix == 0 {
                        // Split Extension to Branch.
                        let idx = existing_key.at(0);
                        let child_memory_usage;
                        let child = if existing_key.len() == 1 {
                            child_memory_usage = children_memory_usage;
                            old_child
                        } else {
                            let inner_child_node = GenericUpdatedTrieNode::Extension {
                                extension: existing_key
                                    .mid(1)
                                    .encoded(false)
                                    .into_vec()
                                    .into_boxed_slice(),
                                child: old_child,
                            };
                            child_memory_usage =
                                children_memory_usage + inner_child_node.memory_usage_direct();
                            let inner_child = GenericUpdatedTrieNodeWithSize {
                                node: inner_child_node,
                                memory_usage: child_memory_usage,
                            };
                            GenericNodeOrIndex::Updated(self.place_node(inner_child))
                        };

                        let mut children = Box::<[_; 16]>::default();
                        children[idx as usize] = Some(child);
                        let branch_node = GenericUpdatedTrieNode::Branch { children, value: None };
                        let branch_memory_usage =
                            branch_node.memory_usage_direct() + child_memory_usage;
                        self.place_node_at(
                            node_id,
                            GenericUpdatedTrieNodeWithSize {
                                node: branch_node,
                                memory_usage: branch_memory_usage,
                            },
                        );
                        // Start over from the same position.
                        path.pop();
                        continue;
                    } else if common_prefix == existing_key.len() {
                        // Dereference child and descend into it.
                        let child_id = self.ensure_updated(old_child, opts)?;
                        let node = GenericUpdatedTrieNode::Extension {
                            extension,
                            child: GenericNodeOrIndex::Updated(child_id),
                        };
                        let memory_usage = node.memory_usage_direct();
                        self.place_node_at(
                            node_id,
                            GenericUpdatedTrieNodeWithSize { node, memory_usage },
                        );
                        node_id = child_id;
                        partial = partial.mid(common_prefix);
                        continue;
                    } else {
                        // Partially shared prefix. Convert to shorter extension and descend into it.
                        // On the next step, branch will be created.
                        let inner_child_node = GenericUpdatedTrieNode::Extension {
                            extension: existing_key
                                .mid(common_prefix)
                                .encoded(false)
                                .into_vec()
                                .into_boxed_slice(),
                            child: old_child,
                        };
                        let inner_child_memory_usage =
                            children_memory_usage + inner_child_node.memory_usage_direct();
                        let inner_child_node_id = self.place_node(GenericUpdatedTrieNodeWithSize {
                            node: inner_child_node,
                            memory_usage: inner_child_memory_usage,
                        });
                        let child_node = GenericUpdatedTrieNode::Extension {
                            extension: existing_key
                                .encoded_leftmost(common_prefix, false)
                                .into_vec()
                                .into_boxed_slice(),
                            child: GenericNodeOrIndex::Updated(inner_child_node_id),
                        };
                        let memory_usage = child_node.memory_usage_direct();
                        self.place_node_at(
                            node_id,
                            GenericUpdatedTrieNodeWithSize { node: child_node, memory_usage },
                        );
                        node_id = inner_child_node_id;
                        partial = partial.mid(common_prefix);
                        continue;
                    }
                }
            }
        }

        for i in (0..path.len() - 1).rev() {
            let node_id = path.get(i).unwrap();
            let child_id = path.get(i + 1).unwrap();
            let child_memory_usage = self.get_node_ref(*child_id).memory_usage;
            let mut node = self.take_node(*node_id);
            node.memory_usage += child_memory_usage;
            self.place_node_at(*node_id, node);
        }

        Ok(())
    }

    /// Deletes a key from the trie.
    ///
    /// This will go down from the root of the trie to supposed location of the
    /// key, deleting it if found. It will also keep the trie structure
    /// consistent by changing the types of any nodes along the way.
    ///
    /// Deleting a non-existent key is allowed, and is a no-op.
    fn generic_delete(
        &mut self,
        mut node_id: UpdatedNodeId,
        key: &[u8],
        opts: AccessOptions,
    ) -> Result<(), StorageError> {
        let mut partial = NibbleSlice::new(key);
        // Path to find the key to delete.
        // Needed to squash nodes and recompute memory usages in the end.
        let mut path = vec![];
        let mut key_deleted = true;

        loop {
            path.push(node_id);
            let GenericUpdatedTrieNodeWithSize { node, memory_usage } = self.take_node(node_id);
            let children_memory_usage = memory_usage.saturating_sub(node.memory_usage_direct());

            match node {
                GenericUpdatedTrieNode::Empty => {
                    // Nothing to delete.
                    self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize::empty());
                    key_deleted = false;
                    break;
                }
                GenericUpdatedTrieNode::Leaf { extension, value } => {
                    if NibbleSlice::from_encoded(&extension).0 == partial {
                        self.delete_value(value)?;
                        self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize::empty());
                        break;
                    } else {
                        // Key being deleted doesn't exist. Put the leaf back.
                        let node = GenericUpdatedTrieNode::Leaf { extension, value };
                        self.place_node_at(
                            node_id,
                            GenericUpdatedTrieNodeWithSize { node, memory_usage },
                        );
                        key_deleted = false;
                        break;
                    }
                }
                GenericUpdatedTrieNode::Branch { mut children, value } => {
                    if partial.is_empty() {
                        if value.is_none() {
                            // Key being deleted doesn't exist.
                            let node = GenericUpdatedTrieNode::Branch { children, value };
                            self.place_node_at(
                                node_id,
                                GenericUpdatedTrieNodeWithSize { node, memory_usage },
                            );
                            key_deleted = false;
                            break;
                        };
                        self.delete_value(value.unwrap())?;
                        self.calc_memory_usage_and_store(
                            node_id,
                            children_memory_usage,
                            GenericUpdatedTrieNode::Branch { children, value: None },
                            None,
                        );
                        // if needed, branch will be squashed at the end of the function.
                        break;
                    } else {
                        let child = &mut children[partial.at(0) as usize];
                        let old_child_id = match child.take() {
                            Some(node_id) => node_id,
                            None => {
                                // Key being deleted doesn't exist.
                                let node = GenericUpdatedTrieNode::Branch { children, value };
                                self.place_node_at(
                                    node_id,
                                    GenericUpdatedTrieNodeWithSize { node, memory_usage },
                                );
                                key_deleted = false;
                                break;
                            }
                        };
                        let new_child_id = self.ensure_updated(old_child_id, opts)?;
                        *child = Some(GenericNodeOrIndex::Updated(new_child_id));
                        self.calc_memory_usage_and_store(
                            node_id,
                            children_memory_usage,
                            GenericUpdatedTrieNode::Branch { children, value },
                            Some(new_child_id),
                        );

                        node_id = new_child_id;
                        partial = partial.mid(1);
                        continue;
                    }
                }
                GenericUpdatedTrieNode::Extension { extension, child } => {
                    let (common_prefix, existing_len) = {
                        let extension_nibbles = NibbleSlice::from_encoded(&extension).0;
                        (extension_nibbles.common_prefix(&partial), extension_nibbles.len())
                    };
                    if common_prefix == existing_len {
                        let new_child_id = self.ensure_updated(child, opts)?;
                        self.calc_memory_usage_and_store(
                            node_id,
                            children_memory_usage,
                            GenericUpdatedTrieNode::Extension {
                                extension,
                                child: GenericNodeOrIndex::Updated(new_child_id),
                            },
                            Some(new_child_id),
                        );

                        node_id = new_child_id;
                        partial = partial.mid(existing_len);
                        continue;
                    } else {
                        // Key being deleted doesn't exist.
                        let node = GenericUpdatedTrieNode::Extension { extension, child };
                        self.place_node_at(
                            node_id,
                            GenericUpdatedTrieNodeWithSize { node, memory_usage },
                        );
                        key_deleted = false;
                        break;
                    }
                }
            }
        }

        // Now we recompute memory usage and possibly squash nodes to keep the
        // trie structure unique.
        let mut child_memory_usage = 0;
        for node_id in path.into_iter().rev() {
            // First, recompute memory usage, emulating the recursive descent.
            let GenericUpdatedTrieNodeWithSize { node, mut memory_usage } = self.take_node(node_id);
            memory_usage += child_memory_usage;
            self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize { node, memory_usage });

            // Then, squash node to ensure unique trie structure, changing its
            // type if needed. If `key_deleted` is false, trie structure is
            // untouched.
            if key_deleted {
                self.squash_node(node_id, opts)?;
            }

            child_memory_usage = self.get_node_ref(node_id).memory_usage;
        }

        Ok(())
    }
}

impl<'a, N, V, T> GenericTrieUpdateInsertDelete<'a, N, V> for T
where
    N: std::fmt::Debug,
    V: std::fmt::Debug + HasValueLength,
    T: GenericTrieUpdate<'a, N, V>,
{
}
