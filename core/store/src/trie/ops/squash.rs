use super::interface::{
    GenericNodeOrIndex, GenericTrieUpdate, GenericUpdatedTrieNode, GenericUpdatedTrieNodeWithSize,
    HasValueLength, UpdatedNodeId,
};
use crate::NibbleSlice;
use crate::trie::AccessOptions;
use near_primitives::errors::StorageError;

pub(crate) trait GenericTrieUpdateSquash<'a, N, V>: GenericTrieUpdate<'a, N, V>
where
    N: std::fmt::Debug,
    V: std::fmt::Debug + HasValueLength,
{
    /// When we delete keys, it may be necessary to change types of some nodes,
    /// in order to keep the trie structure unique. For example, if a branch
    /// had two children, but after deletion ended up with one child and no
    /// value, it must be converted to an extension node. Or, if an extension
    /// node ended up having a child which is also an extension node, they must
    /// be combined into a single extension node. This function takes care of
    /// all these cases for a single node.
    ///
    /// To restructure trie correctly, this function must be called in
    /// post-order traversal for every modified node. It may be proven by
    /// induction on subtrees.
    /// For single key removal, it is called for every node on the path from
    /// the leaf to the root.
    /// For range removal, it is called in the end of recursive range removal
    /// function, which is the definition of post-order traversal.
    fn squash_node(
        &mut self,
        node_id: UpdatedNodeId,
        opts: AccessOptions,
    ) -> Result<(), StorageError> {
        let GenericUpdatedTrieNodeWithSize { node, memory_usage } = self.take_node(node_id);
        match node {
            GenericUpdatedTrieNode::Empty => {
                // Empty node will be absorbed by its parent node, so defer that.
                self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize::empty());
            }
            GenericUpdatedTrieNode::Leaf { .. } => {
                // It's impossible that we would squash a leaf node, because if we
                // had deleted a leaf it would become Empty instead.
                unreachable!();
            }
            GenericUpdatedTrieNode::Branch { mut children, value } => {
                // Remove any children that are now empty (removed).
                for child in children.iter_mut() {
                    if let Some(GenericNodeOrIndex::Updated(child_node_id)) = child {
                        if let GenericUpdatedTrieNode::Empty =
                            self.get_node_ref(*child_node_id).node
                        {
                            *child = None;
                        }
                    }
                }
                let num_children = children.iter().filter(|node| node.is_some()).count();
                if num_children == 0 {
                    match value {
                        None => {
                            self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize::empty())
                        }
                        Some(value) => {
                            // Branch with zero children and a value becomes leaf.
                            let leaf_node = GenericUpdatedTrieNode::Leaf {
                                extension: NibbleSlice::new(&[])
                                    .encoded(true)
                                    .into_vec()
                                    .into_boxed_slice(),
                                value,
                            };
                            let memory_usage = leaf_node.memory_usage_direct();
                            self.place_node_at(
                                node_id,
                                GenericUpdatedTrieNodeWithSize { node: leaf_node, memory_usage },
                            );
                        }
                    }
                } else if num_children == 1 && value.is_none() {
                    // Branch with 1 child but no value becomes extension.
                    let (idx, child) = children
                        .into_iter()
                        .enumerate()
                        .find_map(|(idx, node)| node.map(|node| (idx, node)))
                        .unwrap();
                    let extension = NibbleSlice::new(&[(idx << 4) as u8])
                        .encoded_leftmost(1, false)
                        .into_vec()
                        .into_boxed_slice();
                    self.extend_child(node_id, extension, child, opts)?;
                } else {
                    // Branch with more than 1 children stays branch.
                    self.place_node_at(
                        node_id,
                        GenericUpdatedTrieNodeWithSize {
                            node: GenericUpdatedTrieNode::Branch { children, value },
                            memory_usage,
                        },
                    );
                }
            }
            GenericUpdatedTrieNode::Extension { extension, child } => {
                self.extend_child(node_id, extension, child, opts)?;
            }
        }
        Ok(())
    }

    // Creates an extension node at `node_id`, but squashes the extension node according to
    // its child; e.g. if the child is a leaf, the whole node becomes a leaf.
    fn extend_child(
        &mut self,
        // The node being squashed.
        node_id: UpdatedNodeId,
        // The current extension.
        extension: Box<[u8]>,
        // The current child.
        child_id: GenericNodeOrIndex<N>,
        opts: AccessOptions,
    ) -> Result<(), StorageError> {
        let child_id = self.ensure_updated(child_id, opts)?;
        let GenericUpdatedTrieNodeWithSize { node, memory_usage } = self.take_node(child_id);
        let child_child_memory_usage = memory_usage.saturating_sub(node.memory_usage_direct());
        match node {
            GenericUpdatedTrieNode::Empty => {
                self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize::empty());
            }
            // If the child is a leaf (which could happen if a branch node lost
            // all its branches and only had a value left, or is left with only
            // one branch and that was squashed to a leaf).
            GenericUpdatedTrieNode::Leaf { extension: child_extension, value } => {
                let child_extension = NibbleSlice::from_encoded(&child_extension).0;
                let extension = NibbleSlice::from_encoded(&extension)
                    .0
                    .merge_encoded(&child_extension, true)
                    .into_vec()
                    .into_boxed_slice();
                let node = GenericUpdatedTrieNode::Leaf { extension, value };
                let memory_usage = node.memory_usage_direct();
                self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize { node, memory_usage });
            }
            // If the child is a branch, there's nothing to squash.
            child_node @ GenericUpdatedTrieNode::Branch { .. } => {
                self.place_node_at(
                    child_id,
                    GenericUpdatedTrieNodeWithSize { node: child_node, memory_usage },
                );
                let node = GenericUpdatedTrieNode::Extension {
                    extension,
                    child: GenericNodeOrIndex::Updated(child_id),
                };
                let memory_usage = memory_usage + node.memory_usage_direct();
                self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize { node, memory_usage });
            }
            // If the child is an extension (which could happen if a branch node
            // is left with only one branch), join the two extensions into one.
            GenericUpdatedTrieNode::Extension {
                extension: child_extension,
                child: inner_child,
            } => {
                let child_extension = NibbleSlice::from_encoded(&child_extension).0;
                let merged_extension = NibbleSlice::from_encoded(&extension)
                    .0
                    .merge_encoded(&child_extension, false)
                    .into_vec()
                    .into_boxed_slice();
                let node = GenericUpdatedTrieNode::Extension {
                    extension: merged_extension,
                    child: inner_child,
                };
                let memory_usage = node.memory_usage_direct() + child_child_memory_usage;
                self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize { node, memory_usage });
            }
        }
        Ok(())
    }
}

impl<'a, N, V, T> GenericTrieUpdateSquash<'a, N, V> for T
where
    N: std::fmt::Debug,
    V: std::fmt::Debug + HasValueLength,
    T: GenericTrieUpdate<'a, N, V>,
{
}
