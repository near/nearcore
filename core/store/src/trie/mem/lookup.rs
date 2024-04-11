use std::sync::Arc;

use super::flexible_data::value::ValueView;
use super::metrics::MEM_TRIE_NUM_LOOKUPS;
use super::node::{MemTrieNodePtr, MemTrieNodeView};
use crate::NibbleSlice;
use near_primitives::hash::CryptoHash;

/// Performs a lookup in an in-memory trie, while taking care of cache
/// accounting for gas calculation purposes.
///
/// If `nodes_accessed` is provided, each trie node along the lookup path
/// will be added to the vector as (node hash, serialized `RawTrieNodeWithSize`).
/// Even if the key is not found, the nodes that were accessed to make that
/// determination will be added to the vector.
pub fn memtrie_lookup<'a>(
    root: MemTrieNodePtr<'a>,
    key: &[u8],
    mut nodes_accessed: Option<&mut Vec<(CryptoHash, Arc<[u8]>)>>,
) -> Option<ValueView<'a>> {
    MEM_TRIE_NUM_LOOKUPS.inc();
    let mut nibbles = NibbleSlice::new(key);
    let mut node = root;

    loop {
        let view = node.view();
        if let Some(nodes_accessed) = &mut nodes_accessed {
            let raw_node_serialized = borsh::to_vec(&view.to_raw_trie_node_with_size()).unwrap();
            nodes_accessed.push((view.node_hash(), raw_node_serialized.into()));
        }
        match view {
            MemTrieNodeView::Leaf { extension, value } => {
                if nibbles == NibbleSlice::from_encoded(extension.raw_slice()).0 {
                    return Some(value);
                } else {
                    return None;
                }
            }
            MemTrieNodeView::Extension { extension, child, .. } => {
                let extension_nibbles = NibbleSlice::from_encoded(extension.raw_slice()).0;
                if nibbles.starts_with(&extension_nibbles) {
                    nibbles = nibbles.mid(extension_nibbles.len());
                    node = child;
                } else {
                    return None;
                }
            }
            MemTrieNodeView::Branch { children, .. } => {
                if nibbles.is_empty() {
                    return None;
                }
                let first = nibbles.at(0);
                nibbles = nibbles.mid(1);
                node = match children.get(first as usize) {
                    Some(child) => child,
                    None => return None,
                };
            }
            MemTrieNodeView::BranchWithValue { children, value, .. } => {
                if nibbles.is_empty() {
                    return Some(value);
                }
                let first = nibbles.at(0);
                nibbles = nibbles.mid(1);
                node = match children.get(first as usize) {
                    Some(child) => child,
                    None => return None,
                };
            }
        }
    }
}
