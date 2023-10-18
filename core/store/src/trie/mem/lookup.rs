use super::node::{MemTrieNodePtr, MemTrieNodeView};
use crate::NibbleSlice;
use near_primitives::hash::CryptoHash;
use near_primitives::state::FlatStateValue;
use near_vm_runner::logic::TrieNodesCount;
use std::collections::HashSet;

/// Performs a lookup in an in-memory trie, while taking care of cache
/// accounting for gas calculation purposes.
pub fn memtrie_lookup(
    root: MemTrieNodePtr<'_>,
    key: &[u8],
    mut accessed_cache: Option<&mut HashSet<CryptoHash>>,
    nodes_count: &mut TrieNodesCount,
) -> Option<FlatStateValue> {
    let mut nibbles = NibbleSlice::new(key);
    let mut node = root;

    loop {
        // This logic is carried from on-disk trie. It must remain unchanged,
        // because gas accounting is dependent on these counters.
        if let Some(cache) = &mut accessed_cache {
            if cache.insert(node.view().node_hash()) {
                nodes_count.db_reads += 1;
            } else {
                nodes_count.mem_reads += 1;
            }
        } else {
            nodes_count.db_reads += 1;
        }
        match node.view() {
            MemTrieNodeView::Leaf { extension, value } => {
                if nibbles == NibbleSlice::from_encoded(extension.raw_slice()).0 {
                    return Some(value.to_flat_value());
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
                    return Some(value.to_flat_value());
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
