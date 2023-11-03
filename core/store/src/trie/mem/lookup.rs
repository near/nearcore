use super::metrics::MEM_TRIE_NUM_LOOKUPS;
use super::node::{MemTrieNodePtr, MemTrieNodeView};
use crate::NibbleSlice;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state::FlatStateValue;

/// Performs a lookup in an in-memory trie, while taking care of cache
/// accounting for gas calculation purposes.
///
/// The provided callback is invoked for:
///  - each node accessed during the lookup, with the parameters
///    (false, node hash, borsh serialized node), as well as
///  - once for the value, if the value is inlined, with the parameters
///    (true, value hash, the value).
///
/// The callback is used for:
///  - Gas accounting of touched nodes and values
///  - Populating the trie accounting cache (for legacy compatibility)
///  - Recording accessed nodes and values, for state witness production.
pub fn memtrie_lookup(
    root: MemTrieNodePtr<'_>,
    key: &[u8],
    mut node_access_callback: impl FnMut(bool, CryptoHash, Vec<u8>),
) -> Option<FlatStateValue> {
    MEM_TRIE_NUM_LOOKUPS.inc();
    let mut nibbles = NibbleSlice::new(key);
    let mut node = root;

    let result = loop {
        let view = node.view();
        let raw_node_serialized = borsh::to_vec(&view.to_raw_trie_node_with_size()).unwrap();
        node_access_callback(false, view.node_hash(), raw_node_serialized);
        match view {
            MemTrieNodeView::Leaf { extension, value } => {
                if nibbles == NibbleSlice::from_encoded(extension.raw_slice()).0 {
                    break value.to_flat_value();
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
                    break value.to_flat_value();
                }
                let first = nibbles.at(0);
                nibbles = nibbles.mid(1);
                node = match children.get(first as usize) {
                    Some(child) => child,
                    None => return None,
                };
            }
        }
    };
    if let FlatStateValue::Inlined(value) = &result {
        node_access_callback(true, hash(value), value.clone());
    }
    Some(result)
}
