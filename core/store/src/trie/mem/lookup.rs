use super::arena::ArenaMemory;
use super::flexible_data::value::ValueView;
use super::metrics::MEMTRIE_NUM_LOOKUPS;
use super::node::{MemTrieNodePtr, MemTrieNodeView};
use crate::NibbleSlice;

/// If `nodes_accessed` is provided, each trie node along the lookup path
/// will be added to the vector as (node hash, serialized `RawTrieNodeWithSize`).
/// Even if the key is not found, the nodes that were accessed to make that
/// determination will be added to the vector.
pub fn memtrie_lookup<'a, M: ArenaMemory>(
    root: MemTrieNodePtr<'a, M>,
    key: &[u8],
    mut nodes_accessed: Option<&mut Vec<MemTrieNodeView<'a, M>>>,
) -> Option<ValueView<'a>> {
    MEMTRIE_NUM_LOOKUPS.inc();
    let mut nibbles = NibbleSlice::new(key);
    let mut node = root;

    loop {
        let view = node.view();
        if let Some(nodes_accessed) = &mut nodes_accessed {
            nodes_accessed.push(view.clone());
        }
        match view {
            MemTrieNodeView::Leaf { extension, value } => {
                if nibbles == NibbleSlice::from_encoded(extension).0 {
                    return Some(value);
                } else {
                    return None;
                }
            }
            MemTrieNodeView::Extension { extension, child, .. } => {
                let extension_nibbles = NibbleSlice::from_encoded(extension).0;
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
