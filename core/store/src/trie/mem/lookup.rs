use smallvec::SmallVec;

use super::arena::ArenaMemory;
use super::flexible_data::value::ValueView;
use super::metrics::MEMTRIE_NUM_LOOKUPS;
use super::node::{MemTrieNodePtr, MemTrieNodeView};
use crate::NibbleSlice;
use crate::trie::trie_recording::RecordedNodeId;
use std::sync::Arc;

/// If `nodes_accessed` is provided, each trie node along the lookup path
/// will be added to the vector as (node hash, serialized `RawTrieNodeWithSize`).
/// Even if the key is not found, the nodes that were accessed to make that
/// determination will be added to the vector.
pub fn memtrie_lookup<'a, M: ArenaMemory>(
    root: MemTrieNodePtr<'a, M>,
    key: &[u8],
    mut nodes_accessed: Option<&mut Vec<(RecordedNodeId, Arc<[u8]>)>>,
) -> Option<ValueView<'a>> {
    MEMTRIE_NUM_LOOKUPS.inc();
    let mut nibbles = NibbleSlice::new(key);
    let mut node = root;
    let mut cur_path_length = 0;

    loop {
        let view = node.view();
        if let Some(nodes_accessed) = &mut nodes_accessed {
            let raw_node_serialized = borsh::to_vec(&view.to_raw_trie_node_with_size()).unwrap();
            // todo - memtrie node id?
            let record_id = match view.node_hash_if_available() {
                Some(hash) => RecordedNodeId::Hash(hash),
                None => RecordedNodeId::TriePath(if cur_path_length % 2 == 0 {
                    key[..cur_path_length / 2].into()
                } else {
                    let mut vec: SmallVec<[u8; 32]> = key[..cur_path_length / 2].into();
                    vec.push(NibbleSlice::new(key).at(cur_path_length - 1));
                    vec
                }),
            };
            nodes_accessed.push((record_id, raw_node_serialized.into()));
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
                    cur_path_length += extension_nibbles.len();
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
                cur_path_length += 1;
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
                cur_path_length += 1;
            }
        }
    }
}
