use super::arena::ArenaMemory;
use crate::NibbleSlice;
use crate::trie::NUM_CHILDREN;
use crate::trie::mem::flexible_data::children::ChildrenView;
use crate::trie::mem::node::{MemTrieNodePtr, MemTrieNodeView};
use derive_where::derive_where;
use itertools::Itertools;
use near_primitives::trie_key::col::{ACCESS_KEY, ACCOUNT, CONTRACT_CODE, CONTRACT_DATA};
use near_primitives::types::AccountId;
use smallvec::SmallVec;

const MAX_NIBBLES: usize = AccountId::MAX_LEN * 2;
// The order of subtrees matters - accounts must go first (!)
// We don't want to go deeper into other subtrees when reaching a leaf in the accounts' tree.
// Chunks are split by account IDs, not arbitrary byte sequences.
const SUBTREES: [u8; 4] = [ACCOUNT, CONTRACT_CODE, ACCESS_KEY, CONTRACT_DATA];

/// This represents a descent stage of a single subtree (e.g. accounts or access keys)
/// in a descent that involves multiple subtrees.
#[derive_where(Debug)]
enum TrieDescentStage<'a, M: ArenaMemory> {
    /// The current descent path is not present in this subtree. Either we have descended
    /// below a leaf or took a branch that was not consistent with an extension in this subtree.
    CutOff,
    /// The current descent path leads to a leaf node (and includes all extension nibbles from
    /// that node if it has any).
    AtLeaf { memory_usage: u64 },
    /// The current descent path leads to an extension node (or leaf node with an extension),
    /// possibly including some (but not all) nibbles belonging to that extension.  
    InsideExtension {
        memory_usage: u64,
        remaining_nibbles: SmallVec<[u8; MAX_NIBBLES]>, // non-empty (!)
        child: Option<MemTrieNodePtr<'a, M>>,
    },
    /// The current descent path leads to a branch node.
    AtBranch { memory_usage: u64, children: ChildrenView<'a, M> },
}

impl<'a, M: ArenaMemory> From<MemTrieNodePtr<'a, M>> for TrieDescentStage<'a, M> {
    fn from(ptr: MemTrieNodePtr<'a, M>) -> Self {
        let node = ptr.view();
        match node {
            MemTrieNodeView::Leaf { extension, .. } if extension.is_empty() => {
                Self::AtLeaf { memory_usage: node.memory_usage() }
            }
            MemTrieNodeView::Leaf { extension, .. } => Self::InsideExtension {
                memory_usage: node.memory_usage(),
                remaining_nibbles: extension_to_nibbles(extension),
                child: None,
            },
            MemTrieNodeView::Extension { memory_usage, extension, child, .. } => {
                Self::InsideExtension {
                    memory_usage,
                    remaining_nibbles: extension_to_nibbles(extension),
                    child: Some(child),
                }
            }
            MemTrieNodeView::Branch { memory_usage, children, .. }
            | MemTrieNodeView::BranchWithValue { memory_usage, children, .. } => {
                Self::AtBranch { memory_usage, children }
            }
        }
    }
}

impl<'a, M: ArenaMemory> From<Option<MemTrieNodePtr<'a, M>>> for TrieDescentStage<'a, M> {
    fn from(value: Option<MemTrieNodePtr<'a, M>>) -> Self {
        match value {
            None => Self::CutOff,
            Some(ptr) => ptr.into(),
        }
    }
}

fn extension_to_nibbles(extension: &[u8]) -> SmallVec<[u8; MAX_NIBBLES]> {
    let (nibble_slice, _) = NibbleSlice::from_encoded(extension);
    nibble_slice.iter().collect()
}

impl<'a, M: ArenaMemory> TrieDescentStage<'a, M> {
    fn can_descend(&self) -> bool {
        match self {
            Self::CutOff | Self::AtLeaf { .. } => false,
            Self::InsideExtension { .. } | Self::AtBranch { .. } => true,
        }
    }

    fn current_node_memory_usage(&self) -> u64 {
        match self {
            Self::CutOff => 0,
            Self::AtLeaf { memory_usage, .. }
            | Self::InsideExtension { memory_usage, .. }
            | Self::AtBranch { memory_usage, .. } => *memory_usage,
        }
    }

    fn children_memory_usage(&self) -> [u64; NUM_CHILDREN] {
        match self {
            Self::CutOff | Self::AtLeaf { .. } => [0; NUM_CHILDREN],
            Self::InsideExtension { memory_usage, remaining_nibbles, .. } => {
                let next_nibble = remaining_nibbles[0];
                let mut result = [0; NUM_CHILDREN];
                result[next_nibble as usize] = *memory_usage;
                result
            }
            Self::AtBranch { children, .. } => (0..NUM_CHILDREN)
                .map(|i| children.get(i).map(|ptr| ptr.view().memory_usage()).unwrap_or_default())
                .collect_vec()
                .try_into()
                .unwrap(),
        }
    }

    fn descend(&mut self, nibble: u8) {
        tracing::trace!(target = "memtrie", ?self, %nibble, "descending");
        match self {
            Self::CutOff => {}
            Self::AtLeaf { .. } => *self = Self::CutOff,
            Self::InsideExtension { memory_usage, remaining_nibbles, child } => {
                if remaining_nibbles.get(0).is_some_and(|x| *x == nibble) {
                    remaining_nibbles.remove(0);
                } else {
                    *self = Self::CutOff;
                    return;
                }

                if remaining_nibbles.is_empty() {
                    match child {
                        None => *self = Self::AtLeaf { memory_usage: *memory_usage },
                        Some(child) => *self = (*child).into(),
                    }
                }
            }
            Self::AtBranch { children, .. } => *self = children.get(nibble as usize).into(),
        }
    }
}

/// This struct is used to find an account ID which splits a state trie into two, possibly even
/// parts, according to the `memory_usage` stored in nodes. It descends all `SUBTREES`
/// simultaneously, choosing a path which provides the best split (binary search).
#[derive(Debug)]
struct TrieDescent<'a, M: ArenaMemory> {
    /// Descent stage of each subtree
    subtree_stages: SmallVec<[TrieDescentStage<'a, M>; SUBTREES.len()]>,
    /// Nibbles walked so far (path from subtree root to current node)
    /// Doesn't include the first byte, which identifies the subtree.
    nibbles: SmallVec<[u8; MAX_NIBBLES]>,
    /// Total memory usage of nodes that will go into the left part
    left_memory: u64,
    /// Total memory usage of nodes that will go into the right part
    right_memory: u64,
    /// Total memory usage of the trie part that is left to split
    middle_memory: u64,
}

impl<'a, M: ArenaMemory> TrieDescent<'a, M> {
    pub fn new(root: MemTrieNodePtr<'a, M>) -> Self {
        let mut subtree_stages = SmallVec::new();
        let mut middle_memory = 0;

        for subtree_key in SUBTREES {
            let mut subtree_stage: TrieDescentStage<M> = root.into();
            let (nib1, nib2) = byte_to_nibbles(subtree_key);
            subtree_stage.descend(nib1);
            subtree_stage.descend(nib2);
            middle_memory += subtree_stage.current_node_memory_usage();
            subtree_stages.push(subtree_stage);
        }

        Self {
            subtree_stages,
            nibbles: SmallVec::new(),
            left_memory: 0,
            right_memory: 0,
            middle_memory,
        }
    }

    fn total_memory(&self) -> u64 {
        self.left_memory + self.right_memory + self.middle_memory
    }

    /// Aggregate children memory usage across all subtrees
    fn aggregate_children_mem_usage(&self) -> [u64; NUM_CHILDREN] {
        let mut children_mem_usage = [0u64; NUM_CHILDREN];
        for subtree in &self.subtree_stages {
            let subtree_children = subtree.children_memory_usage();
            for i in 0..NUM_CHILDREN {
                children_mem_usage[i] += subtree_children[i];
            }
        }
        children_mem_usage
    }

    /// Find the key (nibbles) which splits the trie into two parts with possibly equal
    /// memory usage. Returns the key and the memory usage of the left and right part.
    pub fn find_mem_usage_split(mut self) -> TrieSplit {
        while let Some((nibble, child_mem_usage, left_mem_usage)) = self.next_step() {
            self.descend_step(nibble, child_mem_usage, left_mem_usage);
        }

        // `middle_memory` is added to `right_memory`, because the boundary belongs to the right part.
        TrieSplit::new(self.nibbles, self.left_memory, self.right_memory + self.middle_memory)
    }

    /// Find the next step `(nibble, child_mem_usage, left_mem_usage)`.
    ///     * `nibble` – next nibble on the path (index of the middle child node)
    ///     * `child_mem_usage` – memory usage of the middle child
    ///     * `left_mem_usage` – total memory usage of left siblings of the middle child
    /// Returns `None` if the end of the searched is reached.
    fn next_step(&self) -> Option<(u8, u64, u64)> {
        // Stop when a leaf is reached in the accounts subtree
        if !self.subtree_stages[0].can_descend() {
            tracing::debug!(target = "memtrie", "leaf reached in accounts subtree");
            return None;
        }

        // Total memory is constant. Left memory is initially 0. With every step, left memory
        // is increased by the amount returned by `find_middle_child`, which is strictly lower
        // than the threshold. Therefore, the threshold will always be >= 0.
        debug_assert!(self.total_memory() / 2 >= self.left_memory);
        let threshold = self.total_memory() / 2 - self.left_memory;

        let children_mem_usage = self.aggregate_children_mem_usage();

        // Find the middle child
        tracing::debug!(target = "memtrie", ?children_mem_usage, %threshold, "finding middle child");
        let (middle_child, left_mem_usage) = find_middle_child(&children_mem_usage, threshold)?;
        let child_mem_usage = children_mem_usage[middle_child];
        tracing::debug!(target = "memtrie", %middle_child, %child_mem_usage, %left_mem_usage, "middle child found");

        Some((middle_child as u8, child_mem_usage, left_mem_usage))
    }

    fn descend_step(&mut self, nibble: u8, child_mem_usage: u64, left_mem_usage: u64) {
        // Update left, right, and middle memory
        self.left_memory += left_mem_usage;
        self.right_memory += self.middle_memory - left_mem_usage - child_mem_usage;
        self.middle_memory = child_mem_usage;
        tracing::debug!(target = "memtrie", %self.left_memory, %self.right_memory, %self.middle_memory, "remaining memory updated");

        // Update descent stages for all subtrees
        for stage in &mut self.subtree_stages {
            stage.descend(nibble);
        }
        self.nibbles.push(nibble);
        tracing::debug!(target = "memtrie", ?self.nibbles, nibble_str = String::from_utf8_lossy(&nibbles_to_bytes(&self.nibbles)).to_string(), "nibbles updated");
    }
}

/// Find the lowest child index `i` for which `sum(children_mem_usage[..i+1]) >= threshold`.
/// Returns `Some(i, sum(children_mem_usage[..i])` if such `i` exists, otherwise `None`.
fn find_middle_child(
    children_mem_usage: &[u64; NUM_CHILDREN],
    threshold: u64,
) -> Option<(usize, u64)> {
    let mut left_memory = 0u64;
    for i in 0..NUM_CHILDREN {
        if left_memory + children_mem_usage[i] >= threshold {
            return Some((i, left_memory));
        }
        left_memory += children_mem_usage[i];
    }
    None
}

/// The result of splitting a memtrie into two possibly even parts, according to `memory_usage`
/// stored in the trie nodes.
///
/// **NOTE: This is an artificial value calculated according to `TRIE_COST`. Hence, it does not
/// represent actual memory allocation, but the split ratio should be roughly consistent with that.**
#[derive(Debug, Clone)]
pub struct TrieSplit {
    /// Nibbles making up the path which splits the trie
    pub split_path_nibbles: SmallVec<[u8; MAX_NIBBLES]>,
    /// Total `memory_usage` of the left part (excluding the split path)
    pub left_memory: u64,
    /// Total `memory_usage` of the right part (including the split path)
    pub right_memory: u64,
}

impl TrieSplit {
    pub fn new(
        split_path_nibbles: SmallVec<[u8; MAX_NIBBLES]>,
        left_memory: u64,
        right_memory: u64,
    ) -> Self {
        debug_assert!(split_path_nibbles.len() % 2 == 0);
        Self { split_path_nibbles, left_memory, right_memory }
    }

    /// Get the split path as bytes
    pub fn split_path_bytes(&self) -> Vec<u8> {
        nibbles_to_bytes(&self.split_path_nibbles)
    }
}

fn byte_to_nibbles(byte: u8) -> (u8, u8) {
    (byte >> 4, byte & 0x0F)
}

fn nibbles_to_bytes(nibbles: &[u8]) -> Vec<u8> {
    nibbles.chunks_exact(2).map(|pair| (pair[0] << 4) | pair[1]).collect()
}

pub fn find_trie_split<M: ArenaMemory>(trie_ptr: MemTrieNodePtr<M>) -> TrieSplit {
    TrieDescent::new(trie_ptr).find_mem_usage_split()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn middle_child() {
        let children_mem_usage = [0u64; NUM_CHILDREN];
        let threshold = 10;
        assert_eq!(find_middle_child(&children_mem_usage, threshold), None);

        let children_mem_usage = [100u64; NUM_CHILDREN];
        let threshold = 2000;
        assert_eq!(find_middle_child(&children_mem_usage, threshold), None);

        let threshold = 1050;
        assert_eq!(find_middle_child(&children_mem_usage, threshold), Some((10, 1000)));
    }
}
