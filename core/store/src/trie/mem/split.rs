use super::arena::ArenaMemory;
use crate::NibbleSlice;
use crate::trie::mem::flexible_data::children::ChildrenView;
use crate::trie::mem::node::{MemTrieNodePtr, MemTrieNodeView};
use itertools::Itertools;
use near_primitives::trie_key::col::{ACCESS_KEY, ACCOUNT, CONTRACT_CODE, CONTRACT_DATA};
use near_primitives::types::AccountId;
use smallvec::SmallVec;

const MAX_NIBBLES: usize = AccountId::MAX_LEN * 2;
const SUBTREES: [u8; 4] = [ACCOUNT, CONTRACT_CODE, ACCESS_KEY, CONTRACT_DATA];
const NUM_CHILDREN: usize = 16;

#[derive(Debug)]
enum TrieDescentStage<'a, M: ArenaMemory> {
    CutOff,
    AtExtension {
        memory_usage: u64,
        remaining_nibbles: SmallVec<[u8; MAX_NIBBLES]>, // non-empty (!)
        child: Option<MemTrieNodePtr<'a, M>>,
    },
    AtBranch {
        memory_usage: u64,
        children: ChildrenView<'a, M>,
    },
}

impl<'a, M: ArenaMemory> From<Option<MemTrieNodePtr<'a, M>>> for TrieDescentStage<'a, M> {
    fn from(value: Option<MemTrieNodePtr<'a, M>>) -> Self {
        let Some(ptr) = value else { return Self::CutOff };
        let node = ptr.view();
        match node {
            MemTrieNodeView::Leaf { extension, .. } => Self::AtExtension {
                memory_usage: node.memory_usage(),
                remaining_nibbles: extension_to_nibbles(extension),
                child: None,
            },
            MemTrieNodeView::Extension { memory_usage, extension, child, .. } => {
                Self::AtExtension {
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

fn extension_to_nibbles(extension: &[u8]) -> SmallVec<[u8; MAX_NIBBLES]> {
    let (nibble_slice, _) = NibbleSlice::from_encoded(extension);
    nibble_slice.iter().collect()
}

impl<'a, M: ArenaMemory> TrieDescentStage<'a, M> {
    fn current_node_memory_usage(&self) -> u64 {
        match self {
            Self::CutOff => 0,
            Self::AtExtension { memory_usage, .. } => *memory_usage,
            Self::AtBranch { memory_usage, .. } => *memory_usage,
        }
    }

    fn children_memory_usage(&self) -> [u64; NUM_CHILDREN] {
        match self {
            Self::CutOff => [0; NUM_CHILDREN],
            Self::AtExtension { memory_usage, remaining_nibbles, child } => {
                let child_mem_usage = if remaining_nibbles.len() > 1 {
                    // if we're in the middle of an extension, 'child' is the current node itself
                    *memory_usage
                } else {
                    child.map(|ptr| ptr.view().memory_usage()).unwrap_or_default()
                };
                let next_nibble = remaining_nibbles[0];
                let mut result = [0; NUM_CHILDREN];
                result[next_nibble as usize] = child_mem_usage;
                result
            }
            Self::AtBranch { children, .. } => (0..NUM_CHILDREN)
                .map(|i| children.get(i).map(|ptr| ptr.view().memory_usage()).unwrap_or_default())
                .collect_vec()
                .try_into()
                .unwrap(),
        }
    }

    fn descend(mut self, nibble: u8) -> Self {
        match &mut self {
            Self::CutOff => self,
            Self::AtExtension { remaining_nibbles, child, .. } => {
                if remaining_nibbles.len() == 1 && remaining_nibbles[0] == nibble {
                    (*child).into()
                } else if remaining_nibbles[0] == nibble {
                    remaining_nibbles.remove(0);
                    self
                } else {
                    Self::CutOff
                }
            }
            Self::AtBranch { children, .. } => children.get(nibble as usize).into(),
        }
    }
}

#[derive(Debug)]
struct TrieDescent<'a, M: ArenaMemory> {
    /// Descent stage of each subtree
    subtree_stages: SmallVec<[TrieDescentStage<'a, M>; SUBTREES.len()]>,
    /// Nibbles walked so far (path from subtree root to current node)
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

        let children = match root.view() {
            MemTrieNodeView::Branch { children, .. } => children,
            _ => panic!("root pointer is not a branch: {root:?}"),
        };

        for subtree_key in SUBTREES {
            let subtree = children.get(subtree_key as usize);
            let subtree_stage: TrieDescentStage<M> = subtree.into();
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

    /// Find the key (nibbles) which splits the trie into two parts with possibly equal
    /// memory usage. Returns the key and the memory usage of the left and right part.
    pub fn find_mem_usage_split(mut self) -> TrieSplit {
        while self.middle_memory > 0 {
            self.descend_step();
        }
        TrieSplit::new(self.nibbles, self.left_memory, self.right_memory)
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

    fn descend_step(&mut self) {
        let children_mem_usage = self.aggregate_children_mem_usage();

        // Find the middle child
        let threshold = self.total_memory() / 2 - self.left_memory;
        let middle_child = find_middle_child(&children_mem_usage, threshold);
        let Some((middle_child, left_memory)) = middle_child else {
            // If no middle child can be found, we have reached the end of the search.
            // `middle_memory` is added to `right_memory`, because the boundary belongs to the right part.
            self.right_memory += self.middle_memory;
            self.middle_memory = 0;
            return;
        };

        // Update left, right, and middle memory
        let middle_child_memory = children_mem_usage[middle_child];
        self.left_memory += left_memory;
        self.right_memory += self.middle_memory - left_memory - middle_child_memory;
        self.middle_memory = middle_child_memory;

        // Update descent stages for all subtrees
        let subtree_stages = std::mem::take(&mut self.subtree_stages);
        self.subtree_stages =
            subtree_stages.into_iter().map(|subtree| subtree.descend(middle_child as u8)).collect();
    }
}

/// Find the lowest child index `i` fox which `sum(children_mem_usage[..i+1]) >= threshold`.
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
        self.split_path_nibbles
            .chunks_exact(2)
            .skip(1) // Skip the prefix
            .map(|pair| (pair[0] << 4) | pair[1])
            .collect()
    }
}

pub fn find_trie_split<M: ArenaMemory>(trie_ptr: MemTrieNodePtr<M>) -> TrieSplit {
    TrieDescent::new(trie_ptr).find_mem_usage_split()
}
