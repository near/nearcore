use super::arena::ArenaMemory;
use crate::NibbleSlice;
use crate::trie::mem::flexible_data::children::ChildrenView;
use crate::trie::mem::node::{MemTrieNodePtr, MemTrieNodeView};
use itertools::Itertools;
use near_primitives::trie_key::col::{ACCESS_KEY, ACCOUNT, CONTRACT_CODE, CONTRACT_DATA};
use near_primitives::types::AccountId;
use smallvec::SmallVec;

const MAX_NIBBLES: usize = AccountId::MAX_LEN * 2;
const SUBTRIES: [u8; 4] = [ACCOUNT, CONTRACT_CODE, ACCESS_KEY, CONTRACT_DATA];

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

    fn children_memory_usage(&self) -> [u64; 16] {
        match self {
            Self::CutOff => [0; 16],
            Self::AtExtension { memory_usage, remaining_nibbles, child } => {
                let child_mem_usage = if remaining_nibbles.len() > 1 {
                    // if we're in the middle of an extension, 'child' is the current node itself
                    *memory_usage
                } else {
                    child.map(|ptr| ptr.view().memory_usage()).unwrap_or_default()
                };
                let next_nibble = remaining_nibbles[0];
                let mut result = [0; 16];
                result[next_nibble as usize] = child_mem_usage;
                result
            }
            Self::AtBranch { children, .. } => (0..16)
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
    /// Descent stage of each subtrie
    subtrie_stages: SmallVec<[TrieDescentStage<'a, M>; SUBTRIES.len()]>,
    /// Nibbles walked so far (path from subtrie root to current node)
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
        let mut subtrie_stages = SmallVec::new();
        let mut middle_memory = 0;

        let children = match root.view() {
            MemTrieNodeView::Branch { children, .. } => children,
            _ => panic!("root pointer is not a branch: {root:?}"),
        };

        for subtrie_key in SUBTRIES {
            let subtrie = children.get(subtrie_key as usize);
            middle_memory += subtrie.map(|ptr| ptr.view().memory_usage()).unwrap_or_default();
            subtrie_stages.push(subtrie.into());
        }

        Self {
            subtrie_stages,
            nibbles: SmallVec::new(),
            left_memory: 0,
            right_memory: 0,
            middle_memory,
        }
    }

    /// Find the key (nibbles) which splits the trie into two parts with possibly equal
    /// memory usage. Returns the key and the memory usage of the left and right part.
    pub fn find_mem_usage_split(mut self) -> (SmallVec<[u8; MAX_NIBBLES]>, u64, u64) {
        while self.middle_memory > 0 {
            self.descend_step();
        }
        (self.nibbles, self.left_memory, self.right_memory)
    }

    fn descend_step(&mut self) {
        todo!()
    }
}
