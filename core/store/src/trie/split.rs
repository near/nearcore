use crate::trie::iterator::DiskTrieIteratorInner;
use crate::trie::mem::iter::MemTrieIteratorInner;
use crate::trie::ops::interface::{
    GenericTrieInternalStorage, GenericTrieNode, GenericTrieNodeWithSize,
};
use crate::trie::{AccessOptions, NUM_CHILDREN};
use crate::{NibbleSlice, Trie};
use derive_where::derive_where;
use itertools::Itertools;
use near_primitives::trie_key::col::{ACCESS_KEY, ACCOUNT, CONTRACT_CODE, CONTRACT_DATA};
use near_primitives::types::AccountId;
use smallvec::SmallVec;
use std::fmt::Debug;
use std::marker::PhantomData;

const MAX_NIBBLES: usize = AccountId::MAX_LEN * 2;
// The order of subtrees matters - accounts must go first (!)
// We don't want to go deeper into other subtrees when reaching a leaf in the accounts' tree.
// Chunks are split by account IDs, not arbitrary byte sequences. Therefore, also make sure
// not to add any subtrees here that do not use account ID as key (or key prefix).
const SUBTREES: [u8; 4] = [ACCOUNT, CONTRACT_CODE, ACCESS_KEY, CONTRACT_DATA];

/// This represents a descent stage of a single subtree (e.g. accounts or access keys)
/// in a descent that involves multiple subtrees.
#[derive_where(Debug)]
enum TrieDescentStage<NodePtr: Debug> {
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
        child: Option<NodePtr>,
    },
    /// The current descent path leads to a branch node.
    AtBranch { memory_usage: u64, children: Box<[Option<NodePtr>; NUM_CHILDREN]> },
}

impl<NodePtr: Debug, Value> From<GenericTrieNodeWithSize<NodePtr, Value>>
    for TrieDescentStage<NodePtr>
{
    fn from(node: GenericTrieNodeWithSize<NodePtr, Value>) -> Self {
        let memory_usage = node.memory_usage;
        match node.node {
            GenericTrieNode::Empty => Self::CutOff,
            GenericTrieNode::Leaf { extension, .. } if extension.is_empty() => {
                Self::AtLeaf { memory_usage }
            }
            GenericTrieNode::Leaf { extension, .. } => Self::InsideExtension {
                memory_usage,
                remaining_nibbles: extension_to_nibbles(&extension),
                child: None,
            },
            GenericTrieNode::Extension { extension, child } => Self::InsideExtension {
                memory_usage,
                remaining_nibbles: extension_to_nibbles(&extension),
                child: Some(child),
            },
            GenericTrieNode::Branch { children, .. } => Self::AtBranch { memory_usage, children },
        }
    }
}

impl<NodePtr: Debug, Value> From<Option<GenericTrieNodeWithSize<NodePtr, Value>>>
    for TrieDescentStage<NodePtr>
{
    fn from(value: Option<GenericTrieNodeWithSize<NodePtr, Value>>) -> Self {
        match value {
            None => Self::CutOff,
            Some(node) => node.into(),
        }
    }
}

fn extension_to_nibbles(extension: &[u8]) -> SmallVec<[u8; MAX_NIBBLES]> {
    let (nibble_slice, _) = NibbleSlice::from_encoded(extension);
    nibble_slice.iter().collect()
}

impl<NodePtr: Debug + Copy> TrieDescentStage<NodePtr> {
    fn current_node_memory_usage(&self) -> u64 {
        match self {
            Self::CutOff => 0,
            Self::AtLeaf { memory_usage, .. }
            | Self::InsideExtension { memory_usage, .. }
            | Self::AtBranch { memory_usage, .. } => *memory_usage,
        }
    }

    fn children_memory_usage<Value, Getter>(&self, get_node: Getter) -> [u64; NUM_CHILDREN]
    where
        Getter: Fn(NodePtr) -> GenericTrieNodeWithSize<NodePtr, Value>,
    {
        match self {
            Self::CutOff | Self::AtLeaf { .. } => [0; NUM_CHILDREN],
            Self::InsideExtension { memory_usage, remaining_nibbles, .. } => {
                let next_nibble = remaining_nibbles[0];
                let mut result = [0; NUM_CHILDREN];
                result[next_nibble as usize] = *memory_usage;
                result
            }
            Self::AtBranch { children, .. } => children
                .iter()
                .map(|child| child.map(|child| get_node(child).memory_usage).unwrap_or_default())
                .collect_vec()
                .try_into()
                .unwrap(),
        }
    }

    fn can_descend(&self) -> bool {
        match self {
            Self::CutOff | Self::AtLeaf { .. } => false,
            Self::InsideExtension { .. } | Self::AtBranch { .. } => true,
        }
    }

    fn first_child(&self) -> Option<u8> {
        match self {
            Self::CutOff | Self::AtLeaf { .. } => None,
            Self::InsideExtension { remaining_nibbles, .. } => Some(remaining_nibbles[0]),
            Self::AtBranch { children, .. } => {
                for i in 0..NUM_CHILDREN {
                    if children[i].is_some() {
                        return Some(i as u8);
                    }
                }
                unreachable!("branch should have at least one child");
            }
        }
    }

    fn descend<Value, Getter>(&mut self, nibble: u8, get_node: Getter)
    where
        Getter: Fn(NodePtr) -> GenericTrieNodeWithSize<NodePtr, Value>,
    {
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
                        Some(child) => *self = get_node(*child).into(),
                    }
                }
            }
            Self::AtBranch { children, .. } => {
                *self = children[nibble as usize].map(get_node).into()
            }
        }
    }
}

/// This struct is used to find an account ID which splits a state trie into two, possibly even
/// parts, according to the `memory_usage` stored in nodes. It descends all `SUBTREES`
/// simultaneously, choosing a path which provides the best split (binary search).
#[derive(Debug)]
struct TrieDescent<NodePtr: Debug, Value, Storage> {
    _phantom: PhantomData<Value>,
    trie_storage: Storage,
    /// Descent stage of each subtree
    subtree_stages: SmallVec<[TrieDescentStage<NodePtr>; SUBTREES.len()]>,
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

impl<NodePtr, Value, Storage> TrieDescent<NodePtr, Value, Storage>
where
    NodePtr: Debug + Copy,
    Storage: GenericTrieInternalStorage<NodePtr, Value>,
{
    pub fn new(trie_storage: Storage) -> Self {
        let root_ptr = trie_storage.get_root().expect("no root in trie");
        let get_node = |ptr| trie_storage.get_node_with_size(ptr, AccessOptions::DEFAULT).unwrap();
        let mut subtree_stages = SmallVec::new();
        let mut middle_memory = 0;

        for subtree_key in SUBTREES {
            let mut subtree_stage: TrieDescentStage<NodePtr> = get_node(root_ptr).into();
            let (nib1, nib2) = byte_to_nibbles(subtree_key);
            subtree_stage.descend(nib1, get_node);
            subtree_stage.descend(nib2, get_node);
            middle_memory += subtree_stage.current_node_memory_usage();
            subtree_stages.push(subtree_stage);
        }

        Self {
            _phantom: PhantomData,
            trie_storage,
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
        let get_node =
            |ptr| self.trie_storage.get_node_with_size(ptr, AccessOptions::DEFAULT).unwrap();
        let mut children_mem_usage = [0u64; NUM_CHILDREN];
        for subtree in &self.subtree_stages {
            let subtree_children = subtree.children_memory_usage(get_node);
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

        // The split path needs to be convertible into an account ID. Therefore, it has to be
        // at least 2 bytes long, and include an even number of nibbles.
        while self.nibbles.len() < AccountId::MIN_LEN * 2 || self.nibbles.len() % 2 != 0 {
            self.force_next_step();
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

    /// Force descent into the first available child. This is done to ensure the correct length
    /// of the split path, even though further descent will not improve the memory usage balance.
    fn force_next_step(&mut self) {
        let children_mem_usage = self.aggregate_children_mem_usage();
        let first_child = self.subtree_stages[0]
            .first_child()
            .expect("no child to force descend – the keys are expected to be account IDs");
        let child_mem_usage = children_mem_usage[first_child as usize];
        let left_mem_usage = children_mem_usage[0..first_child as usize].iter().sum();
        self.descend_step(first_child, child_mem_usage, left_mem_usage);
    }

    fn descend_step(&mut self, nibble: u8, child_mem_usage: u64, left_mem_usage: u64) {
        // Update left, right, and middle memory
        self.left_memory += left_mem_usage;
        self.right_memory += self.middle_memory - left_mem_usage - child_mem_usage;
        self.middle_memory = child_mem_usage;
        tracing::debug!(target = "memtrie", %self.left_memory, %self.right_memory, %self.middle_memory, "remaining memory updated");

        // Update descent stages for all subtrees
        let get_node =
            |ptr| self.trie_storage.get_node_with_size(ptr, AccessOptions::DEFAULT).unwrap();
        for stage in &mut self.subtree_stages {
            stage.descend(nibble, get_node);
        }
        self.nibbles.push(nibble);
        tracing::debug!(target = "memtrie", ?self.nibbles, nibble_str = String::from_utf8_lossy(&nibbles_to_bytes(&self.nibbles)).to_string(), "nibbles updated");
    }
}

/// Find the lowest child index `i` for which `sum(children_mem_usage[..i+1]) > threshold`.
/// Returns `Some(i, sum(children_mem_usage[..i])` if such `i` exists, otherwise `None`.
/// If no such `i` exists, returns the highest `i` for which `children_mem_usage[i] > 0`.
fn find_middle_child(
    children_mem_usage: &[u64; NUM_CHILDREN],
    threshold: u64,
) -> Option<(usize, u64)> {
    let mut left_memory = 0u64;
    for i in 0..NUM_CHILDREN {
        if left_memory + children_mem_usage[i] > threshold {
            return Some((i, left_memory));
        }
        left_memory += children_mem_usage[i];
    }

    // If middle child cannot be found, pick the last child, to cut off as much as possible
    for i in (0..NUM_CHILDREN).rev() {
        if children_mem_usage[i] > 0 {
            left_memory -= children_mem_usage[i];
            return Some((i, left_memory));
        }
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
        debug_assert!(split_path_nibbles.len() >= AccountId::MIN_LEN * 2);
        Self { split_path_nibbles, left_memory, right_memory }
    }

    /// Get the split path as bytes
    pub fn split_path_bytes(&self) -> Vec<u8> {
        nibbles_to_bytes(&self.split_path_nibbles)
    }

    // Parse the split path into `AccountId`. Panics if parsing fails.
    pub fn boundary_account(&self) -> AccountId {
        std::str::from_utf8(&self.split_path_bytes()).unwrap().parse().unwrap()
    }
}

fn byte_to_nibbles(byte: u8) -> (u8, u8) {
    (byte >> 4, byte & 0x0F)
}

fn nibbles_to_bytes(nibbles: &[u8]) -> Vec<u8> {
    nibbles.chunks_exact(2).map(|pair| (pair[0] << 4) | pair[1]).collect()
}

pub fn find_trie_split(trie: &Trie) -> TrieSplit {
    match trie.lock_memtries() {
        Some(memtries) => {
            let trie_storage = MemTrieIteratorInner::new(&memtries, trie);
            TrieDescent::new(trie_storage).find_mem_usage_split()
        }
        None => {
            let trie_storage = DiskTrieIteratorInner::new(trie);
            TrieDescent::new(trie_storage).find_mem_usage_split()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie::TRIE_COSTS;
    use crate::trie::mem::arena::Arena;
    use crate::trie::mem::arena::single_thread::STArena;
    use crate::trie::mem::memtrie_update::MemTrieNodeWithSize;
    use crate::trie::mem::node::{InputMemTrieNode, MemTrieNodeId};
    use assert_matches::assert_matches;
    use near_primitives::state::FlatStateValue;

    // For historical reasons, node_cost for leaves is doubled (?)
    const EMPTY_LEAF_MEM: u64 = TRIE_COSTS.node_cost * 2;

    #[test]
    fn middle_child() {
        let children_mem_usage = [0u64; NUM_CHILDREN];
        let threshold = 10;
        assert_eq!(find_middle_child(&children_mem_usage, threshold), None);

        let children_mem_usage = [100u64; NUM_CHILDREN];
        let threshold = 1000;
        assert_eq!(find_middle_child(&children_mem_usage, threshold), Some((10, 1000)));

        // If middle child cannot be found, the last child should be picked.
        let threshold = 2000;
        assert_eq!(find_middle_child(&children_mem_usage, threshold), Some((15, 1500)));
    }

    #[test]
    fn nibble_ops() {
        assert_eq!(byte_to_nibbles(0x00), (0x00, 0x00));
        assert_eq!(byte_to_nibbles(0x01), (0x00, 0x01));
        assert_eq!(byte_to_nibbles(0x10), (0x01, 0x00));
        assert_eq!(byte_to_nibbles(0x11), (0x01, 0x01));

        assert_eq!(nibbles_to_bytes(&[]), &[] as &[u8]);
        assert_eq!(nibbles_to_bytes(&[0x01]), &[] as &[u8]);
        assert_eq!(nibbles_to_bytes(&[0x00, 0x01]), &[0x01]);
        assert_eq!(nibbles_to_bytes(&[0x01, 0x02, 0x03]), &[0x12]);
    }

    fn new_leaf(arena: &mut STArena, extension_nibbles: &[u8], value: &[u8]) -> MemTrieNodeId {
        let value = FlatStateValue::inlined(value);
        let extension = if extension_nibbles.is_empty() {
            SmallVec::new()
        } else {
            NibbleSlice::encode_nibbles(extension_nibbles, true)
        };
        let input = InputMemTrieNode::Leaf { value: &value, extension: &extension };
        MemTrieNodeId::new(arena, input)
    }

    fn new_extension(
        arena: &mut STArena,
        extension_nibbles: &[u8],
        child: MemTrieNodeId,
    ) -> MemTrieNodeId {
        let extension = NibbleSlice::encode_nibbles(extension_nibbles, false);
        let input = InputMemTrieNode::Extension { extension: &extension, child };
        MemTrieNodeId::new(arena, input)
    }

    fn new_branch(
        arena: &mut STArena,
        value: Option<&[u8]>,
        children: [Option<MemTrieNodeId>; NUM_CHILDREN],
    ) -> MemTrieNodeId {
        let input = match value {
            Some(value) => InputMemTrieNode::BranchWithValue {
                children,
                value: &FlatStateValue::inlined(value),
            },
            None => InputMemTrieNode::Branch { children },
        };
        MemTrieNodeId::new(arena, input)
    }

    /// A convenient trait to avoid boilerplate code when converting `MemTrieNodeId` to `TrieDescentStage`
    trait ToDescentStage {
        fn to_descent_stage(&self, arena: &STArena) -> TrieDescentStage<MemTrieNodeId>;
    }

    impl ToDescentStage for MemTrieNodeId {
        fn to_descent_stage(&self, arena: &STArena) -> TrieDescentStage<MemTrieNodeId> {
            get_node(arena)(*self).into()
        }
    }

    fn get_node_stub<P>(_: P) -> GenericTrieNodeWithSize<P, ()> {
        unreachable!()
    }

    fn get_node(arena: &STArena) -> impl Fn(MemTrieNodeId) -> MemTrieNodeWithSize {
        |node_id| {
            MemTrieNodeWithSize::from_existing_node_view(node_id.as_ptr(arena.memory()).view())
        }
    }

    /// These tests verify memory usage calculation of the current node and children nodes.
    mod mem_usage {
        use super::*;

        #[test]
        fn cut_off() {
            let descent_stage = TrieDescentStage::<()>::CutOff;
            assert_eq!(descent_stage.current_node_memory_usage(), 0);
            assert_eq!(descent_stage.children_memory_usage(get_node_stub), [0u64; NUM_CHILDREN]);
        }

        #[test]
        fn leaf() {
            let mut arena = STArena::new("test".to_string());

            let empty_leaf = new_leaf(&mut arena, &[], &[]).to_descent_stage(&arena);
            assert_eq!(empty_leaf.current_node_memory_usage(), EMPTY_LEAF_MEM);
            assert_eq!(empty_leaf.children_memory_usage(get_node_stub), [0u64; NUM_CHILDREN]);

            let nonempty_leaf = new_leaf(&mut arena, &[], &[1, 2, 3]).to_descent_stage(&arena);
            let exp_memory = EMPTY_LEAF_MEM + TRIE_COSTS.byte_of_value * 3;
            assert_eq!(nonempty_leaf.current_node_memory_usage(), exp_memory);
            assert_eq!(nonempty_leaf.children_memory_usage(get_node_stub), [0u64; NUM_CHILDREN]);

            let extension_leaf = new_leaf(&mut arena, &[1, 2], &[]).to_descent_stage(&arena);
            let exp_memory = EMPTY_LEAF_MEM + TRIE_COSTS.byte_of_key * 2;
            let mut exp_children_mem = [0u64; NUM_CHILDREN];
            exp_children_mem[1] = exp_memory; // The first nibble in extension is '1'
            assert_eq!(extension_leaf.current_node_memory_usage(), exp_memory);
            assert_eq!(extension_leaf.children_memory_usage(get_node_stub), exp_children_mem);
        }

        #[test]
        fn extension() {
            let mut arena = STArena::new("test".to_string());

            let empty_leaf = new_leaf(&mut arena, &[], &[]);
            let extension = new_extension(&mut arena, &[4, 5], empty_leaf).to_descent_stage(&arena);

            let exp_memory = EMPTY_LEAF_MEM + TRIE_COSTS.node_cost + TRIE_COSTS.byte_of_key * 2;
            let mut exp_children_mem = [0u64; NUM_CHILDREN];
            exp_children_mem[4] = exp_memory; // The first nibble in extension is '4'
            assert_eq!(extension.current_node_memory_usage(), exp_memory);
            assert_eq!(extension.children_memory_usage(get_node_stub), exp_children_mem);
        }

        #[test]
        fn branch() {
            let mut arena = STArena::new("test".to_string());

            let empty_branch = new_branch(&mut arena, None, [None; 16]).to_descent_stage(&arena);
            assert_eq!(empty_branch.current_node_memory_usage(), TRIE_COSTS.node_cost);
            assert_eq!(empty_branch.children_memory_usage(get_node_stub), [0u64; NUM_CHILDREN]);

            let branch_with_value =
                new_branch(&mut arena, Some(&[1, 2, 3]), [None; 16]).to_descent_stage(&arena);
            // For some reason, node_cost for branch with value is doubled (?)
            let exp_memory = TRIE_COSTS.node_cost * 2 + TRIE_COSTS.byte_of_value * 3;
            assert_eq!(branch_with_value.current_node_memory_usage(), exp_memory);
            assert_eq!(
                branch_with_value.children_memory_usage(get_node_stub),
                [0u64; NUM_CHILDREN]
            );

            let leaf1 = new_leaf(&mut arena, &[], &[1, 2, 3]);
            let leaf2 = new_leaf(&mut arena, &[1, 2], &[1]);
            let leaf1_mem = EMPTY_LEAF_MEM + TRIE_COSTS.byte_of_value * 3;
            let leaf2_mem = EMPTY_LEAF_MEM + TRIE_COSTS.byte_of_value + TRIE_COSTS.byte_of_key * 2;
            let mut children: [Option<MemTrieNodeId>; NUM_CHILDREN] = [None; NUM_CHILDREN];
            children[3] = Some(leaf1);
            children[5] = Some(leaf2);
            let branch_with_children =
                new_branch(&mut arena, None, children).to_descent_stage(&arena);
            let exp_memory = TRIE_COSTS.node_cost + leaf1_mem + leaf2_mem;
            let mut exp_children_mem = [0u64; NUM_CHILDREN];
            exp_children_mem[3] = leaf1_mem;
            exp_children_mem[5] = leaf2_mem;
            assert_eq!(branch_with_children.current_node_memory_usage(), exp_memory);
            assert_eq!(
                branch_with_children.children_memory_usage(get_node(&arena)),
                exp_children_mem
            );
        }
    }

    /// These tests verify descent logic for a single subtree.
    mod descent {
        use super::*;

        #[test]
        fn cut_off() {
            let mut descent_stage = TrieDescentStage::<()>::CutOff;
            assert!(!descent_stage.can_descend());
            descent_stage.descend(0, get_node_stub);
            assert_matches!(descent_stage, TrieDescentStage::CutOff);
        }

        #[test]
        fn leaf() {
            let mut arena = STArena::new("test".to_string());

            let mut empty_leaf = new_leaf(&mut arena, &[], &[]).to_descent_stage(&arena);
            assert!(!empty_leaf.can_descend());
            empty_leaf.descend(0, get_node_stub);
            assert_matches!(empty_leaf, TrieDescentStage::CutOff);

            let mut leaf_with_extension =
                new_leaf(&mut arena, &[1, 2], &[]).to_descent_stage(&arena);
            assert!(leaf_with_extension.can_descend());
            leaf_with_extension.descend(1, get_node_stub);
            assert_matches!(&leaf_with_extension, TrieDescentStage::InsideExtension { remaining_nibbles, ..} if **remaining_nibbles == [2]);
            assert!(leaf_with_extension.can_descend());
            leaf_with_extension.descend(2, get_node_stub);
            assert_matches!(leaf_with_extension, TrieDescentStage::AtLeaf { .. });
            assert!(!leaf_with_extension.can_descend());

            let mut leaf_with_extension =
                new_leaf(&mut arena, &[1, 2], &[]).to_descent_stage(&arena);
            leaf_with_extension.descend(6, get_node_stub); // Nibble **not** in the extension
            assert_matches!(leaf_with_extension, TrieDescentStage::CutOff);
        }

        #[test]
        fn extension() {
            let mut arena = STArena::new("test".to_string());

            let leaf = new_leaf(&mut arena, &[], &[]);
            let mut extension = new_extension(&mut arena, &[1, 2], leaf).to_descent_stage(&arena);
            assert!(extension.can_descend());
            extension.descend(1, get_node(&arena));
            assert_matches!(&extension, TrieDescentStage::InsideExtension { remaining_nibbles, ..} if **remaining_nibbles == [2]);
            assert!(extension.can_descend());
            extension.descend(2, get_node(&arena));
            assert_matches!(extension, TrieDescentStage::AtLeaf { .. });

            let mut extension = new_extension(&mut arena, &[1, 2], leaf).to_descent_stage(&arena);
            extension.descend(3, get_node_stub); // Nibble **not** in extension
            assert_matches!(extension, TrieDescentStage::CutOff);
        }

        #[test]
        fn branch() {
            let mut arena = STArena::new("test".to_string());

            let leaf = new_leaf(&mut arena, &[], &[]);
            let mut children = [None; NUM_CHILDREN];
            children[0] = Some(leaf);
            let mut branch = new_branch(&mut arena, None, children).to_descent_stage(&arena);
            assert!(branch.can_descend());
            branch.descend(0, get_node(&arena));
            assert_matches!(branch, TrieDescentStage::AtLeaf { .. });

            let mut branch = new_branch(&mut arena, None, children).to_descent_stage(&arena);
            branch.descend(1, get_node_stub); // Not child at this index
            assert_matches!(branch, TrieDescentStage::CutOff);
        }
    }

    /// These tests verify the logic of aggregate (multi-subtree) descent.
    mod trie_descent {
        use super::*;
        use near_primitives::errors::StorageError;

        /// A minimal implementation of `GenericTrieInternalStorage` for tests
        struct TestStorage<'a> {
            root: MemTrieNodeId,
            arena: &'a STArena,
        }

        impl<'a> GenericTrieInternalStorage<MemTrieNodeId, FlatStateValue> for TestStorage<'a> {
            fn get_root(&self) -> Option<MemTrieNodeId> {
                Some(self.root)
            }

            fn get_node_with_size(
                &self,
                ptr: MemTrieNodeId,
                _opts: AccessOptions,
            ) -> Result<MemTrieNodeWithSize, StorageError> {
                Ok(get_node(self.arena)(ptr))
            }

            fn get_value(
                &self,
                _value_ref: FlatStateValue,
                _opts: AccessOptions,
            ) -> Result<Vec<u8>, StorageError> {
                unimplemented!()
            }
        }

        fn init(
            arena: &mut STArena,
            subtrees: [MemTrieNodeId; SUBTREES.len()],
        ) -> TrieDescent<MemTrieNodeId, FlatStateValue, TestStorage<'_>> {
            let subtree_nibbles = SUBTREES.iter().map(|key| byte_to_nibbles(*key)).collect_vec();
            let first_nibble = subtree_nibbles[0].0;
            for (nib1, _) in &subtree_nibbles {
                assert_eq!(first_nibble, *nib1); // ensure all subtrees have the same first nibble
            }
            let mut children = [None; NUM_CHILDREN];
            for i in 0..SUBTREES.len() {
                let idx = subtree_nibbles[i].1 as usize;
                assert!(children[idx].is_none()); // ensure all subtrees have a different second nibble
                children[idx] = Some(subtrees[i]);
            }
            let branch = new_branch(arena, None, children);
            let root = new_extension(arena, &[first_nibble], branch);
            let storage = TestStorage { root, arena };
            TrieDescent::new(storage)
        }

        #[test]
        fn init_empty() {
            let mut arena = STArena::new("test".to_string());

            let subtrees = [new_leaf(&mut arena, &[], &[]); SUBTREES.len()];
            let trie_descent = init(&mut arena, subtrees);

            assert_eq!(trie_descent.total_memory(), SUBTREES.len() as u64 * EMPTY_LEAF_MEM);
            assert_eq!(trie_descent.aggregate_children_mem_usage(), [0u64; NUM_CHILDREN]);
            assert!(trie_descent.next_step().is_none());
        }

        #[test]
        fn init_nonempty() {
            let mut arena = STArena::new("test".to_string());

            let mut leaves = [None; NUM_CHILDREN];
            for i in 0..NUM_CHILDREN {
                let value = vec![0; i];
                leaves[i] = Some(new_leaf(&mut arena, &[], &value));
            }
            let subtrees = [new_branch(&mut arena, None, leaves); SUBTREES.len()];
            let trie_descent = init(&mut arena, subtrees);

            let exp_children_mem: [u64; NUM_CHILDREN] = (0..NUM_CHILDREN)
                .map(|i| {
                    (EMPTY_LEAF_MEM + TRIE_COSTS.byte_of_value * i as u64) * SUBTREES.len() as u64
                })
                .collect_vec()
                .try_into()
                .unwrap();
            let exp_total_mem =
                exp_children_mem.iter().sum::<u64>() + TRIE_COSTS.node_cost * SUBTREES.len() as u64;
            assert_eq!(trie_descent.total_memory(), exp_total_mem);
            assert_eq!(trie_descent.aggregate_children_mem_usage(), exp_children_mem);
            assert!(trie_descent.next_step().is_some());
        }

        #[test]
        #[should_panic]
        fn key_too_short() {
            let mut arena = STArena::new("test".to_string());

            // A single entry with 1-nibble key (not long enough for an account ID)
            let mut subtrees = [new_leaf(&mut arena, &[], &[]); SUBTREES.len()];
            subtrees[0] = new_leaf(&mut arena, &[1], &[1u8; 100]);
            init(&mut arena, subtrees).find_mem_usage_split();
        }

        #[test]
        #[should_panic]
        fn odd_key_length() {
            let mut arena = STArena::new("test".to_string());

            // The keys in this subtree are long enough for an account ID, but each has an odd
            // number of nibbles, hence cannot be correctly parsed as an account ID.
            //
            //    ROOT
            //     │
            //     1       <-- 4 nibbles extension (long enough for an account ID)
            //     1
            //     1
            //     1
            //     │
            //  0  │  1    <-- branch (extra nibble here makes the keys odd-length)
            //  ┌──┴──┐
            //  │     │
            //  X     X    <-- leaves with 100-byte values

            let mut subtrees = [new_leaf(&mut arena, &[], &[]); SUBTREES.len()];
            subtrees[0] = {
                let leaf = new_leaf(&mut arena, &[], &[1u8; 100]);
                let branch = {
                    let mut children = [None; NUM_CHILDREN];
                    children[0] = Some(leaf);
                    children[1] = Some(leaf);
                    new_branch(&mut arena, None, children)
                };
                new_extension(&mut arena, &[1, 1, 1, 1], branch)
            };
            init(&mut arena, subtrees).find_mem_usage_split();
        }

        #[test]
        fn search_trivial_accounts() {
            let mut arena = STArena::new("test".to_string());

            // Only accounts subtree is populated. Others are empty.
            // The subtree structure looks like this:
            //
            //         ROOT
            //     0    │    1
            //     ┌────┴────┐
            //  0  │  1   0  │  1
            //  ┌──┴──┐   ┌──┴──┐
            //  │     │   │     │
            //  0     0   0     0   <-- extension zeroes to reach the minimum length (2 bytes)
            //  0     0   0     0
            //  │     │   │     │
            //  X     X   X     X    <-- leaves with 100-byte values
            //
            // The expected split path is nibbles 1, 0, 0, 0 resulting in bytes 0x10, 0x00

            let accounts_subtree = {
                let leaf = new_leaf(&mut arena, &[0, 0], &[1u8; 100]);
                let branch = {
                    let mut children = [None; NUM_CHILDREN];
                    children[0] = Some(leaf);
                    children[1] = Some(leaf);
                    new_branch(&mut arena, None, children)
                };
                let mut children = [None; NUM_CHILDREN];
                children[0] = Some(branch);
                children[1] = Some(branch);
                new_branch(&mut arena, None, children)
            };
            let mut subtrees = [new_leaf(&mut arena, &[], &[]); SUBTREES.len()];
            subtrees[0] = accounts_subtree;

            let trie_descent = init(&mut arena, subtrees);
            let total_memory = trie_descent.total_memory();
            let split = trie_descent.find_mem_usage_split();
            assert_eq!(split.split_path_bytes(), vec![0x10, 0x00]);
            assert_eq!(split.left_memory + split.right_memory, total_memory);

            // Now add some heavy contract code under 0x11 path to change the optimal split path.
            subtrees[1] = new_leaf(&mut arena, &[1, 1, 1, 1, 1, 1], &[1u8; 200]);
            let trie_descent = init(&mut arena, subtrees);
            let total_memory = trie_descent.total_memory();
            let split = trie_descent.find_mem_usage_split();
            // The extra nibbles from extension in contract code subtree should *not* be included.
            // Trie descent is over when a leaf is reached in the accounts' subtree.
            assert_eq!(split.split_path_bytes(), vec![0x11, 0x00]);
            assert_eq!(split.left_memory + split.right_memory, total_memory);
        }
    }

    mod find_trie_split {
        use super::*;
        use crate::test_utils::TestTriesBuilder;
        use crate::trie::mem::iter::MemTrieIteratorInner;
        use crate::trie::ops::interface::GenericTrieInternalStorage;
        use crate::trie::update::TrieUpdateResult;
        use crate::{Trie, TrieUpdate, set, set_access_key, set_account};
        use itertools::Itertools;
        use near_crypto::{ED25519PublicKey, PublicKey};
        use near_primitives::account::AccessKey;
        use near_primitives::hash::CryptoHash;
        use near_primitives::test_utils::account_new;
        use near_primitives::trie_key::TrieKey;
        use near_primitives::types::{AccountId, Balance, StateChangeCause};
        use rand::RngCore;
        use rand::distributions::{Distribution, Uniform};
        use rand::prelude::SliceRandom;
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        use std::fmt::Debug;

        impl<NodePtr, Value, Storage> TrieDescent<NodePtr, Value, Storage>
        where
            NodePtr: Debug + Copy,
            Storage: GenericTrieInternalStorage<NodePtr, Value>,
        {
            /// Helper method to check the split yielded by a pre-defined key.
            fn check_split(mut self, key_bytes: &[u8]) -> TrieSplit {
                let key_nibbles = key_bytes
                    .iter()
                    .flat_map(|byte| {
                        let (nib1, nib2) = byte_to_nibbles(*byte);
                        [nib1, nib2]
                    })
                    .collect_vec();

                for nibble in key_nibbles {
                    let children_mem_usage = self.aggregate_children_mem_usage();
                    let child_mem_usage = children_mem_usage[nibble as usize];
                    let left_mem_usage = children_mem_usage[..nibble as usize].iter().sum();
                    self.descend_step(nibble, child_mem_usage, left_mem_usage);
                }

                TrieSplit::new(
                    self.nibbles,
                    self.left_memory,
                    self.right_memory + self.middle_memory,
                )
            }
        }

        /// Helper function to get the split yielded by the given boundary account.
        /// Assumes the trie is using memtries.
        fn get_memtrie_split(trie: &Trie, boundary_account: &AccountId) -> TrieSplit {
            let key_bytes = boundary_account.as_bytes();
            let memtries = trie.lock_memtries().unwrap();
            let trie_storage = MemTrieIteratorInner::new(&memtries, trie);
            TrieDescent::new(trie_storage).check_split(key_bytes)
        }

        impl TrieSplit {
            /// Get difference between right and left memory
            fn mem_diff(&self) -> i64 {
                self.right_memory as i64 - self.left_memory as i64
            }
        }

        #[test]
        fn big_random_trie() {
            // Seeds between 0 and 10000 were tested and these were picked as they uncovered
            // some bugs and corner cases.
            big_random_trie_inner(1).unwrap();
            big_random_trie_inner(4).unwrap();
            big_random_trie_inner(11).unwrap();
            big_random_trie_inner(369).unwrap();
        }

        fn big_random_trie_inner(seed: u64) -> anyhow::Result<()> {
            // Initialize an empty trie and start an update to populate it with some data
            let (shard_tries, layout) =
                TestTriesBuilder::new().with_in_memory_tries(true).with_flat_storage(true).build2();
            let shard_uid = layout.get_shard_uid(0)?;
            let trie = shard_tries.get_trie_for_shard(shard_uid, Trie::EMPTY_ROOT);
            let mut trie_update = TrieUpdate::new(trie);

            println!("Seed: {seed}");
            let mut rng = StdRng::seed_from_u64(seed);

            // Set up 1000 accounts with random IDs (between 5 and 15 characters)
            // and random balance between 10 and 1000 NEAR.
            let num_accounts = 1000;
            let lengths = Uniform::try_from(5..=15)?;
            let chars = Uniform::try_from('a'..='z')?;
            let balances = Uniform::from(10_u128..=1000);
            let mut rand_account = || -> anyhow::Result<AccountId> {
                let length = lengths.sample(&mut rng);
                let account_str: String = chars.sample_iter(&mut rng).take(length).collect();
                Ok(account_str.parse()?)
            };
            let mut account_ids = (0..num_accounts)
                .map(|_| rand_account())
                .collect::<anyhow::Result<Vec<AccountId>>>()?;

            for account_id in &account_ids {
                let balance = Balance::from_near(balances.sample(&mut rng));
                let account = account_new(balance, CryptoHash::default());
                set_account(&mut trie_update, account_id.clone(), &account);

                // Randomly assign access keys to approx. half of the accounts
                if rng.gen_bool(0.5) {
                    let mut pub_key_bytes = [0u8; 32];
                    rng.fill_bytes(&mut pub_key_bytes);
                    let pub_key = PublicKey::from(ED25519PublicKey::from(pub_key_bytes));
                    let access_key = AccessKey::full_access();
                    set_access_key(&mut trie_update, account_id.clone(), pub_key, &access_key);
                }
            }

            // Add 20 contracts to randomly selected accounts. Each contract has between 100 and 200
            // bytes of code, and between 5 and 10 data entries. Each data entry consists of
            // a random key (between 10 and 20 bytes) and random value (between 50 and 100 bytes).
            let num_contracts = 20;
            let code_lengths = Uniform::try_from(100..=200)?;
            let num_keys = Uniform::try_from(5..=10)?;
            let key_lengths = Uniform::try_from(10..=20)?;
            let value_lengths = Uniform::try_from(50..=100)?;
            account_ids.shuffle(&mut rng);
            for i in 0..num_contracts {
                let account_id = &account_ids[i];
                let code_length = code_lengths.sample(&mut rng);
                let mut contract_code = vec![0u8; code_length];
                rng.fill_bytes(&mut contract_code);
                set(
                    &mut trie_update,
                    TrieKey::ContractCode { account_id: account_id.clone() },
                    &contract_code,
                );

                let num_keys = num_keys.sample(&mut rng);
                for _ in 0..num_keys {
                    let key_length = key_lengths.sample(&mut rng);
                    let mut key = vec![0u8; key_length];
                    rng.fill_bytes(&mut key);
                    let value_length = value_lengths.sample(&mut rng);
                    let mut value = vec![0u8; value_length];
                    rng.fill_bytes(&mut value);
                    set(
                        &mut trie_update,
                        TrieKey::ContractData { account_id: account_id.clone(), key },
                        &value,
                    );
                }
            }

            // Commit and apply all changes to the trie
            trie_update.commit(StateChangeCause::InitialState);
            let TrieUpdateResult { trie_changes, .. } = trie_update.finalize()?;
            let mut store_update = shard_tries.store_update();
            let new_root = shard_tries.apply_all(&trie_changes, shard_uid, &mut store_update);
            shard_tries.apply_memtrie_changes(&trie_changes, shard_uid, 0);
            store_update.commit()?;

            // Find the boundary account
            let trie = shard_tries.get_trie_for_shard(shard_uid, new_root);
            let trie_split = find_trie_split(&trie);
            println!("Found trie split: {trie_split:?}");
            let boundary_account = trie_split.boundary_account();
            println!("Boundary account: {boundary_account:?}");

            // Assert that previous and next account gives worse splits than the account found
            // by `find_trie_split`. As the accounts are sorted, we don't need to check them all.
            // If the split given by account at index `i` is better than the split given by accounts
            // at `i-1` and `i+1`, it is also the best split of all the accounts.
            account_ids.sort();
            let (left_account, right_account) =
                find_neighbor_accounts(&account_ids, &boundary_account);

            if let Some(left_account) = left_account {
                let left_split = get_memtrie_split(&trie, left_account);
                println!("Left account split: {left_split:?}");
                assert!(left_split.mem_diff() >= trie_split.mem_diff());
            }

            if let Some(right_account) = right_account {
                let right_split = get_memtrie_split(&trie, right_account);
                println!("Right account split: {right_split:?}");

                // `find_trie_split` finds the best possible split for which `right_memory >= left_memory`.
                // Hence, it is acceptable that the absolute `mem_diff` for `right_split` is lower, as
                // long as the `right_memory >= left_memory` condition doesn't hold.
                assert!(
                    right_split.mem_diff() >= trie_split.mem_diff() || right_split.mem_diff() < 0
                );
            }

            Ok(())
        }

        /// Find neighbors of `boundary_account` in `account_ids` array.
        /// Assumes that the array is sorted.
        fn find_neighbor_accounts<'a>(
            account_ids: &'a [AccountId],
            boundary_account: &AccountId,
        ) -> (Option<&'a AccountId>, Option<&'a AccountId>) {
            let num_accounts = account_ids.len();
            for i in 0..num_accounts {
                let left_account = (i > 0).then(|| &account_ids[i - 1]);
                if &account_ids[i] == boundary_account {
                    let right_account = (i < num_accounts - 1).then(|| &account_ids[i + 1]);
                    return (left_account, right_account);
                }
                if &account_ids[i] > boundary_account {
                    let right_account = Some(&account_ids[i]);
                    return (left_account, right_account);
                }
            }
            unreachable!("neighbor accounts not found")
        }
    }
}
