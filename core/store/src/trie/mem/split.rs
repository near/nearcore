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
// Chunks are split by account IDs, not arbitrary byte sequences.
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
        let get_node =
            |ptr| self.trie_storage.get_node_with_size(ptr, AccessOptions::DEFAULT).unwrap();
        for stage in &mut self.subtree_stages {
            stage.descend(nibble, get_node);
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
        let threshold = 2000;
        assert_eq!(find_middle_child(&children_mem_usage, threshold), None);

        let threshold = 1050;
        assert_eq!(find_middle_child(&children_mem_usage, threshold), Some((10, 1000)));
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
            //  X     X   X     X    <-- leaves with 100-byte values
            //
            // The expected split path is nibbles 1, 0 resulting in byte 0x10

            let accounts_subtree = {
                let leaf = new_leaf(&mut arena, &[], &[1u8; 100]);
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
            assert_eq!(split.split_path_bytes(), vec![0x10]);
            assert_eq!(split.left_memory + split.right_memory, total_memory);

            // Now add some heavy contract code under 0x11 path to change the optimal split path.
            subtrees[1] = new_leaf(&mut arena, &[1, 1, 1, 1, 1, 1], &[1u8; 200]);
            let trie_descent = init(&mut arena, subtrees);
            let total_memory = trie_descent.total_memory();
            let split = trie_descent.find_mem_usage_split();
            // The extra nibbles from extension in contract code subtree should *not* be included.
            // Trie descent is over when a leaf is reached in the accounts' subtree.
            assert_eq!(split.split_path_bytes(), vec![0x11]);
            assert_eq!(split.left_memory + split.right_memory, total_memory);
        }
    }
}
