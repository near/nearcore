use crate::trie::iterator::DiskTrieIteratorInner;
use crate::trie::mem::iter::MemTrieIteratorInner;
use crate::trie::ops::interface::{
    GenericTrieInternalStorage, GenericTrieNode, GenericTrieNodeWithSize,
};
use crate::trie::{AccessOptions, NUM_CHILDREN};
use crate::{NibbleSlice, Trie};
use derive_where::derive_where;
use near_primitives::account::id::ParseAccountError;
use near_primitives::errors::StorageError;
use near_primitives::trie_key::col::{ACCESS_KEY, ACCOUNT, CONTRACT_CODE, CONTRACT_DATA};
use near_primitives::trie_key::{ACCESS_KEY_SEPARATOR, ACCOUNT_DATA_SEPARATOR};
use near_primitives::trie_split::TrieSplit;
use near_primitives::types::AccountId;
use smallvec::SmallVec;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::str::FromStr;
use thiserror::Error;

const MAX_NIBBLES: usize = AccountId::MAX_LEN * 2;

// NOTE: The order of subtrees in this array should match `SubtreeIdx` enum.
// Also, do not add any subtrees here that do not use account ID as key (or key prefix).
// The reason for this is that chunks are split by account IDs, not arbitrary byte sequences.
const SUBTREES: [u8; 4] = [ACCOUNT, CONTRACT_CODE, ACCESS_KEY, CONTRACT_DATA];

// NOTE: These indices should match the order of `SUBTREES` array.
enum SubtreeIdx {
    Account = 0,
    #[allow(dead_code)]
    ContractCode = 1,
    AccessKey = 2,
    ContractData = 3,
}

// Subtrees which store data related to an account in a subtree rather than in a single node
// identified by the account ID. The data is separated from the account ID by a separator byte.
const ATTACHED_DATA: [(SubtreeIdx, u8); 2] = [
    (SubtreeIdx::AccessKey, ACCESS_KEY_SEPARATOR),
    (SubtreeIdx::ContractData, ACCOUNT_DATA_SEPARATOR),
];

#[derive(Error, Debug)]
pub enum FindSplitError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("no root in trie")]
    NoRoot,
    #[error("split not found – trie is empty or contains invalid keys")]
    NotFound,
    #[error("key in the trie is not a valid account ID (too short or odd length). nibbles: {0:?}")]
    Key(Vec<u8>),
    #[error("split key is not valid UTF-8 string")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("split key is not a valid account ID")]
    AccountId(#[from] ParseAccountError),
}

type FindSplitResult<T> = Result<T, FindSplitError>;

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
            GenericTrieNode::Leaf { extension, .. } => {
                let nibbles = extension_to_nibbles(&extension);
                if nibbles.is_empty() {
                    Self::AtLeaf { memory_usage }
                } else {
                    Self::InsideExtension { memory_usage, remaining_nibbles: nibbles, child: None }
                }
            }
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

impl<NodePtr: Debug + Copy> TrieDescentStage<NodePtr> {
    /// Memory usage of the subtree under the current node (including the node itself).
    fn subtree_memory_usage(&self) -> u64 {
        match self {
            Self::CutOff => 0,
            Self::AtLeaf { memory_usage, .. }
            | Self::InsideExtension { memory_usage, .. }
            | Self::AtBranch { memory_usage, .. } => *memory_usage,
        }
    }

    /// Memory usage of the current node itself, **excluding** all descendant nodes.
    fn node_memory_usage<Value, Getter>(&self, get_node: Getter) -> FindSplitResult<u64>
    where
        Getter: Fn(NodePtr) -> Result<GenericTrieNodeWithSize<NodePtr, Value>, StorageError>,
    {
        match self {
            TrieDescentStage::AtLeaf { memory_usage } => Ok(*memory_usage),
            TrieDescentStage::AtBranch { memory_usage, .. } => {
                let children_mem_usage: u64 = self.children_memory_usage(get_node)?.iter().sum();
                Ok(*memory_usage - children_mem_usage)
            }
            TrieDescentStage::InsideExtension {
                memory_usage,
                child: Some(child),
                remaining_nibbles,
            } if remaining_nibbles.len() == 1 => {
                let child_mem_usage = get_node(*child)?.memory_usage;
                Ok(*memory_usage - child_mem_usage)
            }
            _ => Ok(0),
        }
    }

    /// Memory usage of the subtree under a descendant node (including the descendant node itself)
    fn descendant_mem_usage<Value, Getter>(
        &self,
        get_node: Getter,
        nibbles: &[u8],
    ) -> FindSplitResult<u64>
    where
        Getter: Fn(NodePtr) -> Result<GenericTrieNodeWithSize<NodePtr, Value>, StorageError>,
    {
        if nibbles.is_empty() {
            return Ok(self.subtree_memory_usage());
        }
        match self {
            TrieDescentStage::InsideExtension { remaining_nibbles, memory_usage, .. } => {
                if remaining_nibbles.starts_with(nibbles) {
                    Ok(*memory_usage)
                } else {
                    Ok(0)
                }
            }
            TrieDescentStage::AtBranch { children, .. } => {
                if let Some(child) = children[nibbles[0] as usize] {
                    let child: Self = get_node(child)?.into();
                    child.descendant_mem_usage(get_node, &nibbles[1..])
                } else {
                    Ok(0)
                }
            }
            _ => Ok(0),
        }
    }

    /// Memory usage of children subtrees. For a branch node these are actual children.
    /// For an extension, as single 'virtual' child will be listed under the index representing
    /// the next nibble in the extension.
    fn children_memory_usage<Value, Getter>(
        &self,
        get_node: Getter,
    ) -> FindSplitResult<[u64; NUM_CHILDREN]>
    where
        Getter: Fn(NodePtr) -> Result<GenericTrieNodeWithSize<NodePtr, Value>, StorageError>,
    {
        let mut result = [0; NUM_CHILDREN];
        match self {
            Self::CutOff | Self::AtLeaf { .. } => {}
            // If there is only one nibble remaining, and the extension has a child node (i.e. it is
            // not a leaf with extension), we return the child's memory usage.
            Self::InsideExtension { child: Some(child), remaining_nibbles, .. }
                if remaining_nibbles.len() == 1 =>
            {
                let next_nibble = remaining_nibbles[0] as usize;
                result[next_nibble] = get_node(*child)?.memory_usage;
            }
            Self::InsideExtension { memory_usage, remaining_nibbles, .. } => {
                let next_nibble = remaining_nibbles[0] as usize;
                result[next_nibble] = *memory_usage;
            }
            Self::AtBranch { children, .. } => {
                for i in 0..NUM_CHILDREN {
                    if let Some(child) = children[i] {
                        result[i] = get_node(child)?.memory_usage;
                    }
                }
            }
        }
        Ok(result)
    }

    fn can_descend(&self) -> bool {
        match self {
            Self::CutOff | Self::AtLeaf { .. } => false,
            Self::InsideExtension { .. } | Self::AtBranch { .. } => true,
        }
    }

    fn can_descend_nibble(&self, nibble: u8) -> bool {
        match self {
            TrieDescentStage::CutOff | TrieDescentStage::AtLeaf { .. } => false,
            TrieDescentStage::InsideExtension { remaining_nibbles, .. } => {
                nibble == remaining_nibbles[0]
            }
            TrieDescentStage::AtBranch { children, .. } => children[nibble as usize].is_some(),
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

    fn descend<Value, Getter>(&mut self, nibble: u8, get_node: Getter) -> FindSplitResult<()>
    where
        Getter: Fn(NodePtr) -> Result<GenericTrieNodeWithSize<NodePtr, Value>, StorageError>,
    {
        tracing::trace!(?self, %nibble, "descending");
        match self {
            Self::CutOff => {}
            Self::AtLeaf { .. } => *self = Self::CutOff,
            Self::InsideExtension { memory_usage, remaining_nibbles, child } => {
                if remaining_nibbles.get(0).is_some_and(|x| *x == nibble) {
                    remaining_nibbles.remove(0);
                } else {
                    *self = Self::CutOff;
                    return Ok(());
                }

                if remaining_nibbles.is_empty() {
                    match child {
                        None => *self = Self::AtLeaf { memory_usage: *memory_usage },
                        Some(child) => *self = get_node(*child)?.into(),
                    }
                }
            }
            Self::AtBranch { children, .. } => {
                if let Some(child) = children[nibble as usize] {
                    *self = get_node(child)?.into();
                } else {
                    *self = Self::CutOff;
                }
            }
        }
        Ok(())
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
    pub fn new(trie_storage: Storage) -> FindSplitResult<Self> {
        let root_ptr = trie_storage.get_root().ok_or(FindSplitError::NoRoot)?;
        let get_node = |ptr| trie_storage.get_node_with_size(ptr, AccessOptions::DEFAULT);
        let mut subtree_stages = SmallVec::new();
        let mut middle_memory = 0;

        for subtree_key in SUBTREES {
            let mut subtree_stage: TrieDescentStage<NodePtr> = get_node(root_ptr)?.into();
            let nibbles = byte_to_nibbles(subtree_key);
            subtree_stage.descend(nibbles[0], get_node)?;
            subtree_stage.descend(nibbles[1], get_node)?;
            middle_memory += subtree_stage.subtree_memory_usage();
            subtree_stages.push(subtree_stage);
        }

        Ok(Self {
            _phantom: PhantomData,
            trie_storage,
            subtree_stages,
            nibbles: SmallVec::new(),
            left_memory: 0,
            right_memory: 0,
            middle_memory,
        })
    }

    fn accounts(&self) -> &TrieDescentStage<NodePtr> {
        &self.subtree_stages[SubtreeIdx::Account as usize]
    }

    fn total_memory(&self) -> u64 {
        self.left_memory + self.right_memory + self.middle_memory
    }

    /// Aggregate children memory usage across all subtrees
    fn aggregate_children_mem_usage(&self) -> FindSplitResult<[u64; NUM_CHILDREN]> {
        let get_node = |ptr| self.trie_storage.get_node_with_size(ptr, AccessOptions::DEFAULT);
        let mut children_mem_usage = [0u64; NUM_CHILDREN];
        for subtree in &self.subtree_stages {
            let subtree_children = subtree.children_memory_usage(get_node)?;
            for i in 0..NUM_CHILDREN {
                children_mem_usage[i] += subtree_children[i];
            }
        }
        Ok(children_mem_usage)
    }

    /// Get total memory usage of the 'current nodes' i.e. nodes that we are currently visiting
    /// in each subtree. It is the memory used by the nodes themselves, **excluding descendants**.
    fn current_nodes_mem_usage(&self) -> FindSplitResult<u64> {
        let get_node = |ptr| self.trie_storage.get_node_with_size(ptr, AccessOptions::DEFAULT);
        let mut result = 0;
        for subtree in &self.subtree_stages {
            result += subtree.node_memory_usage(get_node)?;
        }
        Ok(result)
    }

    /// Find the key (nibbles) which splits the trie into two parts with possibly equal
    /// memory usage. Returns the key and the memory usage of the left and right part.
    pub fn find_mem_usage_split(mut self) -> FindSplitResult<TrieSplit> {
        // dummy split that will be worse than any actual split
        let mut best_split = TrieSplit::dummy();

        while let Some((nibble, child_mem_usage, left_mem_usage)) = self.next_step()? {
            self.descend_step(nibble, child_mem_usage, left_mem_usage)?;

            let Some(current_split) = self.best_split_at_current_path()? else { continue };
            if current_split.mem_diff() < best_split.mem_diff() {
                best_split = current_split;
            }
        }

        // The split path needs to be convertible into an account ID, which means an even number
        // of nibbles is required. If we ended with an odd number of nibbles, we can try descending
        // into the first child.
        if self.nibbles.len() % 2 != 0 {
            self.force_next_step()?;
            if let Some(current_split) = self.best_split_at_current_path()? {
                if current_split.mem_diff() < best_split.mem_diff() {
                    best_split = current_split;
                }
            }
        }

        if best_split.is_dummy() { Err(FindSplitError::NotFound) } else { Ok(best_split) }
    }

    /// Find the next step `(nibble, child_mem_usage, left_mem_usage)`.
    ///     * `nibble` – next nibble on the path (index of the middle child node)
    ///     * `child_mem_usage` – memory usage of the middle child
    ///     * `left_mem_usage` – total memory usage of left siblings of the middle child
    /// Returns `None` if the end of the searched is reached.
    fn next_step(&self) -> FindSplitResult<Option<(u8, u64, u64)>> {
        // Stop when a leaf is reached in the accounts subtree
        if !self.accounts().can_descend() {
            tracing::debug!("leaf reached in accounts subtree");
            return Ok(None);
        }

        let current_nodes_memory = self.current_nodes_mem_usage()?;
        if self.left_memory + current_nodes_memory > self.total_memory() / 2 {
            return Ok(None);
        }
        let threshold = (self.total_memory() / 2) - self.left_memory - current_nodes_memory;
        let children_mem_usage = self.aggregate_children_mem_usage()?;

        // Find the middle child
        tracing::debug!(?children_mem_usage, %threshold, "finding middle child");
        let Some((middle_child, left_mem_usage)) =
            find_middle_child(&children_mem_usage, threshold)
        else {
            return Ok(None);
        };
        let child_mem_usage = children_mem_usage[middle_child];
        tracing::debug!(%middle_child, %child_mem_usage, %left_mem_usage, "middle child found");

        // Stop if the further path does not exist in the accounts subtree
        let middle_child = middle_child as u8;
        if !self.accounts().can_descend_nibble(middle_child) {
            return Ok(None);
        }
        Ok(Some((middle_child, child_mem_usage, left_mem_usage)))
    }

    /// Current split path parsed as account ID.
    fn current_account(&self) -> Option<AccountId> {
        nibbles_to_account_id(&self.nibbles)
    }

    /// Get memory usage of data 'attached' to the account represented by the current path.
    /// This includes data which is not stored at the current path, but in subtrees, namely:
    ///  * access keys
    ///  * contract data
    /// Contract code is **not included** as it is stored at account ID path, not in a subtree.
    fn attached_data_mem_usage(&self) -> FindSplitResult<u64> {
        if self.current_account().is_none() {
            return Ok(0);
        }
        let get_node = |ptr| self.trie_storage.get_node_with_size(ptr, AccessOptions::DEFAULT);
        let mut result = 0;
        for (idx, separator) in ATTACHED_DATA {
            let separator = byte_to_nibbles(separator);
            let subtree = &self.subtree_stages[idx as usize];
            result += subtree.descendant_mem_usage(get_node, &separator)?;
        }
        Ok(result)
    }

    /// Find best split at the current path. Considers two splits and picks the better one:
    ///  a) split at current path – all the middle memory is put in the right child,
    ///  b) append "-0" suffix – the current node is put in the left child, but all its
    ///     descendants go to the right child.
    /// Returns `None` if current path is not a valid account ID (neither as-is, nor with -0 suffix).
    fn best_split_at_current_path(&self) -> FindSplitResult<Option<TrieSplit>> {
        let split_a = self.current_account().map(|account_id| {
            // When splitting at the current path, middle memory goes to the right, as the boundary
            // account is included in the right child.
            TrieSplit::new(account_id, self.left_memory, self.right_memory + self.middle_memory)
        });

        let mut ext_nibbles = self.nibbles.clone();
        ext_nibbles.extend(bytes_to_nibbles("-0".as_bytes()));
        let split_b = nibbles_to_account_id(&ext_nibbles)
            .map(|account_id| {
                // When splitting at the suffixed path, nodes at the current path go to the left
                // together with their 'attached data'. All children accounts go to the right.
                let curr_nodes_mem = self.current_nodes_mem_usage()?;
                let attached_data_mem = self.attached_data_mem_usage()?;
                let left = self.left_memory + curr_nodes_mem + attached_data_mem;
                let right =
                    self.right_memory + self.middle_memory - curr_nodes_mem - attached_data_mem;
                Ok::<_, FindSplitError>(TrieSplit::new(account_id, left, right))
            })
            .transpose()?;

        Ok([split_a, split_b].into_iter().flatten().min_by_key(|split| split.mem_diff()))
    }

    /// Force descent into the first available child. This is done to ensure the correct length
    /// of the split path, even though further descent will not improve the memory usage balance.
    fn force_next_step(&mut self) -> FindSplitResult<()> {
        let children_mem_usage = self.aggregate_children_mem_usage()?;
        let first_child = self
            .accounts()
            .first_child()
            .ok_or_else(|| FindSplitError::Key(self.nibbles.to_vec()))?;
        let child_mem_usage = children_mem_usage[first_child as usize];
        let left_mem_usage = children_mem_usage[0..first_child as usize].iter().sum();
        self.descend_step(first_child, child_mem_usage, left_mem_usage)
    }

    fn descend_step(
        &mut self,
        nibble: u8,
        child_mem_usage: u64,
        left_mem_usage: u64,
    ) -> FindSplitResult<()> {
        let parent_mem_usage = self.current_nodes_mem_usage()?;
        // Left siblings and parents are lower than the current path, so their mem usage is added to the left
        self.left_memory += left_mem_usage + parent_mem_usage;
        // Right siblings are higher than the current path, so their mem usage is added to the right
        self.right_memory +=
            self.middle_memory - left_mem_usage - child_mem_usage - parent_mem_usage;
        self.middle_memory = child_mem_usage;
        tracing::debug!(%self.left_memory, %self.right_memory, %self.middle_memory, "remaining memory updated");

        // Update descent stages for all subtrees
        let get_node = |ptr| self.trie_storage.get_node_with_size(ptr, AccessOptions::DEFAULT);
        for stage in &mut self.subtree_stages {
            stage.descend(nibble, get_node)?;
        }
        self.nibbles.push(nibble);
        tracing::debug!(?self.nibbles, nibble_str = String::from_utf8_lossy(&nibbles_to_bytes(&self.nibbles)).to_string(), "nibbles updated");
        Ok(())
    }
}

/// Find the lowest child index `i` for which `sum(children_mem_usage[..i+1]) > threshold`.
/// Returns `Some(i, sum(children_mem_usage[..i])` if such `i` exists.
/// If no such `i` exists, returns the highest `i` for which `children_mem_usage[i] > 0`.
/// If all children's memory usage is 0, returns `None`.
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

fn byte_to_nibbles(byte: u8) -> [u8; 2] {
    [byte >> 4, byte & 0x0F]
}

fn bytes_to_nibbles(bytes: &[u8]) -> impl Iterator<Item = u8> {
    bytes.iter().flat_map(|b| byte_to_nibbles(*b))
}

fn nibbles_to_bytes(nibbles: &[u8]) -> Vec<u8> {
    nibbles.chunks_exact(2).map(|pair| (pair[0] << 4) | pair[1]).collect()
}

fn nibbles_to_account_id(nibbles: &[u8]) -> Option<AccountId> {
    if nibbles.len() % 2 != 0 {
        return None;
    }
    let bytes = nibbles_to_bytes(nibbles);
    let account_str = std::str::from_utf8(&bytes).ok()?;
    AccountId::from_str(account_str).ok()
}
fn extension_to_nibbles(extension: &[u8]) -> SmallVec<[u8; MAX_NIBBLES]> {
    let (nibble_slice, _) = NibbleSlice::from_encoded(extension);
    nibble_slice.iter().collect()
}

pub fn find_trie_split(trie: &Trie) -> FindSplitResult<TrieSplit> {
    match trie.lock_memtries() {
        Some(memtries) => {
            let trie_storage = MemTrieIteratorInner::new(&memtries, trie);
            TrieDescent::new(trie_storage)?.find_mem_usage_split()
        }
        None => {
            let trie_storage = DiskTrieIteratorInner::new(trie);
            TrieDescent::new(trie_storage)?.find_mem_usage_split()
        }
    }
}

pub fn total_mem_usage(trie: &Trie) -> FindSplitResult<u64> {
    match trie.lock_memtries() {
        Some(memtries) => {
            let trie_storage = MemTrieIteratorInner::new(&memtries, trie);
            let root_id = trie_storage.get_root().ok_or(FindSplitError::NoRoot)?;
            let root_node = trie_storage.get_node_with_size(root_id, AccessOptions::DEFAULT)?;
            Ok(root_node.memory_usage)
        }
        None => {
            let trie_storage = DiskTrieIteratorInner::new(trie);
            let root_id = trie_storage.get_root().ok_or(FindSplitError::NoRoot)?;
            let root_node = trie_storage.get_node_with_size(root_id, AccessOptions::DEFAULT)?;
            Ok(root_node.memory_usage)
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
        assert_eq!(byte_to_nibbles(0x00), [0x00, 0x00]);
        assert_eq!(byte_to_nibbles(0x01), [0x00, 0x01]);
        assert_eq!(byte_to_nibbles(0x10), [0x01, 0x00]);
        assert_eq!(byte_to_nibbles(0x11), [0x01, 0x01]);

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
            get_node(arena)(*self).unwrap().into()
        }
    }

    fn get_node_stub<P>(_: P) -> Result<GenericTrieNodeWithSize<P, ()>, StorageError> {
        unreachable!()
    }

    fn get_node(
        arena: &STArena,
    ) -> impl Fn(MemTrieNodeId) -> Result<MemTrieNodeWithSize, StorageError> {
        |node_id| {
            Ok(MemTrieNodeWithSize::from_existing_node_view(node_id.as_ptr(arena.memory()).view()))
        }
    }

    /// These tests verify memory usage calculation of the current node and children nodes.
    mod mem_usage {
        use super::*;

        #[test]
        fn cut_off() {
            let descent_stage = TrieDescentStage::<()>::CutOff;
            assert_eq!(descent_stage.subtree_memory_usage(), 0);
            assert_eq!(
                descent_stage.children_memory_usage(get_node_stub).unwrap(),
                [0u64; NUM_CHILDREN]
            );
        }

        #[test]
        fn leaf() {
            let mut arena = STArena::new("test".to_string());

            let empty_leaf = new_leaf(&mut arena, &[], &[]).to_descent_stage(&arena);
            assert_eq!(empty_leaf.subtree_memory_usage(), EMPTY_LEAF_MEM);
            assert_eq!(
                empty_leaf.children_memory_usage(get_node_stub).unwrap(),
                [0u64; NUM_CHILDREN]
            );

            let nonempty_leaf = new_leaf(&mut arena, &[], &[1, 2, 3]).to_descent_stage(&arena);
            let exp_memory = EMPTY_LEAF_MEM + TRIE_COSTS.byte_of_value * 3;
            assert_eq!(nonempty_leaf.subtree_memory_usage(), exp_memory);
            assert_eq!(
                nonempty_leaf.children_memory_usage(get_node_stub).unwrap(),
                [0u64; NUM_CHILDREN]
            );

            let extension_leaf = new_leaf(&mut arena, &[1, 2], &[]).to_descent_stage(&arena);
            let exp_memory = EMPTY_LEAF_MEM + TRIE_COSTS.byte_of_key * 2;
            let mut exp_children_mem = [0u64; NUM_CHILDREN];
            exp_children_mem[1] = exp_memory; // The first nibble in extension is '1'
            assert_eq!(extension_leaf.subtree_memory_usage(), exp_memory);
            assert_eq!(
                extension_leaf.children_memory_usage(get_node_stub).unwrap(),
                exp_children_mem
            );
        }

        #[test]
        fn extension() {
            let mut arena = STArena::new("test".to_string());

            let empty_leaf = new_leaf(&mut arena, &[], &[]);
            let extension = new_extension(&mut arena, &[4, 5], empty_leaf).to_descent_stage(&arena);

            let exp_memory = EMPTY_LEAF_MEM + TRIE_COSTS.node_cost + TRIE_COSTS.byte_of_key * 2;
            let mut exp_children_mem = [0u64; NUM_CHILDREN];
            exp_children_mem[4] = exp_memory; // The first nibble in extension is '4'
            assert_eq!(extension.subtree_memory_usage(), exp_memory);
            assert_eq!(extension.children_memory_usage(get_node_stub).unwrap(), exp_children_mem);
        }

        #[test]
        fn branch() {
            let mut arena = STArena::new("test".to_string());

            let empty_branch = new_branch(&mut arena, None, [None; 16]).to_descent_stage(&arena);
            assert_eq!(empty_branch.subtree_memory_usage(), TRIE_COSTS.node_cost);
            assert_eq!(
                empty_branch.children_memory_usage(get_node_stub).unwrap(),
                [0u64; NUM_CHILDREN]
            );

            let branch_with_value =
                new_branch(&mut arena, Some(&[1, 2, 3]), [None; 16]).to_descent_stage(&arena);
            // For some reason, node_cost for branch with value is doubled (?)
            let exp_memory = TRIE_COSTS.node_cost * 2 + TRIE_COSTS.byte_of_value * 3;
            assert_eq!(branch_with_value.subtree_memory_usage(), exp_memory);
            assert_eq!(
                branch_with_value.children_memory_usage(get_node_stub).unwrap(),
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
            assert_eq!(branch_with_children.subtree_memory_usage(), exp_memory);
            assert_eq!(
                branch_with_children.children_memory_usage(get_node(&arena)).unwrap(),
                exp_children_mem
            );
        }
    }

    /// These tests verify descent logic for a single subtree.
    mod descent {
        use super::*;

        #[test]
        fn cut_off() -> anyhow::Result<()> {
            let mut descent_stage = TrieDescentStage::<()>::CutOff;
            assert!(!descent_stage.can_descend());
            descent_stage.descend(0, get_node_stub)?;
            assert_matches!(descent_stage, TrieDescentStage::CutOff);
            Ok(())
        }

        #[test]
        fn leaf() -> anyhow::Result<()> {
            let mut arena = STArena::new("test".to_string());

            let mut empty_leaf = new_leaf(&mut arena, &[], &[]).to_descent_stage(&arena);
            assert!(!empty_leaf.can_descend());
            empty_leaf.descend(0, get_node_stub)?;
            assert_matches!(empty_leaf, TrieDescentStage::CutOff);

            let mut leaf_with_extension =
                new_leaf(&mut arena, &[1, 2], &[]).to_descent_stage(&arena);
            assert!(leaf_with_extension.can_descend());
            leaf_with_extension.descend(1, get_node_stub)?;
            assert_matches!(&leaf_with_extension, TrieDescentStage::InsideExtension { remaining_nibbles, ..} if **remaining_nibbles == [2]);
            assert!(leaf_with_extension.can_descend());
            leaf_with_extension.descend(2, get_node_stub)?;
            assert_matches!(leaf_with_extension, TrieDescentStage::AtLeaf { .. });
            assert!(!leaf_with_extension.can_descend());

            let mut leaf_with_extension =
                new_leaf(&mut arena, &[1, 2], &[]).to_descent_stage(&arena);
            leaf_with_extension.descend(6, get_node_stub)?; // Nibble **not** in the extension
            assert_matches!(leaf_with_extension, TrieDescentStage::CutOff);

            Ok(())
        }

        #[test]
        fn extension() -> anyhow::Result<()> {
            let mut arena = STArena::new("test".to_string());

            let leaf = new_leaf(&mut arena, &[], &[]);
            let mut extension = new_extension(&mut arena, &[1, 2], leaf).to_descent_stage(&arena);
            assert!(extension.can_descend());
            extension.descend(1, get_node(&arena))?;
            assert_matches!(&extension, TrieDescentStage::InsideExtension { remaining_nibbles, ..} if **remaining_nibbles == [2]);
            assert!(extension.can_descend());
            extension.descend(2, get_node(&arena))?;
            assert_matches!(extension, TrieDescentStage::AtLeaf { .. });

            let mut extension = new_extension(&mut arena, &[1, 2], leaf).to_descent_stage(&arena);
            extension.descend(3, get_node_stub)?; // Nibble **not** in extension
            assert_matches!(extension, TrieDescentStage::CutOff);

            Ok(())
        }

        #[test]
        fn branch() -> anyhow::Result<()> {
            let mut arena = STArena::new("test".to_string());

            let leaf = new_leaf(&mut arena, &[], &[]);
            let mut children = [None; NUM_CHILDREN];
            children[0] = Some(leaf);
            let mut branch = new_branch(&mut arena, None, children).to_descent_stage(&arena);
            assert!(branch.can_descend());
            branch.descend(0, get_node(&arena))?;
            assert_matches!(branch, TrieDescentStage::AtLeaf { .. });

            let mut branch = new_branch(&mut arena, None, children).to_descent_stage(&arena);
            branch.descend(1, get_node_stub)?; // No child at this index
            assert_matches!(branch, TrieDescentStage::CutOff);

            Ok(())
        }
    }

    /// These tests verify the logic of aggregate (multi-subtree) descent.
    mod trie_descent {
        use super::*;
        use itertools::Itertools;
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
                get_node(self.arena)(ptr)
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
            let first_nibble = subtree_nibbles[0][0];
            for nibbles in &subtree_nibbles {
                assert_eq!(first_nibble, nibbles[0]); // ensure all subtrees have the same first nibble
            }
            let mut children = [None; NUM_CHILDREN];
            for i in 0..SUBTREES.len() {
                let idx = subtree_nibbles[i][1] as usize;
                assert!(children[idx].is_none()); // ensure all subtrees have a different second nibble
                children[idx] = Some(subtrees[i]);
            }
            let branch = new_branch(arena, None, children);
            let root = new_extension(arena, &[first_nibble], branch);
            let storage = TestStorage { root, arena };
            TrieDescent::new(storage).unwrap()
        }

        #[test]
        fn init_empty() {
            let mut arena = STArena::new("test".to_string());

            let subtrees = [new_leaf(&mut arena, &[], &[]); SUBTREES.len()];
            let trie_descent = init(&mut arena, subtrees);

            assert_eq!(trie_descent.total_memory(), SUBTREES.len() as u64 * EMPTY_LEAF_MEM);
            assert_eq!(trie_descent.aggregate_children_mem_usage().unwrap(), [0u64; NUM_CHILDREN]);
            assert!(trie_descent.next_step().unwrap().is_none());
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
            assert_eq!(trie_descent.aggregate_children_mem_usage().unwrap(), exp_children_mem);
            assert!(trie_descent.next_step().unwrap().is_some());
        }

        #[test]
        fn key_too_short() {
            let mut arena = STArena::new("test".to_string());

            // A single entry with 1-nibble key (not long enough for an account ID)
            let mut subtrees = [new_leaf(&mut arena, &[], &[]); SUBTREES.len()];
            subtrees[0] = new_leaf(&mut arena, &[1], &[1u8; 100]);
            assert!(init(&mut arena, subtrees).find_mem_usage_split().is_err());
        }

        #[test]
        fn invalid_bytes() {
            let mut arena = STArena::new("test".to_string());

            // Nibbles (0, 1, 0, 1) do not form a proper ASCII string
            let mut subtrees = [new_leaf(&mut arena, &[], &[]); SUBTREES.len()];
            subtrees[0] = new_leaf(&mut arena, &[0, 1, 0, 1], &[1u8; 100]);
            assert!(init(&mut arena, subtrees).find_mem_usage_split().is_err());
        }

        #[test]
        fn big_middle_account() {
            let mut arena = STArena::new("test".to_string());

            let mut subtrees = [new_leaf(&mut arena, &[], &[]); SUBTREES.len()];
            subtrees[0] = {
                let mut children = [None; NUM_CHILDREN];
                children[1] = Some(new_leaf(&mut arena, &[], &[1u8; 1000])); // aa
                children[2] = Some(new_leaf(&mut arena, &[], &[1u8; 10000])); // ab
                children[3] = Some(new_leaf(&mut arena, &[], &[1u8; 3000])); // ac
                let branch = new_branch(&mut arena, None, children);
                new_extension(&mut arena, &[6, 1, 6], branch)
            };
            let split = init(&mut arena, subtrees).find_mem_usage_split().unwrap();

            // The 'middle' account is placed at 6, 1, 6, 2 path. However, using it as a boundary
            // would result in a suboptimal split of roughly 100 bytes vs. 1300 bytes. There is a
            // simple optimization implemented such cases, which appends '-0' to the boundary
            // account ID. That's where the two extra bytes in the split path come from.
            assert_eq!(split.split_path_bytes(), [0x61, 0x62, 0x2d, 0x30]);
            assert_eq!(split.boundary_account.as_str(), "ab-0");
        }

        #[test]
        fn big_middle_account_with_children() {
            let mut arena = STArena::new("test".to_string());

            let mut subtrees = [new_leaf(&mut arena, &[], &[]); SUBTREES.len()];
            subtrees[0] = {
                let mut children = [None; NUM_CHILDREN];
                children[1] = Some(new_leaf(&mut arena, &[], &[1u8; 1000]));
                children[3] = Some(new_leaf(&mut arena, &[], &[1u8; 3000]));

                let mut children2 = [None; NUM_CHILDREN];
                children2[0] = Some(new_leaf(&mut arena, &[0], &[1u8; 5]));
                children2[1] = Some(new_leaf(&mut arena, &[0], &[1u8; 5]));

                children[2] = Some(new_branch(&mut arena, Some(&[1u8; 10000]), children2));
                let branch = new_branch(&mut arena, None, children);
                new_extension(&mut arena, &[6, 1, 6], branch)
            };
            let split = init(&mut arena, subtrees).find_mem_usage_split().unwrap();

            // The 'middle' account is placed at 6, 1, 6, 2 path. However, using it as a boundary
            // would result in a suboptimal split of roughly 100 bytes vs. 1300 bytes. There is a
            // simple optimization implemented such cases, which appends '-0' to the boundary
            // account ID. That's where the two extra bytes in the split path come from.
            assert_eq!(split.split_path_bytes(), [0x61, 0x62, 0x2d, 0x30]);
            assert_eq!(split.boundary_account.as_str(), "ab-0");
        }

        #[test]
        fn search_trivial_accounts() {
            let mut arena = STArena::new("test".to_string());

            // Only accounts subtree is populated. Others are empty.
            // The subtree structure looks like this:
            //
            //         ROOT
            //          │
            //          6           <-- extension
            //     1    │    2
            //     ┌────┴────┐
            //     │         │
            //     6         6      <-- extension
            //     │         │
            //  1  │  2   1  │  2
            //  ┌──┴──┐   ┌──┴──┐
            //  │     │   │     │
            //  X     X   X     X    <-- leaves with 1000-byte values
            //
            // The expected split path is nibbles 6, 1, 6, 2 with "-0" suffix (account "ab-0")
            // That means bytes 0x61, 0x62, 0x2d, 0x30

            let accounts_subtree = {
                let leaf = new_leaf(&mut arena, &[], &[1u8; 1000]);
                let branch = {
                    let mut children = [None; NUM_CHILDREN];
                    children[1] = Some(leaf);
                    children[2] = Some(leaf);
                    new_branch(&mut arena, None, children)
                };
                let ext_branch = new_extension(&mut arena, &[6], branch);
                let mut children = [None; NUM_CHILDREN];
                children[1] = Some(ext_branch);
                children[2] = Some(ext_branch);
                let top_branch = new_branch(&mut arena, None, children);
                new_extension(&mut arena, &[6], top_branch)
            };
            let mut subtrees = [new_leaf(&mut arena, &[], &[]); SUBTREES.len()];
            subtrees[0] = accounts_subtree;

            let trie_descent = init(&mut arena, subtrees);
            let total_memory = trie_descent.total_memory();
            let split = trie_descent.find_mem_usage_split().unwrap();
            assert_eq!(split.split_path_bytes(), vec![0x61, 0x62, 0x2d, 0x30]);
            assert_eq!(split.boundary_account.as_str(), "ab-0");
            assert_eq!(split.left_memory + split.right_memory, total_memory);

            // Now add some heavy contract code under 6, 2, 6, 2 path (account "bb") to change the optimal split path.
            subtrees[1] = new_leaf(&mut arena, &[6, 2, 6, 2], &[1u8; 5000]);
            let trie_descent = init(&mut arena, subtrees);
            let total_memory = trie_descent.total_memory();
            let split = trie_descent.find_mem_usage_split().unwrap();
            // The extra nibbles from extension in contract code subtree should *not* be included.
            // Trie descent is over when a leaf is reached in the accounts' subtree.
            assert_eq!(split.split_path_bytes(), vec![0x62, 0x62]);
            assert_eq!(split.boundary_account.as_str(), "bb");
            assert_eq!(split.left_memory + split.right_memory, total_memory);
        }
    }

    mod find_trie_split {
        use super::*;
        use crate::test_utils::TestTriesBuilder;
        use crate::trie::mem::iter::MemTrieIteratorInner;
        use crate::trie::update::TrieUpdateResult;
        use crate::{ShardTries, Trie, TrieUpdate, set, set_access_key, set_account};
        use itertools::Itertools;
        use near_crypto::{ED25519PublicKey, PublicKey};
        use near_primitives::account::AccessKey;
        use near_primitives::hash::CryptoHash;
        use near_primitives::shard_layout::ShardUId;
        use near_primitives::test_utils::account_new;
        use near_primitives::trie_key::TrieKey;
        use near_primitives::types::{AccountId, Balance, StateChangeCause, StateRoot};
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
            /// Helper method to get the split yielded by a pre-defined key.
            fn get_split(mut self, key_bytes: &[u8]) -> FindSplitResult<TrieSplit> {
                for nibble in bytes_to_nibbles(key_bytes) {
                    let children_mem_usage = self.aggregate_children_mem_usage()?;
                    let child_mem_usage = children_mem_usage[nibble as usize];
                    let left_mem_usage = children_mem_usage[..nibble as usize].iter().sum();
                    self.descend_step(nibble, child_mem_usage, left_mem_usage)?;
                }

                let boundary_account = self.current_account().unwrap();
                Ok(TrieSplit::new(
                    boundary_account,
                    self.left_memory,
                    self.right_memory + self.middle_memory,
                ))
            }
        }

        /// Helper function to get the split yielded by the given boundary account.
        /// Assumes the trie is using memtries.
        fn get_memtrie_split(
            trie: &Trie,
            boundary_account: &AccountId,
        ) -> FindSplitResult<TrieSplit> {
            let key_bytes = boundary_account.as_bytes();
            let memtries = trie.lock_memtries().unwrap();
            let trie_storage = MemTrieIteratorInner::new(&memtries, trie);
            TrieDescent::new(trie_storage)?.get_split(key_bytes)
        }

        #[test]
        fn big_random_trie() {
            // Seeds between 0 and 10000 were tested and these were picked as they uncovered
            // some bugs and corner cases.
            big_random_trie_inner(1).unwrap();
            big_random_trie_inner(99).unwrap();
            big_random_trie_inner(128).unwrap();
            big_random_trie_inner(146).unwrap();
        }

        fn create_accounts(
            trie_update: &mut TrieUpdate,
            rng: &mut StdRng,
        ) -> anyhow::Result<Vec<AccountId>> {
            // Set up 1000 accounts with random IDs (between 5 and 15 characters)
            // and random balance between 10 and 1000 NEAR.
            let num_accounts = 1000;
            let lengths = Uniform::try_from(5..=15)?;
            let chars = Uniform::try_from('a'..='z')?;
            let balances = Uniform::from(10_u128..=1000);
            let mut rand_account = || -> AccountId {
                let length = lengths.sample(&mut *rng);
                let account_str: String = chars.sample_iter(&mut *rng).take(length).collect();
                account_str.parse().unwrap()
            };
            let mut account_ids = (0..num_accounts).map(|_| rand_account()).collect_vec();

            for account_id in &account_ids {
                let balance = Balance::from_near(balances.sample(rng));
                let account = account_new(balance, CryptoHash::default());
                set_account(trie_update, account_id.clone(), &account);

                // Randomly assign access keys to approx. half of the accounts
                if rng.gen_bool(0.5) {
                    let mut pub_key_bytes = [0u8; 32];
                    rng.fill_bytes(&mut pub_key_bytes);
                    let pub_key = PublicKey::from(ED25519PublicKey::from(pub_key_bytes));
                    let access_key = AccessKey::full_access();
                    set_access_key(trie_update, account_id.clone(), pub_key, &access_key);
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
            account_ids.shuffle(rng);
            for i in 0..num_contracts {
                let account_id = &account_ids[i];
                let code_length = code_lengths.sample(rng);
                let mut contract_code = vec![0u8; code_length];
                rng.fill_bytes(&mut contract_code);
                set(
                    trie_update,
                    TrieKey::ContractCode { account_id: account_id.clone() },
                    &contract_code,
                );

                let num_keys = num_keys.sample(rng);
                for _ in 0..num_keys {
                    let key_length = key_lengths.sample(rng);
                    let mut key = vec![0u8; key_length];
                    rng.fill_bytes(&mut key);
                    let value_length = value_lengths.sample(rng);
                    let mut value = vec![0u8; value_length];
                    rng.fill_bytes(&mut value);
                    set(
                        trie_update,
                        TrieKey::ContractData { account_id: account_id.clone(), key },
                        &value,
                    );
                }
            }

            Ok(account_ids)
        }

        fn start_setup() -> (ShardTries, ShardUId, TrieUpdate) {
            let (shard_tries, layout) =
                TestTriesBuilder::new().with_in_memory_tries(true).with_flat_storage(true).build2();
            let shard_uid = layout.get_shard_uid(0).unwrap();
            let trie = shard_tries.get_trie_for_shard(shard_uid, Trie::EMPTY_ROOT);
            let trie_update = TrieUpdate::new(trie);
            (shard_tries, shard_uid, trie_update)
        }

        fn finish_setup(
            mut trie_update: TrieUpdate,
            shard_tries: &ShardTries,
            shard_uid: ShardUId,
        ) -> StateRoot {
            trie_update.commit(StateChangeCause::InitialState);
            let TrieUpdateResult { trie_changes, .. } = trie_update.finalize().unwrap();
            let mut store_update = shard_tries.store_update();
            let new_root = shard_tries.apply_all(&trie_changes, shard_uid, &mut store_update);
            shard_tries.apply_memtrie_changes(&trie_changes, shard_uid, 0);
            store_update.commit().unwrap();

            new_root
        }

        fn big_random_trie_inner(seed: u64) -> anyhow::Result<()> {
            println!("Seed: {seed}");
            let mut rng = StdRng::seed_from_u64(seed);

            // Initialize an empty trie and start an update to populate it with some data
            let (shard_tries, shard_uid, mut trie_update) = start_setup();
            let mut account_ids = create_accounts(&mut trie_update, &mut rng)?;
            let root = finish_setup(trie_update, &shard_tries, shard_uid);

            // Find the boundary account
            let trie =
                shard_tries.get_trie_for_shard(shard_uid, root).recording_reads_new_recorder();
            let trie_split = find_trie_split(&trie)?;
            println!("Found trie split: {trie_split:?}");
            let boundary_account = &trie_split.boundary_account;

            // Verify if running the algorithm on recorded storage gives the same result
            let recorded_storage = trie.recorded_storage().unwrap();
            let recorded_trie = Trie::from_recorded_storage(recorded_storage, root, true);
            let recorded_trie_split = find_trie_split(&recorded_trie)?;
            assert_eq!(trie_split, recorded_trie_split);

            // Assert that previous and next account gives worse splits than the account found
            // by `find_trie_split`. As the accounts are sorted, we don't need to check them all.
            // If the split given by account at index `i` is better than the split given by accounts
            // at `i-1` and `i+1`, it is also the best split of all the accounts.
            account_ids.sort();
            let (left_account, right_account) =
                find_neighbor_accounts(&account_ids, boundary_account);
            let trie = shard_tries.get_trie_for_shard(shard_uid, root);

            // This assertion acts as a sanity check for `get_memtrie_split`
            let verify_split = get_memtrie_split(&trie, boundary_account)?;
            assert_eq!(verify_split, trie_split);

            if let Some(left_account) = left_account {
                let left_split = get_memtrie_split(&trie, left_account)?;
                println!("Left account split: {left_split:?}");
                assert!(left_split.mem_diff() >= trie_split.mem_diff());
            }

            if let Some(right_account) = right_account {
                let right_split = get_memtrie_split(&trie, right_account)?;
                println!("Right account split: {right_split:?}");
                assert!(right_split.mem_diff() >= trie_split.mem_diff());
            }

            Ok(())
        }

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

        fn create_big_account(
            trie_update: &mut TrieUpdate,
            rng: &mut StdRng,
            num_bytes: usize,
        ) -> AccountId {
            let chars = Uniform::try_from('a'..='z').unwrap();
            let big_account_id: AccountId =
                chars.sample_iter(&mut *rng).take(3).collect::<String>().parse().unwrap();
            let balance = Balance::from_near(1000);
            let account = account_new(balance, CryptoHash::default());
            set_account(trie_update, big_account_id.clone(), &account);
            let mut value = vec![0u8; num_bytes];
            rng.fill_bytes(&mut value);
            set(
                trie_update,
                TrieKey::ContractData { account_id: big_account_id.clone(), key: vec![1] },
                &value,
            );

            big_account_id
        }

        #[test]
        fn random_trie_big_account() {
            // Seeds between 0 and 1000 were tested.
            // These are copied from `big_random_trie` test
            random_trie_big_account_inner(1).unwrap();
            random_trie_big_account_inner(99).unwrap();
            random_trie_big_account_inner(128).unwrap();
            random_trie_big_account_inner(146).unwrap();
        }

        fn random_trie_big_account_inner(seed: u64) -> anyhow::Result<()> {
            println!("Seed: {seed}");
            let mut rng = StdRng::seed_from_u64(seed);
            let big_account_bytes = 1_000_000;

            let (shard_tries, shard_uid, mut trie_update) = start_setup();
            create_accounts(&mut trie_update, &mut rng)?;
            let account_id = create_big_account(&mut trie_update, &mut rng, big_account_bytes);
            let root = finish_setup(trie_update, &shard_tries, shard_uid);
            let trie = shard_tries.get_trie_for_shard(shard_uid, root);

            let trie_split = find_trie_split(&trie)?;
            let boundary_account = &trie_split.boundary_account;

            // Check if putting the big account into the other child (left vs. right) does not
            // result in a better split
            let big_account_bytes = big_account_bytes as u64;
            if trie_split.left_memory > trie_split.right_memory {
                assert!(&account_id < boundary_account);
                assert!(trie_split.left_memory - big_account_bytes < trie_split.right_memory);
            } else {
                assert!(&account_id >= boundary_account);
                assert!(trie_split.right_memory - big_account_bytes < trie_split.left_memory);
            }

            Ok(())
        }
    }
}
