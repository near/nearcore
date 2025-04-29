use std::collections::{BTreeMap, HashMap};

use near_primitives::errors::StorageError;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::state::FlatStateValue;

use crate::trie::ops::insert_delete::GenericTrieUpdateInsertDelete;
use crate::trie::ops::interface::{
    GenericNodeOrIndex, GenericTrieNode, GenericTrieNodeWithSize, GenericTrieUpdate,
    GenericTrieValue, GenericUpdatedTrieNode, GenericUpdatedTrieNodeWithSize, UpdatedNodeId,
};
use crate::trie::trie_recording::TrieRecorder;
use crate::trie::{AccessOptions, Children, MemTrieChanges, TrieRefcountDeltaMap};
use crate::{RawTrieNode, RawTrieNodeWithSize, TrieChanges};

use super::arena::{ArenaMemory, ArenaMut};
use super::flexible_data::children::ChildrenView;
use super::metrics::MEMTRIE_NUM_NODES_CREATED_FROM_UPDATES;
use super::node::{InputMemTrieNode, MemTrieNodeId, MemTrieNodeView};

pub type OldOrUpdatedNodeId = GenericNodeOrIndex<MemTrieNodeId>;

pub type MemTrieNode = GenericTrieNode<MemTrieNodeId, FlatStateValue>;

pub type UpdatedMemTrieNode = GenericUpdatedTrieNode<MemTrieNodeId, FlatStateValue>;

pub type MemTrieNodeWithSize = GenericTrieNodeWithSize<MemTrieNodeId, FlatStateValue>;

pub type UpdatedMemTrieNodeWithSize = GenericUpdatedTrieNodeWithSize<MemTrieNodeId, FlatStateValue>;

impl MemTrieNode {
    pub fn from_existing_node_view<'a, M: ArenaMemory>(view: MemTrieNodeView<'a, M>) -> Self {
        match view {
            MemTrieNodeView::Leaf { extension, value } => MemTrieNode::Leaf {
                extension: extension.to_vec().into_boxed_slice(),
                value: value.to_flat_value(),
            },
            MemTrieNodeView::Branch { children, .. } => MemTrieNode::Branch {
                children: Box::new(Self::convert_children_to_updated(children)),
                value: None,
            },
            MemTrieNodeView::BranchWithValue { children, value, .. } => MemTrieNode::Branch {
                children: Box::new(Self::convert_children_to_updated(children)),
                value: Some(value.to_flat_value()),
            },
            MemTrieNodeView::Extension { extension, child, .. } => MemTrieNode::Extension {
                extension: extension.to_vec().into_boxed_slice(),
                child: child.id(),
            },
        }
    }

    fn convert_children_to_updated<'a, M: ArenaMemory>(
        view: ChildrenView<'a, M>,
    ) -> [Option<MemTrieNodeId>; 16] {
        let mut children = [None; 16];
        for i in 0..16 {
            if let Some(child) = view.get(i) {
                children[i] = Some(child.id());
            }
        }
        children
    }
}

impl MemTrieNodeWithSize {
    pub fn from_existing_node_view<'a, M: ArenaMemory>(view: MemTrieNodeView<'a, M>) -> Self {
        let memory_usage = view.memory_usage();
        Self { node: MemTrieNode::from_existing_node_view(view), memory_usage }
    }
}

/// Allows using in-memory tries to construct the trie node changes entirely
/// (for both in-memory and on-disk updates) because it's much faster.
pub enum TrackingMode<'a> {
    /// Don't track any nodes.
    None,
    /// Track disk refcount changes for trie nodes.
    Refcounts,
    /// Track disk refcount changes and record all accessed trie nodes.
    /// The latter one is needed to record storage proof which is handled by
    /// `TrieRecorder`.
    /// The main case why recording is needed is a branch with two children,
    /// one of which got removed. In this case we need to read another child
    /// and squash it together with parent.
    RefcountsAndAccesses(&'a mut TrieRecorder),
}

/// Tracks intermediate trie changes, final version of which is to be committed
/// to disk after finishing trie update.
struct TrieChangesTracker<'a> {
    /// Counts hashes deleted so far.
    /// Includes hashes of both trie nodes and state values!
    refcount_deleted_hashes: BTreeMap<CryptoHash, u32>,
    /// Counts state values inserted so far.
    /// Separated from `refcount_deleted_hashes` to postpone hash computation
    /// as far as possible.
    refcount_inserted_values: BTreeMap<Vec<u8>, u32>,
    /// Recorder for observed internal nodes.
    /// Note that negative `refcount_deleted_hashes` does not fully cover it,
    /// as node or value of the same hash can be removed and inserted for the
    /// same update in different parts of trie!
    recorder: Option<&'a mut TrieRecorder>,
}

impl<'a> TrieChangesTracker<'a> {
    fn with_recorder(recorder: Option<&'a mut TrieRecorder>) -> Self {
        Self {
            refcount_deleted_hashes: BTreeMap::new(),
            refcount_inserted_values: BTreeMap::new(),
            recorder,
        }
    }

    fn record<M: ArenaMemory>(&mut self, node: &MemTrieNodeView<'a, M>) {
        let node_hash = node.node_hash();
        let raw_node_serialized = borsh::to_vec(&node.to_raw_trie_node_with_size()).unwrap();
        *self.refcount_deleted_hashes.entry(node_hash).or_default() += 1;
        if let Some(recorder) = self.recorder.as_mut() {
            recorder.record(&node_hash, raw_node_serialized.into());
        }
    }

    /// Prepare final refcount difference and also return all trie accesses.
    fn finalize(self) -> TrieRefcountDeltaMap {
        let mut refcount_delta_map = TrieRefcountDeltaMap::new();
        for (value, rc) in self.refcount_inserted_values {
            refcount_delta_map.add(hash(&value), value, rc);
        }
        for (hash, rc) in self.refcount_deleted_hashes {
            refcount_delta_map.subtract(hash, rc);
        }
        refcount_delta_map
    }
}

/// Structure to build an update to the in-memory trie.
pub struct MemTrieUpdate<'a, M: ArenaMemory> {
    /// The original root before updates. It is None iff the original trie had no keys.
    root: Option<MemTrieNodeId>,
    memory: &'a M,
    shard_uid: String, // for metrics only
    /// All the new nodes that are to be constructed. A node may be None if
    /// (1) temporarily we take out the node from the slot to process it and put it back
    /// later; or (2) the node is deleted afterwards.
    pub updated_nodes: Vec<Option<UpdatedMemTrieNodeWithSize>>,
    /// Tracks trie changes necessary to make on-disk updates and recorded
    /// storage.
    nodes_tracker: Option<TrieChangesTracker<'a>>,
}

impl<'a, M: ArenaMemory> GenericTrieUpdate<'a, MemTrieNodeId, FlatStateValue>
    for MemTrieUpdate<'a, M>
{
    fn ensure_updated(
        &mut self,
        node: GenericNodeOrIndex<MemTrieNodeId>,
        _opts: AccessOptions,
    ) -> Result<UpdatedNodeId, StorageError> {
        Ok(match node {
            GenericNodeOrIndex::Old(node_id) => self.convert_existing_to_updated(Some(node_id)),
            GenericNodeOrIndex::Updated(node_id) => node_id,
        })
    }

    fn take_node(&mut self, index: UpdatedNodeId) -> UpdatedMemTrieNodeWithSize {
        self.updated_nodes.get_mut(index).unwrap().take().expect("Node taken twice")
    }

    fn place_node_at(&mut self, index: UpdatedNodeId, node: UpdatedMemTrieNodeWithSize) {
        assert!(self.updated_nodes[index].is_none(), "Node placed twice");
        self.updated_nodes[index] = Some(node);
    }

    fn get_node_ref(&self, node_id: UpdatedNodeId) -> &UpdatedMemTrieNodeWithSize {
        self.updated_nodes[node_id].as_ref().unwrap()
    }

    fn place_node(&mut self, node: UpdatedMemTrieNodeWithSize) -> UpdatedNodeId {
        let index = self.updated_nodes.len();
        self.updated_nodes.push(Some(node));
        index
    }

    fn store_value(&mut self, value: GenericTrieValue) -> FlatStateValue {
        let (flat_value, full_value) = match value {
            // If value is provided only for memtrie, it is flat, so we can't
            // record nodes. Just return flat value back.
            // TODO: check consistency with trie recorder setup.
            // `GenericTrieValue::MemtrieOnly` must not be used if
            // `nodes_tracker` is set and vice versa.
            GenericTrieValue::MemtrieOnly(flat_value) => return flat_value,
            GenericTrieValue::MemtrieAndDisk(full_value) => {
                (FlatStateValue::on_disk(full_value.as_slice()), full_value)
            }
        };

        // Otherwise, record disk changes if needed.
        let Some(nodes_tracker) = self.nodes_tracker.as_mut() else {
            return flat_value;
        };
        *nodes_tracker.refcount_inserted_values.entry(full_value).or_default() += 1;

        flat_value
    }

    fn delete_value(&mut self, value: FlatStateValue) -> Result<(), StorageError> {
        let Some(nodes_tracker) = self.nodes_tracker.as_mut() else {
            return Ok(());
        };

        let hash = value.to_value_ref().hash;
        *nodes_tracker.refcount_deleted_hashes.entry(hash).or_default() += 1;
        Ok(())
    }
}

impl<'a, M: ArenaMemory> MemTrieUpdate<'a, M> {
    pub fn new(
        root: Option<MemTrieNodeId>,
        memory: &'a M,
        shard_uid: String,
        mode: TrackingMode<'a>,
    ) -> Self {
        let nodes_tracker = match mode {
            TrackingMode::None => None,
            TrackingMode::Refcounts => Some(TrieChangesTracker::with_recorder(None)),
            TrackingMode::RefcountsAndAccesses(recorder) => {
                Some(TrieChangesTracker::with_recorder(Some(recorder)))
            }
        };
        let mut trie_update =
            Self { root, memory, shard_uid, updated_nodes: vec![], nodes_tracker };
        assert_eq!(trie_update.convert_existing_to_updated(root), 0usize);
        trie_update
    }

    /// Creates a new updated node, assigning it a new ID.
    fn new_updated_node(&mut self, node: UpdatedMemTrieNodeWithSize) -> UpdatedNodeId {
        let index = self.updated_nodes.len();
        self.updated_nodes.push(Some(node));
        index
    }

    /// This is called when we need to mutate a subtree of the original trie.
    /// It decrements the refcount of the original trie node (since logically
    /// we are removing it), and creates a new node that is equivalent to the
    /// original node. The ID of the new node is returned.
    ///
    /// If the original node is None, it is a marker for the root of an empty
    /// trie.
    fn convert_existing_to_updated(&mut self, node: Option<MemTrieNodeId>) -> UpdatedNodeId {
        let Some(node) = node else {
            return self.new_updated_node(UpdatedMemTrieNodeWithSize::empty());
        };
        let node_view = node.as_ptr(self.memory).view();
        if let Some(tracked_trie_changes) = self.nodes_tracker.as_mut() {
            tracked_trie_changes.record(&node_view);
        }
        self.new_updated_node(MemTrieNodeWithSize::from_existing_node_view(node_view).into())
    }

    /// Inserts the given key value pair into the trie.
    pub fn insert(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StorageError> {
        self.generic_insert(0, key, GenericTrieValue::MemtrieAndDisk(value), AccessOptions::DEFAULT)
    }

    /// Inserts the given key value pair into the trie, but the value may be a reference.
    /// This is used to update the in-memory trie only, without caring about on-disk changes.
    pub fn insert_memtrie_only(
        &mut self,
        key: &[u8],
        value: FlatStateValue,
    ) -> Result<(), StorageError> {
        self.generic_insert(0, key, GenericTrieValue::MemtrieOnly(value), AccessOptions::DEFAULT)
    }
}

impl<'a, M: ArenaMemory> MemTrieUpdate<'a, M> {
    /// To construct the new trie nodes, we need to create the new nodes in an
    /// order such that children are created before their parents - essentially
    /// a topological sort. We do this via a post-order traversal of the
    /// updated nodes. After this function, `ordered_nodes` contains the IDs of
    /// the updated nodes in the order they should be created.
    fn post_order_traverse_updated_nodes(
        node_id: UpdatedNodeId,
        updated_nodes: &Vec<Option<UpdatedMemTrieNodeWithSize>>,
        ordered_nodes: &mut Vec<UpdatedNodeId>,
    ) {
        let node = updated_nodes[node_id].as_ref().unwrap();
        match &node.node {
            UpdatedMemTrieNode::Empty => {
                assert_eq!(node_id, 0); // only root can be empty
                return;
            }
            UpdatedMemTrieNode::Branch { children, .. } => {
                for child in children.iter() {
                    if let Some(OldOrUpdatedNodeId::Updated(child_node_id)) = child {
                        Self::post_order_traverse_updated_nodes(
                            *child_node_id,
                            updated_nodes,
                            ordered_nodes,
                        );
                    }
                }
            }
            UpdatedMemTrieNode::Extension { child, .. } => {
                if let OldOrUpdatedNodeId::Updated(child_node_id) = child {
                    Self::post_order_traverse_updated_nodes(
                        *child_node_id,
                        updated_nodes,
                        ordered_nodes,
                    );
                }
            }
            _ => {}
        }
        ordered_nodes.push(node_id);
    }

    /// For each node in `ordered_nodes`, computes its hash and serialized data.
    /// `ordered_nodes` is expected to follow the post-order traversal of the
    /// updated nodes.
    /// `updated_nodes` must be indexed by the node IDs in `ordered_nodes`.
    pub(crate) fn compute_hashes_and_serialized_nodes(
        &self,
        ordered_nodes: &Vec<UpdatedNodeId>,
        updated_nodes: &Vec<Option<UpdatedMemTrieNodeWithSize>>,
    ) -> Vec<(UpdatedNodeId, CryptoHash, Vec<u8>)> {
        let memory = self.memory;
        let mut result = Vec::<(CryptoHash, Vec<u8>)>::new();
        for _ in 0..updated_nodes.len() {
            result.push((CryptoHash::default(), Vec::new()));
        }
        let get_hash =
            |node: OldOrUpdatedNodeId, result: &Vec<(CryptoHash, Vec<u8>)>| -> CryptoHash {
                match node {
                    OldOrUpdatedNodeId::Updated(node_id) => result[node_id].0,
                    // IMPORTANT: getting a node hash for a child doesn't
                    // record a new node read. In recorded storage, child node
                    // is referenced by its hash, and we don't need to need the
                    // whole node to verify parent hash.
                    // TODO(#12361): consider fixing it, perhaps by taking this
                    // hash from old version of the parent node.
                    OldOrUpdatedNodeId::Old(node_id) => node_id.as_ptr(memory).view().node_hash(),
                }
            };

        for node_id in ordered_nodes {
            let node = updated_nodes[*node_id].as_ref().unwrap();
            let raw_node = match &node.node {
                UpdatedMemTrieNode::Empty => unreachable!(),
                UpdatedMemTrieNode::Branch { children, value } => {
                    let mut child_hashes = vec![];
                    for child in children.iter() {
                        match child {
                            Some(child) => {
                                let child_hash = get_hash(*child, &result);
                                child_hashes.push(Some(child_hash));
                            }
                            None => {
                                child_hashes.push(None);
                            }
                        }
                    }
                    let children = Children(child_hashes.as_slice().try_into().unwrap());
                    let value_ref = value.as_ref().map(|value| value.to_value_ref());
                    RawTrieNode::branch(children, value_ref)
                }
                UpdatedMemTrieNode::Extension { extension, child } => {
                    let child_hash = get_hash(*child, &result);
                    RawTrieNode::Extension(extension.to_vec(), child_hash)
                }
                UpdatedMemTrieNode::Leaf { extension, value } => {
                    RawTrieNode::Leaf(extension.to_vec(), value.to_value_ref())
                }
            };

            let memory_usage = node.memory_usage;
            let raw_node_with_size = RawTrieNodeWithSize { node: raw_node, memory_usage };
            let node_serialized = borsh::to_vec(&raw_node_with_size).unwrap();
            let node_hash = hash(&node_serialized);
            result[*node_id] = (node_hash, node_serialized);
        }

        ordered_nodes
            .iter()
            .map(|node_id| {
                let (hash, serialized) = &mut result[*node_id];
                (*node_id, *hash, std::mem::take(serialized))
            })
            .collect()
    }

    /// Converts the changes to memtrie changes. Also returns the list of new nodes inserted,
    /// in hash and serialized form.
    fn to_memtrie_changes_internal(self) -> (MemTrieChanges, Vec<(CryptoHash, Vec<u8>)>) {
        MEMTRIE_NUM_NODES_CREATED_FROM_UPDATES
            .with_label_values(&[&self.shard_uid])
            .inc_by(self.updated_nodes.len() as u64);
        let mut ordered_nodes = Vec::new();
        Self::post_order_traverse_updated_nodes(0, &self.updated_nodes, &mut ordered_nodes);

        let hashes_and_serialized_nodes =
            self.compute_hashes_and_serialized_nodes(&ordered_nodes, &self.updated_nodes);

        let node_ids_with_hashes = hashes_and_serialized_nodes
            .iter()
            .map(|(node_id, hash, _)| (*node_id, *hash))
            .collect();
        (
            MemTrieChanges { node_ids_with_hashes, updated_nodes: self.updated_nodes },
            hashes_and_serialized_nodes
                .into_iter()
                .map(|(_, hash, serialized)| (hash, serialized))
                .collect(),
        )
    }

    /// Converts the updates to memtrie changes only.
    pub fn to_memtrie_changes_only(self) -> MemTrieChanges {
        let (memtrie_changes, _) = self.to_memtrie_changes_internal();
        memtrie_changes
    }

    /// Converts the updates to trie changes as well as memtrie changes.
    pub(crate) fn to_trie_changes(mut self) -> TrieChanges {
        let old_root =
            self.root.map(|root| root.as_ptr(self.memory).view().node_hash()).unwrap_or_default();
        let mut refcount_changes = self
            .nodes_tracker
            .take()
            .expect("Cannot to_trie_changes for memtrie changes only")
            .finalize();
        let (memtrie_changes, hashes_and_serialized) = self.to_memtrie_changes_internal();

        // We've accounted for the dereferenced nodes, as well as value addition/subtractions.
        // The only thing left is to increment refcount for all new nodes.
        for (node_hash, node_serialized) in hashes_and_serialized {
            refcount_changes.add(node_hash, node_serialized, 1);
        }
        let (insertions, deletions) = refcount_changes.into_changes();

        TrieChanges {
            old_root,
            new_root: memtrie_changes
                .node_ids_with_hashes
                .last()
                .map(|(_, hash)| *hash)
                .unwrap_or_default(),
            insertions,
            deletions,
            memtrie_changes: Some(memtrie_changes),
            children_memtrie_changes: Default::default(),
        }
    }
}

/// Applies the given memtrie changes to the in-memory trie data structure.
/// Returns the new root hash.
pub(super) fn construct_root_from_changes<A: ArenaMut>(
    arena: &mut A,
    changes: &MemTrieChanges,
) -> Option<MemTrieNodeId> {
    let mut last_node_id: Option<MemTrieNodeId> = None;
    let map_to_new_node_id = |node_id: OldOrUpdatedNodeId,
                              old_to_new_map: &HashMap<UpdatedNodeId, MemTrieNodeId>|
     -> MemTrieNodeId {
        match node_id {
            OldOrUpdatedNodeId::Updated(node_id) => *old_to_new_map.get(&node_id).unwrap(),
            OldOrUpdatedNodeId::Old(node_id) => node_id,
        }
    };

    let mut updated_to_new_map = HashMap::<UpdatedNodeId, MemTrieNodeId>::new();
    let updated_nodes = &changes.updated_nodes;
    let node_ids_with_hashes = &changes.node_ids_with_hashes;
    for (node_id, node_hash) in node_ids_with_hashes {
        let node = updated_nodes.get(*node_id).unwrap().clone().unwrap();
        let node = match &node.node {
            UpdatedMemTrieNode::Empty => unreachable!(),
            UpdatedMemTrieNode::Branch { children, value } => {
                let mut new_children = [None; 16];
                for i in 0..16 {
                    if let Some(child) = children[i] {
                        new_children[i] = Some(map_to_new_node_id(child, &updated_to_new_map));
                    }
                }
                match value {
                    Some(value) => {
                        InputMemTrieNode::BranchWithValue { children: new_children, value }
                    }
                    None => InputMemTrieNode::Branch { children: new_children },
                }
            }
            UpdatedMemTrieNode::Extension { extension, child } => InputMemTrieNode::Extension {
                extension,
                child: map_to_new_node_id(*child, &updated_to_new_map),
            },
            UpdatedMemTrieNode::Leaf { extension, value } => {
                InputMemTrieNode::Leaf { value, extension }
            }
        };
        let mem_node_id = MemTrieNodeId::new_with_hash(arena, node, *node_hash);
        updated_to_new_map.insert(*node_id, mem_node_id);
        last_node_id = Some(mem_node_id);
    }

    last_node_id
}

#[cfg(test)]
mod tests {
    use crate::test_utils::TestTriesBuilder;
    use crate::trie::mem::arena::hybrid::HybridArena;
    use crate::trie::mem::lookup::memtrie_lookup;
    use crate::trie::mem::memtrie_update::GenericTrieUpdateInsertDelete;
    use crate::trie::mem::memtries::MemTries;
    use crate::trie::{AccessOptions, MemTrieChanges};
    use crate::{KeyLookupMode, ShardTries, TrieChanges};
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::state::{FlatStateValue, ValueRef};
    use near_primitives::types::{BlockHeight, StateRoot};
    use rand::Rng;
    use std::collections::{HashMap, HashSet};

    use super::TrackingMode;

    struct TestTries {
        mem: MemTries,
        disk: ShardTries,
        truth: HashMap<Vec<u8>, Option<ValueRef>>,
        state_root: StateRoot,
        check_deleted_keys: bool,
    }

    impl TestTries {
        fn new(check_deleted_keys: bool) -> Self {
            let mem = MemTries::new(ShardUId::single_shard());
            let disk = TestTriesBuilder::new().build();
            Self {
                mem,
                disk,
                truth: HashMap::new(),
                state_root: StateRoot::default(),
                check_deleted_keys,
            }
        }

        fn make_all_changes(&self, changes: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> TrieChanges {
            let mut update =
                self.mem.update(self.state_root, TrackingMode::Refcounts).unwrap_or_else(|_| {
                    panic!("Trying to update root {:?} but it's not in memtries", self.state_root)
                });
            for (key, value) in changes {
                if let Some(value) = value {
                    update.insert(&key, value).unwrap();
                } else {
                    update.generic_delete(0, &key, AccessOptions::DEFAULT).unwrap();
                }
            }
            update.to_trie_changes()
        }

        fn make_memtrie_changes_only(
            &self,
            changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        ) -> MemTrieChanges {
            let mut update =
                self.mem.update(self.state_root, TrackingMode::None).unwrap_or_else(|_| {
                    panic!("Trying to update root {:?} but it's not in memtries", self.state_root)
                });
            for (key, value) in changes {
                if let Some(value) = value {
                    update.insert_memtrie_only(&key, FlatStateValue::on_disk(&value)).unwrap();
                } else {
                    update.generic_delete(0, &key, AccessOptions::DEFAULT).unwrap();
                }
            }
            update.to_memtrie_changes_only()
        }

        fn make_disk_changes_only(&self, changes: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> TrieChanges {
            let trie = self.disk.get_trie_for_shard(ShardUId::single_shard(), self.state_root);
            trie.update(changes, AccessOptions::DEFAULT).unwrap()
        }

        fn check_consistency_across_all_changes_and_apply(
            &mut self,
            changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        ) {
            // First check consistency between the changes.
            let memtrie_changes = self.make_memtrie_changes_only(changes.clone());
            let disk_changes = self.make_disk_changes_only(changes.clone());
            let mut all_changes = self.make_all_changes(changes.clone());

            let memtrie_changes_from_all_changes = all_changes.memtrie_changes.take().unwrap();
            assert_eq!(memtrie_changes, memtrie_changes_from_all_changes);
            assert_eq!(disk_changes, all_changes);

            // Then apply the changes and check consistency of new state roots.
            let new_state_root_from_mem = self.mem.apply_memtrie_changes(0, &memtrie_changes);
            let mut store_update = self.disk.store_update();
            let new_state_root_from_disk =
                self.disk.apply_all(&disk_changes, ShardUId::single_shard(), &mut store_update);
            assert_eq!(new_state_root_from_mem, new_state_root_from_disk);
            store_update.commit().unwrap();
            self.state_root = new_state_root_from_mem;

            // Update our truth.
            for (key, value) in changes {
                if let Some(value) = value {
                    self.truth.insert(key, Some(ValueRef::new(&value)));
                } else {
                    if self.check_deleted_keys {
                        self.truth.insert(key, None);
                    } else {
                        self.truth.remove(&key);
                    }
                }
            }

            // Check the truth against both memtrie and on-disk trie.
            for (key, value_ref) in &self.truth {
                let memtrie_root = if self.state_root == StateRoot::default() {
                    None
                } else {
                    Some(self.mem.get_root(&self.state_root).unwrap())
                };
                let disk_trie =
                    self.disk.get_trie_for_shard(ShardUId::single_shard(), self.state_root);
                let memtrie_result =
                    memtrie_root.and_then(|memtrie_root| memtrie_lookup(memtrie_root, key, None));
                let disk_result = disk_trie
                    .get_optimized_ref(key, KeyLookupMode::MemOrTrie, AccessOptions::DEFAULT)
                    .unwrap();
                if let Some(value_ref) = value_ref {
                    let memtrie_value_ref = memtrie_result
                        .unwrap_or_else(|| {
                            panic!("Key {} is in truth but not in memtrie", hex::encode(key))
                        })
                        .to_flat_value()
                        .to_value_ref();
                    let disk_value_ref = disk_result
                        .unwrap_or_else(|| {
                            panic!("Key {} is in truth but not in disk trie", hex::encode(key))
                        })
                        .into_value_ref();
                    assert_eq!(
                        memtrie_value_ref,
                        *value_ref,
                        "Value for key {} is incorrect for memtrie",
                        hex::encode(key)
                    );
                    assert_eq!(
                        disk_value_ref,
                        *value_ref,
                        "Value for key {} is incorrect for disk trie",
                        hex::encode(key)
                    );
                } else {
                    assert!(
                        memtrie_result.is_none(),
                        "Key {} is not in truth but is in memtrie",
                        hex::encode(key)
                    );
                    assert!(
                        disk_result.is_none(),
                        "Key {} is not in truth but is in disk trie",
                        hex::encode(key)
                    );
                }
            }
        }
    }

    fn parse_changes(s: &str) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        s.split('\n')
            .map(|s| s.split('#').next().unwrap().trim())
            .filter(|s| !s.is_empty())
            .map(|s| {
                let mut parts = s.split(" = ");
                let key = parts.next().unwrap();
                let value = parts.next().unwrap();
                let value =
                    if value == "delete" { None } else { Some(hex::decode(value).unwrap()) };
                (hex::decode(key).unwrap(), value)
            })
            .collect()
    }

    #[test]
    fn test_meta_parse_changes() {
        // Make sure that our test utility itself is fine.
        let changes = parse_changes(
            "
                00ff = 00000001  # comments
                01dd = delete
                # comments
                02ac = 0003
            ",
        );
        assert_eq!(
            changes,
            vec![
                (vec![0x00, 0xff], Some(vec![0x00, 0x00, 0x00, 0x01])),
                (vec![0x01, 0xdd], None),
                (vec![0x02, 0xac], Some(vec![0x00, 0x03])),
            ]
        );
    }

    // As of Oct 2023 this test by itself achieves 100% test coverage for the
    // logic in this file (minus the unreachable cases). If you modify the code
    // or the test, please check code coverage with e.g. tarpaulin.
    #[test]
    fn test_trie_consistency_manual() {
        let mut tries = TestTries::new(true);
        // Simple insertion from empty trie.
        tries.check_consistency_across_all_changes_and_apply(parse_changes(
            "
                00 = 0000
                01 = 0001
                02 = 0002
            ",
        ));
        // Prepare some more complex values.
        tries.check_consistency_across_all_changes_and_apply(parse_changes(
            "
                0000 = 0010  # extends a leaf
                0100 = 0011  # extends another leaf
                03 = 0012  # adds a branch
                0444 = 0013  # adds a branch with a longer leaf
                0500 = 0014  # adds a branch that has a branch underneath
                05100000 = 0015
                05100001 = 0016
                05200000 = 0017
                05200001 = 0018
                05300000 = 0019
                05300001 = 001a
                05400000 = 001b
                05400001 = 001c
                05500000 = 001d
                05501000 = 001e
                05501001 = 001f
            ",
        ));
        // Check insertion and deletion in a variety of cases.
        // Code coverage is used to confirm we have covered all cases.
        tries.check_consistency_across_all_changes_and_apply(parse_changes(
            "
                00 = delete  # turns a branch with value into an extension
                01 = 0027  # modifies the value at a branch
                0100 = delete  # turns a branch with value into a leaf
                03 = delete  # deletes a branch
                0444 = 0020  # overwrites a leaf
                0455 = 0022  # split leaf into branch at start
                0456 = 0023  # split (pending) leaf into branch
                05 = 0021  # turn branch into branch with value
                05110000 = 0024  # split extension node into branch at start
                05201000 = 0025  # split extension node into branch in the middle
                05300010 = 0026  # split extension node into branch at the end
                05400000 = delete  # turn 2-branch node into leaf that squashes with extension
                05500000 = delete  # turn 2-branch node into extension that squashes with another extension
            ",
        ));

        // sanity check here the truth is correct - i.e. our test itself is good.
        let expected_truth = parse_changes(
            "
                00 = delete
                0000 = 0010
                01 = 0027
                0100 = delete
                02 = 0002
                03 = delete
                0444 = 0020
                0455 = 0022
                0456 = 0023
                05 = 0021
                0500 = 0014
                05100000 = 0015
                05100001 = 0016
                05110000 = 0024
                05200000 = 0017
                05200001 = 0018
                05201000 = 0025
                05300000 = 0019
                05300001 = 001a
                05300010 = 0026
                05400000 = delete
                05400001 = 001c
                05500000 = delete
                05501000 = 001e
                05501001 = 001f
            ",
        )
        .into_iter()
        .map(|(k, v)| (k, v.map(|v| ValueRef::new(&v))))
        .collect::<HashMap<_, _>>();
        assert_eq!(
            tries.truth,
            expected_truth,
            "Differing keys: {:?}",
            expected_truth
                .keys()
                .cloned()
                .chain(tries.truth.keys().cloned())
                .collect::<HashSet<_>>()
                .into_iter()
                .filter(|k| { expected_truth.get(k) != tries.truth.get(k) })
                .collect::<Vec<_>>()
        );

        // Delete some non-existent keys.
        tries.check_consistency_across_all_changes_and_apply(parse_changes(
            "
                00 = delete  # non-existent branch
                04 = delete  # branch without value
                0445 = delete  # non-matching leaf
                055011 = delete  # non-matching extension
            ",
        ));

        // Make no changes
        tries.check_consistency_across_all_changes_and_apply(Vec::new());

        // Finally delete all keys.
        tries.check_consistency_across_all_changes_and_apply(parse_changes(
            "
                0000 = delete
                01 = delete
                02 = delete
                03 = delete
                0444 = delete
                0455 = delete
                0456 = delete
                05 = delete
                0500 = delete
                05100000 = delete
                05100001 = delete
                05110000 = delete
                05200000 = delete
                05200001 = delete
                05201000 = delete
                05300000 = delete
                05300001 = delete
                05300010 = delete
                05400001 = delete
                05501000 = delete
                05501001 = delete
            ",
        ));

        // Check a corner case that deleting a non-existent key from
        // an empty trie does not panic.
        tries.check_consistency_across_all_changes_and_apply(parse_changes(
            "
                08 = delete  # non-existent key when whole trie is empty
            ",
        ));

        assert_eq!(tries.state_root, StateRoot::default());
        // Garbage collect all roots we've added. This checks that the refcounts
        // maintained by the in-memory tries are correct, because if any
        // refcounts are too low this would panic, and if any refcounts are too
        // high the number of allocs in the end would be non-zero.
        tries.mem.delete_until_height(1);
        assert_eq!(tries.mem.num_roots(), 0);
        assert_eq!(tries.mem.arena().num_active_allocs(), 0);
    }

    // As of Oct 2023 this randomized test was seen to cover all branches except
    // deletion of keys from empty tries and deleting all keys from the trie.
    #[test]
    fn test_trie_consistency_random() {
        const MAX_KEYS: usize = 100;
        const SLOWDOWN: usize = 5;
        let mut tries = TestTries::new(false);
        for batch in 0..1000 {
            println!("Batch {}:", batch);
            let mut existing_keys = tries.truth.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
            // The more keys we have, the less we insert, the more we delete.
            let num_insertions =
                rand::thread_rng().gen_range(0..=(MAX_KEYS - existing_keys.len()) / SLOWDOWN);
            let num_deletions =
                rand::thread_rng().gen_range(0..=(existing_keys.len() + SLOWDOWN - 1) / SLOWDOWN);
            let mut changes = Vec::new();
            for _ in 0..num_insertions {
                let key_length = rand::thread_rng().gen_range(0..=10);
                let existing_key = existing_keys
                    .get(rand::thread_rng().gen_range(0..existing_keys.len().max(1)))
                    .cloned()
                    .unwrap_or_default();
                let reuse_prefix_length = rand::thread_rng().gen_range(0..=existing_key.len());
                let mut key = Vec::<u8>::new();
                for i in 0..key_length {
                    if i < reuse_prefix_length {
                        key.push(existing_key[i]);
                    } else {
                        // Limit nibbles to 4, so that we can generate keys that relate to
                        // each other more frequently.
                        let nibble0 = rand::thread_rng().r#gen::<u8>() % 4;
                        let nibble1 = rand::thread_rng().r#gen::<u8>() % 4;
                        key.push(nibble0 << 4 | nibble1);
                    }
                }

                let mut value_length = rand::thread_rng().gen_range(0..=10);
                if value_length == 10 {
                    value_length = 8000; // make a long value that is not inlined
                }
                let mut value = Vec::<u8>::new();
                for _ in 0..value_length {
                    value.push(rand::thread_rng().r#gen());
                }
                println!(
                    "  {} = {}",
                    hex::encode(&key),
                    if value.len() > 10 {
                        hex::encode(&value[0..10]) + "..."
                    } else {
                        hex::encode(&value)
                    }
                );
                changes.push((key.clone(), Some(value.clone())));
                // Add it to existing keys so that we can insert more keys similar
                // to this as well as delete some of these keys too.
                existing_keys.push(key);
            }
            for _ in 0..num_deletions {
                let key = existing_keys
                    .get(rand::thread_rng().gen_range(0..existing_keys.len()))
                    .cloned()
                    .unwrap_or_default();
                println!("  {} = delete", hex::encode(&key));
                changes.push((key.clone(), None));
            }
            tries.check_consistency_across_all_changes_and_apply(changes);
        }
    }

    fn insert_changes_to_memtrie(
        memtrie: &mut MemTries,
        prev_state_root: CryptoHash,
        block_height: BlockHeight,
        changes: &str,
    ) -> CryptoHash {
        let changes = parse_changes(changes);
        let mut update = memtrie.update(prev_state_root, TrackingMode::None).unwrap();

        for (key, value) in changes {
            if let Some(value) = value {
                update.insert_memtrie_only(&key, FlatStateValue::on_disk(&value)).unwrap();
            } else {
                update.generic_delete(0, &key, AccessOptions::DEFAULT).unwrap();
            }
        }

        let changes = update.to_memtrie_changes_only();
        memtrie.apply_memtrie_changes(block_height, &changes)
    }

    #[test]
    fn test_gc_hybrid_memtrie() {
        let state_root = StateRoot::default();
        let mut memtrie = MemTries::new(ShardUId::single_shard());
        assert!(!memtrie.arena.has_shared_memory());

        // Insert in some initial data for height 0
        let changes = "
            ff00 = 0000
            ff01 = 0100
            ff0101 = 0101
        ";
        let state_root = insert_changes_to_memtrie(&mut memtrie, state_root, 0, changes);

        // Freeze the current memory in memtrie
        let frozen_arena = memtrie.arena.freeze();
        let hybrid_arena =
            HybridArena::from_frozen("test_hybrid".to_string(), frozen_arena.clone());
        memtrie.arena = hybrid_arena;
        assert!(memtrie.arena.has_shared_memory());

        // Insert in some more data for height 1 in hybrid memtrie
        // Try to make sure we share some node allocations (ff01 and ff0101) with height 0
        // Node ff01 effectively has a refcount of 2, one from height 0 and one from height 1

        let changes = "
            ff0000 = 1000
            ff0001 = 1001
        ";
        insert_changes_to_memtrie(&mut memtrie, state_root, 1, changes);

        // Now try to garbage collect the height 0 root
        // Memory consumption should not change as height 0 is frozen
        let num_active_allocs = memtrie.arena.num_active_allocs();
        let active_allocs_bytes = memtrie.arena.active_allocs_bytes();
        memtrie.delete_until_height(1);
        assert_eq!(memtrie.arena.num_active_allocs(), num_active_allocs);
        assert_eq!(memtrie.arena.active_allocs_bytes(), active_allocs_bytes);

        // Now try to garbage collect the height 1 root
        // The final memory allocation should be what we had during the time of freezing
        memtrie.delete_until_height(2);
        assert_eq!(memtrie.arena.num_active_allocs(), frozen_arena.num_active_allocs());
        assert_eq!(memtrie.arena.active_allocs_bytes(), frozen_arena.active_allocs_bytes());
    }
}
