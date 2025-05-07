use self::mem::flexible_data::value::ValueView;
use self::trie_storage::TrieMemoryPartialStorage;
use crate::StorageError;
use crate::flat::{FlatStateChanges, FlatStorageChunkView};
pub use crate::trie::config::TrieConfig;
pub(crate) use crate::trie::config::{
    DEFAULT_SHARD_CACHE_DELETIONS_QUEUE_CAPACITY, DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT,
};
pub use crate::trie::nibble_slice::NibbleSlice;
pub use crate::trie::prefetching_trie_storage::{PrefetchApi, PrefetchError};
pub use crate::trie::shard_tries::{KeyForStateChanges, ShardTries, WrappedTrieChanges};
pub use crate::trie::state_snapshot::{
    STATE_SNAPSHOT_COLUMNS, SnapshotError, StateSnapshot, StateSnapshotConfig, state_snapshots_dir,
};
pub use crate::trie::trie_storage::{TrieCache, TrieCachingStorage, TrieDBStorage, TrieStorage};
use borsh::{BorshDeserialize, BorshSerialize};
pub use from_flat::construct_trie_from_flat;
use iterator::{DiskTrieIterator, DiskTrieIteratorInner, TrieIterator};
use itertools::Itertools;
use mem::memtrie_update::{TrackingMode, UpdatedMemTrieNodeWithSize};
use mem::memtries::MemTries;
use near_primitives::hash::{CryptoHash, hash};
pub use near_primitives::shard_layout::ShardUId;
use near_primitives::state::PartialState;
use near_primitives::state::{FlatStateValue, ValueRef};
use near_primitives::state_record::StateRecord;
use near_primitives::trie_key::TrieKey;
use near_primitives::trie_key::trie_key_parsers::parse_account_id_prefix;
use near_primitives::types::{AccountId, StateRoot, StateRootNode};
use near_schema_checker_lib::ProtocolSchema;
use near_vm_runner::ContractCode;
use ops::insert_delete::GenericTrieUpdateInsertDelete;
#[cfg(test)]
use ops::interface::{GenericNodeOrIndex, GenericTrieNode, GenericTrieUpdate};
use ops::interface::{GenericTrieValue, UpdatedNodeId};
use ops::resharding::{GenericTrieUpdateRetain, RetainMode};
use parking_lot::{RwLock, RwLockReadGuard};
pub use raw_node::{Children, RawTrieNode, RawTrieNodeWithSize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write;
use std::hash::Hash;
use std::str;
use std::sync::Arc;
pub use trie_recording::{SubtreeSize, TrieRecorder, TrieRecorderStats};
use trie_storage_update::{
    TrieStorageNodeWithSize, TrieStorageUpdate, UpdatedTrieStorageNodeWithSize,
};

mod config;
mod from_flat;
pub mod iterator;
pub mod mem;
mod nibble_slice;
pub mod ops;
pub mod outgoing_metadata;
mod prefetching_trie_storage;
mod raw_node;
pub mod receipts_column_helper;
mod shard_tries;
mod state_parts;
mod state_snapshot;
mod trie_recording;
mod trie_storage;
pub mod trie_storage_update;
#[cfg(test)]
mod trie_tests;
pub mod update;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartialStorage {
    pub nodes: PartialState,
}

#[derive(Clone, Hash, Debug, Copy)]
pub(crate) struct StorageHandle(usize);

/// Stores index of value in the array of new values and its length for memory
/// counting.
#[derive(Clone, Hash, Debug, Copy)]
pub struct StorageValueHandle(usize, usize);

pub struct TrieCosts {
    pub byte_of_key: u64,
    pub byte_of_value: u64,
    pub node_cost: u64,
}

/// Whether a key lookup will be performed through flat storage or through iterating the trie
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum KeyLookupMode {
    /// Try memtries first, if loaded. Then try flat storage if available. Otherwise fall-back to
    /// the on-disk trie.
    ///
    /// NOTE: whether different types of storage are loaded or not can affect the protocol! In
    /// particular operations with the flat storage have a different effect on TTN compared to trie
    /// or memtrie (e.g. if `use_accounting_cache` is set to `true`!)
    MemOrFlatOrTrie,
    /// Try memtrie first, if loaded. If not, then go straight to on-disk trie.
    MemOrTrie,
}

#[derive(Clone, Copy, Debug)]
pub struct AccessOptions<'access_tracker> {
    /// The interface through which TTNs are tracked.
    ///
    /// This also allows for some cache implementations, though the plan is to remove that
    /// functionality eventually.
    trie_access_tracker: &'access_tracker dyn AccessTracker,

    /// Whether a storage operation should record intermediate accesses to the state witness.
    ///
    /// This usually should be true, but in certain situations such as transparent optimizations
    /// that mustn't have an effect on the protocol this can be set to `false`.
    enable_state_witness_recording: bool,
}

pub trait AccessTracker: std::fmt::Debug {
    /// Lookup a value in the in-memory store.
    ///
    /// If result is `Some`, implementations should increment memory TTN counter.
    fn track_mem_lookup(&self, key: &CryptoHash) -> Option<Arc<[u8]>>;

    /// Insert the value into the in-memory store.
    ///
    /// Implementations should increment the disk TTN counter.
    fn track_disk_lookup(&self, key: CryptoHash, value: Arc<[u8]>);
}

#[derive(Debug)]
struct NoAccessTracker;
impl AccessTracker for NoAccessTracker {
    fn track_mem_lookup(&self, _: &CryptoHash) -> Option<Arc<[u8]>> {
        None
    }
    fn track_disk_lookup(&self, _: CryptoHash, _: Arc<[u8]>) {}
}

impl AccessOptions<'static> {
    pub const DEFAULT: Self =
        Self { trie_access_tracker: &NoAccessTracker, enable_state_witness_recording: true };
    pub const NO_SIDE_EFFECTS: Self =
        Self { trie_access_tracker: &NoAccessTracker, enable_state_witness_recording: false };
}

impl<'a> AccessOptions<'a> {
    pub fn contract_runtime(access_tracker: &'a dyn AccessTracker) -> Self {
        Self { trie_access_tracker: access_tracker, enable_state_witness_recording: true }
    }
}

const TRIE_COSTS: TrieCosts = TrieCosts { byte_of_key: 2, byte_of_value: 1, node_cost: 50 };

#[derive(Clone, Copy, Hash)]
pub enum ValueHandle {
    InMemory(StorageValueHandle),
    HashAndSize(ValueRef),
}

impl std::fmt::Debug for ValueHandle {
    fn fmt(&self, fmtr: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HashAndSize(value) => write!(fmtr, "{value:?}"),
            Self::InMemory(StorageValueHandle(num, _)) => write!(fmtr, "@{num}"),
        }
    }
}

impl UpdatedTrieStorageNodeWithSize {
    #[cfg(test)]
    fn print(
        &self,
        f: &mut dyn std::fmt::Write,
        trie_update: &TrieStorageUpdate,
        spaces: &mut String,
    ) -> std::fmt::Result {
        match &self.node {
            GenericTrieNode::Empty => {
                write!(f, "{}Empty", spaces)?;
            }
            GenericTrieNode::Leaf { extension, .. } => {
                let slice = NibbleSlice::from_encoded(&extension);
                write!(f, "{}Leaf({:?}, val)", spaces, slice.0)?;
            }
            GenericTrieNode::Branch { children, value } => {
                writeln!(
                    f,
                    "{}Branch({}){{",
                    spaces,
                    if value.is_some() { "Some" } else { "None" }
                )?;
                spaces.push(' ');
                for (idx, child) in children.iter().enumerate() {
                    let Some(child) = child else {
                        continue;
                    };
                    write!(f, "{}{:01x}->", spaces, idx)?;
                    match child {
                        GenericNodeOrIndex::Old(hash) => {
                            write!(f, "{}", hash)?;
                        }
                        GenericNodeOrIndex::Updated(id) => {
                            let child = trie_update.get_node_ref(*id);
                            child.print(f, trie_update, spaces)?;
                        }
                    }
                    writeln!(f)?;
                }
                spaces.remove(spaces.len() - 1);
                write!(f, "{}}}", spaces)?;
            }
            GenericTrieNode::Extension { extension, child } => {
                let slice = NibbleSlice::from_encoded(&extension);
                writeln!(f, "{}Extension({:?})", spaces, slice)?;
                spaces.push(' ');
                match child {
                    GenericNodeOrIndex::Old(hash) => {
                        write!(f, "{}{}", spaces, hash)?;
                    }
                    GenericNodeOrIndex::Updated(id) => {
                        let child = trie_update.get_node_ref(*id);
                        child.print(f, trie_update, spaces)?;
                    }
                }
                writeln!(f)?;
                spaces.remove(spaces.len() - 1);
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn deep_to_string(&self, trie_update: &TrieStorageUpdate) -> String {
        let mut buf = String::new();
        self.print(&mut buf, trie_update, &mut "".to_string()).expect("printing failed");
        buf
    }
}

pub struct Trie {
    storage: Arc<dyn TrieStorage>,
    memtries: Option<Arc<RwLock<MemTries>>>,
    /// In super rare cases it's possible that we see forks close to the resharding boundary.
    /// We would like to apply the same set of trie changes to the child memtrie to keep
    /// a consistent view across forks.
    children_memtries: HashMap<ShardUId, Arc<RwLock<MemTries>>>,
    root: StateRoot,
    /// If present, flat storage is used to look up keys (if asked for).
    /// Otherwise, we would crawl through the trie.
    flat_storage_chunk_view: Option<FlatStorageChunkView>,
    /// If present, we're capturing all trie nodes that have been accessed
    /// during the lifetime of this Trie struct. This is used to produce a
    /// state proof so that the same access pattern can be replayed using only
    /// the captured result.
    // FIXME: make `TrieRecorder` internally MT-safe, instead of locking the entire structure.
    recorder: Option<RwLock<TrieRecorder>>,
    /// If true, accesses to trie nodes are recorded with node access tracker, thus counting TTNs.
    ///
    /// NOTE that depending on the implementation of the tracker the lookups may get cached and
    /// later served from the storage from memory.
    ///
    /// Further, NOTE that regardless of this variable, lookups with `KeyLookupMode::MemOrTrie`
    /// will record node accesses with the access tracker regardless of this field's value. The
    /// logic here is ultra-subtle: it is possible for lookups with
    /// `KeyLookupMode::MemOrFlatOrTrie` to get served from either memtries, flat storage or
    /// regular trie depending on which of the storage types are loaded at the time `Trie` is
    /// constructed. However flat storage will only record accesses for dereference of a
    /// non-inlined value (and will do so regardless of this setting.) So the exact count accesses
    /// recorded will depend not only on this field, but also on which types of storage are loaded,
    /// which `KeyLookupMode` is used and probably some other subtle factors.
    // FIXME(nagisa): lets get rid of this field somehow? it seems to be utilized mostly for/in
    // tests.
    use_access_tracker: bool,
}

/// Trait for reading data from a trie.
pub trait TrieAccess {
    /// Retrieves value with given key from the trie.
    ///
    /// This doesn’t allow to read data from different chunks (be it from
    /// different shards or different blocks).  That is, the shard and state
    /// root are already known by the object rather than being passed as
    /// argument.
    fn get(&self, key: &TrieKey, opts: AccessOptions) -> Result<Option<Vec<u8>>, StorageError>;

    /// Check if the key is present.
    ///
    /// Equivalent to `Self::get(k)?.is_some()`, but avoids reading out the value.
    fn contains_key(&self, key: &TrieKey, opts: AccessOptions) -> Result<bool, StorageError>;
}

/// Stores reference count addition for some key-value pair in DB.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Hash,
    ProtocolSchema,
)]
pub struct TrieRefcountAddition {
    /// Hash of trie_node_or_value and part of the DB key.
    /// Used for uniting with shard id to get actual DB key.
    trie_node_or_value_hash: CryptoHash,
    /// DB value. Can be either serialized RawTrieNodeWithSize or value corresponding to
    /// some TrieKey.
    trie_node_or_value: Vec<u8>,
    /// Reference count difference which will be added to the total refcount.
    rc: std::num::NonZeroU32,
}

/// Stores reference count subtraction for some key in DB.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Hash,
    ProtocolSchema,
)]
pub struct TrieRefcountSubtraction {
    /// Hash of trie_node_or_value and part of the DB key.
    /// Used for uniting with shard id to get actual DB key.
    trie_node_or_value_hash: CryptoHash,
    /// Obsolete field but which we cannot remove because this data is persisted
    /// to the database.
    _ignored: IgnoredVecU8,
    /// Reference count difference which will be subtracted to the total refcount.
    rc: std::num::NonZeroU32,
}

/// Struct that is borsh compatible with Vec<u8> but which is logically the unit type.
#[derive(Default, BorshSerialize, BorshDeserialize, Clone, Debug, ProtocolSchema)]
struct IgnoredVecU8 {
    _ignored: Vec<u8>,
}

impl PartialEq for IgnoredVecU8 {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for IgnoredVecU8 {}
impl Hash for IgnoredVecU8 {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}
impl PartialOrd for IgnoredVecU8 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for IgnoredVecU8 {
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

impl TrieRefcountAddition {
    pub fn hash(&self) -> &CryptoHash {
        &self.trie_node_or_value_hash
    }

    pub fn payload(&self) -> &[u8] {
        self.trie_node_or_value.as_slice()
    }

    pub fn revert(&self) -> TrieRefcountSubtraction {
        TrieRefcountSubtraction::new(self.trie_node_or_value_hash, self.rc)
    }
}

impl TrieRefcountSubtraction {
    pub fn new(trie_node_or_value_hash: CryptoHash, rc: std::num::NonZeroU32) -> Self {
        Self { trie_node_or_value_hash, _ignored: Default::default(), rc }
    }
}

/// Helps produce a list of additions and subtractions to the trie,
/// especially in the case where deletions don't carry the full value.
pub struct TrieRefcountDeltaMap {
    map: BTreeMap<CryptoHash, (Option<Vec<u8>>, i32)>,
}

impl TrieRefcountDeltaMap {
    pub fn new() -> Self {
        Self { map: BTreeMap::new() }
    }

    pub fn add(&mut self, hash: CryptoHash, data: Vec<u8>, refcount: u32) {
        let (old_value, old_rc) = self.map.entry(hash).or_insert((None, 0));
        *old_value = Some(data);
        *old_rc += refcount as i32;
    }

    pub fn subtract(&mut self, hash: CryptoHash, refcount: u32) {
        let (_, old_rc) = self.map.entry(hash).or_insert((None, 0));
        *old_rc -= refcount as i32;
    }

    pub fn into_changes(self) -> (Vec<TrieRefcountAddition>, Vec<TrieRefcountSubtraction>) {
        let num_insertions = self.map.iter().filter(|(_h, (_v, rc))| *rc > 0).count();
        let mut insertions = Vec::with_capacity(num_insertions);
        let mut deletions = Vec::with_capacity(self.map.len().saturating_sub(num_insertions));
        for (hash, (value, rc)) in self.map {
            if rc > 0 {
                insertions.push(TrieRefcountAddition {
                    trie_node_or_value_hash: hash,
                    trie_node_or_value: value.expect("value must be present"),
                    rc: std::num::NonZeroU32::new(rc as u32).unwrap(),
                });
            } else if rc < 0 {
                deletions.push(TrieRefcountSubtraction::new(
                    hash,
                    std::num::NonZeroU32::new((-rc) as u32).unwrap(),
                ));
            }
        }
        // Sort so that trie changes have unique representation.
        insertions.sort();
        deletions.sort();
        (insertions, deletions)
    }
}

/// Changes to be applied to in-memory trie.
/// Result is the new state root attached to existing persistent trie structure.
#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct MemTrieChanges {
    /// Node ids with hashes of updated nodes.
    /// Should be in the post-order traversal of the updated nodes.
    /// It implies that the root node is the last one in the list.
    node_ids_with_hashes: Vec<(UpdatedNodeId, CryptoHash)>,
    updated_nodes: Vec<Option<UpdatedMemTrieNodeWithSize>>,
}

///
/// TrieChanges stores delta for refcount.
/// Multiple versions of the state work the following way:
///         __changes1___state1
/// state0 /
///        \__changes2___state2
///
/// To store state0, state1 and state2, apply insertions from changes1 and changes2
///
/// Then, to discard state2, apply insertions from changes2 as deletions
///
/// Then, to discard state0, apply deletions from changes1.
/// deleting state0 while both state1 and state2 exist is not possible.
/// Applying deletions from changes1 while state2 exists makes accessing state2 invalid.
///
///
/// create a fork -> apply insertions
/// resolve a fork -> apply opposite of insertions
/// discard old parent which has no forks from it -> apply deletions
///
/// Having old_root and values in deletions allows to apply TrieChanges in reverse
///
/// StoreUpdate are the changes from current state refcount to refcount + delta.
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug, ProtocolSchema)]
pub struct TrieChanges {
    pub old_root: StateRoot,
    pub new_root: StateRoot,
    insertions: Vec<TrieRefcountAddition>,
    deletions: Vec<TrieRefcountSubtraction>,
    // If Some, in-memory changes are applied as well.
    #[borsh(skip)]
    pub memtrie_changes: Option<MemTrieChanges>,
    // In super rare cases it's possible that we see forks close to the resharding boundary.
    // We would like to apply the same set of trie changes to the child memtrie to keep
    // a consistent view across forks.
    #[borsh(skip)]
    pub children_memtrie_changes: HashMap<ShardUId, MemTrieChanges>,
}

impl TrieChanges {
    pub fn empty(old_root: StateRoot) -> Self {
        TrieChanges {
            old_root,
            new_root: old_root,
            insertions: vec![],
            deletions: vec![],
            memtrie_changes: Default::default(),
            children_memtrie_changes: Default::default(),
        }
    }

    pub fn insertions(&self) -> &[TrieRefcountAddition] {
        self.insertions.as_slice()
    }

    pub fn deletions(&self) -> &[TrieRefcountSubtraction] {
        self.deletions.as_slice()
    }
}

/// Result of applying state part to Trie.
pub struct ApplyStatePartResult {
    /// Trie changes after applying state part.
    pub trie_changes: TrieChanges,
    /// Flat state changes after applying state part, stored as delta.
    pub flat_state_delta: FlatStateChanges,
    /// Contract codes belonging to the state part.
    pub contract_codes: Vec<ContractCode>,
}

enum NodeOrValue {
    Node,
    Value(Arc<[u8]>),
}

/// Like a ValueRef, but allows for optimized retrieval of the value if the
/// value were already readily available when the ValueRef was retrieved.
///
/// This can be the case if the value came from flat storage, for example,
/// when some values are inlined into the storage.
///
/// Information-wise, this struct contains the same information as a
/// FlatStateValue; however, we make this a separate struct because
/// dereferencing a ValueRef (and likewise, OptimizedValueRef) requires proper
/// gas accounting; it is not a free operation. Therefore, while
/// OptimizedValueRef can be directly converted to a ValueRef, dereferencing
/// the value, even if the value is already available, can only be done via
/// `Trie::deref_optimized`.
#[derive(Debug, PartialEq, Eq)]
pub enum OptimizedValueRef {
    Ref(ValueRef),
    AvailableValue(ValueAccessToken),
}

/// Opaque wrapper around Vec<u8> so that the value cannot be used directly and
/// must instead be dereferenced via `Trie::deref_optimized`, so that gas
/// accounting is never skipped.
#[derive(Debug, PartialEq, Eq)]
pub struct ValueAccessToken {
    // Must stay private.
    value: Vec<u8>,
}

impl OptimizedValueRef {
    fn from_flat_value(value: FlatStateValue) -> Self {
        match value {
            FlatStateValue::Ref(value_ref) => Self::Ref(value_ref),
            FlatStateValue::Inlined(value) => Self::AvailableValue(ValueAccessToken { value }),
        }
    }

    /// Returns the length (in num bytes) of the value pointed by this reference.
    pub fn len(&self) -> usize {
        match self {
            Self::Ref(value_ref) => value_ref.len(),
            Self::AvailableValue(token) => token.value.len(),
        }
    }

    /// Returns the hash of the value pointed by this reference.
    pub fn value_hash(&self) -> CryptoHash {
        match self {
            OptimizedValueRef::Ref(value_ref) => value_ref.hash,
            OptimizedValueRef::AvailableValue(ValueAccessToken { value }) => hash(value.as_slice()),
        }
    }

    pub fn into_value_ref(self) -> ValueRef {
        match self {
            Self::Ref(value_ref) => value_ref,
            Self::AvailableValue(token) => ValueRef::new(&token.value),
        }
    }
}

impl Trie {
    pub const EMPTY_ROOT: StateRoot = StateRoot::new();

    /// Starts accessing a trie with the given storage.
    /// By default, the accounting cache is not enabled. To enable or disable it
    /// (only in this crate), call self.accounting_cache.borrow_mut().set_enabled().
    pub fn new(
        storage: Arc<dyn TrieStorage>,
        root: StateRoot,
        flat_storage_chunk_view: Option<FlatStorageChunkView>,
    ) -> Self {
        Self::new_with_memtries(storage, None, Default::default(), root, flat_storage_chunk_view)
    }

    pub fn new_with_memtries(
        storage: Arc<dyn TrieStorage>,
        memtries: Option<Arc<RwLock<MemTries>>>,
        children_memtries: HashMap<ShardUId, Arc<RwLock<MemTries>>>,
        root: StateRoot,
        flat_storage_chunk_view: Option<FlatStorageChunkView>,
    ) -> Self {
        // Technically the use_trie_accounting_cache should be set based on the flat storage
        // protocol feature. When flat storage is enabled the trie node access should be free and
        // the charge flag should be set to false.
        let use_trie_accounting_cache = false;
        Trie {
            storage,
            memtries,
            children_memtries,
            root,
            use_access_tracker: use_trie_accounting_cache,
            flat_storage_chunk_view,
            recorder: None,
        }
    }

    /// Returns `true` if this `Trie` is configured to use in memory tries.
    pub fn has_memtries(&self) -> bool {
        self.memtries.is_some()
    }

    /// Helper to simulate gas costs as if flat storage was present.
    pub fn set_use_trie_accounting_cache(&mut self, value: bool) {
        self.use_access_tracker = value;
    }

    /// Makes a new trie that has everything the same except that access
    /// through that trie accumulates a state proof for all nodes accessed.
    pub fn recording_reads_new_recorder(&self) -> Self {
        let recorder = RwLock::new(TrieRecorder::new(None));
        self.recording_reads_with_recorder(recorder)
    }

    /// Makes a new trie that has everything the same except that access
    /// through that trie accumulates a state proof for all nodes accessed.
    /// We also supply a proof size limit to prevent the proof from growing too large.
    pub fn recording_reads_with_proof_size_limit(&self, proof_size_limit: usize) -> Self {
        let recorder = RwLock::new(TrieRecorder::new(Some(proof_size_limit)));
        self.recording_reads_with_recorder(recorder)
    }

    pub fn recording_reads_with_recorder(&self, recorder: RwLock<TrieRecorder>) -> Self {
        let mut trie = Self::new_with_memtries(
            self.storage.clone(),
            self.memtries.clone(),
            self.children_memtries.clone(),
            self.root,
            self.flat_storage_chunk_view.clone(),
        );
        trie.recorder = Some(recorder);
        trie.use_access_tracker = self.use_access_tracker;
        trie
    }

    // TODO(resharding): remove this method after proper fix for refcount issue
    pub fn take_recorder(self) -> Option<RwLock<TrieRecorder>> {
        self.recorder
    }

    /// Takes the recorded state proof out of the trie.
    pub fn recorded_storage(&self) -> Option<PartialStorage> {
        self.recorder.as_ref().map(|recorder| recorder.write().recorded_storage())
    }

    /// Returns the in-memory size of the recorded state proof. Useful for checking size limit of state witness
    pub fn recorded_storage_size(&self) -> usize {
        self.recorder
            .as_ref()
            .map(|recorder| recorder.read().recorded_storage_size())
            .unwrap_or_default()
    }

    /// Size of the recorded state proof plus some additional size added to cover removals.
    /// An upper-bound estimation of the true recorded size after finalization.
    pub fn recorded_storage_size_upper_bound(&self) -> usize {
        self.recorder
            .as_ref()
            .map(|recorder| recorder.read().recorded_storage_size_upper_bound())
            .unwrap_or_default()
    }

    pub fn check_proof_size_limit_exceed(&self) -> bool {
        self.recorder
            .as_ref()
            .is_some_and(|recorder| recorder.read().check_proof_size_limit_exceed())
    }

    /// Constructs a Trie from the partial storage (i.e. state proof) that
    /// was returned from recorded_storage(). If used to access the same trie
    /// nodes as when the partial storage was generated, this trie will behave
    /// identically.
    ///
    /// The flat_storage_used parameter should be true iff originally the trie
    /// was accessed with flat storage present. It will be used to simulate the
    /// same costs as if flat storage were present.
    pub fn from_recorded_storage(
        partial_storage: PartialStorage,
        root: StateRoot,
        flat_storage_used: bool,
    ) -> Self {
        let PartialState::TrieValues(nodes) = partial_storage.nodes;
        let recorded_storage = nodes.into_iter().map(|value| (hash(&value), value)).collect();
        let storage = Arc::new(TrieMemoryPartialStorage::new(recorded_storage));
        let mut trie = Self::new(storage, root, None);
        trie.use_access_tracker = !flat_storage_used;
        trie
    }

    /// Get statistics about the recorded trie. Useful for observability and debugging.
    /// This scans all of the recorded data, so could potentially be expensive to run.
    pub fn recorder_stats(&self) -> Option<TrieRecorderStats> {
        self.recorder.as_ref().map(|recorder| recorder.read().get_stats(&self.root))
    }

    pub fn get_root(&self) -> &StateRoot {
        &self.root
    }

    pub fn has_flat_storage_chunk_view(&self) -> bool {
        self.flat_storage_chunk_view.is_some()
    }

    pub fn internal_get_storage_as_caching_storage(&self) -> Option<&TrieCachingStorage> {
        self.storage.as_caching_storage()
    }

    #[cfg(feature = "test_features")]
    pub fn record_storage_garbage(&self, size_mbs: usize) -> bool {
        let Some(recorder) = &self.recorder else {
            return false;
        };
        let mut data = vec![0u8; (size_mbs as usize) * 1000_000];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut data);
        // We want to have at most 1 instance of garbage data included per chunk so
        // that it is possible to generated continuous stream of witnesses with a fixed
        // size. Using static key achieves that since in case of multiple receipts garbage
        // data will simply be overwritten, not accumulated.
        recorder.write().record_unaccounted(
            &CryptoHash::hash_bytes(b"__garbage_data_key_1720025071757228"),
            data.into(),
        );
        true
    }

    /// All access to trie nodes or values must go through this method, so it
    /// can be properly cached and recorded.
    ///
    /// count_cost can be false to skip caching. This is used when we're
    /// generating a state proof, but the value is supposed to fetched from
    /// flat storage.
    fn internal_retrieve_trie_node(
        &self,
        hash: &CryptoHash,
        use_accounting_cache: bool,
        access_options: AccessOptions,
    ) -> Result<Arc<[u8]>, StorageError> {
        let result = if use_accounting_cache {
            match access_options.trie_access_tracker.track_mem_lookup(hash) {
                Some(v) => v,
                None => {
                    let v = self.storage.retrieve_raw_bytes(hash)?;
                    access_options.trie_access_tracker.track_disk_lookup(*hash, Arc::clone(&v));
                    v
                }
            }
        } else {
            self.storage.retrieve_raw_bytes(hash)?
        };
        if access_options.enable_state_witness_recording {
            if let Some(recorder) = &self.recorder {
                recorder.write().record(hash, result.clone());
            }
        }
        Ok(result)
    }

    #[cfg(test)]
    fn memory_usage_verify(
        &self,
        trie_update: &TrieStorageUpdate,
        handle: GenericNodeOrIndex<CryptoHash>,
    ) -> u64 {
        // Cannot compute memory usage naively if given only partial storage.

        if self.storage.as_partial_storage().is_some() {
            return 0;
        }
        // We don't want to impact recorded storage by retrieving nodes for
        // this sanity check.
        if self.recorder.is_some() {
            return 0;
        }

        let UpdatedTrieStorageNodeWithSize { node, memory_usage } = match handle {
            GenericNodeOrIndex::Updated(h) => trie_update.get_node_ref(h).clone(),
            GenericNodeOrIndex::Old(h) => {
                let raw_node = self
                    .retrieve_raw_node(&h, false, AccessOptions::NO_SIDE_EFFECTS)
                    .expect("storage failure")
                    .expect("node cannot be Empty")
                    .1;
                TrieStorageNodeWithSize::from_raw_trie_node_with_size(raw_node).into()
            }
        };

        let mut memory_usage_naive = node.memory_usage_direct();
        match &node {
            GenericTrieNode::Empty => {}
            GenericTrieNode::Leaf { .. } => {}
            GenericTrieNode::Branch { children, .. } => {
                memory_usage_naive += children
                    .iter()
                    .filter_map(|handle| {
                        handle.as_ref().map(|h| self.memory_usage_verify(trie_update, *h))
                    })
                    .sum::<u64>();
            }
            GenericTrieNode::Extension { child, .. } => {
                memory_usage_naive += self.memory_usage_verify(trie_update, *child);
            }
        };
        if memory_usage_naive != memory_usage {
            eprintln!("Incorrectly calculated memory usage");
            eprintln!("Correct is {}", memory_usage_naive);
            eprintln!("Computed is {}", memory_usage);
            match handle {
                GenericNodeOrIndex::Updated(h) => {
                    eprintln!("In-memory node:");
                    let node = trie_update.get_node_ref(h);
                    eprintln!("{}", node.deep_to_string(trie_update));
                }
                GenericNodeOrIndex::Old(_h) => {
                    eprintln!("Bad node in storage!");
                }
            };
            assert_eq!(memory_usage_naive, memory_usage);
        }
        memory_usage
    }

    /// Prints the trie nodes starting from `hash`, up to `max_depth` depth. The node hash can be any node in the trie.
    /// Depending on arguments provided, can limit output to no more than `limit` entries,
    /// show only subtree for a given `record_type`, or skip subtrees where `AccountId` is less than `from` or greater than `to`.
    pub fn print_recursive(
        &self,
        f: &mut dyn std::io::Write,
        hash: &CryptoHash,
        max_depth: u32,
        limit: Option<u32>,
        record_type: Option<u8>,
        from: &Option<&AccountId>,
        to: &Option<&AccountId>,
    ) {
        match self.debug_retrieve_raw_node_or_value(hash) {
            Ok(NodeOrValue::Node) => {
                let mut prefix: Vec<u8> = Vec::new();
                let mut limit = limit.unwrap_or(u32::MAX);
                self.print_recursive_internal(
                    f,
                    hash,
                    &mut "".to_string(),
                    &mut prefix,
                    max_depth,
                    &mut limit,
                    record_type,
                    from,
                    to,
                )
                .expect("write failed");
            }
            Ok(NodeOrValue::Value(value_bytes)) => {
                writeln!(
                    f,
                    "Given node is a value. Len: {}, Data: {:?} ",
                    value_bytes.len(),
                    &value_bytes[..std::cmp::min(10, value_bytes.len())]
                )
                .expect("write failed");
            }
            Err(err) => {
                writeln!(f, "Error when reading: {}", err).expect("write failed");
            }
        };
    }

    /// Prints the trie leaves starting from the state root node, up to max_depth depth.
    /// This method can only iterate starting from the root node and it only prints the
    /// leaf nodes but it shows output in more human friendly way.
    /// Optional arguments `limit` and `record_type` limits the output to at most `limit`
    /// entries and shows trie nodes of `record_type` type only.
    /// `from` and `to` can be used skip leaves with `AccountId` less than `from` or greater than `to`.
    pub fn print_recursive_leaves(
        &self,
        f: &mut dyn std::io::Write,
        max_depth: u32,
        limit: Option<u32>,
        record_type: Option<u8>,
        from: &Option<&AccountId>,
        to: &Option<&AccountId>,
    ) {
        let mut limit = limit.unwrap_or(u32::MAX);
        let from = from.cloned();
        let to = to.cloned();

        let prune_condition = move |key_nibbles: &Vec<u8>| {
            if key_nibbles.len() > max_depth as usize {
                return true;
            }
            let (partial_key, _) = Self::nibbles_to_bytes(&key_nibbles);
            Self::should_prune_view_trie(&partial_key, record_type, &from.as_ref(), &to.as_ref())
        };

        let iter = match self.disk_iter_with_prune_condition(Some(Box::new(prune_condition))) {
            Ok(iter) => iter,
            Err(err) => {
                writeln!(f, "Error when getting the trie iterator: {}", err).expect("write failed");
                return;
            }
        };

        for node in iter {
            if limit == 0 {
                break;
            }
            let (key, value) = match node {
                Ok((key, value)) => (key, value),
                Err(err) => {
                    writeln!(f, "Failed to iterate node with error: {err}").expect("write failed");
                    continue;
                }
            };

            // Try to parse the key in UTF8 which works only for the simplest keys (e.g. account),
            // or get whitespace padding instead.
            let key_string = match str::from_utf8(&key) {
                Ok(value) => String::from(value),
                Err(_) => " ".repeat(key.len()),
            };
            let state_record = StateRecord::from_raw_key_value(&key, value);

            limit -= 1;
            writeln!(f, "{} {state_record:?}", key_string).expect("write failed");
        }
    }

    /// Converts the list of Nibbles to `Vec<u8>` and remainder (in case the length of the input was odd).
    fn nibbles_to_bytes(nibbles: &[u8]) -> (Vec<u8>, &[u8]) {
        let (chunks, remainder) = stdx::as_chunks::<2, _>(nibbles);
        let bytes = chunks.into_iter().map(|chunk| (chunk[0] * 16) + chunk[1]).collect::<Vec<u8>>();
        (bytes, remainder)
    }

    // Converts the list of Nibbles to a readable string.
    fn nibbles_to_string(prefix: &[u8]) -> String {
        let (bytes, remainder) = Self::nibbles_to_bytes(prefix);
        let mut result = bytes
            .iter()
            .flat_map(|ch| std::ascii::escape_default(*ch).map(char::from))
            .collect::<String>();
        if let Some(final_nibble) = remainder.first() {
            write!(&mut result, "\\x{:x}_", final_nibble).unwrap();
        }
        result
    }

    /// Checks whether the provided `account_id_prefix` is lexicographically greater than `to` (if provided),
    /// or whether it is lexicographically less than `from` (if provided, except being a prefix of `from`).
    /// Although prefix of `from` is lexicographically less than `from`, pruning such subtree would cut off `from`.
    fn is_out_of_account_id_bounds(
        account_id_prefix: &[u8],
        from: &Option<&AccountId>,
        to: &Option<&AccountId>,
    ) -> bool {
        if let Some(from) = from {
            if !from.as_bytes().starts_with(account_id_prefix)
                && from.as_bytes() > account_id_prefix
            {
                return true;
            }
        }
        if let Some(to) = to {
            return account_id_prefix > to.as_bytes();
        }
        false
    }

    /// Returns true if the node with key `node_key` and its subtree should be skipped based on provided arguments.
    /// If `record_type` is provided and the node is of different type, returns true.
    /// If `AccountId`s in the subtree will not fall in the range [`from`, `to`], returns true.
    /// Otherwise returns false.
    fn should_prune_view_trie(
        node_key: &Vec<u8>,
        record_type: Option<u8>,
        from: &Option<&AccountId>,
        to: &Option<&AccountId>,
    ) -> bool {
        if node_key.is_empty() {
            return false;
        }

        let column = node_key[0];
        if let Some(record_type) = record_type {
            if column != record_type {
                return true;
            }
        }
        if let Ok(account_id_prefix) = parse_account_id_prefix(column, &node_key) {
            if Self::is_out_of_account_id_bounds(account_id_prefix, from, to) {
                return true;
            }
        }
        false
    }

    fn print_recursive_internal(
        &self,
        f: &mut dyn std::io::Write,
        hash: &CryptoHash,
        spaces: &mut String,
        prefix: &mut Vec<u8>,
        max_depth: u32,
        limit: &mut u32,
        record_type: Option<u8>,
        from: &Option<&AccountId>,
        to: &Option<&AccountId>,
    ) -> std::io::Result<()> {
        if max_depth == 0 || *limit == 0 {
            return Ok(());
        }
        *limit -= 1;

        let opts = AccessOptions::DEFAULT;

        let (bytes, raw_node, mem_usage) = match self.retrieve_raw_node(hash, true, opts) {
            Ok(Some((bytes, raw_node))) => (bytes, raw_node.node, raw_node.memory_usage),
            Ok(None) => return writeln!(f, "{spaces}EmptyNode"),
            Err(err) => return writeln!(f, "{spaces}error {err}"),
        };

        let children = match raw_node {
            RawTrieNode::Leaf(key, value) => {
                let (slice, _) = NibbleSlice::from_encoded(key.as_slice());
                prefix.extend(slice.iter());

                let (leaf_key, remainder) = Self::nibbles_to_bytes(&prefix);
                assert!(remainder.is_empty());

                if !Self::should_prune_view_trie(&leaf_key, record_type, from, to) {
                    let state_record = StateRecord::from_raw_key_value(&leaf_key, bytes.to_vec());

                    writeln!(
                        f,
                        "{spaces}Leaf {slice:?} {value:?} prefix:{} hash:{hash} mem_usage:{mem_usage} state_record:{:?}",
                        Self::nibbles_to_string(prefix),
                        state_record.map(|sr| format!("{}", sr)),
                    )?;
                }

                prefix.truncate(prefix.len() - slice.len());
                return Ok(());
            }
            RawTrieNode::BranchNoValue(children) => {
                writeln!(
                    f,
                    "{spaces}Branch value:(none) prefix:{} hash:{hash} mem_usage:{mem_usage}",
                    Self::nibbles_to_string(prefix),
                )?;
                children
            }
            RawTrieNode::BranchWithValue(value, children) => {
                writeln!(
                    f,
                    "{spaces}Branch value:{value:?} prefix:{} hash:{hash} mem_usage:{mem_usage}",
                    Self::nibbles_to_string(prefix),
                )?;
                children
            }
            RawTrieNode::Extension(key, child) => {
                let (slice, _) = NibbleSlice::from_encoded(key.as_slice());
                let node_info = format!(
                    "{}Extension {:?} child_hash:{} prefix:{} hash:{hash} mem_usage:{mem_usage}",
                    spaces,
                    slice,
                    child,
                    Self::nibbles_to_string(prefix),
                );
                spaces.push_str("  ");
                prefix.extend(slice.iter());

                let (partial_key, _) = Self::nibbles_to_bytes(&prefix);

                if !Self::should_prune_view_trie(&partial_key, record_type, from, to) {
                    writeln!(f, "{}", node_info)?;

                    self.print_recursive_internal(
                        f,
                        &child,
                        spaces,
                        prefix,
                        max_depth - 1,
                        limit,
                        record_type,
                        from,
                        to,
                    )?;
                }

                prefix.truncate(prefix.len() - slice.len());
                spaces.truncate(spaces.len() - 2);
                return Ok(());
            }
        };

        for (idx, child) in children.iter() {
            writeln!(f, "{spaces} {idx:01x}->")?;
            spaces.push_str("  ");
            prefix.push(idx);
            self.print_recursive_internal(
                f,
                child,
                spaces,
                prefix,
                max_depth - 1,
                limit,
                record_type,
                from,
                to,
            )?;
            prefix.pop();
            spaces.truncate(spaces.len() - 2);
        }

        Ok(())
    }

    /// Retrieves decoded raw node alongside with its raw bytes representation.
    fn retrieve_raw_node(
        &self,
        hash: &CryptoHash,
        use_accounting_cache: bool,
        operation_options: AccessOptions,
    ) -> Result<Option<(Arc<[u8]>, RawTrieNodeWithSize)>, StorageError> {
        if hash == &Self::EMPTY_ROOT {
            return Ok(None);
        }
        let bytes =
            self.internal_retrieve_trie_node(hash, use_accounting_cache, operation_options)?;
        let node = RawTrieNodeWithSize::try_from_slice(&bytes).map_err(|err| {
            StorageError::StorageInconsistentState(format!("Failed to decode node {hash}: {err}"))
        })?;
        Ok(Some((bytes, node)))
    }

    // Similar to retrieve_raw_node but handles the case where there is a Value (and not a Node) in the database.
    // This method is not safe to be used in any real scenario as it can incorrectly interpret a value as a trie node.
    // It's only provided as a convenience for debugging tools.
    fn debug_retrieve_raw_node_or_value(
        &self,
        hash: &CryptoHash,
    ) -> Result<NodeOrValue, StorageError> {
        let op_opts = AccessOptions::DEFAULT;
        let bytes = self.internal_retrieve_trie_node(hash, true, op_opts)?;
        match RawTrieNodeWithSize::try_from_slice(&bytes) {
            Ok(_) => Ok(NodeOrValue::Node),
            Err(_) => Ok(NodeOrValue::Value(bytes)),
        }
    }

    pub(crate) fn move_node_to_mutable(
        &self,
        trie_update: &mut TrieStorageUpdate,
        hash: &CryptoHash,
        opts: AccessOptions,
    ) -> Result<StorageHandle, StorageError> {
        match self.retrieve_raw_node(hash, true, opts)? {
            None => Ok(trie_update.store(UpdatedTrieStorageNodeWithSize::empty())),
            Some((_, node)) => {
                let result = trie_update
                    .store(TrieStorageNodeWithSize::from_raw_trie_node_with_size(node).into());
                trie_update.refcount_changes.subtract(*hash, 1);
                Ok(result)
            }
        }
    }

    pub fn retrieve_root_node(&self) -> Result<StateRootNode, StorageError> {
        let opts = AccessOptions::DEFAULT;
        match self.retrieve_raw_node(&self.root, true, opts)? {
            None => Ok(StateRootNode::empty()),
            Some((bytes, node)) => {
                Ok(StateRootNode { data: bytes, memory_usage: node.memory_usage })
            }
        }
    }

    /// Retrieves the value (inlined or reference) for the given key, from flat storage.
    /// In general, flat storage may inline a value if the value is short, but otherwise
    /// it would defer the storage of the value to the trie. This method will return
    /// whatever the flat storage has.
    ///
    /// If an inlined value is returned, this method will charge the corresponding gas
    /// as if the value were accessed from the trie storage. It will also insert the
    /// value into the accounting cache, as well as recording the access to the value
    /// if recording is enabled. In other words, if an inlined value is returned the
    /// behavior is equivalent to if the trie were used to access the value reference
    /// and then the reference were used to look up the full value.
    ///
    /// If `ref_only` is true, even if the flat storage gives us the inlined value, we
    /// would still convert it to a reference. This is useful if making an access for
    /// the value (thereby charging gas for it) is not desired.
    fn lookup_from_flat_storage(
        &self,
        key: &[u8],
        operation_options: AccessOptions,
    ) -> Result<Option<OptimizedValueRef>, StorageError> {
        let flat_storage_chunk_view = self.flat_storage_chunk_view.as_ref().unwrap();
        let value = flat_storage_chunk_view.get_value(key)?;
        if operation_options.enable_state_witness_recording && self.recorder.is_some() {
            // If recording, we need to look up in the trie as well to record the trie nodes,
            // as they are needed to prove the value. Also, it's important that this lookup
            // is done even if the key was not found, because intermediate trie nodes may be
            // needed to prove the non-existence of the key.
            let value_ref_from_trie =
                self.lookup_from_state_column(NibbleSlice::new(key), false, operation_options)?;
            debug_assert_eq!(
                &value_ref_from_trie,
                &value.as_ref().map(|value| value.to_value_ref())
            );
        }
        Ok(value.map(OptimizedValueRef::from_flat_value))
    }

    /// Looks up the given key by walking the trie nodes stored in the
    /// `DBCol::State` column in the database (but still going through
    /// applicable caches).
    ///
    /// The `use_trie_accounting_cache` parameter controls whether the
    /// lookup incurs any gas.
    fn lookup_from_state_column(
        &self,
        mut key: NibbleSlice<'_>,
        use_trie_accounting_cache: bool,
        operation_options: AccessOptions,
    ) -> Result<Option<ValueRef>, StorageError> {
        let mut hash = self.root;
        loop {
            let node = match self.retrieve_raw_node(
                &hash,
                use_trie_accounting_cache,
                operation_options,
            )? {
                None => return Ok(None),
                Some((_bytes, node)) => node.node,
            };
            match node {
                RawTrieNode::Leaf(existing_key, value) => {
                    return Ok(if NibbleSlice::from_encoded(&existing_key).0 == key {
                        Some(value)
                    } else {
                        None
                    });
                }
                RawTrieNode::Extension(existing_key, child) => {
                    let existing_key = NibbleSlice::from_encoded(&existing_key).0;
                    if key.starts_with(&existing_key) {
                        hash = child;
                        key = key.mid(existing_key.len());
                    } else {
                        return Ok(None);
                    }
                }
                RawTrieNode::BranchNoValue(mut children) => {
                    if key.is_empty() {
                        return Ok(None);
                    } else if let Some(h) = children[key.at(0)].take() {
                        hash = h;
                        key = key.mid(1);
                    } else {
                        return Ok(None);
                    }
                }
                RawTrieNode::BranchWithValue(value, mut children) => {
                    if key.is_empty() {
                        return Ok(Some(value));
                    } else if let Some(h) = children[key.at(0)].take() {
                        hash = h;
                        key = key.mid(1);
                    } else {
                        return Ok(None);
                    }
                }
            };
        }
    }

    /// Retrieves an `OptimizedValueRef` (a hash of or inlined value) for the given
    /// key from the in-memory trie. In general, in-memory tries may inline a value
    /// if the value is short, but otherwise it would defer the storage of the value
    /// to the state column. This method will return whichever the in-memory trie has.
    /// Refer to `get_optimized_ref` for the semantics of using the returned type.
    ///
    /// `use_trie_accounting_cache` is used to control whether Trie node
    /// accesses accumulate TTN counters and potentially store data into the `TrieAccountingCache`
    /// so that future accesses count towards TTN counters.
    ///
    /// The storage of memtries and the data therein are behind a lock, as thus unlike many other
    /// functions here, the access to the value reference is provided as an argument to the
    /// `map_result` closure.
    ///
    /// This function also takes care of the accounting cache for gas calculation purposes.
    fn lookup_from_memory<R: 'static>(
        &self,
        key: &[u8],
        use_trie_accounting_cache: bool,
        access_options: AccessOptions,
        map_result: impl FnOnce(ValueView<'_>) -> R,
    ) -> Result<Option<R>, StorageError> {
        if self.root == Self::EMPTY_ROOT {
            return Ok(None);
        }

        let lock = self.memtries.as_ref().unwrap().read();
        let mem_value = if use_trie_accounting_cache
            || access_options.enable_state_witness_recording
        {
            let mut accessed_nodes = Vec::new();
            let mem_value = lock.lookup(&self.root, key, Some(&mut accessed_nodes))?;
            if use_trie_accounting_cache {
                for (node_hash, serialized_node) in &accessed_nodes {
                    if access_options.trie_access_tracker.track_mem_lookup(node_hash).is_none() {
                        access_options
                            .trie_access_tracker
                            .track_disk_lookup(*node_hash, Arc::clone(serialized_node));
                    }
                }
            }
            if access_options.enable_state_witness_recording {
                if let Some(recorder) = &self.recorder {
                    for (node_hash, serialized_node) in accessed_nodes {
                        recorder.write().record(&node_hash, serialized_node);
                    }
                }
            }
            mem_value
        } else {
            lock.lookup(&self.root, key, None)?
        };
        Ok(mem_value.map(map_result))
    }

    /// For debugging only. Returns the raw node at the given path starting from the root.
    /// The format of the nibbles parameter is that each element represents 4 bits of the
    /// path. (Even though we use a u8 for each element, we only use the lower 4 bits.)
    pub fn debug_get_node(&self, nibbles: &[u8]) -> Result<Option<RawTrieNode>, StorageError> {
        // We need to construct an equivalent NibbleSlice so we can easily use it
        // to traverse the trie. The tricky part is that the NibbleSlice implementation
        // only allows *starting* from the middle of a byte, and always requires ending at
        // the end of the internal array - this is sufficient because for production purposes
        // we always have a complete leaf path to use. But for our debugging purposes we
        // specify an incomplete trie path that may *end* at the middle of a byte, so to get
        // around that, if the provided path length is odd, we prepend a 0 and then start in
        // the middle of the first byte.
        let odd = nibbles.len() % 2 == 1;
        let mut nibble_array = Vec::new();
        if odd {
            nibble_array.push(0);
        }
        for nibble in nibbles {
            nibble_array.push(*nibble);
        }
        let mut nibble_data = Vec::new();
        for i in 0..nibble_array.len() / 2 {
            let first = nibble_array[i * 2];
            let second = nibble_array[i * 2 + 1];
            nibble_data.push((first << 4) + second);
        }
        let mut key = NibbleSlice::new_offset(&nibble_data, if odd { 1 } else { 0 });

        // The rest of the logic is very similar to the standard lookup() function, except
        // we return the raw node and don't expect to hit a leaf.
        let opts = AccessOptions::DEFAULT;
        let mut node = self.retrieve_raw_node(&self.root, true, opts)?;
        while !key.is_empty() {
            match node {
                Some((_, raw_node)) => match raw_node.node {
                    RawTrieNode::Leaf(_, _) => {
                        return Ok(None);
                    }
                    RawTrieNode::BranchNoValue(children)
                    | RawTrieNode::BranchWithValue(_, children) => {
                        let child = children[key.at(0)];
                        match child {
                            Some(child) => {
                                node = self.retrieve_raw_node(&child, true, opts)?;
                                key = key.mid(1);
                            }
                            None => return Ok(None),
                        }
                    }
                    RawTrieNode::Extension(existing_key, child) => {
                        let existing_key = NibbleSlice::from_encoded(&existing_key).0;
                        if key.starts_with(&existing_key) {
                            node = self.retrieve_raw_node(&child, true, opts)?;
                            key = key.mid(existing_key.len());
                        } else {
                            return Ok(None);
                        }
                    }
                },
                None => return Ok(None),
            }
        }
        match node {
            Some((_, raw_node)) => Ok(Some(raw_node.node)),
            None => Ok(None),
        }
    }

    /// Returns the raw bytes corresponding to a ValueRef that came from a node with
    /// value (either Leaf or BranchWithValue).
    pub fn retrieve_value(
        &self,
        hash: &CryptoHash,
        opts: AccessOptions,
    ) -> Result<Vec<u8>, StorageError> {
        let bytes = self.internal_retrieve_trie_node(hash, true, opts)?;
        Ok(bytes.to_vec())
    }

    /// Check if the column contains a value with the given `key`.
    ///
    /// This method is guaranteed to not inspect the value stored for this key, which would
    /// otherwise have potential gas cost implications.
    pub fn contains_key(&self, key: &[u8], opts: AccessOptions) -> Result<bool, StorageError> {
        self.contains_key_mode(key, KeyLookupMode::MemOrFlatOrTrie, opts)
    }

    /// Check if the column contains a value with the given `key`.
    ///
    /// This method is guaranteed to not inspect the value stored for this key, which would
    /// otherwise have potential gas cost implications.
    pub fn contains_key_mode(
        &self,
        key: &[u8],
        mode: KeyLookupMode,
        opts: AccessOptions,
    ) -> Result<bool, StorageError> {
        let use_trie_accounting_cache = mode == KeyLookupMode::MemOrTrie || self.use_access_tracker;
        if self.memtries.is_some() {
            return Ok(self
                .lookup_from_memory(key, use_trie_accounting_cache, opts, |_| ())?
                .is_some());
        }

        'flat: {
            let KeyLookupMode::MemOrFlatOrTrie = mode else { break 'flat };
            let Some(flat_storage_chunk_view) = &self.flat_storage_chunk_view else { break 'flat };
            let value = flat_storage_chunk_view.contains_key(key)?;
            if self.recorder.is_some() {
                // If recording, we need to look up in the trie as well to record the trie nodes,
                // as they are needed to prove the value. Also, it's important that this lookup
                // is done even if the key was not found, because intermediate trie nodes may be
                // needed to prove the non-existence of the key.
                let value_ref_from_trie =
                    self.lookup_from_state_column(NibbleSlice::new(key), false, opts)?;
                debug_assert_eq!(&value_ref_from_trie.is_some(), &value);
            }
            return Ok(value);
        }

        Ok(self
            .lookup_from_state_column(NibbleSlice::new(key), use_trie_accounting_cache, opts)?
            .is_some())
    }

    /// Retrieves an `OptimizedValueRef`` for the given key. See `OptimizedValueRef`.
    ///
    /// `mode`: whether we will try to perform the lookup through flat storage or trie.
    ///         Note that even if `mode == KeyLookupMode::FlatStorage`, we still may not use
    ///         flat storage if the trie is not created with a flat storage object in it.
    ///         Such double check may seem redundant but it is necessary for now.
    ///         Not all tries are created with flat storage, for example, we don't
    ///         enable flat storage for state-viewer. And we do not use flat
    ///         storage for key lookup performed in `storage_write`, so we need
    ///         the `use_flat_storage` to differentiate whether the lookup is performed for
    ///         storage_write or not.
    pub fn get_optimized_ref(
        &self,
        key: &[u8],
        mode: KeyLookupMode,
        opts: AccessOptions,
    ) -> Result<Option<OptimizedValueRef>, StorageError> {
        let use_trie_accounting_cache = mode == KeyLookupMode::MemOrTrie || self.use_access_tracker;
        if self.memtries.is_some() {
            self.lookup_from_memory(key, use_trie_accounting_cache, opts, |v| {
                v.to_optimized_value_ref()
            })
        } else if mode == KeyLookupMode::MemOrFlatOrTrie && self.flat_storage_chunk_view.is_some() {
            self.lookup_from_flat_storage(key, opts)
        } else {
            Ok(self
                .lookup_from_state_column(NibbleSlice::new(key), use_trie_accounting_cache, opts)?
                .map(OptimizedValueRef::Ref))
        }
    }

    /// Dereferences an `OptimizedValueRef` into the full value, and properly
    /// accounts for the gas, caching, and recording (if enabled). This may or
    /// may not incur a on-disk lookup, depending on whether the
    /// `OptimizedValueRef` contains an already available value.
    pub fn deref_optimized(
        &self,
        operation_options: AccessOptions,
        optimized_value_ref: &OptimizedValueRef,
    ) -> Result<Vec<u8>, StorageError> {
        match optimized_value_ref {
            OptimizedValueRef::Ref(value_ref) => {
                self.retrieve_value(&value_ref.hash, operation_options)
            }
            OptimizedValueRef::AvailableValue(ValueAccessToken { value }) => {
                let value_hash = hash(value);
                let arc_value: Arc<[u8]> = value.clone().into();
                if operation_options.trie_access_tracker.track_mem_lookup(&value_hash).is_none() {
                    operation_options
                        .trie_access_tracker
                        .track_disk_lookup(value_hash, arc_value.clone());
                }
                if operation_options.enable_state_witness_recording {
                    if let Some(recorder) = &self.recorder {
                        recorder.write().record(&value_hash, arc_value);
                    }
                }
                Ok(value.clone())
            }
        }
    }

    /// Retrieves the full value for the given key.
    pub fn get(&self, key: &[u8], opts: AccessOptions) -> Result<Option<Vec<u8>>, StorageError> {
        match self.get_optimized_ref(key, KeyLookupMode::MemOrFlatOrTrie, opts)? {
            Some(optimized_ref) => Ok(Some(self.deref_optimized(opts, &optimized_ref)?)),
            None => Ok(None),
        }
    }

    pub fn update<I>(&self, changes: I, opts: AccessOptions) -> Result<TrieChanges, StorageError>
    where
        I: IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    {
        // Call `get` for contract codes requested to be recorded.
        let codes_to_record = if opts.enable_state_witness_recording {
            if let Some(recorder) = &self.recorder {
                recorder.read().codes_to_record.clone()
            } else {
                HashSet::default()
            }
        } else {
            Default::default()
        };
        for account_id in codes_to_record {
            let trie_key = TrieKey::ContractCode { account_id: account_id.clone() };
            let _ = self.get(&trie_key.to_vec(), opts);
        }

        if self.memtries.is_some() {
            self.update_with_memtrie(changes, opts)
        } else {
            self.update_with_trie_storage(changes, opts)
        }
    }

    fn update_with_memtrie<I>(
        &self,
        changes: I,
        opts: AccessOptions,
    ) -> Result<TrieChanges, StorageError>
    where
        I: IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    {
        // Get trie_update for memtrie
        let guard = self.memtries.as_ref().unwrap().read();
        let mut recorder = if opts.enable_state_witness_recording {
            self.recorder.as_ref().map(|recorder| recorder.write())
        } else {
            None
        };
        let tracking_mode = match &mut recorder {
            Some(recorder) => TrackingMode::RefcountsAndAccesses(&mut *recorder),
            None => TrackingMode::Refcounts,
        };
        let mut trie_update = guard.update(self.root, tracking_mode)?;

        // Get trie_update for all child memtries
        let child_guards = self
            .children_memtries
            .iter()
            .map(|(shard_uid, memtrie)| (shard_uid, memtrie.read()))
            .collect_vec();
        let mut child_updates = child_guards
            .iter()
            .map(|(shard_uid, memtrie)| {
                // It's fine to use tracking mode as None here because the recording is handled by
                // the parent memtrie and doesn't need to be tracked for children.
                (shard_uid, memtrie.update(self.root, TrackingMode::None).unwrap())
            })
            .collect_vec();

        // Insert key, value into both the child memtries as well as the main memtrie
        for (key, value) in changes {
            match value {
                Some(arr) => {
                    // Update all child memtries. This is a rare case where parent shard has forks
                    // at the resharding epoch boundary.
                    // It is fine to clone the value here as this is a very rare occurrence.
                    for trie_update in &mut child_updates {
                        trie_update.1.insert(&key, arr.clone())?;
                    }
                    trie_update.insert(&key, arr)?;
                }
                None => {
                    // Update all child memtries. This is a rare case where parent shard
                    // has forks after resharding.
                    for trie_update in &mut child_updates {
                        trie_update.1.generic_delete(0, &key, opts)?;
                    }
                    trie_update.generic_delete(0, &key, opts)?;
                }
            }
        }

        let mut trie_changes = trie_update.to_trie_changes();
        for (shard_uid, trie_update) in child_updates {
            trie_changes
                .children_memtrie_changes
                .insert(**shard_uid, trie_update.to_memtrie_changes_only());
        }

        Ok(trie_changes)
    }

    fn update_with_trie_storage<I>(
        &self,
        changes: I,
        opts: AccessOptions,
    ) -> Result<TrieChanges, StorageError>
    where
        I: IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    {
        let mut trie_update = TrieStorageUpdate::new(&self);
        let root_node = self.move_node_to_mutable(&mut trie_update, &self.root, opts)?;
        for (key, value) in changes {
            match value {
                Some(arr) => trie_update.generic_insert(
                    root_node.0,
                    &key,
                    GenericTrieValue::MemtrieAndDisk(arr),
                    opts,
                )?,
                None => trie_update.generic_delete(0, &key, opts)?,
            };
        }

        #[cfg(test)]
        {
            self.memory_usage_verify(&trie_update, GenericNodeOrIndex::Updated(root_node.0));
        }

        trie_update.flatten_nodes(&self.root, root_node.0)
    }

    /// Returns an iterator that can be used to traverse any range in the trie.
    /// This only uses the on-disk trie. If memtrie iteration is desired, see
    /// `lock_for_iter`.
    #[inline]
    pub fn disk_iter(&self) -> Result<DiskTrieIterator, StorageError> {
        self.disk_iter_with_prune_condition(None)
    }

    #[cfg(test)]
    pub(crate) fn disk_iter_with_max_depth(
        &self,
        max_depth: usize,
    ) -> Result<DiskTrieIterator, StorageError> {
        let prune_condition = Box::new(move |key_nibbles: &Vec<u8>| key_nibbles.len() > max_depth);
        self.disk_iter_with_prune_condition(Some(prune_condition))
    }

    #[inline]
    pub fn disk_iter_with_prune_condition(
        &self,
        prune_condition: Option<Box<dyn Fn(&Vec<u8>) -> bool>>,
    ) -> Result<DiskTrieIterator, StorageError> {
        DiskTrieIterator::new(DiskTrieIteratorInner::new(self), prune_condition)
    }

    /// Grabs a read lock on the trie, so that a memtrie iterator can be
    /// constructed afterward. This is needed because memtries are not
    /// thread-safe.
    pub fn lock_for_iter(&self) -> TrieWithReadLock<'_> {
        TrieWithReadLock { trie: self, memtries: self.memtries.as_ref().map(|m| m.read()) }
    }

    /// Splits the trie, separating entries by the boundary account.
    /// Leaves the left or right part of the trie, depending on the retain mode.
    ///
    /// Returns the new root hash of the trie.
    pub fn retain_split_shard(
        &self,
        boundary_account: &AccountId,
        retain_mode: RetainMode,
    ) -> Result<TrieChanges, StorageError> {
        if self.memtries.is_some() {
            self.retain_split_shard_with_memtrie(boundary_account, retain_mode)
        } else {
            self.retain_split_shard_with_trie_storage(boundary_account, retain_mode)
        }
    }

    fn retain_split_shard_with_memtrie(
        &self,
        boundary_account: &AccountId,
        retain_mode: RetainMode,
    ) -> Result<TrieChanges, StorageError> {
        // Get trie_update for memtrie
        let guard = self.memtries.as_ref().unwrap().read();
        let mut recorder = self.recorder.as_ref().map(|recorder| recorder.write());
        let tracking_mode = match &mut recorder {
            Some(recorder) => TrackingMode::RefcountsAndAccesses(&mut *recorder),
            None => TrackingMode::Refcounts,
        };
        let mut trie_update = guard.update(self.root, tracking_mode)?;
        trie_update.retain_split_shard(boundary_account, retain_mode, AccessOptions::DEFAULT);
        let mut trie_changes = trie_update.to_trie_changes();

        // Get child trie_changes for all child memtries
        for (shard_uid, memtrie) in &self.children_memtries {
            let inner_guard = memtrie.read();
            let mut trie_update = inner_guard.update(self.root, TrackingMode::None).unwrap();
            trie_update.retain_split_shard(boundary_account, retain_mode, AccessOptions::DEFAULT);
            trie_changes
                .children_memtrie_changes
                .insert(*shard_uid, trie_update.to_memtrie_changes_only());
        }

        Ok(trie_changes)
    }

    fn retain_split_shard_with_trie_storage(
        &self,
        boundary_account: &AccountId,
        retain_mode: RetainMode,
    ) -> Result<TrieChanges, StorageError> {
        let mut trie_update = TrieStorageUpdate::new(&self);
        let root_node =
            self.move_node_to_mutable(&mut trie_update, &self.root, AccessOptions::DEFAULT)?;
        trie_update.retain_split_shard(boundary_account, retain_mode, AccessOptions::DEFAULT);
        #[cfg(test)]
        {
            self.memory_usage_verify(&trie_update, GenericNodeOrIndex::Updated(root_node.0));
        }
        let trie_changes = trie_update.flatten_nodes(&self.root, root_node.0)?;
        Ok(trie_changes)
    }
}

/// A wrapper around `Trie`, but holding a read lock on memtries if they are present.
/// This is needed to construct an memtrie iterator, as memtries are not thread-safe.
pub struct TrieWithReadLock<'a> {
    trie: &'a Trie,
    memtries: Option<RwLockReadGuard<'a, MemTries>>,
}

impl<'a> TrieWithReadLock<'a> {
    /// Obtains an iterator that can be used to traverse any range in the trie.
    /// If memtries are present, returns an iterator that traverses the memtrie.
    /// Otherwise, it falls back to an iterator that traverses the on-disk trie.
    pub fn iter(&self) -> Result<TrieIterator<'_>, StorageError> {
        match &self.memtries {
            Some(memtries) => Ok(TrieIterator::Memtrie(memtries.get_iter(self.trie)?)),
            None => Ok(TrieIterator::Disk(DiskTrieIterator::new(
                DiskTrieIteratorInner::new(&self.trie),
                None,
            )?)),
        }
    }
}

impl TrieAccess for Trie {
    fn get(&self, key: &TrieKey, opts: AccessOptions) -> Result<Option<Vec<u8>>, StorageError> {
        Trie::get(self, &key.to_vec(), opts)
    }

    fn contains_key(&self, key: &TrieKey, opts: AccessOptions) -> Result<bool, StorageError> {
        Trie::contains_key(&self, &key.to_vec(), opts)
    }
}

// FIXME(nagisa): this doesn't have to be inside near-store anymore.
/// Counts trie nodes reads during tx/receipt execution for proper storage costs charging.
#[derive(Debug, PartialEq)]
pub struct TrieNodesCount {
    /// Potentially expensive trie node reads which are served from disk in the worst case.
    pub db_reads: u64,
    /// Cheap trie node reads which are guaranteed to be served from RAM.
    pub mem_reads: u64,
}

impl TrieNodesCount {
    /// Used to determine the number of trie nodes touched during some operation.
    pub fn checked_sub(self, other: &Self) -> Option<Self> {
        Some(Self {
            db_reads: self.db_reads.checked_sub(other.db_reads)?,
            mem_reads: self.mem_reads.checked_sub(other.mem_reads)?,
        })
    }
}

/// Methods used in the runtime-parameter-estimator for measuring trie internal
/// operations.
pub mod estimator {
    use borsh::BorshDeserialize;
    use near_primitives::hash::CryptoHash;

    /// Create an encoded extension node with the given value as the key.
    /// This serves no purpose other than for the estimator.
    pub fn encode_extension_node(key: Vec<u8>) -> Vec<u8> {
        let hash = CryptoHash::hash_bytes(&key);
        let node = super::RawTrieNode::Extension(key, hash);
        let node = super::RawTrieNodeWithSize { node, memory_usage: 1 };
        borsh::to_vec(&node).unwrap()
    }
    /// Decode am extension node and return its inner key.
    /// This serves no purpose other than for the estimator.
    pub fn decode_extension_node(bytes: &[u8]) -> Vec<u8> {
        let node = super::RawTrieNodeWithSize::try_from_slice(bytes).unwrap();
        match node.node {
            super::RawTrieNode::Extension(v, _) => v,
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    // cspell:ignore cataa catbb docu dogaa dogax dogbb
    use assert_matches::assert_matches;
    use near_primitives::shard_layout::ShardLayout;
    use rand::Rng;

    use crate::MissingTrieValueContext;
    use crate::test_utils::{
        TestTriesBuilder, create_test_store, gen_changes, simplify_changes,
        test_populate_flat_storage, test_populate_trie,
    };

    use super::*;

    type TrieChanges = Vec<(Vec<u8>, Option<Vec<u8>>)>;
    const SHARD_VERSION: u32 = 1;

    fn test_clear_trie(
        tries: &ShardTries,
        root: &CryptoHash,
        shard_uid: ShardUId,
        changes: TrieChanges,
    ) -> CryptoHash {
        let delete_changes: TrieChanges =
            changes.iter().map(|(key, _)| (key.clone(), None)).collect();
        let trie_changes = tries
            .get_trie_for_shard(shard_uid, *root)
            .update(delete_changes, AccessOptions::DEFAULT)
            .unwrap();
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        let trie = tries.get_trie_for_shard(shard_uid, root);
        store_update.commit().unwrap();
        for (key, _) in changes {
            assert_eq!(trie.get(&key, AccessOptions::DEFAULT), Ok(None));
        }
        root
    }

    #[test]
    fn test_basic_trie() {
        // test trie version > 0
        let shard_layout = ShardLayout::multi_shard(2, SHARD_VERSION);
        let shard_uid = shard_layout.shard_uids().next().unwrap();

        let tries = TestTriesBuilder::new().with_shard_layout(shard_layout).build();
        let trie = tries.get_trie_for_shard(shard_uid, Trie::EMPTY_ROOT);
        assert_eq!(trie.get(&[122], AccessOptions::DEFAULT), Ok(None));
        let changes = vec![
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"docu".to_vec(), Some(b"value".to_vec())),
            (b"do".to_vec(), Some(b"verb".to_vec())),
            (b"horse".to_vec(), Some(b"stallion".to_vec())),
            (b"dog".to_vec(), Some(b"puppy".to_vec())),
            (b"h".to_vec(), Some(b"value".to_vec())),
        ];
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, changes.clone());
        let new_root = test_clear_trie(&tries, &root, shard_uid, changes);
        assert_eq!(new_root, Trie::EMPTY_ROOT);
        assert_eq!(trie.disk_iter().unwrap().fold(0, |acc, _| acc + 1), 0);
    }

    #[test]
    fn test_trie_iter() {
        let shard_layout = ShardLayout::multi_shard(2, SHARD_VERSION);
        let shard_uid = shard_layout.shard_uids().next().unwrap();

        let tries = TestTriesBuilder::new().with_shard_layout(shard_layout).build();
        let pairs = vec![
            (b"a".to_vec(), Some(b"111".to_vec())),
            (b"b".to_vec(), Some(b"222".to_vec())),
            (b"x".to_vec(), Some(b"333".to_vec())),
            (b"y".to_vec(), Some(b"444".to_vec())),
        ];
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, pairs.clone());
        let trie = tries.get_trie_for_shard(shard_uid, root);
        let mut iter_pairs = vec![];
        for pair in trie.disk_iter().unwrap() {
            let (key, value) = pair.unwrap();
            iter_pairs.push((key, Some(value.to_vec())));
        }
        assert_eq!(pairs, iter_pairs);

        let assert_has_next = |want, other_iter: &mut DiskTrieIterator| {
            assert_eq!(Some(want), other_iter.next().map(|item| item.unwrap().0).as_deref());
        };

        let mut other_iter = trie.disk_iter().unwrap();
        other_iter.seek_prefix(b"r").unwrap();
        assert_eq!(other_iter.next(), None);
        other_iter.seek_prefix(b"x").unwrap();
        assert_has_next(b"x", &mut other_iter);
        assert_eq!(other_iter.next(), None);
        other_iter.seek_prefix(b"y").unwrap();
        assert_has_next(b"y", &mut other_iter);
        assert_eq!(other_iter.next(), None);
    }

    #[test]
    fn test_trie_leaf_into_branch() {
        let shard_layout = ShardLayout::multi_shard(2, SHARD_VERSION);
        let shard_uid = shard_layout.shard_uids().next().unwrap();

        let tries = TestTriesBuilder::new().with_shard_layout(shard_layout).build();
        let changes = vec![
            (b"dog".to_vec(), Some(b"puppy".to_vec())),
            (b"dog2".to_vec(), Some(b"puppy".to_vec())),
            (b"xxx".to_vec(), Some(b"puppy".to_vec())),
        ];
        test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, changes);
    }

    #[test]
    fn test_trie_same_node() {
        let tries = TestTriesBuilder::new().build();
        let changes = vec![
            (b"dogaa".to_vec(), Some(b"puppy".to_vec())),
            (b"dogbb".to_vec(), Some(b"puppy".to_vec())),
            (b"cataa".to_vec(), Some(b"puppy".to_vec())),
            (b"catbb".to_vec(), Some(b"puppy".to_vec())),
            (b"dogax".to_vec(), Some(b"puppy".to_vec())),
        ];
        test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), changes);
    }

    #[test]
    fn test_trie_iter_seek_stop_at_extension() {
        let tries = TestTriesBuilder::new().build();
        let changes = vec![
            (vec![0, 116, 101, 115, 116], Some(vec![0])),
            (vec![2, 116, 101, 115, 116], Some(vec![0])),
            (
                vec![
                    0, 116, 101, 115, 116, 44, 98, 97, 108, 97, 110, 99, 101, 115, 58, 98, 111, 98,
                    46, 110, 101, 97, 114,
                ],
                Some(vec![0]),
            ),
            (
                vec![
                    0, 116, 101, 115, 116, 44, 98, 97, 108, 97, 110, 99, 101, 115, 58, 110, 117,
                    108, 108,
                ],
                Some(vec![0]),
            ),
        ];
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), changes);
        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), root);
        let mut iter = trie.disk_iter().unwrap();
        iter.seek_prefix(&[0, 116, 101, 115, 116, 44]).unwrap();
        let mut pairs = vec![];
        for pair in iter {
            pairs.push(pair.unwrap().0);
        }
        assert_eq!(
            pairs,
            [
                vec![
                    0, 116, 101, 115, 116, 44, 98, 97, 108, 97, 110, 99, 101, 115, 58, 98, 111, 98,
                    46, 110, 101, 97, 114
                ],
                vec![
                    0, 116, 101, 115, 116, 44, 98, 97, 108, 97, 110, 99, 101, 115, 58, 110, 117,
                    108, 108
                ],
            ]
        );
    }

    #[test]
    fn test_trie_remove_non_existent_key() {
        let tries = TestTriesBuilder::new().build();
        let initial = vec![
            (vec![99, 44, 100, 58, 58, 49], Some(vec![1])),
            (vec![99, 44, 100, 58, 58, 50], Some(vec![1])),
            (vec![99, 44, 100, 58, 58, 50, 51], Some(vec![1])),
        ];
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), initial);

        let changes = vec![
            (vec![99, 44, 100, 58, 58, 45, 49], None),
            (vec![99, 44, 100, 58, 58, 50, 52], None),
        ];
        let root = test_populate_trie(&tries, &root, ShardUId::single_shard(), changes);
        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), root);
        for r in trie.disk_iter().unwrap() {
            r.unwrap();
        }
    }

    #[test]
    fn test_contains_key() {
        let sid = ShardUId::single_shard();
        let bid = CryptoHash::default();
        let tries = TestTriesBuilder::new().with_flat_storage(true).build();
        let opts = AccessOptions::DEFAULT;
        let initial = vec![
            (vec![99, 44, 100, 58, 58, 49], Some(vec![1])),
            (vec![99, 44, 100, 58, 58, 50], Some(vec![1])),
            (vec![99, 44, 100, 58, 58, 50, 51], Some(vec![1])),
        ];
        test_populate_flat_storage(&tries, sid, &bid, &bid, &initial);
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, sid, initial);
        let trie = tries.get_trie_with_block_hash_for_shard(sid, root, &bid, false);
        assert!(trie.has_flat_storage_chunk_view());
        assert!(
            trie.contains_key_mode(&[99, 44, 100, 58, 58, 49], KeyLookupMode::MemOrTrie, opts)
                .unwrap()
        );
        assert!(
            trie.contains_key_mode(
                &[99, 44, 100, 58, 58, 49],
                KeyLookupMode::MemOrFlatOrTrie,
                opts
            )
            .unwrap()
        );
        assert!(
            !trie
                .contains_key_mode(&[99, 44, 100, 58, 58, 48], KeyLookupMode::MemOrTrie, opts)
                .unwrap()
        );
        assert!(
            !trie
                .contains_key_mode(&[99, 44, 100, 58, 58, 48], KeyLookupMode::MemOrFlatOrTrie, opts)
                .unwrap()
        );
        let changes = vec![(vec![99, 44, 100, 58, 58, 49], None)];
        test_populate_flat_storage(&tries, sid, &bid, &bid, &changes);
        let root = test_populate_trie(&tries, &root, sid, changes);
        let trie = tries.get_trie_with_block_hash_for_shard(sid, root, &bid, false);
        assert!(trie.has_flat_storage_chunk_view());
        assert!(
            trie.contains_key_mode(&[99, 44, 100, 58, 58, 50], KeyLookupMode::MemOrTrie, opts)
                .unwrap()
        );
        assert!(
            trie.contains_key_mode(
                &[99, 44, 100, 58, 58, 50],
                KeyLookupMode::MemOrFlatOrTrie,
                opts
            )
            .unwrap()
        );
        assert!(
            !trie
                .contains_key_mode(&[99, 44, 100, 58, 58, 49], KeyLookupMode::MemOrFlatOrTrie, opts)
                .unwrap()
        );
        assert!(
            !trie
                .contains_key_mode(&[99, 44, 100, 58, 58, 49], KeyLookupMode::MemOrTrie, opts)
                .unwrap()
        );
    }

    #[test]
    fn test_equal_leafs() {
        let initial = vec![
            (vec![1, 2, 3], Some(vec![1])),
            (vec![2, 2, 3], Some(vec![1])),
            (vec![3, 2, 3], Some(vec![1])),
        ];
        let tries = TestTriesBuilder::new().build();
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), initial);
        tries.get_trie_for_shard(ShardUId::single_shard(), root).disk_iter().unwrap().for_each(
            |result| {
                result.unwrap();
            },
        );

        let changes = vec![(vec![1, 2, 3], None)];
        let root = test_populate_trie(&tries, &root, ShardUId::single_shard(), changes);
        tries.get_trie_for_shard(ShardUId::single_shard(), root).disk_iter().unwrap().for_each(
            |result| {
                result.unwrap();
            },
        );
    }

    #[test]
    fn test_trie_unique() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let tries = TestTriesBuilder::new().build();
            let trie = tries.get_trie_for_shard(ShardUId::single_shard(), Trie::EMPTY_ROOT);
            let trie_changes = gen_changes(&mut rng, 20);
            let simplified_changes = simplify_changes(&trie_changes);

            let trie_changes1 =
                trie.update(trie_changes.iter().cloned(), AccessOptions::DEFAULT).unwrap();
            let trie_changes2 =
                trie.update(simplified_changes.iter().cloned(), AccessOptions::DEFAULT).unwrap();
            if trie_changes1.new_root != trie_changes2.new_root {
                eprintln!("{:?}", trie_changes);
                eprintln!("{:?}", simplified_changes);
                eprintln!("root1: {:?}", trie_changes1.new_root);
                eprintln!("root2: {:?}", trie_changes2.new_root);
                panic!("MISMATCH!");
            }
            // TODO: compare state updates?
        }
    }

    #[test]
    fn test_iterator_seek_prefix() {
        let mut rng = rand::thread_rng();
        for _test_run in 0..10 {
            let tries = TestTriesBuilder::new().build();
            let trie_changes = gen_changes(&mut rng, 500);
            let state_root = test_populate_trie(
                &tries,
                &Trie::EMPTY_ROOT,
                ShardUId::single_shard(),
                trie_changes.clone(),
            );
            let trie = tries.get_trie_for_shard(ShardUId::single_shard(), state_root);

            // Those known keys.
            #[allow(clippy::needless_collect)] // It is necessary to build a map to deduplicate keys
            for (key, value) in trie_changes.into_iter().collect::<HashMap<_, _>>() {
                if let Some(value) = value {
                    let want = Some(Ok((key.clone(), value)));
                    let mut iterator = trie.disk_iter().unwrap();
                    iterator.seek_prefix(&key).unwrap();
                    assert_eq!(want, iterator.next(), "key: {key:x?}");
                }
            }

            // Test some more random keys.
            let queries = gen_changes(&mut rng, 500).into_iter().map(|(key, _)| key);
            for query in queries {
                let mut iterator = trie.disk_iter().unwrap();
                iterator.seek_prefix(&query).unwrap();
                if let Some(Ok((key, _))) = iterator.next() {
                    assert!(key.starts_with(&query), "‘{key:x?}’ does not start with ‘{query:x?}’");
                }
            }
        }
    }

    #[test]
    fn test_refcounts() {
        let mut rng = rand::thread_rng();
        for _test_run in 0..10 {
            let num_iterations = rng.gen_range(1..20);
            let tries = TestTriesBuilder::new().build();
            let store = tries.store();
            let mut state_root = Trie::EMPTY_ROOT;
            for _ in 0..num_iterations {
                let trie_changes = gen_changes(&mut rng, 20);
                state_root =
                    test_populate_trie(&tries, &state_root, ShardUId::single_shard(), trie_changes);
                let memory_usage = tries
                    .get_trie_for_shard(ShardUId::single_shard(), state_root)
                    .retrieve_root_node()
                    .unwrap()
                    .memory_usage;
                println!("New memory_usage: {memory_usage}");
            }

            let trie = tries.get_trie_for_shard(ShardUId::single_shard(), state_root);
            let trie_changes = trie
                .disk_iter()
                .unwrap()
                .map(|item| {
                    let (key, _) = item.unwrap();
                    (key, None)
                })
                .collect::<Vec<_>>();
            state_root =
                test_populate_trie(&tries, &state_root, ShardUId::single_shard(), trie_changes);
            assert_eq!(state_root, Trie::EMPTY_ROOT, "Trie must be empty");
            assert!(store.iter_raw_bytes().peekable().peek().is_none(), "Storage must be empty");
        }
    }

    #[test]
    fn test_trie_restart() {
        let store = create_test_store();
        let tries = TestTriesBuilder::new().with_store(store.clone()).build();
        let empty_root = Trie::EMPTY_ROOT;
        let changes = vec![
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"docu".to_vec(), Some(b"value".to_vec())),
            (b"do".to_vec(), Some(b"verb".to_vec())),
            (b"horse".to_vec(), Some(b"stallion".to_vec())),
            (b"dog".to_vec(), Some(b"puppy".to_vec())),
            (b"h".to_vec(), Some(b"value".to_vec())),
        ];
        let root = test_populate_trie(&tries, &empty_root, ShardUId::single_shard(), changes);

        let tries2 = TestTriesBuilder::new().with_store(store).build();
        let trie2 = tries2.get_trie_for_shard(ShardUId::single_shard(), root);
        assert_eq!(trie2.get(b"doge", AccessOptions::DEFAULT), Ok(Some(b"coin".to_vec())));
    }

    // TODO: somehow also test that we don't record unnecessary nodes
    #[test]
    fn test_trie_recording_reads() {
        let tries = TestTriesBuilder::new().build();
        let empty_root = Trie::EMPTY_ROOT;
        let changes = vec![
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"docu".to_vec(), Some(b"value".to_vec())),
            (b"do".to_vec(), Some(b"verb".to_vec())),
            (b"horse".to_vec(), Some(b"stallion".to_vec())),
            (b"dog".to_vec(), Some(b"puppy".to_vec())),
            (b"h".to_vec(), Some(b"value".to_vec())),
        ];
        let root = test_populate_trie(&tries, &empty_root, ShardUId::single_shard(), changes);

        let trie2 =
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder();
        trie2.get(b"dog", AccessOptions::DEFAULT).unwrap();
        trie2.get(b"horse", AccessOptions::DEFAULT).unwrap();
        let partial_storage = trie2.recorded_storage();

        let trie3 = Trie::from_recorded_storage(partial_storage.unwrap(), root, false);

        assert_eq!(trie3.get(b"dog", AccessOptions::DEFAULT), Ok(Some(b"puppy".to_vec())));
        assert_eq!(trie3.get(b"horse", AccessOptions::DEFAULT), Ok(Some(b"stallion".to_vec())));
        assert_matches!(
            trie3.get(b"doge", AccessOptions::DEFAULT),
            Err(StorageError::MissingTrieValue(
                MissingTrieValueContext::TrieMemoryPartialStorage,
                _
            ))
        );
    }

    #[test]
    fn test_trie_recording_reads_update() {
        let tries = TestTriesBuilder::new().build();
        let empty_root = Trie::EMPTY_ROOT;
        let changes = vec![
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"docu".to_vec(), Some(b"value".to_vec())),
        ];
        let root = test_populate_trie(&tries, &empty_root, ShardUId::single_shard(), changes);
        // Trie: extension -> branch -> 2 leaves
        {
            let trie2 = tries
                .get_trie_for_shard(ShardUId::single_shard(), root)
                .recording_reads_new_recorder();
            trie2.get(b"doge", AccessOptions::DEFAULT).unwrap();
            // record extension, branch and one leaf with value, but not the other
            assert_eq!(trie2.recorded_storage().unwrap().nodes.len(), 4);
        }

        {
            let trie2 = tries
                .get_trie_for_shard(ShardUId::single_shard(), root)
                .recording_reads_new_recorder();
            let updates = vec![(b"doge".to_vec(), None)];
            trie2.update(updates, AccessOptions::DEFAULT).unwrap();
            // record extension, branch and both leaves, but not the value.
            assert_eq!(trie2.recorded_storage().unwrap().nodes.len(), 4);
        }

        {
            let trie2 = tries
                .get_trie_for_shard(ShardUId::single_shard(), root)
                .recording_reads_new_recorder();
            let updates = vec![(b"dodo".to_vec(), Some(b"asdf".to_vec()))];
            trie2.update(updates, AccessOptions::DEFAULT).unwrap();
            // record extension and branch, but not leaves
            assert_eq!(trie2.recorded_storage().unwrap().nodes.len(), 2);
        }
    }

    #[test]
    fn test_dump_load_trie() {
        let store = create_test_store();
        let tries = TestTriesBuilder::new().with_store(store.clone()).build();
        let empty_root = Trie::EMPTY_ROOT;
        let changes = vec![
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"docu".to_vec(), Some(b"value".to_vec())),
        ];
        let root = test_populate_trie(&tries, &empty_root, ShardUId::single_shard(), changes);
        let dir = tempfile::Builder::new().prefix("test_dump_load_trie").tempdir().unwrap();
        store.save_state_to_file(&dir.path().join("test.bin")).unwrap();
        let store2 = create_test_store();
        store2.load_state_from_file(&dir.path().join("test.bin")).unwrap();
        let tries2 = TestTriesBuilder::new().with_store(store2).build();
        let trie2 = tries2.get_trie_for_shard(ShardUId::single_shard(), root);
        assert_eq!(trie2.get(b"doge", AccessOptions::DEFAULT).unwrap().unwrap(), b"coin");
    }
}

#[cfg(test)]
mod borsh_compatibility_test {
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_primitives::hash::{CryptoHash, hash};
    use near_primitives::types::StateRoot;

    use crate::TrieChanges;
    use crate::trie::{TrieRefcountAddition, TrieRefcountSubtraction};

    #[test]
    fn test_trie_changes_compatibility() {
        #[derive(BorshSerialize)]
        struct LegacyTrieRefcountChange {
            trie_node_or_value_hash: CryptoHash,
            trie_node_or_value: Vec<u8>,
            rc: std::num::NonZeroU32,
        }

        #[derive(BorshSerialize)]
        struct LegacyTrieChanges {
            old_root: StateRoot,
            new_root: StateRoot,
            insertions: Vec<LegacyTrieRefcountChange>,
            deletions: Vec<LegacyTrieRefcountChange>,
        }

        let changes = LegacyTrieChanges {
            old_root: hash(b"a"),
            new_root: hash(b"b"),
            insertions: vec![LegacyTrieRefcountChange {
                trie_node_or_value_hash: hash(b"c"),
                trie_node_or_value: b"d".to_vec(),
                rc: std::num::NonZeroU32::new(1).unwrap(),
            }],
            deletions: vec![LegacyTrieRefcountChange {
                trie_node_or_value_hash: hash(b"e"),
                trie_node_or_value: b"f".to_vec(),
                rc: std::num::NonZeroU32::new(2).unwrap(),
            }],
        };

        let serialized = borsh::to_vec(&changes).unwrap();
        let deserialized = TrieChanges::try_from_slice(&serialized).unwrap();
        assert_eq!(
            deserialized,
            TrieChanges {
                old_root: hash(b"a"),
                new_root: hash(b"b"),
                insertions: vec![TrieRefcountAddition {
                    trie_node_or_value_hash: hash(b"c"),
                    trie_node_or_value: b"d".to_vec(),
                    rc: std::num::NonZeroU32::new(1).unwrap(),
                }],
                deletions: vec![TrieRefcountSubtraction::new(
                    hash(b"e"),
                    std::num::NonZeroU32::new(2).unwrap(),
                )],
                memtrie_changes: None,
                children_memtrie_changes: Default::default(),
            }
        );
    }
}
