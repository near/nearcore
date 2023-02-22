//! FlatStorage is created as an additional representation of the state alongside with Tries.
//! It simply stores a mapping for all key value pairs stored in our Tries (leaves of the trie)
//! It is used for fast key value look up in the state. Reading a single key/value in trie
//! requires traversing the trie from the root, loading many nodes from the database. In flat storage,
//! we store a mapping from keys to value references so that key value lookup will only require two
//! db accesses - one to get value reference, one to get the value itself. In fact, in the case of small
//! values, flat storage only needs one read, because value will be stored in the mapping instead of
//! value ref.
//!
//! The main challenge in the flat storage implementation is that we need to able to handle forks,
//! so the flat storage API must support key value lookups for different blocks.
//! To achieve that, we store the key value pairs of the state on a block (head of the flat storage)
//! on disk and also store the change deltas for some other blocks in memory. With these deltas,
//! we can perform lookups for the other blocks. See comments in `FlatStorageState` to see
//! which block should be the head of flat storage and which other blocks do flat storage support.
//!
//! This file contains the implementation of FlatStorage. It has three essential structs.
//!
//! `FlatState`: this provides an interface to get value or value references from flat storage. This
//!              is the struct that will be stored as part of Trie. All trie reads will be directed
//!              to the flat state.
//! `FlatStateFactory`: this is to construct flat state.
//! `FlatStorageState`: this stores some information about the state of the flat storage itself,
//!                     for example, all block deltas that are stored in flat storage and a representation
//!                     of the chain formed by these blocks (because we can't access ChainStore
//!                     inside flat storage).

#[allow(unused)]
const POISONED_LOCK_ERR: &str = "The lock was poisoned.";
const BORSH_ERR: &str = "Borsh cannot fail";

#[derive(strum::AsRefStr, Debug, PartialEq, Eq)]
pub enum FlatStorageError {
    /// This means we can't find a path from `flat_head` to the block. Includes `flat_head` hash and block hash,
    /// respectively.
    BlockNotSupported((CryptoHash, CryptoHash)),
    StorageInternalError,
}

impl From<FlatStorageError> for StorageError {
    fn from(err: FlatStorageError) -> Self {
        match err {
            FlatStorageError::BlockNotSupported((head_hash, block_hash)) => {
                StorageError::FlatStorageError(format!(
                    "FlatStorage with head {:?} does not support this block {:?}",
                    head_hash, block_hash
                ))
            }
            FlatStorageError::StorageInternalError => StorageError::StorageInternalError,
        }
    }
}

#[cfg(feature = "protocol_feature_flat_state")]
mod imp {
    use crate::flat_state::{store_helper, FlatStorageState, POISONED_LOCK_ERR};
    use near_primitives::errors::StorageError;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::state::ValueRef;
    use near_primitives::types::ShardId;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tracing::debug;

    use crate::{Store, StoreUpdate};

    /// Struct for getting value references from the flat storage, corresponding
    /// to some block defined in `blocks_to_head`.
    ///
    /// The main interface is the `get_ref` method, which is called in `Trie::get`
    /// and `Trie::get_ref` because they are the same for each shard and they are
    /// requested only once during applying chunk.
    // TODO (#7327): lock flat state when `get_ref` is called or head is being updated. Otherwise, `apply_chunks` and
    // `postprocess_block` parallel execution may corrupt the state.
    #[derive(Clone)]
    pub struct FlatState {
        /// Used to access flat state stored at the head of flat storage.
        /// It should store all trie keys and values/value refs for the state on top of
        /// flat_storage_state.head, except for delayed receipt keys.
        #[allow(unused)]
        store: Store,
        /// The block for which key-value pairs of its state will be retrieved. The flat state
        /// will reflect the state AFTER the block is applied.
        block_hash: CryptoHash,
        /// In-memory cache for the key value pairs stored on disk.
        #[allow(unused)]
        cache: FlatStateCache,
        /// Stores the state of the flat storage, for example, where the head is at and which
        /// blocks' state are stored in flat storage.
        #[allow(unused)]
        flat_storage_state: FlatStorageState,
    }

    #[derive(Clone)]
    pub struct FlatStateCache {
        // TODO: add implementation
    }

    impl FlatState {
        pub fn new(
            store: Store,
            block_hash: CryptoHash,
            cache: FlatStateCache,
            flat_storage_state: FlatStorageState,
        ) -> Self {
            Self { store, block_hash, cache, flat_storage_state }
        }
        /// Returns value reference using raw trie key, taken from the state
        /// corresponding to `FlatState::block_hash`.
        ///
        /// To avoid duplication, we don't store values themselves in flat state,
        /// they are stored in `DBCol::State`. Also the separation is done so we
        /// could charge users for the value length before loading the value.
        // TODO (#7327): consider inlining small values, so we could use only one db access.
        pub fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, crate::StorageError> {
            self.flat_storage_state.get_ref(&self.block_hash, key)
        }
    }

    /// `FlatStateFactory` provides a way to construct new flat state to pass to new tries.
    /// It is owned by NightshadeRuntime, and thus can be owned by multiple threads, so the implementation
    /// must be thread safe.
    #[derive(Clone)]
    pub struct FlatStateFactory(Arc<FlatStateFactoryInner>);

    pub struct FlatStateFactoryInner {
        store: Store,
        caches: Mutex<HashMap<ShardId, FlatStateCache>>,
        /// Here we store the flat_storage_state per shard. The reason why we don't use the same
        /// FlatStorageState for all shards is that there are two modes of block processing,
        /// normal block processing and block catchups. Since these are performed on different range
        /// of blocks, we need flat storage to be able to support different range of blocks
        /// on different shards. So we simply store a different state for each shard.
        /// This may cause some overhead because the data like shards that the node is processing for
        /// this epoch can share the same `head` and `tail`, similar for shards for the next epoch,
        /// but such overhead is negligible comparing the delta sizes, so we think it's ok.
        flat_storage_states: Mutex<HashMap<ShardId, FlatStorageState>>,
    }

    impl FlatStateFactory {
        pub fn new(store: Store) -> Self {
            Self(Arc::new(FlatStateFactoryInner {
                store,
                caches: Default::default(),
                flat_storage_states: Default::default(),
            }))
        }

        /// When a node starts from an empty database, this function must be called to ensure
        /// information such as flat head is set up correctly in the database.
        /// Note that this function is different from `add_flat_storage_state_for_shard`,
        /// it must be called before `add_flat_storage_state_for_shard` if the node starts from
        /// an empty database.
        pub fn set_flat_storage_state_for_genesis(
            &self,
            store_update: &mut StoreUpdate,
            shard_id: ShardId,
            genesis_block: &CryptoHash,
        ) {
            let flat_storage_states = self.0.flat_storage_states.lock().expect(POISONED_LOCK_ERR);
            assert!(!flat_storage_states.contains_key(&shard_id));
            store_helper::set_flat_head(store_update, shard_id, genesis_block);
        }

        /// Add a flat storage state for shard `shard_id`. The function also checks that
        /// the shard's flat storage state hasn't been set before, otherwise it panics.
        /// TODO (#7327): this behavior may change when we implement support for state sync
        /// and resharding.
        pub fn add_flat_storage_state_for_shard(
            &self,
            shard_id: ShardId,
            flat_storage_state: FlatStorageState,
        ) {
            let mut flat_storage_states =
                self.0.flat_storage_states.lock().expect(POISONED_LOCK_ERR);
            let original_value = flat_storage_states.insert(shard_id, flat_storage_state);
            // TODO (#7327): maybe we should propagate the error instead of assert here
            // assert is fine now because this function is only called at construction time, but we
            // will need to be more careful when we want to implement flat storage for resharding
            assert!(original_value.is_none());
        }

        /// Creates `FlatState` to access state for `shard_id` and block `block_hash`. Note that
        /// the state includes changes by the block `block_hash`.
        /// `block_hash`: only create FlatState if it is not None. This is a hack we have temporarily
        ///               to not introduce too many changes in the trie interface.
        /// `is_view`: whether this flat state is used for view client. We use a separate set of caches
        ///            for flat state for client vs view client. For now, we don't support flat state
        ///            for view client, so we simply return None if `is_view` is True.
        /// TODO (#7327): take block_hash as CryptoHash instead of Option<CryptoHash>
        /// TODO (#7327): implement support for view_client
        pub fn new_flat_state_for_shard(
            &self,
            shard_id: ShardId,
            block_hash: Option<CryptoHash>,
            is_view: bool,
        ) -> Option<FlatState> {
            let block_hash = match block_hash {
                Some(block_hash) => block_hash,
                None => {
                    return None;
                }
            };

            if is_view {
                // TODO (#7327): Technically, like TrieCache, we should have a separate set of caches for Client and
                // ViewClient. Right now, we can get by by not enabling flat state for view trie
                None
            } else {
                let cache = {
                    let mut caches = self.0.caches.lock().expect(POISONED_LOCK_ERR);
                    caches.entry(shard_id).or_insert_with(|| FlatStateCache {}).clone()
                };
                let flat_storage_state = {
                    let flat_storage_states =
                        self.0.flat_storage_states.lock().expect(POISONED_LOCK_ERR);
                    // It is possible that flat storage state does not exist yet because it is being created in
                    // background.
                    match flat_storage_states.get(&shard_id) {
                        Some(flat_storage_state) => flat_storage_state.clone(),
                        None => {
                            debug!(target: "chain", "FlatStorageState is not ready");
                            return None;
                        }
                    }
                };
                Some(FlatState::new(self.0.store.clone(), block_hash, cache, flat_storage_state))
            }
        }

        // TODO (#7327): change the function signature to Result<FlatStorageState, Error> when
        // we stabilize feature protocol_feature_flat_state. We use option now to return None when
        // the feature is not enabled. Ideally, it should return an error because it is problematic
        // if the flat storage state does not exist
        pub fn get_flat_storage_state_for_shard(
            &self,
            shard_id: ShardId,
        ) -> Option<FlatStorageState> {
            let flat_storage_states = self.0.flat_storage_states.lock().expect(POISONED_LOCK_ERR);
            flat_storage_states.get(&shard_id).cloned()
        }

        pub fn remove_flat_storage_state_for_shard(
            &self,
            shard_id: ShardId,
            shard_layout: ShardLayout,
        ) -> Result<(), StorageError> {
            let mut flat_storage_states =
                self.0.flat_storage_states.lock().expect(POISONED_LOCK_ERR);

            match flat_storage_states.remove(&shard_id) {
                None => {}
                Some(flat_storage_state) => {
                    flat_storage_state.clear_state(shard_layout)?;
                }
            }

            Ok(())
        }
    }
}

#[cfg(not(feature = "protocol_feature_flat_state"))]
mod imp {
    use crate::flat_state::FlatStorageState;
    use crate::{Store, StoreUpdate};
    use near_primitives::errors::StorageError;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::ShardId;

    /// Since this has no variants it can never be instantiated.
    ///
    /// To use flat state enable `protocol_feature_flat_state` cargo feature.
    #[derive(Clone)]
    pub enum FlatState {}

    impl FlatState {
        pub fn get_ref(&self, _key: &[u8]) -> ! {
            match *self {}
        }
    }

    #[derive(Clone)]
    pub struct FlatStateFactory {}

    impl FlatStateFactory {
        pub fn new(_store: Store) -> Self {
            Self {}
        }

        pub fn new_flat_state_for_shard(
            &self,
            _shard_id: ShardId,
            _block_hash: Option<CryptoHash>,
            _is_view: bool,
        ) -> Option<FlatState> {
            None
        }

        pub fn get_flat_storage_state_for_shard(
            &self,
            _shard_id: ShardId,
        ) -> Option<FlatStorageState> {
            None
        }

        pub fn add_flat_storage_state_for_shard(
            &self,
            _shard_id: ShardId,
            _flat_storage_state: FlatStorageState,
        ) {
        }

        pub fn remove_flat_storage_state_for_shard(
            &self,
            _shard_id: ShardId,
            _shard_layout: ShardLayout,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        pub fn set_flat_storage_state_for_genesis(
            &self,
            _store_update: &mut StoreUpdate,
            _shard_id: ShardId,
            _genesis_block: &CryptoHash,
        ) {
        }
    }
}

use borsh::{BorshDeserialize, BorshSerialize};

use crate::{metrics, CryptoHash, Store, StoreUpdate};
pub use imp::{FlatState, FlatStateFactory};
use near_primitives::state::ValueRef;
use near_primitives::types::{BlockHeight, RawStateChangesWithTrieKey, ShardId};
use std::collections::{HashMap, HashSet};

#[derive(BorshSerialize, BorshDeserialize)]
pub struct KeyForFlatStateDelta {
    pub shard_id: ShardId,
    pub block_hash: CryptoHash,
}

/// Delta of the state for some shard and block, stores mapping from keys to value refs or None, if key was removed in
/// this block.
#[derive(BorshSerialize, BorshDeserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct FlatStateDelta(HashMap<Vec<u8>, Option<ValueRef>>);

impl<const N: usize> From<[(Vec<u8>, Option<ValueRef>); N]> for FlatStateDelta {
    fn from(arr: [(Vec<u8>, Option<ValueRef>); N]) -> Self {
        Self(HashMap::from(arr))
    }
}

impl FlatStateDelta {
    /// Assumed number of bytes used to store an entry in the cache.
    ///
    /// Based on 36 bytes for `ValueRef` + guessed overhead of 24 bytes for `Vec` and `HashMap`.
    pub(crate) const PER_ENTRY_OVERHEAD: u64 = 60;

    /// Returns `Some(Option<ValueRef>)` from delta for the given key. If key is not present, returns None.
    pub fn get(&self, key: &[u8]) -> Option<Option<ValueRef>> {
        self.0.get(key).cloned()
    }

    /// Inserts a key-value pair to delta.
    pub fn insert(&mut self, key: Vec<u8>, value: Option<ValueRef>) -> Option<Option<ValueRef>> {
        self.0.insert(key, value)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    fn total_size(&self) -> u64 {
        self.0.keys().map(|key| key.len() as u64 + Self::PER_ENTRY_OVERHEAD).sum()
    }

    /// Merge two deltas. Values from `other` should override values from `self`.
    pub fn merge(&mut self, other: &Self) {
        self.0.extend(other.0.iter().map(|(k, v)| (k.clone(), v.clone())))
    }

    /// Creates delta using raw state changes for some block.
    pub fn from_state_changes(changes: &[RawStateChangesWithTrieKey]) -> Self {
        let mut delta = HashMap::new();
        for change in changes.iter() {
            let key = change.trie_key.to_vec();
            if near_primitives::state_record::is_delayed_receipt_key(&key) {
                continue;
            }

            // `RawStateChangesWithTrieKey` stores all sequential changes for a key within a chunk, so it is sufficient
            // to take only the last change.
            let last_change = &change
                .changes
                .last()
                .expect("Committed entry should have at least one change")
                .data;
            match last_change {
                Some(value) => {
                    delta.insert(key, Some(near_primitives::state::ValueRef::new(value)))
                }
                None => delta.insert(key, None),
            };
        }
        Self(delta)
    }

    /// Applies delta to the flat state.
    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn apply_to_flat_state(self, store_update: &mut StoreUpdate) {
        for (key, value) in self.0.into_iter() {
            store_helper::set_ref(store_update, key, value).expect(BORSH_ERR);
        }
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    pub fn apply_to_flat_state(self, _store_update: &mut StoreUpdate) {}
}

use near_o11y::metrics::IntGauge;
use near_primitives::errors::StorageError;
use near_primitives::shard_layout::ShardLayout;
use std::sync::{Arc, RwLock};
#[cfg(feature = "protocol_feature_flat_state")]
use tracing::info;

/// FlatStorageState stores information on which blocks flat storage current supports key lookups on.
/// Note that this struct is shared by multiple threads, the chain thread, threads that apply chunks,
/// and view client, so the implementation here must be thread safe and must have interior mutability,
/// thus all methods in this class are with &self instead of &mut self.
#[derive(Clone)]
pub struct FlatStorageState(Arc<RwLock<FlatStorageStateInner>>);

/// Max number of blocks that flat storage can keep
/// FlatStorage will only support blocks at height [tail_height, tail_height + FLAT_STORAGE_MAX_BLOCKS).
/// Since there is at most one block at each height, flat storage will keep at most FLAT_STORAGE_MAX_BLOCKS
/// of block deltas in memory.
#[allow(unused)]
const FLAT_STORAGE_MAX_BLOCKS: u64 = 16;

#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq)]
pub struct BlockInfo {
    pub hash: CryptoHash,
    pub height: BlockHeight,
    pub prev_hash: CryptoHash,
}

// FlatStorageState need to support concurrent access and be consistent if node crashes or restarts,
// so we make sure to keep the following invariants in our implementation.
// - `flat_head` is stored on disk. The value of flat_head in memory and on disk should always
//   be consistent with the flat state stored in `DbCol::FlatState` on disk. This means, updates to
//   these values much be atomic from the outside.
// - `blocks` and `deltas` store the same set of blocks, except that `flat_head` is in `blocks`,
//     but not in `deltas`. For any block in `blocks`, `flat_head`
//    must be on the same chain as the block and all blocks between `flat_head` and the block must
//    also be in `blocks`.
// - All deltas in `deltas` are stored on disk. And if a block is accepted by chain, its deltas
//   must be stored on disk as well, if the block is children of `flat_head`.
//   This makes sure that when a node restarts, FlatStorageState can load deltas for all blocks
//   after the `flat_head` block successfully.
struct FlatStorageStateInner {
    #[allow(unused)]
    store: Store,
    /// Id of the shard which state is accessed by this flat storage.
    #[allow(unused)]
    shard_id: ShardId,
    /// The block for which we store the key value pairs of the state after it is applied.
    /// For non catchup mode, it should be the last final block.
    #[allow(unused)]
    flat_head: CryptoHash,
    /// Stores some information for all blocks supported by flat storage, this is used for finding
    /// paths between the root block and a target block
    #[allow(unused)]
    blocks: HashMap<CryptoHash, BlockInfo>,
    /// State deltas for all blocks supported by this flat storage.
    /// All these deltas here are stored on disk too.
    #[allow(unused)]
    deltas: HashMap<CryptoHash, Arc<FlatStateDelta>>,
    #[allow(unused)]
    metrics: FlatStorageMetrics,
}

struct FlatStorageMetrics {
    flat_head_height: IntGauge,
    cached_blocks: IntGauge,
    cached_deltas: IntGauge,
    cached_deltas_num_items: IntGauge,
    cached_deltas_size: IntGauge,
    #[allow(unused)]
    distance_to_head: IntGauge,
}

/// Number of traversed parts during a single step of fetching state.
#[allow(unused)]
pub const NUM_PARTS_IN_ONE_STEP: u64 = 20;

/// Memory limit for state part being fetched.
#[allow(unused)]
pub const STATE_PART_MEMORY_LIMIT: bytesize::ByteSize = bytesize::ByteSize(10 * bytesize::MIB);

/// Current step of fetching state to fill flat storage.
#[derive(BorshSerialize, BorshDeserialize, Copy, Clone, Debug, PartialEq, Eq)]
pub struct FetchingStateStatus {
    /// Hash of block on top of which we create flat storage.
    pub block_hash: CryptoHash,
    /// Number of the first state part to be fetched in this step.
    pub part_id: u64,
    /// Number of parts fetched in one step.
    pub num_parts_in_step: u64,
    /// Total number of state parts.
    pub num_parts: u64,
}

/// If a node has flat storage enabled but it didn't have flat storage data on disk, its creation should be initiated.
/// Because this is a heavy work requiring ~5h for testnet rpc node and ~10h for testnet archival node, we do it on
/// background during regular block processing.
/// This struct reveals what is the current status of creating flat storage data on disk.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FlatStorageCreationStatus {
    /// Flat storage state does not exist. We are saving `FlatStorageDelta`s to disk.
    /// During this step, we save current chain head, start saving all deltas for blocks after chain head and wait until
    /// final chain head moves after saved chain head.
    SavingDeltas,
    /// Flat storage state misses key-value pairs. We need to fetch Trie state to fill flat storage for some final chain
    /// head. It is the heaviest work, so it is done in multiple steps, see comment for `FetchingStateStatus` for more
    /// details.
    /// During each step we spawn background threads to fill some contiguous range of state keys.
    /// Status contains block hash for which we fetch the shard state and number of current step. Progress of each step
    /// is saved to disk, so if creation is interrupted during some step, we don't repeat previous steps, starting from
    /// the saved step again.
    #[allow(unused)]
    FetchingState(FetchingStateStatus),
    /// Flat storage data exists on disk but block which is corresponds to is earlier than chain final head.
    /// We apply deltas from disk until the head reaches final head.
    /// Includes block hash of flat storage head.
    #[allow(unused)]
    CatchingUp(CryptoHash),
    /// Flat storage is ready to use.
    Ready,
    /// Flat storage cannot be created.
    DontCreate,
}

impl Into<i64> for &FlatStorageCreationStatus {
    /// Converts status to integer to export to prometheus later.
    /// Cast inside enum does not work because it is not fieldless.
    fn into(self) -> i64 {
        match self {
            FlatStorageCreationStatus::SavingDeltas => 0,
            FlatStorageCreationStatus::FetchingState(_) => 1,
            FlatStorageCreationStatus::CatchingUp(_) => 2,
            FlatStorageCreationStatus::Ready => 3,
            FlatStorageCreationStatus::DontCreate => 4,
        }
    }
}

#[cfg(feature = "protocol_feature_flat_state")]
pub mod store_helper {
    use crate::flat_state::{
        FetchingStateStatus, FlatStorageCreationStatus, FlatStorageError, KeyForFlatStateDelta,
    };
    use crate::{FlatStateDelta, Store, StoreUpdate};
    use borsh::{BorshDeserialize, BorshSerialize};
    use byteorder::ReadBytesExt;
    use near_primitives::errors::StorageError;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::{account_id_to_shard_id, ShardLayout};
    use near_primitives::state::ValueRef;
    use near_primitives::trie_key::trie_key_parsers::parse_account_id_from_raw_key;
    use near_primitives::types::ShardId;
    use std::sync::Arc;

    /// Prefixes determining type of flat storage creation status stored in DB.
    /// Note that non-existent status is treated as SavingDeltas if flat storage
    /// does not exist and Ready if it does.
    const FETCHING_STATE: u8 = 0;
    const CATCHING_UP: u8 = 1;

    /// Prefixes for keys in `FlatStateMisc` DB column.
    pub const FLAT_STATE_HEAD_KEY_PREFIX: &[u8; 4] = b"HEAD";
    pub const FLAT_STATE_CREATION_STATUS_KEY_PREFIX: &[u8; 6] = b"STATUS";

    pub fn get_delta(
        store: &Store,
        shard_id: ShardId,
        block_hash: CryptoHash,
    ) -> Result<Option<Arc<FlatStateDelta>>, FlatStorageError> {
        let key = KeyForFlatStateDelta { shard_id, block_hash };
        Ok(store
            .get_ser::<FlatStateDelta>(crate::DBCol::FlatStateDeltas, &key.try_to_vec().unwrap())
            .map_err(|_| FlatStorageError::StorageInternalError)?
            .map(|delta| Arc::new(delta)))
    }

    pub fn set_delta(
        store_update: &mut StoreUpdate,
        shard_id: ShardId,
        block_hash: CryptoHash,
        delta: &FlatStateDelta,
    ) -> Result<(), FlatStorageError> {
        let key = KeyForFlatStateDelta { shard_id, block_hash };
        store_update
            .set_ser(crate::DBCol::FlatStateDeltas, &key.try_to_vec().unwrap(), delta)
            .map_err(|_| FlatStorageError::StorageInternalError)
    }

    pub fn remove_delta(store_update: &mut StoreUpdate, shard_id: ShardId, block_hash: CryptoHash) {
        let key = KeyForFlatStateDelta { shard_id, block_hash };
        store_update.delete(crate::DBCol::FlatStateDeltas, &key.try_to_vec().unwrap());
    }

    fn flat_head_key(shard_id: ShardId) -> Vec<u8> {
        let mut fetching_state_step_key = FLAT_STATE_HEAD_KEY_PREFIX.to_vec();
        fetching_state_step_key.extend_from_slice(&shard_id.try_to_vec().unwrap());
        fetching_state_step_key
    }

    pub fn get_flat_head(store: &Store, shard_id: ShardId) -> Option<CryptoHash> {
        store
            .get_ser(crate::DBCol::FlatStateMisc, &flat_head_key(shard_id))
            .expect("Error reading flat head from storage")
    }

    pub fn set_flat_head(store_update: &mut StoreUpdate, shard_id: ShardId, val: &CryptoHash) {
        store_update
            .set_ser(crate::DBCol::FlatStateMisc, &flat_head_key(shard_id), val)
            .expect("Error writing flat head from storage")
    }

    pub fn remove_flat_head(store_update: &mut StoreUpdate, shard_id: ShardId) {
        store_update.delete(crate::DBCol::FlatStateMisc, &flat_head_key(shard_id));
    }

    pub(crate) fn get_ref(store: &Store, key: &[u8]) -> Result<Option<ValueRef>, FlatStorageError> {
        let raw_ref = store
            .get(crate::DBCol::FlatState, key)
            .map_err(|_| FlatStorageError::StorageInternalError)?;
        if let Some(raw_ref) = raw_ref {
            let bytes = raw_ref
                .as_slice()
                .try_into()
                .map_err(|_| FlatStorageError::StorageInternalError)?;
            Ok(Some(ValueRef::decode(bytes)))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn set_ref(
        store_update: &mut StoreUpdate,
        key: Vec<u8>,
        value: Option<ValueRef>,
    ) -> Result<(), FlatStorageError> {
        match value {
            Some(value) => store_update
                .set_ser(crate::DBCol::FlatState, &key, &value)
                .map_err(|_| FlatStorageError::StorageInternalError),
            None => Ok(store_update.delete(crate::DBCol::FlatState, &key)),
        }
    }

    fn creation_status_key(shard_id: ShardId) -> Vec<u8> {
        let mut key = FLAT_STATE_CREATION_STATUS_KEY_PREFIX.to_vec();
        key.extend_from_slice(&shard_id.try_to_vec().unwrap());
        key
    }

    pub fn set_flat_storage_creation_status(
        store_update: &mut StoreUpdate,
        shard_id: ShardId,
        status: FlatStorageCreationStatus,
    ) {
        let value = match status {
            FlatStorageCreationStatus::FetchingState(status) => {
                let mut value = vec![FETCHING_STATE];
                value.extend_from_slice(&status.try_to_vec().unwrap());
                value
            }
            FlatStorageCreationStatus::CatchingUp(block_hash) => {
                let mut value = vec![CATCHING_UP];
                value.extend_from_slice(block_hash.as_bytes());
                value
            }
            status @ _ => {
                panic!("Attempted to write incorrect flat storage creation status {status:?} for shard {shard_id}");
            }
        };
        store_update.set(crate::DBCol::FlatStateMisc, &creation_status_key(shard_id), &value);
    }

    pub fn get_flat_storage_creation_status(
        store: &Store,
        shard_id: ShardId,
    ) -> FlatStorageCreationStatus {
        match get_flat_head(store, shard_id) {
            Some(_) => {
                return FlatStorageCreationStatus::Ready;
            }
            None => {}
        }

        let value = store
            .get(crate::DBCol::FlatStateMisc, &creation_status_key(shard_id))
            .expect("Error reading status from storage");
        match value {
            None => FlatStorageCreationStatus::SavingDeltas,
            Some(bytes) => {
                let mut bytes = bytes.as_slice();
                let status_type = bytes.read_u8().unwrap();
                match status_type {
                    FETCHING_STATE => FlatStorageCreationStatus::FetchingState(
                        FetchingStateStatus::try_from_slice(bytes).unwrap(),
                    ),
                    CATCHING_UP => FlatStorageCreationStatus::CatchingUp(
                        CryptoHash::try_from_slice(bytes).unwrap(),
                    ),
                    value @ _ => {
                        panic!(
                            "Unexpected value type during getting flat storage creation status: {value}"
                        );
                    }
                }
            }
        }
    }

    pub fn remove_flat_storage_creation_status(store_update: &mut StoreUpdate, shard_id: ShardId) {
        store_update.delete(crate::DBCol::FlatStateMisc, &creation_status_key(shard_id));
    }

    /// Iterate over flat storage entries for a given shard.
    /// It reads data only from the 'main' column - which represents the state as of final head.k
    ///
    /// WARNING: flat storage keeps changing, so the results might be inconsistent, unless you're running
    /// this method on the shapshot of the data.
    pub fn iter_flat_state_entries<'a>(
        shard_layout: ShardLayout,
        shard_id: u64,
        store: &'a Store,
        from: Option<Vec<u8>>,
        to: Option<Vec<u8>>,
    ) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a {
        store.iter(crate::DBCol::FlatState).filter_map(move |result| {
            if let Ok((key, value)) = result {
                // Currently all the data in flat storage is 'together' - so we have to parse the key,
                // to see if this element belongs to this shard.
                if let Ok(key_in_shard) = key_belongs_to_shard(&key, &shard_layout, shard_id) {
                    if key_in_shard {
                        // Right now this function is very slow, as we iterate over whole flat storage DB (and ignore most of the keys).
                        // We should add support to our Database object to handle range iterators.
                        if from.as_ref().map_or(true, |x| x.as_slice() <= key.as_ref())
                            && to.as_ref().map_or(true, |x| x.as_slice() >= key.as_ref())
                        {
                            return Some((key, value));
                        }
                    }
                }
            }
            return None;
        })
    }

    /// Currently all the data in flat storage is 'together' - so we have to parse the key,
    /// to see if this element belongs to this shard.
    pub fn key_belongs_to_shard(
        key: &Box<[u8]>,
        shard_layout: &ShardLayout,
        shard_id: u64,
    ) -> Result<bool, StorageError> {
        let account_id = parse_account_id_from_raw_key(key)
            .map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?
            .ok_or(StorageError::FlatStorageError(format!(
                "Failed to find account id in flat storage key {:?}",
                key
            )))?;
        Ok(account_id_to_shard_id(&account_id, &shard_layout) == shard_id)
    }
}

#[cfg(not(feature = "protocol_feature_flat_state"))]
pub mod store_helper {
    use crate::flat_state::{FlatStateDelta, FlatStorageCreationStatus, FlatStorageError};
    use crate::Store;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::ShardId;
    use std::sync::Arc;

    pub fn get_flat_head(_store: &Store, _shard_id: ShardId) -> Option<CryptoHash> {
        None
    }

    pub fn get_delta(
        _store: &Store,
        _shard_id: ShardId,
        _block_hash: CryptoHash,
    ) -> Result<Option<Arc<FlatStateDelta>>, FlatStorageError> {
        Err(FlatStorageError::StorageInternalError)
    }

    pub fn get_flat_storage_creation_status(
        _store: &Store,
        _shard_id: ShardId,
    ) -> FlatStorageCreationStatus {
        FlatStorageCreationStatus::DontCreate
    }
}

// Unfortunately we don't have access to ChainStore inside this file because of package
// dependencies, so we create this trait that provides the functions that FlatStorageState needs
// to access chain information
pub trait ChainAccessForFlatStorage {
    fn get_block_info(&self, block_hash: &CryptoHash) -> BlockInfo;
    fn get_block_hashes_at_height(&self, block_height: BlockHeight) -> HashSet<CryptoHash>;
}

#[cfg(feature = "protocol_feature_flat_state")]
impl FlatStorageStateInner {
    /// Creates `BlockNotSupported` error for the given block.
    fn create_block_not_supported_error(&self, block_hash: &CryptoHash) -> FlatStorageError {
        FlatStorageError::BlockNotSupported((self.flat_head, *block_hash))
    }

    /// Gets delta for the given block and shard `self.shard_id`.
    fn get_delta(&self, block_hash: &CryptoHash) -> Result<Arc<FlatStateDelta>, FlatStorageError> {
        // TODO (#7327): add limitation on cached deltas number to limit RAM usage
        // and read single `ValueRef` from delta if it is not cached.
        Ok(self
            .deltas
            .get(block_hash)
            .ok_or(self.create_block_not_supported_error(block_hash))?
            .clone())
    }

    /// Get sequence of blocks `target_block_hash` (inclusive) to flat head (exclusive)
    /// in backwards chain order. Returns an error if there is no path between them.
    fn get_blocks_to_head(
        &self,
        target_block_hash: &CryptoHash,
    ) -> Result<Vec<CryptoHash>, FlatStorageError> {
        let shard_id = &self.shard_id;
        let flat_head = &self.flat_head;
        let flat_head_info = self
            .blocks
            .get(flat_head)
            .expect(&format!("Inconsistent flat storage state for shard {shard_id}: head {flat_head} not found in cached blocks"));

        let mut block_hash = target_block_hash.clone();
        let mut blocks = vec![];
        while block_hash != *flat_head {
            let block_info = self
                .blocks
                .get(&block_hash)
                .ok_or(self.create_block_not_supported_error(target_block_hash))?;

            if block_info.height < flat_head_info.height {
                return Err(self.create_block_not_supported_error(target_block_hash));
            }

            blocks.push(block_hash);
            block_hash = block_info.prev_hash;
        }
        self.metrics.distance_to_head.set(blocks.len() as i64);

        Ok(blocks)
    }
}

impl FlatStorageState {
    /// Create a new FlatStorageState for `shard_id` using flat head if it is stored on storage.
    /// We also load all blocks with height between flat head to `latest_block_height`
    /// including those on forks into the returned FlatStorageState.
    pub fn new(
        store: Store,
        shard_id: ShardId,
        latest_block_height: BlockHeight,
        // Unfortunately we don't have access to ChainStore inside this file because of package
        // dependencies, so we pass these functions in to access chain info
        chain_access: &dyn ChainAccessForFlatStorage,
    ) -> Self {
        let flat_head = store_helper::get_flat_head(&store, shard_id)
            .unwrap_or_else(|| panic!("Cannot read flat head for shard {} from storage", shard_id));
        let flat_head_info = chain_access.get_block_info(&flat_head);
        let flat_head_height = flat_head_info.height;
        let mut blocks = HashMap::from([(
            flat_head,
            BlockInfo {
                hash: flat_head,
                height: flat_head_height,
                prev_hash: flat_head_info.prev_hash,
            },
        )]);
        let mut deltas = HashMap::new();

        // `itoa` is much faster for printing shard_id to a string than trivial alternatives.
        let mut buffer = itoa::Buffer::new();
        let shard_id_label = buffer.format(shard_id);
        let metrics = FlatStorageMetrics {
            flat_head_height: metrics::FLAT_STORAGE_HEAD_HEIGHT
                .with_label_values(&[shard_id_label]),
            cached_blocks: metrics::FLAT_STORAGE_CACHED_BLOCKS.with_label_values(&[shard_id_label]),
            cached_deltas: metrics::FLAT_STORAGE_CACHED_DELTAS.with_label_values(&[shard_id_label]),
            cached_deltas_num_items: metrics::FLAT_STORAGE_CACHED_DELTAS_NUM_ITEMS
                .with_label_values(&[shard_id_label]),
            cached_deltas_size: metrics::FLAT_STORAGE_CACHED_DELTAS_SIZE
                .with_label_values(&[shard_id_label]),
            distance_to_head: metrics::FLAT_STORAGE_DISTANCE_TO_HEAD
                .with_label_values(&[shard_id_label]),
        };
        metrics.flat_head_height.set(flat_head_height as i64);

        for height in flat_head_height + 1..=latest_block_height {
            for hash in chain_access.get_block_hashes_at_height(height) {
                let block_info = chain_access.get_block_info(&hash);
                assert!(
                    blocks.contains_key(&block_info.prev_hash),
                    "Can't find a path from the current flat head {:?}@{} to block {:?}@{}",
                    flat_head,
                    flat_head_height,
                    hash,
                    block_info.height
                );
                blocks.insert(hash, block_info);
                metrics.cached_blocks.inc();
                let delta = store_helper::get_delta(&store, shard_id, hash)
                    .expect(BORSH_ERR)
                    .unwrap_or_else(|| {
                        panic!("Cannot find block delta for block {:?} shard {}", hash, shard_id)
                    });
                metrics.cached_deltas.inc();
                metrics.cached_deltas_num_items.add(delta.len() as i64);
                metrics.cached_deltas_size.add(delta.total_size() as i64);
                deltas.insert(hash, delta);
            }
        }

        Self(Arc::new(RwLock::new(FlatStorageStateInner {
            store,
            shard_id,
            flat_head,
            blocks,
            deltas,
            metrics,
        })))
    }

    /// Get sequence of blocks `target_block_hash` (inclusive) to flat head (exclusive)
    /// in backwards chain order. Returns an error if there is no path between them.
    #[cfg(feature = "protocol_feature_flat_state")]
    #[cfg(test)]
    fn get_blocks_to_head(
        &self,
        target_block_hash: &CryptoHash,
    ) -> Result<Vec<CryptoHash>, FlatStorageError> {
        let guard = self.0.write().expect(POISONED_LOCK_ERR);
        guard.get_blocks_to_head(target_block_hash)
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    #[allow(unused)]
    fn get_blocks_to_head(
        &self,
        _target_block_hash: &CryptoHash,
    ) -> Result<Vec<CryptoHash>, FlatStorageError> {
        Err(FlatStorageError::StorageInternalError)
    }

    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn get_ref(
        &self,
        block_hash: &CryptoHash,
        key: &[u8],
    ) -> Result<Option<ValueRef>, crate::StorageError> {
        let guard = self.0.write().expect(POISONED_LOCK_ERR);
        let blocks_to_head =
            guard.get_blocks_to_head(block_hash).map_err(|e| StorageError::from(e))?;
        for block_hash in blocks_to_head.iter() {
            // If we found a key in delta, we can return a value because it is the most recent key update.
            let delta = guard.get_delta(block_hash)?;
            match delta.get(key) {
                Some(value_ref) => {
                    return Ok(value_ref);
                }
                None => {}
            };
        }

        Ok(store_helper::get_ref(&guard.store, key)?)
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    #[allow(unused)]
    fn get_ref(
        &self,
        _block_hash: &CryptoHash,
        _key: &[u8],
    ) -> Result<Option<ValueRef>, crate::StorageError> {
        Err(StorageError::StorageInternalError)
    }

    /// Update the head of the flat storage, including updating the flat state in memory and on disk
    /// and updating the flat state to reflect the state at the new head. If updating to given head is not possible,
    /// returns an error.
    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn update_flat_head(&self, new_head: &CryptoHash) -> Result<(), FlatStorageError> {
        let mut guard = self.0.write().expect(POISONED_LOCK_ERR);
        let blocks = guard.get_blocks_to_head(new_head)?;
        for block in blocks.into_iter().rev() {
            let mut store_update = StoreUpdate::new(guard.store.storage.clone());
            let delta = guard.get_delta(&block)?.as_ref().clone();
            delta.apply_to_flat_state(&mut store_update);
            store_helper::set_flat_head(&mut store_update, guard.shard_id, &block);

            // Remove old blocks and deltas from disk and memory.
            // Do it for each head update separately to ensure that old data is removed properly if node was
            // interrupted in the middle.
            // TODO (#7327): in case of long forks it can take a while and delay processing of some chunk.
            // Consider avoid iterating over all blocks and make removals lazy.
            let gc_height = guard
                .blocks
                .get(&block)
                .ok_or(guard.create_block_not_supported_error(&block))?
                .height;
            let hashes_to_remove: Vec<_> = guard
                .blocks
                .iter()
                .filter(|(_, block_info)| block_info.height <= gc_height)
                .map(|(block_hash, _)| block_hash)
                .cloned()
                .collect();
            for hash in hashes_to_remove {
                // It is fine to remove all deltas in single store update, because memory overhead of `DeleteRange`
                // operation is low.
                store_helper::remove_delta(&mut store_update, guard.shard_id, hash);
                match guard.deltas.remove(&hash) {
                    Some(delta) => {
                        guard.metrics.cached_deltas.dec();
                        guard.metrics.cached_deltas_num_items.sub(delta.len() as i64);
                        guard.metrics.cached_deltas_size.sub(delta.total_size() as i64);
                    }
                    None => {}
                }

                // Note that we need to keep block info for new flat storage head to know its height.
                if &hash != new_head {
                    match guard.blocks.remove(&hash) {
                        Some(_) => {
                            guard.metrics.cached_blocks.dec();
                        }
                        None => {}
                    }
                }
            }

            store_update.commit().unwrap();
        }

        let shard_id = guard.shard_id;
        guard.flat_head = *new_head;
        let flat_head_height = guard
            .blocks
            .get(&new_head)
            .ok_or(guard.create_block_not_supported_error(&new_head))?
            .height;
        guard.metrics.flat_head_height.set(flat_head_height as i64);
        info!(target: "chain", %shard_id, %new_head, %flat_head_height, "Moved flat storage head");

        Ok(())
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    pub fn update_flat_head(&self, _new_head: &CryptoHash) -> Result<(), FlatStorageError> {
        Ok(())
    }

    /// Adds a block (including the block delta and block info) to flat storage,
    /// returns a StoreUpdate to store the delta on disk. Node that this StoreUpdate should be
    /// committed to disk in one db transaction together with the rest of changes caused by block,
    /// in case the node stopped or crashed in between and a block is on chain but its delta is not
    /// stored or vice versa.
    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn add_block(
        &self,
        block_hash: &CryptoHash,
        delta: FlatStateDelta,
        block: BlockInfo,
    ) -> Result<StoreUpdate, FlatStorageError> {
        let mut guard = self.0.write().expect(POISONED_LOCK_ERR);
        let shard_id = guard.shard_id;
        let block_height = block.height;
        info!(target: "chain", %shard_id, %block_hash, %block_height, "Adding block to flat storage");
        if !guard.blocks.contains_key(&block.prev_hash) {
            return Err(guard.create_block_not_supported_error(block_hash));
        }
        let mut store_update = StoreUpdate::new(guard.store.storage.clone());
        store_helper::set_delta(&mut store_update, guard.shard_id, block_hash.clone(), &delta)?;
        guard.metrics.cached_deltas.inc();
        guard.metrics.cached_deltas_num_items.add(delta.len() as i64);
        guard.metrics.cached_deltas_size.add(delta.total_size() as i64);
        guard.deltas.insert(*block_hash, Arc::new(delta));
        guard.blocks.insert(*block_hash, block);
        guard.metrics.cached_blocks.inc();
        Ok(store_update)
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    pub fn add_block(
        &self,
        _block_hash: &CryptoHash,
        _delta: FlatStateDelta,
        _block_info: BlockInfo,
    ) -> Result<StoreUpdate, FlatStorageError> {
        panic!("not implemented")
    }

    /// Clears all State key-value pairs from flat storage.
    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn clear_state(&self, shard_layout: ShardLayout) -> Result<(), StorageError> {
        let guard = self.0.write().expect(POISONED_LOCK_ERR);
        let shard_id = guard.shard_id;

        // Removes all items belonging to the shard one by one.
        // Note that it does not work for resharding.
        // TODO (#7327): call it just after we stopped tracking a shard.
        // TODO (#7327): remove FlatStateDeltas. Consider custom serialization of keys to remove them by
        // prefix.
        // TODO (#7327): support range deletions which are much faster than naive deletions. For that, we
        // can delete ranges of keys like
        // [ [0]+boundary_accounts(shard_id) .. [0]+boundary_accounts(shard_id+1) ), etc.
        // We should also take fixed accounts into account.
        let mut store_update = guard.store.store_update();
        let mut removed_items = 0;
        for item in guard.store.iter(crate::DBCol::FlatState) {
            let (key, _) =
                item.map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?;

            if store_helper::key_belongs_to_shard(&key, &shard_layout, shard_id)? {
                removed_items += 1;
                store_update.delete(crate::DBCol::FlatState, &key);
            }
        }
        info!(target: "chain", %shard_id, %removed_items, "Removing old items from flat storage");

        store_helper::remove_flat_head(&mut store_update, shard_id);
        store_update.commit().map_err(|_| StorageError::StorageInternalError)?;
        Ok(())
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    pub fn clear_state(&self, _shard_layout: ShardLayout) {}
}

#[cfg(test)]
#[cfg(feature = "protocol_feature_flat_state")]
mod tests {
    use crate::flat_state::{
        store_helper, BlockInfo, ChainAccessForFlatStorage, FlatStateFactory, FlatStorageError,
        FlatStorageState,
    };
    use crate::test_utils::create_test_store;
    use crate::FlatStateDelta;
    use crate::StorageError;
    use borsh::BorshSerialize;
    use near_primitives::borsh::maybestd::collections::HashSet;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::state::ValueRef;
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::{
        BlockHeight, RawStateChange, RawStateChangesWithTrieKey, StateChangeCause,
    };

    use assert_matches::assert_matches;
    use std::collections::HashMap;

    struct MockChain {
        height_to_hashes: HashMap<BlockHeight, CryptoHash>,
        blocks: HashMap<CryptoHash, BlockInfo>,
        head_height: BlockHeight,
    }

    impl ChainAccessForFlatStorage for MockChain {
        fn get_block_info(&self, block_hash: &CryptoHash) -> BlockInfo {
            self.blocks.get(block_hash).unwrap().clone()
        }

        fn get_block_hashes_at_height(&self, block_height: BlockHeight) -> HashSet<CryptoHash> {
            self.height_to_hashes.get(&block_height).cloned().iter().cloned().collect()
        }
    }

    impl MockChain {
        fn block_hash(height: BlockHeight) -> CryptoHash {
            hash(&height.try_to_vec().unwrap())
        }

        /// Build a chain with given set of heights and a function mapping block heights to heights of their parents.
        fn build(
            heights: Vec<BlockHeight>,
            get_parent: fn(BlockHeight) -> Option<BlockHeight>,
        ) -> MockChain {
            let height_to_hashes: HashMap<_, _> = heights
                .iter()
                .cloned()
                .map(|height| (height, MockChain::block_hash(height)))
                .collect();
            let blocks = heights
                .iter()
                .cloned()
                .map(|height| {
                    let hash = height_to_hashes.get(&height).unwrap().clone();
                    let prev_hash = match get_parent(height) {
                        None => CryptoHash::default(),
                        Some(parent_height) => *height_to_hashes.get(&parent_height).unwrap(),
                    };
                    (hash, BlockInfo { hash, height, prev_hash })
                })
                .collect();
            MockChain { height_to_hashes, blocks, head_height: heights.last().unwrap().clone() }
        }

        // Create a chain with no forks with length n.
        fn linear_chain(n: usize) -> MockChain {
            Self::build(
                (0..n as BlockHeight).collect(),
                |i| if i == 0 { None } else { Some(i - 1) },
            )
        }

        // Create a linear chain of length n where blocks with odd numbers are skipped:
        // 0 -> 2 -> 4 -> ...
        fn linear_chain_with_skips(n: usize) -> MockChain {
            Self::build((0..n as BlockHeight).map(|i| i * 2).collect(), |i| {
                if i == 0 {
                    None
                } else {
                    Some(i - 2)
                }
            })
        }

        // Create a chain with two forks, where blocks 1 and 2 have a parent block 0, and each next block H
        // has a parent block H-2:
        // 0 |-> 1 -> 3 -> 5 -> ...
        //   --> 2 -> 4 -> 6 -> ...
        fn chain_with_two_forks(n: usize) -> MockChain {
            Self::build((0..n as BlockHeight).collect(), |i| {
                if i == 0 {
                    None
                } else {
                    Some(i.max(2) - 2)
                }
            })
        }

        fn get_block_hash(&self, height: BlockHeight) -> CryptoHash {
            *self.height_to_hashes.get(&height).unwrap()
        }

        /// create a new block on top the current chain head, return the new block hash
        fn create_block(&mut self) -> CryptoHash {
            let hash = MockChain::block_hash(self.head_height + 1);
            self.height_to_hashes.insert(self.head_height + 1, hash);
            self.blocks.insert(
                hash,
                BlockInfo {
                    hash,
                    height: self.head_height + 1,
                    prev_hash: self.get_block_hash(self.head_height),
                },
            );
            self.head_height += 1;
            hash
        }
    }

    /// Check correctness of creating `FlatStateDelta` from state changes.
    #[test]
    fn flat_state_delta_creation() {
        let alice_trie_key = TrieKey::ContractCode { account_id: "alice".parse().unwrap() };
        let bob_trie_key = TrieKey::ContractCode { account_id: "bob".parse().unwrap() };
        let carol_trie_key = TrieKey::ContractCode { account_id: "carol".parse().unwrap() };

        let state_changes = vec![
            RawStateChangesWithTrieKey {
                trie_key: alice_trie_key.clone(),
                changes: vec![
                    RawStateChange {
                        cause: StateChangeCause::InitialState,
                        data: Some(vec![1, 2]),
                    },
                    RawStateChange {
                        cause: StateChangeCause::ReceiptProcessing {
                            receipt_hash: Default::default(),
                        },
                        data: Some(vec![3, 4]),
                    },
                ],
            },
            RawStateChangesWithTrieKey {
                trie_key: bob_trie_key.clone(),
                changes: vec![
                    RawStateChange {
                        cause: StateChangeCause::InitialState,
                        data: Some(vec![5, 6]),
                    },
                    RawStateChange {
                        cause: StateChangeCause::ReceiptProcessing {
                            receipt_hash: Default::default(),
                        },
                        data: None,
                    },
                ],
            },
        ];

        let flat_state_delta = FlatStateDelta::from_state_changes(&state_changes);
        assert_eq!(
            flat_state_delta.get(&alice_trie_key.to_vec()),
            Some(Some(ValueRef::new(&[3, 4])))
        );
        assert_eq!(flat_state_delta.get(&bob_trie_key.to_vec()), Some(None));
        assert_eq!(flat_state_delta.get(&carol_trie_key.to_vec()), None);
    }

    /// Check that keys related to delayed receipts are not included to `FlatStateDelta`.
    #[test]
    fn flat_state_delta_delayed_keys() {
        let delayed_trie_key = TrieKey::DelayedReceiptIndices;
        let delayed_receipt_trie_key = TrieKey::DelayedReceipt { index: 1 };

        let state_changes = vec![
            RawStateChangesWithTrieKey {
                trie_key: delayed_trie_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: Some(vec![1]),
                }],
            },
            RawStateChangesWithTrieKey {
                trie_key: delayed_receipt_trie_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: Some(vec![2]),
                }],
            },
        ];

        let flat_state_delta = FlatStateDelta::from_state_changes(&state_changes);
        assert!(flat_state_delta.get(&delayed_trie_key.to_vec()).is_none());
        assert!(flat_state_delta.get(&delayed_receipt_trie_key.to_vec()).is_none());
    }

    /// Check that merge of `FlatStateDelta`s overrides the old changes for the same keys and doesn't conflict with
    /// different keys.
    #[test]
    fn flat_state_delta_merge() {
        let mut delta = FlatStateDelta::from([
            (vec![1], Some(ValueRef::new(&[4]))),
            (vec![2], Some(ValueRef::new(&[5]))),
            (vec![3], None),
            (vec![4], Some(ValueRef::new(&[6]))),
        ]);
        let delta_new = FlatStateDelta::from([
            (vec![2], Some(ValueRef::new(&[7]))),
            (vec![3], Some(ValueRef::new(&[8]))),
            (vec![4], None),
            (vec![5], Some(ValueRef::new(&[9]))),
        ]);
        delta.merge(&delta_new);

        assert_eq!(delta.get(&[1]), Some(Some(ValueRef::new(&[4]))));
        assert_eq!(delta.get(&[2]), Some(Some(ValueRef::new(&[7]))));
        assert_eq!(delta.get(&[3]), Some(Some(ValueRef::new(&[8]))));
        assert_eq!(delta.get(&[4]), Some(None));
        assert_eq!(delta.get(&[5]), Some(Some(ValueRef::new(&[9]))));
    }

    #[test]
    fn block_not_supported_errors() {
        // Create a chain with two forks. Set flat head to be at block 0.
        let chain = MockChain::chain_with_two_forks(5);
        let store = create_test_store();
        let mut store_update = store.store_update();
        store_helper::set_flat_head(&mut store_update, 0, &chain.get_block_hash(0));
        for i in 1..5 {
            store_helper::set_delta(
                &mut store_update,
                0,
                chain.get_block_hash(i),
                &FlatStateDelta::default(),
            )
            .unwrap();
        }
        store_update.commit().unwrap();

        let flat_storage_state = FlatStorageState::new(store.clone(), 0, 4, &chain);
        let flat_state_factory = FlatStateFactory::new(store.clone());
        flat_state_factory.add_flat_storage_state_for_shard(0, flat_storage_state);
        let flat_storage_state = flat_state_factory.get_flat_storage_state_for_shard(0).unwrap();

        // Check that flat head can be moved to block 1.
        let flat_head_hash = chain.get_block_hash(1);
        assert_eq!(flat_storage_state.update_flat_head(&flat_head_hash), Ok(()));
        // Check that attempt to move flat head to block 2 results in error because it lays in unreachable fork.
        let fork_block_hash = chain.get_block_hash(2);
        assert_eq!(
            flat_storage_state.update_flat_head(&fork_block_hash),
            Err(FlatStorageError::BlockNotSupported((flat_head_hash, fork_block_hash)))
        );
        // Check that attempt to move flat head to block 0 results in error because it is an unreachable parent.
        let parent_block_hash = chain.get_block_hash(0);
        assert_eq!(
            flat_storage_state.update_flat_head(&parent_block_hash),
            Err(FlatStorageError::BlockNotSupported((flat_head_hash, parent_block_hash)))
        );
        // Check that attempt to move flat head to non-existent block results in the same error.
        let not_existing_hash = hash(&[1, 2, 3]);
        assert_eq!(
            flat_storage_state.update_flat_head(&not_existing_hash),
            Err(FlatStorageError::BlockNotSupported((flat_head_hash, not_existing_hash)))
        );
    }

    #[test]
    fn skipped_heights() {
        // Create a linear chain where some heights are skipped.
        let chain = MockChain::linear_chain_with_skips(5);
        let store = create_test_store();
        let mut store_update = store.store_update();
        store_helper::set_flat_head(&mut store_update, 0, &chain.get_block_hash(0));
        for i in 1..5 {
            store_helper::set_delta(
                &mut store_update,
                0,
                chain.get_block_hash(i * 2),
                &FlatStateDelta::default(),
            )
            .unwrap();
        }
        store_update.commit().unwrap();

        // Check that flat storage state is created correctly for chain which has skipped heights.
        let flat_storage_state = FlatStorageState::new(store.clone(), 0, 8, &chain);
        let flat_state_factory = FlatStateFactory::new(store.clone());
        flat_state_factory.add_flat_storage_state_for_shard(0, flat_storage_state);
        let flat_storage_state = flat_state_factory.get_flat_storage_state_for_shard(0).unwrap();

        // Check that flat head can be moved to block 8.
        let flat_head_hash = chain.get_block_hash(8);
        assert_eq!(flat_storage_state.update_flat_head(&flat_head_hash), Ok(()));
    }

    // This test tests basic use cases for FlatState and FlatStorageState.
    // We created a linear chain with no forks, start with flat head at the genesis block, then
    // moves the flat head forward, which checking that flat_state.get_ref() still returns the correct
    // values and the state is being updated in store.
    #[test]
    fn flat_storage_state_sanity() {
        // 1. Create a chain with 10 blocks with no forks. Set flat head to be at block 0.
        //    Block i sets value for key &[1] to &[i].
        let mut chain = MockChain::linear_chain(10);
        let store = create_test_store();
        let mut store_update = store.store_update();
        store_helper::set_flat_head(&mut store_update, 0, &chain.get_block_hash(0));
        store_helper::set_ref(&mut store_update, vec![1], Some(ValueRef::new(&[0]))).unwrap();
        for i in 1..10 {
            store_helper::set_delta(
                &mut store_update,
                0,
                chain.get_block_hash(i),
                &FlatStateDelta::from([(vec![1], Some(ValueRef::new(&[i as u8])))]),
            )
            .unwrap();
        }
        store_update.commit().unwrap();

        let flat_storage_state = FlatStorageState::new(store.clone(), 0, 9, &chain);
        let flat_state_factory = FlatStateFactory::new(store.clone());
        flat_state_factory.add_flat_storage_state_for_shard(0, flat_storage_state);
        let flat_storage_state = flat_state_factory.get_flat_storage_state_for_shard(0).unwrap();

        // 2. Check that the flat_state at block i reads the value of key &[1] as &[i]
        for i in 0..10 {
            let block_hash = chain.get_block_hash(i);
            let blocks = flat_storage_state.get_blocks_to_head(&block_hash).unwrap();
            assert_eq!(blocks.len(), i as usize);
            let flat_state =
                flat_state_factory.new_flat_state_for_shard(0, Some(block_hash), false).unwrap();
            assert_eq!(flat_state.get_ref(&[1]).unwrap(), Some(ValueRef::new(&[i as u8])));
        }

        // 3. Create a new block that deletes &[1] and add a new value &[2]
        //    Add the block to flat storage.
        let hash = chain.create_block();
        let store_update = flat_storage_state
            .add_block(
                &hash,
                FlatStateDelta::from([(vec![1], None), (vec![2], Some(ValueRef::new(&[1])))]),
                chain.get_block_info(&hash),
            )
            .unwrap();
        store_update.commit().unwrap();

        // 4. Create a flat_state0 at block 10 and flat_state1 at block 4
        //    Verify that they return the correct values
        let blocks = flat_storage_state.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
        assert_eq!(blocks.len(), 10);
        let flat_state0 = flat_state_factory
            .new_flat_state_for_shard(0, Some(chain.get_block_hash(10)), false)
            .unwrap();
        let flat_state1 = flat_state_factory
            .new_flat_state_for_shard(0, Some(chain.get_block_hash(4)), false)
            .unwrap();
        assert_eq!(flat_state0.get_ref(&[1]).unwrap(), None);
        assert_eq!(flat_state0.get_ref(&[2]).unwrap(), Some(ValueRef::new(&[1])));
        assert_eq!(flat_state1.get_ref(&[1]).unwrap(), Some(ValueRef::new(&[4])));
        assert_eq!(flat_state1.get_ref(&[2]).unwrap(), None);
        assert_matches!(
            store_helper::get_delta(&store, 0, chain.get_block_hash(5)).unwrap(),
            Some(_)
        );
        assert_matches!(
            store_helper::get_delta(&store, 0, chain.get_block_hash(10)).unwrap(),
            Some(_)
        );

        // 5. Move the flat head to block 5, verify that flat_state0 still returns the same values
        // and flat_state1 returns an error. Also check that DBCol::FlatState is updated correctly
        flat_storage_state.update_flat_head(&chain.get_block_hash(5)).unwrap();
        assert_eq!(store_helper::get_ref(&store, &[1]).unwrap(), Some(ValueRef::new(&[5])));
        let blocks = flat_storage_state.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
        assert_eq!(blocks.len(), 5);
        assert_eq!(flat_state0.get_ref(&[1]).unwrap(), None);
        assert_eq!(flat_state0.get_ref(&[2]).unwrap(), Some(ValueRef::new(&[1])));
        assert_matches!(flat_state1.get_ref(&[1]), Err(StorageError::FlatStorageError(_)));
        assert_matches!(store_helper::get_delta(&store, 0, chain.get_block_hash(5)).unwrap(), None);
        assert_matches!(
            store_helper::get_delta(&store, 0, chain.get_block_hash(10)).unwrap(),
            Some(_)
        );

        // 6. Move the flat head to block 10, verify that flat_state0 still returns the same values
        //    Also checks that DBCol::FlatState is updated correctly.
        flat_storage_state.update_flat_head(&chain.get_block_hash(10)).unwrap();
        let blocks = flat_storage_state.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
        assert_eq!(blocks.len(), 0);
        assert_eq!(store_helper::get_ref(&store, &[1]).unwrap(), None);
        assert_eq!(store_helper::get_ref(&store, &[2]).unwrap(), Some(ValueRef::new(&[1])));
        assert_eq!(flat_state0.get_ref(&[1]).unwrap(), None);
        assert_eq!(flat_state0.get_ref(&[2]).unwrap(), Some(ValueRef::new(&[1])));
        assert_matches!(
            store_helper::get_delta(&store, 0, chain.get_block_hash(10)).unwrap(),
            None
        );
    }
}
