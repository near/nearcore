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
#[cfg(feature = "protocol_feature_flat_state")]
const BORSH_ERR: &str = "Borsh cannot fail";

#[cfg(feature = "protocol_feature_flat_state")]
mod imp {
    use crate::flat_state::{FlatStorageState, POISONED_LOCK_ERR};
    use near_primitives::errors::StorageError;
    use near_primitives::hash::CryptoHash;
    use near_primitives::state::ValueRef;
    use near_primitives::types::ShardId;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::Store;

    /// Struct for getting value references from the flat storage.
    ///
    /// The main interface is the `get_ref` method, which is called in `Trie::get`
    /// `Trie::get_ref`
    /// because they are the same for each shard and they are requested only
    /// once during applying chunk.
    // TODO (#7327): lock flat state when `get_ref` is called or head is being updated. Otherwise, `apply_chunks` and
    // `postprocess_block` parallel execution may corrupt the state.
    #[derive(Clone)]
    pub struct FlatState {
        /// Used to access flat state stored at the head of flat storage.
        /// It should store all trie keys and values/value refs for the state on top of
        /// flat_storage_state.head, except for delayed receipt keys.
        store: Store,
        /// The block for which key-value pairs of its state will be retrieved.
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
    struct FlatStateCache {
        // TODO: add implementation
    }

    impl FlatState {
        /// Returns value reference using raw trie key, taken from the state
        /// corresponding to `FlatState::block_hash`.
        ///
        /// To avoid duplication, we don't store values themselves in flat state,
        /// they are stored in `DBCol::State`. Also the separation is done so we
        /// could charge users for the value length before loading the value.
        // TODO (#7327): support different roots (or block hashes).
        // TODO (#7327): consider inlining small values, so we could use only one db access.
        pub fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, StorageError> {
            // Take deltas ordered from `self.block_hash` to flat state head.
            // In other words, order of deltas is the opposite of the order of blocks in chain.
            let deltas = self.flat_storage_state.get_deltas_between_blocks(&self.block_hash)?;
            for delta in deltas {
                // If we found a key in delta, we can return a value because it is the most recent key update.
                match delta.get(key) {
                    Some(value_ref) => {
                        return Ok(value_ref);
                    }
                    None => {}
                };
            }

            let raw_ref = self
                .store
                .get(crate::DBCol::FlatState, key)
                .map_err(|_| StorageError::StorageInternalError);
            match raw_ref? {
                Some(bytes) => ValueRef::decode(&bytes)
                    .map(Some)
                    .map_err(|_| StorageError::StorageInternalError),
                None => Ok(None),
            }
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

        /// Creates `FlatState` for accessing flat storage data for particular shard and the given block.
        /// If flat state feature was not enabled (see parallel implementation below), request was made from view client
        /// or block was not provided, returns None.
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
                    // We unwrap here because flat storage state for this shard should already be
                    // added. If not, it is a bug in our code and we can't keep processing blocks
                    flat_storage_states
                        .get(&shard_id)
                        .unwrap_or_else(|| {
                            panic!("FlatStorageState for shard {} is not ready", shard_id)
                        })
                        .clone()
                };
                Some(FlatState {
                    store: self.0.store.clone(),
                    block_hash,
                    cache,
                    flat_storage_state,
                })
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
    }
}

#[cfg(not(feature = "protocol_feature_flat_state"))]
mod imp {
    use crate::flat_state::FlatStorageState;
    use crate::Store;
    use near_primitives::hash::CryptoHash;
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
    }
}

use borsh::{BorshDeserialize, BorshSerialize};

use crate::{CryptoHash, Store, StoreUpdate};
pub use imp::{FlatState, FlatStateFactory};
use near_primitives::state::ValueRef;
use near_primitives::types::{BlockHeight, RawStateChangesWithTrieKey, ShardId};
use std::collections::HashMap;
#[cfg(feature = "protocol_feature_flat_state")]
use std::collections::HashSet;

#[derive(BorshSerialize, BorshDeserialize)]
pub struct KeyForFlatStateDelta {
    pub shard_id: ShardId,
    pub block_hash: CryptoHash,
}

/// Delta of the state for some shard and block, stores mapping from keys to value refs or None, if key was removed in
/// this block.
#[derive(BorshSerialize, BorshDeserialize)]
pub struct FlatStateDelta(pub HashMap<Vec<u8>, Option<ValueRef>>);

impl FlatStateDelta {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Returns `Some(Option<ValueRef>)` from delta for the given key. If key is not present, returns None.
    pub fn get(&self, key: &[u8]) -> Option<Option<ValueRef>> {
        self.0.get(key).cloned()
    }

    /// Merge two deltas. Values from `other` should override values from `self`.
    pub fn merge(&mut self, other: Self) {
        self.0.extend(other.0)
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
            match value {
                Some(value) => {
                    store_update
                        .set_ser(crate::DBCol::FlatState, &key, &value)
                        .expect("Borsh cannot fail");
                }
                None => {
                    store_update.delete(crate::DBCol::FlatState, &key);
                }
            }
        }
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    pub fn apply_to_flat_state(self, _store_update: &mut StoreUpdate) {}
}

#[cfg(feature = "protocol_feature_flat_state")]
use near_primitives::block_header::BlockHeader;
use std::sync::{Arc, RwLock};

/// FlatStorageState stores information on which blocks flat storage current supports key lookups on.
/// Note that this struct is shared by multiple threads, the chain thread, threads that apply chunks,
/// and view client, so the implementation here must be thread safe and must have interior mutability,
/// thus all methods in this class are with &self intead of &mut self.
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

struct FlatStorageStateInner {
    #[allow(unused)]
    store: Store,
    /// Id of the shard which state is accessed by this flat storage.
    #[allow(unused)]
    shard_id: ShardId,
    /// The block for which we store the key value pairs of its state. For non catchup mode,
    /// it should be the last final block.
    #[allow(unused)]
    flat_head: CryptoHash,
    /// Stores some information for all blocks supported by flat storage, this is used for finding
    /// paths between the root block and a target block
    #[allow(unused)]
    blocks: HashMap<CryptoHash, BlockInfo>,
    /// State deltas for all blocks supported by this flat storage.
    /// All these deltas here are stored on disk too.
    #[allow(unused)]
    deltas: HashMap<CryptoHash, FlatStateDelta>,
}

#[cfg(feature = "protocol_feature_flat_state")]
pub mod store_helper {
    use crate::flat_state::KeyForFlatStateDelta;
    use crate::{FlatStateDelta, Store, StoreUpdate};
    use borsh::BorshSerialize;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::ShardId;

    pub(crate) fn get_delta(
        store: &Store,
        shard_id: ShardId,
        block_hash: CryptoHash,
    ) -> Result<Option<FlatStateDelta>, crate::StorageError> {
        let key = KeyForFlatStateDelta { shard_id, block_hash };
        store
            .get_ser(crate::DBCol::FlatStateDeltas, &key.try_to_vec().unwrap())
            .map_err(|_| crate::StorageError::StorageInternalError)
    }

    pub fn set_delta(
        store_update: &mut StoreUpdate,
        shard_id: ShardId,
        block_hash: CryptoHash,
        delta: &FlatStateDelta,
    ) -> Result<(), crate::StorageError> {
        let key = KeyForFlatStateDelta { shard_id, block_hash };
        store_update
            .set_ser(crate::DBCol::FlatStateDeltas, &key.try_to_vec().unwrap(), delta)
            .map_err(|_| crate::StorageError::StorageInternalError)
    }

    pub(crate) fn get_flat_head(store: &Store, shard_id: ShardId) -> CryptoHash {
        store
            .get_ser(crate::DBCol::FlatStateMisc, &shard_id.try_to_vec().unwrap())
            .expect("Error reading flat head from storage")
            .unwrap_or_else(|| panic!("Cannot read flat head for shard {} from storage", shard_id))
    }

    pub fn set_flat_head(store_update: &mut StoreUpdate, shard_id: ShardId, val: &CryptoHash) {
        store_update
            .set_ser(crate::DBCol::FlatStateMisc, &shard_id.try_to_vec().unwrap(), val)
            .expect("Error writing flat head from storage")
    }
}

impl FlatStorageState {
    /// Create a new FlatStorageState for `shard_id`.
    /// Flat head is initialized to be what is stored on storage.
    /// We also load all blocks with height between flat head to the current chain head,
    /// including those on forks into the returned FlatStorageState.
    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn new(
        store: Store,
        chain_head_height: BlockHeight,
        // Unfortunately we don't have access to ChainStore inside this file because of package
        // dependencies, so we pass these functions in to access chain info
        hash_to_header: &dyn Fn(&CryptoHash) -> BlockHeader,
        height_to_hashes: &dyn Fn(BlockHeight) -> HashSet<CryptoHash>,
        shard_id: ShardId,
    ) -> Self {
        let flat_head = store_helper::get_flat_head(&store, shard_id);
        let header = hash_to_header(&flat_head);
        let flat_head_height = header.height();
        let mut blocks = HashMap::from([(
            flat_head,
            BlockInfo { hash: flat_head, height: flat_head_height, prev_hash: *header.prev_hash() },
        )]);
        let mut deltas = HashMap::new();
        for height in flat_head_height + 1..=chain_head_height {
            for hash in height_to_hashes(height) {
                let header = hash_to_header(&hash);
                let prev_hash = *header.prev_hash();
                assert!(
                    blocks.contains_key(&prev_hash),
                    "Can't find a path from the current flat storage root {:?}@{} to block {:?}@{}",
                    flat_head,
                    flat_head_height,
                    hash,
                    header.height()
                );
                blocks.insert(hash, BlockInfo { hash, height: header.height(), prev_hash });
            }
        }
        for hash in blocks.keys() {
            deltas.insert(
                *hash,
                store_helper::get_delta(&store, shard_id, *hash).expect(BORSH_ERR).unwrap_or_else(
                    || panic!("Cannot find block delta for block {:?} shard {}", hash, shard_id),
                ),
            );
        }

        Self(Arc::new(RwLock::new(FlatStorageStateInner {
            store,
            shard_id,
            flat_head,
            blocks,
            deltas,
        })))
    }

    /// Get deltas for blocks, ordered from `self.block_hash` to flat state head (backwards chain order).
    /// If sequence of deltas contains final block, head is moved to this block and all deltas until new head are
    /// applied to flat state.
    // TODO (#7327): move updating flat state head to block postprocessing.
    // TODO (#7327): come up how the flat state head and tail should be positioned.
    // TODO (#7327): implement garbage collection of old deltas.
    // TODO (#7327): cache deltas to speed up multiple DB reads.
    #[cfg(feature = "protocol_feature_flat_state")]
    fn get_deltas_between_blocks(
        &self,
        target_block_hash: &CryptoHash,
    ) -> Result<Vec<FlatStateDelta>, crate::StorageError> {
        let guard = self.0.write().expect(POISONED_LOCK_ERR);
        let flat_state_head = store_helper::get_flat_head(&guard.store, guard.shard_id);

        let block_header: BlockHeader = guard
            .store
            .get_ser(crate::DBCol::BlockHeader, target_block_hash.as_ref())
            .map_err(|_| crate::StorageError::StorageInternalError)?
            .unwrap();
        let final_block_hash = block_header.last_final_block().clone();

        let mut block_hash = target_block_hash.clone();
        let mut deltas = vec![];
        let mut deltas_to_apply = vec![];
        let mut found_final_block = false;
        while block_hash != flat_state_head {
            if block_hash == final_block_hash {
                assert!(!found_final_block);
                found_final_block = true;
            }

            let delta = store_helper::get_delta(&guard.store, guard.shard_id, block_hash)?;
            match delta {
                Some(delta) => {
                    if found_final_block {
                        deltas_to_apply.push(delta);
                    } else {
                        deltas.push(delta);
                    }
                }
                None => {}
            }

            let block_header: BlockHeader = guard
                .store
                .get_ser(crate::DBCol::BlockHeader, block_hash.as_ref())
                .map_err(|_| crate::StorageError::StorageInternalError)?
                .unwrap();
            block_hash = block_header.prev_hash().clone();
        }

        let storage = guard.store.storage.clone();
        std::mem::drop(guard);

        if found_final_block {
            let mut store_update = StoreUpdate::new(storage);
            let mut delta = FlatStateDelta::new();
            for new_delta in deltas_to_apply.drain(..).rev() {
                delta.merge(new_delta);
            }
            delta.apply_to_flat_state(&mut store_update);
            match self.update_flat_head(&final_block_hash) {
                Some(new_store_update) => {
                    store_update.merge(new_store_update);
                }
                None => {}
            };
            store_update.commit().map_err(|_| crate::StorageError::StorageInternalError)?
        }
        Ok(deltas)
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    #[allow(unused)]
    fn get_deltas_between_blocks(
        &self,
        _target_block_hash: &CryptoHash,
    ) -> Result<Vec<FlatStateDelta>, crate::StorageError> {
        Ok(vec![])
    }

    // Update the head of the flat storage, this might require updating the flat state stored on disk.
    // Returns a StoreUpdate for the disk update if there is any
    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn update_flat_head(&self, new_head: &CryptoHash) -> Option<StoreUpdate> {
        let guard = self.0.write().expect(POISONED_LOCK_ERR);
        let mut store_update = StoreUpdate::new(guard.store.storage.clone());
        store_helper::set_flat_head(&mut store_update, guard.shard_id, new_head);
        Some(store_update)
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    pub fn update_flat_head(&self, _new_head: &CryptoHash) -> Option<StoreUpdate> {
        None
    }

    /// Adds a delta for a block to flat storage, returns a StoreUpdate.
    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn add_delta(
        &self,
        block_hash: &CryptoHash,
        delta: FlatStateDelta,
    ) -> Result<StoreUpdate, crate::StorageError> {
        let guard = self.0.write().expect(POISONED_LOCK_ERR);
        let mut store_update = StoreUpdate::new(guard.store.storage.clone());
        store_helper::set_delta(&mut store_update, guard.shard_id, block_hash.clone(), &delta)?;
        Ok(store_update)
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    pub fn add_delta(
        &self,
        _block_hash: &CryptoHash,
        _delta: FlatStateDelta,
    ) -> Result<StoreUpdate, crate::StorageError> {
        panic!("not implemented")
    }
}

#[cfg(test)]
mod tests {
    use crate::FlatStateDelta;
    use near_primitives::state::ValueRef;
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::{RawStateChange, RawStateChangesWithTrieKey, StateChangeCause};
    use std::collections::HashMap;

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
        let mut delta = FlatStateDelta(HashMap::from([
            (vec![1], Some(ValueRef::new(&[4]))),
            (vec![2], Some(ValueRef::new(&[5]))),
            (vec![3], None),
            (vec![4], Some(ValueRef::new(&[6]))),
        ]));
        let delta_new = FlatStateDelta(HashMap::from([
            (vec![2], Some(ValueRef::new(&[7]))),
            (vec![3], Some(ValueRef::new(&[8]))),
            (vec![4], None),
            (vec![5], Some(ValueRef::new(&[9]))),
        ]));
        delta.merge(delta_new);

        assert_eq!(delta.get(&[1]), Some(Some(ValueRef::new(&[4]))));
        assert_eq!(delta.get(&[2]), Some(Some(ValueRef::new(&[7]))));
        assert_eq!(delta.get(&[3]), Some(Some(ValueRef::new(&[8]))));
        assert_eq!(delta.get(&[4]), Some(None));
        assert_eq!(delta.get(&[5]), Some(Some(ValueRef::new(&[9]))));
    }

    #[test]
    fn flat_state_apply_single_delta() {
        // TODO (#7327): check this scenario after implementing flat storage state:
        // 1) create FlatState for one shard with FlatStorage
        // 2) create FlatStateDelta and apply it
        // 3) check that FlatStorageState contains the right values
    }

    #[test]
    fn flat_state_apply_delta_range() {
        // TODO (#7327): check this scenario after implementing flat storage state:
        // 1) add tree of blocks and FlatStateDeltas for them
        // 2) call `get_deltas_between_blocks` and check its correctness
        // 3) apply deltas and check that FlatStorageState contains the right values;
        // e.g. for the same key only the latest value is applied
    }
}
