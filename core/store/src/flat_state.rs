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

#[cfg(feature = "protocol_feature_flat_state")]
mod imp {
    use crate::flat_state::FlatStorageState;
    use near_primitives::errors::StorageError;
    use near_primitives::hash::CryptoHash;
    use near_primitives::state::ValueRef;
    use near_primitives::types::ShardId;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::flat_state::KeyForFlatStateDelta;
    use crate::{DBCol, FlatStateDelta, Store, StoreUpdate};
    use borsh::BorshSerialize;
    use near_primitives::block_header::BlockHeader;

    const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

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
        /// Id of the shard which state is accessed by this object.
        shard_id: ShardId,
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
        /// Update the head of the flat storage. Return a StoreUpdate for the disk update.
        pub fn update_head(
            shard_id: ShardId,
            block_hash: &CryptoHash,
            store: &Store,
        ) -> StoreUpdate {
            let mut store_update = StoreUpdate::new(store.storage.clone());
            store_update
                .set_ser(DBCol::FlatStateMisc, &shard_id.try_to_vec().unwrap(), block_hash)
                .expect("Borsh cannot fail");
            store_update
        }

        /// Get deltas for blocks, ordered from `self.block_hash` to flat state head (backwards chain order).
        /// If sequence of deltas contains final block, head is moved to this block and all deltas until new head are
        /// applied to flat state.
        // TODO (#7327): move updating flat state head to block postprocessing.
        // TODO (#7327): come up how the flat state head and tail should be positioned.
        // TODO (#7327): implement garbage collection of old deltas.
        // TODO (#7327): cache deltas to speed up multiple DB reads.
        fn get_deltas_between_blocks(&self) -> Result<Vec<FlatStateDelta>, StorageError> {
            let flat_state_head: CryptoHash = self
                .store
                .get_ser(DBCol::FlatStateMisc, &self.shard_id.try_to_vec().unwrap())
                .map_err(|_| StorageError::StorageInternalError)?
                .expect("Borsh cannot fail");

            let block_header: BlockHeader = self
                .store
                .get_ser(DBCol::BlockHeader, self.block_hash.as_ref())
                .map_err(|_| StorageError::StorageInternalError)?
                .unwrap();
            let final_block_hash = block_header.last_final_block().clone();

            let mut block_hash = self.block_hash.clone();
            let mut deltas = vec![];
            let mut deltas_to_apply = vec![];
            let mut found_final_block = false;
            while block_hash != flat_state_head {
                if block_hash == final_block_hash {
                    assert!(!found_final_block);
                    found_final_block = true;
                }

                let key = KeyForFlatStateDelta { shard_id: self.shard_id, block_hash };
                let delta: Option<FlatStateDelta> = self
                    .store
                    .get_ser(crate::DBCol::FlatStateDeltas, &key.try_to_vec().unwrap())
                    .map_err(|_| StorageError::StorageInternalError)?;
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

                let block_header: BlockHeader = self
                    .store
                    .get_ser(DBCol::BlockHeader, block_hash.as_ref())
                    .map_err(|_| StorageError::StorageInternalError)?
                    .unwrap();
                block_hash = block_header.prev_hash().clone();
            }

            if found_final_block {
                let mut store_update = StoreUpdate::new(self.store.storage.clone());
                for delta in deltas_to_apply.drain(..).rev() {
                    delta.apply_to_flat_state(&mut store_update);
                }
                store_update.merge(FlatState::update_head(
                    self.shard_id,
                    &final_block_hash,
                    &self.store,
                ));
                store_update.commit().map_err(|_| StorageError::StorageInternalError)?
            }
            Ok(deltas)
        }

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
            let deltas = self.get_deltas_between_blocks()?;
            for delta in deltas {
                // If we found a key in delta, we can return a value because it is the most recent key update.
                if delta.0.contains_key(key) {
                    return Ok(delta.0.get(key).unwrap().clone());
                }
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
                // TODO: Technically, like TrieCache, we should have a separate set of caches for Client and
                // ViewClient. Right now, we can get by by not enabling flat state for view trie
                None
            } else {
                let cache = {
                    let mut caches = self.0.caches.lock().expect(POISONED_LOCK_ERR);
                    caches.entry(shard_id).or_insert_with(|| FlatStateCache {}).clone()
                };
                // TODO: initialize flat storage states correctly, the current FlatStorageState function
                // doesn't take any argument
                let flat_storage_state = {
                    let mut flat_storage_states =
                        self.0.flat_storage_states.lock().expect(POISONED_LOCK_ERR);
                    flat_storage_states
                        .entry(shard_id)
                        .or_insert_with(|| FlatStorageState::new())
                        .clone()
                };
                Some(FlatState {
                    store: self.0.store.clone(),
                    shard_id,
                    block_hash,
                    cache,
                    flat_storage_state,
                })
            }
        }

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
    use crate::{Store, StoreUpdate};
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::ShardId;

    /// Since this has no variants it can never be instantiated.
    ///
    /// To use flat state enable `protocol_feature_flat_state` cargo feature.
    #[derive(Clone)]
    pub enum FlatState {}

    impl FlatState {
        pub fn update_head(
            _shard_id: ShardId,
            _block_hash: &CryptoHash,
            store: &Store,
        ) -> StoreUpdate {
            StoreUpdate::new(store.storage.clone())
        }

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
    }
}

use borsh::{BorshDeserialize, BorshSerialize};

use crate::{CryptoHash, StoreUpdate};
pub use imp::{FlatState, FlatStateFactory};
use near_primitives::state::ValueRef;
use near_primitives::types::{BlockHeight, RawStateChangesWithTrieKey, ShardId};
use std::collections::HashMap;

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

use std::sync::{Arc, RwLock};

pub enum FlatStateDeltaDir {
    Forward,
    Backward,
}

pub struct FlatStateDeltaWithDir {
    #[allow(unused)]
    delta: FlatStateDelta,
    #[allow(unused)]
    dir: FlatStateDeltaDir,
}

pub enum Error {}

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

struct FlatStorageStateInner {
    /// The block for which we store the key value pairs of its state. Note that it is not necessarily
    /// the latest block.
    #[allow(unused)]
    head: CryptoHash,
    /// This is the oldest block that whose state is supported by flat storage. In normal block
    /// processing mode, this should be the latest final block.
    #[allow(unused)]
    tail: CryptoHash,
    #[allow(unused)]
    tail_height: BlockHeight,
    /// A mapping from all blocks in flat storage to its previous block hash. This is needed because
    /// we don't have access to ChainStore inside this class (because ChainStore is not thread safe),
    /// so we store a representation of the chain formed by the blocks in flat storage.
    #[allow(unused)]
    prev_blocks: HashMap<CryptoHash, CryptoHash>,
    /// State deltas for all blocks supported by this flat storage.
    /// All these deltas here are stored on disk too.
    #[allow(unused)]
    deltas: HashMap<CryptoHash, FlatStateDelta>,
}

impl FlatStorageState {
    #[allow(unused)]
    fn new() -> Self {
        // TODO: implement this correctly
        Self(Arc::new(RwLock::new(FlatStorageStateInner {
            head: Default::default(),
            tail: Default::default(),
            tail_height: 0,
            prev_blocks: Default::default(),
            deltas: Default::default(),
        })))
    }

    #[allow(unused)]
    fn get_deltas_between_blocks(
        &self,
        _target_block_hash: &CryptoHash,
    ) -> Result<Vec<FlatStateDeltaWithDir>, Error> {
        // TODO: add implementation
        Ok(vec![])
    }

    // Update the head of the flat storage, this might require updating the flat state stored on disk.
    // Returns a StoreUpdate for the disk update if there is any
    #[allow(unused)]
    pub fn update_head(&self, _new_head: &CryptoHash) -> Option<StoreUpdate> {
        // TODO:
        None
    }

    // Update the tail of the flat storage, remove the deltas between the old tail and the new tail
    // both in memory and on disk
    // Return a StoreUpdate for the disk update if there is any
    #[allow(unused)]
    pub fn update_tail(&self, _new_tail: &CryptoHash) -> Option<StoreUpdate> {
        // TODO:
        None
    }

    // Add a block delta to flat storage
    #[allow(unused)]
    pub fn add_delta(&self, _block_hash: &CryptoHash, delta: FlatStateDelta) {
        // TODO:
    }
}
