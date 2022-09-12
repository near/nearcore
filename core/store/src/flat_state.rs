//! Contains flat storage optimization logic.
//!
//! FlatStorage is created as an additional representation of the state alongside with Tries.
//! It simply stores a mapping from all key value pairs stored in our Tries (leaves of the trie)
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
    use std::sync::RwLock;

    use crate::Store;

    const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

    /// Struct for getting value references from the flat storage.
    /// The main interface is the `get_ref` method, which is called in `Trie::get`
    /// `Trie::get_ref`
    /// because they are the same for each shard and they are requested only
    /// once during applying chunk.
    // only go forward.
    #[derive(Clone)]
    pub struct FlatState {
        /// Used to access flat state stored at the head of flat storage.
        /// It should store all trie keys and values/value refs for the state on top of
        /// flat_storage_state.head, except for delayed receipt keys,
        store: Store,
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
        fn get_raw_ref(&self, key: &[u8]) -> Result<Option<crate::db::DBSlice<'_>>, StorageError> {
            return self
                .store
                .get(crate::DBCol::FlatState, key)
                .map_err(|_| StorageError::StorageInternalError);
        }

        /// Returns value reference using raw trie key and state root.
        ///
        /// We assume that flat state contains data for this root.  To avoid
        /// duplication, we don't store values themselves in flat state, they
        /// are stored in `DBCol::State`. Also the separation is done so we
        /// could charge users for the value length before loading the value.
        // TODO (#7327): support different roots (or block hashes).
        // TODO (#7327): consider inlining small values, so we could use only one db access.
        pub fn get_ref(
            &self,
            _block_hash: &CryptoHash,
            key: &[u8],
        ) -> Result<Option<ValueRef>, StorageError> {
            // TODO: take deltas into account here
            match self.get_raw_ref(key)? {
                Some(bytes) => ValueRef::decode(&bytes)
                    .map(Some)
                    .map_err(|_| StorageError::StorageInternalError),
                None => Ok(None),
            }
        }
    }

    /// `FlatStateFactory` provides a way to construct new flat state to pass to new tries.
    pub struct FlatStateFactory {
        store: Store,
        caches: RwLock<HashMap<ShardId, FlatStateCache>>,
        /// Here we store the flat_storage_state per shard. The reason why we don't use the same
        /// FlatStorageState for all shards is that there are two modes of block processing,
        /// normal block processing and block catchups. Since these are performed on different range
        /// of blocks, we need flat storage to be able to support different range of blocks
        /// on different shards. So we simply store a different state for each shard.
        /// This may cause some overhead because the data like shards that the node is processing for
        /// this epoch can share the same `head` and `tail`, similar for shards for the next epoch,
        /// but such overhead is negligible comparing the delta sizes, so we think it's ok.
        flat_storage_states: HashMap<ShardId, FlatStorageState>,
    }

    impl FlatStateFactory {
        pub fn new(store: Store) -> Self {
            Self { store, caches: Default::default(), flat_storage_states: Default::default() }
        }

        /// Initialize all flat storage states for all shards
        /// This function should be called at node start up, to load deltas from disk and set up
        pub fn init(&self, _shards: Vec<ShardId>) {
            // TODO: implement it and call this function at the necessary place
        }

        pub fn new_flat_state_for_shard(
            &self,
            shard_id: ShardId,
            is_view: bool,
        ) -> Option<FlatState> {
            if is_view {
                // TODO: Technically, like TrieCache, we should have a separate set of caches for Client and
                // ViewClient. Right now, we can get by by not enabling flat state for view trie
                None
            } else {
                let cache = {
                    let mut caches = self.caches.write().expect(POISONED_LOCK_ERR);
                    caches.entry(shard_id).or_insert_with(|| FlatStateCache {}).clone()
                };
                Some(FlatState {
                    store: self.store.clone(),
                    cache,
                    // TODO: remove the unwrap here and do a proper error handling
                    flat_storage_state: self.flat_storage_states.get(&shard_id).unwrap().clone(),
                })
            }
        }

        pub fn get_flat_storage_state_for_shard(
            &self,
            shard_id: ShardId,
        ) -> Option<FlatStorageState> {
            self.flat_storage_states.get(&shard_id).cloned()
        }
    }
}

#[cfg(not(feature = "protocol_feature_flat_state"))]
mod imp {
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::ShardId;

    use crate::flat_state::FlatStorageState;
    use crate::Store;

    /// Since this has no variants it can never be instantiated.
    ///
    /// To use flat state enable `protocol_feature_flat_state` cargo feature.
    #[derive(Clone)]
    pub enum FlatState {}

    impl FlatState {
        pub fn get_ref(&self, _root: &CryptoHash, _key: &[u8]) -> ! {
            match *self {}
        }
    }

    pub struct FlatStateFactory {}

    impl FlatStateFactory {
        pub fn new(_store: Store) -> Self {
            Self {}
        }

        pub fn new_flat_state_for_shard(
            &self,
            _shard_id: ShardId,
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

use crate::{StoreUpdate, WrappedTrieChanges};
pub use imp::{FlatState, FlatStateFactory};
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[allow(unused)]
pub struct FlatStateDelta {
    // TODO: add delta implementation
}

impl FlatStateDelta {
    #[allow(unused)]
    pub fn from_wrapped_trie_changes(trie_changes: &WrappedTrieChanges) -> Self {
        // TODO: implement
        Self {}
    }
}

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
