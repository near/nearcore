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
//! we can perform lookups for the other blocks. See comments in `FlatStorage` to see
//! which block should be the head of flat storage and which other blocks do flat storage support.
//!
//! This file contains the implementation of FlatStorage. It has three essential structs.
//!
//! `FlatStorageChunkView`: this provides an interface to get value or value references from flat storage. This
//!              is the struct that will be stored as part of Trie. All trie reads will be directed
//!              to the flat state.
//! `FlatStorageManager`: this is to construct flat state.
//! `FlatStorage`: this stores some information about the state of the flat storage itself,
//!                     for example, all block deltas that are stored in flat storage and a representation
//!                     of the chain formed by these blocks (because we can't access ChainStore
//!                     inside flat storage).

#[cfg(feature = "protocol_feature_flat_state")]
mod chunk_view;
mod delta;
#[cfg(feature = "protocol_feature_flat_state")]
mod manager;
mod storage;
#[cfg(feature = "protocol_feature_flat_state")]
pub mod store_helper;
mod types;

pub use chunk_view::FlatStorageChunkView;
pub use delta::FlatStateDelta;
pub use manager::FlatStorageManager;
pub use storage::FlatStorage;
pub use types::{
    BlockInfo, ChainAccessForFlatStorage, FetchingStateStatus, FlatStorageCreationStatus,
    FlatStorageError,
};

#[allow(unused)]
pub(crate) const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Number of traversed parts during a single step of fetching state.
#[allow(unused)]
pub const NUM_PARTS_IN_ONE_STEP: u64 = 20;

/// Memory limit for state part being fetched.
#[allow(unused)]
pub const STATE_PART_MEMORY_LIMIT: bytesize::ByteSize = bytesize::ByteSize(10 * bytesize::MIB);

#[cfg(not(feature = "protocol_feature_flat_state"))]
pub mod store_helper {
    use super::delta::FlatStateDelta;
    use super::types::{FlatStorageCreationStatus, FlatStorageError};
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

#[cfg(not(feature = "protocol_feature_flat_state"))]
mod manager {
    use super::storage::FlatStorage;
    use super::FlatStorageChunkView;
    use crate::{Store, StoreUpdate};
    use near_primitives::errors::StorageError;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::ShardId;

    #[derive(Clone)]
    pub struct FlatStorageManager {}

    impl FlatStorageManager {
        pub fn new(_store: Store) -> Self {
            Self {}
        }

        pub fn chunk_view(
            &self,
            _shard_id: ShardId,
            _block_hash: Option<CryptoHash>,
            _is_view: bool,
        ) -> Option<FlatStorageChunkView> {
            None
        }

        pub fn get_flat_storage_for_shard(&self, _shard_id: ShardId) -> Option<FlatStorage> {
            None
        }

        pub fn add_flat_storage_for_shard(&self, _shard_id: ShardId, _flat_storage: FlatStorage) {}

        pub fn remove_flat_storage_for_shard(
            &self,
            _shard_id: ShardId,
            _shard_layout: ShardLayout,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        pub fn set_flat_storage_for_genesis(
            &self,
            _store_update: &mut StoreUpdate,
            _shard_id: ShardId,
            _genesis_block: &CryptoHash,
        ) {
        }
    }
}

#[cfg(not(feature = "protocol_feature_flat_state"))]
mod chunk_view {
    /// Since this has no variants it can never be instantiated.
    ///
    /// To use flat state enable `protocol_feature_flat_state` cargo feature.
    #[derive(Clone)]
    pub enum FlatStorageChunkView {}

    impl FlatStorageChunkView {
        pub fn get_ref(&self, _key: &[u8]) -> ! {
            match *self {}
        }
    }
}
