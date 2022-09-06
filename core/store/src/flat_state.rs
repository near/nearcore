//! Contains flat state optimization logic.
//!
//! The state of the contract is a key-value map, `Map<Vec<u8>, Vec<u8>>`.
//! In the database, we store this map as a trie, which allows us to construct succinct proofs that a certain key/value
//! belongs to contract's state. Using a trie has a drawback -- reading a single key/value requires traversing the trie
//! from the root, loading many nodes from the database.
//! To optimize this, we want to use flat state: alongside the trie, we store a mapping from keys to value
//! references so that, if you don't need a proof, you can do a db lookup in just two db accesses - one to get value
//! reference, one to get value itself.
// TODO (#7327): consider inlining small values, so we could use only one db access.

#[cfg(feature = "protocol_feature_flat_state")]
mod imp {
    use crate::db::FLAT_STATE_HEAD_KEY;
    use near_primitives::block_header::BlockHeader;
    use near_primitives::errors::StorageError;
    use near_primitives::hash::CryptoHash;
    use near_primitives::state::ValueRef;

    use crate::{KeyForStateChanges, Store};

    /// Struct for getting value references from the flat storage.
    ///
    /// Used to speed up `get` and `get_ref` trie methods.  It should store all
    /// trie keys for state on top of chain head, except delayed receipt keys,
    /// because they are the same for each shard and they are requested only
    /// once during applying chunk.
    // TODO (#7327): implement flat state deltas to support forks.
    // TODO (#7327): store on top of final head (or earlier) so updates will
    // only go forward.
    #[derive(Clone)]
    pub struct FlatState {
        store: Store,
        prev_block_hash: CryptoHash,
    }

    impl FlatState {
        fn get_raw_ref(&self, key: &[u8]) -> Result<Option<crate::db::DBSlice<'_>>, StorageError> {
            tracing::debug!(target: "client", "fs_get_raw_ref: {:?}", key);
            let flat_state_head: CryptoHash = self
                .store
                .get_ser(crate::DBCol::BlockMisc, FLAT_STATE_HEAD_KEY)
                .map_err(|_| StorageError::StorageInternalError)?
                .unwrap();
            let block_header: BlockHeader = self
                .store
                .get_ser(crate::DBCol::BlockHeader, flat_state_head.as_ref())
                .map_err(|_| StorageError::StorageInternalError)?
                .unwrap();
            tracing::debug!(target: "client", "fs_get_raw_ref: flat_state_head: {:?} height: {}", flat_state_head, block_header.height());
            let mut block_hash = self.prev_block_hash;
            while block_hash != flat_state_head {
                tracing::debug!(target: "client", "fs_get_raw_ref: block_hash: {:?}", block_hash);
                let flat_state_delta_key = KeyForStateChanges::from_raw_key(&block_hash, &key);
                let value_from_delta = self
                    .store
                    .get(crate::DBCol::FlatStateDeltas, flat_state_delta_key.as_ref())
                    .map_err(|_| StorageError::StorageInternalError)?;
                tracing::debug!(target: "client", "fs_get_raw_ref: block_hash: {:?}", value_from_delta);
                match value_from_delta {
                    Some(value) => {
                        return Ok(Some(value));
                    }
                    None => {}
                }
                tracing::debug!(target: "client", "fs_get_raw_ref: getting header");
                let block_header: BlockHeader = self
                    .store
                    .get_ser(crate::DBCol::BlockHeader, block_hash.as_ref())
                    .map_err(|_| StorageError::StorageInternalError)?
                    .unwrap();
                block_hash = block_header.prev_hash().clone();
            }
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
        pub fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, StorageError> {
            tracing::debug!(target: "client", "fs_get_ref: {:?}", key);
            let raw_ref = self.get_raw_ref(key);
            tracing::debug!(target: "client", "fs_get_ref: raw_ref: {:?}", raw_ref);
            match raw_ref? {
                Some(bytes) => ValueRef::decode(&bytes)
                    .map(Some)
                    .map_err(|_| StorageError::StorageInternalError),
                None => Ok(None),
            }
        }
    }

    /// Possibly creates a new [`FlateState`] object backed by given storage.
    ///
    /// Always returns `None` if the `protocol_feature_flat_state` Cargo feature is
    /// not enabled.  Otherwise, returns a new [`FlatState`] object backed by
    /// specified storage if `use_flat_state` argument is true.
    pub fn maybe_new(
        use_flat_state: bool,
        store: &Store,
        prev_block_hash: &CryptoHash,
    ) -> Option<FlatState> {
        use_flat_state
            .then(|| FlatState { store: store.clone(), prev_block_hash: prev_block_hash.clone() })
    }
}

#[cfg(not(feature = "protocol_feature_flat_state"))]
mod imp {
    use near_primitives::hash::CryptoHash;

    use crate::Store;

    /// Since this has no variants it can never be instantiated.
    ///
    /// To use flat state enable `protocol_feature_flat_state` cargo feature.
    #[derive(Clone)]
    pub enum FlatState {}

    impl FlatState {
        pub fn get_ref(&self, _root: &CryptoHash, key: &[u8]) -> ! {
            tracing::debug!(target: "client", "fs_wrong_get_ref: {:?}", key);
            match *self {}
        }
    }

    /// Always returns `None`; to use of flat state enable
    /// `protocol_feature_flat_state` cargo feature.
    #[inline]
    pub fn maybe_new(_use_flat_state: bool, _store: &Store) -> Option<FlatState> {
        None
    }
}

pub use imp::{maybe_new, FlatState};
