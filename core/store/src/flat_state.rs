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

use borsh::{BorshDeserialize, BorshSerialize};

#[cfg(feature = "protocol_feature_flat_state")]
mod imp {
    use borsh::BorshSerialize;

    use near_primitives::block_header::BlockHeader;
    use near_primitives::errors::StorageError;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::state::ValueRef;

    use crate::flat_state::KeyForFlatStateDelta;
    use crate::{DBCol, FlatStateDelta, Store, StoreUpdate};

    /// Struct for getting value references from the flat storage.
    ///
    /// Used to speed up `get` and `get_ref` trie methods.  It should store all
    /// trie keys for state on top of chain head, except delayed receipt keys,
    /// because they are the same for each shard and they are requested only
    /// once during applying chunk.
    // TODO (#7327): lock flat state when `get_ref` is called or tail is being updated. Otherwise, `apply_chunks` and
    // `postprocess_block` parallel execution may corrupt the state.
    #[derive(Clone)]
    pub struct FlatState {
        /// Used to access flat state stored at the head of flat storage.
        /// It should store all trie keys and values/value refs for the state on top of
        /// flat_storage_state.head, except for delayed receipt keys.
        store: Store,
        /// Id of the shard which state is accessed by this object.
        shard_uid: ShardUId,
        /// The block for which key-value pairs of its state. Note that it is not necessarily
        /// the latest block.
        head: CryptoHash,
    }

    impl FlatState {
        /// Possibly creates a new [`FlatState`] object backed by given storage.
        ///
        /// Always returns `None` if the `protocol_feature_flat_state` Cargo feature is
        /// not enabled.  Otherwise, returns a new [`FlatState`] object backed by
        /// specified storage.
        pub fn maybe_new(
            shard_uid: ShardUId,
            prev_block_hash: &CryptoHash,
            store: &Store,
        ) -> Option<FlatState> {
            Some(FlatState { store: store.clone(), shard_uid, head: prev_block_hash.clone() })
        }

        /// Update the tail of the flat storage. Return a StoreUpdate for the disk update.
        pub fn update_tail(
            shard_uid: ShardUId,
            block_hash: &CryptoHash,
            store: &Store,
        ) -> StoreUpdate {
            let mut store_update = StoreUpdate::new(store.storage.clone());
            store_update
                .set_ser(DBCol::FlatStateMisc, &shard_uid.try_to_vec().unwrap(), block_hash)
                .expect("Borsh cannot fail");
            store_update
        }

        /// Get deltas for blocks between flat state tail and current head.
        /// If sequence of deltas contains final block, tail is moved to this tail and all deltas until the tail are
        /// applied.
        fn get_deltas_between_blocks(&self) -> Result<Vec<FlatStateDelta>, StorageError> {
            let target_block_hash = self.head;
            let flat_state_tail: CryptoHash = self
                .store
                .get_ser(DBCol::FlatStateMisc, &self.shard_uid.try_to_vec().unwrap())
                .map_err(|_| StorageError::StorageInternalError)?
                .expect("Borsh cannot fail");

            let block_header: BlockHeader = self
                .store
                .get_ser(DBCol::BlockHeader, target_block_hash.as_ref())
                .map_err(|_| StorageError::StorageInternalError)?
                .unwrap();
            let final_block_hash = block_header.last_final_block().clone();

            let mut block_hash = target_block_hash.clone();
            let mut deltas = vec![];
            let mut deltas_to_apply = vec![];
            let mut found_final_block = false;
            while block_hash != flat_state_tail {
                if block_hash == final_block_hash {
                    assert!(!found_final_block);
                    found_final_block = true;
                }

                let key = KeyForFlatStateDelta { shard_uid: self.shard_uid, block_hash };
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
                store_update.merge(FlatState::update_tail(
                    self.shard_uid,
                    &final_block_hash,
                    &self.store,
                ));
                store_update.commit().map_err(|_| StorageError::StorageInternalError)?
            }
            Ok(deltas)
        }

        /// Returns value reference using raw trie key and state root.
        ///
        /// We assume that flat state contains data for this root.  To avoid
        /// duplication, we don't store values themselves in flat state, they
        /// are stored in `DBCol::State`. Also the separation is done so we
        /// could charge users for the value length before loading the value.
        // TODO (#7327): support different roots (or block hashes).
        pub fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, StorageError> {
            let deltas = self.get_deltas_between_blocks()?;
            for delta in deltas {
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
}

#[cfg(not(feature = "protocol_feature_flat_state"))]
mod imp {
    use crate::{Store, StoreUpdate};
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardUId;

    /// Since this has no variants it can never be instantiated.
    ///
    /// To use flat state enable `protocol_feature_flat_state` cargo feature.
    #[derive(Clone)]
    pub enum FlatState {}

    impl FlatState {
        /// Always returns `None`; to use of flat state enable `protocol_feature_flat_state` cargo feature.
        #[inline]
        pub fn maybe_new(
            _shard_uid: ShardUId,
            _prev_block_hash: &CryptoHash,
            _store: &Store,
        ) -> Option<FlatState> {
            None
        }

        pub fn update_tail(
            _shard_uid: ShardUId,
            _block_hash: &CryptoHash,
            store: &Store,
        ) -> StoreUpdate {
            StoreUpdate::new(store.storage.clone())
        }

        pub fn get_ref(&self, _key: &[u8]) -> ! {
            match *self {}
        }
    }
}

use crate::{CryptoHash, StoreUpdate};
pub use imp::FlatState;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::ValueRef;
use near_primitives::types::RawStateChangesWithTrieKey;
use std::collections::HashMap;

#[derive(BorshSerialize, BorshDeserialize)]
pub struct KeyForFlatStateDelta {
    pub shard_uid: ShardUId,
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
