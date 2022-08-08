//! Contains flat state optimization logic.
//!
//! The state of the contract is a key-value map, `Map<Vec<u8>, Vec<u8>>`.
//! In the database, we store this map as a trie, which allows us to construct succinct proofs that a certain key/value
//! belongs to contract's state. Using a trie has a drawback -- reading a single key/value requires traversing the trie
//! from the root, loading many nodes from the database.
//! To optimize this, we want to use flat state: alongside the trie, we store a *direct* mapping from keys to value
//! references so that, if you don't need a proof, you can do a db lookup in just two db accesses.
/// TODO (#7327): consider inlining small values, so we could use only one db access.

#[cfg(feature = "protocol_feature_flat_state")]
use crate::DBCol;
use crate::Store;
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::state::ValueRef;

/// Struct for getting value references from the flat storage.
/// Used to speed up `get` and `get_ref` trie methods.
/// It should store all trie keys for state on top of chain head, except delayed receipt keys, because they are the
/// same for each shard and they are requested only once during applying chunk.
/// TODO (#7327): implement flat state deltas to support forks
/// TODO (#7327): store on top of final head (or earlier) so updates will only go forward
#[derive(Clone)]
pub struct FlatState {
    #[allow(dead_code)]
    store: Store,
}

impl FlatState {
    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    #[allow(unused_variables)]
    fn get_raw_ref(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        #[cfg(feature = "protocol_feature_flat_state")]
        return self
            .store
            .get(DBCol::FlatState, key)
            .map_err(|_| StorageError::StorageInternalError);
        #[cfg(not(feature = "protocol_feature_flat_state"))]
        unreachable!();
    }

    /// Get value reference using raw trie key and state root. We assume that flat state contains data for this root.
    /// To avoid duplication, we don't store values themselves in flat state, they are stored in `DBCol::State`.
    /// TODO (#7327): support different roots (or block hashes).
    pub fn get_ref(
        &self,
        _root: &CryptoHash,
        key: &[u8],
    ) -> Result<Option<ValueRef>, StorageError> {
        match self.get_raw_ref(key)? {
            Some(bytes) => {
                ValueRef::decode(&bytes).map(Some).map_err(|_| StorageError::StorageInternalError)
            }
            None => Ok(None),
        }
    }
}
