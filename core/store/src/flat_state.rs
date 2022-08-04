#[cfg(feature = "protocol_feature_flat_state")]
use crate::DBCol;
use crate::Store;
use near_primitives::errors::StorageError;
use near_primitives::state::ValueRef;

/// Struct for getting value references from the flat storage.
/// Used to speed up `get` and `get_ref` trie methods.
#[derive(Clone)]
pub struct FlatState {
    #[allow(dead_code)]
    pub(crate) store: Store,
}

impl FlatState {
    fn get_raw_ref(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        #[cfg(feature = "protocol_feature_flat_state")]
        return self
            .store
            .get(DBCol::FlatState, key)
            .map_err(|_| StorageError::StorageInternalError);
        #[cfg(not(feature = "protocol_feature_flat_state"))]
        {
            let _ = key;
            unreachable!();
        }
    }

    pub fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, StorageError> {
        match self.get_raw_ref(key)? {
            Some(bytes) => ValueRef::decode(&bytes).map_err(|_| StorageError::StorageInternalError),
            None => Ok(None),
        }
    }
}
