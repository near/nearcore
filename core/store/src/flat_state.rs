use crate::{DBCol, Store};
use alloc::vec::Vec;
use byteorder::{LittleEndian, ReadBytesExt};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::state::ValueRef;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::ValueRef;
use near_store::columns::DBCol;
use std::io::{Cursor, Read};

/// Struct for getting value references from the flat storage.
/// Used to speed up `get` and `get_ref` trie methods.
#[derive(Clone)]
pub struct FlatState {
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
