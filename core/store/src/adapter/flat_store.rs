use std::io;

use borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;

use crate::flat::delta::{BlockWithChangesInfo, KeyForFlatStateDelta};
use crate::flat::{
    FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata, FlatStateIterator, FlatStorageError,
    FlatStorageReadyStatus, FlatStorageStatus,
};
use crate::{DBCol, Store, StoreUpdate};

use super::{StoreAdapter, StoreUpdateAdapter, StoreUpdateHolder};

#[derive(Clone)]
pub struct FlatStoreAdapter {
    store: Store,
}

impl StoreAdapter for FlatStoreAdapter {
    fn store(&self) -> Store {
        self.store.clone()
    }
}

impl FlatStoreAdapter {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn store_update(&self) -> FlatStoreUpdateAdapter<'static> {
        FlatStoreUpdateAdapter { store_update: StoreUpdateHolder::Owned(self.store.store_update()) }
    }

    pub fn exists(&self, shard_uid: ShardUId, key: &[u8]) -> Result<bool, FlatStorageError> {
        let db_key = encode_flat_state_db_key(shard_uid, key);
        self.store.exists(DBCol::FlatState, &db_key).map_err(|err| {
            FlatStorageError::StorageInternalError(format!("failed to read FlatState value: {err}"))
        })
    }

    pub fn get(
        &self,
        shard_uid: ShardUId,
        key: &[u8],
    ) -> Result<Option<FlatStateValue>, FlatStorageError> {
        let db_key = encode_flat_state_db_key(shard_uid, key);
        self.store.get_ser(DBCol::FlatState, &db_key).map_err(|err| {
            FlatStorageError::StorageInternalError(format!("failed to read FlatState value: {err}"))
        })
    }

    pub fn get_flat_storage_status(
        &self,
        shard_uid: ShardUId,
    ) -> Result<FlatStorageStatus, FlatStorageError> {
        self.store
            .get_ser(DBCol::FlatStorageStatus, &shard_uid.to_bytes())
            .map(|status| status.unwrap_or(FlatStorageStatus::Empty))
            .map_err(|err| {
                FlatStorageError::StorageInternalError(format!(
                    "failed to read flat storage status: {err}"
                ))
            })
    }

    pub fn get_delta(
        &self,
        shard_uid: ShardUId,
        block_hash: CryptoHash,
    ) -> Result<Option<FlatStateChanges>, FlatStorageError> {
        let key = KeyForFlatStateDelta { shard_uid, block_hash };
        self.store.get_ser::<FlatStateChanges>(DBCol::FlatStateChanges, &key.to_bytes()).map_err(
            |err| {
                FlatStorageError::StorageInternalError(format!(
                    "failed to read delta changes for {key:?}: {err}"
                ))
            },
        )
    }

    pub fn get_all_deltas_metadata(
        &self,
        shard_uid: ShardUId,
    ) -> Result<Vec<FlatStateDeltaMetadata>, FlatStorageError> {
        self.store
            .iter_prefix_ser(DBCol::FlatStateDeltaMetadata, &shard_uid.to_bytes())
            .map(|res| {
                res.map(|(_, value)| value).map_err(|err| {
                    FlatStorageError::StorageInternalError(format!(
                        "failed to read delta metadata: {err}"
                    ))
                })
            })
            .collect()
    }

    pub fn get_prev_block_with_changes(
        &self,
        shard_uid: ShardUId,
        block_hash: CryptoHash,
        prev_hash: CryptoHash,
    ) -> Result<Option<BlockWithChangesInfo>, FlatStorageError> {
        let key = KeyForFlatStateDelta { shard_uid, block_hash: prev_hash }.to_bytes();
        let prev_delta_metadata: Option<FlatStateDeltaMetadata> =
            self.store.get_ser(DBCol::FlatStateDeltaMetadata, &key).map_err(|err| {
                FlatStorageError::StorageInternalError(format!(
                    "failed to read delta metadata for {key:?}: {err}"
                ))
            })?;

        let prev_block_with_changes = match prev_delta_metadata {
            None => {
                // DeltaMetadata not found, which means the prev block is the flat head.
                let flat_storage_status = self.get_flat_storage_status(shard_uid)?;
                match flat_storage_status {
                    FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head }) => {
                        if flat_head.hash == prev_hash {
                            Some(BlockWithChangesInfo { hash: prev_hash, height: flat_head.height })
                        } else {
                            tracing::error!(target: "store", ?block_hash, ?prev_hash, "Missing delta metadata");
                            None
                        }
                    }
                    // Don't do any performance optimizations while flat storage is not ready.
                    _ => None,
                }
            }
            Some(metadata) => {
                // If the prev block contains `prev_block_with_changes`, then use that value.
                // Otherwise reference the prev block.
                Some(metadata.prev_block_with_changes.unwrap_or(BlockWithChangesInfo {
                    hash: metadata.block.hash,
                    height: metadata.block.height,
                }))
            }
        };
        Ok(prev_block_with_changes)
    }

    /// Returns iterator over entire range of flat storage entries.
    /// It reads data only from `FlatState` column which represents the state at
    /// flat storage head. Reads only committed changes.
    pub fn iter<'a>(&'a self, shard_uid: ShardUId) -> FlatStateIterator<'a> {
        self.iter_range(shard_uid, None, None)
    }

    /// Returns iterator over flat storage entries for a given shard and range of state keys.
    /// It reads data only from `FlatState` column which represents the state at
    /// flat storage head. Reads only committed changes.
    pub fn iter_range<'a>(
        &'a self,
        shard_uid: ShardUId,
        from: Option<&[u8]>,
        to: Option<&[u8]>,
    ) -> FlatStateIterator<'a> {
        // If left direction is unbounded, encoded `shard_uid` serves as the
        // smallest possible key in DB for the shard.
        let db_key_from = match from {
            Some(from) => encode_flat_state_db_key(shard_uid, from),
            None => shard_uid.to_bytes().to_vec(),
        };
        // If right direction is unbounded, `ShardUId::next_shard_prefix` serves as
        // the key which is strictly bigger than all keys in DB for this shard and
        // still doesn't include keys from other shards.
        let db_key_to = match to {
            Some(to) => encode_flat_state_db_key(shard_uid, to),
            None => ShardUId::next_shard_prefix(&shard_uid.to_bytes()).to_vec(),
        };
        let iter = self
            .store
            .iter_range(DBCol::FlatState, Some(&db_key_from), Some(&db_key_to))
            .map(|result| match result {
                Ok((key, value)) => Ok((
                    decode_flat_state_db_key(&key)
                        .map_err(|err| {
                            FlatStorageError::StorageInternalError(format!(
                                "invalid FlatState key format: {err}"
                            ))
                        })?
                        .1,
                    FlatStateValue::try_from_slice(&value).map_err(|err| {
                        FlatStorageError::StorageInternalError(format!(
                            "invalid FlatState value format: {err}"
                        ))
                    })?,
                )),
                Err(err) => Err(FlatStorageError::StorageInternalError(format!(
                    "FlatState iterator error: {err}"
                ))),
            });
        Box::new(iter)
    }
}

pub struct FlatStoreUpdateAdapter<'a> {
    store_update: StoreUpdateHolder<'a>,
}

impl Into<StoreUpdate> for FlatStoreUpdateAdapter<'static> {
    fn into(self) -> StoreUpdate {
        self.store_update.into()
    }
}

impl FlatStoreUpdateAdapter<'static> {
    pub fn commit(self) -> io::Result<()> {
        let store_update: StoreUpdate = self.into();
        store_update.commit()
    }
}

impl<'a> StoreUpdateAdapter for FlatStoreUpdateAdapter<'a> {
    fn store_update(&mut self) -> &mut StoreUpdate {
        &mut self.store_update
    }
}

impl<'a> FlatStoreUpdateAdapter<'a> {
    pub fn new(store_update: &'a mut StoreUpdate) -> Self {
        Self { store_update: StoreUpdateHolder::Reference(store_update) }
    }

    pub fn set(&mut self, shard_uid: ShardUId, key: Vec<u8>, value: Option<FlatStateValue>) {
        let db_key = encode_flat_state_db_key(shard_uid, &key);
        match value {
            Some(value) => self
                .store_update
                .set_ser(DBCol::FlatState, &db_key, &value)
                .expect("Borsh should not have failed here"),
            None => self.store_update.delete(DBCol::FlatState, &db_key),
        }
    }

    pub fn remove_all(&mut self, shard_uid: ShardUId) {
        self.remove_range_by_shard_uid(shard_uid, DBCol::FlatState);
    }

    pub fn set_flat_storage_status(&mut self, shard_uid: ShardUId, status: FlatStorageStatus) {
        self.store_update
            .set_ser(DBCol::FlatStorageStatus, &shard_uid.to_bytes(), &status)
            .expect("Borsh should not have failed here")
    }

    pub fn remove_status(&mut self, shard_uid: ShardUId) {
        self.store_update.delete(DBCol::FlatStorageStatus, &shard_uid.to_bytes());
    }

    pub fn set_delta(&mut self, shard_uid: ShardUId, delta: &FlatStateDelta) {
        let key =
            KeyForFlatStateDelta { shard_uid, block_hash: delta.metadata.block.hash }.to_bytes();
        self.store_update
            .set_ser(DBCol::FlatStateChanges, &key, &delta.changes)
            .expect("Borsh should not have failed here");
        self.store_update
            .set_ser(DBCol::FlatStateDeltaMetadata, &key, &delta.metadata)
            .expect("Borsh should not have failed here");
    }

    pub fn remove_delta(&mut self, shard_uid: ShardUId, block_hash: CryptoHash) {
        let key = KeyForFlatStateDelta { shard_uid, block_hash }.to_bytes();
        self.store_update.delete(DBCol::FlatStateChanges, &key);
        self.store_update.delete(DBCol::FlatStateDeltaMetadata, &key);
    }

    pub fn remove_all_deltas(&mut self, shard_uid: ShardUId) {
        self.remove_range_by_shard_uid(shard_uid, DBCol::FlatStateChanges);
        self.remove_range_by_shard_uid(shard_uid, DBCol::FlatStateDeltaMetadata);
    }

    // helper
    fn remove_range_by_shard_uid(&mut self, shard_uid: ShardUId, col: DBCol) {
        let key_from = shard_uid.to_bytes();
        let key_to = ShardUId::next_shard_prefix(&key_from);
        self.store_update.delete_range(col, &key_from, &key_to);
    }
}

pub fn encode_flat_state_db_key(shard_uid: ShardUId, key: &[u8]) -> Vec<u8> {
    let mut buffer = vec![];
    buffer.extend_from_slice(&shard_uid.to_bytes());
    buffer.extend_from_slice(key);
    buffer
}

pub fn decode_flat_state_db_key(key: &[u8]) -> io::Result<(ShardUId, Vec<u8>)> {
    let (shard_uid_bytes, trie_key) = key.split_at_checked(8).ok_or_else(|| {
        io::Error::other(format!("expected FlatState key length to be at least 8: {key:?}"))
    })?;
    let shard_uid = shard_uid_bytes.try_into().map_err(|err| {
        io::Error::other(format!("failed to decode shard_uid as part of FlatState key: {err}"))
    })?;
    Ok((shard_uid, trie_key.to_vec()))
}

#[cfg(test)]
mod tests {
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::state::FlatStateValue;

    use crate::adapter::{StoreAdapter, StoreUpdateAdapter};
    use crate::test_utils::create_test_store;

    #[test]
    fn iter_flat_state_entries() {
        // Setup shards and store
        let store = create_test_store().flat_store();
        let shard_uids = [0, 1, 2].map(|id| ShardUId { version: 0, shard_id: id });

        for (i, shard_uid) in shard_uids.iter().enumerate() {
            let mut store_update = store.store_update();
            let key: Vec<u8> = vec![0, 1, i as u8];
            let val: Vec<u8> = vec![0, 1, 2, i as u8];

            // Add value to FlatState
            store_update.flat_store_update().set(
                *shard_uid,
                key.clone(),
                Some(FlatStateValue::inlined(&val)),
            );

            store_update.commit().unwrap();
        }

        for (i, shard_uid) in shard_uids.iter().enumerate() {
            let entries: Vec<_> = store.iter(*shard_uid).collect();
            assert_eq!(entries.len(), 1);
            let key: Vec<u8> = vec![0, 1, i as u8];
            let val: Vec<u8> = vec![0, 1, 2, i as u8];

            assert_eq!(entries, vec![Ok((key, FlatStateValue::inlined(&val)))]);
        }
    }
}
