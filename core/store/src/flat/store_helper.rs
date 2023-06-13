//! This file contains helper functions for accessing flat storage data in DB
//! TODO(#8577): remove this file and move functions to the corresponding structs

use std::io;

use crate::db::FLAT_STATE_VALUES_INLINING_MIGRATION_STATUS_KEY;
use crate::flat::delta::{FlatStateChanges, KeyForFlatStateDelta};
use crate::flat::types::FlatStorageError;
use crate::{DBCol, Store, StoreUpdate};
use borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;

use super::delta::{FlatStateDelta, FlatStateDeltaMetadata};
use super::types::{
    FlatStateIterator, FlatStateValuesInliningMigrationStatus, FlatStorageResult, FlatStorageStatus,
};

pub fn get_delta_changes(
    store: &Store,
    shard_uid: ShardUId,
    block_hash: CryptoHash,
) -> FlatStorageResult<Option<FlatStateChanges>> {
    let key = KeyForFlatStateDelta { shard_uid, block_hash };
    store.get_ser::<FlatStateChanges>(DBCol::FlatStateChanges, &key.to_bytes()).map_err(|err| {
        FlatStorageError::StorageInternalError(format!(
            "failed to read delta changes for {key:?}: {err}"
        ))
    })
}

pub fn get_all_deltas_metadata(
    store: &Store,
    shard_uid: ShardUId,
) -> FlatStorageResult<Vec<FlatStateDeltaMetadata>> {
    store
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

pub fn set_delta(store_update: &mut StoreUpdate, shard_uid: ShardUId, delta: &FlatStateDelta) {
    let key = KeyForFlatStateDelta { shard_uid, block_hash: delta.metadata.block.hash }.to_bytes();
    store_update
        .set_ser(DBCol::FlatStateChanges, &key, &delta.changes)
        .expect("Borsh should not have failed here");
    store_update
        .set_ser(DBCol::FlatStateDeltaMetadata, &key, &delta.metadata)
        .expect("Borsh should not have failed here");
}

pub fn remove_delta(store_update: &mut StoreUpdate, shard_uid: ShardUId, block_hash: CryptoHash) {
    let key = KeyForFlatStateDelta { shard_uid, block_hash }.to_bytes();
    store_update.delete(DBCol::FlatStateChanges, &key);
    store_update.delete(DBCol::FlatStateDeltaMetadata, &key);
}

fn remove_range_by_shard_uid(store_update: &mut StoreUpdate, shard_uid: ShardUId, col: DBCol) {
    let key_from = shard_uid.to_bytes();
    let key_to = ShardUId::next_shard_prefix(&key_from);
    store_update.delete_range(col, &key_from, &key_to);
}

pub fn remove_all_deltas(store_update: &mut StoreUpdate, shard_uid: ShardUId) {
    remove_range_by_shard_uid(store_update, shard_uid, DBCol::FlatStateChanges);
    remove_range_by_shard_uid(store_update, shard_uid, DBCol::FlatStateDeltaMetadata);
}

pub fn remove_all_flat_state_values(store_update: &mut StoreUpdate, shard_uid: ShardUId) {
    remove_range_by_shard_uid(store_update, shard_uid, DBCol::FlatState);
}

pub(crate) fn encode_flat_state_db_key(shard_uid: ShardUId, key: &[u8]) -> Vec<u8> {
    let mut buffer = vec![];
    buffer.extend_from_slice(&shard_uid.to_bytes());
    buffer.extend_from_slice(key);
    buffer
}

pub fn decode_flat_state_db_key(key: &[u8]) -> io::Result<(ShardUId, Vec<u8>)> {
    if key.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("expected FlatState key length to be at least 8: {key:?}"),
        ));
    }
    let (shard_uid_bytes, trie_key) = key.split_at(8);
    let shard_uid = shard_uid_bytes.try_into().map_err(|err| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("failed to decode shard_uid as part of FlatState key: {err}"),
        )
    })?;
    Ok((shard_uid, trie_key.to_vec()))
}

pub fn get_flat_state_values_inlining_migration_status(
    store: &Store,
) -> FlatStorageResult<FlatStateValuesInliningMigrationStatus> {
    store
        .get_ser(DBCol::Misc, FLAT_STATE_VALUES_INLINING_MIGRATION_STATUS_KEY)
        .map(|status| status.unwrap_or(FlatStateValuesInliningMigrationStatus::Empty))
        .map_err(|err| {
            FlatStorageError::StorageInternalError(format!(
                "failed to read FlatState values inlining migration status: {err}"
            ))
        })
}

pub fn set_flat_state_values_inlining_migration_status(
    store: &Store,
    status: FlatStateValuesInliningMigrationStatus,
) -> FlatStorageResult<()> {
    let mut store_update = store.store_update();
    store_update
        .set_ser(DBCol::Misc, FLAT_STATE_VALUES_INLINING_MIGRATION_STATUS_KEY, &status)
        .expect("Borsh should not have failed here");
    store_update.commit().map_err(|err| {
        FlatStorageError::StorageInternalError(format!(
            "failed to commit FlatState values inlining migration status: {err}"
        ))
    })
}

pub(crate) fn get_flat_state_value(
    store: &Store,
    shard_uid: ShardUId,
    key: &[u8],
) -> FlatStorageResult<Option<FlatStateValue>> {
    let db_key = encode_flat_state_db_key(shard_uid, key);
    store.get_ser(DBCol::FlatState, &db_key).map_err(|err| {
        FlatStorageError::StorageInternalError(format!("failed to read FlatState value: {err}"))
    })
}

// TODO(#8577): make pub(crate) once flat storage creator is moved inside `flat` module.
pub fn set_flat_state_value(
    store_update: &mut StoreUpdate,
    shard_uid: ShardUId,
    key: Vec<u8>,
    value: Option<FlatStateValue>,
) {
    let db_key = encode_flat_state_db_key(shard_uid, &key);
    match value {
        Some(value) => store_update
            .set_ser(DBCol::FlatState, &db_key, &value)
            .expect("Borsh should not have failed here"),
        None => store_update.delete(DBCol::FlatState, &db_key),
    }
}

pub fn get_flat_storage_status(
    store: &Store,
    shard_uid: ShardUId,
) -> FlatStorageResult<FlatStorageStatus> {
    store
        .get_ser(DBCol::FlatStorageStatus, &shard_uid.to_bytes())
        .map(|status| status.unwrap_or(FlatStorageStatus::Empty))
        .map_err(|err| {
            FlatStorageError::StorageInternalError(format!(
                "failed to read flat storage status: {err}"
            ))
        })
}

pub fn set_flat_storage_status(
    store_update: &mut StoreUpdate,
    shard_uid: ShardUId,
    status: FlatStorageStatus,
) {
    store_update
        .set_ser(DBCol::FlatStorageStatus, &shard_uid.to_bytes(), &status)
        .expect("Borsh should not have failed here")
}

/// Returns iterator over flat storage entries for a given shard and range of
/// state keys. `None` means that there is no bound in respective direction.
/// It reads data only from `FlatState` column which represents the state at
/// flat storage head. Reads only commited changes.
pub fn iter_flat_state_entries<'a>(
    shard_uid: ShardUId,
    store: &'a Store,
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
    let iter =
        store.iter_range(DBCol::FlatState, Some(&db_key_from), Some(&db_key_to)).map(|result| {
            match result {
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
            }
        });
    Box::new(iter)
}

#[cfg(test)]
mod tests {
    use crate::flat::store_helper::set_flat_state_value;
    use crate::test_utils::create_test_store;
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::state::FlatStateValue;

    #[test]
    fn iter_flat_state_entries() {
        // Setup shards and store
        let store = create_test_store();
        let shard_uids = [0, 1, 2].map(|id| ShardUId { version: 0, shard_id: id });

        for (i, shard_uid) in shard_uids.iter().enumerate() {
            let mut store_update = store.store_update();
            let key: Vec<u8> = vec![0, 1, i as u8];
            let val: Vec<u8> = vec![0, 1, 2, i as u8];

            // Add value to FlatState
            set_flat_state_value(
                &mut store_update,
                *shard_uid,
                key.clone(),
                Some(FlatStateValue::inlined(&val)),
            );

            store_update.commit().unwrap();
        }

        for (i, shard_uid) in shard_uids.iter().enumerate() {
            let entries: Vec<_> =
                super::iter_flat_state_entries(*shard_uid, &store, None, None).collect();
            assert_eq!(entries.len(), 1);
            let key: Vec<u8> = vec![0, 1, i as u8];
            let val: Vec<u8> = vec![0, 1, 2, i as u8];

            assert_eq!(entries, vec![Ok((key, FlatStateValue::inlined(&val)))]);
        }
    }
}
