//! This file contains helper functions for accessing flat storage data in DB
//! TODO(#8577): remove this file and move functions to the corresponding structs

use crate::db::FLAT_STATE_VALUES_INLINING_MIGRATION_STATUS_KEY;
use crate::flat::delta::{FlatStateChanges, KeyForFlatStateDelta};
use crate::flat::types::FlatStorageError;
use crate::{DBCol, Store, StoreUpdate};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;

use super::delta::{FlatStateDelta, FlatStateDeltaMetadata};
use super::types::{FlatStateValuesInliningMigrationStatus, FlatStorageStatus};

/// Prefixes for keys in `FlatStateMisc` DB column.
pub const FLAT_STATE_HEAD_KEY_PREFIX: &[u8; 4] = b"HEAD";
pub const FLAT_STATE_CREATION_STATUS_KEY_PREFIX: &[u8; 6] = b"STATUS";

pub fn get_delta_changes(
    store: &Store,
    shard_uid: ShardUId,
    block_hash: CryptoHash,
) -> Result<Option<FlatStateChanges>, FlatStorageError> {
    let key = KeyForFlatStateDelta { shard_uid, block_hash };
    Ok(store
        .get_ser::<FlatStateChanges>(DBCol::FlatStateChanges, &key.to_bytes())
        .map_err(|_| FlatStorageError::StorageInternalError)?)
}

pub fn get_all_deltas_metadata(
    store: &Store,
    shard_uid: ShardUId,
) -> Result<Vec<FlatStateDeltaMetadata>, FlatStorageError> {
    store
        .iter_prefix_ser(DBCol::FlatStateDeltaMetadata, &shard_uid.to_bytes())
        .map(|res| res.map(|(_, value)| value).map_err(|_| FlatStorageError::StorageInternalError))
        .collect()
}

pub fn set_delta(
    store_update: &mut StoreUpdate,
    shard_uid: ShardUId,
    delta: &FlatStateDelta,
) -> Result<(), FlatStorageError> {
    let key = KeyForFlatStateDelta { shard_uid, block_hash: delta.metadata.block.hash }.to_bytes();
    store_update
        .set_ser(DBCol::FlatStateChanges, &key, &delta.changes)
        .map_err(|_| FlatStorageError::StorageInternalError)?;
    store_update
        .set_ser(DBCol::FlatStateDeltaMetadata, &key, &delta.metadata)
        .map_err(|_| FlatStorageError::StorageInternalError)?;
    Ok(())
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

pub(crate) fn decode_flat_state_db_key(key: &[u8]) -> Result<(ShardUId, Vec<u8>), StorageError> {
    if key.len() < 8 {
        return Err(StorageError::StorageInconsistentState(format!(
            "Found key in flat storage with length < 8: {key:?}"
        )));
    }
    let (shard_uid_bytes, trie_key) = key.split_at(8);
    let shard_uid = shard_uid_bytes.try_into().map_err(|_| {
        StorageError::StorageInconsistentState(format!(
            "Incorrect raw shard uid: {shard_uid_bytes:?}"
        ))
    })?;
    Ok((shard_uid, trie_key.to_vec()))
}

pub fn get_flat_state_values_inlining_migration_status(
    store: &Store,
) -> Result<FlatStateValuesInliningMigrationStatus, FlatStorageError> {
    store
        .get_ser(DBCol::Misc, FLAT_STATE_VALUES_INLINING_MIGRATION_STATUS_KEY)
        .map(|status| status.unwrap_or(FlatStateValuesInliningMigrationStatus::Empty))
        .map_err(|_| FlatStorageError::StorageInternalError)
}

pub fn set_flat_state_values_inlining_migration_status(
    store: &Store,
    status: FlatStateValuesInliningMigrationStatus,
) -> Result<(), FlatStorageError> {
    let mut store_update = store.store_update();
    store_update
        .set_ser(DBCol::Misc, FLAT_STATE_VALUES_INLINING_MIGRATION_STATUS_KEY, &status)
        .map_err(|_| FlatStorageError::StorageInternalError)?;
    store_update.commit().map_err(|_| FlatStorageError::StorageInternalError)
}

pub(crate) fn get_flat_state_value(
    store: &Store,
    shard_uid: ShardUId,
    key: &[u8],
) -> Result<Option<FlatStateValue>, FlatStorageError> {
    let db_key = encode_flat_state_db_key(shard_uid, key);
    store.get_ser(DBCol::FlatState, &db_key).map_err(|_| FlatStorageError::StorageInternalError)
}

// TODO(#8577): make pub(crate) once flat storage creator is moved inside `flat` module.
pub fn set_flat_state_value(
    store_update: &mut StoreUpdate,
    shard_uid: ShardUId,
    key: Vec<u8>,
    value: Option<FlatStateValue>,
) -> Result<(), FlatStorageError> {
    let db_key = encode_flat_state_db_key(shard_uid, &key);
    match value {
        Some(value) => store_update
            .set_ser(DBCol::FlatState, &db_key, &value)
            .map_err(|_| FlatStorageError::StorageInternalError),
        None => Ok(store_update.delete(DBCol::FlatState, &db_key)),
    }
}

pub fn get_flat_storage_status(store: &Store, shard_uid: ShardUId) -> FlatStorageStatus {
    store
        .get_ser(DBCol::FlatStorageStatus, &shard_uid.to_bytes())
        .expect("Error reading flat head from storage")
        .unwrap_or(FlatStorageStatus::Empty)
}

pub fn set_flat_storage_status(
    store_update: &mut StoreUpdate,
    shard_uid: ShardUId,
    status: FlatStorageStatus,
) {
    store_update
        .set_ser(DBCol::FlatStorageStatus, &shard_uid.to_bytes(), &status)
        .expect("Borsh should not fail")
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
) -> impl Iterator<Item = (Vec<u8>, Box<[u8]>)> + 'a {
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
    store.iter_range(DBCol::FlatState, Some(&db_key_from), Some(&db_key_to)).filter_map(
        move |result| {
            if let Ok((key, value)) = result {
                let (_, trie_key) = decode_flat_state_db_key(&key).unwrap();
                return Some((trie_key, value));
            }
            return None;
        },
    )
}

/// Currently all the data in flat storage is 'together' - so we have to parse the key,
/// to see if this element belongs to this shard.
pub fn key_belongs_to_shard(key: &[u8], shard_uid: &ShardUId) -> Result<bool, StorageError> {
    let (key_shard_uid, _) = decode_flat_state_db_key(key)?;
    Ok(key_shard_uid == *shard_uid)
}

#[cfg(test)]
mod tests {
    use super::iter_flat_state_entries;
    use crate::flat::store_helper::set_flat_state_value;
    use crate::test_utils::create_test_store;
    use borsh::BorshDeserialize;
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::state::FlatStateValue;

    #[test]
    fn test_iter_flat_state_entries() {
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
            )
            .unwrap();

            store_update.commit().unwrap();
        }

        for (i, shard_uid) in shard_uids.iter().enumerate() {
            let entries: Vec<_> = iter_flat_state_entries(*shard_uid, &store, None, None).collect();
            assert_eq!(entries.len(), 1);
            let key: Vec<u8> = vec![0, 1, i as u8];
            let val: Vec<u8> = vec![0, 1, 2, i as u8];

            assert_eq!(entries.get(0).unwrap().0, key);
            let actual_val = &entries.get(0).unwrap().1;
            assert_eq!(
                FlatStateValue::try_from_slice(actual_val).unwrap(),
                FlatStateValue::inlined(&val)
            );
        }
    }
}
