//! This file contains helper functions for accessing flat storage data in DB
//! TODO(#8577): remove this file and move functions to the corresponding structs

use crate::flat::delta::{FlatStateChanges, KeyForFlatStateDelta};
use crate::flat::types::FlatStorageError;
use crate::{DBCol, Store, StoreUpdate};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardLayout, ShardUId};

use super::delta::{FlatStateDelta, FlatStateDeltaMetadata};
use super::types::{FlatStateValue, FlatStorageStatus};

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

pub fn remove_all_deltas(store_update: &mut StoreUpdate, shard_uid: ShardUId) {
    let key_from = shard_uid.to_bytes();
    let key_to = ShardUId::next_shard_prefix(&key_from);
    store_update.delete_range(DBCol::FlatStateChanges, &key_from, &key_to);
    store_update.delete_range(DBCol::FlatStateDeltaMetadata, &key_from, &key_to);
}

fn encode_flat_state_db_key(shard_uid: ShardUId, key: &[u8]) -> Vec<u8> {
    let mut buffer = vec![];
    buffer.extend_from_slice(&shard_uid.to_bytes());
    buffer.extend_from_slice(key);
    buffer
}

fn decode_flat_state_db_key(key: &Box<[u8]>) -> Result<(ShardUId, Vec<u8>), StorageError> {
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

/// Iterate over flat storage entries for a given shard.
/// It reads data only from the 'main' column - which represents the state as of final head.
///
/// WARNING: flat storage keeps changing, so the results might be inconsistent, unless you're running
/// this method on the shapshot of the data.
// TODO(#8676): Support non-trivial ranges and maybe pass `shard_uid` as key prefix.
pub fn iter_flat_state_entries<'a>(
    shard_layout: ShardLayout,
    shard_id: u64,
    store: &'a Store,
    from: Option<&'a Vec<u8>>,
    to: Option<&'a Vec<u8>>,
) -> impl Iterator<Item = (Vec<u8>, Box<[u8]>)> + 'a {
    store
        .iter_range(DBCol::FlatState, from.map(|x| x.as_slice()), to.map(|x| x.as_slice()))
        .filter_map(move |result| {
            if let Ok((key, value)) = result {
                // Currently all the data in flat storage is 'together' - so we have to parse the key,
                // to see if this element belongs to this shard.
                if let Ok(key_in_shard) = key_belongs_to_shard(&key, &shard_layout, shard_id) {
                    if key_in_shard {
                        let (_, trie_key) = decode_flat_state_db_key(&key).unwrap();
                        return Some((trie_key, value));
                    }
                }
            }
            return None;
        })
}

/// Currently all the data in flat storage is 'together' - so we have to parse the key,
/// to see if this element belongs to this shard.
pub fn key_belongs_to_shard(
    key: &Box<[u8]>,
    shard_layout: &ShardLayout,
    shard_id: u64,
) -> Result<bool, StorageError> {
    let (key_shard_uid, _) = decode_flat_state_db_key(key)?;
    Ok(key_shard_uid.version == shard_layout.version() && key_shard_uid.shard_id as u64 == shard_id)
}
