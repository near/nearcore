//! This file contains helper functions for accessing flat storage data in DB
//! TODO(#8577): remove this file and move functions to the corresponding structs

use crate::flat::delta::{FlatStateDelta, KeyForFlatStateDelta};
use crate::flat::types::{FetchingStateStatus, FlatStorageCreationStatus, FlatStorageError};
use crate::{Store, StoreUpdate};
use borsh::{BorshDeserialize, BorshSerialize};
use byteorder::ReadBytesExt;
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_id, ShardLayout};
use near_primitives::state::ValueRef;
use near_primitives::trie_key::trie_key_parsers::parse_account_id_from_raw_key;
use near_primitives::types::ShardId;

/// Prefixes determining type of flat storage creation status stored in DB.
/// Note that non-existent status is treated as SavingDeltas if flat storage /// does not exist and Ready if it does.
const FETCHING_STATE: u8 = 0;
const CATCHING_UP: u8 = 1;

/// Prefixes for keys in `FlatStateMisc` DB column.
pub const FLAT_STATE_HEAD_KEY_PREFIX: &[u8; 4] = b"HEAD";
pub const FLAT_STATE_CREATION_STATUS_KEY_PREFIX: &[u8; 6] = b"STATUS";

/// This is needed to avoid `#[cfg(feature = "protocol_feature_flat_state")]`
/// from `DBCol::FlatState*` cascading all over the code.
/// Should be removed along with protocol_feature_flat_state feature.
pub enum FlatStateColumn {
    State,
    Deltas,
    Misc,
}

impl FlatStateColumn {
    pub const fn to_db_col(&self) -> crate::DBCol {
        #[cfg(feature = "protocol_feature_flat_state")]
        match self {
            FlatStateColumn::State => crate::DBCol::FlatState,
            FlatStateColumn::Deltas => crate::DBCol::FlatStateDeltas,
            FlatStateColumn::Misc => crate::DBCol::FlatStateMisc,
        }
        #[cfg(not(feature = "protocol_feature_flat_state"))]
        panic!("protocol_feature_flat_state feature is not enabled")
    }
}

pub fn get_delta(
    store: &Store,
    shard_id: ShardId,
    block_hash: CryptoHash,
) -> Result<Option<FlatStateDelta>, FlatStorageError> {
    let key = KeyForFlatStateDelta { shard_id, block_hash };
    Ok(store
        .get_ser::<FlatStateDelta>(FlatStateColumn::Deltas.to_db_col(), &key.try_to_vec().unwrap())
        .map_err(|_| FlatStorageError::StorageInternalError)?)
}

pub fn set_delta(
    store_update: &mut StoreUpdate,
    shard_id: ShardId,
    block_hash: CryptoHash,
    delta: &FlatStateDelta,
) -> Result<(), FlatStorageError> {
    let key = KeyForFlatStateDelta { shard_id, block_hash };
    store_update
        .set_ser(FlatStateColumn::Deltas.to_db_col(), &key.try_to_vec().unwrap(), delta)
        .map_err(|_| FlatStorageError::StorageInternalError)
}

pub fn remove_delta(store_update: &mut StoreUpdate, shard_id: ShardId, block_hash: CryptoHash) {
    let key = KeyForFlatStateDelta { shard_id, block_hash };
    store_update.delete(FlatStateColumn::Deltas.to_db_col(), &key.try_to_vec().unwrap());
}

fn flat_head_key(shard_id: ShardId) -> Vec<u8> {
    let mut fetching_state_step_key = FLAT_STATE_HEAD_KEY_PREFIX.to_vec();
    fetching_state_step_key.extend_from_slice(&shard_id.try_to_vec().unwrap());
    fetching_state_step_key
}

pub fn get_flat_head(store: &Store, shard_id: ShardId) -> Option<CryptoHash> {
    if !cfg!(feature = "protocol_feature_flat_state") {
        return None;
    }
    store
        .get_ser(FlatStateColumn::Misc.to_db_col(), &flat_head_key(shard_id))
        .expect("Error reading flat head from storage")
}

pub fn set_flat_head(store_update: &mut StoreUpdate, shard_id: ShardId, val: &CryptoHash) {
    store_update
        .set_ser(FlatStateColumn::Misc.to_db_col(), &flat_head_key(shard_id), val)
        .expect("Error writing flat head from storage")
}

pub fn remove_flat_head(store_update: &mut StoreUpdate, shard_id: ShardId) {
    store_update.delete(FlatStateColumn::Misc.to_db_col(), &flat_head_key(shard_id));
}

pub(crate) fn get_ref(store: &Store, key: &[u8]) -> Result<Option<ValueRef>, FlatStorageError> {
    let raw_ref = store
        .get(FlatStateColumn::State.to_db_col(), key)
        .map_err(|_| FlatStorageError::StorageInternalError)?;
    if let Some(raw_ref) = raw_ref {
        let bytes =
            raw_ref.as_slice().try_into().map_err(|_| FlatStorageError::StorageInternalError)?;
        Ok(Some(ValueRef::decode(bytes)))
    } else {
        Ok(None)
    }
}

pub(crate) fn set_ref(
    store_update: &mut StoreUpdate,
    key: Vec<u8>,
    value: Option<ValueRef>,
) -> Result<(), FlatStorageError> {
    match value {
        Some(value) => store_update
            .set_ser(FlatStateColumn::State.to_db_col(), &key, &value)
            .map_err(|_| FlatStorageError::StorageInternalError),
        None => Ok(store_update.delete(FlatStateColumn::State.to_db_col(), &key)),
    }
}

fn creation_status_key(shard_id: ShardId) -> Vec<u8> {
    let mut key = FLAT_STATE_CREATION_STATUS_KEY_PREFIX.to_vec();
    key.extend_from_slice(&shard_id.try_to_vec().unwrap());
    key
}

pub fn set_flat_storage_creation_status(
    store_update: &mut StoreUpdate,
    shard_id: ShardId,
    status: FlatStorageCreationStatus,
) {
    let value = match status {
        FlatStorageCreationStatus::FetchingState(status) => {
            let mut value = vec![FETCHING_STATE];
            value.extend_from_slice(&status.try_to_vec().unwrap());
            value
        }
        FlatStorageCreationStatus::CatchingUp(block_hash) => {
            let mut value = vec![CATCHING_UP];
            value.extend_from_slice(block_hash.as_bytes());
            value
        }
        status @ _ => {
            panic!("Attempted to write incorrect flat storage creation status {status:?} for shard {shard_id}");
        }
    };
    store_update.set(FlatStateColumn::Misc.to_db_col(), &creation_status_key(shard_id), &value);
}

pub fn get_flat_storage_creation_status(
    store: &Store,
    shard_id: ShardId,
) -> FlatStorageCreationStatus {
    if !cfg!(feature = "protocol_feature_flat_state") {
        return FlatStorageCreationStatus::DontCreate;
    }

    match get_flat_head(store, shard_id) {
        Some(_) => {
            return FlatStorageCreationStatus::Ready;
        }
        None => {}
    }

    let value = store
        .get(FlatStateColumn::Misc.to_db_col(), &creation_status_key(shard_id))
        .expect("Error reading status from storage");
    match value {
        None => FlatStorageCreationStatus::SavingDeltas,
        Some(bytes) => {
            let mut bytes = bytes.as_slice();
            let status_type = bytes.read_u8().unwrap();
            match status_type {
                FETCHING_STATE => FlatStorageCreationStatus::FetchingState(
                    FetchingStateStatus::try_from_slice(bytes).unwrap(),
                ),
                CATCHING_UP => FlatStorageCreationStatus::CatchingUp(
                    CryptoHash::try_from_slice(bytes).unwrap(),
                ),
                value @ _ => {
                    panic!(
                            "Unexpected value type during getting flat storage creation status: {value}"
                        );
                }
            }
        }
    }
}

pub fn remove_flat_storage_creation_status(store_update: &mut StoreUpdate, shard_id: ShardId) {
    store_update.delete(FlatStateColumn::Misc.to_db_col(), &creation_status_key(shard_id));
}

/// Iterate over flat storage entries for a given shard.
/// It reads data only from the 'main' column - which represents the state as of final head.k
///
/// WARNING: flat storage keeps changing, so the results might be inconsistent, unless you're running
/// this method on the shapshot of the data.
pub fn iter_flat_state_entries<'a>(
    shard_layout: ShardLayout,
    shard_id: u64,
    store: &'a Store,
    from: Option<&'a Vec<u8>>,
    to: Option<&'a Vec<u8>>,
) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a {
    store
        .iter_range(
            FlatStateColumn::State.to_db_col(),
            from.map(|x| x.as_slice()),
            to.map(|x| x.as_slice()),
        )
        .filter_map(move |result| {
            if let Ok((key, value)) = result {
                // Currently all the data in flat storage is 'together' - so we have to parse the key,
                // to see if this element belongs to this shard.
                if let Ok(key_in_shard) = key_belongs_to_shard(&key, &shard_layout, shard_id) {
                    if key_in_shard {
                        return Some((key, value));
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
    let account_id = parse_account_id_from_raw_key(key)
        .map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?
        .ok_or(StorageError::FlatStorageError(format!(
            "Failed to find account id in flat storage key {:?}",
            key
        )))?;
    Ok(account_id_to_shard_id(&account_id, &shard_layout) == shard_id)
}
