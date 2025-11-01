//! This module is responsible for storing the latest ChunkStateWitnesses observed
//! by the node. The latest witnesses are stored in the database and can be fetched
//! for analysis and debugging.
//! The number of stored witnesses is limited. When the limit is reached
//! the oldest witness is removed from the database.
//! At the moment this module is used only for debugging purposes.

use std::io::ErrorKind;

use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use near_primitives::types::EpochId;
use near_primitives::types::ShardId;
use near_store::{DBCol, Store};

use crate::ChainStoreAccess;
use crate::stateless_validation;

use super::ChainStore;

use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_schema_checker_lib::ProtocolSchema;
use near_store::db::{INVALID_WITNESSES_INFO, LATEST_WITNESSES_INFO};
use parking_lot::Mutex;
use parking_lot::const_mutex;
use rand::RngCore;
use rand::rngs::OsRng;

/// Maximum size of the latest witnesses stored in the database.
const LATEST_WITNESSES_MAX_SIZE: ByteSize = ByteSize::gb(4);

/// Maximum size of a single latest witness stored in the database.
const SINGLE_LATEST_WITNESS_MAX_SIZE: ByteSize = ByteSize::mb(128);

/// Maximum number of latest witnesses stored in the database.
const LATEST_WITNESSES_MAX_COUNT: u64 = 60 * 30;

/// Maximum size of the invalid witnesses stored in the database.
const INVALID_WITNESSES_MAX_SIZE: ByteSize = ByteSize::gb(4);

/// Maximum size of a single invalid witness stored in the database.
const SINGLE_INVALID_WITNESS_MAX_SIZE: ByteSize = ByteSize::mb(128);

/// Maximum number of invalid witnesses stored in the database.
const INVALID_WITNESSES_MAX_COUNT: u64 = 60 * 30;

/// The same key type is used to store both the latest witnesses
/// and the invalid witnesses. They are kept in different DB columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredWitnessesKey {
    pub height_created: u64,
    pub shard_id: ShardId,
    pub epoch_id: EpochId,
    pub witness_size: u64,
    /// Each witness has a random UUID to ensure that the key is unique.
    /// It allows to store multiple witnesses with the same height and shard_id.
    pub random_uuid: [u8; 16],
}

impl StoredWitnessesKey {
    /// `StoredWitnessesKey` has custom serialization to ensure that the binary representation
    /// starts with big-endian height and shard_id.
    /// This allows to query using a key prefix to find all witnesses for a given height (and shard_id).
    pub fn serialized(&self) -> [u8; 72] {
        let mut result = [0u8; 72];
        result[..8].copy_from_slice(&self.height_created.to_be_bytes());
        result[8..16].copy_from_slice(&self.shard_id.to_be_bytes());
        result[16..48].copy_from_slice(&self.epoch_id.0.0);
        result[48..56].copy_from_slice(&self.witness_size.to_be_bytes());
        result[56..].copy_from_slice(&self.random_uuid);
        result
    }

    pub fn deserialize(data: &[u8]) -> Result<StoredWitnessesKey, std::io::Error> {
        if data.len() != 72 {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Cannot deserialize StoredWitnessesKey, expected 72 bytes, got {}",
                    data.len()
                ),
            ));
        }

        // Data length is known, so it's safe to unwrap the slices, they'll fit.
        Ok(StoredWitnessesKey {
            height_created: u64::from_be_bytes(data[0..8].try_into().unwrap()),
            shard_id: ShardId::new(u64::from_be_bytes(data[8..16].try_into().unwrap())),
            epoch_id: EpochId(CryptoHash(data[16..48].try_into().unwrap())),
            witness_size: u64::from_be_bytes(data[48..56].try_into().unwrap()),
            random_uuid: data[56..].try_into().unwrap(),
        })
    }
}

/// Keeps aggregate information about all witnesses stored in `DBCol::LatestChunkStateWitnesses`.
/// Used for enforcing limits on the number of witnesses stored in the database.
#[derive(
    Debug, Clone, Copy, BorshSerialize, BorshDeserialize, PartialEq, Eq, Default, ProtocolSchema,
)]
pub struct LatestWitnessesInfo {
    pub count: u64,
    pub total_size: u64,
    pub lowest_index: u64,
    pub next_witness_index: u64,
}

impl LatestWitnessesInfo {
    pub fn is_within_limits(&self) -> bool {
        self.count <= LATEST_WITNESSES_MAX_COUNT
            && self.total_size <= LATEST_WITNESSES_MAX_SIZE.as_u64()
    }
}

/// Keeps aggregate information about all witnesses stored in `DBCol::InvalidChunkStateWitnesses`.
/// Used for enforcing limits on the number of witnesses stored in the database.
#[derive(
    Debug, Clone, Copy, BorshSerialize, BorshDeserialize, PartialEq, Eq, Default, ProtocolSchema,
)]
pub struct InvalidWitnessesInfo {
    pub count: u64,
    pub total_size: u64,
    pub lowest_index: u64,
    pub next_witness_index: u64,
}

impl InvalidWitnessesInfo {
    pub fn is_within_limits(&self) -> bool {
        self.count <= INVALID_WITNESSES_MAX_COUNT
            && self.total_size <= INVALID_WITNESSES_MAX_SIZE.as_u64()
    }
}

impl ChainStore {
    /// Saves an observed `ChunkStateWitness` to the database for later analysis and debugging.
    /// The witness is stored in `DBCol::LatestChunkStateWitnesses`.
    /// This function does a read-before-write. It can be called in parallel on the same database
    /// because operations are serialized by an internal lock.
    pub fn save_latest_chunk_state_witness(
        &self,
        witness: &ChunkStateWitness,
    ) -> Result<(), std::io::Error> {
        // Static lock to serialize access and prevent race conditions when multiple threads
        // try to save witnesses concurrently.
        static LOCK: Mutex<()> = const_mutex(());
        let _guard = LOCK.lock();

        let start_time = std::time::Instant::now();
        let ChunkProductionKey { shard_id, epoch_id, height_created } =
            witness.chunk_production_key();
        let _span = tracing::info_span!(target: "client", "save_latest_chunk_state_witness", ?height_created, %shard_id).entered();

        let serialized_witness = borsh::to_vec(witness)?;
        let witness_size: u64 =
            serialized_witness.len().try_into().expect("Cannot convert usize to u64");

        if witness_size > SINGLE_LATEST_WITNESS_MAX_SIZE.as_u64() {
            tracing::warn!(
                "Cannot save latest ChunkStateWitness because it's too big. Witness size: {} byte. Size limit: {} bytes.",
                serialized_witness.len(),
                SINGLE_LATEST_WITNESS_MAX_SIZE.as_u64()
            );
            return Ok(());
        }

        // Read the current `LatestWitnessesInfo`, or create a new one if there is none.
        let mut info = self
            .store()
            .get_ser::<LatestWitnessesInfo>(DBCol::Misc, LATEST_WITNESSES_INFO)?
            .unwrap_or_default();

        let new_witness_index = info.next_witness_index;

        // Adjust the info to include the new witness.
        info.count += 1;
        info.total_size += serialized_witness.len() as u64;
        info.next_witness_index += 1;

        let mut store_update = self.store().store_update();

        // Go over witnesses with increasing indexes and remove them until the limits are satisfied.
        while !info.is_within_limits() && info.lowest_index < info.next_witness_index {
            let store = self.store();
            let key_to_delete = store
                .get(DBCol::LatestWitnessesByIndex, &info.lowest_index.to_be_bytes())?
                .ok_or_else(|| {
                    std::io::Error::new(
                        ErrorKind::NotFound,
                        format!(
                            "Cannot find witness key to delete with index {}",
                            info.lowest_index
                        ),
                    )
                })?;
            // cspell:words deser
            let key_deser = StoredWitnessesKey::deserialize(&key_to_delete)?;

            store_update.delete(DBCol::LatestChunkStateWitnesses, &key_to_delete);
            store_update.delete(DBCol::LatestWitnessesByIndex, &info.lowest_index.to_be_bytes());
            info.lowest_index += 1;
            info.count -= 1;
            info.total_size -= key_deser.witness_size;
        }

        // Limits are ok, insert the new witness.
        let mut random_uuid = [0u8; 16];
        OsRng.fill_bytes(&mut random_uuid);
        let key =
            StoredWitnessesKey { shard_id, epoch_id, height_created, witness_size, random_uuid };
        store_update.set(DBCol::LatestChunkStateWitnesses, &key.serialized(), &serialized_witness);
        store_update.set(
            DBCol::LatestWitnessesByIndex,
            &new_witness_index.to_be_bytes(),
            &key.serialized(),
        );

        // Update LatestWitnessesInfo
        store_update.set(DBCol::Misc, &LATEST_WITNESSES_INFO, &borsh::to_vec(&info)?);

        let store_update_time = start_time.elapsed();

        // Commit the transaction
        store_update.commit()?;

        let store_commit_time = start_time.elapsed().saturating_sub(store_update_time);

        let shard_id_str = shard_id.to_string();
        stateless_validation::metrics::SAVE_LATEST_WITNESS_GENERATE_UPDATE_TIME
            .with_label_values(&[shard_id_str.as_str()])
            .observe(store_update_time.as_secs_f64());
        stateless_validation::metrics::SAVE_LATEST_WITNESS_COMMIT_UPDATE_TIME
            .with_label_values(&[shard_id_str.as_str()])
            .observe(store_commit_time.as_secs_f64());
        stateless_validation::metrics::SAVED_LATEST_WITNESSES_COUNT.set(info.count as i64);
        stateless_validation::metrics::SAVED_LATEST_WITNESSES_SIZE.set(info.total_size as i64);

        tracing::debug!(
            ?store_update_time,
            ?store_commit_time,
            total_count = info.count,
            total_size = info.total_size,
            "Saved latest witness",
        );

        Ok(())
    }

    /// Queries the specified DBCol using a key prefix to find
    /// all saved witnesses that match the given criteria.
    fn get_stored_witnesses(
        &self,
        height: Option<u64>,
        shard_id: Option<u64>,
        epoch_id: Option<EpochId>,
        db_col: DBCol,
    ) -> Result<Vec<ChunkStateWitness>, std::io::Error> {
        let mut key_prefix: Vec<u8> = Vec::new();
        if let Some(h) = height {
            key_prefix.extend_from_slice(&h.to_be_bytes());

            if let Some(id) = shard_id {
                key_prefix.extend_from_slice(&id.to_be_bytes());
            }
        }

        let mut result: Vec<ChunkStateWitness> = Vec::new();

        for read_result in self.store().iter_prefix_ser::<ChunkStateWitness>(db_col, &key_prefix) {
            let (key_bytes, witness) = read_result?;

            let key = StoredWitnessesKey::deserialize(&key_bytes)?;
            if let Some(h) = height {
                if key.height_created != h {
                    continue;
                }
            }
            if let Some(id) = shard_id {
                if key.shard_id != id {
                    continue;
                }
            }
            if let Some(epoch) = &epoch_id {
                if &key.epoch_id != epoch {
                    continue;
                }
            }
            result.push(witness);
        }

        Ok(result)
    }

    /// Fetches observed latest witnesses matching the given criteria.
    pub fn get_latest_witnesses(
        &self,
        height: Option<u64>,
        shard_id: Option<u64>,
        epoch_id: Option<EpochId>,
    ) -> Result<Vec<ChunkStateWitness>, std::io::Error> {
        self.get_stored_witnesses(height, shard_id, epoch_id, DBCol::LatestChunkStateWitnesses)
    }

    /// Fetches observed invalid witnesses matching the given criteria.
    pub fn get_invalid_witnesses(
        &self,
        height: Option<u64>,
        shard_id: Option<u64>,
        epoch_id: Option<EpochId>,
    ) -> Result<Vec<ChunkStateWitness>, std::io::Error> {
        self.get_stored_witnesses(height, shard_id, epoch_id, DBCol::InvalidChunkStateWitnesses)
    }
}

static INVALID_WITNESSES_STORE_UPDATE_LOCK: parking_lot::Mutex<()> = parking_lot::const_mutex(());

/// Saves an observed `ChunkStateWitness` to the database for later analysis and debugging.
/// The witness is stored in `DBCol::InvalidChunkStateWitnesses`.
///
/// This function does a read-before-write. Don't call it in parallel on the same database,
/// or there will be race conditions.
///
/// Unlike the save_latest version, there is a risk of this one being called in parallel
/// because the witness processing is spawned async and we don't know if a witness is invalid
/// until we process it. Hence a mutex is used to guard the store update.
pub fn save_invalid_chunk_state_witness(
    store: Store,
    witness: &ChunkStateWitness,
) -> Result<(), std::io::Error> {
    let start_time = std::time::Instant::now();
    let _span = tracing::info_span!(
        target: "client",
        "save_invalid_chunk_state_witness",
        witness_height = witness.chunk_header().height_created(),
        witness_shard = ?witness.chunk_header().shard_id(),
    )
    .entered();

    let serialized_witness = borsh::to_vec(witness)?;
    let serialized_witness_size: u64 =
        serialized_witness.len().try_into().expect("Cannot convert usize to u64");

    if serialized_witness_size > SINGLE_INVALID_WITNESS_MAX_SIZE.as_u64() {
        tracing::warn!(
            "Cannot save invalid ChunkStateWitness because it's too big. Witness size: {} byte. Size limit: {} bytes.",
            serialized_witness.len(),
            SINGLE_INVALID_WITNESS_MAX_SIZE.as_u64()
        );
        return Ok(());
    }

    // Read the current `InvalidWitnessesInfo`, or create a new one if there is none.
    let mut info = store
        .get_ser::<InvalidWitnessesInfo>(DBCol::Misc, INVALID_WITNESSES_INFO)?
        .unwrap_or_default();

    let new_witness_index = info.next_witness_index;

    // Adjust the info to include the new witness.
    info.count += 1;
    info.total_size += serialized_witness.len() as u64;
    info.next_witness_index += 1;

    let (store_update_time, store_commit_time) = {
        let _guard = INVALID_WITNESSES_STORE_UPDATE_LOCK.lock();

        let mut store_update = store.store_update();

        // Go over witnesses with increasing indexes and remove them until the limits are satisfied.
        while !info.is_within_limits() && info.lowest_index < info.next_witness_index {
            let key_to_delete = store
                .get(DBCol::InvalidWitnessesByIndex, &info.lowest_index.to_be_bytes())?
                .ok_or_else(|| {
                    std::io::Error::new(
                        ErrorKind::NotFound,
                        format!(
                            "Cannot find witness key to delete with index {}",
                            info.lowest_index
                        ),
                    )
                })?;
            // cspell:words deser
            let key_deser = StoredWitnessesKey::deserialize(&key_to_delete)?;

            store_update.delete(DBCol::InvalidChunkStateWitnesses, &key_to_delete);
            store_update.delete(DBCol::InvalidWitnessesByIndex, &info.lowest_index.to_be_bytes());
            info.lowest_index += 1;
            info.count -= 1;
            info.total_size -= key_deser.witness_size;
        }

        // Limits are ok, insert the new witness.
        let mut random_uuid = [0u8; 16];
        OsRng.fill_bytes(&mut random_uuid);
        let key = StoredWitnessesKey {
            height_created: witness.chunk_header().height_created(),
            shard_id: witness.chunk_header().shard_id().into(),
            epoch_id: *witness.epoch_id(),
            witness_size: serialized_witness_size,
            random_uuid,
        };
        store_update.set(DBCol::InvalidChunkStateWitnesses, &key.serialized(), &serialized_witness);
        store_update.set(
            DBCol::InvalidWitnessesByIndex,
            &new_witness_index.to_be_bytes(),
            &key.serialized(),
        );

        // Update InvalidWitnessesInfo
        store_update.set(DBCol::Misc, &INVALID_WITNESSES_INFO, &borsh::to_vec(&info)?);

        let store_update_time = start_time.elapsed();

        // Commit the transaction
        store_update.commit()?;

        let store_commit_time = start_time.elapsed().saturating_sub(store_update_time);

        (store_update_time, store_commit_time)
    };

    let shard_id_str = witness.chunk_header().shard_id().to_string();
    stateless_validation::metrics::SAVE_INVALID_WITNESS_GENERATE_UPDATE_TIME
        .with_label_values(&[shard_id_str.as_str()])
        .observe(store_update_time.as_secs_f64());
    stateless_validation::metrics::SAVE_INVALID_WITNESS_COMMIT_UPDATE_TIME
        .with_label_values(&[shard_id_str.as_str()])
        .observe(store_commit_time.as_secs_f64());
    stateless_validation::metrics::SAVED_INVALID_WITNESSES_COUNT.set(info.count as i64);
    stateless_validation::metrics::SAVED_INVALID_WITNESSES_SIZE.set(info.total_size as i64);

    tracing::debug!(
        ?store_update_time,
        ?store_commit_time,
        total_count = info.count,
        total_size = info.total_size,
        "Saved invalid witness",
    );

    Ok(())
}
