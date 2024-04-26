//! This module is responsible for storing the latest ChunkStateWitnesses observed
//! by the node. The latest witnesses are stored in the database and can be fetched
//! for analysis and debugging.
//! The number of stored witnesses is limited. When the limit is reached
//! the witness with the oldest height is removed from the database.
//! At the moment this module is used only for debugging purposes.

use std::io::ErrorKind;

use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::ChunkStateWitness;
use near_primitives::types::EpochId;
use near_store::DBCol;

use crate::ChainStoreAccess;

use super::ChainStore;

use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_store::db::LATEST_WITNESSES_INFO;
use rand::rngs::OsRng;
use rand::RngCore;

/// Maximum size of the latest witnesses stored in the database.
const LATEST_WITNESSES_MAX_SIZE: ByteSize = ByteSize::gb(4);

/// Maximum size of a single latest witness stored in the database.
const SINGLE_LATEST_WITNESS_MAX_SIZE: ByteSize = ByteSize::mb(128);

/// Maximum number of latest witnesses stored in the database.
const LATEST_WITNESSES_MAX_COUNT: u64 = 60 * 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestWitnessesKey {
    pub height: u64,
    pub shard_id: u64,
    pub epoch_id: EpochId,
    /// Each witness has a random UUID to ensure that the key is unique.
    /// It allows to store multiple witnesses with the same height and shard_id.
    pub random_uuid: [u8; 16],
}

impl LatestWitnessesKey {
    /// `LatestWitnessesKey` has custom serialization to ensure that the binary representation
    /// starts with big-endian height and shard_id.
    /// This allows to query using a key prefix to find all witnesses for a given height (and shard_id).
    pub fn serialized(&self) -> [u8; 64] {
        let mut result = [0u8; 64];
        result[..8].copy_from_slice(&self.height.to_be_bytes());
        result[8..16].copy_from_slice(&self.shard_id.to_be_bytes());
        result[16..48].copy_from_slice(&self.epoch_id.0 .0);
        result[48..].copy_from_slice(&self.random_uuid);
        result
    }

    pub fn deserialize(data: &[u8]) -> Result<LatestWitnessesKey, std::io::Error> {
        if data.len() != 64 {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Cannot deserialize LatestWitnessesKey, expected 64 bytes, got {}",
                    data.len()
                ),
            ));
        }

        // Data length is known, so it's safe to unwrap the slices, they'll fit.
        Ok(LatestWitnessesKey {
            height: u64::from_be_bytes(data[0..8].try_into().unwrap()),
            shard_id: u64::from_be_bytes(data[8..16].try_into().unwrap()),
            epoch_id: EpochId(CryptoHash(data[16..48].try_into().unwrap())),
            random_uuid: data[48..].try_into().unwrap(),
        })
    }
}

/// Keeps aggregate information about all witnesses stored in `DBCol::LatestChunkStateWitnesses`.
/// Used for enforcing limits on the number of witnesses stored in the database.
#[derive(Debug, Clone, Copy, BorshSerialize, BorshDeserialize, PartialEq, Eq, Default)]
struct LatestWitnessesInfo {
    pub count: u64,
    pub total_size: u64,
}

impl LatestWitnessesInfo {
    pub fn is_within_limits(&self) -> bool {
        self.count <= LATEST_WITNESSES_MAX_COUNT
            && self.total_size <= LATEST_WITNESSES_MAX_SIZE.as_u64()
    }
}

impl ChainStore {
    /// Saves an observed `ChunkStateWitness` to the database for later analysis and debugging.
    /// The witness is stored in `DBCol::LatestChunkStateWitnesses`.
    /// This function does a read-before-write. Don't call it in parallel on the same database,
    /// or there will be race conditions.
    pub fn save_latest_chunk_state_witness(
        &mut self,
        witness: &ChunkStateWitness,
    ) -> Result<(), std::io::Error> {
        let serialized_witness = borsh::to_vec(witness)?;
        let serialized_witness_size: u64 =
            serialized_witness.len().try_into().expect("Cannot convert usize to u64");

        if serialized_witness_size > SINGLE_LATEST_WITNESS_MAX_SIZE.as_u64() {
            tracing::warn!(
                "Cannot save latest ChunkStateWitness because it's too big. Witness size: {} byte. Size limit: {} bytes.",
                serialized_witness.len(),
                SINGLE_LATEST_WITNESS_MAX_SIZE.as_u64()
            );
        }

        // Read the current `LatestWitnessesInfo`, or create a new one if there is none.
        let mut info = self
            .store()
            .get_ser::<LatestWitnessesInfo>(DBCol::Misc, LATEST_WITNESSES_INFO)?
            .unwrap_or_default();

        // Adjust the info to include the new witness.
        info.count += 1;
        info.total_size += serialized_witness.len() as u64;

        // Go over witnesses with increasing (height, shard_id) and remove them until the limits are satisfied.
        // Height and shard id are stored in big-endian representation, so sorting the binary representation is
        // the same as sorting the integers.
        let mut store_update = self.store().store_update();
        for item in self.store().iter(DBCol::LatestChunkStateWitnesses) {
            if info.is_within_limits() {
                break;
            }

            let (key_bytes, witness_bytes) = item?;
            store_update.delete(DBCol::LatestChunkStateWitnesses, &key_bytes);
            info.count -= 1;
            info.total_size -= witness_bytes.len() as u64;
        }

        // Limits are ok, insert the new witness.
        let mut random_uuid = [0u8; 16];
        OsRng.fill_bytes(&mut random_uuid);
        let key = LatestWitnessesKey {
            height: witness.chunk_header.height_created(),
            shard_id: witness.chunk_header.shard_id(),
            epoch_id: witness.epoch_id.clone(),
            random_uuid,
        };
        store_update.set(DBCol::LatestChunkStateWitnesses, &key.serialized(), &serialized_witness);

        // Update LatestWitnessesInfo
        store_update.set(DBCol::Misc, &LATEST_WITNESSES_INFO, &borsh::to_vec(&info)?);

        // Commit the transaction
        store_update.commit()?;

        Ok(())
    }

    /// Fetch observed witnesses with the given height, and optionally shard_id.
    /// Queries `DBCol::LatestChunkStateWitnesses` using a key prefix to find all witnesses that match the criteria.
    pub fn get_latest_witnesses(
        &self,
        height: Option<u64>,
        shard_id: Option<u64>,
        epoch_id: Option<EpochId>,
    ) -> Result<Vec<ChunkStateWitness>, std::io::Error> {
        let mut key_prefix: Vec<u8> = Vec::new();
        if let Some(h) = height {
            key_prefix.extend_from_slice(&h.to_be_bytes());

            if let Some(id) = shard_id {
                key_prefix.extend_from_slice(&id.to_be_bytes());
            }
        }

        let mut result: Vec<ChunkStateWitness> = Vec::new();

        for read_result in self
            .store()
            .iter_prefix_ser::<ChunkStateWitness>(DBCol::LatestChunkStateWitnesses, &key_prefix)
        {
            let (key_bytes, witness) = read_result?;

            let key = LatestWitnessesKey::deserialize(&key_bytes)?;
            if let Some(h) = height {
                if key.height != h {
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
}
