use crate::sharding::ShardChunkHeader;
use crate::types::{AccountId, EpochId};
use borsh::BorshDeserialize;
use std::io::Cursor;
use std::sync::{Arc, OnceLock};

use super::{ChunkProductionKey, state_witness::ChunkStateWitness};

/// LazyChunkStateWitness provides immediate access to header fields while
/// deserializing heavy data in the background. When heavy data is first needed,
/// consume this witness to get a normal ChunkStateWitness.
pub struct LazyChunkStateWitness {
    // Light fields - available immediately
    epoch_id: EpochId,
    chunk_header: ShardChunkHeader,

    // Complete witness data - loaded lazily via background thread
    complete_witness: Arc<OnceLock<ChunkStateWitness>>,
}

impl LazyChunkStateWitness {
    /// Creates a LazyChunkStateWitness from serialized bytes with header-first parsing.
    /// Header fields (epoch_id, chunk_header) are available immediately.
    /// Complete witness is loaded asynchronously in the background via rayon.
    pub fn from_bytes(data: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut cursor = Cursor::new(data);

        let variant: u8 = BorshDeserialize::deserialize_reader(&mut cursor)?;

        let (epoch_id, chunk_header) = match variant {
            0 => {
                // V1: chunk_producer, epoch_id, chunk_header, then heavy data
                let _chunk_producer: AccountId = BorshDeserialize::deserialize_reader(&mut cursor)?;
                let epoch_id: EpochId = BorshDeserialize::deserialize_reader(&mut cursor)?;
                let chunk_header: ShardChunkHeader =
                    BorshDeserialize::deserialize_reader(&mut cursor)?;
                (epoch_id, chunk_header)
            }
            1 => {
                // V2: epoch_id, chunk_header, then heavy data
                let epoch_id: EpochId = BorshDeserialize::deserialize_reader(&mut cursor)?;
                let chunk_header: ShardChunkHeader =
                    BorshDeserialize::deserialize_reader(&mut cursor)?;
                (epoch_id, chunk_header)
            }
            _ => return Err(format!("Unknown ChunkStateWitness variant: {}", variant).into()),
        };

        let complete_witness = Arc::new(OnceLock::new());
        let complete_witness_clone = complete_witness.clone();

        // Spawn rayon task to deserialize complete witness in background
        let data_shared: Arc<[u8]> = Arc::from(data);
        rayon::spawn(move || {
            if let Ok(witness) = Self::deserialize_complete_witness(&data_shared) {
                let _ = complete_witness_clone.set(witness);
            }
        });

        Ok(LazyChunkStateWitness { epoch_id, chunk_header, complete_witness })
    }

    /// Deserialize the complete witness from serialized bytes
    fn deserialize_complete_witness(
        data: &[u8],
    ) -> Result<ChunkStateWitness, Box<dyn std::error::Error + Send + Sync>> {
        // TODO: could be optimized to skip header re-parsing
        let witness: ChunkStateWitness = borsh::from_slice(data)?;
        Ok(witness)
    }

    /// Check if the complete witness is ready without blocking
    pub fn is_ready(&self) -> bool {
        self.complete_witness.get().is_some()
    }

    /// Consume this lazy witness to get a complete ChunkStateWitness.
    /// This will block if the background deserialization is not yet complete.
    pub fn into_chunk_state_witness(self) -> ChunkStateWitness {
        // Wait for background thread to complete and get the witness
        loop {
            if let Some(_) = self.complete_witness.get() {
                // Try to extract the witness from the Arc if we're the only owner
                match Arc::try_unwrap(self.complete_witness) {
                    Ok(once_lock) => {
                        return once_lock.into_inner().expect("OnceLock should be filled");
                    }
                    Err(arc) => {
                        return arc.get().expect("OnceLock should be filled").clone();
                    }
                }
            }
            // If not ready, yield and retry
            // TODO: could be improved with condvar for more efficient waiting
            std::thread::yield_now();
        }
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        ChunkProductionKey {
            shard_id: self.chunk_header.shard_id(),
            epoch_id: self.epoch_id,
            height_created: self.chunk_header.height_created(),
        }
    }

    pub fn epoch_id(&self) -> &EpochId {
        &self.epoch_id
    }

    pub fn chunk_header(&self) -> &ShardChunkHeader {
        &self.chunk_header
    }
}

impl std::fmt::Debug for LazyChunkStateWitness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyChunkStateWitness")
            .field("epoch_id", &self.epoch_id)
            .field("chunk_header", &self.chunk_header)
            .field("is_ready", &self.is_ready())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stateless_validation::state_witness::ChunkStateWitness;

    #[test]
    fn test_lazy_header_access() {
        // TODO
    }

    #[test]
    fn test_witness() {
        // TODO
    }
}
