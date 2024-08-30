use bitvec::prelude::*;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::ShardId;
use near_schema_checker_lib::ProtocolSchema;

/// Represents a collection of bitmaps, one per shard, to store whether the endorsements from the chunk validators has been received.
///
/// For each shard, the endorsements are encoded as a sequence of bits: 1 means endorsement received and 0 means not received.
/// While the number of chunk validator seats is fixed, the number of chunk-validator assignments may be smaller and may change,
/// since the seats are assigned to validators weighted by their stake. Thus, we represent the bits as a vector of bytes.
/// The number of assignments may be less or equal to the number of total bytes. This representation allows increasing
/// the chunk validator seats in the future (which will be represented by a vector of greater length).
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Eq,
    PartialEq,
    Default,
    ProtocolSchema,
)]
pub struct ChunkEndorsementsBitmap {
    inner: Vec<Vec<u8>>,
}

/// Type of BitVec used in the internal implementation.
type BitVecType = BitVec<u8, Lsb0>;

impl ChunkEndorsementsBitmap {
    /// Creates an empty endorsement bitmap for each shard.
    pub fn new(num_shards: usize) -> Self {
        Self { inner: vec![Default::default(); num_shards] }
    }

    // Creates an endorsement bitmap for all the shards.
    pub fn from_endorsements(shards_to_endorsements: Vec<Vec<bool>>) -> Self {
        let mut bitmap = ChunkEndorsementsBitmap::new(shards_to_endorsements.len());
        for (shard_id, endorsements) in shards_to_endorsements.into_iter().enumerate() {
            bitmap.add_endorsements(shard_id as ShardId, endorsements);
        }
        bitmap
    }

    /// Adds the provided endorsements to the bitmap for the specified shard.
    pub fn add_endorsements(&mut self, shard_id: ShardId, endorsements: Vec<bool>) {
        let bitvec: BitVecType = endorsements.iter().collect();
        self.inner[shard_id as usize] = bitvec.into();
    }

    /// Returns an iterator over the endorsements (yields true if the endorsement for the respective position was received).
    pub fn iter(&self, shard_id: ShardId) -> Box<dyn Iterator<Item = bool>> {
        let bitvec = BitVecType::from_vec(self.inner[shard_id as usize].clone());
        Box::new(bitvec.into_iter())
    }

    /// Returns the number of shards in the endorsements.
    pub fn num_shards(&self) -> usize {
        self.inner.len()
    }

    /// Returns the full length of the bitmap for a given shard.
    /// Note that the size may be greater than the number of validator assignments.
    pub fn len(&self, shard_id: ShardId) -> Option<usize> {
        self.inner.get(shard_id as usize).map(|v| v.len())
    }
}

#[cfg(test)]
mod tests {
    use super::ChunkEndorsementsBitmap;
    use itertools::Itertools;
    use near_primitives_core::types::ShardId;
    use rand::Rng;

    const NUM_SHARDS: usize = 4;

    /// Asserts that the bitmap encodes the expected endorsements and zeros for the other bits.
    fn assert_bitmap(
        bitmap: &ChunkEndorsementsBitmap,
        num_assignments: usize,
        expected_endorsements: &Vec<Vec<bool>>,
    ) {
        // Endorsements from the bitmap iterator must match the endorsements given previously.
        for (shard_id, endorsements) in expected_endorsements.iter().enumerate() {
            // 1. Endorsements from the bitmap iterator must match the endorsements given previously.
            assert_eq!(
                bitmap.iter(shard_id as ShardId).take(num_assignments).collect_vec(),
                *endorsements
            );
            // 2. All remaining bits after the endorsement bits must be 0.
            for value in bitmap.iter(shard_id as ShardId).skip(num_assignments) {
                assert_eq!(value, false);
            }
        }
    }

    fn run_bitmap_test(num_assignments: usize, num_produced: usize) {
        let mut rng = rand::thread_rng();
        let mut bitmap = ChunkEndorsementsBitmap::new(NUM_SHARDS);
        let mut expected_endorsements = vec![];
        for shard_id in 0..NUM_SHARDS {
            let mut endorsements = vec![false; num_assignments];
            for _ in 0..num_produced {
                endorsements[rng.gen_range(0..num_assignments)] = true;
            }
            expected_endorsements.push(endorsements.clone());
            bitmap.add_endorsements(shard_id as ShardId, endorsements);
        }
        // Check before serialization.
        assert_bitmap(&bitmap, num_assignments, &expected_endorsements);

        // Check after serialization.
        let serialized = borsh::to_vec(&bitmap).unwrap();
        let bitmap = borsh::from_slice::<ChunkEndorsementsBitmap>(&serialized).unwrap();
        assert_bitmap(&bitmap, num_assignments, &expected_endorsements);
    }

    #[test]
    fn test_chunk_endorsements_bitmap() {
        run_bitmap_test(0, 0);
        run_bitmap_test(60, 20);
        run_bitmap_test(68, 20);
        run_bitmap_test(125, 30);
        run_bitmap_test(130, 50);
        run_bitmap_test(300, 100);
    }
}
