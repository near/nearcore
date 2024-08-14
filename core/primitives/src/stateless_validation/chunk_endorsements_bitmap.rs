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
#[borsh(init=init)]
pub struct ChunkEndorsementsBitmap {
    inner: Vec<Vec<u8>>,
}

/// Type of unsigned integer used in the internal implementation.
///
/// The implementation of the bitmap uses an unsigned integer type whose length-in-bits is greater than
/// the number of chunk validator seats. The number of seats as of the the first implementation was 68,
/// thus we use u128 for now, but it may change in the future. This does not break the representation
/// because we do not store the extra bytes when serializing. Instead, we discard the extra bytes for
/// the positions that are more than the number of validator assignments (which is deterministic per block height).
/// This representation allows simple implementation for setting and iterating over the bits.
///
/// More specifically, if the endorsement from the validator at the n^th position is received, we set the n^th bit
/// in the integer to 1 (0 otherwise). Then we take the bytes of the integer in little-endian representation and store
/// only the bytes needed to represent the assignments. For example, if there are 68 validator seats but 20 assignments,
/// `20.div_ceil(8) = 4` bytes (24 bits) are sufficient to represent the endorsements, so we will store the first 4 bytes
/// of the little-endian representation.
type UIntType = u128;
const UINT_SIZE: usize = std::mem::size_of::<UIntType>();

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

    /// Performs sanity check that the size of the unsigned int used for the internal implementation
    /// is sufficient to store the endorsements encoded in the deserialized bytes.
    pub fn init(&mut self) {
        for encoded_bytes in self.inner.iter() {
            let num_bytes = encoded_bytes.len();
            debug_assert!(
                num_bytes <= UINT_SIZE,
                "Expected size of endorsements bitmap less than {} bytes but found {} bytes",
                UINT_SIZE,
                num_bytes,
            );
        }
    }

    /// Adds the provided endorsements to the bitmap for the specified shard.
    pub fn add_endorsements(&mut self, shard_id: ShardId, endorsements: Vec<bool>) {
        let num_endorsements = endorsements.len();
        debug_assert!(
            num_endorsements <= UINT_SIZE * 8,
            "Expected number of endorsements less than {} but found {}",
            UINT_SIZE * 8,
            num_endorsements,
        );
        let mut encoded: UIntType = 0;
        let mut mask: UIntType = 1;
        for produced in endorsements.into_iter() {
            if produced {
                encoded |= mask;
            }
            mask <<= 1;
        }
        // Take the first n bytes of the little endian representation of the integer
        // where n bytes are sufficient to contain all the assignment positions.
        let encoded_bytes = encoded.to_le_bytes();
        let (compacted_bytes, _) = encoded_bytes.split_at(num_endorsements.div_ceil(8));
        self.inner[shard_id as usize] = compacted_bytes.to_vec();
    }

    pub fn iter(&self, shard_id: ShardId) -> Box<dyn Iterator<Item = bool>> {
        let compacted_bytes = self
            .inner
            .get(shard_id as usize)
            .unwrap_or_else(|| panic!("No endorsements found for shard {}", shard_id));
        Box::new(BitmapIterator::new(compacted_bytes))
    }
}

/// Iterates over the endorsements for the validator assignments.
///
/// Returns true if the endorsement is received from the validator assigned at the next position,
/// and false otherwise.
struct BitmapIterator {
    encoded: UIntType,
    mask: UIntType,
}

impl BitmapIterator {
    fn new(compacted_bytes: &Vec<u8>) -> Self {
        debug_assert!(compacted_bytes.len() <= UINT_SIZE);
        let mut encoded_bytes: [u8; UINT_SIZE] = Default::default();
        encoded_bytes[0..compacted_bytes.len()].copy_from_slice(compacted_bytes.as_slice());
        Self { encoded: UIntType::from_le_bytes(encoded_bytes), mask: 1 }
    }
}

impl Iterator for BitmapIterator {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        // Once we shift the mask all the way to the leftmost position, it becomes 0.
        if self.mask == 0 {
            None
        } else {
            let value = self.encoded & self.mask;
            self.mask <<= 1;
            Some(value > 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ChunkEndorsementsBitmap;
    use crate::stateless_validation::chunk_endorsements_bitmap::UINT_SIZE;
    use itertools::Itertools;
    use near_primitives_core::types::ShardId;
    use rand::Rng;

    const NUM_SHARDS: usize = 4;
    const MAX_ENDORSEMENTS: usize = UINT_SIZE * 8;

    fn run_bitmap_test(num_assignments: usize, num_produced: usize) {
        let assert_bitmap = |bitmap: &ChunkEndorsementsBitmap,
                             expected_endorsements: &Vec<Vec<bool>>| {
            // 1. Endorsements from the bitmap iterator must match the endorsements given previously.
            for (shard_id, endorsements) in expected_endorsements.iter().enumerate() {
                assert_eq!(
                    bitmap.iter(shard_id as ShardId).take(num_assignments).collect_vec(),
                    *endorsements
                );
            }
            // 2. All remaining bits after the endorsement bits must be 0.
            for shard_id in 0..NUM_SHARDS {
                assert_eq!(
                    bitmap.iter(shard_id as ShardId).skip(num_assignments).collect_vec(),
                    std::iter::repeat(false).take(UINT_SIZE * 8 - num_assignments).collect_vec()
                );
            }
        };

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
        assert_bitmap(&bitmap, &expected_endorsements);

        // Check after serialization.
        let serialized = borsh::to_vec(&bitmap).unwrap();
        let bitmap = borsh::from_slice::<ChunkEndorsementsBitmap>(&serialized).unwrap();
        assert_bitmap(&bitmap, &expected_endorsements);
    }

    #[test]
    fn test_chunk_endorsements_bitmap() {
        run_bitmap_test(0, 0);
        run_bitmap_test(MAX_ENDORSEMENTS / 2 + 4, MAX_ENDORSEMENTS / 4);
        run_bitmap_test(MAX_ENDORSEMENTS - 4, MAX_ENDORSEMENTS / 2);
        run_bitmap_test(MAX_ENDORSEMENTS, MAX_ENDORSEMENTS / 2);
        run_bitmap_test(MAX_ENDORSEMENTS, 0);
    }

    #[test]
    fn test_chunk_endorsements_bitmap_size_insufficient() {
        assert!(std::panic::catch_unwind(|| {
            let mut bitmap = ChunkEndorsementsBitmap::new(NUM_SHARDS);
            bitmap.add_endorsements(
                0 as ShardId,
                std::iter::repeat(true).take(MAX_ENDORSEMENTS + 1).collect_vec(),
            )
        })
        .is_err());
    }
}
