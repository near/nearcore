use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::ShardId;
use near_schema_checker_lib::ProtocolSchema;

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

type UIntType = u128;
const UINT_SIZE: usize = std::mem::size_of::<UIntType>();

impl ChunkEndorsementsBitmap {
    pub fn new(num_shards: usize) -> Self {
        Self { inner: Vec::with_capacity(num_shards) }
    }

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
        let encoded_bytes = encoded.to_le_bytes();
        let (compacted_bytes, _) = encoded_bytes.split_at(num_endorsements.div_ceil(8));
        debug_assert_eq!(self.inner.len(), shard_id as usize);
        self.inner.insert(shard_id as usize, compacted_bytes.to_vec());
    }

    pub fn iter(&self, shard_id: ShardId) -> Box<dyn Iterator<Item = bool>> {
        let compacted_bytes = self
            .inner
            .get(shard_id as usize)
            .unwrap_or_else(|| panic!("No endorsements found for shard {}", shard_id));
        Box::new(BitmapIterator::new(compacted_bytes))
    }
}

struct BitmapIterator {
    encoded: UIntType,
    mask: UIntType,
}

impl BitmapIterator {
    fn new(compacted_bytes: &Vec<u8>) -> Self {
        debug_assert!(compacted_bytes.len() <= UINT_SIZE);
        let mut encoded_bytes: [u8; UINT_SIZE] = Default::default();
        encoded_bytes[0..compacted_bytes.len()].copy_from_slice(compacted_bytes.as_slice());
        Self { encoded: UIntType::from_le_bytes(encoded_bytes), mask: 1 as UIntType }
    }
}

impl Iterator for BitmapIterator {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.mask == 0 {
            None
        } else {
            let value = self.encoded & self.mask;
            self.mask <<= 1;
            Some(value > 0)
        }
    }
}

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
            // 1. Endorsements (of num validator seats) from the iterator must match the endorsements specified in the set method.
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
        run_bitmap_test(MAX_ENDORSEMENTS / 2 + 4, MAX_ENDORSEMENTS / 4);
        run_bitmap_test(MAX_ENDORSEMENTS - 4, MAX_ENDORSEMENTS / 2);
        run_bitmap_test(MAX_ENDORSEMENTS, MAX_ENDORSEMENTS / 2);
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
