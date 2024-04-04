use borsh::BorshDeserialize;
use lru::LruCache;

use near_async::messaging::Sender;
use near_network::client::ChunkStateWitnessMessage;
use near_primitives::sharding::{ChunkHash, ReedSolomonWrapper};
use near_primitives::stateless_validation::{ChunkStateWitness, PartialEncodedStateWitness};

const NUM_CHUNKS_IN_WITNESS_MANAGER_CACHE: usize = 200;

pub struct StateWitnessManager {
    client_adapter: Sender<ChunkStateWitnessMessage>,
    parts_cache: LruCache<ChunkHash, CacheEntry>,
}

struct CacheEntry {
    processed: bool,
    num_parts: usize,
    parts: Vec<Option<Box<[u8]>>>,
}

impl CacheEntry {
    pub fn new(witness: &PartialEncodedStateWitness) -> Self {
        let num_parts = witness.forward_accounts.len();
        Self { processed: false, num_parts: 0, parts: vec![None; num_parts] }
    }

    pub fn can_process(&self) -> bool {
        !self.processed && self.num_parts >= self.parts.len() * 2 / 3
    }
}

impl StateWitnessManager {
    pub fn new(client_adapter: Sender<ChunkStateWitnessMessage>) -> Self {
        Self { client_adapter, parts_cache: LruCache::new(NUM_CHUNKS_IN_WITNESS_MANAGER_CACHE) }
    }

    pub fn track_partial_encoded_state_witness(
        &mut self,
        partial_witness: PartialEncodedStateWitness,
    ) {
        self.parts_cache
            .get_or_insert(partial_witness.chunk_hash.clone(), || CacheEntry::new(&partial_witness))
            .unwrap();
        let entry = self.parts_cache.get_mut(&partial_witness.chunk_hash).unwrap();
        if entry.parts[partial_witness.part_ord].is_none() {
            entry.num_parts += 1;
            entry.parts[partial_witness.part_ord] = Some(partial_witness.part);
        }
        if let Some(witness) = reconstruct_state_witness(
            partial_witness.chunk_hash,
            partial_witness.witness_size,
            entry,
        ) {
            self.client_adapter.send(ChunkStateWitnessMessage(witness));
        }
    }
}

fn reconstruct_state_witness(
    chunk_hash: ChunkHash,
    witness_size: usize,
    entry: &mut CacheEntry,
) -> Option<ChunkStateWitness> {
    if !entry.can_process() {
        return None;
    }
    entry.processed = true;

    let total_parts = entry.parts.len();
    let data_parts = total_parts * 2 / 3;
    let mut rs = ReedSolomonWrapper::new(data_parts, total_parts - data_parts);
    rs.reconstruct(entry.parts.as_mut_slice()).unwrap();

    // Reconstruct the witness from the parts and return it.
    let encoded_data = entry
        .parts
        .iter()
        .flat_map(|option| option.as_ref().unwrap().iter())
        .cloned()
        .take(witness_size)
        .collect::<Vec<u8>>();
    Some(ChunkStateWitness::try_from_slice(&encoded_data).unwrap())
}
