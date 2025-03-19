use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{BlockHeight, Gas, ProtocolVersion, ShardId};

use crate::bandwidth_scheduler::BandwidthRequests;
use crate::congestion_info::CongestionInfo;
use crate::sharding::{EncodedShardChunk, ShardChunk};
use crate::types::StateRoot;
use crate::validator_signer::EmptyValidatorSigner;

type ShardChunkReedSolomon = reed_solomon_erasure::galois_8::ReedSolomon;

/// The shard_ids, state_roots and congestion_infos must be in the same order.
pub fn genesis_chunks(
    state_roots: Vec<StateRoot>,
    congestion_infos: Vec<Option<CongestionInfo>>,
    shard_ids: &[ShardId],
    initial_gas_limit: Gas,
    genesis_height: BlockHeight,
    genesis_protocol_version: ProtocolVersion,
) -> Vec<ShardChunk> {
    let rs = ShardChunkReedSolomon::new(1, 2).unwrap();
    let state_roots = if state_roots.len() == shard_ids.len() {
        state_roots
    } else {
        assert_eq!(state_roots.len(), 1);
        std::iter::repeat(state_roots[0]).take(shard_ids.len()).collect()
    };

    let mut chunks = vec![];

    let num = shard_ids.len();
    assert_eq!(state_roots.len(), num);

    for (shard_index, &shard_id) in shard_ids.iter().enumerate() {
        let state_root = state_roots[shard_index];
        let congestion_info = congestion_infos[shard_index];

        let encoded_chunk = genesis_chunk(
            &rs,
            genesis_protocol_version,
            genesis_height,
            initial_gas_limit,
            shard_id,
            state_root,
            congestion_info,
        );
        let mut chunk = encoded_chunk.decode_chunk(1).expect("Failed to decode genesis chunk");
        chunk.set_height_included(genesis_height);
        chunks.push(chunk);
    }

    chunks
}

// Creates the genesis encoded shard chunk. The genesis chunks have most of the
// fields set to defaults. The remaining fields are set to the provided values.
fn genesis_chunk(
    rs: &ShardChunkReedSolomon,
    genesis_protocol_version: u32,
    genesis_height: u64,
    initial_gas_limit: u64,
    shard_id: ShardId,
    state_root: CryptoHash,
    congestion_info: Option<CongestionInfo>,
) -> EncodedShardChunk {
    let (encoded_chunk, _) = EncodedShardChunk::new(
        CryptoHash::default(),
        state_root,
        CryptoHash::default(),
        genesis_height,
        shard_id,
        rs,
        0,
        initial_gas_limit,
        0,
        CryptoHash::default(),
        vec![],
        vec![],
        &[],
        CryptoHash::default(),
        congestion_info,
        BandwidthRequests::default_for_protocol_version(genesis_protocol_version),
        &EmptyValidatorSigner::default().into(),
        genesis_protocol_version,
    )
    .expect("Failed to decode genesis chunk");
    encoded_chunk
}
