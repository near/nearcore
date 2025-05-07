use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{BlockHeight, Gas, ProtocolVersion, ShardId};
use near_primitives_core::version::PROD_GENESIS_PROTOCOL_VERSION;

use crate::bandwidth_scheduler::BandwidthRequests;
use crate::congestion_info::CongestionInfo;
use crate::reed_solomon::reed_solomon_encode;
use crate::sharding::{
    EncodedShardChunk, EncodedShardChunkBody, ShardChunk, ShardChunkHeaderV1, ShardChunkV1,
    ShardChunkWithEncoding, TransactionReceipt,
};
use crate::types::StateRoot;
use crate::validator_signer::EmptyValidatorSigner;

type ShardChunkReedSolomon = reed_solomon_erasure::galois_8::ReedSolomon;

/// The shard_ids, state_roots and congestion_infos must be in the same order.
pub fn genesis_chunks(
    state_roots: Vec<StateRoot>,
    congestion_infos: Vec<CongestionInfo>,
    shard_ids: &[ShardId],
    initial_gas_limit: Gas,
    genesis_height: BlockHeight,
    genesis_protocol_version: ProtocolVersion,
) -> Vec<ShardChunk> {
    assert!(genesis_protocol_version > PROD_GENESIS_PROTOCOL_VERSION);
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
            genesis_height,
            initial_gas_limit,
            shard_id,
            state_root,
            congestion_info,
        );
        let mut chunk = encoded_chunk.decode_chunk().expect("Failed to decode genesis chunk");
        chunk.set_height_included(genesis_height);
        chunks.push(chunk);
    }

    chunks
}

// Creates the genesis encoded shard chunk. The genesis chunks have most of the
// fields set to defaults. The remaining fields are set to the provided values.
fn genesis_chunk(
    rs: &ShardChunkReedSolomon,
    genesis_height: u64,
    initial_gas_limit: u64,
    shard_id: ShardId,
    state_root: CryptoHash,
    congestion_info: CongestionInfo,
) -> EncodedShardChunk {
    let (chunk, _) = ShardChunkWithEncoding::new(
        CryptoHash::default(),
        state_root,
        CryptoHash::default(),
        genesis_height,
        shard_id,
        0,
        initial_gas_limit,
        0,
        vec![],
        vec![],
        vec![],
        CryptoHash::default(),
        CryptoHash::default(),
        congestion_info,
        BandwidthRequests::empty(),
        &EmptyValidatorSigner::default().into(),
        rs,
    );
    chunk.into_parts().1
}

pub fn prod_genesis_chunks(
    state_roots: Vec<StateRoot>,
    shard_ids: &[ShardId],
    initial_gas_limit: Gas,
    genesis_height: BlockHeight,
) -> Vec<ShardChunk> {
    assert_eq!(state_roots.len(), 1);
    assert_eq!(shard_ids.len(), 1);

    let rs = ShardChunkReedSolomon::new(1, 2).unwrap();
    let (transaction_receipts_parts, encoded_length) =
        reed_solomon_encode(&rs, &TransactionReceipt(vec![], vec![]));
    let content = EncodedShardChunkBody { parts: transaction_receipts_parts };
    let (encoded_merkle_root, _) = content.get_merkle_hash_and_paths();

    let header = ShardChunkHeaderV1::new(
        CryptoHash::default(),
        state_roots[0],
        CryptoHash::default(),
        encoded_merkle_root,
        encoded_length as u64,
        genesis_height,
        shard_ids[0],
        0,
        initial_gas_limit,
        0,
        CryptoHash::default(),
        CryptoHash::default(),
        vec![],
        &EmptyValidatorSigner::default().into(),
    );

    let mut chunk = ShardChunk::V1(ShardChunkV1 {
        chunk_hash: header.chunk_hash(),
        header,
        transactions: vec![],
        prev_outgoing_receipts: vec![],
    });
    chunk.set_height_included(genesis_height);

    vec![chunk]
}
