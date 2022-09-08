use near_chain::{ChainGenesis, ChainStoreAccess};
use near_chunks::ShardsManager;
use near_client::test_utils::TestEnv;
use near_o11y::testonly::init_test_logger;

use near_network::types::PartialEncodedChunkRequestMsg;

/// Checks that various ways of preparing partial encode chunk request give the
/// same result.
///
/// The test generates a block and than uses different methods of constructing
/// a PartialEncodedChunkResponseMsg to at the end compare if all of the methods
/// gave the same result.
#[test]
fn test_prepare_partial_encoded_chunk_response() {
    init_test_logger();

    let mut env = TestEnv::builder(ChainGenesis::test()).build();

    let height = env.clients[0].chain.head().unwrap().height;
    env.produce_block(0, height + 1);
    // No idea why, but the test does not work if we produce just one block.
    // For some reason the partial chunks aren’t saved in the storage and then
    // various reads we expect to succeed fail.  Generating two blocks makes
    // things work. -- mina86
    env.produce_block(0, height + 2);

    // Figure out chunk hash.
    let block = env.clients[0].chain.get_block_by_height(height + 2).unwrap();
    let chunk_hash = block.chunks()[0].chunk_hash();
    tracing::debug!(target: "chunks", "block hash: {}; chunk_hash: {}", block.hash(), chunk_hash.0);

    // Request partial encoded chunk from the validator.  This goes through the
    // regular network path and since we’ve only ever produced one chunk this
    // will be served from an in-memory cache.
    let request = PartialEncodedChunkRequestMsg {
        chunk_hash: chunk_hash.clone(),
        part_ords: (0..(env.clients[0].rs.total_shard_count() as u64)).collect(),
        tracking_shards: Some(0u64).into_iter().collect(),
    };
    let res = Some(env.get_partial_encoded_chunk_response(0, request.clone()));

    // Make the same request but this time call directly to ShardsManager and
    // get the request from a PartialEncodedChunk object.
    let partial_chunk = env.clients[0].chain.mut_store().get_partial_chunk(&chunk_hash).unwrap();
    let res_from_partial = ShardsManager::prepare_partial_encoded_chunk_response_from_partial(
        request.clone(),
        &partial_chunk,
    );

    // And finally, once more make the same request but this time construct the
    // response from ShardChunk object.
    let chunk = env.clients[0].chain.mut_store().get_chunk(&chunk_hash).unwrap();
    let client = &mut env.clients[0];
    let res_from_chunk = client.shards_mgr.prepare_partial_encoded_chunk_response_from_chunk(
        request.clone(),
        &mut client.rs,
        &chunk,
    );

    assert_eq!(res, res_from_partial);
    assert_eq!(res, res_from_chunk);
}
