use crate::setup::builder::TestLoopBuilder;
use assert_matches::assert_matches;
use near_async::messaging::Handler as _;
use near_chain::ChainStoreAccess as _;
use near_client::GetLightClientChunkExecutionProof;
use near_client_primitives::types::GetLightClientProofError;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{ChunkExecutionRoots, ChunkExecutionRootsV1, SpiceChunkId};
use near_primitives::views::{ChunkExecutionProofView, LightClientBlockLiteView};
use near_store::spice_proof_verifier::{SpiceProofVerificationError, verify_chunk_execution_proof};

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_light_client_chunk_execution_proof() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(1, 0).build();

    // Certify the chain through the current tip, then let finality advance strictly
    // past it. The handler serves from the certifying-block index (written at
    // finality), and the head must be strictly after a chunk's certifying block for
    // the block merkle proof to recompute the head's block_merkle_root from it.
    let tip_height = env.validator().head().height;
    env.validator_runner().run_until_certified(tip_height);
    let certified_head_height = env.validator().head().height;
    env.validator_runner().run_until_final_head_height(certified_head_height + 1);

    let light_client_head = env.validator().final_head().last_block_hash;
    let head_header = env.validator().client().chain.get_block_header(&light_client_head).unwrap();
    let trusted_block_merkle_root = *head_header.block_merkle_root();

    // Any chunk certified by a block strictly below the head is servable. Walk back
    // from the head to the first block that carries execution results.
    let chain_store = &env.validator().client().chain.chain_store;
    let mut hash = *head_header.prev_hash();
    let (chunk_id, certifying_block_hash) = loop {
        let block = chain_store.get_block(&hash).unwrap();
        if let Some((chunk_id, _)) = block.spice_core_statements().iter_execution_results().next() {
            break (chunk_id.clone(), hash);
        }
        hash = *block.header().prev_hash();
        assert_ne!(hash, CryptoHash::default(), "no certified chunk below the light client head");
    };

    let chunk_proof: ChunkExecutionProofView = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientChunkExecutionProof { chunk_id: chunk_id.clone(), light_client_head })
        .unwrap();
    verify_chunk_execution_proof(&chunk_proof, &chunk_id, &trusted_block_merkle_root).unwrap();

    // Every merkle path in this proof is genuine, so only the chunk id check stops a
    // server from serving it as the answer for a chunk in another block.
    let chunk_id_in_another_block =
        SpiceChunkId { block_hash: light_client_head, shard_id: chunk_id.shard_id };
    assert_matches!(
        verify_chunk_execution_proof(
            &chunk_proof,
            &chunk_id_in_another_block,
            &trusted_block_merkle_root
        ),
        Err(SpiceProofVerificationError::UnexpectedChunkId { .. })
    );

    // Tampering a committed root no longer recomputes the chunk_execution_root.
    let ChunkExecutionRoots::V1(good_roots) = &chunk_proof.roots;
    let mut tampered_proof = chunk_proof.clone();
    tampered_proof.roots = ChunkExecutionRoots::V1(ChunkExecutionRootsV1 {
        state_root: CryptoHash::hash_bytes(b"tampered state root"),
        ..good_roots.clone()
    });
    assert_matches!(
        verify_chunk_execution_proof(&tampered_proof, &chunk_id, &trusted_block_merkle_root),
        Err(SpiceProofVerificationError::InvalidRootsProof)
    );

    // A correct proof must not verify against a wrong trusted root.
    let wrong_root = CryptoHash::hash_bytes(b"not the head block merkle root");
    assert_matches!(
        verify_chunk_execution_proof(&chunk_proof, &chunk_id, &wrong_root),
        Err(SpiceProofVerificationError::InvalidBlockProof)
    );

    // The certifying block's own block merkle root does not commit to itself, so a head
    // at exactly that height is rejected.
    let result =
        env.validator_mut().view_client_actor().handle(GetLightClientChunkExecutionProof {
            chunk_id: chunk_id.clone(),
            light_client_head: certifying_block_hash,
        });
    assert_matches!(result, Err(GetLightClientProofError::LightClientHeadTooOld { .. }));

    // A not-yet-certified chunk (in the non-final head block) is not served.
    let head_block = env.validator().head_block();
    let uncertified_chunk_id =
        SpiceChunkId { block_hash: *head_block.hash(), shard_id: chunk_id.shard_id };
    let result =
        env.validator_mut().view_client_actor().handle(GetLightClientChunkExecutionProof {
            chunk_id: uncertified_chunk_id,
            light_client_head,
        });
    assert_matches!(result, Err(GetLightClientProofError::ChunkNotCertified { .. }));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_light_client_block_lite_view_hash() {
    init_test_logger();

    // `build()` warms up, so the chain already has produced a block.
    let env = TestLoopBuilder::new().validators(1, 0).build();

    // The verifier derives the certifying block's hash by calling
    // LightClientBlockLiteView::hash(), so that reconstruction (rebuilding the spice
    // header's inner-lite, including chunk_execution_root) must equal the real hash.
    let block = env.validator().head_block();
    assert_eq!(
        LightClientBlockLiteView::from(BlockHeader::clone(block.header())).hash(),
        *block.header().hash(),
    );
}
