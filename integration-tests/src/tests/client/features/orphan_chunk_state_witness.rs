use std::collections::HashSet;

use near_chain::{Block, Provenance};
use near_chain_configs::default_orphan_state_witness_max_size;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::HandleOrphanWitnessOutcome;
use near_client::{Client, ProcessingDoneTracker, ProcessingDoneWaiter};
use near_crypto::Signature;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_o11y::testonly::init_integration_logger;
use near_primitives::sharding::{
    ChunkHash, ReceiptProof, ShardChunkHeader, ShardChunkHeaderInner, ShardChunkHeaderInnerV2,
    ShardProof,
};
use near_primitives::stateless_validation::EncodedChunkStateWitness;
use near_primitives::stateless_validation::SignedEncodedChunkStateWitness;
use near_primitives::types::AccountId;
use near_primitives_core::checked_feature;
use near_primitives_core::version::PROTOCOL_VERSION;
use nearcore::test_utils::TestEnvNightshadeSetupExt;

struct OrphanWitnessTestEnv {
    env: TestEnv,
    block1: Block,
    block2: Block,
    signed_witness: SignedEncodedChunkStateWitness,
    excluded_validator: AccountId,
    excluded_validator_idx: usize,
    chunk_producer: AccountId,
}

/// This function prepares a scenario in which an orphaned chunk witness will occur.
/// It creates two blocks (`block1` and `block2`), but doesn't pass them to `excluded_validator`.
/// When `excluded_validator` receives a witness for the chunk belonging to `block2`, it doesn't
/// have `block1` which is required to process the witness, so it becomes an orphaned state witness.
fn setup_orphan_witness_test() -> OrphanWitnessTestEnv {
    let accounts: Vec<AccountId> = (0..4).map(|i| format!("test{i}").parse().unwrap()).collect();
    let genesis = Genesis::test(accounts.clone(), accounts.len().try_into().unwrap());
    let mut env = TestEnv::builder(&genesis.config)
        .clients(accounts.clone())
        .validators(accounts.clone())
        .nightshade_runtimes(&genesis)
        .build();

    // Run the blockchain for a few blocks
    for height in 1..4 {
        // Produce the next block
        let tip = env.clients[0].chain.head().unwrap();
        let block_producer = env.get_block_producer_at_offset(&tip, 1);
        tracing::info!(target: "test", "Producing block at height: {height} by {block_producer}");
        let block = env.client(&block_producer).produce_block(tip.height + 1).unwrap().unwrap();
        tracing::info!(target: "test", "Block produced at height {} has chunk {:?}", height, block.chunks()[0].chunk_hash());

        // The first block after genesis doesn't have any chunks, but all other blocks should have a new chunk inside.
        if height > 1 {
            assert_eq!(
                block.chunks()[0].height_created(),
                block.header().height(),
                "There should be no missing chunks."
            );
        }

        // Pass network messages around
        for i in 0..env.clients.len() {
            let blocks_processed =
                env.clients[i].process_block_test(block.clone().into(), Provenance::NONE).unwrap();
            assert_eq!(blocks_processed, vec![*block.hash()]);
        }

        env.process_partial_encoded_chunks();
        for client_idx in 0..env.clients.len() {
            env.process_shards_manager_responses_and_finish_processing_blocks(client_idx);
        }
        env.propagate_chunk_state_witnesses_and_endorsements(false);

        // Verify heads
        let heads = env
            .clients
            .iter()
            .map(|client| client.chain.head().unwrap().last_block_hash)
            .collect::<HashSet<_>>();
        assert_eq!(heads.len(), 1, "All clients should have the same head");
    }

    // Produce two more blocks (`block1` and `block2`), but don't send them to the `excluded_validator`.
    // The `excluded_validator` will receive a chunk witness for the chunk in `block2`, but it won't
    // have `block1`, so it will become an orphaned chunk state witness.
    let tip = env.clients[0].chain.head().unwrap();

    let block1_producer = env.get_block_producer_at_offset(&tip, 1);
    let block2_producer = env.get_block_producer_at_offset(&tip, 2);
    let block2_chunk_producer = env.get_chunk_producer_at_offset(&tip, 2, 0);

    // The excluded validator shouldn't produce any blocks or chunks in the next two blocks.
    // There's 4 validators and at most 3 aren't a good candidate, so there's always at least
    // one that fits all the criteria, the `unwrap()` won't fail.
    let excluded_validator = accounts
        .into_iter()
        .filter(|acc| {
            acc != &block1_producer && acc != &block2_producer && acc != &block2_chunk_producer
        })
        .next()
        .unwrap();
    let excluded_validator_idx = env.get_client_index(&excluded_validator);
    let clients_without_excluded =
        (0..env.clients.len()).filter(|idx| *idx != excluded_validator_idx);

    tracing::info!(target:"test", "Producing block1 at height {}", tip.height + 1);
    let block1 = env.client(&block1_producer).produce_block(tip.height + 1).unwrap().unwrap();
    assert_eq!(
        block1.chunks()[0].height_created(),
        block1.header().height(),
        "There should be no missing chunks."
    );

    for client_idx in clients_without_excluded.clone() {
        let blocks_processed = env.clients[client_idx]
            .process_block_test(block1.clone().into(), Provenance::NONE)
            .unwrap();
        assert_eq!(blocks_processed, vec![*block1.hash()]);
    }
    env.process_partial_encoded_chunks();
    for client_idx in 0..env.clients.len() {
        env.process_shards_manager_responses(client_idx);
    }

    // At this point chunk producer for the chunk belonging to block2 produces
    // the chunk and sends out a witness for it. Let's intercept the witness
    // and process it on all validators except for `excluded_validator`.
    // The witness isn't processed on `excluded_validator` to give users of
    // `setup_orphan_witness_test()` full control over the events.
    let mut witness_opt = None;
    let network_adapter =
        env.network_adapters[env.get_client_index(&block2_chunk_producer)].clone();
    network_adapter.handle_filtered(|request| match request {
        PeerManagerMessageRequest::NetworkRequests(NetworkRequests::ChunkStateWitness(
            account_ids,
            state_witness,
        )) => {
            let mut witness_processing_done_waiters: Vec<ProcessingDoneWaiter> = Vec::new();
            for account_id in account_ids.iter().filter(|acc| **acc != excluded_validator) {
                let processing_done_tracker = ProcessingDoneTracker::new();
                witness_processing_done_waiters.push(processing_done_tracker.make_waiter());
                env.client(account_id)
                    .process_chunk_state_witness(
                        state_witness.clone(),
                        Some(processing_done_tracker),
                    )
                    .unwrap();
            }
            for waiter in witness_processing_done_waiters {
                waiter.wait();
            }
            witness_opt = Some(state_witness);
            None
        }
        _ => Some(request),
    });
    let signed_witness = witness_opt.unwrap();
    let witness = signed_witness.witness_bytes.decode().unwrap();

    env.propagate_chunk_endorsements(false);

    tracing::info!(target:"test", "Producing block2 at height {}", tip.height + 2);
    let block2 = env.client(&block2_producer).produce_block(tip.height + 2).unwrap().unwrap();
    assert_eq!(
        block2.chunks()[0].height_created(),
        block2.header().height(),
        "There should be no missing chunks."
    );
    assert_eq!(witness.chunk_header.chunk_hash(), block2.chunks()[0].chunk_hash());

    for client_idx in clients_without_excluded {
        let blocks_processed = env.clients[client_idx]
            .process_block_test(block2.clone().into(), Provenance::NONE)
            .unwrap();
        assert_eq!(blocks_processed, vec![*block2.hash()]);
    }

    env.process_partial_encoded_chunks();
    for client_idx in 0..env.clients.len() {
        env.process_shards_manager_responses_and_finish_processing_blocks(client_idx);
    }

    OrphanWitnessTestEnv {
        env,
        block1,
        block2,
        signed_witness,
        excluded_validator,
        excluded_validator_idx,
        chunk_producer: block2_chunk_producer,
    }
}

/// Test that a valid orphan witness is correctly processed once the required block arrives.
#[test]
fn test_orphan_witness_valid() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTestEnv {
        mut env,
        block1,
        block2,
        signed_witness,
        excluded_validator,
        excluded_validator_idx,
        ..
    } = setup_orphan_witness_test();

    // `excluded_validator` receives witness for chunk belonging to `block2`, but it doesn't have `block1`.
    // The witness should become an orphaned witness and it should be saved to the orphan pool.
    env.client(&excluded_validator).process_chunk_state_witness(signed_witness, None).unwrap();

    let block_processed = env
        .client(&excluded_validator)
        .process_block_test(block1.clone().into(), Provenance::NONE)
        .unwrap();
    assert_eq!(block_processed, vec![*block1.hash()]);

    // After processing `block1`, `excluded_validator` should process the orphaned witness for the chunk belonging to `block2`
    // and it should send out an endorsement for this chunk. This happens asynchronously, so we have to wait for it.
    env.wait_for_chunk_endorsement(excluded_validator_idx, &block2.chunks()[0].chunk_hash())
        .unwrap();
}

#[test]
fn test_orphan_witness_bad_signature() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTestEnv { mut env, mut signed_witness, excluded_validator, .. } =
        setup_orphan_witness_test();

    // Modify the witness to contain an invalid signature
    signed_witness.signature = Signature::default();

    let error = env
        .client(&excluded_validator)
        .process_chunk_state_witness(signed_witness, None)
        .unwrap_err();
    let error_message = format!("{error}").to_lowercase();
    tracing::info!(target:"test", "Error message: {}", error_message);
    assert!(error_message.contains("invalid signature"));
}

#[test]
fn test_orphan_witness_signature_from_wrong_peer() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTestEnv { mut env, mut signed_witness, excluded_validator, .. } =
        setup_orphan_witness_test();

    // Sign the witness using another validator's key.
    // Only witnesses from the chunk producer that produced this witness should be accepted.
    resign_witness(&mut signed_witness, env.client(&excluded_validator));

    let error = env
        .client(&excluded_validator)
        .process_chunk_state_witness(signed_witness, None)
        .unwrap_err();
    let error_message = format!("{error}").to_lowercase();
    tracing::info!(target:"test", "Error message: {}", error_message);
    assert!(error_message.contains("invalid signature"));
}

#[test]
fn test_orphan_witness_invalid_shard_id() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTestEnv {
        mut env,
        mut signed_witness,
        excluded_validator,
        chunk_producer,
        ..
    } = setup_orphan_witness_test();

    // Set invalid shard_id in the witness header
    modify_witness_header_inner(&mut signed_witness, |header| header.shard_id = 10000000);
    resign_witness(&mut signed_witness, env.client(&chunk_producer));

    // The witness should be rejected
    let error = env
        .client(&excluded_validator)
        .process_chunk_state_witness(signed_witness, None)
        .unwrap_err();
    let error_message = format!("{error}").to_lowercase();
    tracing::info!(target:"test", "Error message: {}", error_message);
    assert!(error_message.contains("shard"));
}

#[test]
fn test_orphan_witness_too_large() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTestEnv { mut env, signed_witness, excluded_validator, .. } =
        setup_orphan_witness_test();

    let witness = signed_witness.witness_bytes.decode().unwrap();
    // The witness should not be saved too the pool, as it's too big
    let outcome = env
        .client(&excluded_validator)
        .handle_orphan_state_witness(
            witness,
            default_orphan_state_witness_max_size().as_u64() as usize + 1,
        )
        .unwrap();
    assert!(matches!(outcome, HandleOrphanWitnessOutcome::TooBig(_)))
}

/// Witnesses which are too far from the chain head should not be saved to the orphan pool
#[test]
fn test_orphan_witness_far_from_head() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTestEnv {
        mut env,
        mut signed_witness,
        chunk_producer,
        block1,
        excluded_validator,
        ..
    } = setup_orphan_witness_test();

    let bad_height = 10000;
    modify_witness_header_inner(&mut signed_witness, |header| header.height_created = bad_height);
    resign_witness(&mut signed_witness, env.client(&chunk_producer));

    let witness = signed_witness.witness_bytes.decode().unwrap();
    let outcome =
        env.client(&excluded_validator).handle_orphan_state_witness(witness, 2000).unwrap();
    assert_eq!(
        outcome,
        HandleOrphanWitnessOutcome::TooFarFromHead {
            witness_height: bad_height,
            head_height: block1.header().height() - 1
        }
    );
}

// Test that orphan witnesses are only partially validated - an orphan witness with invalid
// `source_receipt_proofs` will be accepted and saved into the pool, as there's no way to validate
// this field without the previous block.
#[test]
fn test_orphan_witness_not_fully_validated() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTestEnv {
        mut env,
        mut signed_witness,
        chunk_producer,
        excluded_validator,
        ..
    } = setup_orphan_witness_test();

    let mut witness = signed_witness.witness_bytes.decode().unwrap();
    // Make the witness invalid in a way that won't be detected during orphan witness validation
    witness.source_receipt_proofs.insert(
        ChunkHash::default(),
        ReceiptProof(
            vec![],
            ShardProof { from_shard_id: 100230230, to_shard_id: 383939, proof: vec![] },
        ),
    );
    signed_witness.witness_bytes = EncodedChunkStateWitness::encode(&witness);
    resign_witness(&mut signed_witness, env.client(&chunk_producer));

    // The witness should be accepted and saved into the pool, even though it's invalid.
    // There is no way to fully validate an orphan witness, so this is the correct behavior.
    // The witness will later be fully validated when the required block arrives.
    env.client(&excluded_validator).process_chunk_state_witness(signed_witness, None).unwrap();
}

fn modify_witness_header_inner(
    signed_witness: &mut SignedEncodedChunkStateWitness,
    f: impl FnOnce(&mut ShardChunkHeaderInnerV2),
) {
    let mut witness = signed_witness.witness_bytes.decode().unwrap();
    match &mut witness.chunk_header {
        ShardChunkHeader::V3(header) => match &mut header.inner {
            ShardChunkHeaderInner::V2(header_inner) => f(header_inner),
            _ => panic!(),
        },
        _ => panic!(),
    };
    signed_witness.witness_bytes = EncodedChunkStateWitness::encode(&witness);
}

fn resign_witness(witness: &mut SignedEncodedChunkStateWitness, signer: &Client) {
    witness.signature =
        signer.validator_signer.as_ref().unwrap().sign_chunk_state_witness(&witness.witness_bytes);
}
