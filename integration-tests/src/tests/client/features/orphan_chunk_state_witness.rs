use std::collections::HashSet;

use near_chain::stateless_validation::processing_tracker::{
    ProcessingDoneTracker, ProcessingDoneWaiter,
};
use near_chain::{Block, Provenance};
use near_chain_configs::default_orphan_state_witness_max_size;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::DistributeStateWitnessRequest;
use near_client::HandleOrphanWitnessOutcome;
use near_o11y::testonly::init_integration_logger;
use near_primitives::sharding::ShardChunkHeaderV3;
use near_primitives::sharding::{
    ChunkHash, ReceiptProof, ShardChunkHeader, ShardChunkHeaderInner, ShardProof,
};
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessSize,
};
use near_primitives::types::AccountId;
use nearcore::test_utils::TestEnvNightshadeSetupExt;

struct OrphanWitnessTestEnv {
    env: TestEnv,
    block1: Block,
    block2: Block,
    witness: ChunkStateWitness,
    excluded_validator: AccountId,
    excluded_validator_idx: usize,
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
    let partial_witness_adapter =
        env.partial_witness_adapters[env.get_client_index(&block2_chunk_producer)].clone();
    while let Some(request) = partial_witness_adapter.pop_distribution_request() {
        let DistributeStateWitnessRequest { epoch_id, chunk_header, state_witness } = request;
        let raw_witness_size = borsh_size(&state_witness);
        let chunk_validators = env
            .client(&block2_chunk_producer)
            .epoch_manager
            .get_chunk_validator_assignments(
                &epoch_id,
                chunk_header.shard_id(),
                chunk_header.height_created(),
            )
            .unwrap()
            .ordered_chunk_validators();

        let mut witness_processing_done_waiters: Vec<ProcessingDoneWaiter> = Vec::new();
        for account_id in chunk_validators.into_iter().filter(|acc| *acc != excluded_validator) {
            let processing_done_tracker = ProcessingDoneTracker::new();
            witness_processing_done_waiters.push(processing_done_tracker.make_waiter());
            let client = env.client(&account_id);
            client
                .process_chunk_state_witness(
                    state_witness.clone(),
                    raw_witness_size,
                    Some(processing_done_tracker),
                    client.validator_signer.get(),
                )
                .unwrap();
        }
        for waiter in witness_processing_done_waiters {
            waiter.wait();
        }
        witness_opt = Some(state_witness);
    }

    env.propagate_chunk_endorsements(false);

    tracing::info!(target:"test", "Producing block2 at height {}", tip.height + 2);
    let block2 = env.client(&block2_producer).produce_block(tip.height + 2).unwrap().unwrap();
    assert_eq!(
        block2.chunks()[0].height_created(),
        block2.header().height(),
        "There should be no missing chunks."
    );
    let witness = witness_opt.unwrap();
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
        witness,
        excluded_validator,
        excluded_validator_idx,
    }
}

/// Test that a valid orphan witness is correctly processed once the required block arrives.
#[test]
fn test_orphan_witness_valid() {
    init_integration_logger();

    let OrphanWitnessTestEnv {
        mut env,
        block1,
        block2,
        witness,
        excluded_validator,
        excluded_validator_idx,
        ..
    } = setup_orphan_witness_test();

    // `excluded_validator` receives witness for chunk belonging to `block2`, but it doesn't have `block1`.
    // The witness should become an orphaned witness and it should be saved to the orphan pool.
    let witness_size = borsh_size(&witness);
    let client = env.client(&excluded_validator);
    client
        .process_chunk_state_witness(witness, witness_size, None, client.validator_signer.get())
        .unwrap();

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
fn test_orphan_witness_too_large() {
    init_integration_logger();

    let OrphanWitnessTestEnv { mut env, witness, excluded_validator, .. } =
        setup_orphan_witness_test();

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

    let OrphanWitnessTestEnv { mut env, mut witness, block1, excluded_validator, .. } =
        setup_orphan_witness_test();

    let bad_height = 10000;
    modify_witness_header_inner(&mut witness, |header| match &mut header.inner {
        ShardChunkHeaderInner::V1(inner) => inner.height_created = bad_height,
        ShardChunkHeaderInner::V2(inner) => inner.height_created = bad_height,
        ShardChunkHeaderInner::V3(inner) => inner.height_created = bad_height,
        ShardChunkHeaderInner::V4(inner) => inner.height_created = bad_height,
    });

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

    let OrphanWitnessTestEnv { mut env, mut witness, excluded_validator, .. } =
        setup_orphan_witness_test();

    // Make the witness invalid in a way that won't be detected during orphan witness validation
    witness.source_receipt_proofs.insert(
        ChunkHash::default(),
        ReceiptProof(
            vec![],
            ShardProof { from_shard_id: 100230230, to_shard_id: 383939, proof: vec![] },
        ),
    );

    // The witness should be accepted and saved into the pool, even though it's invalid.
    // There is no way to fully validate an orphan witness, so this is the correct behavior.
    // The witness will later be fully validated when the required block arrives.
    let witness_size = borsh_size(&witness);
    let client = env.client(&excluded_validator);
    client
        .process_chunk_state_witness(witness, witness_size, None, client.validator_signer.get())
        .unwrap();
}

fn modify_witness_header_inner(
    witness: &mut ChunkStateWitness,
    f: impl FnOnce(&mut ShardChunkHeaderV3),
) {
    match &mut witness.chunk_header {
        ShardChunkHeader::V3(header) => {
            f(header);
        }
        _ => unreachable!(),
    };
}

fn borsh_size(witness: &ChunkStateWitness) -> ChunkStateWitnessSize {
    borsh::to_vec(&witness).unwrap().len()
}
