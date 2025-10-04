use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;
use near_async::messaging::Handler;
use near_chain::chain::ChunkStateWitnessMessage;
use near_chain::stateless_validation::processing_tracker::{
    ProcessingDoneTracker, ProcessingDoneWaiter,
};
use near_chain::{Block, Provenance};
use near_chain_configs::Genesis;
use near_chain_configs::default_orphan_state_witness_max_size;
use near_client::{
    BlockNotificationMessage, ChunkValidationActorInner, HandleOrphanWitnessOutcome,
};
use near_o11y::testonly::init_integration_logger;
use near_primitives::sharding::ShardChunkHeaderV3;
use near_primitives::sharding::{
    ChunkHash, ReceiptProof, ShardChunkHeader, ShardChunkHeaderInner, ShardProof,
};
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use near_primitives::types::{AccountId, ShardId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

trait ChunkStateWitnessExt {
    fn mut_source_receipt_proofs(&mut self) -> &mut HashMap<ChunkHash, ReceiptProof>;
    fn mut_chunk_header(&mut self) -> &mut ShardChunkHeader;
}

impl ChunkStateWitnessExt for ChunkStateWitness {
    fn mut_source_receipt_proofs(&mut self) -> &mut HashMap<ChunkHash, ReceiptProof> {
        match self {
            ChunkStateWitness::V1(witness) => &mut witness.source_receipt_proofs,
            ChunkStateWitness::V2(witness) => &mut witness.source_receipt_proofs,
        }
    }

    fn mut_chunk_header(&mut self) -> &mut ShardChunkHeader {
        match self {
            ChunkStateWitness::V1(witness) => &mut witness.chunk_header,
            ChunkStateWitness::V2(witness) => &mut witness.chunk_header,
        }
    }
}

struct OrphanWitnessTestEnv {
    env: TestEnv,
    block1: Arc<Block>,
    block2: Arc<Block>,
    witness: ChunkStateWitness,
    excluded_validator: AccountId,
    excluded_validator_idx: usize,
    chunk_validation_actor: ChunkValidationActorInner,
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
    let shard_layout = env.clients[0].epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();

    let block1_producer = env.get_block_producer_at_offset(&tip, 1);
    let block2_producer = env.get_block_producer_at_offset(&tip, 2);
    let block2_chunk_producer = env.get_chunk_producer_at_offset(&tip, 2, shard_id);

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

    // Propagate chunk state witnesses for block1
    env.propagate_chunk_state_witnesses(false);
    env.propagate_chunk_endorsements(false);

    // At this point chunk producer for the chunk belonging to block2 produces
    // the chunk and sends out a witness for it. Let's intercept the witness
    // and process it on all validators except for `excluded_validator`.
    // The witness isn't processed on `excluded_validator` to give users of
    // `setup_orphan_witness_test()` full control over the events.

    // Trigger chunk production for block2 by producing the block first
    let block2 = env.client(&block2_producer).produce_block(tip.height + 2).unwrap().unwrap();
    tracing::info!(target:"test", "Producing block2 at height {}", tip.height + 2);
    assert_eq!(
        block2.chunks()[0].height_created(),
        block2.header().height(),
        "There should be no missing chunks."
    );

    let chunk_producer_client = env.client(&block2_chunk_producer);
    let chunk2 = chunk_producer_client.chain.get_chunk(&block2.chunks()[0].chunk_hash()).unwrap();
    let witness = chunk_producer_client
        .chain
        .chain_store()
        .create_state_witness(
            chunk_producer_client.epoch_manager.as_ref(),
            block2_chunk_producer.clone(),
            block1.header(),
            &block1.chunks()[0],
            &chunk2,
        )
        .unwrap()
        .state_witness;

    // Process the manually created witness on all validators except for `excluded_validator`
    let raw_witness_size = borsh::object_length(&witness).unwrap();
    let key = witness.chunk_production_key();
    let chunk_validators = env
        .client(&block2_chunk_producer)
        .epoch_manager
        .get_chunk_validator_assignments(&key.epoch_id, key.shard_id, key.height_created)
        .unwrap()
        .ordered_chunk_validators();

    let mut witness_processing_done_waiters: Vec<ProcessingDoneWaiter> = Vec::new();
    for account_id in chunk_validators.into_iter().filter(|acc| *acc != excluded_validator) {
        let processing_done_tracker = ProcessingDoneTracker::new();
        witness_processing_done_waiters.push(processing_done_tracker.make_waiter());
        let account_index = env.get_client_index(&account_id);
        let witness_message = near_chain::chain::ChunkStateWitnessMessage {
            witness: witness.clone(),
            raw_witness_size,
            processing_done_tracker: Some(processing_done_tracker),
        };
        env.chunk_validation_actors[account_index].handle(witness_message);
    }
    for waiter in witness_processing_done_waiters {
        waiter.wait();
    }

    env.propagate_chunk_state_witnesses_and_endorsements(false);
    assert_eq!(witness.chunk_header().chunk_hash(), block2.chunks()[0].chunk_hash());

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

    let chunk_validation_actor = env.chunk_validation_actors[excluded_validator_idx].clone();

    OrphanWitnessTestEnv {
        env,
        block1,
        block2,
        witness,
        excluded_validator,
        excluded_validator_idx,
        chunk_validation_actor,
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
    let witness_size = borsh::object_length(&witness).unwrap();
    let witness_message = near_chain::chain::ChunkStateWitnessMessage {
        witness: witness,
        raw_witness_size: witness_size,
        processing_done_tracker: None,
    };
    env.chunk_validation_actors[excluded_validator_idx].handle(witness_message);

    let block_processed = env
        .client(&excluded_validator)
        .process_block_test(block1.clone().into(), Provenance::NONE)
        .unwrap();
    assert_eq!(block_processed, vec![*block1.hash()]);

    // Trigger processing of ready orphan witnesses after block1 arrives
    let block_notification = BlockNotificationMessage { block: block1 };
    env.chunk_validation_actors[excluded_validator_idx].handle(block_notification);

    // After processing `block1`, `excluded_validator` should process the orphaned witness for the chunk belonging to `block2`
    // and it should send out an endorsement for this chunk. This happens asynchronously, so we have to wait for it.
    env.wait_for_chunk_endorsement(excluded_validator_idx, &block2.chunks()[0].chunk_hash())
        .unwrap();
}

#[test]
fn test_orphan_witness_too_large() {
    init_integration_logger();

    let OrphanWitnessTestEnv { mut chunk_validation_actor, witness, .. } =
        setup_orphan_witness_test();

    // Test with a witness that exceeds the max size limit
    let oversized_witness_size = default_orphan_state_witness_max_size().as_u64() as usize + 1;
    let outcome =
        chunk_validation_actor.handle_orphan_witness(witness, oversized_witness_size).unwrap();
    assert!(matches!(outcome, HandleOrphanWitnessOutcome::TooBig(_)))
}

/// Witnesses which are too far from the chain head should not be saved to the orphan pool
#[test]
fn test_orphan_witness_far_from_head() {
    init_integration_logger();

    let OrphanWitnessTestEnv { mut chunk_validation_actor, mut witness, block1, .. } =
        setup_orphan_witness_test();

    let bad_height = 10000;
    modify_witness_header_inner(&mut witness, |header| match &mut header.inner {
        ShardChunkHeaderInner::V1(inner) => inner.height_created = bad_height,
        ShardChunkHeaderInner::V2(inner) => inner.height_created = bad_height,
        ShardChunkHeaderInner::V3(inner) => inner.height_created = bad_height,
        ShardChunkHeaderInner::V4(inner) => inner.height_created = bad_height,
        ShardChunkHeaderInner::V5(inner) => inner.height_created = bad_height,
    });

    let outcome = chunk_validation_actor.handle_orphan_witness(witness, 2000).unwrap();
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
    witness.mut_source_receipt_proofs().insert(
        ChunkHash::default(),
        ReceiptProof(
            vec![],
            ShardProof {
                from_shard_id: ShardId::new(100230230),
                to_shard_id: ShardId::new(383939),
                proof: vec![],
            },
        ),
    );

    // The witness should be accepted and saved into the pool, even though it's invalid.
    // There is no way to fully validate an orphan witness, so this is the correct behavior.
    // The witness will later be fully validated when the required block arrives.
    let witness_size = borsh::object_length(&witness).unwrap();
    let witness_message = ChunkStateWitnessMessage {
        witness,
        raw_witness_size: witness_size,
        processing_done_tracker: None,
    };
    let excluded_validator_idx = env.get_client_index(&excluded_validator);
    let result = env.chunk_validation_actors[excluded_validator_idx]
        .process_chunk_state_witness_message(witness_message);
    assert!(result.is_ok(), "Orphan witness should be accepted");
}

fn modify_witness_header_inner(
    witness: &mut ChunkStateWitness,
    f: impl FnOnce(&mut ShardChunkHeaderV3),
) {
    match witness.mut_chunk_header() {
        ShardChunkHeader::V3(header) => {
            f(header);
        }
        _ => unreachable!(),
    };
}
