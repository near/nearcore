use borsh::BorshDeserialize;
use near_chain::stateless_validation::processing_tracker::{
    ProcessingDoneTracker, ProcessingDoneWaiter,
};
use near_chain::{Block, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::DistributeStateWitnessRequest;
use near_o11y::testonly::init_integration_logger;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::sharding::ShardChunkHeaderV3;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessSize,
};
use near_primitives::types::AccountId;
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use std::collections::HashSet;

struct WitnessBitFlipTestEnv {
    env: TestEnv,
    block: Block,
    witness: ChunkStateWitness,
    excluded_validator: AccountId,
    excluded_validator_idx: usize,
}

/// This function prepares a scenario in which an orphaned chunk witness will occur.
/// It creates two blocks (`block1` and `block2`), but doesn't pass them to `excluded_validator`.
/// When `excluded_validator` receives a witness for the chunk belonging to `block2`, it doesn't
/// have `block1` which is required to process the witness, so it becomes an orphaned state witness.
fn setup_witness_bit_flip_test() -> WitnessBitFlipTestEnv {
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
        tracing::info!(target: "test", "QQP Block Hash: {:?}", block.chunks()[0].chunk_hash());
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

    let block_producer = env.get_block_producer_at_offset(&tip, 1);
    let block_chunk_producer = env.get_chunk_producer_at_offset(&tip, 2, shard_id);

    let last_block_producer = env.get_block_producer_at_offset(&tip, 2);

    let excluded_validator = accounts
        .into_iter()
        .filter(|acc| {
            acc != &block_producer && acc != &block_chunk_producer && acc != &last_block_producer
        })
        .next()
        .unwrap();
    let excluded_validator_idx = env.get_client_index(&excluded_validator);

    tracing::info!(target:"test", "Producing block at height {}", tip.height + 1);
    let block = env.client(&block_producer).produce_block(tip.height + 1).unwrap().unwrap();
    assert_eq!(
        block.chunks()[0].height_created(),
        block.header().height(),
        "There should be no missing chunks."
    );
    for client_idx in 0..env.clients.len() {
        let blocks_processed = env.clients[client_idx]
            .process_block_test(block.clone().into(), Provenance::NONE)
            .unwrap();
        assert_eq!(blocks_processed, vec![*block.hash()]);
    }
    env.process_partial_encoded_chunks();
    for client_idx in 0..env.clients.len() {
        env.process_shards_manager_responses(client_idx);
    }

    // At this point the chunk producer for the chunk belonging to block2 produces
    // the chunk and sends out a witness for it. Let's intercept the witness
    // and process it on all validators except for `excluded_validator`.
    // The intercepted witness will be used in the bit flip test.
    let mut witness_opt = None;
    let partial_witness_adapter =
        env.partial_witness_adapters[env.get_client_index(&block_chunk_producer)].clone();
    while let Some(request) = partial_witness_adapter.pop_distribution_request() {
        let DistributeStateWitnessRequest { state_witness, .. } = request;
        let raw_witness_size = borsh_size(&state_witness);
        let key = state_witness.chunk_production_key();
        let chunk_validators = env
            .client(&block_chunk_producer)
            .epoch_manager
            .get_chunk_validator_assignments(&key.epoch_id, key.shard_id, key.height_created)
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

    tracing::info!(target:"test", "Producing block at height {}", tip.height + 2);
    let last_block =
        env.client(&last_block_producer).produce_block(tip.height + 2).unwrap().unwrap();
    assert_eq!(
        last_block.chunks()[0].height_created(),
        last_block.header().height(),
        "There should be no missing chunks."
    );

    let witness = witness_opt.unwrap();
    assert_eq!(witness.chunk_header.chunk_hash(), last_block.chunks()[0].chunk_hash());

    env.process_partial_encoded_chunks();
    for client_idx in 0..env.clients.len() {
        env.process_shards_manager_responses_and_finish_processing_blocks(client_idx);
    }

    WitnessBitFlipTestEnv { env, block, witness, excluded_validator, excluded_validator_idx }
}

/// Test that a valid orphan witness is correctly processed once the required block arrives.
#[test]
fn test_corrupted_witness() {
    init_integration_logger();

    let WitnessBitFlipTestEnv {
        mut env,
        block: _block,
        witness,
        excluded_validator,
        excluded_validator_idx: _excluded_validator_idx,
    } = setup_witness_bit_flip_test();

    let witness_bytes = borsh::to_vec(&witness).unwrap();
    let mut corrupted_bit_idx = 0;
    loop {
        let mut witness_bytes = witness_bytes.clone();
        let res = bit_flip(&mut witness_bytes, corrupted_bit_idx);
        if let Err(err) = &res {
            if err.to_string() == "End" {
                // `corrupted_bit_idx` is out of bounds for correct block length. Should stop iteration.
                break;
            } else {
                panic!("Unexpected error: {}", err);
            }
        }

        let res = check_state_witness(witness_bytes, &mut env, excluded_validator.clone());
        res.unwrap();

        corrupted_bit_idx += 1;
    }

    // let block_processed = env
    //     .client(&excluded_validator)
    //     .process_block_test(block1.clone().into(), Provenance::NONE)
    //     .unwrap();
    // assert_eq!(block_processed, vec![*block1.hash()]);

    // After processing `block1`, `excluded_validator` should process the orphaned witness for the chunk belonging to `block2`
    // and it should send out an endorsement for this chunk. This happens asynchronously, so we have to wait for it.
    // env.wait_for_chunk_endorsement(excluded_validator_idx, &block.chunks()[0].chunk_hash())
    //     .unwrap();
}

const STATE_WITNESS_NOT_PARSED_MSG: &str = "Corrupt state witness didn't parse";

fn check_state_witness(
    bytes: Vec<u8>,
    env: &mut TestEnv,
    excluded_validator: AccountId,
) -> Result<anyhow::Error, anyhow::Error> {
    if let Ok(witness) = ChunkStateWitness::try_from_slice(bytes.as_slice()) {
        // `excluded_validator` receives a corrupted witness for a chunk belonging to the previous block.
        let witness_size = bytes.len();
        let client = env.client(&excluded_validator);
        // expect an error
        client
            .process_chunk_state_witness(witness, witness_size, None, client.validator_signer.get())
            .unwrap_err();
        return Ok(anyhow::anyhow!("Good"));
    } else {
        return Ok(anyhow::anyhow!(STATE_WITNESS_NOT_PARSED_MSG));
    }
}

fn bit_flip(bytes: &mut Vec<u8>, corrupted_bit_idx: usize) -> anyhow::Result<()> {
    if corrupted_bit_idx >= bytes.len() * 8 {
        return Err(anyhow::anyhow!("End"));
    }

    // get indices
    let byte_idx = corrupted_bit_idx / 8;
    let bit_idx = corrupted_bit_idx % 8;

    // flip
    bytes[byte_idx] ^= 1 << bit_idx;
    Ok(())
}

fn _modify_witness_header_inner(
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
