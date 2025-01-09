use borsh::BorshDeserialize;
use near_chain::stateless_validation::processing_tracker::{
    ProcessingDoneTracker, ProcessingDoneWaiter,
};
use near_chain::{Block, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::DistributeStateWitnessRequest;
use near_o11y::testonly::init_integration_logger;
use near_primitives::stateless_validation::state_witness::{
    test_field_offsets::{find_field_for_bit, witness_field_offsets},
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

    // Produce two more blocks. The `excluded_validator` will receive a chunk witness
    // for the chunk in `block2` which will be intercepted and used in the bit flip test.
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

    // Intercept the witness and process it on all validators except for `excluded_validator`.
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

    // Produce the second block and validate the intercepted witness.
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

    let witness_fields = witness_field_offsets(&witness);

    let witness_bytes = borsh::to_vec(&witness).unwrap();
    let mut corrupted_bit_idx = 0;
    // List of reasons `check_process_flipped_block_fails_on_bit` returned `Err`.
    // Should be empty.
    let mut errs = vec![];
    // List of reasons `check_process_flipped_block_fails_on_bit` returned `Ok`.
    // Should contain various validation errors.
    let mut oks = vec![];
    loop {
        let mut witness_bytes = witness_bytes.clone();
        let in_bounds = bit_flip(&mut witness_bytes, corrupted_bit_idx);
        if !in_bounds {
            // `corrupted_bit_idx` is out of bounds for correct witness length. Should stop iteration.
            break;
        }

        let res = check_state_witness(
            witness_bytes,
            corrupted_bit_idx,
            witness.clone(),
            &witness_fields,
            &mut env,
            excluded_validator.clone(),
        );
        match res {
            Ok(msg) => {
                oks.push(msg);
            }
            Err(msg) => {
                errs.push(msg);
            }
        };

        corrupted_bit_idx += 1;
    }

    tracing::info!("All of the Errors:");
    for err in &errs {
        tracing::info!("{:?}", err);
    }
    tracing::info!("{}", ["-"; 100].concat());
    // tracing::info!("All of the Oks:");
    // for ok in &oks {
    //     tracing::info!("{:?}", ok);
    // }
    assert!(errs.is_empty());
    assert!(
        oks.iter()
            .filter(|e| e.to_string() != NOT_BREAKING_CHANGE_MSG
                && e.to_string() != STATE_WITNESS_NOT_PARSED_MSG)
            .count()
            > 0
    );
}

const STATE_WITNESS_NOT_PARSED_MSG: &str = "Corrupt state witness didn't parse";
const NOT_BREAKING_CHANGE_MSG: &str = "Not a breaking change";

fn check_state_witness(
    bytes: Vec<u8>,
    corrupted_bit_idx: usize,
    correct_witness: ChunkStateWitness,
    witness_fields: &Vec<(&'static str, usize, usize)>,
    env: &mut TestEnv,
    excluded_validator: AccountId,
) -> Result<anyhow::Error, anyhow::Error> {
    if let Ok(witness) = ChunkStateWitness::try_from_slice(bytes.as_slice()) {
        // `excluded_validator` receives a corrupted witness for a chunk belonging to the previous block.
        let witness_size = bytes.len();
        let client = env.client(&excluded_validator);
        match client.process_chunk_state_witness(
            witness,
            witness_size,
            None,
            client.validator_signer.get(),
        ) {
            Ok(_) => {
                if let Some(field) = find_field_for_bit(corrupted_bit_idx, witness_fields) {
                    Err(anyhow::anyhow!(
                        "Successfully processed witness with {} bit flipped. It corresponds to the {} ChunkStateWitness struct field.",
                        corrupted_bit_idx,
                        field,
                    ))
                } else {
                    Err(anyhow::anyhow!("Bit {} was out of known ranges.", corrupted_bit_idx))
                }
            }
            Err(e) => {
                if let Err(e) = client.process_chunk_state_witness(
                    correct_witness,
                    witness_size,
                    None,
                    client.validator_signer.get(),
                ) {
                    Err(anyhow::anyhow!(
                        "Unable to process witness after flipping bit {}. {}",
                        corrupted_bit_idx,
                        e
                    ))
                } else {
                    Ok(e.into())
                }
            }
        }
    } else {
        Ok(anyhow::anyhow!(STATE_WITNESS_NOT_PARSED_MSG))
    }
}

// Returns false if the bit index is out of bounds.
fn bit_flip(bytes: &mut Vec<u8>, corrupted_bit_idx: usize) -> bool {
    if corrupted_bit_idx >= bytes.len() * 8 {
        return false;
    }

    // get indices
    let byte_idx = corrupted_bit_idx / 8;
    let bit_idx = corrupted_bit_idx % 8;

    // flip
    bytes[byte_idx] ^= 1 << bit_idx;
    true
}

fn borsh_size(witness: &ChunkStateWitness) -> ChunkStateWitnessSize {
    borsh::to_vec(&witness).unwrap().len()
}
