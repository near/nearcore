use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;
use near_chain::Provenance;
use near_chain::stateless_validation::chunk_validation::{self, MainStateTransitionCache};
use near_primitives::block_header::BlockHeader;
use near_primitives::test_utils::create_test_signer;
use near_primitives::trie_split::TrieSplit;
use near_primitives::types::{AccountId, ShardId};
use near_primitives_core::version::{PROTOCOL_VERSION, ProtocolFeature};
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::sync::Arc;

/// Test if a forged proposed_split in a chunk header gets caught by chunk state witness validation.
#[test]
// Stateless validation (state witnesses) is disabled under SPICE.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn chunk_header_proposed_split_validation() {
    near_o11y::testonly::init_integration_logger();

    let accounts: Vec<AccountId> = (0..4).map(|i| format!("test{i}").parse().unwrap()).collect();
    let genesis = near_chain_configs::Genesis::test(accounts.clone(), accounts.len() as u64);
    let mut env = TestEnv::builder(&genesis.config)
        .validators(accounts.clone())
        .clients(accounts)
        .nightshade_runtimes(&genesis)
        .build();

    // --- Phase 1: Produce blocks to generate real state witnesses ---

    fn produce_block(env: &mut TestEnv, height: u64) -> Arc<near_primitives::block::Block> {
        let tip = env.clients[0].chain.head().unwrap();
        let block_producer = env.get_block_producer_at_offset(&tip, height - tip.height);
        env.client(&block_producer).produce_block(height).unwrap().unwrap()
    }

    // Process block 1 normally.
    let block1 = produce_block(&mut env, 1);
    for i in 0..env.clients.len() {
        env.clients[i].process_block_test(block1.clone().into(), Provenance::NONE).unwrap();
    }
    env.process_partial_encoded_chunks();
    for j in 0..env.clients.len() {
        env.process_shards_manager_responses_and_finish_processing_blocks(j);
    }
    env.propagate_chunk_state_witnesses(false);
    env.propagate_chunk_endorsements(false);

    // Process block 2, but intercept the witness.
    let block2 = produce_block(&mut env, 2);
    for i in 0..env.clients.len() {
        env.clients[i].process_block_test(block2.clone().into(), Provenance::NONE).unwrap();
    }
    env.process_partial_encoded_chunks();
    for j in 0..env.clients.len() {
        env.process_shards_manager_responses_and_finish_processing_blocks(j);
    }

    // --- Phase 2: Intercept the real state witness ---

    let mut intercepted_witness = None;
    for adapter in &env.partial_witness_adapters {
        while let Some(request) = adapter.pop_distribution_request() {
            intercepted_witness = Some(request.state_witness);
        }
    }
    let witness = intercepted_witness.expect("should have intercepted a state witness");

    // --- Phase 3: Pre-validate and tamper with the main transition ---

    let client = &env.clients[0];
    let chain_store = client.chain.chain_store();
    let genesis_block = client.chain.genesis_block();
    let epoch_manager = client.epoch_manager.as_ref();

    let mut pre_validation_output = chunk_validation::pre_validate_chunk_state_witness(
        &witness,
        chain_store,
        genesis_block,
        epoch_manager,
    )
    .expect("pre-validation should succeed");

    // Forge proposed_split in the main transition's NewChunkData. The real value is None
    // (no resharding configured), so any non-None value will cause a mismatch.
    let forged_split = Some(TrieSplit::new("aurora".parse().unwrap(), 500_000_000, 500_000_000));
    match &mut pre_validation_output.main_transition_params {
        chunk_validation::MainTransition::NewChunk { new_chunk_data, .. } => {
            new_chunk_data.proposed_split = forged_split;
        }
        _ => unreachable!("expected NewChunk main transition"),
    }

    // --- Phase 4: Witness validation must reject the forged proposed_split ---

    let runtime_adapter = client.runtime_adapter.as_ref();
    let cache: MainStateTransitionCache = Default::default();
    let data_parts = epoch_manager.num_data_parts();
    let parity_parts = epoch_manager.num_total_parts() - data_parts;
    let rs = Arc::new(ReedSolomon::new(data_parts, parity_parts).unwrap());

    let result = chunk_validation::validate_chunk_state_witness_impl(
        witness,
        pre_validation_output,
        epoch_manager,
        runtime_adapter,
        &cache,
        rs,
    );

    assert!(matches!(result, Err(near_chain::Error::InvalidChunkHeaderShardSplit(_))));
}

#[test]
fn block_header_shard_split_validation() {
    // shard_split is included in block headers only if dynamic resharding is enabled
    if !ProtocolFeature::DynamicResharding.enabled(PROTOCOL_VERSION) {
        return;
    }

    near_o11y::testonly::init_integration_logger();

    let accounts: Vec<AccountId> = (0..4).map(|i| format!("test{i}").parse().unwrap()).collect();
    let genesis = near_chain_configs::Genesis::test(accounts.clone(), accounts.len() as u64);
    let mut env = TestEnv::builder(&genesis.config)
        .validators(accounts.clone())
        .clients(accounts)
        .nightshade_runtimes(&genesis)
        .build();

    // --- Phase 1: Produce a real block to get valid header field values ---

    let tip = env.clients[0].chain.head().unwrap();
    let block_producer = env.get_block_producer_at_offset(&tip, 1);
    let block = env.client(&block_producer).produce_block(1).unwrap().unwrap();
    for i in 0..env.clients.len() {
        env.clients[i].process_block_test(block.clone().into(), Provenance::NONE).unwrap();
    }
    let header = block.header();
    assert_eq!(header.shard_split(), None, "real block should not have shard_split set");

    // --- Phase 2: Create a forged block header with shard_split ---

    // A malicious block producer forges shard_split to contain a duplicate
    // boundary account ("aurora"), which could cause an error in shard layout derivation.
    let forged_shard_split: Option<(ShardId, AccountId)> =
        Some((ShardId::new(0), "aurora".parse().unwrap()));

    let signer = create_test_signer(block_producer.as_ref());
    let forged_header = BlockHeader::new(
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        header.height(),
        *header.prev_hash(),
        header.block_body_hash().unwrap(),
        *header.prev_state_root(),
        *header.prev_chunk_outgoing_receipts_root(),
        *header.chunk_headers_root(),
        *header.chunk_tx_root(),
        *header.outcome_root(),
        header.raw_timestamp(),
        *header.random_value(),
        header.prev_validator_proposals().collect(),
        header.chunk_mask().to_vec(),
        header.block_ordinal(),
        *header.epoch_id(),
        *header.next_epoch_id(),
        header.next_gas_price(),
        header.total_supply(),
        &signer,
        *header.last_final_block(),
        *header.last_ds_final_block(),
        header.epoch_sync_data_hash(),
        header.approvals().to_vec(),
        *header.next_bp_hash(),
        *header.block_merkle_root(),
        header.prev_height().unwrap_or(0),
        header.chunk_endorsements().cloned(),
        forged_shard_split.clone(), // FORGED shard_split
    );

    // Sanity: the forged header is V6 and carries the forged shard_split.
    assert_eq!(
        forged_header.shard_split(),
        forged_shard_split.as_ref(),
        "forged V6 header must carry the forged shard_split"
    );

    // --- Phase 3: Block validation must reject the forged shard_split ---

    let epoch_manager = env.clients[0].epoch_manager.as_ref();
    let result = near_chain::validate::validate_block_shard_split(
        epoch_manager,
        &forged_header,
        &block.chunks(),
    );

    assert!(matches!(result, Err(near_chain::Error::InvalidBlockHeaderShardSplit(_))));
}
