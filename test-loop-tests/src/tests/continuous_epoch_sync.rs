use near_async::time::Duration;
use near_epoch_manager::epoch_sync::derive_epoch_sync_proof_from_last_block;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_store::adapter::StoreAdapter;
use near_store::adapter::epoch_store::EpochStoreAdapter;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};

// Test that epoch sync proof is correctly updated after each epoch.
// Validate the updated proof against derive_epoch_sync_proof_from_last_block.
#[test]
fn test_epoch_sync_proof_update() {
    // This test is only relevant when ContinuousEpochSync is enabled.
    if !ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();
    let epoch_length = 10;
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let epoch_store = env.validator().client().chain.chain_store.epoch_store();

    // Run for 5 epochs
    for _ in 0..5 {
        env.validator_runner().run_until_new_epoch();
    }

    // Run for another 5 epochs
    for _ in 0..5 {
        env.validator_runner().run_until_new_epoch();
        let head_hash = env.validator().head().last_block_hash;
        validate_epoch_sync_proof(&epoch_store, &head_hash);
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// Test that epoch sync proof is correctly updated even in presence of forks.
#[cfg(feature = "test_features")]
#[test]
fn test_epoch_sync_proof_update_with_forks() {
    use near_client::client_actor::AdvProduceBlockHeightSelection;

    // This test is only relevant when ContinuousEpochSync is enabled.
    if !ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();
    let epoch_length = 10;
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    // Run for 5 epochs
    for _ in 0..5 {
        env.validator_runner().run_until_new_epoch();
    }

    // verify if this is the first block of an epoch
    {
        let node = env.validator();
        let chain_store = node.client().chain.chain_store.chain_store();
        let head = chain_store.head().unwrap();
        let prev_block_header = chain_store.get_block_header(&head.prev_block_hash).unwrap();
        assert_ne!(&head.epoch_id, prev_block_header.epoch_id());
    }

    // create a fork
    let head = env.validator().head();
    let height_selection = AdvProduceBlockHeightSelection::NextHeightOnSelectedBlock {
        base_block_height: head.height - 1,
    };
    env.validator_mut().client_actor().adv_produce_blocks_on(3, true, height_selection);

    // verify proof after processing fork
    {
        let node = env.validator();
        let chain_store = node.client().chain.chain_store.chain_store();
        validate_epoch_sync_proof(&chain_store.epoch_store(), &head.last_block_hash);
    }

    // run for another epoch
    env.validator_runner().run_until_new_epoch();

    // verify proof
    {
        let node = env.validator();
        let chain_store = node.client().chain.chain_store.chain_store();
        let head = chain_store.head().unwrap();
        validate_epoch_sync_proof(&chain_store.epoch_store(), &head.last_block_hash);
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// Validate that the epoch sync proof stored in epoch_store matches the one derived
// from derive_epoch_sync_proof_from_last_block.
//
// We pass the first_block_hash of the current epoch T.
// The proof stored in epoch_store is for epoch T-2.
fn validate_epoch_sync_proof(epoch_store: &EpochStoreAdapter, first_block_hash: &CryptoHash) {
    // Get the proof stored in epoch_store, compare this against the expected proof generated
    // from derive_epoch_sync_proof_from_last_block.
    let proof = epoch_store.get_epoch_sync_proof().unwrap().expect("Proof should exist");

    // For derive_epoch_sync_proof_from_last_block, we need to provide the last final block of epoch T-2,
    let first_block_info = epoch_store.get_block_info(first_block_hash).unwrap();
    let last_block_info_in_prev_epoch =
        epoch_store.get_block_info(first_block_info.prev_hash()).unwrap();
    let first_block_hash_in_prev_epoch = last_block_info_in_prev_epoch.epoch_first_block();
    let first_block_info_in_prev_epoch =
        epoch_store.get_block_info(&first_block_hash_in_prev_epoch).unwrap();
    let last_block_hash_in_prev_prev_epoch = first_block_info_in_prev_epoch.prev_hash();

    let expected_proof = derive_epoch_sync_proof_from_last_block(
        &epoch_store,
        &last_block_hash_in_prev_prev_epoch,
        false,
    )
    .unwrap();

    assert_eq!(proof, expected_proof.into_v1());
}
