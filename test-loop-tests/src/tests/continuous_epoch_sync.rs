use near_async::time::Duration;
use near_epoch_manager::epoch_sync::derive_epoch_sync_proof_from_last_final_block;
use near_o11y::testonly::init_test_logger;
use near_store::adapter::StoreAdapter;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::node::TestLoopNode;

/// This test demonstrates how to trigger missing chunk at a certain height.
/// Requires "test_features" feature to be enabled.
#[test]
fn test_epoch_sync_proof_update() {
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
    let validator_node = TestLoopNode::from(&env.node_datas[0]);

    let client = validator_node.client(env.test_loop_data());
    let epoch_store = client.chain.chain_store.epoch_store();

    // Run for 5 epochs
    for _ in 0..5 {
        validator_node.run_until_new_epoch(&mut env.test_loop);
    }

    // Run for another 5 epochs
    for _ in 0..5 {
        validator_node.run_until_new_epoch(&mut env.test_loop);
        let proof = epoch_store.get_epoch_sync_proof().unwrap().expect("Proof should exist");

        // After run_until_new_epoch, the head is on the first block of the new epoch T.
        // The epoch sync proof is generated for epoch T-2
        // For derive_epoch_sync_proof_from_last_final_block, we need to provide the last final block of epoch T-2,
        let head = validator_node.head(&env.test_loop_data());
        let last_block_info_in_prev_epoch =
            epoch_store.get_block_info(&head.prev_block_hash).unwrap();
        let first_block_hash_in_prev_epoch = last_block_info_in_prev_epoch.epoch_first_block();
        let first_block_info_in_prev_epoch =
            epoch_store.get_block_info(&first_block_hash_in_prev_epoch).unwrap();
        let last_block_hash_in_prev_prev_epoch = first_block_info_in_prev_epoch.prev_hash();

        // Note that the proof lags behind by 2 epochs.
        let expected_proof = derive_epoch_sync_proof_from_last_final_block(
            &epoch_store,
            &last_block_hash_in_prev_prev_epoch,
            false,
        )
        .unwrap();
        assert_eq!(proof, expected_proof.into_v1());
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}
