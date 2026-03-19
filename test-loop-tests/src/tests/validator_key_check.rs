use crate::setup::builder::TestLoopBuilder;
use near_crypto::KeyType;
use near_o11y::testonly::init_test_logger;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use std::sync::Arc;

#[test]
#[should_panic(expected = "validator key mismatch")]
fn test_validator_key_mismatch_at_startup() {
    init_test_logger();

    let epoch_length = 5;
    let mut env = TestLoopBuilder::new().validators(4, 0).epoch_length(epoch_length).build();

    // Run a few blocks into the epoch so the node is a validator mid-epoch.
    env.validator_runner().run_until_head_height(3);

    let restart_node_data = &env.node_datas[0];
    let restart_account = restart_node_data.account_id.clone();
    let restart_identifier = restart_node_data.identifier.clone();
    let mut killed_node_state = env.kill_node(&restart_identifier);

    // Replace the signer with one that has a different key for the same account.
    let wrong_signer =
        InMemoryValidatorSigner::from_seed(restart_account, KeyType::ED25519, "wrong_seed");
    killed_node_state.validator_signer = Some(Arc::new(wrong_signer));

    // Restart should panic in ClientActor::new() via check_validator_key_for_epoch.
    env.restart_node(&format!("{}-restart", restart_identifier), killed_node_state);
}

#[test]
#[should_panic(expected = "validator key mismatch")]
fn test_validator_key_mismatch_at_epoch_boundary() {
    init_test_logger();

    let epoch_length = 5;
    let mut env = TestLoopBuilder::new().validators(4, 0).epoch_length(epoch_length).build();

    // Run most of the first epoch (stop 1 block before boundary).
    env.validator_runner().run_until_head_height(epoch_length - 1);

    // Swap validator 0's signer to one with a different key but same account_id.
    let account_id = env.node_datas[0].account_id.clone();
    let wrong_signer =
        InMemoryValidatorSigner::from_seed(account_id, KeyType::ED25519, "wrong_seed");
    env.validator_mut().client_actor().client.validator_signer.update(Some(Arc::new(wrong_signer)));

    // Advance past the epoch boundary to trigger check_validator_key_for_epoch.
    env.validator_runner().run_for_number_of_blocks(1);
}
