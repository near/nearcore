use crate::setup::builder::TestLoopBuilder;
use near_crypto::KeyType;
use near_o11y::testonly::init_test_logger;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use std::sync::Arc;

#[test]
#[should_panic(expected = "validator key mismatch")]
fn test_validator_key_mismatch_at_epoch_boundary() {
    init_test_logger();

    let epoch_length = 5;
    let mut env = TestLoopBuilder::new().validators(4, 0).epoch_length(epoch_length).build();

    // Run most of the first epoch.
    env.validator_runner().run_until_head_height(epoch_length - 1);

    // Swap validator 0's signer to one with a different key but same account_id.
    let account_id = env.node_datas[0].account_id.clone();
    let wrong_signer =
        InMemoryValidatorSigner::from_seed(account_id, KeyType::ED25519, "wrong_seed");
    env.validator_mut().client_actor().client.validator_signer.update(Some(Arc::new(wrong_signer)));

    // Advance into the next epoch; the first head block of the new epoch triggers
    // check_validator_key_for_epoch and should panic on the key mismatch.
    env.validator_runner().run_until_new_epoch();
}
