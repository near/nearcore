use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_validator_ids;
use near_async::time::Duration;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_chain_configs::test_utils::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near_crypto::KeyType;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, StakeAction};
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::AccountInfo;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use std::sync::Arc;

#[test]
#[should_panic(expected = "validator key mismatch")]
fn test_validator_key_mismatch_mid_epoch() {
    init_test_logger();

    let epoch_length = 10;
    let mut env = TestLoopBuilder::new().validators(4, 0).epoch_length(epoch_length).build();

    // Run a few blocks into the epoch.
    env.validator_runner().run_until_head_height(3);

    let restart_node_data = &env.node_datas[0];
    let restart_account = restart_node_data.account_id.clone();
    let restart_identifier = restart_node_data.identifier.clone();
    let mut killed_node_state = env.kill_node(&restart_identifier);

    // Replace the signer with one that has a different key for the same account.
    let wrong_signer =
        InMemoryValidatorSigner::from_seed(restart_account, KeyType::ED25519, "wrong_seed");
    killed_node_state.validator_signer = Some(Arc::new(wrong_signer));

    // Restart the node mid-epoch with the wrong key. last_validator_key_check_epoch
    // starts as None, so the first head block processed triggers the check and panics.
    env.restart_node(&format!("{}-restart", restart_identifier), killed_node_state);
    env.validator_runner().run_for_number_of_blocks(1);
}

#[test]
#[should_panic(expected = "validator key mismatch")]
fn test_validator_key_mismatch_after_on_chain_rotation() {
    init_test_logger();

    let epoch_length = 10;
    let accounts = create_validator_ids(3);

    // accounts[2] will stake on-chain with this key...
    let on_chain_key =
        InMemoryValidatorSigner::from_seed(accounts[2].clone(), KeyType::ED25519, "on_chain")
            .public_key();
    // ...but their local validator_signer holds a different key.
    let local_signer =
        InMemoryValidatorSigner::from_seed(accounts[2].clone(), KeyType::ED25519, "local");

    let validators: Vec<AccountInfo> = accounts[..2]
        .iter()
        .map(|account| AccountInfo {
            account_id: account.clone(),
            public_key: create_test_signer(account.as_str()).public_key(),
            amount: TESTING_INIT_STAKE,
        })
        .collect();
    let mut env = TestLoopBuilder::new()
        .validators_spec(ValidatorsSpec::raw(validators, 2, 2, 2))
        .add_non_validator_client(&accounts[2])
        .add_user_account(&accounts[2], TESTING_INIT_BALANCE)
        .epoch_length(epoch_length)
        .build();

    // Set accounts[2]'s local validator_signer to local_signer (K_local).
    env.node_mut(2).client_actor().client.validator_signer.update(Some(Arc::new(local_signer)));

    // Stake accounts[2] on-chain with on_chain_key (K_staking ≠ K_local). This simulates
    // the primary scenario the check is designed to catch: a validator's on-chain key
    // diverges from their local validator_key.json.
    let stake_tx = env.node(0).tx_from_actions(
        &accounts[2],
        &accounts[2],
        vec![Action::Stake(Box::new(StakeAction {
            stake: TESTING_INIT_STAKE,
            public_key: on_chain_key,
        }))],
    );
    env.node_runner(0).run_tx(stake_tx, Duration::seconds(5));

    // The staked key takes effect after 2 epochs. Run through both epochs from node(2)'s
    // perspective so that node(2) definitely processes the first block of epoch 2, which
    // is when check_validator_key_on_new_head fires and detects the mismatch.
    env.node_runner(2).run_until_new_epoch();
    env.node_runner(2).run_until_new_epoch();
}
