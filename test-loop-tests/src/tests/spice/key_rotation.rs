use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use near_async::time::Duration;
use near_crypto::KeyType;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, StakeAction};
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::types::Balance;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use std::collections::BTreeSet;
use std::slice::from_ref;
use std::sync::Arc;

/// Under spice a chunk is executed, and so endorsed, after it is produced, which may be after
/// the epoch transition. A validator that rotates its key at the transition then endorses the
/// last chunks of epoch E with its key from E+1. Make sure such endorsements are accepted.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_endorsement_signed_with_next_epoch_key() {
    init_test_logger();

    let epoch_length = 10;
    // The chunk validator holds more than a third of the stake, so chunks can't be certified
    // without its endorsements.
    let validators_spec = create_validators_spec(1, 1);
    let clients = validators_spec_clients(&validators_spec);
    let producer = clients[0].clone();
    let rotating = clients[1].clone();

    let mut env = TestLoopBuilder::new()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .add_user_account(&rotating, Balance::from_near(10))
        .build();

    let new_signer =
        Arc::new(InMemoryValidatorSigner::from_seed(rotating.clone(), KeyType::ED25519, "rotated"));
    let new_key = new_signer.public_key();

    let epoch_manager = env.node_for_account(&producer).client().epoch_manager.clone();
    let head = env.node_for_account(&producer).head();
    let stake =
        epoch_manager.get_validator_by_account_id(&head.epoch_id, &rotating).unwrap().stake();
    let stake_tx = env.node_for_account(&producer).tx_from_actions(
        &rotating,
        &rotating,
        vec![Action::Stake(Box::new(StakeAction { stake, public_key: new_key.clone() }))],
    );
    env.runner_for_account(&producer).run_tx(stake_tx, Duration::seconds(5));

    // Run until the new key takes effect in the next epoch, i.e. the head is in epoch E.
    env.runner_for_account(&producer).run_until(
        |node| {
            let next_epoch_id = node.head().next_epoch_id;
            epoch_manager
                .get_validator_by_account_id(&next_epoch_id, &rotating)
                .unwrap()
                .public_key()
                == &new_key
        },
        Duration::seconds(5 * epoch_length as i64),
    );
    let epoch_e = env.node_for_account(&producer).head().epoch_id;
    assert_ne!(
        epoch_manager.get_validator_by_account_id(&epoch_e, &rotating).unwrap().public_key(),
        &new_key
    );

    // Swap the key a few blocks before the end of E, so that endorsements of the last chunks of E
    // are signed with the key from E+1 like they would be if execution lagged behind.
    let epoch_start_height = epoch_manager
        .get_epoch_start_height(&env.node_for_account(&producer).head().last_block_hash)
        .unwrap();
    let swap_height = epoch_start_height + epoch_length - 3;
    env.runner_for_account(&rotating).run_until_head_height(swap_height);
    assert_eq!(env.node_for_account(&rotating).head().epoch_id, epoch_e);
    env.node_for_account_mut(&rotating)
        .client_actor()
        .client
        .validator_signer
        .update(Some(new_signer));

    // Endorsements of the rotating validator are required for certification, so this stalls if
    // they're rejected.
    let target_height = epoch_start_height + 2 * epoch_length;
    env.runner_for_account(&producer).run_until_certified(target_height);

    // The endorsements signed with the new key for chunks of E are on chain.
    let mut endorsed_heights_in_e = BTreeSet::new();
    let mut block = env.node_for_account(&producer).head_block();
    while block.header().height() > swap_height {
        for statement in block.spice_core_statements() {
            let SpiceCoreStatement::Endorsement(endorsement) = statement else { continue };
            if endorsement.account_id() != &rotating {
                continue;
            }
            let endorsed_block =
                env.node_for_account(&producer).block(endorsement.chunk_id().block_hash);
            if endorsed_block.header().epoch_id() == &epoch_e
                && endorsed_block.header().height() > swap_height
            {
                assert!(
                    endorsement.verified_signed_data(from_ref(&new_key)).is_some(),
                    "endorsement of a chunk after the swap isn't signed with the new key"
                );
                endorsed_heights_in_e.insert(endorsed_block.header().height());
            }
        }
        block = env.node_for_account(&producer).block(*block.header().prev_hash());
    }
    let expected_heights = (swap_height + 1..epoch_start_height + epoch_length).collect();
    assert_eq!(endorsed_heights_in_e, expected_heights);
}
