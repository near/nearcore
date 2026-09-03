use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::tests::gas_keys::get_gas_key_nonce;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::AccessKey;
use near_primitives::action::AddKeyAction;
use near_primitives::action::delegate::{DelegateActionV2, VersionedSignedDelegateAction};
use near_primitives::errors::{ActionsValidationError, InvalidTxError};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::{Action, SignedTransaction, TransactionNonce, TransferAction};
use near_primitives::types::{AccountId, Balance, Nonce, NonceIndex};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::{MIN_SUPPORTED_PROTOCOL_VERSION, PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;

const NONCE_INDEX: NonceIndex = 0;

/// Build a meta transaction: the relayer submits a `DelegateV2` that the sender
/// signed with a gas key, wrapping a transfer to `receiver`.
fn gas_key_meta_tx(
    env: &TestLoopEnv,
    sender: &AccountId,
    relayer: &AccountId,
    receiver: &AccountId,
    gas_key_signer: &Signer,
    gas_key_nonce: Nonce,
) -> SignedTransaction {
    let delegate_action = DelegateActionV2 {
        sender_id: sender.clone(),
        receiver_id: receiver.clone(),
        actions: vec![
            Action::Transfer(TransferAction { deposit: Balance::from_near(1) }).try_into().unwrap(),
        ],
        nonce: TransactionNonce::from_nonce_and_index(gas_key_nonce, NONCE_INDEX),
        max_block_height: 1_000_000,
        public_key: gas_key_signer.public_key(),
    };
    let signed_delegate =
        VersionedSignedDelegateAction::sign(gas_key_signer, delegate_action.into());
    env.rpc_node().tx_from_actions(
        relayer,
        sender,
        vec![Action::DelegateV2(Box::new(signed_delegate))],
    )
}

/// Run gas key meta transactions while the network upgrades to the protocol
/// version that removes `Action::DelegateV2`, and check the results.
#[test]
fn test_reject_delegate_v2_protocol_upgrade() {
    init_test_logger();

    if !ProtocolFeature::RejectDelegateV2.enabled(PROTOCOL_VERSION) {
        return;
    }

    let new_protocol = ProtocolFeature::RejectDelegateV2.protocol_version();
    let old_protocol = new_protocol - 1;
    assert!(
        old_protocol >= MIN_SUPPORTED_PROTOCOL_VERSION,
        "no supported protocol version still admits DelegateV2, so there is nothing left to \
         test here - remove this test"
    );

    let sender = create_account_id("alice");
    let relayer = create_account_id("relayer");
    let receiver = create_account_id("zoe");
    let epoch_length = 10;

    // Boundary "mm": "alice" lands on the first shard, "relayer" and "zoe" on
    // the second, so the delegate receipt crosses a shard on its way back.
    let shard_layout = ShardLayout::multi_shard_custom(vec![create_account_id("mm")], 1);

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(old_protocol)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(new_protocol))
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .add_user_account(&sender, Balance::from_near(1_000))
        .add_user_account(&relayer, Balance::from_near(1_000))
        .add_user_account(&receiver, Balance::from_near(1_000))
        .build();

    let gas_key_signer: Signer =
        InMemorySigner::from_seed(sender.clone(), KeyType::ED25519, "gas_key").into();
    let add_key_tx = env.rpc_node().tx_from_actions(
        &sender,
        &sender,
        vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: gas_key_signer.public_key(),
            access_key: AccessKey::gas_key_full_access(1),
        }))],
    );
    env.rpc_runner().run_tx(add_key_tx, Duration::seconds(10));

    // The loop below submits without waiting for execution, so the nonce at the
    // head block goes stale. Read it once and count up from there.
    let mut gas_key_nonce =
        get_gas_key_nonce(&env, &sender, &gas_key_signer.public_key(), NONCE_INDEX);
    let mut next_gas_key_nonce = || {
        gas_key_nonce += 1;
        gas_key_nonce
    };

    // Before the upgrade the meta transaction is admitted and executes. The
    // upgrade takes ~2 epochs with an immediate voting schedule, so we are
    // comfortably still on the old protocol right after the AddKey.
    assert_eq!(
        env.rpc_node().protocol_version_at_head(),
        old_protocol,
        "expected to start pre-upgrade"
    );
    let tx =
        gas_key_meta_tx(&env, &sender, &relayer, &receiver, &gas_key_signer, next_gas_key_nonce());
    let outcome = env
        .rpc_runner()
        .execute_tx(tx, Duration::seconds(10))
        .expect("meta transaction admitted pre-upgrade");
    assert_matches!(
        outcome.status,
        FinalExecutionStatus::SuccessValue(_),
        "pre-upgrade meta transaction should execute",
    );

    // Keep submitting a meta transaction at every height across the upgrade
    // boundary. Nothing should crash: transactions admitted just before the
    // upgrade produce receipts that may only execute after it, and those
    // existing receipts must still be tolerated.
    let mut blocks_after_upgrade = 0;
    let mut iterations = 0;
    while blocks_after_upgrade < 5 {
        iterations += 1;
        assert!(iterations < 20 * epoch_length, "the upgrade never happened");
        let tx = gas_key_meta_tx(
            &env,
            &sender,
            &relayer,
            &receiver,
            &gas_key_signer,
            next_gas_key_nonce(),
        );
        env.rpc_node().submit_tx(tx);
        env.rpc_runner().run_for_number_of_blocks(1);
        if env.rpc_node().protocol_version_at_head() >= new_protocol {
            blocks_after_upgrade += 1;
        }
    }

    // After the upgrade the meta transaction is rejected at admission.
    assert!(ProtocolFeature::RejectDelegateV2.enabled(env.rpc_node().protocol_version_at_head()));
    let tx =
        gas_key_meta_tx(&env, &sender, &relayer, &receiver, &gas_key_signer, next_gas_key_nonce());
    let err = env
        .rpc_runner()
        .execute_tx(tx, Duration::seconds(10))
        .expect_err("meta transaction should be rejected post-upgrade");
    assert_matches!(
        err,
        InvalidTxError::ActionsValidation(ActionsValidationError::RemovedProtocolFeature {
            ref protocol_feature,
            ..
        }) if protocol_feature == "DelegateV2",
        "post-upgrade meta transaction should be rejected as a removed feature, got {err:?}",
    );
}
