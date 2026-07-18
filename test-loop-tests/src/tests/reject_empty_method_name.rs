use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::FunctionCallAction;
use near_primitives::errors::{
    ActionError, ActionErrorKind, ActionsValidationError, FunctionCallError, InvalidTxError,
    MethodResolveError, TxExecutionError,
};
use near_primitives::gas::Gas;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::{Action, SignedTransaction};
use near_primitives::types::{AccountId, Balance, ProtocolVersion};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;

fn protocol_version_at_head(env: &TestLoopEnv) -> ProtocolVersion {
    let head = env.rpc_node().head();
    env.rpc_node().client().epoch_manager.get_epoch_protocol_version(&head.epoch_id).unwrap()
}

/// Run a transaction with an empty `method_name`.
fn empty_method_tx(
    env: &TestLoopEnv,
    signer: &AccountId,
    contract: &AccountId,
) -> SignedTransaction {
    env.rpc_node().tx_from_actions(
        signer,
        contract,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: String::new(),
            args: vec![],
            gas: Gas::from_teragas(30),
            deposit: Balance::ZERO,
        }))],
    )
}

/// Test the `RejectEmptyMethodName` protocol feature across a protocol upgrade. Run a bunch of
/// empty-method FunctionCall transactions while the network updates to new protocol and check the
/// results.
// TODO(spice-test): This test can be removed once the RejectEmptyMethodName feature is stabilized.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_reject_empty_method_name_protocol_upgrade() {
    init_test_logger();

    if !ProtocolFeature::RejectEmptyMethodName.enabled(PROTOCOL_VERSION) {
        return;
    }

    let new_protocol = ProtocolFeature::RejectEmptyMethodName.protocol_version();
    let old_protocol = new_protocol - 1;

    let signer = create_account_id("alice");
    let contract = create_account_id("contract");
    let epoch_length = 10;

    // Boundary "bb": "alice" lands on the first shard, "contract" on the second.
    let shard_layout = ShardLayout::multi_shard_custom(vec![create_account_id("bb")], 1);

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(old_protocol)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(new_protocol))
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .add_user_account(&signer, Balance::from_near(1_000))
        .add_user_account(&contract, Balance::from_near(1_000))
        .build();

    // Deploy a contract
    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&contract);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(10));

    // Before the upgrade: the transaction is admitted and fails on-chain in the VM with the old
    // MethodEmptyName error. The upgrade takes ~2 epochs with an immediate voting schedule, so we
    // are comfortably still on the old protocol right after the deploy.
    assert_eq!(protocol_version_at_head(&env), old_protocol, "expected to start pre-upgrade");
    let tx = empty_method_tx(&env, &signer, &contract);
    let outcome = env
        .rpc_runner()
        .execute_tx(tx, Duration::seconds(10))
        .expect("empty-method tx admitted pre-upgrade");
    assert_matches!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodEmptyName
            )),
            ..
        })),
        "pre-upgrade empty-method call should abort with the old MethodEmptyName error",
    );

    // Keep submitting an empty-method transaction at every height across the upgrade boundary.
    // Nothing should crash: transactions admitted just before the upgrade produce receipts that
    // may only execute after it, and those existing receipts must be tolerated.
    let mut blocks_after_upgrade = 0;
    let mut iterations = 0;
    while blocks_after_upgrade < 5 {
        iterations += 1;
        assert!(iterations < 20 * epoch_length, "the upgrade never happened");
        let tx = empty_method_tx(&env, &signer, &contract);
        env.rpc_node().submit_tx(tx);
        env.rpc_runner().run_for_number_of_blocks(1);
        if protocol_version_at_head(&env) >= new_protocol {
            blocks_after_upgrade += 1;
        }
    }

    // After the upgrade: the transaction is rejected at admission with the new error.
    assert!(ProtocolFeature::RejectEmptyMethodName.enabled(protocol_version_at_head(&env)));
    let tx = empty_method_tx(&env, &signer, &contract);
    let err = env
        .rpc_runner()
        .execute_tx(tx, Duration::seconds(10))
        .expect_err("empty-method tx should be rejected post-upgrade");
    assert_matches!(
        err,
        InvalidTxError::ActionsValidation(ActionsValidationError::FunctionCallEmptyMethodName),
        "post-upgrade empty-method call should be rejected with the new error",
    );
}
