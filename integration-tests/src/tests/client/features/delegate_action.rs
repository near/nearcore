//! DelegateAction is a type of action to support meta transactions.
//!
//! NEP: https://github.com/near/NEPs/pull/366
//! This is the module for its integration tests.

use crate::tests::client::process_blocks::create_nightshade_runtimes;
use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_primitives::errors::{ActionsValidationError, InvalidTxError, TxExecutionError};
use near_primitives::transaction::Action;
use near_primitives::types::AccountId;
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_primitives::views::FinalExecutionStatus;
use nearcore::config::GenesisExt;

fn exec_meta_transaction(
    actions: Vec<Action>,
    protocol_version: ProtocolVersion,
) -> FinalExecutionStatus {
    near_o11y::testonly::init_test_logger();
    let validator: AccountId = "test0".parse().unwrap();
    let user: AccountId = "alice.near".parse().unwrap();
    let receiver: AccountId = "bob.near".parse().unwrap();
    let relayer: AccountId = "relayer.near".parse().unwrap();
    let mut genesis =
        Genesis::test(vec![validator, user.clone(), receiver.clone(), relayer.clone()], 1);
    genesis.config.epoch_length = 1000;
    genesis.config.protocol_version = protocol_version;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();

    let tx = env.meta_tx_from_actions(actions, user, relayer, receiver);

    match env.execute_tx(tx) {
        Ok(outcome) => outcome.status,
        Err(status) => FinalExecutionStatus::Failure(TxExecutionError::InvalidTxError(status)),
    }
}

/// Basic test to ensure the happy path works.
#[test]
fn accept_valid_meta_tx() {
    let protocol_version = ProtocolFeature::DelegateAction.protocol_version();
    let status = exec_meta_transaction(vec![], protocol_version);
    assert!(matches!(status, FinalExecutionStatus::SuccessValue(_)), "{status:?}",);
}

/// During the protocol upgrade phase, before the voting completes, we must not
/// include meta transaction on the chain.
///
/// Imagine a validator with an updated binary. A malicious node sends it a meta
/// transaction to execute before the upgrade has finished. We must ensure the
/// validator will not attempt adding it to the change unless the protocol
/// upgrade has completed.
///
/// Note: This does not prevent problems on the network layer that might arise
/// by having different interpretation of what a valid `SignedTransaction` might
/// be. We must catch that earlier.
#[test]
fn reject_valid_meta_tx_in_older_versions() {
    let protocol_version = ProtocolFeature::DelegateAction.protocol_version() - 1;

    let status = exec_meta_transaction(vec![], protocol_version);
    assert!(
        matches!(
                &status,
                FinalExecutionStatus::Failure(
                    TxExecutionError::InvalidTxError(
                        InvalidTxError::ActionsValidation(
                            ActionsValidationError::UnsupportedProtocolFeature{ protocol_feature, version }
                        )
                    )
                )
                if protocol_feature == "DelegateAction" && *version == ProtocolFeature::DelegateAction.protocol_version()
        ),
        "{status:?}",
    );
}
