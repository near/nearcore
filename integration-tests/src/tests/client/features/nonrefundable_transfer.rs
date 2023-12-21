//! Non-refundable transfers during account creation allow to sponsor an
//! accounts storage staking balance without that someone being able to run off
//! with the money.
//!
//! This feature introduces TransferV2
//!
//! NEP: https://github.com/near/NEPs/pull/491

use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::errors::{ActionsValidationError, InvalidTxError};
use near_primitives::transaction::{Action, TransferActionV2};
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_primitives::views::FinalExecutionOutcomeView;
use nearcore::config::GenesisExt;
use nearcore::test_utils::TestEnvNightshadeSetupExt;

/// Refundable transfer V2 successfully adds balance like a transfer V1.
#[test]
fn transfer_v2() {
    let protocol_version = ProtocolFeature::NonRefundableBalance.protocol_version();
    exec_transfer_v2(protocol_version, 1, false)
        .expect("Transfer V2 should be accepted")
        .assert_success();
}

// TODO: Test non-refundable transfer is rejected on existing account
// TODO: Test for non-refundable transfer V2 successfully adding non-refundable balance when creating implicit account
// TODO: Test for non-refundable transfer V2 successfully adding non-refundable balance when creating named account
// TODO: Test non-refundable balance allowing to have account with zero balance and more than 1kB of state
// TODO: Test non-refundable balance cannot be transferred
// TODO: Test for deleting an account with non-refundable storage (might rip up the balance checker)

/// During the protocol upgrade phase, before the voting completes, we must not
/// include transfer V2 actions on the chain.
///
/// The correct way to handle it is to reject transaction before they even get
/// into the transaction pool. Hence, we check that an `InvalidTxError` error is
/// returned for older protocol versions.
#[test]
fn reject_transfer_v2_in_older_versions() {
    let protocol_version = ProtocolFeature::NonRefundableBalance.protocol_version() - 1;

    let status = exec_transfer_v2(protocol_version, 1, false);
    assert!(
        matches!(
                &status,
                Err(
                    InvalidTxError::ActionsValidation(
                        ActionsValidationError::UnsupportedProtocolFeature{ protocol_feature, version }
                    )
                )
                if protocol_feature == "NonRefundableBalance" && *version == ProtocolFeature::NonRefundableBalance.protocol_version()
        ),
        "{status:?}",
    );
}

/// Sender implicitly used in all test of this module.
fn sender() -> AccountId {
    "test0".parse().unwrap()
}

/// Receiver implicitly used in all test of this module.
fn receiver() -> AccountId {
    "test1".parse().unwrap()
}

/// Creates a test environment and submits a transfer V2 action.
///
/// This methods checks that the balance is subtracted from the sender and added
/// to the receiver, if the status was ok. No checks are done on an error.
fn exec_transfer_v2(
    protocol_version: ProtocolVersion,
    deposit: Balance,
    nonrefundable: bool,
) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
    let mut genesis = Genesis::test(vec![sender(), receiver()], 1);
    let signer = InMemorySigner::from_seed(sender(), KeyType::ED25519, "test0");

    genesis.config.protocol_version = protocol_version;

    let mut env = TestEnv::builder(ChainGenesis::test())
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let sender_pre_balance = env.query_balance(sender());
    let receiver_before = env.query_account(receiver());

    let transfer = Action::TransferV2(Box::new(TransferActionV2 { deposit, nonrefundable }));
    let tx = env.tx_from_actions(vec![transfer], &signer, receiver());

    let status = env.execute_tx(tx);
    let height = env.clients[0].chain.head().unwrap().height;
    for i in 0..2 {
        env.produce_block(0, height + 1 + i);
    }

    if let Ok(outcome) = &status {
        let gas_cost = outcome.gas_cost();
        assert_eq!(sender_pre_balance - deposit - gas_cost, env.query_balance(sender()));

        if matches!(outcome.status, near_primitives::views::FinalExecutionStatus::SuccessValue(_)) {
            let receiver_after = env.query_account(receiver());
            if nonrefundable {
                assert_eq!(receiver_before.amount, receiver_after.amount);
                assert_eq!(receiver_before.nonrefundable + deposit, receiver_after.nonrefundable);
            } else {
                assert_eq!(receiver_before.amount + deposit, receiver_after.amount);
                assert_eq!(receiver_before.nonrefundable, receiver_after.nonrefundable);
            }
        }
    }

    status
}
