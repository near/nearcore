//! Non-refundable transfers during account creation allow to sponsor an
//! accounts storage staking balance without that someone being able to run off
//! with the money.
//!
//! This feature introduces TransferV2
//!
//! NEP: https://github.com/near/NEPs/pull/491

use crate::tests::client::utils::TestEnvNightshadeSetupExt;
use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::errors::{ActionsValidationError, InvalidTxError};
use near_primitives::transaction::{Action, TransferActionV2};
use near_primitives::types::Balance;
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_primitives::views::FinalExecutionOutcomeView;
use nearcore::config::GenesisExt;

// TODO: Test for refundable transfer V2 successfully adding balance
// TODO: Test non-refundable transfer is rejected on existing account
// TODO: Test for non-refundable transfer V2 successfully adding non-refundable balance when creating implicit account
// TODO: Test for non-refundable transfer V2 successfully adding non-refundable balance when creating named account
// TODO: Test non-refundable balance allowing to have account with zero balance and more than 1kB of state

/// During the protocol upgrade phase, before the voting completes, we must not
/// include transaction V" actions on the chain.
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

fn exec_transfer_v2(
    protocol_version: ProtocolVersion,
    deposit: Balance,
    nonrefundable: bool,
) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");

    genesis.config.protocol_version = protocol_version;

    let mut env = TestEnv::builder(ChainGenesis::test())
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let transfer = Action::TransferV2(TransferActionV2 { deposit, nonrefundable });
    let tx = env.tx_from_actions(vec![transfer], &signer, "test1".parse().unwrap());

    let status = env.execute_tx(tx);
    status
}
