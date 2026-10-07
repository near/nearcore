//! A new transfer to a `0u` id is rejected from the ban until universal accounts are enabled, so
//! no receipt funded without the account creation fee executes once the fee applies.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_crypto::{KeyType, PublicKeyHandle, SecretKey};
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, TransferAction};
use near_primitives::errors::{ActionsValidationError, InvalidTxError};
use near_primitives::types::{AccountId, Balance, ProtocolVersion};
use near_primitives::universal_state_init::{UniversalStateInit, UniversalStateInitV1};
use near_primitives::version::ProtocolFeature;
use near_primitives::views::FinalExecutionStatus;
use std::collections::{BTreeMap, BTreeSet};

fn universal_account_id(seed: &str) -> AccountId {
    let public_key = SecretKey::from_seed(KeyType::ED25519, seed).public_key();
    UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::new(),
        access_keys: BTreeSet::from([PublicKeyHandle::from(public_key)]),
    })
    .derive_account_id()
}

fn env_at(version: ProtocolVersion, signer: &AccountId) -> TestLoopEnv {
    TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(version)
        .add_user_account(signer, Balance::from_near(1_000))
        .build()
}

/// A transfer to a universal account is rejected while transfers to them are banned.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_transfer_to_universal_account_rejected_while_banned() {
    init_test_logger();

    let ban_version = ProtocolFeature::RejectUniversalAccountTransfers.protocol_version();
    // The ban has no effect when universal accounts start in the same version.
    if ProtocolFeature::UniversalAccounts.enabled(ban_version) {
        return;
    }

    let signer = create_account_id("alice");
    let receiver = universal_account_id("banned");
    let mut env = env_at(ban_version, &signer);

    let tx = env.rpc_node().tx_from_actions(
        &signer,
        &receiver,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_near(1) })],
    );
    let err = env
        .rpc_runner()
        .execute_tx(tx, Duration::seconds(10))
        .expect_err("transfer to a universal account should be rejected");
    assert_matches!(
        err,
        InvalidTxError::ActionsValidation(
            ActionsValidationError::TransferToUniversalAccountNotAllowed { .. }
        ),
        "expected the transfer to be rejected as not allowed, got {err:?}",
    );
}

/// Once universal accounts are enabled, the same transfer creates the account.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_transfer_to_universal_account_creates_it_once_enabled() {
    init_test_logger();

    let version = ProtocolFeature::UniversalAccounts.protocol_version();
    let signer = create_account_id("alice");
    let receiver = universal_account_id("allowed");
    let mut env = env_at(version, &signer);

    let tx = env.rpc_node().tx_from_actions(
        &signer,
        &receiver,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_near(1) })],
    );
    let outcome = env.rpc_runner().execute_tx(tx, Duration::seconds(10)).expect("admitted");
    assert_matches!(outcome.status, FinalExecutionStatus::SuccessValue(_));
    assert_eq!(env.rpc_node().query_balance(&receiver), Balance::from_near(1));
}
