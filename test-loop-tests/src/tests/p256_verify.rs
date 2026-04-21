use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use base64::Engine;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::errors::{ActionError, ActionErrorKind, TxExecutionError};
use near_primitives::gas::Gas;
use near_primitives::types::Balance;
use near_primitives::version::ProtocolFeature;
use near_primitives::views::FinalExecutionStatus;
use p256::ecdsa::{Signature, SigningKey, VerifyingKey, signature::Signer};

/// Deterministic signing key (shared with the vm-runner unit tests). RustCrypto
/// uses RFC6979, so `sign` on the same key+message always yields the same
/// signature.
const DETERMINISTIC_SECRET: [u8; 32] = [
    0xc9, 0xaf, 0xa9, 0xd8, 0x45, 0xba, 0x75, 0x16, 0x6b, 0x5c, 0x21, 0x57, 0x67, 0xb1, 0xd6, 0x93,
    0x4e, 0x50, 0xc3, 0xdb, 0x36, 0xe8, 0x9b, 0x12, 0x7b, 0x8a, 0x62, 0x2b, 0x12, 0x0f, 0x67, 0x21,
];

fn sign_and_keys(message: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let signing_key = SigningKey::from_bytes(&DETERMINISTIC_SECRET.into()).unwrap();
    let signature: Signature = signing_key.sign(message);
    let public_key = VerifyingKey::from(&signing_key).to_encoded_point(true).as_bytes().to_vec();
    (signature.to_vec(), public_key)
}

/// Build the JSON payload `rs_contract`'s `ext_p256_verify` export expects.
fn ext_p256_verify_args(signature: &[u8], message: &[u8], public_key: &[u8]) -> Vec<u8> {
    let engine = &base64::engine::general_purpose::STANDARD;
    serde_json::to_vec(&serde_json::json!({
        "signature": engine.encode(signature),
        "message": engine.encode(message),
        "public_key": engine.encode(public_key),
    }))
    .unwrap()
}

/// End-to-end: deploy `rs_contract` and invoke `ext_p256_verify` with a
/// correctly-signed message. The host function returns 1 and the contract
/// forwards it back as an 8-byte little-endian `u64`.
#[test]
fn test_p256_verify_valid_returns_1() {
    init_test_logger();

    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&user, Balance::from_near(100))
        .build();

    let message = [7u8; 32];
    let (signature, public_key) = sign_and_keys(&message);

    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&user);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call_tx = env.rpc_node().tx_call(
        &user,
        &user,
        "ext_p256_verify",
        ext_p256_verify_args(&signature, &message, &public_key),
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    match &outcome.status {
        FinalExecutionStatus::SuccessValue(bytes) => {
            assert_eq!(bytes.len(), 8, "expected 8-byte u64 return");
            let value = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            assert_eq!(value, 1, "valid signature should return 1");
        }
        other => panic!("expected SuccessValue(1), got {:?}", other),
    }
}

/// End-to-end: `ext_p256_verify` with a tampered message returns 0 (the host
/// function itself succeeded, it just couldn't verify).
#[test]
fn test_p256_verify_invalid_returns_0() {
    init_test_logger();

    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&user, Balance::from_near(100))
        .build();

    let message = [7u8; 32];
    let (signature, public_key) = sign_and_keys(&message);
    let mut tampered = message;
    tampered[0] ^= 0x01;

    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&user);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call_tx = env.rpc_node().tx_call(
        &user,
        &user,
        "ext_p256_verify",
        ext_p256_verify_args(&signature, &tampered, &public_key),
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    match &outcome.status {
        FinalExecutionStatus::SuccessValue(bytes) => {
            assert_eq!(bytes.len(), 8, "expected 8-byte u64 return");
            let value = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            assert_eq!(value, 0, "tampered message should return 0");
        }
        other => panic!("expected SuccessValue(0), got {:?}", other),
    }
}

/// Before the protocol version that activates `p256_verify`, a contract that
/// imports `env.p256_verify` must fail at call time (the import can't be
/// linked against a runtime that doesn't expose it).
#[test]
fn test_p256_verify_pre_activation_call_fails() {
    init_test_logger();

    let pre_activation_pv = ProtocolFeature::P256Verify.protocol_version() - 1;
    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(pre_activation_pv)
        .add_user_account(&user, Balance::from_near(100))
        .build();

    let message = [7u8; 32];
    let (signature, public_key) = sign_and_keys(&message);

    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&user);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call_tx = env.rpc_node().tx_call(
        &user,
        &user,
        "ext_p256_verify",
        ext_p256_verify_args(&signature, &message, &public_key),
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    // The exact error variant (LinkError vs CompilationError) is VM-specific
    // and protocol-dependent, so just assert that the function call failed.
    assert_matches!(
        &outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::FunctionCallError(_),
            ..
        }))
    );
}
