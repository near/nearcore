use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::errors::{ActionError, ActionErrorKind, TxExecutionError};
use near_primitives::gas::Gas;
use near_primitives::types::Balance;
use near_primitives::views::FinalExecutionStatus;
use p256::ecdsa::{Signature, SigningKey, VerifyingKey, signature::Signer};

/// Build a WASM contract that imports `env.p256_verify`, bakes the supplied
/// `signature || public_key || message` into linear memory at fixed offsets,
/// calls the host function, and returns the resulting `u64` via `value_return`.
///
/// Layout:
///   signature: offset 0, 64 bytes (fixed length)
///   public_key: offset 64, 33 bytes (fixed length)
///   message: offset 97, variable length
///   result (u64): offset 65528 (last 8 bytes of the 64 KiB page)
fn p256_verify_wasm(signature: &[u8], public_key: &[u8], message: &[u8]) -> Vec<u8> {
    fn bytes_to_data(bytes: &[u8]) -> String {
        let mut s = String::new();
        for byte in bytes {
            s.push_str(&format!("\\{:02x}", byte));
        }
        s
    }

    let sig_hex = bytes_to_data(signature);
    let pk_hex = bytes_to_data(public_key);
    let msg_hex = bytes_to_data(message);
    let sig_len = signature.len();
    let pk_len = public_key.len();
    let msg_len = message.len();

    let wat = format!(
        r#"(module
  (import "env" "p256_verify"
    (func $p256_verify (param i64 i64 i64 i64 i64 i64) (result i64)))
  (import "env" "value_return" (func $value_return (param i64 i64)))
  (memory (export "memory") 1)

  (data (i32.const 0) "{sig_hex}")
  (data (i32.const 64) "{pk_hex}")
  (data (i32.const 97) "{msg_hex}")

  (func (export "main")
    (local $result i64)
    (local.set $result
      (call $p256_verify
        (i64.const {sig_len})
        (i64.const 0)
        (i64.const {msg_len})
        (i64.const 97)
        (i64.const {pk_len})
        (i64.const 64))
    )
    ;; Store the u64 result at offset 65528 (last 8 bytes of the 64 KiB page),
    ;; well past any reasonable message so the data segments don't collide
    ;; with the result write.
    (i64.store (i32.const 65528) (local.get $result))
    (call $value_return (i64.const 8) (i64.const 65528))
  )
)"#,
    );
    near_test_contracts::wat_contract(&wat)
}

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

/// End-to-end: deploy a contract that calls `p256_verify` with a correctly-
/// signed message and assert the host function returns 1.
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
    let wasm = p256_verify_wasm(&signature, &public_key, &message);

    let deploy_tx = env.rpc_node().tx_deploy_contract(&user, wasm);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call_tx =
        env.rpc_node().tx_call(&user, &user, "main", vec![], Balance::ZERO, Gas::from_teragas(300));
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

/// End-to-end: same contract shape, but signed over a different message than
/// the one passed to `p256_verify`. The host function returns 0 (verification
/// failure, not an abort).
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
    let wasm = p256_verify_wasm(&signature, &public_key, &tampered);

    let deploy_tx = env.rpc_node().tx_deploy_contract(&user, wasm);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call_tx =
        env.rpc_node().tx_call(&user, &user, "main", vec![], Balance::ZERO, Gas::from_teragas(300));
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
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_p256_verify_pre_activation_call_fails() {
    init_test_logger();

    // p256_verify_host_fn is turned on at protocol version 85 (see 85.yaml).
    let pre_activation_pv = 84;
    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(pre_activation_pv)
        .add_user_account(&user, Balance::from_near(100))
        .build();

    let message = [7u8; 32];
    let (signature, public_key) = sign_and_keys(&message);
    let wasm = p256_verify_wasm(&signature, &public_key, &message);

    let deploy_tx = env.rpc_node().tx_deploy_contract(&user, wasm);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call_tx =
        env.rpc_node().tx_call(&user, &user, "main", vec![], Balance::ZERO, Gas::from_teragas(300));
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
