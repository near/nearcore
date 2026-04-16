use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::errors::{ActionError, ActionErrorKind, FunctionCallError, TxExecutionError};
use near_primitives::gas::Gas;
use near_primitives::types::Balance;
use near_primitives::views::FinalExecutionStatus;
use p256::ecdsa::{Signature, SigningKey, VerifyingKey, signature::Signer};

/// Build a WAT module that invokes `p256_verify` with three inputs laid out
/// contiguously in linear memory and returns the result via `value_return`.
fn p256_verify_wat(
    signature: &[u8],
    message: &[u8],
    public_key: &[u8],
    sig_len_arg: u64,
    pk_len_arg: u64,
) -> Vec<u8> {
    fn bytes_to_data(bytes: &[u8]) -> String {
        let mut s = String::new();
        for byte in bytes {
            s.push_str(&format!("\\{:02x}", byte));
        }
        s
    }

    let sig_hex = bytes_to_data(signature);
    let msg_hex = bytes_to_data(message);
    let pk_hex = bytes_to_data(public_key);
    let msg_len = message.len();

    let wat = format!(
        r#"(module
  (import "env" "p256_verify"
    (func $p256_verify (param i64 i64 i64 i64 i64 i64) (result i64)))
  (import "env" "value_return" (func $value_return (param i64 i64)))
  (memory (export "memory") 1)

  (data (i32.const 0) "{sig_hex}")
  (data (i32.const 128) "{msg_hex}")
  (data (i32.const 192) "{pk_hex}")

  (func (export "main")
    (local $result i64)
    (local.set $result
      (call $p256_verify
        (i64.const {sig_len_arg})
        (i64.const 0)
        (i64.const {msg_len})
        (i64.const 128)
        (i64.const {pk_len_arg})
        (i64.const 192))
    )
    ;; Store the u64 result at offset 256.
    (i64.store (i32.const 256) (local.get $result))
    ;; Return the 8 bytes of that u64 back to the host.
    (call $value_return (i64.const 8) (i64.const 256))
  )
)"#,
    );
    wat::parse_str(&wat).expect("failed to parse wat")
}

/// Deterministic signing key (same as the vm-runner integration tests).
fn make_signing_key() -> SigningKey {
    let secret: [u8; 32] = [
        0xc9, 0xaf, 0xa9, 0xd8, 0x45, 0xba, 0x75, 0x16, 0x6b, 0x5c, 0x21, 0x57, 0x67, 0xb1, 0xd6,
        0x93, 0x4e, 0x50, 0xc3, 0xdb, 0x36, 0xe8, 0x9b, 0x12, 0x7b, 0x8a, 0x62, 0x2b, 0x12, 0x0f,
        0x67, 0x21,
    ];
    SigningKey::from_bytes(&secret.into()).unwrap()
}

fn sign_and_keys(message: &[u8; 32]) -> (Vec<u8>, Vec<u8>) {
    let signing_key = make_signing_key();
    let signature: Signature = signing_key.sign(message);
    let signature_bytes = signature.to_vec();
    let public_key = VerifyingKey::from(&signing_key).to_encoded_point(true).as_bytes().to_vec();
    (signature_bytes, public_key)
}

/// End-to-end test: deploy a WAT contract that calls `p256_verify` with a
/// valid signature and assert the transaction succeeds with return value 1.
/// Then call with a tampered message and assert the return value is 0.
/// Finally, call with an invalid signature length and assert the host error.
#[test]
fn test_p256_verify_e2e() {
    init_test_logger();

    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&user, Balance::from_near(100))
        .build();

    let message = [7u8; 32];
    let (signature, public_key) = sign_and_keys(&message);

    // --- 1. Valid signature: expect return value 1 ---
    let valid_wasm = p256_verify_wat(&signature, &message, &public_key, 64, 33);
    let deploy_tx = env.rpc_node().tx_deploy_contract(&user, valid_wasm);
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
        other => panic!("expected success, got {:?}", other),
    }

    // --- 2. Tampered message: expect return value 0 ---
    let mut tampered_message = message;
    tampered_message[0] ^= 0x01;
    let tampered_wasm = p256_verify_wat(&signature, &tampered_message, &public_key, 64, 33);
    let deploy_tx = env.rpc_node().tx_deploy_contract(&user, tampered_wasm);
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
        other => panic!("expected success, got {:?}", other),
    }

    // --- 3. Invalid signature length: expect HostError abort ---
    let bad_len_wasm = p256_verify_wat(&signature, &message, &public_key, 63, 33);
    let deploy_tx = env.rpc_node().tx_deploy_contract(&user, bad_len_wasm);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call_tx =
        env.rpc_node().tx_call(&user, &user, "main", vec![], Balance::ZERO, Gas::from_teragas(300));
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    // HostError::P256VerifyInvalidInput gets serialized through the views layer as
    // FunctionCallError::ExecutionError with a string message.
    assert_matches!(
        &outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(msg)),
            ..
        })) if msg.contains("invalid signature length")
    );
}
