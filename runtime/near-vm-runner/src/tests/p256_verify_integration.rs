//! End-to-end integration tests for the `p256_verify` host function.
//!
//! These tests build a tiny WASM contract that imports `p256_verify`, loads
//! inputs into memory, invokes the host function, and traps or returns
//! depending on the result. The tests exercise the full pipeline through the
//! real WASM VM (NearVm / Wasmtime), so they cover the imports wiring, memory
//! marshalling, and gas accounting in a way that the logic-only unit tests
//! cannot.

use crate::ContractCode;
use crate::logic::Config;
use crate::logic::errors::{FunctionCallError, HostError};
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::types::ReturnData;
use crate::runner::VMKindExt;
use crate::tests::{create_context, test_vm_config, with_vm_variants};
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::VMKind;
use p256::ecdsa::{Signature, SigningKey, VerifyingKey, signature::hazmat::PrehashSigner};
use std::cell::RefCell;
use std::sync::Arc;

/// Build a WAT module that invokes `p256_verify` with three inputs laid out
/// contiguously in linear memory:
///   signature: offset 0, `sig_len` bytes
///   message:   offset 128, 32 bytes
///   public_key: offset 192, `pk_len` bytes
///
/// The `sig_len_arg` / `pk_len_arg` arguments control what length is passed to
/// the host function, so we can test both the valid and the length-mismatch
/// paths. The result of `p256_verify` is returned via `value_return` as an
/// 8-byte little-endian `u64`.
fn verify_wat(
    signature: &[u8],
    message: &[u8; 32],
    public_key: &[u8],
    sig_len_arg: u64,
    pk_len_arg: u64,
) -> String {
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

    format!(
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
        (i64.const 32)
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
    )
}

/// Deterministic signing key so tests are reproducible.
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
    let signature: Signature = signing_key.sign_prehash(message).unwrap();
    let signature_bytes = signature.to_vec();
    let public_key = VerifyingKey::from(&signing_key).to_encoded_point(true).as_bytes().to_vec();
    (signature_bytes, public_key)
}

enum ExpectedOutcome<'a> {
    /// Execution completes without abort and returns the given u64.
    Value(u64),
    /// Execution aborts with a HostError whose Display contains this
    /// substring.
    HostErrorContains(&'a str),
}

/// Compile a WAT contract and run it under every available VM, asserting that
/// the outcome matches `expected` for each variant and that variants agree
/// with each other.
fn run_wat(wat: &str, expected: ExpectedOutcome) {
    let ran = RefCell::new(false);
    with_vm_variants(|vm_kind: VMKind| {
        let config = Arc::new(test_vm_config(Some(vm_kind)));
        let fees = Arc::new(RuntimeFeesConfig::test());
        let wasm = wat::parse_str(wat).expect("failed to parse wat");
        let code = ContractCode::new(wasm, None);
        let mut fake_external = MockedExternal::with_code(code);
        let context = create_context(vec![]);
        let gas_counter = context.make_gas_counter(&config);
        let runtime = vm_kind.runtime(Arc::<Config>::clone(&config)).expect("no runtime");
        let outcome = runtime
            .prepare(&fake_external, None, gas_counter, "main")
            .run(&mut fake_external, &context, Arc::clone(&fees))
            .expect("execution failed");

        match &expected {
            ExpectedOutcome::Value(want) => {
                assert!(
                    outcome.aborted.is_none(),
                    "contract aborted under {:?}: {:?}",
                    vm_kind,
                    outcome.aborted
                );
                let value = match &outcome.return_data {
                    ReturnData::Value(v) => v.clone(),
                    other => panic!("unexpected return data for {:?}: {:?}", vm_kind, other),
                };
                assert_eq!(value.len(), 8, "expected 8-byte u64 return from {:?}", vm_kind);
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&value);
                assert_eq!(
                    u64::from_le_bytes(arr),
                    *want,
                    "unexpected return value from {:?}",
                    vm_kind
                );
            }
            ExpectedOutcome::HostErrorContains(substr) => {
                let aborted = outcome
                    .aborted
                    .as_ref()
                    .unwrap_or_else(|| panic!("expected abort from {:?}, got none", vm_kind));
                match aborted {
                    FunctionCallError::HostError(HostError::P256VerifyInvalidInput { msg }) => {
                        assert!(
                            msg.contains(substr),
                            "expected P256VerifyInvalidInput msg to contain {:?}, got {:?}",
                            substr,
                            msg,
                        );
                    }
                    other => panic!(
                        "expected P256VerifyInvalidInput error under {:?}, got {:?}",
                        vm_kind, other
                    ),
                }
            }
        }

        *ran.borrow_mut() = true;
    });
    assert!(*ran.borrow(), "no VM variants executed this test");
}

#[test]
fn test_p256_verify_integration_valid_signature() {
    let message = [7u8; 32];
    let (signature, public_key) = sign_and_keys(&message);
    let wat = verify_wat(&signature, &message, &public_key, 64, 33);
    run_wat(&wat, ExpectedOutcome::Value(1));
}

#[test]
fn test_p256_verify_integration_tampered_message() {
    let message = [7u8; 32];
    let (signature, public_key) = sign_and_keys(&message);

    // Flip a bit in the message post-signing. Verification must fail with 0.
    let mut tampered = message;
    tampered[0] ^= 0x01;
    let wat = verify_wat(&signature, &tampered, &public_key, 64, 33);
    run_wat(&wat, ExpectedOutcome::Value(0));
}

#[test]
fn test_p256_verify_integration_unparseable_signature() {
    // 64 bytes of zeros is a parseable length but cannot be parsed as a valid
    // P-256 signature, so the host function should return 0 (not abort).
    let message = [7u8; 32];
    let (_sig, public_key) = sign_and_keys(&message);
    let bad_signature = vec![0u8; 64];
    let wat = verify_wat(&bad_signature, &message, &public_key, 64, 33);
    run_wat(&wat, ExpectedOutcome::Value(0));
}

#[test]
fn test_p256_verify_integration_invalid_signature_length_aborts() {
    // When sig_len != 64, the host function must raise P256VerifyInvalidInput,
    // which propagates as an abort back to the VM.
    let message = [7u8; 32];
    let (signature, public_key) = sign_and_keys(&message);
    let wat = verify_wat(&signature, &message, &public_key, 63, 33);
    run_wat(&wat, ExpectedOutcome::HostErrorContains("invalid signature length"));
}

#[test]
fn test_p256_verify_integration_invalid_public_key_length_aborts() {
    // Valid signature + 64-byte signature length, but invalid pk length.
    // Host function must reach the pk length check and raise
    // P256VerifyInvalidInput.
    let message = [7u8; 32];
    let (signature, public_key) = sign_and_keys(&message);
    let wat = verify_wat(&signature, &message, &public_key, 64, 32);
    run_wat(&wat, ExpectedOutcome::HostErrorContains("invalid public key length"));
}

#[test]
fn test_p256_verify_integration_empty_signature_length_aborts() {
    // sig_len = 0 should also hit the length check.
    let message = [7u8; 32];
    let (signature, public_key) = sign_and_keys(&message);
    let wat = verify_wat(&signature, &message, &public_key, 0, 33);
    run_wat(&wat, ExpectedOutcome::HostErrorContains("invalid signature length"));
}
