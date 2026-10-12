//! End-to-end tests for the `universal_state_init_to_account_id` host function.
//!
//! These go through the real import table rather than calling `VMLogic`
//! directly, so they cover linking, argument order and the feature gate. The
//! expected account id is hardcoded: `derive_universal_account_id` is the
//! authority for it and lives in `near-primitives`, out of this crate's reach,
//! so the cross-check against it is a test-loop test.

use crate::ContractCode;
use crate::logic::Config;
use crate::logic::errors::FunctionCallError;
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::types::ReturnData;
use crate::runner::VMKindExt;
use crate::tests::{create_context, test_vm_config, with_vm_variants};
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::VMKind;
use near_primitives_core::universal_state_init::StateInitError;
use std::cell::RefCell;
use std::sync::Arc;

/// The smallest well-formed `UniversalStateInit::V1`: version tag, `code: None`,
/// an empty data map and an empty access-key set, all borsh zeroes.
const EMPTY_STATE_INIT: [u8; 10] = [0; 10];
const EMPTY_STATE_INIT_ACCOUNT_ID: &str = "0u1kajgpx8a97y8ap8y03pvt8kbm2p2cn9k5h17bgw1wa21j88865g"; // cspell:disable-line

/// A contract that derives the id of `state_init` and returns it, exercising the
/// three arguments in order: length, pointer, destination register. If the host
/// refuses the bytes it returns the exit status as eight little-endian bytes instead.
fn derive_wat(state_init: &[u8]) -> String {
    let len = state_init.len();
    let data = wat_data(state_init);
    format!(
        r#"(module
  (import "env" "universal_state_init_to_account_id" (func $derive (param i64 i64 i64) (result i64)))
  (import "env" "value_return" (func $value_return (param i64 i64)))
  (memory (export "memory") 1)

  (data (i32.const 0) "{data}")

  (func (export "main")
    (local $status i64)
    (local.set $status (call $derive (i64.const {len}) (i64.const 0) (i64.const 0)))
    (if (i64.eqz (local.get $status))
      (then
        ;; return register 0 (value_len == u64::MAX selects register mode)
        (call $value_return (i64.const -1) (i64.const 0)))
      (else
        (i64.store (i32.const 512) (local.get $status))
        (call $value_return (i64.const 8) (i64.const 512))))
  )
)"#,
    )
}

fn wat_data(bytes: &[u8]) -> String {
    let mut data = String::new();
    for byte in bytes {
        data.push_str(&format!("\\{:02x}", byte));
    }
    data
}

/// The flags and the host's answer to run a contract under.
#[derive(Clone, Copy)]
struct Setup {
    universal_accounts: bool,
    canonical_universal_state_init: bool,
    /// What the mocked host reports for `check_state_init`.
    state_init_error: Option<StateInitError>,
}

impl Setup {
    /// What a protocol with universal accounts, but without the canonical check, has.
    const BEFORE_CANONICAL: Setup = Setup {
        universal_accounts: true,
        canonical_universal_state_init: false,
        state_init_error: None,
    };
    /// What a protocol with the canonical check has.
    const CANONICAL: Setup = Setup {
        universal_accounts: true,
        canonical_universal_state_init: true,
        state_init_error: None,
    };
}

/// Run `wat` under every available VM with the flags of `setup`. Returns nothing on
/// success; the caller asserts through `expected`: the bytes the contract returns, or
/// `None` for a contract that must fail to link.
fn run_wat(wat: &str, setup: Setup, expected: Option<&[u8]>) {
    let ran = RefCell::new(false);
    with_vm_variants(|vm_kind: VMKind| {
        let mut config = test_vm_config(Some(vm_kind));
        config.universal_accounts = setup.universal_accounts;
        config.canonical_universal_state_init = setup.canonical_universal_state_init;
        let config = Arc::new(config);
        let fees = Arc::new(RuntimeFeesConfig::test());
        let wasm = wat::parse_str(wat).expect("failed to parse wat");
        let code = ContractCode::new(wasm, None);
        let mut fake_external = MockedExternal::with_code(code);
        fake_external.state_init_error = setup.state_init_error;
        let context = create_context(vec![]);
        let gas_counter = context.make_gas_counter(&config);
        let runtime = vm_kind.runtime(Arc::<Config>::clone(&config)).expect("no runtime");
        let outcome = runtime
            .prepare(&fake_external, None, gas_counter, "main")
            .run(&mut fake_external, &context, Arc::clone(&fees))
            .expect("execution failed");

        match expected {
            Some(expected) => {
                assert!(
                    outcome.aborted.is_none(),
                    "contract aborted under {vm_kind:?}: {:?}",
                    outcome.aborted
                );
                let value = match &outcome.return_data {
                    ReturnData::Value(v) => v.clone(),
                    other => panic!("unexpected return data for {vm_kind:?}: {other:?}"),
                };
                assert_eq!(value, expected, "unexpected return value from {vm_kind:?}");
            }
            None => {
                let aborted = outcome
                    .aborted
                    .as_ref()
                    .unwrap_or_else(|| panic!("expected link error from {vm_kind:?}, got none"));
                assert!(
                    matches!(aborted, FunctionCallError::LinkError { .. }),
                    "expected LinkError under {vm_kind:?}, got {aborted:?}",
                );
            }
        }

        *ran.borrow_mut() = true;
    });
    assert!(*ran.borrow(), "no VM variants executed this test");
}

#[test]
fn test_universal_state_init_to_account_id_integration() {
    run_wat(
        &derive_wat(&EMPTY_STATE_INIT),
        Setup::CANONICAL,
        Some(EMPTY_STATE_INIT_ACCOUNT_ID.as_bytes()),
    );
}

#[test]
fn test_universal_state_init_to_account_id_integration_feature_gate_off_fails_to_link() {
    // With the feature disabled the import is not exported, so the contract
    // cannot link.
    run_wat(
        &derive_wat(&EMPTY_STATE_INIT),
        Setup { universal_accounts: false, ..Setup::CANONICAL },
        None,
    );
}

/// The exit status comes back through the real import table, so the signature
/// really is `-> i64`.
#[test]
fn test_universal_state_init_to_account_id_integration_returns_status() {
    for (error, status) in [(StateInitError::Malformed, 1u64), (StateInitError::NonCanonical, 2)] {
        run_wat(
            &derive_wat(&EMPTY_STATE_INIT),
            Setup { state_init_error: Some(error), ..Setup::CANONICAL },
            Some(&status.to_le_bytes()),
        );
    }
}

/// Before `CanonicalUniversalStateInit` the host is not asked to check anything, so the
/// status is `0` and the id is written, whatever the bytes are.
#[test]
fn test_universal_state_init_to_account_id_integration_before_canonical_never_refuses() {
    run_wat(
        &derive_wat(&EMPTY_STATE_INIT),
        Setup { state_init_error: Some(StateInitError::NonCanonical), ..Setup::BEFORE_CANONICAL },
        Some(EMPTY_STATE_INIT_ACCOUNT_ID.as_bytes()),
    );
}
