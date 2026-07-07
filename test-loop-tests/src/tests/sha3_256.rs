use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfigStore;
use near_primitives::errors::{ActionError, ActionErrorKind, TxExecutionError};
use near_primitives::gas::Gas;
use near_primitives::types::Balance;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::FinalExecutionStatus;

/// SHA3-256("abc"), the FIPS-202 test vector. An independent oracle for the
/// digest the host function must produce (algorithm correctness is also covered
/// by the vm-runner unit test; this exercises the full deploy/call path).
const SHA3_256_ABC: [u8; 32] = [
    58, 152, 93, 167, 79, 226, 37, 178, 4, 92, 23, 45, 107, 211, 144, 189, 133, 95, 8, 110, 62,
    157, 82, 91, 70, 191, 226, 69, 17, 67, 21, 50,
];

/// `sha3_host_fns` has no `ProtocolFeature` variant; it is turned on purely by
/// a runtime-config diff (156.yaml). Check the flag for the current protocol
/// version so the test skips on stable, runs on nightly, and re-enables itself
/// automatically once the feature stabilizes.
fn sha3_256_enabled() -> bool {
    RuntimeConfigStore::new(None).get_config(PROTOCOL_VERSION).wasm_config.sha3_host_fns
}

/// Build a WASM contract that imports `env.sha3_256`, bakes `input` into linear
/// memory at offset 0, hashes it into register 0, and returns the register
/// contents (the 32-byte digest) via `value_return`.
fn sha3_256_wasm(input: &[u8]) -> Vec<u8> {
    let mut data = String::new();
    for byte in input {
        data.push_str(&format!("\\{:02x}", byte));
    }
    let input_len = input.len();
    let wat = format!(
        r#"(module
  (import "env" "sha3_256" (func $sha3_256 (param i64 i64 i64)))
  (import "env" "value_return" (func $value_return (param i64 i64)))
  (memory (export "memory") 1)

  (data (i32.const 0) "{data}")

  (func (export "main")
    ;; hash [0, input_len) into register 0
    (call $sha3_256 (i64.const {input_len}) (i64.const 0) (i64.const 0))
    ;; return register 0 (value_len == u64::MAX selects register mode)
    (call $value_return (i64.const -1) (i64.const 0))
  )
)"#,
    );
    near_test_contracts::wat_contract(&wat)
}

/// End-to-end: deploy a contract that calls `sha3_256` and assert it returns the
/// FIPS-202 digest of the input.
#[test]
fn test_sha3_256_matches_known_digest() {
    init_test_logger();
    if !sha3_256_enabled() {
        tracing::info!(
            "skipping: sha3_256 host fn not enabled at protocol version {PROTOCOL_VERSION}"
        );
        return;
    }

    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&user, Balance::from_near(100))
        .build();

    let wasm = sha3_256_wasm(b"abc");
    let deploy_tx = env.rpc_node().tx_deploy_contract(&user, wasm);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call_tx =
        env.rpc_node().tx_call(&user, &user, "main", vec![], Balance::ZERO, Gas::from_teragas(300));
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    match &outcome.status {
        FinalExecutionStatus::SuccessValue(bytes) => {
            assert_eq!(bytes.as_slice(), &SHA3_256_ABC[..], "unexpected sha3-256 digest");
        }
        other => panic!("expected SuccessValue, got {other:?}"),
    }
}

/// Before the protocol version that activates `sha3_256`, a contract that imports
/// `env.sha3_256` must fail at call time (the import can't be linked against a
/// runtime that doesn't expose it).
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_sha3_256_pre_activation_call_fails() {
    init_test_logger();
    if !sha3_256_enabled() {
        tracing::info!(
            "skipping: sha3_256 host fn not enabled at protocol version {PROTOCOL_VERSION}"
        );
        return;
    }

    // sha3_host_fns is turned on at protocol version 156 (see 156.yaml), so 155
    // is the last version without it.
    let pre_activation_pv = 155;
    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(pre_activation_pv)
        .add_user_account(&user, Balance::from_near(100))
        .build();

    let wasm = sha3_256_wasm(b"abc");
    let deploy_tx = env.rpc_node().tx_deploy_contract(&user, wasm);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call_tx =
        env.rpc_node().tx_call(&user, &user, "main", vec![], Balance::ZERO, Gas::from_teragas(300));
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    // The exact error variant (LinkError vs CompilationError) is VM-specific and
    // protocol-dependent, so just assert that the function call failed.
    assert_matches!(
        &outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::FunctionCallError(_),
            ..
        }))
    );
}
