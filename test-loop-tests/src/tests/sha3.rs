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

/// FIPS-202 test vectors for the empty string and "abc". These are independent
/// oracles for the digests the host functions must produce (algorithm correctness
/// is also covered by the vm-runner unit tests; this exercises the full
/// deploy/call path).
const SHA3_256_EMPTY: [u8; 32] = [
    167, 255, 198, 248, 191, 30, 215, 102, 81, 193, 71, 86, 160, 97, 214, 98, 245, 128, 255, 77,
    228, 59, 73, 250, 130, 216, 10, 75, 128, 248, 67, 74,
];
const SHA3_256_ABC: [u8; 32] = [
    58, 152, 93, 167, 79, 226, 37, 178, 4, 92, 23, 45, 107, 211, 144, 189, 133, 95, 8, 110, 62,
    157, 82, 91, 70, 191, 226, 69, 17, 67, 21, 50,
];
const SHA3_384_EMPTY: [u8; 48] = [
    12, 99, 167, 91, 132, 94, 79, 125, 1, 16, 125, 133, 46, 76, 36, 133, 197, 26, 80, 170, 170,
    148, 252, 97, 153, 94, 113, 187, 238, 152, 58, 42, 195, 113, 56, 49, 38, 74, 219, 71, 251, 107,
    209, 224, 88, 213, 240, 4,
];
const SHA3_384_ABC: [u8; 48] = [
    236, 1, 73, 130, 136, 81, 111, 201, 38, 69, 159, 88, 226, 198, 173, 141, 249, 180, 115, 203,
    15, 192, 140, 37, 150, 218, 124, 240, 228, 155, 228, 178, 152, 216, 140, 234, 146, 122, 199,
    245, 57, 241, 237, 242, 40, 55, 109, 37,
];
const SHA3_512_EMPTY: [u8; 64] = [
    166, 159, 115, 204, 162, 58, 154, 197, 200, 181, 103, 220, 24, 90, 117, 110, 151, 201, 130, 22,
    79, 226, 88, 89, 224, 209, 220, 193, 71, 92, 128, 166, 21, 178, 18, 58, 241, 245, 249, 76, 17,
    227, 233, 64, 44, 58, 197, 88, 245, 0, 25, 157, 149, 182, 211, 227, 1, 117, 133, 134, 40, 29,
    205, 38,
];
const SHA3_512_ABC: [u8; 64] = [
    183, 81, 133, 11, 26, 87, 22, 138, 86, 147, 205, 146, 75, 107, 9, 110, 8, 246, 33, 130, 116,
    68, 247, 13, 136, 79, 93, 2, 64, 210, 113, 46, 16, 225, 22, 233, 25, 42, 243, 201, 26, 126,
    197, 118, 71, 227, 147, 64, 87, 52, 11, 76, 244, 8, 213, 165, 101, 146, 248, 39, 78, 236, 83,
    240,
];

/// `sha3_host_fns` has no `ProtocolFeature` variant; it is turned on purely by
/// a runtime-config diff (156.yaml). Check the flag for the current protocol
/// version so the test skips on stable, runs on nightly, and re-enables itself
/// automatically once the feature stabilizes.
fn sha3_enabled() -> bool {
    RuntimeConfigStore::new(None).get_config(PROTOCOL_VERSION).wasm_config.sha3_host_fns
}

/// Build a WASM contract that imports `env.{func}`, bakes `input` into linear
/// memory at offset 0, hashes it into register 0, and returns the register
/// contents (the digest) via `value_return`.
fn sha3_wasm(func: &str, input: &[u8]) -> Vec<u8> {
    let mut data = String::new();
    for byte in input {
        data.push_str(&format!("\\{:02x}", byte));
    }
    let input_len = input.len();
    let wat = format!(
        r#"(module
  (import "env" "{func}" (func ${func} (param i64 i64 i64)))
  (import "env" "value_return" (func $value_return (param i64 i64)))
  (memory (export "memory") 1)

  (data (i32.const 0) "{data}")

  (func (export "main")
    ;; hash [0, input_len) into register 0
    (call ${func} (i64.const {input_len}) (i64.const 0) (i64.const 0))
    ;; return register 0 (value_len == u64::MAX selects register mode)
    (call $value_return (i64.const -1) (i64.const 0))
  )
)"#,
    );
    near_test_contracts::wat_contract(&wat)
}

/// End-to-end: deploy a contract that calls `sha3_256`/`sha3_384`/`sha3_512` and
/// assert it returns the FIPS-202 digest of the input, for both the empty string
/// and "abc".
#[test]
fn test_sha3_matches_known_digest() {
    init_test_logger();
    if !sha3_enabled() {
        tracing::info!(
            "skipping: sha3 host fns not enabled at protocol version {PROTOCOL_VERSION}"
        );
        return;
    }

    let cases: [(&str, &[u8], &[u8]); 6] = [
        ("sha3_256", b"", &SHA3_256_EMPTY),
        ("sha3_256", b"abc", &SHA3_256_ABC),
        ("sha3_384", b"", &SHA3_384_EMPTY),
        ("sha3_384", b"abc", &SHA3_384_ABC),
        ("sha3_512", b"", &SHA3_512_EMPTY),
        ("sha3_512", b"abc", &SHA3_512_ABC),
    ];

    // Give each (function, input) case its own account so the baked-in contracts
    // don't clobber each other.
    let users: Vec<_> = (0..cases.len()).map(|i| create_account_id(&format!("user{i}"))).collect();
    let mut builder = TestLoopBuilder::new().enable_rpc();
    for user in &users {
        builder = builder.add_user_account(user, Balance::from_near(100));
    }
    let mut env = builder.build();

    for (user, (func, input, expected)) in users.iter().zip(cases) {
        let wasm = sha3_wasm(func, input);
        let deploy_tx = env.rpc_node().tx_deploy_contract(user, wasm);
        env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

        let call_tx = env.rpc_node().tx_call(
            user,
            user,
            "main",
            vec![],
            Balance::ZERO,
            Gas::from_teragas(300),
        );
        let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
        match &outcome.status {
            FinalExecutionStatus::SuccessValue(bytes) => {
                assert_eq!(bytes.as_slice(), expected, "unexpected {func} digest for {input:?}");
            }
            other => panic!("expected SuccessValue for {func}({input:?}), got {other:?}"),
        }
    }
}

/// Before the protocol version that activates `sha3_host_fns`, a contract that
/// imports a `env.sha3_*` host function must fail at call time (the import can't
/// be linked against a runtime that doesn't expose it).
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_sha3_pre_activation_call_fails() {
    init_test_logger();
    if !sha3_enabled() {
        tracing::info!(
            "skipping: sha3 host fns not enabled at protocol version {PROTOCOL_VERSION}"
        );
        return;
    }

    // sha3_host_fns is turned on at protocol version 156 (see 156.yaml), so 155
    // is the last version without it.
    let pre_activation_pv = 155;
    let fns = ["sha3_256", "sha3_384", "sha3_512"];

    // Give each function its own account so the baked-in contracts don't clobber
    // each other.
    let users: Vec<_> = fns.iter().map(|func| create_account_id(func)).collect();
    let mut builder = TestLoopBuilder::new().enable_rpc().protocol_version(pre_activation_pv);
    for user in &users {
        builder = builder.add_user_account(user, Balance::from_near(100));
    }
    let mut env = builder.build();

    for (user, func) in users.iter().zip(fns) {
        let wasm = sha3_wasm(func, b"abc");
        let deploy_tx = env.rpc_node().tx_deploy_contract(user, wasm);
        env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

        let call_tx = env.rpc_node().tx_call(
            user,
            user,
            "main",
            vec![],
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
            })),
            "{func} import should fail pre-activation"
        );
    }
}
