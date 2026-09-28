//! Method resolution for ECC-only functions, by [`MethodCallKind`].

use crate::MethodCallKind;
use crate::ecc::ECC_ONLY_FUNCTIONS_SECTION;
use crate::logic::errors::{FunctionCallError, MethodResolveError};
use crate::logic::mocks::mock_external::MockedExternal;
use crate::runner::VMKindExt;
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::VMKind;
use near_primitives_core::code::ContractCode;
use near_primitives_core::types::Gas;
use std::borrow::Cow;
use std::sync::Arc;
use wasm_encoder::{CustomSection, Encode, Section};

/// Exports normal method `normal`, ECC-only method `ecc`, and ECC-only method
/// `ecc_bad_sig` with an invalid signature for a method.
fn ecc_contract() -> ContractCode {
    let mut wasm = wat::parse_str(
        r#"(module
            (func (export "normal"))
            (func (export "ecc"))
            (func (export "ecc_bad_sig") (param i32))
        )"#,
    )
    .unwrap();
    let section = CustomSection {
        name: Cow::Borrowed(ECC_ONLY_FUNCTIONS_SECTION),
        data: Cow::Borrowed(b"ecc,ecc_bad_sig"),
    };
    wasm.push(section.id());
    section.encode(&mut wasm);
    ContractCode::new(wasm, None)
}

struct CallResult {
    aborted: Option<FunctionCallError>,
    burnt_gas: Gas,
}

fn call(ecc_enabled: bool, method: &str, call_kind: MethodCallKind) -> CallResult {
    let mut config = super::test_vm_config(Some(VMKind::Wasmtime));
    config.ecc_only_functions = ecc_enabled;
    // With this fix, `OutcomeAbortButNopInOldProtocol` burns gas too, which
    // would hide the difference from `OutcomeAbort` checked below.
    config.fix_contract_loading_cost = false;
    let config = Arc::new(config);
    let mut ext = MockedExternal::with_code(ecc_contract());
    let context = super::create_context(vec![]);
    let gas_counter = context.make_gas_counter(&config);
    let outcome = VMKind::Wasmtime
        .runtime(config)
        .unwrap()
        .prepare(&ext, None, gas_counter, method, call_kind)
        .run(&mut ext, &context, Arc::new(RuntimeFeesConfig::test()))
        .unwrap();
    CallResult { aborted: outcome.aborted, burnt_gas: outcome.burnt_gas }
}

fn resolve_error(e: MethodResolveError) -> Option<FunctionCallError> {
    Some(FunctionCallError::MethodResolveError(e))
}

#[test]
fn test_call_kind_rules() {
    use MethodCallKind::{External, Internal, View};
    use MethodResolveError::{MethodIsECCOnly, MethodIsNotECC};

    let cases = [
        (View, "normal", None),
        (View, "ecc", None),
        (Internal, "normal", None),
        (Internal, "ecc", resolve_error(MethodIsECCOnly)),
        (External, "normal", resolve_error(MethodIsNotECC)),
        (External, "ecc", None),
    ];
    for (call_kind, method, expected) in cases {
        let result = call(true, method, call_kind);
        assert_eq!(result.aborted, expected, "{call_kind:?} call to {method}");
    }
}

/// `MethodNotFound` and `MethodInvalidSignature` take precedence over the
/// call kind checks.
#[test]
fn test_call_kind_error_precedence() {
    use MethodCallKind::{External, Internal, View};
    use MethodResolveError::{MethodInvalidSignature, MethodNotFound};

    for call_kind in [View, Internal, External] {
        let result = call(true, "missing", call_kind);
        assert_eq!(result.aborted, resolve_error(MethodNotFound), "{call_kind:?}");
        let result = call(true, "ecc_bad_sig", call_kind);
        assert_eq!(result.aborted, resolve_error(MethodInvalidSignature), "{call_kind:?}");
    }
}

/// The call kind errors are gas-bearing aborts: the contract-loading fee is
/// charged, unlike the zero-gas `MethodNotFound` in the old protocol.
#[test]
fn test_call_kind_errors_burn_gas() {
    let result = call(true, "ecc", MethodCallKind::Internal);
    assert_eq!(result.aborted, resolve_error(MethodResolveError::MethodIsECCOnly));
    assert!(result.burnt_gas > Gas::ZERO);

    let result = call(true, "normal", MethodCallKind::External);
    assert_eq!(result.aborted, resolve_error(MethodResolveError::MethodIsNotECC));
    assert!(result.burnt_gas > Gas::ZERO);

    let result = call(true, "missing", MethodCallKind::Internal);
    assert_eq!(result.aborted, resolve_error(MethodResolveError::MethodNotFound));
    assert_eq!(result.burnt_gas, Gas::ZERO);
}

/// With the config flag disabled the section is ignored and the `call_kind` check
/// error path is skipped.
#[test]
fn test_ecc_only_functions_disabled() {
    let result = call(false, "ecc", MethodCallKind::Internal);
    assert_eq!(result.aborted, None);
    let result = call(false, "ecc", MethodCallKind::External);
    assert_eq!(result.aborted, None);
}
