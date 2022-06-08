use super::test_builder::test_builder;
use expect_test::expect;
use near_primitives::version::ProtocolFeature;

static INFINITE_INITIALIZER_CONTRACT: &str = r#"
(module
  (type (;0;) (func))
  (func (;0;) (type 0) (loop (br 0)))
  (func (;1;) (type 0))
  (start 0)
  (export "hello" (func 1))
)"#;

#[test]
fn test_infinite_initializer() {
    test_builder()
        .wat(INFINITE_INITIALIZER_CONTRACT)
        .method("hello")
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100000000000000 used gas 100000000000000
            Err: Exceeded the prepaid gas.
        "#]]);
}

#[test]
fn test_infinite_initializer_export_not_found() {
    test_builder()
        .wat(INFINITE_INITIALIZER_CONTRACT)
        .method("hello2")
        .protocol_features(&[
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            ProtocolFeature::FixContractLoadingCost,
        ]).expects(&[
            expect![[r#"
                VMOutcome: balance 0 storage_usage 0 return data None burnt gas 0 used gas 0
                Err: MethodNotFound
            "#]],
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 45633213 used gas 45633213
                Err: MethodNotFound
            "#]],
        ]);
}

static SIMPLE_CONTRACT: &str = r#"
(module
  (type (;0;) (func))
  (func (;0;) (type 0))
  (export "hello" (func 0))
)"#;

#[test]
fn test_simple_contract() {
    test_builder().wat(SIMPLE_CONTRACT).method("hello").expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 43032213 used gas 43032213
    "#]]);
}

#[test]
fn test_multiple_memories() {
    test_builder()
        .wasm(&[
            0, 97, 115, 109, 1, 0, 0, 0, 2, 12, 1, 3, 101, 110, 118, 0, 2, 1, 239, 1, 248, 1, 4, 6,
            1, 112, 0, 143, 129, 32, 7, 12, 1, 8, 0, 17, 17, 17, 17, 17, 17, 2, 2, 0,
        ])
        .method("hello")
        // Wasmtime classifies this error as link error at the moment.
        .opaque_error()
        .protocol_features(&[
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: ...
            "#]],
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 44982963 used gas 44982963
                Err: ...
            "#]],
        ]);
}

#[test]
fn test_export_not_found() {
    test_builder().wat(SIMPLE_CONTRACT)
        .method("hello2")
        .protocol_features(&[
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            ProtocolFeature::FixContractLoadingCost,
        ]).expects(&[
            expect![[r#"
                VMOutcome: balance 0 storage_usage 0 return data None burnt gas 0 used gas 0
                Err: MethodNotFound
            "#]],
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 43032213 used gas 43032213
                Err: MethodNotFound
            "#]],
        ]);
}

#[test]
fn test_empty_method() {
    test_builder().wat(SIMPLE_CONTRACT).method("").expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
        Err: MethodEmptyName
    "#]]);
}

#[test]
fn test_trap_contract() {
    test_builder()
        .wat(
            r#"
(module
  (type (;0;) (func))
  (func (;0;) (type 0) (unreachable))
  (export "hello" (func 0))
)"#,
        )
        .method("hello")
        .skip_wasmtime()
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 44071719 used gas 44071719
            Err: WebAssembly trap: An `unreachable` opcode was executed.
        "#]]);
}

#[test]
fn test_trap_initializer() {
    test_builder()
        .wat(
            r#"
(module
  (type (;0;) (func))
  (func (;0;) (type 0) (unreachable))
  (start 0)
  (export "hello" (func 0))
)"#,
        )
        .method("hello")
        .skip_wasmtime()
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 44721969 used gas 44721969
            Err: WebAssembly trap: An `unreachable` opcode was executed.
        "#]]);
}

#[test]
fn test_div_by_zero_contract() {
    test_builder()
        .wat(
            r#"
(module
  (type (;0;) (func))
  (func (;0;) (type 0)
    i32.const 1
    i32.const 0
    i32.div_s
    return
  )
  (export "hello" (func 0))
)"#,
        )
        .method("hello")
        .skip_wasmtime()
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 47623737 used gas 47623737
            Err: WebAssembly trap: An arithmetic exception, e.g. divided by zero.
        "#]]);
}

#[test]
fn test_float_to_int_contract() {
    for op in ["i32.trunc_f64_s", "i32.trunc_f64_u", "i64.trunc_f64_s", "i64.trunc_f64_u"] {
        test_builder()
            .wat(&format!(
                r#"
(module
  (type (;0;) (func))
  (func (;0;) (type 0)
    f64.const 0x1p+1023
    {op}
    return
  )
  (export "hello" (func 0))
)"#,
            ))
            .method("hello")
            .skip_wasmtime()
            .expect(expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 47884731 used gas 47884731
                Err: WebAssembly trap: An arithmetic exception, e.g. divided by zero.
            "#]]);
    }
}

#[test]
fn test_indirect_call_to_null_contract() {
    test_builder()
        .wat(
            r#"
(module
  (type (;0;) (func))
  (table (;0;) 2 funcref)
  (func (;0;) (type 0)
    i32.const 1
    call_indirect (type 0)
    return
  )
  (export "hello" (func 0))
)"#,
        )
        .method("hello")
        .opaque_error()
        .skip_wasmtime()
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 48101481 used gas 48101481
            Err: ...
        "#]])
}

#[test]
fn test_indirect_call_to_wrong_signature_contract() {
    test_builder()
        .wat(
            r#"
(module
  (type (;0;) (func))
  (type (;1;) (func (result i32)))
  (func (;0;) (type 0)
    i32.const 1
    call_indirect (type 1)
    return
  )
  (func (;1;) (type 1)
    i32.const 0
    return
  )
  (table (;0;) 3 3 funcref)
  (elem (;0;) (i32.const 1) 0 1)
  (export "hello" (func 0))
)"#,
        )
        .method("hello")
        .skip_wasmtime()
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 52869981 used gas 52869981
            Err: WebAssembly trap: Call indirect incorrect signature trap.
        "#]])
}

#[test]
fn test_wrong_signature_contract() {
    test_builder()
        .wat(
            r#"
(module
  (type (;0;) (func (param i32)))
  (func (;0;) (type 0))
  (export "hello" (func 0))
)"#,
        )
        .method("hello")
        .protocol_features(&[
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 0 storage_usage 0 return data None burnt gas 0 used gas 0
                Err: MethodInvalidSignature
            "#]],
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 43248963 used gas 43248963
                Err: MethodInvalidSignature
            "#]],
        ]);
}

#[test]
fn test_export_wrong_type() {
    test_builder()
        .wat(
            r#"
(module
  (global (;0;) i32 (i32.const 123))
  (export "hello" (global 0))
)"#,
        )
        .method("hello")
        .protocol_features(&[
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 0 storage_usage 0 return data None burnt gas 0 used gas 0
                Err: MethodNotFound
            "#]],
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 41514963 used gas 41514963
                Err: MethodNotFound
            "#]],
        ]);
}

#[test]
fn test_guest_panic() {
    test_builder()
        .wat(
            r#"
(module
  (type (;0;) (func))
  (import "env" "panic" (func (;0;) (type 0)))
  (func (;1;) (type 0) (call 0))
  (export "hello" (func 1))
)"#,
        )
        .method("hello")
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 312307830 used gas 312307830
            Err: Smart contract panicked: explicit guest panic
        "#]]);
}

#[test]
fn test_stack_overflow() {
    test_builder()
        .wat(
            r#"
(module
  (type (;0;) (func))
  (func (;0;) (type 0) (call 0))
  (export "hello" (func 0))
)"#,
        )
        .method("hello")
        .skip_wasmtime()
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 13523500017 used gas 13523500017
            Err: WebAssembly trap: An `unreachable` opcode was executed.
        "#]]);
}

#[test]
fn test_stack_instrumentation_protocol_upgrade() {
    test_builder()
        .wat(
            r#"
(module
  (func $f1 (export "f1")
    (local i32)
    (call $f1))
  (func $f2 (export "f2")
    (local i32 i32 i32 i32)
    (call $f2))
)"#,
        )
        .method("f1")
        .protocol_features(&[ProtocolFeature::CorrectStackLimit])
        .skip_wasmtime()
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 6789985365 used gas 6789985365
                Err: WebAssembly trap: An `unreachable` opcode was executed.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 6789985365 used gas 6789985365
                Err: WebAssembly trap: An `unreachable` opcode was executed.
            "#]],
        ]);

    test_builder()
        .wat(
            r#"
(module
  (func $f1 (export "f1")
    (local i32)
    (call $f1))
  (func $f2 (export "f2")
    (local i32 i32 i32 i32)
    (call $f2))
)"#,
        )
        .method("f2")
        .protocol_features(&[ProtocolFeature::CorrectStackLimit])
        .skip_wasmtime()
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 6789985365 used gas 6789985365
                Err: WebAssembly trap: An `unreachable` opcode was executed.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 2745316869 used gas 2745316869
                Err: WebAssembly trap: An `unreachable` opcode was executed.
            "#]],
        ]);
}

#[test]
fn test_memory_grow() {
    test_builder()
        .wat(
            r#"
(module
  (type (;0;) (func))
  (func (;0;) (type 0)
    (loop
      (memory.grow (i32.const 1))
      drop
      br 0
    )
  )
  (memory (;0;) 17 32)
  (export "hello" (func 0))
)"#,
        )
        .method("hello")
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100000000000000 used gas 100000000000000
            Err: Exceeded the prepaid gas.
        "#]]);
}

fn bad_import_global(env: &str) -> Vec<u8> {
    wat::parse_str(format!(
        r#"
            (module
              (type (;0;) (func))
              (import "{}" "input" (global (;0;) i32))
              (func (;0;) (type 0))
              (export "hello" (func 0))
            )"#,
        env
    ))
    .unwrap()
}

fn bad_import_func(env: &str) -> Vec<u8> {
    wat::parse_str(format!(
        r#"
            (module
              (type (;0;) (func))
              (import "{}" "wtf" (func (;0;) (type 0)))
              (func (;0;) (type 0))
              (export "hello" (func 0))
            )"#,
        env
    ))
    .unwrap()
}

#[test]
// Weird behavior:
// Invalid import not from "env" -> PrepareError::Instantiate
// Invalid import from "env" -> LinkError
fn test_bad_import_1() {
    test_builder()
        .wasm(&bad_import_global("wtf"))
        .method("hello")
        .protocol_features(&[
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened during instantiation.
            "#]],
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 46500213 used gas 46500213
                Err: PrepareError: Error happened during instantiation.
            "#]],
        ]);
}

#[test]
fn test_bad_import_2() {
    test_builder()
        .wasm(&bad_import_func("wtf"))
        .method("hello")
        .protocol_features(&[
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened during instantiation.
            "#]],
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 45849963 used gas 45849963
                Err: PrepareError: Error happened during instantiation.
            "#]],
        ]);
}

#[test]
fn test_bad_import_3() {
    test_builder().wasm(&bad_import_global("env")).method("hello").opaque_error().expect(expect![
        [r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 46500213 used gas 46500213
        Err: ...
    "#]
    ]);
}

#[test]
fn test_bad_import_4() {
    test_builder().wasm(&bad_import_func("env")).method("hello").opaque_error().expect(expect![[
        r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 45849963 used gas 45849963
        Err: ...
    "#
    ]]);
}

#[test]
fn test_initializer_no_gas() {
    test_builder()
        .wat(
            r#"
(module
  (type (;0;) (func))
  (func (;0;) (type 0) nop)
  (start 0)
  (export "hello" (func 0))
)"#,
        )
        .method("hello")
        .gas(0)
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
            Err: Exceeded the prepaid gas.
        "#]]);
}

#[test]
fn test_bad_many_imports() {
    let mut imports = String::new();
    for i in 0..100 {
        imports.push_str(&format!(
            r#"
            (import "env" "wtf{}" (func (;{};) (type 0)))
         "#,
            i, i
        ));
    }

    test_builder()
        .wat(&format!(
            r#"
                (module
                  (type (;0;) (func))
                  {}
                  (export "hello" (func 0))
                )"#,
            imports
        ))
        .method("hello")
        .opaque_error()
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 299664213 used gas 299664213
            Err: ...
        "#]])
}

static EXTERNAL_CALL_CONTRACT: &str = r#"
(module
  (import "env" "prepaid_gas" (func (;0;) (result i64)))
  (export "hello" (func 1))
  (func (;1;)
      (drop (call 0))
      )
)"#;

#[test]
fn test_external_call_ok() {
    test_builder().wat(EXTERNAL_CALL_CONTRACT).method("hello").expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 315514836 used gas 315514836
    "#]]);
}

#[test]
fn test_external_call_error() {
    test_builder().wat(EXTERNAL_CALL_CONTRACT).method("hello").gas(100).expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100 used gas 100
        Err: Exceeded the prepaid gas.
    "#]]);
}

static EXTERNAL_INDIRECT_CALL_CONTRACT: &str = r#"
(module
  (import "env" "prepaid_gas" (func $lol (result i64)))
  (type $lol_t (func (result i64)))

  (table 1 funcref)
  (elem (i32.const 0) $lol)

  (func (export "main")
    (call_indirect (type $lol_t) (i32.const 0))
    drop
  )
)"#;

#[test]
fn test_external_call_indirect() {
    test_builder().wat(EXTERNAL_INDIRECT_CALL_CONTRACT).expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 325441092 used gas 325441092
    "#]]);
}

const ADDRESS_OVERFLOW: &str = r#"
(module
  (memory 1)
  (func (export "main")
    i32.const 1
    i64.load32_u offset=4294967295 (;2^32 - 1;) align=1
    drop
  )
)"#;

/// Load from address so far out of bounds that it causes integer overflow.
#[test]
fn test_address_overflow() {
    test_builder().wat(ADDRESS_OVERFLOW).skip_wasmtime().skip_wasmer0().expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 48534981 used gas 48534981
        Err: WebAssembly trap: Memory out of bounds trap.
    "#]]);

    // wasmer0 incorrectly doesn't catch overflow during address calculation
    test_builder().wat(ADDRESS_OVERFLOW).skip_wasmtime().skip_wasmer2().expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 48534981 used gas 48534981
    "#]]);
}

const NAN_SIGN: &str = r#"
(module
  (func (export "main")
    (i32.div_u
      (i32.const 0)
      (f32.gt
        (f32.copysign
          (f32.const 1.0)
          (f32.sqrt (f32.const -1.0)))
        (f32.const 0)))
    drop
  )
)"#;

/// Uses `f32.copysign` to observe a sign of `NaN`.
///
/// WASM specification allows different behaviors here:
///
///   https://github.com/WebAssembly/design/blob/main/Nondeterminism.md
///
/// We solve this problem by canonicalizing NaNs.
#[test]
fn test_nan_sign() {
    test_builder().wat(NAN_SIGN).skip_wasmtime().skip_wasmer0().expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 54988767 used gas 54988767
    "#]]);

    // wasmer0 doesn't canonicalize NaNs
    test_builder().wat(NAN_SIGN).skip_wasmtime().skip_wasmer2().expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 54988767 used gas 54988767
        Err: WebAssembly trap: An arithmetic exception, e.g. divided by zero.
    "#]]);
}

// Check that a `GasExceeded` error is returned when there is not enough gas to
// even load a contract.
#[test]
fn test_gas_exceed_loading() {
    test_builder().wat(SIMPLE_CONTRACT).method("non_empty_non_existing").gas(1).expect(expect![[
        r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 1 used gas 1
            Err: Exceeded the prepaid gas.
        "#
    ]]);
}

// Call the "gas" host function with unreasonably large values, trying to force
// overflow
#[test]
fn gas_overflow_direct_call() {
    with_vm_variants(|vm_kind: VMKind| {
        let direct_call = wat::parse_str(
            r#"
(module
  (import "env" "gas" (func $gas (param i32)))
  (func (export "main")
    (call $gas (i32.const 0xffff_ffff)))
)"#,
        )
        .unwrap();

        let actual = make_simple_contract_call_vm(&direct_call, "main", vm_kind);
        gas_and_error_match(
            actual,
            100000000000000,
            Some(VMError::FunctionCallError(FunctionCallError::HostError(
                HostError::GasLimitExceeded,
            ))),
        );
    });
}

// Call the "gas" host function indirectly with unreasonably large values,
// trying to force overflow.
#[test]
fn gas_overflow_indirect_call() {
    with_vm_variants(|vm_kind: VMKind| {
        let indirect_call = wat::parse_str(
            r#"
(module
  (import "env" "gas" (func $gas (param i32)))
  (type $gas_ty (func (param i32)))

  (table 1 anyfunc)
  (elem (i32.const 0) $gas)

  (func (export "main")
    (call_indirect
      (type $gas_ty)
      (i32.const 0xffff_ffff)
      (i32.const 0)))
)"#,
        )
        .unwrap();

        let actual = make_simple_contract_call_vm(&indirect_call, "main", vm_kind);
        gas_and_error_match(
            actual,
            100000000000000,
            Some(VMError::FunctionCallError(FunctionCallError::HostError(
                HostError::GasLimitExceeded,
            ))),
        );
    });
}

#[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
mod fix_contract_loading_cost_protocol_upgrade {
    use super::*;

    static ALMOST_TRIVIAL_CONTRACT: &str = r#"
(module
    (func (export "main")
      i32.const 1
      i32.const 1
      i32.div_s
      return
    )
)"#;

    // Normal execution should be unchanged before and after.
    #[test]
    fn test_fn_loading_gas_protocol_upgrade() {
        test_builder()
            .wat(ALMOST_TRIVIAL_CONTRACT)
            .protocol_features(&[ProtocolFeature::FixContractLoadingCost])
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 47406987 used gas 47406987
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 47406987 used gas 47406987
                "#]],
            ]);
    }

    // Executing with just enough gas to load the contract will fail before and
    // after. Both charge the same amount of gas.
    #[test]
    fn test_fn_loading_gas_protocol_upgrade_exceed_loading() {
        test_builder()
            .wat(ALMOST_TRIVIAL_CONTRACT)
            .gas(44115963)
            .protocol_features(&[ProtocolFeature::FixContractLoadingCost])
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 44115963 used gas 44115963
                    Err: Exceeded the prepaid gas.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 44115963 used gas 44115963
                    Err: Exceeded the prepaid gas.
                "#]],
            ]);
    }

    /// Executing with enough gas to finish loading but not to execute the full
    /// contract should have the same outcome before and after.
    #[test]
    fn test_fn_loading_gas_protocol_upgrade_exceed_executing() {
        test_builder()
            .wat(ALMOST_TRIVIAL_CONTRACT)
            .gas(47406986)
            .protocol_features(&[ProtocolFeature::FixContractLoadingCost])
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 47406986 used gas 47406986
                    Err: Exceeded the prepaid gas.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 47406986 used gas 47406986
                    Err: Exceeded the prepaid gas.
                "#]],
            ]);
    }

    fn function_not_defined_contract() -> Vec<u8> {
        wat::parse_str(
            r#"
            (module
              (export "hello" (func 0))
            )"#,
        )
        .unwrap()
    }

    /// Failure during preparation must remain free of gas charges for old versions
    /// but new versions must charge the loading gas.
    #[test]
    fn test_fn_loading_gas_protocol_upgrade_fail_preparing() {
        // This list covers all control flows that are expected to change
        // with the protocol feature.
        // Having a test for every possible preparation error would be even
        // better, to ensure triggering any of them will always remain
        // compatible with versions before this upgrade. Unfortunately, we
        // currently do not have tests ready to trigger each error.

        test_builder()
            .wasm(&function_not_defined_contract())
            .protocol_features(&[ProtocolFeature::FixContractLoadingCost])
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                    Err: PrepareError: Error happened while deserializing the module.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 39564213 used gas 39564213
                    Err: PrepareError: Error happened while deserializing the module.
                "#]],
            ]);

        test_builder()
            .wasm(&bad_import_global("wtf"))
            .protocol_features(&[ProtocolFeature::FixContractLoadingCost])
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                    Err: PrepareError: Error happened during instantiation.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 46500213 used gas 46500213
                    Err: PrepareError: Error happened during instantiation.
                "#]],
            ]);

        test_builder()
            .wasm(&bad_import_func("wtf"))
            .protocol_features(&[ProtocolFeature::FixContractLoadingCost])
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                    Err: PrepareError: Error happened during instantiation.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 45849963 used gas 45849963
                    Err: PrepareError: Error happened during instantiation.
                "#]],
            ]);

        test_builder()
            .wasm(&near_test_contracts::LargeContract {
                functions: 101,
                locals_per_function: 9901,
                ..Default::default()
            }
            .make())
            .protocol_features(&[ProtocolFeature::FixContractLoadingCost])
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                    Err: PrepareError: Too many locals declared in the contract.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 195407463 used gas 195407463
                    Err: PrepareError: Too many locals declared in the contract.
                "#]],
            ]);

        let functions_number_limit: u32 = 10_000;
        test_builder()
            .wasm(&near_test_contracts::LargeContract {
                functions: functions_number_limit / 2,
                panic_imports: functions_number_limit / 2 + 1,
                ..Default::default()
            }
            .make())
            .protocol_features(&[ProtocolFeature::FixContractLoadingCost])
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                    Err: PrepareError: Too many functions in contract.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 19554433713 used gas 19554433713
                    Err: PrepareError: Too many functions in contract.
                "#]],
            ]);
    }
}
