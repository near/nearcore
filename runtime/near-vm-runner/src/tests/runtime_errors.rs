use super::test_builder::test_builder;
use expect_test::expect;
use near_primitives_core::version::ProtocolFeature;
use std::fmt::Write;

static INFINITE_INITIALIZER_CONTRACT: &str = r#"
(module
  (func $start (loop (br 0)))
  (func (export "main"))
  (start $start)
)"#;

#[test]
fn test_infinite_initializer() {
    test_builder()
        .wat(INFINITE_INITIALIZER_CONTRACT)
        .gas(10u64.pow(10))
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 10000000000 used gas 10000000000
            Err: Exceeded the prepaid gas.
        "#]]);
}

#[test]
fn test_infinite_initializer_export_not_found() {
    test_builder()
        .wat(INFINITE_INITIALIZER_CONTRACT)
        .method("no-such-method")
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
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 49101213 used gas 49101213
                Err: MethodNotFound
            "#]],
        ]);
}

static SIMPLE_CONTRACT: &str = r#"(module (func (export "main")))"#;

#[test]
fn test_simple_contract() {
    test_builder()
        .wat(SIMPLE_CONTRACT)
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 42815463 used gas 42815463
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 49397511 used gas 49397511
            "#]],
        ]);
}

#[test]
fn test_imported_memory() {
    test_builder()
        .wasm(&[
            0, 97, 115, 109, 1, 0, 0, 0, 2, 12, 1, 3, 101, 110, 118, 0, 2, 1, 239, 1, 248, 1, 4, 6,
            1, 112, 0, 143, 129, 32, 7, 12, 1, 8, 0, 17, 17, 17, 17, 17, 17, 2, 2, 0,
        ])
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
fn test_multiple_memories() {
    test_builder()
        .wat("(module (memory 1 2) (memory 3 4))")
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
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 39130713 used gas 39130713
                Err: ...
            "#]],
        ]);
}

#[test]
fn test_export_not_found() {
    test_builder().wat(SIMPLE_CONTRACT)
        .method("no-such-method")
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
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 42815463 used gas 42815463
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
        .wat(r#"(module (func (export "main") (unreachable)) )"#)
        .skip_wasmtime()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 43854969 used gas 43854969
                Err: WebAssembly trap: An `unreachable` opcode was executed.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 50437017 used gas 50437017
                Err: WebAssembly trap: An `unreachable` opcode was executed.
            "#]],
        ]);
}

#[test]
fn test_trap_initializer() {
    test_builder()
        .wat(
            r#"
                (module
                  (func $f (export "main") (unreachable))
                  (start $f)
                )
            "#,
        )
        .skip_wasmtime()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 47322969 used gas 47322969
                Err: WebAssembly trap: An `unreachable` opcode was executed.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 53905017 used gas 53905017
                Err: WebAssembly trap: An `unreachable` opcode was executed.
            "#]],
        ]);
}

#[test]
fn test_div_by_zero_contract() {
    test_builder()
        .wat(
            r#"
                (module
                  (func (export "main")
                    i32.const 1
                    i32.const 0
                    i32.div_s
                    return
                  )
                )
            "#,
        )
        .skip_wasmtime()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 47406987 used gas 47406987
                Err: WebAssembly trap: An arithmetic exception, e.g. divided by zero.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 53166279 used gas 53166279
                Err: WebAssembly trap: An arithmetic exception, e.g. divided by zero.
            "#]],
        ]);
}

#[test]
fn test_float_to_int_contract() {
    for op in ["i32.trunc_f64_s", "i32.trunc_f64_u", "i64.trunc_f64_s", "i64.trunc_f64_u"] {
        test_builder()
            .wat(&format!(
                r#"
                    (module
                      (func (export "main")
                        f64.const 0x1p+1023
                        {op}
                        return
                      )
                    )
                "#,
            ))
            .skip_wasmtime()
            .protocol_features(&[
                ProtocolFeature::PreparationV2,
            ])
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 47667981 used gas 47667981
                    Err: WebAssembly trap: An arithmetic exception, e.g. divided by zero.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 53427273 used gas 53427273
                    Err: WebAssembly trap: An arithmetic exception, e.g. divided by zero.
                "#]],
            ]);
    }
}

#[test]
fn test_indirect_call_to_null_contract() {
    test_builder()
        .wat(
            r#"
                (module
                  (type $ty (func))
                  (table 1 funcref)
                  (func (export "main")
                    i32.const 0
                    call_indirect (type $ty)
                    return
                  )
                )
            "#,
        )
        .opaque_error()
        .skip_wasmtime()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 50919231 used gas 50919231
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 56678523 used gas 56678523
                Err: ...
            "#]],
        ])
}

#[test]
fn test_indirect_call_to_wrong_signature_contract() {
    test_builder()
        .wat(
            r#"
                (module
                  (type $ty (func (result i32)))
                  (func $f)

                  (table 1 funcref)
                  (elem (i32.const 0) $f)

                  (func (export "main")
                    i32.const 0
                    call_indirect (type $ty)
                    return
                  )
                )
            "#,
        )
        .skip_wasmtime()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 55904481 used gas 55904481
                Err: WebAssembly trap: Call indirect incorrect signature trap.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 61663773 used gas 61663773
                Err: WebAssembly trap: Call indirect incorrect signature trap.
            "#]]
        ])
}

#[test]
fn test_wrong_signature_contract() {
    test_builder()
        .wat(r#"(module (func (export "main") (param i32)))"#)
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
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 43032213 used gas 43032213
                Err: MethodInvalidSignature
            "#]],
        ]);
}

#[test]
fn test_export_wrong_type() {
    test_builder()
        .wat(r#"(module (global (export "main") i32 (i32.const 123)))"#)
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
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 41298213 used gas 41298213
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
                  (import "env" "panic" (func $panic))
                  (func (export "main") (call $panic))
                )
            "#,
        )
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 315775830 used gas 315775830
                Err: Smart contract panicked: explicit guest panic
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 322357878 used gas 322357878
                Err: Smart contract panicked: explicit guest panic
            "#]],
        ]);
}

#[test]
fn test_panic_re_export() {
    test_builder()
        .wat(
            r#"
(module
  (import "env" "panic" (func $panic))
  (export "main" (func $panic))
)"#,
        )
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 312352074 used gas 312352074
            Err: Smart contract panicked: explicit guest panic
        "#]]);
}

#[test]
fn test_stack_overflow() {
    test_builder()
        .wat(r#"(module (func $f (export "main") (call $f)))"#)
        .skip_wasmtime()
        .opaque_error()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 13526101017 used gas 13526101017
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 30376143897 used gas 30376143897
                Err: ...
            "#]],
        ]);
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
                )
            "#,
        )
        .method("f1")
        .protocol_features(&[
            ProtocolFeature::CorrectStackLimit,
            ProtocolFeature::PreparationV2,
        ])
        .skip_wasmtime()
        .opaque_error()
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 6789985365 used gas 6789985365
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 6789985365 used gas 6789985365
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 31767212013 used gas 31767212013
                Err: ...
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
                )
            "#,
        )
        .method("f2")
        .opaque_error()
        .protocol_features(&[
            ProtocolFeature::CorrectStackLimit,
            ProtocolFeature::PreparationV2,
        ])
        .skip_wasmtime()
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 6789985365 used gas 6789985365
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 2745316869 used gas 2745316869
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 29698803429 used gas 29698803429
                Err: ...
            "#]],
        ]);
}

#[test]
fn test_memory_grow() {
    test_builder()
        .wat(
            r#"
(module
  (memory 17 32)
  (func (export "main")
    (loop
      (memory.grow (i32.const 1))
      drop
      br 0
    )
  )
)"#,
        )
        .gas(10u64.pow(10))
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 10000000000 used gas 10000000000
            Err: Exceeded the prepaid gas.
        "#]]);
}

fn bad_import_global(env: &str) -> Vec<u8> {
    wat::parse_str(format!(
        r#"
            (module
              (import "{env}" "no-such-global" (global i32))
              (func (export "main"))
            )"#,
    ))
    .unwrap()
}

fn bad_import_func(env: &str) -> Vec<u8> {
    wat::parse_str(format!(
        r#"
            (module
              (import "{env}" "no-such-fn" (func $f))
              (export "main" (func $f))
            )"#,
    ))
    .unwrap()
}

#[test]
// Weird behavior:
// Invalid import not from "env" -> PrepareError::Instantiate
// Invalid import from "env" -> LinkError
fn test_bad_import_1() {
    test_builder()
        .wasm(&bad_import_global("no-such-module"))
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
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 50618463 used gas 50618463
                Err: PrepareError: Error happened during instantiation.
            "#]],
        ]);
}

#[test]
fn test_bad_import_2() {
    test_builder()
        .wasm(&bad_import_func("no-such-module"))
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
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 50184963 used gas 50184963
                Err: PrepareError: Error happened during instantiation.
            "#]],
        ]);
}

#[test]
fn test_bad_import_3() {
    test_builder()
        .wasm(&bad_import_global("env"))
        .opaque_error()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 48234213 used gas 48234213
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: ...
            "#]],
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 48234213 used gas 48234213
                Err: ...
            "#]],
        ]);
}

#[test]
fn test_bad_import_4() {
    test_builder().wasm(&bad_import_func("env")).opaque_error().expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 47800713 used gas 47800713
        Err: ...
    "#]]);
}

#[test]
fn test_initializer_no_gas() {
    test_builder()
        .wat(
            r#"
(module
  (func $f (export "main") nop)
  (start $f)
)"#,
        )
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
        writeln!(imports, r#"(import "env" "wtf{i}" (func))"#).unwrap();
    }

    test_builder()
        .wat(&format!(
            r#"
                (module
                  {imports}
                  (export "main" (func 0))
                )"#,
        ))
        .opaque_error()
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 299447463 used gas 299447463
            Err: ...
        "#]])
}

static EXTERNAL_CALL_CONTRACT: &str = r#"
(module
  (import "env" "prepaid_gas" (func $prepaid_gas (result i64)))
  (func (export "main")
      (drop (call $prepaid_gas)))
)"#;

#[test]
fn test_external_call_ok() {
    test_builder()
        .wat(EXTERNAL_CALL_CONTRACT)
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 320283336 used gas 320283336
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 326865384 used gas 326865384
            "#]],
        ]);
}

#[test]
fn test_external_call_error() {
    test_builder().wat(EXTERNAL_CALL_CONTRACT).gas(100).expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100 used gas 100
        Err: Exceeded the prepaid gas.
    "#]]);
}

#[test]
fn test_external_call_indirect() {
    test_builder()
        .wat(
            r#"
            (module
              (import "env" "prepaid_gas" (func $prepaid_gas (result i64)))
              (type $prepaid_gas_t (func (result i64)))

              (table 1 funcref)
              (elem (i32.const 0) $prepaid_gas)

              (func (export "main")
                (call_indirect (type $prepaid_gas_t) (i32.const 0))
                drop
              )
            )
            "#
        )
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 328909092 used gas 328909092
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 335491140 used gas 335491140
            "#]],
        ]);
}

/// Load from address so far out of bounds that it causes integer overflow.
#[test]
fn test_address_overflow() {
    let code = r#"
        (module
          (memory 1)
          (func (export "main")
            i32.const 1
            i64.load32_u offset=4294967295 (;2^32 - 1;) align=1
            drop
          )
        )
    "#;

    test_builder()
        .wat(code)
        .skip_wasmtime()
        .skip_wasmer0()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 48534981 used gas 48534981
                Err: WebAssembly trap: Memory out of bounds trap.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 54294273 used gas 54294273
                Err: WebAssembly trap: Memory out of bounds trap.
            "#]],
        ]);

    // wasmer0 incorrectly doesn't catch overflow during address calculation
    test_builder()
        .wat(code)
        .only_wasmer0()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 48534981 used gas 48534981
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 55117029 used gas 55117029
            "#]],
        ]);
}

/// Uses `f32.copysign` to observe a sign of `NaN`.
///
/// WASM specification allows different behaviors here:
///
///   https://github.com/WebAssembly/design/blob/main/Nondeterminism.md
///
/// We solve this problem by canonicalizing NaNs.
#[test]
fn test_nan_sign() {
    let code = r#"
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

    test_builder()
        .wat(code)
        .skip_wasmtime()
        .skip_wasmer0()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 54988767 used gas 54988767
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 61570815 used gas 61570815
            "#]],
        ]);

    // wasmer0 doesn't canonicalize NaNs
    test_builder()
        .wat(code)
        .only_wasmer0()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 54988767 used gas 54988767
                Err: WebAssembly trap: An arithmetic exception, e.g. divided by zero.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 60748059 used gas 60748059
                Err: WebAssembly trap: An arithmetic exception, e.g. divided by zero.
            "#]],
        ]);
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
    test_builder()
        .wat(
            r#"
(module
  (import "env" "gas" (func $gas (param i32)))
  (func (export "main")
    (call $gas (i32.const 0xffff_ffff)))
)"#,
        )
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100000000000000 used gas 100000000000000
            Err: Exceeded the maximum amount of gas allowed to burn per contract.
        "#]]);
}

// Call the "gas" host function indirectly with unreasonably large values,
// trying to force overflow.
#[test]
fn gas_overflow_indirect_call() {
    test_builder()
        .wat(
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
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100000000000000 used gas 100000000000000
            Err: Exceeded the maximum amount of gas allowed to burn per contract.
        "#]]);
}

#[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
mod fix_contract_loading_cost_protocol_upgrade {
    use super::*;
    use crate::tests::prepaid_loading_gas;

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
            .protocol_features(&[
                ProtocolFeature::FixContractLoadingCost,
                ProtocolFeature::PreparationV2
            ])
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 53989035 used gas 53989035
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 47406987 used gas 47406987
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 53989035 used gas 53989035
                "#]],
            ]);
    }

    // Executing with just enough gas to load the contract will fail before and
    // after. Both charge the same amount of gas.
    #[test]
    fn test_fn_loading_gas_protocol_upgrade_exceed_loading() {
        let tb = test_builder().wat(ALMOST_TRIVIAL_CONTRACT);
        let loading_cost = prepaid_loading_gas(tb.get_wasm().len());
        tb
            .gas(loading_cost)
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
        let tb = test_builder().wat(ALMOST_TRIVIAL_CONTRACT);
        let loading_cost = prepaid_loading_gas(tb.get_wasm().len());
        tb
            .gas(loading_cost + 884037)
            .protocol_features(&[ProtocolFeature::FixContractLoadingCost])
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 45000000 used gas 45000000
                    Err: Exceeded the prepaid gas.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 45000000 used gas 45000000
                    Err: Exceeded the prepaid gas.
                "#]],
            ]);
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
            .wat(r#"(module (export "main" (func 0)))"#)
            .protocol_features(&[ProtocolFeature::FixContractLoadingCost])
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                    Err: PrepareError: Error happened while deserializing the module.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 39347463 used gas 39347463
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
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 48234213 used gas 48234213
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
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 47800713 used gas 47800713
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
