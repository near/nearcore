use super::test_builder::test_builder;
use expect_test::expect;
use std::fmt::Write;

const FIX_CONTRACT_LOADING_COST: u32 = 129;

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
        .expect(&expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 10000000000 used gas 10000000000
            Err: Exceeded the prepaid gas.
        "#]]);
}

#[test]
fn test_infinite_initializer_export_not_found() {
    #[allow(deprecated)]
    test_builder()
        .wat(INFINITE_INITIALIZER_CONTRACT)
        .method("no-such-method")
        .protocol_version(FIX_CONTRACT_LOADING_COST)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 0 storage_usage 0 return data None burnt gas 0 used gas 0
                Err: MethodNotFound
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 104071548 used gas 104071548
                Err: MethodNotFound
            "#]],
        ]);
}

static SIMPLE_CONTRACT: &str = r#"(module (func (export "main")))"#;

#[test]
fn test_simple_contract() {
    test_builder()
        .wat(SIMPLE_CONTRACT)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 79064041 used gas 79064041
            "#]],
        ]);
}

#[test]
fn test_imported_memory() {
    #[allow(deprecated)]
    test_builder()
        .wasm(&[
            0, 97, 115, 109, 1, 0, 0, 0, 2, 12, 1, 3, 101, 110, 118, 0, 2, 1, 239, 1, 248, 1, 4, 6,
            1, 112, 0, 143, 129, 32, 7, 12, 1, 8, 0, 17, 17, 17, 17, 17, 17, 2, 2, 0,
        ])
        // Wasmtime classifies this error as link error at the moment.
        .opaque_error()
        .protocol_version(FIX_CONTRACT_LOADING_COST)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 83374943 used gas 83374943
                Err: ...
            "#]],
        ]);
}

#[test]
fn test_multiple_memories() {
    #[allow(deprecated)]
    test_builder()
        .wat("(module (memory 1 2) (memory 3 4))")
        .opaque_error()
        .protocol_version(FIX_CONTRACT_LOADING_COST)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 53963978 used gas 53963978
                Err: ...
            "#]],
        ]);
}

#[test]
fn test_export_not_found() {
    #[allow(deprecated)]
    test_builder().wat(SIMPLE_CONTRACT)
        .method("no-such-method")
        .protocol_version(FIX_CONTRACT_LOADING_COST)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 0 storage_usage 0 return data None burnt gas 0 used gas 0
                Err: MethodNotFound
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 72481993 used gas 72481993
                Err: MethodNotFound
            "#]],
        ]);
}

#[test]
fn test_empty_method() {
    test_builder().wat(SIMPLE_CONTRACT).method("").expect(&expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
        Err: MethodEmptyName
    "#]]);
}

#[test]
fn test_trap_contract() {
    test_builder()
        .wat(r#"(module (func (export "main") (unreachable)) )"#)
        .skip_wasmtime()
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 80976092 used gas 80976092
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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 98404812 used gas 98404812
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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 88068079 used gas 88068079
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
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 92691798 used gas 92691798
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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 109031223 used gas 109031223
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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 134085008 used gas 134085008
                Err: WebAssembly trap: Call indirect incorrect signature trap.
            "#]]
        ])
}

#[test]
fn test_wrong_signature_contract() {
    #[allow(deprecated)]
    test_builder()
        .wat(r#"(module (func (export "main") (param i32)))"#)
        .protocol_version(FIX_CONTRACT_LOADING_COST)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 0 storage_usage 0 return data None burnt gas 0 used gas 0
                Err: MethodInvalidSignature
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 73571288 used gas 73571288
                Err: MethodInvalidSignature
            "#]],
        ]);
}

#[test]
fn test_export_wrong_type() {
    #[allow(deprecated)]
    test_builder()
        .wat(r#"(module (global (export "main") i32 (i32.const 123)))"#)
        .protocol_version(FIX_CONTRACT_LOADING_COST)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 0 storage_usage 0 return data None burnt gas 0 used gas 0
                Err: MethodNotFound
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 64856928 used gas 64856928
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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 381690938 used gas 381690938
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
        .expect(&expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 361214594 used gas 361214594
            Err: Smart contract panicked: explicit guest panic
        "#]]);
}

#[test]
fn test_stack_overflow() {
    test_builder()
        .wat(r#"(module (func $f (export "main") (call $f)))"#)
        .skip_wasmtime()
        .opaque_error()
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 30418898602 used gas 30418898602
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
        .skip_wasmtime()
        .opaque_error()
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 31825672528 used gas 31825672528
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
        .skip_wasmtime()
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 29757263944 used gas 29757263944
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
        .expect(&expect![[r#"
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
    #[allow(deprecated)]
    test_builder()
        .wasm(&bad_import_global("no-such-module"))
        .protocol_version(FIX_CONTRACT_LOADING_COST)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened during instantiation.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 111696613 used gas 111696613
                Err: PrepareError: Error happened during instantiation.
            "#]],
        ]);
}

#[test]
fn test_bad_import_2() {
    #[allow(deprecated)]
    test_builder()
        .wasm(&bad_import_func("no-such-module"))
        .protocol_version(FIX_CONTRACT_LOADING_COST)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened during instantiation.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 109518023 used gas 109518023
                Err: PrepareError: Error happened during instantiation.
            "#]],
        ]);
}

#[test]
fn test_bad_import_3() {
    #[allow(deprecated)]
    test_builder()
        .wasm(&bad_import_global("env"))
        .opaque_error()
        .protocol_version(FIX_CONTRACT_LOADING_COST)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 99714368 used gas 99714368
                Err: ...
            "#]],
        ]);
}

#[test]
fn test_bad_import_4() {
    test_builder().wasm(&bad_import_func("env")).opaque_error().expect(&expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 97535778 used gas 97535778
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
        .expect(&expect![[r#"
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
        .expect(&expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 1362207273 used gas 1362207273
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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 401031709 used gas 401031709
            "#]],
        ]);
}

#[test]
fn test_external_call_error() {
    test_builder().wat(EXTERNAL_CALL_CONTRACT).gas(100).expect(&expect![[r#"
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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 441069085 used gas 441069085
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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 97048978 used gas 97048978
                Err: WebAssembly trap: Memory out of bounds trap.
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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 110433335 used gas 110433335
            "#]],
        ]);
}

// Check that a `GasExceeded` error is returned when there is not enough gas to
// even load a contract.
#[test]
fn test_gas_exceed_loading() {
    test_builder().wat(SIMPLE_CONTRACT).method("non_empty_non_existing").gas(1).expect(&expect![[
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
        .expect(&expect![[r#"
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
        .expect(&expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100000000000000 used gas 100000000000000
            Err: Exceeded the maximum amount of gas allowed to burn per contract.
        "#]]);
}

mod fix_contract_loading_cost_protocol_upgrade {
    use super::*;
    use near_parameters::ExtCosts;

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
        #[allow(deprecated)]
        test_builder()
            .wat(ALMOST_TRIVIAL_CONTRACT)
            .protocol_version(FIX_CONTRACT_LOADING_COST)
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 88890835 used gas 88890835
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 88890835 used gas 88890835
                "#]],
            ]);
    }

    // Executing with just enough gas to load the contract will fail before and
    // after. Both charge the same amount of gas.
    #[test]
    fn test_fn_loading_gas_protocol_upgrade_exceed_loading() {
        let expect = expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 79017763 used gas 79017763
            Err: Exceeded the prepaid gas.
        "#]];
        let test_after = test_builder().wat(ALMOST_TRIVIAL_CONTRACT);
        let cfg_costs = &test_after.configs().next().unwrap().wasm_config.ext_costs;
        let loading_base = cfg_costs.gas_cost(ExtCosts::contract_loading_base);
        let loading_byte = cfg_costs.gas_cost(ExtCosts::contract_loading_bytes);
        let wasm_length = test_after.get_wasm().len();
        test_after.gas(loading_base + wasm_length as u64 * loading_byte).expect(&expect);
        #[allow(deprecated)]
        test_builder()
            .wat(ALMOST_TRIVIAL_CONTRACT)
            .only_protocol_versions(vec![FIX_CONTRACT_LOADING_COST - 1])
            .gas(loading_base + wasm_length as u64 * loading_byte)
            .expect(&expect);
    }

    /// Executing with enough gas to finish loading but not to execute the full
    /// contract should have the same outcome before and after.
    #[test]
    fn test_fn_loading_gas_protocol_upgrade_exceed_executing() {
        let expect = expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 79901800 used gas 79901800
            Err: Exceeded the prepaid gas.
        "#]];
        let test_after = test_builder().wat(ALMOST_TRIVIAL_CONTRACT);
        let cfg_costs = &test_after.configs().next().unwrap().wasm_config.ext_costs;
        let loading_base = cfg_costs.gas_cost(ExtCosts::contract_loading_base);
        let loading_byte = cfg_costs.gas_cost(ExtCosts::contract_loading_bytes);
        let wasm_length = test_after.get_wasm().len();
        let prepaid_gas = loading_base + wasm_length as u64 * loading_byte + 884037;
        test_after.gas(prepaid_gas).expect(&expect);
        #[allow(deprecated)]
        test_builder()
            .wat(ALMOST_TRIVIAL_CONTRACT)
            .only_protocol_versions(vec![FIX_CONTRACT_LOADING_COST - 1])
            .gas(prepaid_gas)
            .expect(&expect);
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

        #[allow(deprecated)]
        test_builder()
            .wat(r#"(module (export "main" (func 0)))"#)
            .protocol_version(FIX_CONTRACT_LOADING_COST)
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                    Err: PrepareError: Error happened while deserializing the module.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 55053273 used gas 55053273
                    Err: PrepareError: Error happened while deserializing the module.
                "#]],
            ]);

        #[allow(deprecated)]
        test_builder()
            .wasm(&bad_import_global("wtf"))
            .protocol_version(FIX_CONTRACT_LOADING_COST)
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                    Err: PrepareError: Error happened during instantiation.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 99714368 used gas 99714368
                    Err: PrepareError: Error happened during instantiation.
                "#]],
            ]);

        #[allow(deprecated)]
        test_builder()
            .wasm(&bad_import_func("wtf"))
            .protocol_version(FIX_CONTRACT_LOADING_COST)
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                    Err: PrepareError: Error happened during instantiation.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 97535778 used gas 97535778
                    Err: PrepareError: Error happened during instantiation.
                "#]],
            ]);

        #[allow(deprecated)]
        test_builder()
            .wasm(&near_test_contracts::LargeContract {
                functions: 101,
                locals_per_function: 9901,
                ..Default::default()
            }
            .make())
            .protocol_version(FIX_CONTRACT_LOADING_COST)
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                    Err: PrepareError: Too many locals declared in the contract.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 839345673 used gas 839345673
                    Err: PrepareError: Too many locals declared in the contract.
                "#]],
            ]);

        let functions_number_limit: u32 = 10_000;
        #[allow(deprecated)]
        test_builder()
            .wasm(&near_test_contracts::LargeContract {
                functions: functions_number_limit / 2,
                panic_imports: functions_number_limit / 2 + 1,
                ..Default::default()
            }
            .make())
            .protocol_version(FIX_CONTRACT_LOADING_COST)
            .expects(&[
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                    Err: PrepareError: Too many functions in contract.
                "#]],
                expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 98129728598 used gas 98129728598
                    Err: PrepareError: Too many functions in contract.
                "#]],
            ]);
    }
}

#[test]
fn test_regression_9393() {
    let after_builder = test_builder();
    let after_cost = after_builder.configs().next().unwrap().wasm_config.regular_op_cost;
    let cost = u64::from(after_cost);
    let nops = (i32::MAX as u64 + cost - 1) / cost;
    let contract = near_test_contracts::function_with_a_lot_of_nop(nops);
    after_builder.wasm(&contract).expects(&[
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 5073607792 used gas 5073607792
        "#]],
    ]);
}
