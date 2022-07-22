use super::test_builder::test_builder;
use expect_test::expect;
use near_primitives::version::ProtocolFeature;

#[test]
fn test_initializer_wrong_signature_contract() {
    test_builder()
        .wat(
            r#"
(module
  (func $f (param i32))
  (start 0)
  (func (export "main"))
)"#,
        )
        .protocol_features(&[
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 48017463 used gas 48017463
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
        ]);
}

#[test]
/// StackHeightInstrumentation is weird but it's what we return for now
fn test_function_not_defined_contract() {
    test_builder()
        .wat(r#"(module (export "hello" (func 0)))"#)
        .method("hello")
        .protocol_features(&[
            ProtocolFeature::FixContractLoadingCost,
        ])
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
}

fn function_type_not_defined_contract(bad_type: u64) -> Vec<u8> {
    wat::parse_str(&format!(
        r#"
            (module
              (func $main (type {bad_type}))
              (export "main" (func $main))
            )"#,
    ))
    .unwrap()
}

#[test]
fn test_function_type_not_defined_contract_1() {
    test_builder()
        .wasm(&function_type_not_defined_contract(1))
        .protocol_features(&[
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 44982963 used gas 44982963
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
        ]);
}

#[test]
// Weird case. It's not valid wasm (wat2wasm validate will fail), but wasmer allows it.
fn test_function_type_not_defined_contract_2() {
    test_builder()
        .wasm(&function_type_not_defined_contract(0))
        .protocol_features(&[
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 44982963 used gas 44982963
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
        ]);
}

#[test]
fn test_garbage_contract() {
    test_builder()
        .wasm(&[])
        .protocol_features(&[
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 35445963 used gas 35445963
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
        ]);
}

#[test]
fn test_evil_function_index() {
    test_builder()
        .wat(r#"(module (func (export "main") call 4294967295))"#)
        .method("abort_with_zero")
        .protocol_features(&[
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 44115963 used gas 44115963
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
        ]);
}

#[test]
fn test_limit_contract_functions_number() {
    let functions_number_limit: u32 = 10_000;

    test_builder().wasm(
        &near_test_contracts::LargeContract {
            functions: functions_number_limit,
            ..Default::default()
        }
        .make(),
    )
    .protocol_features(&[
        ProtocolFeature::LimitContractFunctionsNumber,
        ProtocolFeature::FixContractLoadingCost,
    ])
    .expects(&[
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 13048032213 used gas 13048032213
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 13048032213 used gas 13048032213
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 13048032213 used gas 13048032213
        "#]],
    ]);

    test_builder().wasm(
        &near_test_contracts::LargeContract {
            functions: functions_number_limit + 1,
            ..Default::default()
        }
        .make(),
    )
    .protocol_features(&[
        ProtocolFeature::LimitContractFunctionsNumber,
        ProtocolFeature::FixContractLoadingCost,
    ])
    .expects(&[
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 13049332713 used gas 13049332713
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
            Err: PrepareError: Too many functions in contract.
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 13049332713 used gas 13049332713
            Err: PrepareError: Too many functions in contract.
        "#]],
    ]);

    test_builder()
        .wasm(
            &near_test_contracts::LargeContract {
                functions: functions_number_limit / 2,
                panic_imports: functions_number_limit / 2 + 1,
                ..Default::default()
            }
            .make(),
        )
        .protocol_features(&[
            ProtocolFeature::FixContractLoadingCost,
        ])
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

    test_builder()
        .wasm(
            &near_test_contracts::LargeContract {
                functions: functions_number_limit,
                panic_imports: 1,
                ..Default::default()
            }
            .make(),
        )
        .protocol_features(&[
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Too many functions in contract.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 13051283463 used gas 13051283463
                Err: PrepareError: Too many functions in contract.
            "#]],
        ]);
}

#[test]
fn test_limit_locals() {
    test_builder()
        .wasm(
            &near_test_contracts::LargeContract {
                functions: 1,
                locals_per_function: 50_001,
                ..Default::default()
            }
            .make(),
        )
        .protocol_features(&[
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 43682463 used gas 43682463
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
        ]);

    test_builder()
        .wasm(
            &near_test_contracts::LargeContract {
                functions: 1,
                locals_per_function: 50_000,
                ..Default::default()
            }
            .make(),
        )
        .skip_wasmtime()
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 43682463 used gas 43682463
            Err: WebAssembly trap: An `unreachable` opcode was executed.
        "#]]);
}

#[test]
fn test_limit_locals_global() {
    test_builder().wasm(&near_test_contracts::LargeContract {
        functions: 101,
        locals_per_function: 9901,
        ..Default::default()
    }
    .make())
    .protocol_features(&[
        ProtocolFeature::LimitContractLocals,
        ProtocolFeature::FixContractLoadingCost,
    ])
    .expects(&[
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 195407463 used gas 195407463
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
            Err: PrepareError: Too many locals declared in the contract.
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 195407463 used gas 195407463
            Err: PrepareError: Too many locals declared in the contract.
        "#]],
    ]);

    test_builder()
        .wasm(
            &near_test_contracts::LargeContract {
                functions: 64,
                locals_per_function: 15625,
                ..Default::default()
            }
            .make(),
        )
        .expect(expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 139269213 used gas 139269213
        "#]]);
}

#[test]
pub fn test_stablized_host_function() {
    test_builder()
        .wat(
            r#"
(module
  (import "env" "ripemd160" (func $ripemd160 (param i64 i64 i64)))
  (func (export "main")
    (call $ripemd160 (i64.const 0) (i64.const 0) (i64.const 0)))
)"#,
        )
        .protocol_features(&[ProtocolFeature::MathExtension])
        .opaque_error()
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 54519963 used gas 54519963
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 7143010623 used gas 7143010623
            "#]],
        ]);
}

#[test]
fn test_sandbox_only_function() {
    let tb = test_builder()
        .wat(
            r#"
(module
  (import "env" "sandbox_debug_log" (func $sandbox_debug_log (param i64 i64)))
  (func (export "main")
    (call $sandbox_debug_log (i64.const 0) (i64.const 1)))
)"#,
        )
        .opaque_error();

    #[cfg(feature = "sandbox")]
    tb.expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 59805981 used gas 59805981
    "#]]);

    #[cfg(not(feature = "sandbox"))]
    tb.expect(expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 57337713 used gas 57337713
        Err: ...
    "#]]);
}
