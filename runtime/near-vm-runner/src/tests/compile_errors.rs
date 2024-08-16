use super::test_builder::test_builder;
use expect_test::expect;
use near_primitives_core::version::ProtocolFeature;

const FIX_CONTRACT_LOADING_COST: u32 = 129;

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
        .protocol_version(FIX_CONTRACT_LOADING_COST)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 98625073 used gas 98625073
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
        .protocol_version(
            FIX_CONTRACT_LOADING_COST
        )
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 56142568 used gas 56142568
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
        .protocol_version(
            FIX_CONTRACT_LOADING_COST
        )
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 83374943 used gas 83374943
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
        ]);
}

#[test]
// Weird case. It's not valid wasm (wat2wasm validate will fail), but wasmer allows it.
fn test_function_type_not_defined_contract_2() {
    test_builder()
        .wasm(&function_type_not_defined_contract(0))
        .protocol_version(
            FIX_CONTRACT_LOADING_COST
        )
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 83374943 used gas 83374943
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
        ]);
}

#[test]
fn test_garbage_contract() {
    test_builder()
        .wasm(&[])
        .protocol_version(FIX_CONTRACT_LOADING_COST
        )
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
        .protocol_version(
            FIX_CONTRACT_LOADING_COST
        )
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 79017763 used gas 79017763
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
        ProtocolFeature::PreparationV2,
    ])
    .protocol_version(FIX_CONTRACT_LOADING_COST)
    .expects(&[
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 13048032213 used gas 13048032213
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 13048032213 used gas 13048032213
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 65437853336 used gas 65437853336
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 65437853336 used gas 65437853336
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
        ProtocolFeature::PreparationV2,
    ])
    .protocol_version(FIX_CONTRACT_LOADING_COST)
    .expects(&[
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 13049332713 used gas 13049332713
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
            Err: PrepareError: Too many functions in contract.
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
            Err: PrepareError: Too many functions in contract.
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 65437807058 used gas 65437807058
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
            ProtocolFeature::PreparationV2,
        ])
        .protocol_version(
            FIX_CONTRACT_LOADING_COST
                )
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Too many functions in contract.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Too many functions in contract.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 98129728598 used gas 98129728598
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
            ProtocolFeature::PreparationV2,

        ])
        .protocol_version(FIX_CONTRACT_LOADING_COST)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Too many functions in contract.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Too many functions in contract.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 65447610713 used gas 65447610713
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
            ProtocolFeature::PreparationV2,
        ])

        .protocol_version(
            FIX_CONTRACT_LOADING_COST
        )
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 76839173 used gas 76839173
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
        .skip_wasmer0()
        .opaque_error()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 43682463 used gas 43682463
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 76839173 used gas 76839173
                Err: ...
            "#]],
        ]);
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
        ProtocolFeature::PreparationV2,

    ])
    .protocol_version(FIX_CONTRACT_LOADING_COST,)
    .expects(&[
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 195407463 used gas 195407463
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
            Err: PrepareError: Too many locals declared in the contract.
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
            Err: PrepareError: Too many locals declared in the contract.
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 839345673 used gas 839345673
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
        .opaque_error()
        .protocol_features(&[
            ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 139269213 used gas 139269213
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 13419362816 used gas 13419362816
            "#]]
        ]);
}

#[test]
pub fn test_stabilized_host_function() {
    test_builder()
        .wat(
            r#"
(module
  (import "env" "ripemd160" (func $ripemd160 (param i64 i64 i64)))
  (func (export "main")
    (call $ripemd160 (i64.const 0) (i64.const 0) (i64.const 0)))
)"#,
        )
        .protocol_features(&[
            ProtocolFeature::MathExtension,
            ProtocolFeature::PreparationV2,
        ])
        .opaque_error()
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 54519963 used gas 54519963
                Err: ...
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 7143010623 used gas 7143010623
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 7226376631 used gas 7226376631
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
    tb.expect(&expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 154515074 used gas 154515074
    "#]]);

    #[cfg(not(feature = "sandbox"))]
    tb.expect(&expect![[r#"
        VMOutcome: balance 4 storage_usage 12 return data None burnt gas 145464758 used gas 145464758
        Err: ...
    "#]]);
}

#[test]
fn extension_saturating_float_to_int() {
    test_builder()
        .wat(
            r#"
            (module
                (func $test_trunc (param $x f64) (result i32) (i32.trunc_sat_f64_s (local.get $x)))
            )
            "#,
        )
        .protocol_version(FIX_CONTRACT_LOADING_COST)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100803663 used gas 100803663
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
        ]);
}

#[test]
fn extension_signext() {
    let tb = test_builder()
        .wat(
            r#"
            (module
                (func $extend8_s (param $x i32) (result i32) (i32.extend8_s (local.get $x)))
                (func (export "main"))
            )
            "#,
        )
        .protocol_features(&[ProtocolFeature::PreparationV2]);
    tb.expects(&[
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
            Err: PrepareError: Error happened while deserializing the module.
        "#]],
        expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 123725136 used gas 123725136
        "#]],
    ]);
}
