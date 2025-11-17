use super::test_builder::test_builder;
#[cfg(feature = "prepare")]
use super::test_vm_config;
#[cfg(feature = "prepare")]
use crate::{MEMORY_EXPORT, REMAINING_GAS_EXPORT, START_EXPORT};
use expect_test::expect;
use near_primitives_core::version::ProtocolFeature;

static SIMD: &str = r#"
(module
  (func $entry (result i32) i32.const 0)
  (func $test_simd (result i32)
    i32.const 42
    i32x4.splat
    i32x4.extract_lane 0)
  (export "test_simd" (func $test_simd))
)
"#;

static THREADS: &str = r#"
(module
  (func $entry (result i32) i32.const 0)
  (func (export "32.load8u") (param i32) (result i32)
    local.get 0 i32.atomic.load8_u)
)
"#;

static REFERENCE_TYPES: &str = r#"
(module
  (func $entry (result i32) i32.const 0)
  (table 2 externref)
  (elem (i32.const 0) externref (ref.null extern))
  (elem (i32.const 1) externref (ref.null extern))
)
"#;

static BULK_MEMORY: &str = r#"
(module
  (func $entry (result i32) i32.const 0)
  (func (export "memory.copy") (param i32 i32 i32)
    local.get 0
    local.get 1
    local.get 2
    memory.copy)
)
"#;

static MULTI_VALUE: &str = r#"
(module
  (func $entry (result i32) i32.const 0)
  (func $pick0 (param i64) (result i64 i64)
    (local.get 0) (local.get 0))
)
"#;

static TAIL_CALL: &str = r#"
(module
  (func $entry (result i32) i32.const 0)
  (func $const-i32 (result i32) (i32.const 0x132))
  (func (export "type-i32") (result i32) (return_call $const-i32))
)
"#;

// WAT does not understand the `register` thing…
// static MODULE_LINKING: &str = r#"
// (module (memory 0)
//   (func $entry (result i32) i32.const 0))
// (register "M")
// "#;

static MULTI_MEMORY: &str = r#"
(module
  (memory 0)
  (memory 1)
  (func $entry (result i32) i32.const 0)
)
"#;

static MEMORY64: &str = r#"
(module (memory i64 0 0)
  (func $entry (result i32) i32.const 0)
)
"#;

static EXCEPTIONS: &str = r#"
(module
  (func $entry (result i32) i32.const 0)
  (tag $e0 (export "e0"))
  (func (export "throw") (throw $e0))
)
"#;

static EXPECTED_UNSUPPORTED: &[(&str, &str)] = &[
    ("exceptions", EXCEPTIONS),
    ("memory64", MEMORY64),
    ("multi_memory", MULTI_MEMORY),
    // ("module_linking", MODULE_LINKING),
    ("tail_call", TAIL_CALL),
    ("multi_value", MULTI_VALUE),
    ("bulk_memory", BULK_MEMORY),
    ("reference_types", REFERENCE_TYPES),
    ("threads", THREADS),
    ("simd", SIMD),
];

fn componentize(wat: &str) -> String {
    format!(
        r#"
(component
  (core {}
  (core instance (instantiate 0))
)"#,
        wat.trim().strip_prefix("(").unwrap()
    )
}

#[test]
#[cfg(feature = "prepare")]
fn ensure_fails_verification() {
    crate::tests::with_vm_variants(|kind| {
        let mut config = test_vm_config(Some(kind));
        for (feature_name, wat) in EXPECTED_UNSUPPORTED {
            use near_parameters::vm::VMKind;

            let wasm = wat::parse_str(wat).expect("parsing test wat should succeed");
            if let Ok(_) = crate::prepare::prepare_contract(&wasm, &config, kind) {
                panic!("wasm containing use of {} feature did not fail to prepare", feature_name);
            }

            if kind == VMKind::Wasmtime {
                let wasm = wat::parse_str(componentize(wat))
                    .expect("parsing test component wat should succeed");
                config.component_model = true;
                if let Ok(_) = crate::prepare::prepare_contract(&wasm, &config, kind) {
                    panic!(
                        "component wasm containing use of {} feature did not fail to prepare",
                        feature_name
                    );
                }
            }
        }
    });
}

#[test]
fn ensure_fails_execution() {
    for (_feature_name, wat) in EXPECTED_UNSUPPORTED {
        test_builder()
            .wat(wat)
            .component_wat(&componentize(wat))
            .opaque_error()
            .opaque_outcome()
            .expect(&expect![[r#"
            Err: ...
        "#]])
            .component_expect(&expect![[r#"
            Err: ...
        "#]]);
    }
}

#[test]
fn extension_saturating_float_to_int() {
    #[allow(deprecated)]
    test_builder()
        .wat(
            r#"
            (module
                (func $test_trunc (param $x f64) (result i32) (i32.trunc_sat_f64_s (local.get $x)))
            )
            "#,
        )
        .protocol_features(&[
            ProtocolFeature::SaturatingFloatToInt,
            ProtocolFeature::FixContractLoadingCost,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 0 used gas 0
                Err: PrepareError: Error happened while deserializing the module.
            "#]],
            expect![[r#"
                VMOutcome: balance 0 storage_usage 0 return data None burnt gas 0 used gas 0
                Err: MethodNotFound
            "#]],
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100803663 used gas 100803663
                Err: MethodNotFound
            "#]],
        ]);
}

#[cfg(feature = "prepare")]
#[test]
fn memory_export_method() {
    test_builder()
        .wat(&format!(
            r#"
            (module
              (func (export "{MEMORY_EXPORT}"))
            )"#,
        ))
            .component_wat(&format!(
            r#"
            (component
              (core module
                (func (export "{MEMORY_EXPORT}"))
              )
              (core instance (instantiate 0))
              (func (export "{MEMORY_EXPORT}") (canon lift
                (core func 0 "{MEMORY_EXPORT}"))
              )
            )"#,
        ))
        .method(MEMORY_EXPORT)
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 81242631 used gas 81242631
            "#]],
        ])
        .component_expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 145511036 used gas 145511036
            "#]],
        ])
    ;
}

#[cfg(feature = "prepare")]
#[test]
fn memory_export_clash() {
    test_builder()
        .wat(&format!(
            r#"
            (module
              (func (export "{MEMORY_EXPORT}"))
              (func (export "main"))
            )"#,
        ))
        .component_wat(&format!(
            r#"
            (component
              (core module
                (func (export "{MEMORY_EXPORT}"))
                (func (export "main"))
              )
              (core instance (instantiate 0))
              (func (export "{MEMORY_EXPORT}") (canon lift
                (core func 0 "{MEMORY_EXPORT}"))
              )
              (func (export "main") (canon lift
                (core func 0 "main"))
              )
            )"#,
        ))
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 93224876 used gas 93224876
            "#]],
        ])
        .component_expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 196707901 used gas 196707901
            "#]],
        ]);
}

#[cfg(feature = "prepare")]
#[test]
fn gas_export_clash() {
    test_builder()
        .wat(&format!(
            r#"
            (module
              (global (export "{REMAINING_GAS_EXPORT}") (mut i64) i64.const 0)
              (func (export "main"))
            )"#,
        ))
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 105207121 used gas 105207121
            "#]],
        ]);
}

#[cfg(feature = "prepare")]
#[test]
fn start_export_clash() {
    test_builder()
        .wat(&format!(
            r#"
            (module
              (func (export "{START_EXPORT}"))
              (func (export "main"))
            )"#,
        ))
        .component_wat(&format!(
            r#"
            (component
              (core module
                (func (export "{START_EXPORT}"))
                (func (export "main"))
              )
              (core instance (instantiate 0))
              (func (export "{START_EXPORT}") (canon lift
                (core func 0 "{START_EXPORT}"))
              )
              (func (export "main") (canon lift
                (core func 0 "main"))
              )
            )"#,
        ))
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 92135581 used gas 92135581
            "#]],
        ])
        .component_expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 193440016 used gas 193440016
            "#]],
        ]);
}

#[cfg(feature = "prepare")]
#[test]
fn start_export_clash_duplicate() {
    test_builder()
        .wat(&format!(
            r#"
            (module
              (func (export "main"))
              (func (export "{START_EXPORT}") call 0)
              (start 0)
            )"#,
        ))
        .component_wat(&format!(
            r#"
            (component
              (core module
                (func (export "main"))
                (func (export "{START_EXPORT}") call 0)
                (start 0)
              )
              (core instance (instantiate 0))
              (func (export "main") (canon lift
                (core func 0 "main"))
              )
              (func (export "{START_EXPORT}") (canon lift
                (core func 0 "{START_EXPORT}"))
              )
            )"#,
        ))
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 104164104 used gas 104164104
            "#]],
        ])
        .component_expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 205468539 used gas 205468539
            "#]],
        ]);
}

#[cfg(feature = "prepare")]
#[test]
fn memory_export_internal() {
    test_builder()
        .wat(&format!(
            r#"
            (module
              (memory (export "{MEMORY_EXPORT}") 0 0)
              (func (export "main"))
            )"#,
        ))
        .component_wat(&format!(
            r#"
            (component
              (core module
                (memory (export "{MEMORY_EXPORT}") 0 0)
                (func (export "main"))
              )
              (core instance (instantiate 0))
              (func (export "main") (canon lift
                (core func 0 "main"))
              )
            )"#,
        ))
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 95403466 used gas 95403466
            "#]],
        ])
        .component_expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 155314691 used gas 155314691
            "#]],
        ]);
}

#[test]
fn memory_custom() {
    test_builder()
        .wat(
            r#"
            (module
              (memory (export "foo") 42 42)
              (func (export "main"))
            )"#,
        )
        .component_wat(
            r#"
            (component
              (core module
                (memory (export "foo") 42 42)
                (func (export "main"))
              )
              (core instance (instantiate 0))
              (func (export "main") (canon lift
                (core func 0 "main"))
              )
            )"#,
        )
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 92135581 used gas 92135581
            "#]],
        ])
        .component_expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 152046806 used gas 152046806
            "#]],
        ]);
}

#[test]
fn too_many_table_elements() {
    test_builder()
        .wat(
            r#"
            (module
              (func (export "main"))
              (table 1000001 funcref)
            )"#,
        )
        .component_wat(
            r#"
            (component
              (core module
                (func (export "main"))
                (table 1000001 funcref)
              )
              (core instance (instantiate 0))
              (func (export "main") (canon lift
                (core func 0 "main"))
              )
            )"#,
        )
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 81196353 used gas 81196353
                Err: PrepareError: Too many table elements declared in the contract.
            "#]],
        ])
        .component_expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 141107578 used gas 141107578
                Err: PrepareError: Too many table elements declared in the contract.
            "#]],
        ]);
}

#[test]
fn too_many_tables() {
    test_builder()
        .wat(
            r#"
            (module
              (func (export "main"))
              (table 0 funcref)
              (table 0 funcref)
              (table 0 funcref)
              (table 0 funcref)
              (table 0 funcref)
              (table 0 funcref)
            )"#,
        )
        .component_wat(
            r#"
            (component
              (core module
                (func (export "main"))
                (table 0 funcref)
                (table 0 funcref)
                (table 0 funcref)
                (table 0 funcref)
                (table 0 funcref)
                (table 0 funcref)
              )
              (core instance (instantiate 0))
              (func (export "main") (canon lift
                (core func 0 "main"))
              )
            )"#,
        )
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 95357188 used gas 95357188
                Err: PrepareError: Too many tables declared in the contract.
            "#]],
        ])
        .component_expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 155268413 used gas 155268413
                Err: PrepareError: Too many tables declared in the contract.
            "#]],
        ]);
}

#[test]
fn use_imports() {
    test_builder()
        .wat(
            r#"
            (module
              (import "env" "account_locked_balance" (func (param i64)))
              (import "env" "alt_bn128_g1_multiexp" (func (param i64 i64 i64)))
              (import "env" "alt_bn128_g1_sum" (func (param i64 i64 i64)))
              (import "env" "alt_bn128_pairing_check" (func (param i64 i64) (result i64)))
              (import "env" "keccak256" (func (param i64 i64 i64)))
              (import "env" "keccak512" (func (param i64 i64 i64)))
              (import "env" "bls12381_pairing_check" (func (param i64 i64) (result i64)))
              (import "env" "write_register" (func (param i64 i64 i64)))
              (import "env" "storage_has_key" (func (param i64 i64) (result i64)))
              (func (export "main")
                i64.const 0
                call 0

                i64.const 0
                i64.const 0
                i64.const 0
                call 1

                i64.const 0
                i64.const 0
                i64.const 0
                call 2

                i64.const 0
                i64.const 0
                call 3
                drop

                i64.const 0
                i64.const 0
                i64.const 0
                call 4

                i64.const 0
                i64.const 0
                i64.const 0
                call 5

                i64.const 0
                i64.const 0
                call 6
                drop

                i64.const 0
                i64.const 0
                i64.const 0
                call 7

                i64.const 0
                i64.const 0
                call 8
                drop
              )
            )"#,
        )
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 12637863260998 used gas 12637863260998
            "#]],
        ]);
}

#[test]
fn use_component_imports() {
    test_builder()
        .component_wat(
            r#"
            (component
              (core module
                (memory (export "memory") 17)
              )
              (core instance (instantiate 0))
              (alias core export 0 "memory" (core memory))

              (type
                (instance
                  (type (func (result u64)))
                  (export "block-height" (func (type 0)))

                  (type (option u64))
                  (type (func (param "register-id" u64) (result 1)))
                  (export "register-len" (func (type 2)))
                )
              )
              (core module
                (import "near:nearcore/runtime@0.1.0" "block-height" (func (result i64)))
                (import "near:nearcore/runtime@0.1.0" "register-len" (func (param i64 i32)))
                (func (export "main")
                  call 0
                  drop

                  i64.const 0
                  i32.const 24
                  call 1
                )
              )

              (import "near:nearcore/runtime@0.1.0" (instance (type 0)))
              (core func (canon lower (func 0 "block-height")))
              (core func (canon lower (func 0 "register-len") (memory 0)))
              (core instance
                (export "block-height" (func 0))
                (export "register-len" (func 1))
              )

              (core instance (instantiate 1
                (with "near:nearcore/runtime@0.1.0" (instance 1))
              ))
              (func (export "main") (canon lift
                (core func 2 "main"))
              )
            )"#,
        )
        .component_expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 1075664418 used gas 1075664418
            "#]],
        ]);
}

#[cfg(feature = "prepare")]
#[test]
fn import_unsafe_intrinsics() {
    test_builder()
        .component_wat(&format!(
            r#"
            (component
              (type
                (instance
                  (type (func (result u64)))
                  (type (func (param "ptr" u64) (result u64)))
                  (type (func (param "ptr" u64) (param "val" u64)))
                  (export "store-data-address" (func (type 0)))
                  (export "u64-native-load" (func (type 1)))
                  (export "u64-native-store" (func (type 2)))
                )
              )
              (import "unsafe-intrinsics" (instance (type 0)))
              (core module
                (func (export "main"))
              )
              (core instance (instantiate 0))
              (func (export "main") (canon lift
                (core func 0 "main"))
              )
            )"#,
        ))
        .component_expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 266376503 used gas 266376503
                Err: PrepareError: Error happened during instantiation.
            "#]],
        ]);
}
