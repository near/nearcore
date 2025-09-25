use super::test_builder::test_builder;
#[cfg(feature = "prepare")]
use super::test_vm_config;
use crate::tests::with_vm_variants;
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
    (get_local 0) (get_local 0))
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
  (func $entry (result i32) i32.const 0))
"#;

static MEMORY64: &str = r#"
(module (memory i64 0 0)
  (func $entry (result i32) i32.const 0))
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

#[test]
#[cfg(feature = "prepare")]
fn ensure_fails_verification() {
    with_vm_variants(|kind| {
        let config = test_vm_config(Some(kind));
        for (feature_name, wat) in EXPECTED_UNSUPPORTED {
            let wasm = wat::parse_str(wat).expect("parsing test wat should succeed");
            if let Ok(_) = crate::prepare::prepare_contract(&wasm, &config, kind) {
                panic!("wasm containing use of {} feature did not fail to prepare", feature_name);
            }
        }
    });
}

#[test]
fn ensure_fails_execution() {
    for (_feature_name, wat) in EXPECTED_UNSUPPORTED {
        test_builder().wat(wat).opaque_error().opaque_outcome().expect(&expect![[r#"
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

#[test]
fn memory_export_method() {
    test_builder()
        .wat(
            r#"
            (module
              (func (export "memory"))
            )"#,
        )
        .method("memory")
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 81242631 used gas 81242631
            "#]],
        ]);
}

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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 93224876 used gas 93224876
            "#]],
        ]);
}

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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 92135581 used gas 92135581
            "#]],
        ]);
}

#[test]
fn start_export_clash_duplicate() {
    test_builder()
        .wat(&format!(
            r#"
            (module
              (func (export "{START_EXPORT}") call 1)
              (func (export "main"))
              (start 1)
            )"#,
        ))
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 104164104 used gas 104164104
            "#]],
        ]);
}

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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 95403466 used gas 95403466
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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 92135581 used gas 92135581
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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 81196353 used gas 81196353
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
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 95357188 used gas 95357188
                Err: PrepareError: Error happened while deserializing the module.
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
