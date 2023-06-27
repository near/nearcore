use super::test_builder::test_builder;
use crate::logic::VMConfig;
use crate::prepare::prepare_contract;
use crate::tests::with_vm_variants;
use expect_test::expect;

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
fn ensure_fails_verification() {
    let config = VMConfig::test();
    with_vm_variants(&config, |kind| {
        for (feature_name, wat) in EXPECTED_UNSUPPORTED {
            let wasm = wat::parse_str(wat).expect("parsing test wat should succeed");
            if let Ok(_) = prepare_contract(&wasm, &config, kind) {
                panic!("wasm containing use of {} feature did not fail to prepare", feature_name);
            }
        }
    });
}

#[test]
fn ensure_fails_execution() {
    for (_feature_name, wat) in EXPECTED_UNSUPPORTED {
        test_builder().wat(wat).opaque_error().opaque_outcome().expect(expect![[r#"
            Err: ...
        "#]]);
    }
}
