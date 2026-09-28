//! Module that takes care of loading, checking and preprocessing of a
//! wasm module before execution.

use crate::ecc::EccOnlyFunctions;
use crate::logic::errors::PrepareError;
use near_parameters::vm::{Config, VMKind};

mod instrument_v3;
mod prepare_v3;

/// A contract after preparation, together with metadata extracted from the
/// original code.
#[derive(Debug)]
pub struct PreparedCode {
    /// The validated and instrumented wasm module.
    pub code: Vec<u8>,
    /// List of functions tagged as ECC-only
    /// (see [ECC tracking issue:](https://github.com/near/nearcore/issues/16423)).
    /// This list is always empty when the `ecc_only_functions` config flag is disabled.
    pub ecc_only_functions: EccOnlyFunctions,
}

/// Loads the given module given in `original_code`, performs some checks on it and
/// does some preprocessing.
///
/// The checks are:
///
/// - module doesn't define an internal memory instance,
/// - imported memory (if any) doesn't reserve more memory than permitted by the `config`,
/// - all imported functions from the external environment matches defined by `env` module,
/// - functions number does not exceed limit specified in Config,
///
/// The preprocessing includes injecting code for gas metering and metering the height of stack.
///
/// When the `ecc_only_functions` config flag is enabled, this also reads and
/// validates the `ecc_only_functions` custom section.
pub fn prepare_contract(
    original_code: &[u8],
    config: &Config,
    kind: VMKind,
) -> Result<PreparedCode, PrepareError> {
    let features = crate::features::WasmFeatures::new(config);
    prepare_v3::prepare_contract(original_code, features, config, kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ecc::ECC_ONLY_FUNCTIONS_SECTION;
    use crate::tests::{test_vm_config, with_vm_variants};
    use assert_matches::assert_matches;
    use std::borrow::Cow;
    use wasm_encoder::{CustomSection, Encode, Section};

    fn parse_and_prepare_wat(
        config: &Config,
        vm_kind: VMKind,
        wat: &str,
    ) -> Result<PreparedCode, PrepareError> {
        let wasm = wat::parse_str(wat).unwrap();
        prepare_contract(wasm.as_ref(), &config, vm_kind)
    }

    fn append_custom_section_padding(wasm: &mut Vec<u8>, size: usize) {
        let section =
            CustomSection { name: Cow::Borrowed("padding"), data: Cow::Owned(vec![0; size]) };
        wasm.push(section.id());
        section.encode(wasm);
    }

    #[test]
    fn locals_limit_depends_on_contract_size() {
        const LOCALS: usize = 100;

        let mut config = test_vm_config(Some(VMKind::Wasmtime));
        config.limit_config.max_locals_per_contract = None;
        config.limit_config.min_contract_size_per_local = Some(2);

        let compact = near_test_contracts::LargeContract {
            functions: 1,
            locals_per_function: LOCALS as u32,
            ..Default::default()
        }
        .make();
        assert!(compact.len() < 2 * LOCALS);
        assert_matches!(
            prepare_contract(&compact, &config, VMKind::Wasmtime),
            Err(PrepareError::TooManyLocals)
        );

        let mut padded = compact;
        append_custom_section_padding(&mut padded, 2 * LOCALS);
        assert!(padded.len() >= 2 * LOCALS);
        assert_matches!(prepare_contract(&padded, &config, VMKind::Wasmtime), Ok(_));

        // The absolute limit continues to apply independently of contract size.
        config.limit_config.max_locals_per_contract = Some(LOCALS as u64 - 1);
        assert_matches!(
            prepare_contract(&padded, &config, VMKind::Wasmtime),
            Err(PrepareError::TooManyLocals)
        );
    }

    #[test]
    fn internal_memory_declaration() {
        with_vm_variants(|kind| {
            let config = test_vm_config(Some(kind));
            let r = parse_and_prepare_wat(&config, kind, r#"(module (memory 1 1))"#);
            assert_matches!(r, Ok(_));
        })
    }

    #[test]
    fn memory_imports() {
        with_vm_variants(|kind| {
            let config = test_vm_config(Some(kind));
            // This test assumes that maximum page number is configured to a certain number.
            assert_eq!(config.limit_config.max_memory_pages, 2048);

            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module (import "env" "memory" (memory 1 1)))"#,
            );
            assert_matches!(r, Err(PrepareError::Memory));

            // No memory import
            let r = parse_and_prepare_wat(&config, kind, r#"(module)"#);
            assert_matches!(r, Ok(_));

            // initial exceed maximum
            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module (import "env" "memory" (memory 17 1)))"#,
            );
            assert_matches!(r, Err(PrepareError::Deserialization));

            // no maximum
            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module (import "env" "memory" (memory 1)))"#,
            );
            assert_matches!(r, Err(PrepareError::Memory));

            // requested maximum exceed configured maximum
            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module (import "env" "memory" (memory 1 33)))"#,
            );
            assert_matches!(r, Err(PrepareError::Memory));
        })
    }

    #[test]
    fn multiple_valid_memory_are_disabled() {
        with_vm_variants(|kind| {
            let config = test_vm_config(Some(kind));
            // Our preparation and sanitization pass assumes a single memory, so we should fail when
            // there are multiple specified.
            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module
                    (import "env" "memory" (memory 1 2048))
                    (import "env" "memory" (memory 1 2048))
                )"#,
            );
            assert_matches!(r, Err(_));
            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module
                    (import "env" "memory" (memory 1 2048))
                    (memory 1)
                )"#,
            );
            assert_matches!(r, Err(_));
        })
    }

    #[test]
    fn imports() {
        with_vm_variants(|kind| {
            let config = test_vm_config(Some(kind));
            // nothing can be imported from non-"env" module for now.
            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module (import "another_module" "memory" (memory 1 1)))"#,
            );
            assert_matches!(r, Err(PrepareError::Instantiate));

            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module (import "env" "gas" (func (param i32))))"#,
            );
            assert_matches!(r, Ok(_));

            // TODO: Address tests once we check proper function signatures.
            /*
            // wrong signature
            let r = parse_and_prepare_wat(r#"(module (import "env" "gas" (func (param i64))))"#);
            assert_matches!(r, Err(Error::Instantiate));

            // unknown function name
            let r = parse_and_prepare_wat(r#"(module (import "env" "unknown_func" (func)))"#);
            assert_matches!(r, Err(Error::Instantiate));
            */
        })
    }

    #[test]
    fn function_body_too_large() {
        with_vm_variants(|kind| {
            let limit: u64 = 1000;
            let mut config = test_vm_config(Some(kind));
            config.limit_config.max_function_body_size = Some(limit);

            // A function body with nops just over the limit should be rejected.
            let wasm = near_test_contracts::function_with_a_lot_of_nop(limit);
            let r = prepare_contract(&wasm, &config, kind);
            assert_matches!(r, Err(PrepareError::FunctionBodyTooLarge));

            // A function body with nops just under the limit should be accepted.
            let wasm = near_test_contracts::function_with_a_lot_of_nop(limit / 2);
            let r = prepare_contract(&wasm, &config, kind);
            assert_matches!(r, Ok(_));
        });
    }

    /// Build a wasm module with many small functions, each containing a single
    /// `if` block. The gas instrumentation inserts metering at every block
    /// boundary, so the instrumented output is much larger than the input.
    // TODO: move to near-test-contracts.
    fn contract_with_many_blocks(num_functions: u32) -> Vec<u8> {
        use wasm_encoder::{
            CodeSection, ExportKind, ExportSection, Function, FunctionSection, Instruction, Module,
            TypeSection, ValType,
        };
        let mut module = Module::new();
        let mut types = TypeSection::new();
        types.ty().function([], []);
        types.ty().function([ValType::I32], []);
        module.section(&types);

        let mut functions = FunctionSection::new();
        // function 0 is "main" with type 0
        functions.function(0);
        // remaining functions have type 1 (take an i32 param)
        for _ in 0..num_functions {
            functions.function(1);
        }
        module.section(&functions);

        let mut exports = ExportSection::new();
        exports.export("main", ExportKind::Func, 0);
        module.section(&exports);

        let mut code = CodeSection::new();
        // main: empty
        let mut main_fn = Function::new([]);
        main_fn.instruction(&Instruction::End);
        code.function(&main_fn);
        // each helper function: if (param) { nop } end
        for _ in 0..num_functions {
            let mut f = Function::new([]);
            f.instruction(&Instruction::LocalGet(0));
            f.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
            f.instruction(&Instruction::Nop);
            f.instruction(&Instruction::End); // end if
            f.instruction(&Instruction::End); // end function
            code.function(&f);
        }
        module.section(&code);
        module.finish()
    }

    /// Build a wasm module with a single function containing `num_blocks`
    /// sequential if-blocks.
    // TODO: move to near-test-contracts.
    fn contract_with_blocks_in_one_function(num_blocks: u32) -> Vec<u8> {
        use wasm_encoder::{
            CodeSection, ExportKind, ExportSection, Function, FunctionSection, Instruction, Module,
            TypeSection, ValType,
        };
        let mut module = Module::new();
        let mut types = TypeSection::new();
        types.ty().function([ValType::I32], []);
        module.section(&types);
        let mut functions = FunctionSection::new();
        functions.function(0);
        module.section(&functions);
        let mut exports = ExportSection::new();
        exports.export("main", ExportKind::Func, 0);
        module.section(&exports);
        let mut code = CodeSection::new();
        let mut f = Function::new([]);
        for _ in 0..num_blocks {
            f.instruction(&Instruction::LocalGet(0));
            f.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
            f.instruction(&Instruction::Nop);
            f.instruction(&Instruction::End);
        }
        f.instruction(&Instruction::End);
        code.function(&f);
        module.section(&code);
        module.finish()
    }

    #[test]
    fn too_many_blocks_per_function() {
        with_vm_variants(|kind| {
            let limit: u64 = 100;
            let mut config = test_vm_config(Some(kind));
            config.limit_config.max_blocks_per_function = Some(limit);

            // A function with blocks over the limit should be rejected.
            let wasm = contract_with_blocks_in_one_function(limit as u32 + 1);
            let r = prepare_contract(&wasm, &config, kind);
            assert_matches!(r, Err(PrepareError::TooManyBlocksPerFunction));

            // A function with blocks at the limit should be accepted.
            let wasm = contract_with_blocks_in_one_function(limit as u32);
            let r = prepare_contract(&wasm, &config, kind);
            assert_matches!(r, Ok(_));
        });
    }

    #[test]
    fn too_many_blocks_per_contract() {
        with_vm_variants(|kind| {
            let limit: u64 = 50;
            let mut config = test_vm_config(Some(kind));
            config.limit_config.max_blocks_per_contract = Some(limit);
            // No per-function limit.
            config.limit_config.max_blocks_per_function = None;

            // 100 functions x 1 block = 100 total blocks, should be rejected.
            let wasm = contract_with_many_blocks(100);
            let r = prepare_contract(&wasm, &config, kind);
            assert_matches!(r, Err(PrepareError::TooManyBlocksPerContract));

            // 50 functions x 1 block = 50 total blocks, should be accepted.
            let wasm = contract_with_many_blocks(50);
            let r = prepare_contract(&wasm, &config, kind);
            assert_matches!(r, Ok(_));
        });
    }

    /// Build a wasm module that declares `n` entries in the type section,
    /// each a `(func)` signature with a different i32 param count. One
    /// trivial `main` function exercises type 0 so the module is otherwise
    /// valid.
    fn contract_with_n_types(n: u32) -> Vec<u8> {
        use wasm_encoder::{
            CodeSection, ExportKind, ExportSection, Function, FunctionSection, Instruction, Module,
            TypeSection, ValType,
        };
        assert!(n >= 1, "need at least one type for the main function");
        let mut module = Module::new();
        let mut types = TypeSection::new();
        for i in 0..n {
            let params = vec![ValType::I32; i as usize];
            types.ty().function(params, []);
        }
        module.section(&types);
        let mut functions = FunctionSection::new();
        functions.function(0);
        module.section(&functions);
        let mut exports = ExportSection::new();
        exports.export("main", ExportKind::Func, 0);
        module.section(&exports);
        let mut code = CodeSection::new();
        let mut main_fn = Function::new([]);
        main_fn.instruction(&Instruction::End);
        code.function(&main_fn);
        module.section(&code);
        module.finish()
    }

    #[test]
    fn too_many_types() {
        with_vm_variants(|kind| {
            let limit: u64 = 16;
            let mut config = test_vm_config(Some(kind));
            config.limit_config.max_types_per_contract = Some(limit);

            let wasm = contract_with_n_types(limit as u32 + 1);
            let r = prepare_contract(&wasm, &config, kind);
            assert_matches!(r, Err(PrepareError::TooManyTypes));

            let wasm = contract_with_n_types(limit as u32);
            let r = prepare_contract(&wasm, &config, kind);
            assert_matches!(r, Ok(_));
        });
    }

    #[test]
    fn too_many_globals() {
        with_vm_variants(|kind| {
            let limit: u64 = 1000;
            let mut config = test_vm_config(Some(kind));
            config.limit_config.max_globals_per_contract = Some(limit);

            // Over the limit: rejected at prepare time.
            let wasm = near_test_contracts::contract_with_num_globals((limit + 1) as u32);
            let r = prepare_contract(&wasm, &config, kind);
            assert_matches!(r, Err(PrepareError::TooManyGlobals));

            // At the limit: accepted.
            let wasm = near_test_contracts::contract_with_num_globals(limit as u32);
            let r = prepare_contract(&wasm, &config, kind);
            assert_matches!(r, Ok(_));
        });
    }

    #[test]
    fn instrumented_code_too_large() {
        with_vm_variants(|kind| {
            let mut config = test_vm_config(Some(kind));
            // Raise the function body size limit so it doesn't interfere.
            config.limit_config.max_function_body_size = None;

            // First, figure out the instrumented size without a limit so we can
            // set a meaningful threshold.
            config.limit_config.max_instrumented_code_size = None;
            let wasm = contract_with_many_blocks(200);
            let instrumented = prepare_contract(&wasm, &config, kind).unwrap().code;
            let threshold = instrumented.len() as u64;

            // With a limit just below the instrumented size, preparation should
            // fail.
            config.limit_config.max_instrumented_code_size = Some(threshold - 1);
            let r = prepare_contract(&wasm, &config, kind);
            assert_matches!(r, Err(PrepareError::InstrumentedCodeTooLarge));

            // With a limit at exactly the instrumented size, it should pass.
            config.limit_config.max_instrumented_code_size = Some(threshold);
            let r = prepare_contract(&wasm, &config, kind);
            assert_matches!(r, Ok(_));
        });
    }

    #[test]
    fn too_many_function_params() {
        // Hard-coding config parameters in this test.
        // If the config changes and you have to update these numbers, it most
        // likely means you are about to make a breaking change to WASM
        // contracts, so be careful if that's why you are reading this.
        let max_per_fn = 64;
        let max_per_contract = 50_000;

        // num_params, num_functions, expected preparation Result
        check(1, 1000, Ok(()));
        check(max_per_fn, max_per_contract / max_per_fn, Ok(()));
        check(
            max_per_fn,
            (max_per_contract + max_per_fn) / max_per_fn,
            Err(PrepareError::TooManyParamsPerContract),
        );

        // check that TooManyParamsPerFunction hits before TooManyParamsPerContract
        check(
            max_per_fn + 1,
            (max_per_contract + max_per_fn) / max_per_fn,
            Err(PrepareError::TooManyParamsPerFunction),
        );

        // check that TooManyFunctions hits before TooManyParamsPerFunction
        check(max_per_fn + 1, 1, Err(PrepareError::TooManyParamsPerFunction));
        check(max_per_fn + 1, 10_001, Err(PrepareError::TooManyFunctions));

        // check that TooManyFunctions hits before TooManyParamsPerContract
        check(
            (max_per_contract + 10_000) / 10_000,
            10_000,
            Err(PrepareError::TooManyParamsPerContract),
        );
        check((max_per_contract + 10_000) / 10_000, 10_001, Err(PrepareError::TooManyFunctions));

        #[track_caller]
        fn check(num_params: usize, num_functions: usize, expect: Result<(), PrepareError>) {
            with_vm_variants(|kind| {
                let config = test_vm_config(Some(kind));
                let params =
                    std::iter::repeat("i32").take(num_params).collect::<Vec<_>>().join(" ");
                // anonymous function with N parameters and no body
                let function_def = format!("(func (param {params}))\n");
                let all_function_defs = function_def.repeat(num_functions);
                let test_result = parse_and_prepare_wat(
                    &config,
                    kind,
                    &format!(
                        r#"(module
                            {all_function_defs}
                        )"#
                    ),
                );

                if let Err(expected_err) = &expect {
                    let Err(err) = test_result else {
                        panic!(
                            "got Ok expecting error {expected_err}, vm={kind:?}, num_params={num_params}, num_functions={num_functions}"
                        );
                    };
                    assert_eq!(
                        err, *expected_err,
                        "got the wrong error, got {err} but was expecting {expected_err}, vm={kind:?}, num_params={num_params}, num_functions={num_functions}"
                    );
                } else {
                    assert!(
                        test_result.is_ok(),
                        "got error when expecting ok, {test_result:?}, vm={kind:?}, num_params={num_params}, num_functions={num_functions}"
                    );
                }
            })
        }
    }

    /// Reject contracts whose static operand-stack size (bytes) in any single
    /// function exceeds `max_operand_stack_bytes_per_function`.
    #[test]
    fn operand_stack_too_large() {
        with_vm_variants(|kind| {
            // 16 i64 pushes leave 128 bytes on the operand stack at peak.
            // Cap of 127 should reject; cap of 128 should accept.
            let push_then_drop = "(i64.const 0) ".repeat(16) + &"(drop) ".repeat(16);
            let wat = format!(
                r#"(module
                    (func (export "main") {push_then_drop})
                )"#
            );

            let mut config = test_vm_config(Some(kind));
            config.limit_config.max_operand_stack_bytes_per_function = Some(127);
            let r = parse_and_prepare_wat(&config, kind, &wat);
            assert_matches!(r, Err(PrepareError::OperandStackTooLarge));

            config.limit_config.max_operand_stack_bytes_per_function = Some(128);
            let r = parse_and_prepare_wat(&config, kind, &wat);
            assert_matches!(r, Ok(_));
        })
    }

    /// Module used by the `ecc_only_functions` tests: exports functions `a` and
    /// `b`, and a global `g`.
    const ECC_TEST_MODULE: &str = r#"(module
        (func (export "a"))
        (func (export "b"))
        (global (export "g") i32 (i32.const 0))
    )"#;

    fn encode_custom_section(name: &str, data: &[u8]) -> Vec<u8> {
        let section = CustomSection { name: Cow::Borrowed(name), data: Cow::Owned(data.to_vec()) };
        let mut bytes = vec![section.id()];
        section.encode(&mut bytes);
        bytes
    }

    /// Returns `ECC_TEST_MODULE` with an `ecc_only_functions` custom section
    /// for each entry of `sections`, appended after all other sections.
    fn ecc_test_module(sections: &[&[u8]]) -> Vec<u8> {
        let mut wasm = wat::parse_str(ECC_TEST_MODULE).unwrap();
        for data in sections {
            wasm.extend(encode_custom_section(ECC_ONLY_FUNCTIONS_SECTION, data));
        }
        wasm
    }

    fn ecc_test_config(enabled: bool) -> Config {
        let mut config = test_vm_config(Some(VMKind::Wasmtime));
        config.ecc_only_functions = enabled;
        config
    }

    fn prepare_ecc(config: &Config, wasm: &[u8]) -> Result<EccOnlyFunctions, PrepareError> {
        prepare_contract(wasm, config, VMKind::Wasmtime).map(|p| p.ecc_only_functions)
    }

    #[test]
    fn ecc_only_functions_valid() {
        let config = ecc_test_config(true);
        let ecc = prepare_ecc(&config, &ecc_test_module(&[b"b,a"])).unwrap();
        assert_eq!(ecc.iter().collect::<Vec<_>>(), ["a", "b"]);
        assert!(ecc.contains("a"));
        assert!(ecc.contains("b"));
        assert!(!ecc.contains("g"));
        assert!(!ecc.contains(""));

        let ecc = prepare_ecc(&config, &ecc_test_module(&[b"a"])).unwrap();
        assert_eq!(ecc.iter().collect::<Vec<_>>(), ["a"]);
    }

    /// The export section may come after the custom section.
    #[test]
    fn ecc_only_functions_before_export_section() {
        let config = ecc_test_config(true);
        let module = wat::parse_str(ECC_TEST_MODULE).unwrap();
        // Insert the custom section right after the 8-byte module header.
        let (header, rest) = module.split_at(8);
        let mut wasm = header.to_vec();
        wasm.extend(encode_custom_section(ECC_ONLY_FUNCTIONS_SECTION, b"a"));
        wasm.extend_from_slice(rest);
        let ecc = prepare_ecc(&config, &wasm).unwrap();
        assert_eq!(ecc.iter().collect::<Vec<_>>(), ["a"]);
    }

    #[test]
    fn ecc_only_functions_absent() {
        let config = ecc_test_config(true);
        let ecc = prepare_ecc(&config, &ecc_test_module(&[])).unwrap();
        assert!(ecc.is_empty());

        // Custom sections with other names are ignored.
        let mut wasm = ecc_test_module(&[]);
        wasm.extend(encode_custom_section("ecc_only_functions_", b"not valid"));
        let ecc = prepare_ecc(&config, &wasm).unwrap();
        assert!(ecc.is_empty());
    }

    /// With the config flag disabled the section is not parsed or validated.
    #[test]
    fn ecc_only_functions_disabled() {
        let config = ecc_test_config(false);
        for sections in [&[&b"a"[..]][..], &[b"\xff"], &[b"a b"], &[b"c"], &[b"a", b"b"]] {
            let ecc = prepare_ecc(&config, &ecc_test_module(sections)).unwrap();
            assert!(ecc.is_empty());
        }
    }

    #[test]
    fn ecc_only_functions_invalid() {
        let config = ecc_test_config(true);
        let cases: &[(&[&[u8]], PrepareError)] = &[
            (&[b"\xff"], PrepareError::ECCSectionInvalidUTF8),
            (&[b"a,\xc3"], PrepareError::ECCSectionInvalidUTF8),
            (&[b""], PrepareError::ECCSectionInvalidEntry),
            (&[b"a,,b"], PrepareError::ECCSectionInvalidEntry),
            (&[b",a"], PrepareError::ECCSectionInvalidEntry),
            (&[b"a,"], PrepareError::ECCSectionInvalidEntry),
            (&[b" a"], PrepareError::ECCSectionInvalidEntry),
            (&[b"a, b"], PrepareError::ECCSectionInvalidEntry),
            (&[b"a\n"], PrepareError::ECCSectionInvalidEntry),
            (&[b"a-b"], PrepareError::ECCSectionInvalidEntry),
            (&["\u{e1}".as_bytes()], PrepareError::ECCSectionInvalidEntry),
            (&[b"a\0"], PrepareError::ECCSectionInvalidEntry),
            (&[b"a,a"], PrepareError::ECCSectionDuplicateEntry),
            (&[b"a,b,a"], PrepareError::ECCSectionDuplicateEntry),
            (&[b"c"], PrepareError::ECCSectionUnknownFunction),
            (&[b"a,c"], PrepareError::ECCSectionUnknownFunction),
            // `g` is exported, but it is a global rather than a function.
            (&[b"g"], PrepareError::ECCSectionUnknownFunction),
            // The memory is exported under this name after preparation, not before.
            (&[b"memory"], PrepareError::ECCSectionUnknownFunction),
            (&[b"a", b"b"], PrepareError::ECCSectionRepeated),
            (&[b"a", b"a"], PrepareError::ECCSectionRepeated),
        ];
        for (sections, expected) in cases {
            let result = prepare_ecc(&config, &ecc_test_module(sections));
            assert_eq!(result, Err(expected.clone()), "sections: {sections:?}");
        }
    }
}
