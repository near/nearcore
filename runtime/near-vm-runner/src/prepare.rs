//! Module that takes care of loading, checking and preprocessing of a
//! wasm module before execution.

use crate::logic::errors::PrepareError;
use near_parameters::vm::{Config, VMKind};

mod instrument_v3;
mod prepare_v2;
mod prepare_v3;

/// Cheap section-header scan that extracts module-level counts from raw wasm.
/// Does not validate the module; just counts sections.
#[derive(Debug, Default)]
pub struct WasmStats {
    pub types: u32,
    pub imports: u32,
    pub functions: u32,
    pub tables: u32,
    pub memories: u32,
    pub globals: u32,
    pub exports: u32,
    pub data_segments: u32,
    pub element_segments: u32,
    pub max_function_body: u32,
}

pub fn extract_wasm_stats(code: &[u8]) -> WasmStats {
    use finite_wasm_6::wasmparser as wp;
    let mut stats = WasmStats::default();
    let parser = wp::Parser::new(0);
    for payload in parser.parse_all(code) {
        let Ok(payload) = payload else { break };
        match payload {
            wp::Payload::TypeSection(r) => stats.types = r.count(),
            wp::Payload::ImportSection(r) => stats.imports = r.count(),
            wp::Payload::FunctionSection(r) => stats.functions = r.count(),
            wp::Payload::TableSection(r) => stats.tables = r.count(),
            wp::Payload::MemorySection(r) => stats.memories = r.count(),
            wp::Payload::GlobalSection(r) => stats.globals = r.count(),
            wp::Payload::ExportSection(r) => stats.exports = r.count(),
            wp::Payload::DataSection(r) => stats.data_segments = r.count(),
            wp::Payload::ElementSection(r) => stats.element_segments = r.count(),
            wp::Payload::CodeSectionEntry(f) => {
                stats.max_function_body = stats.max_function_body.max(f.range().len() as u32);
            }
            _ => {}
        }
    }
    stats
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
pub fn prepare_contract(
    original_code: &[u8],
    config: &Config,
    kind: VMKind,
) -> Result<Vec<u8>, PrepareError> {
    let features = crate::features::WasmFeatures::new(config);
    if config.reftypes_bulk_memory || config.vm_kind == VMKind::Wasmtime {
        prepare_v3::prepare_contract(original_code, features, config, kind)
    } else {
        prepare_v2::prepare_contract(original_code, features, config, kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{test_vm_config, with_vm_variants};
    use assert_matches::assert_matches;

    fn parse_and_prepare_wat(
        config: &Config,
        vm_kind: VMKind,
        wat: &str,
    ) -> Result<Vec<u8>, PrepareError> {
        let wasm = wat::parse_str(wat).unwrap();
        prepare_contract(wasm.as_ref(), &config, vm_kind)
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
            let instrumented = prepare_contract(&wasm, &config, kind).unwrap();
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
}
