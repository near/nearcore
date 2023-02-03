//! Module that takes care of loading, checking and preprocessing of a
//! wasm module before execution.

use crate::internal::VMKind;
use near_vm_errors::PrepareError;
use near_vm_logic::VMConfig;

mod prepare_v0;
mod prepare_v1;
mod prepare_v2;

pub(crate) const WASM_FEATURES: wasmparser::WasmFeatures = wasmparser::WasmFeatures {
    reference_types: false,
    // wasmer singlepass compiler requires multi_value return values to be disabled.
    multi_value: false,
    bulk_memory: false,
    module_linking: false,
    simd: false,
    threads: false,
    tail_call: false,
    deterministic_only: false,
    multi_memory: false,
    exceptions: false,
    memory64: false,
};

/// Decode and validate the provided WebAssembly code with the `wasmparser` crate.
///
/// This function will return the number of functions defined globally in the provided WebAssembly
/// module as well as the number of locals declared by all functions. If either counter overflows,
/// `None` is returned in its place.
fn wasmparser_decode(
    code: &[u8],
) -> Result<(Option<u64>, Option<u64>), wasmparser::BinaryReaderError> {
    use wasmparser::{ImportSectionEntryType, ValidPayload};
    let mut validator = wasmparser::Validator::new();
    validator.wasm_features(WASM_FEATURES);
    let mut function_count = Some(0u64);
    let mut local_count = Some(0u64);
    for payload in wasmparser::Parser::new(0).parse_all(code) {
        let payload = payload?;

        // The validator does not output `ValidPayload::Func` for imported functions.
        if let wasmparser::Payload::ImportSection(ref import_section_reader) = payload {
            let mut import_section_reader = import_section_reader.clone();
            for _ in 0..import_section_reader.get_count() {
                let import = import_section_reader.read()?;
                match import.ty {
                    ImportSectionEntryType::Function(_) => {
                        function_count = function_count.and_then(|f| f.checked_add(1))
                    }
                    ImportSectionEntryType::Table(_)
                    | ImportSectionEntryType::Memory(_)
                    | ImportSectionEntryType::Event(_)
                    | ImportSectionEntryType::Global(_)
                    | ImportSectionEntryType::Module(_)
                    | ImportSectionEntryType::Instance(_) => {}
                }
            }
        }

        match validator.payload(&payload)? {
            ValidPayload::Ok => (),
            ValidPayload::Submodule(_) => panic!("submodules are not reachable (not enabled)"),
            ValidPayload::Func(mut validator, body) => {
                validator.validate(&body)?;
                function_count = function_count.and_then(|f| f.checked_add(1));
                // Count the global number of local variables.
                let mut local_reader = body.get_locals_reader()?;
                for _ in 0..local_reader.get_count() {
                    let (count, _type) = local_reader.read()?;
                    local_count = local_count.and_then(|l| l.checked_add(count.into()));
                }
            }
        }
    }
    Ok((function_count, local_count))
}

fn validate_contract(code: &[u8], config: &VMConfig) -> Result<(), PrepareError> {
    let (function_count, local_count) = wasmparser_decode(code).map_err(|e| {
        tracing::debug!(err=?e, "wasmparser failed decoding a contract");
        PrepareError::Deserialization
    })?;
    // Verify the number of functions does not exceed the limit we imposed. Note that the ordering
    // of this check is important. In the past we first validated the entire module and only then
    // verified that the limit is not exceeded. While it would be more efficient to check for this
    // before validating the function bodies, it would change the results for malformed WebAssembly
    // modules.
    if let Some(max_functions) = config.limit_config.max_functions_number_per_contract {
        if function_count.ok_or(PrepareError::TooManyFunctions)? > max_functions {
            return Err(PrepareError::TooManyFunctions);
        }
    }
    // Similarly, do the same for the number of locals.
    if let Some(max_locals) = config.limit_config.max_locals_per_contract {
        if local_count.ok_or(PrepareError::TooManyLocals)? > max_locals {
            return Err(PrepareError::TooManyLocals);
        }
    }
    Ok(())
}

pub fn prepare_contract(
    original_code: &[u8],
    config: &VMConfig,
    kind: VMKind,
) -> Result<Vec<u8>, PrepareError> {
    validate_contract(original_code, config)?;
    match config.limit_config.contract_prepare_version {
        // Support for old protocol versions, where we incorrectly didn't
        // account for many locals of the same type.
        //
        // See `test_stack_instrumentation_protocol_upgrade` test.
        near_vm_logic::ContractPrepareVersion::V0 => {
            prepare_v0::prepare_contract(original_code, config)
        }
        near_vm_logic::ContractPrepareVersion::V1 => {
            prepare_v1::prepare_contract(original_code, config)
        }
        near_vm_logic::ContractPrepareVersion::V2 => {
            prepare_v2::prepare_contract(original_code, config, kind)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    fn parse_and_prepare_wat(wat: &str) -> Result<Vec<u8>, PrepareError> {
        let wasm = wat::parse_str(wat).unwrap();
        let config = VMConfig::test();
        prepare_contract(wasm.as_ref(), &config, VMKind::Wasmtime)
    }

    #[test]
    fn internal_memory_declaration() {
        let r = parse_and_prepare_wat(r#"(module (memory 1 1))"#);
        assert_matches!(r, Ok(_));
    }

    #[test]
    fn memory_imports() {
        // This test assumes that maximum page number is configured to a certain number.
        assert_eq!(VMConfig::test().limit_config.max_memory_pages, 2048);

        let r = parse_and_prepare_wat(r#"(module (import "env" "memory" (memory 1 1)))"#);
        assert_matches!(r, Err(PrepareError::Memory));

        // No memory import
        let r = parse_and_prepare_wat(r#"(module)"#);
        assert_matches!(r, Ok(_));

        // initial exceed maximum
        let r = parse_and_prepare_wat(r#"(module (import "env" "memory" (memory 17 1)))"#);
        assert_matches!(r, Err(PrepareError::Deserialization));

        // no maximum
        let r = parse_and_prepare_wat(r#"(module (import "env" "memory" (memory 1)))"#);
        assert_matches!(r, Err(PrepareError::Memory));

        // requested maximum exceed configured maximum
        let r = parse_and_prepare_wat(r#"(module (import "env" "memory" (memory 1 33)))"#);
        assert_matches!(r, Err(PrepareError::Memory));
    }

    #[test]
    fn multiple_valid_memory_are_disabled() {
        // Our preparation and sanitization pass assumes a single memory, so we should fail when
        // there are multiple specified.
        let r = parse_and_prepare_wat(
            r#"(module
          (import "env" "memory" (memory 1 2048))
          (import "env" "memory" (memory 1 2048))
        )"#,
        );
        assert_matches!(r, Err(_));
        let r = parse_and_prepare_wat(
            r#"(module
          (import "env" "memory" (memory 1 2048))
          (memory 1)
        )"#,
        );
        assert_matches!(r, Err(_));
    }

    #[test]
    fn imports() {
        // nothing can be imported from non-"env" module for now.
        let r =
            parse_and_prepare_wat(r#"(module (import "another_module" "memory" (memory 1 1)))"#);
        assert_matches!(r, Err(PrepareError::Instantiate));

        let r = parse_and_prepare_wat(r#"(module (import "env" "gas" (func (param i32))))"#);
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
    }

    #[test]
    fn preparation_generates_valid_contract() {
        bolero::check!().for_each(|input: &[u8]| {
            // DO NOT use ArbitraryModule. We do want modules that may be invalid here, if they pass our validation step!
            let config = VMConfig::test();
            if let Ok(_) = validate_contract(input, &config) {
                match prepare_contract(input, &config, VMKind::Wasmtime) {
                    Err(_e) => (), // TODO: this should be a panic, but for now it’d actually trigger
                    Ok(code) => {
                        let mut validator = wasmparser::Validator::new();
                        validator.wasm_features(WASM_FEATURES);
                        match validator.validate_all(&code) {
                            Ok(()) => (),
                            Err(e) => panic!(
                                "prepared code failed validation: {e:?}\ncontract: {}",
                                hex::encode(input),
                            ),
                        }
                    }
                }
            }
        });
    }
}
