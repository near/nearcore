//! Module that takes care of loading, checking and preprocessing of a
//! wasm module before execution.

use near_vm_errors::PrepareError;
use near_vm_logic::VMConfig;

mod prepare_v0;
mod prepare_v1;

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

/// Loads the given module given in `original_code`, performs some checks on it and
/// does some preprocessing.
///
/// The checks are:
///
/// - module doesn't define an internal memory instance,
/// - imported memory (if any) doesn't reserve more memory than permitted by the `config`,
/// - all imported functions from the external environment matches defined by `env` module,
/// - functions number does not exceed limit specified in VMConfig,
///
/// The preprocessing includes injecting code for gas metering and metering the height of stack.
pub fn prepare_contract(original_code: &[u8], config: &VMConfig) -> Result<Vec<u8>, PrepareError> {
    prepare_v1::validate_contract(original_code, config)?;
    match config.limit_config.stack_limiter_version {
        near_vm_logic::StackLimiterVersion::V0 => {
            prepare_v0::prepare_contract(original_code, config)
        }
        near_vm_logic::StackLimiterVersion::V1 => {
            prepare_v1::prepare_contract(original_code, config)
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    fn parse_and_prepare_wat(wat: &str) -> Result<Vec<u8>, PrepareError> {
        let wasm = wat::parse_str(wat).unwrap();
        let config = VMConfig::test();
        prepare_contract(wasm.as_ref(), &config)
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
            if let Ok(_) = prepare_v1::validate_contract(input, &config) {
                match prepare_contract(input, &config) {
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
