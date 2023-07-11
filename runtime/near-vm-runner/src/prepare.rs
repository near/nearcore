//! Module that takes care of loading, checking and preprocessing of a
//! wasm module before execution.

use crate::internal::VMKind;
use crate::logic::errors::PrepareError;
use crate::logic::VMConfig;

mod prepare_v0;
mod prepare_v1;
mod prepare_v2;

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
pub fn prepare_contract(
    original_code: &[u8],
    config: &VMConfig,
    kind: VMKind,
) -> Result<Vec<u8>, PrepareError> {
    let prepare = config.limit_config.contract_prepare_version;
    // NearVM => ContractPrepareVersion::V2
    assert!(
        (kind != VMKind::NearVm) || (prepare == crate::logic::ContractPrepareVersion::V2),
        "NearVM only works with contract prepare version V2",
    );
    let features = crate::features::WasmFeatures::from(prepare);
    match prepare {
        crate::logic::ContractPrepareVersion::V0 => {
            // NB: v1 here is not a bug, we are reusing the code.
            prepare_v1::validate_contract(original_code, features, config)?;
            prepare_v0::prepare_contract(original_code, config)
        }
        crate::logic::ContractPrepareVersion::V1 => {
            prepare_v1::validate_contract(original_code, features, config)?;
            prepare_v1::prepare_contract(original_code, config)
        }
        crate::logic::ContractPrepareVersion::V2 => {
            prepare_v2::prepare_contract(original_code, features, config, kind)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::with_vm_variants;
    use assert_matches::assert_matches;

    fn parse_and_prepare_wat(
        config: &VMConfig,
        vm_kind: VMKind,
        wat: &str,
    ) -> Result<Vec<u8>, PrepareError> {
        let wasm = wat::parse_str(wat).unwrap();
        prepare_contract(wasm.as_ref(), &config, vm_kind)
    }

    #[test]
    fn internal_memory_declaration() {
        let config = VMConfig::test();
        with_vm_variants(&config, |kind| {
            let r = parse_and_prepare_wat(&config, kind, r#"(module (memory 1 1))"#);
            assert_matches!(r, Ok(_));
        })
    }

    #[test]
    fn memory_imports() {
        let config = VMConfig::test();

        // This test assumes that maximum page number is configured to a certain number.
        assert_eq!(config.limit_config.max_memory_pages, 2048);

        with_vm_variants(&config, |kind| {
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
        let config = VMConfig::test();
        with_vm_variants(&config, |kind| {
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
        let config = VMConfig::test();
        with_vm_variants(&config, |kind| {
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
}
