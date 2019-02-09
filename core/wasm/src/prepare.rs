//! Module that takes care of loading, checking and preprocessing of a
//! wasm module before execution.

use parity_wasm::elements::{self, External, MemoryType, Type, MemorySection};
use parity_wasm::builder;
use pwasm_utils::{self, rules};
use crate::types::{Config, PrepareError as Error};

struct ContractModule<'a> {
    // An `Option` is used here for loaning (`take()`-ing) the module.
    // Invariant: Can't be `None` (i.e. on enter and on exit from the function
    // the value *must* be `Some`).
    module: Option<elements::Module>,
    config: &'a Config,
}

impl<'a> ContractModule<'a> {
    fn init(original_code: &[u8], config: &'a Config) -> Result<ContractModule<'a>, Error> {
        let module =
            elements::deserialize_buffer(original_code).map_err(|_| Error::Deserialization)?;
        Ok(ContractModule {
            module: Some(module),
            config,
        })
    }

    fn externalize_mem(&mut self) -> Result<(), Error> {
        let mut module = self
            .module
            .take()
            .expect("On entry to the function `module` can't be `None`; qed");

        let mut tmp = MemorySection::default();

        let entry = module.memory_section_mut()
            .unwrap_or_else(|| &mut tmp)
            .entries_mut()
            .pop();

        match entry {
            Some(mut entry) => {
                if entry.limits().maximum().is_none() {
                    entry = elements::MemoryType::new(
                        entry.limits().initial(),
                        Some(self.config.max_memory_pages));
                }

                let mut builder = builder::from_module(module);
                builder.push_import(elements::ImportEntry::new(
                    "env".to_owned(),
                    "memory".to_owned(),
                    elements::External::Memory(entry),
                ));

                self.module = Some(builder.build());
            }
            None => self.module = Some(module)
        };

        Ok(())
    }

    /// Ensures that module doesn't declare internal memories.
    ///
    /// In this runtime we only allow wasm module to import memory from the environment.
    /// Memory section contains declarations of internal linear memories, so if we find one
    /// we reject such a module.
    fn ensure_no_internal_memory(&self) -> Result<(), Error> {
        let module = self
            .module
            .as_ref()
            .expect("On entry to the function `module` can't be None; qed");
        if module
            .memory_section()
            .map_or(false, |ms| !ms.entries().is_empty())
        {
            return Err(Error::InternalMemoryDeclared);
        }
        Ok(())
    }

    fn inject_gas_metering(&mut self) -> Result<(), Error> {
        // TODO(#194): Re-enable .with_forbidden_floats() once AssemblyScript is fixed.
        let gas_rules = rules::Set::new(self.config.regular_op_cost, Default::default())
            .with_grow_cost(self.config.grow_mem_cost);

        let module = self
            .module
            .take()
            .expect("On entry to the function `module` can't be `None`; qed");

        let contract_module = pwasm_utils::inject_gas_counter(module, &gas_rules)
            .map_err(|_| Error::GasInstrumentation)?;

        self.module = Some(contract_module);
        Ok(())
    }

    fn inject_stack_height_metering(&mut self) -> Result<(), Error> {
        let module = self
            .module
            .take()
            .expect("On entry to the function `module` can't be `None`; qed");

        let contract_module =
            pwasm_utils::stack_height::inject_limiter(module, self.config.max_stack_height)
                .map_err(|_| Error::StackHeightInstrumentation)?;

        self.module = Some(contract_module);
        Ok(())
    }

    /// Scan an import section if any.
    ///
    /// This accomplishes two tasks:
    ///
    /// - checks any imported function against defined host functions set, incl.
    ///   their signatures.
    /// - if there is a memory import, returns it's descriptor
    fn scan_imports(&self) -> Result<Option<&MemoryType>, Error> {
        let module = self
            .module
            .as_ref()
            .expect("On entry to the function `module` can't be `None`; qed");

        let types = module.type_section().map(|ts| ts.types()).unwrap_or(&[]);
        let import_entries = module
            .import_section()
            .map(|is| is.entries())
            .unwrap_or(&[]);

        let mut imported_mem_type = None;

        for import in import_entries {
            if import.module() != "env" {
                // This import tries to import something from non-"env" module,
                // but all imports are located in "env" at the moment.
                return Err(Error::Instantiate);
            }

            let type_idx = match *import.external() {
                External::Function(ref type_idx) => type_idx,
                External::Memory(ref memory_type) => {
                    imported_mem_type = Some(memory_type);
                    continue;
                }
                _ => continue,
            };

            let Type::Function(ref _func_ty) = types
                .get(*type_idx as usize)
                .ok_or_else(|| Error::Instantiate)?;

            // TODO: Function type check with Env
			/*

			let ext_func = env
				.funcs
				.get(import.field().as_bytes())
				.ok_or_else(|| Error::Instantiate)?;
			if !ext_func.func_type_matches(func_ty) {
				return Err(Error::Instantiate);
			}
			*/        }
        Ok(imported_mem_type)
    }

    fn into_wasm_code(mut self) -> Result<Vec<u8>, Error> {
        elements::serialize(
            self.module
                .take()
                .expect("On entry to the function `module` can't be `None`; qed"),
        ).map_err(|_| Error::Serialization)
    }
}

/// Loads the given module given in `original_code`, performs some checks on it and
/// does some preprocessing.
///
/// The checks are:
///
/// - module doesn't define an internal memory instance,
/// - imported memory (if any) doesn't reserve more memory than permitted by the `config`,
/// - all imported functions from the external environment matches defined by `env` module,
///
/// The preprocessing includes injecting code for gas metering and metering the height of stack.
pub(super) fn prepare_contract(
    original_code: &[u8],
    config: &Config,
) -> Result<Vec<u8>, Error> {
    let mut contract_module = ContractModule::init(original_code, config)?;
    contract_module.externalize_mem()?;
    contract_module.ensure_no_internal_memory()?;
    contract_module.inject_gas_metering()?;
    contract_module.inject_stack_height_metering()?;

    if let Some(memory_type) = contract_module.scan_imports()? {
        // Inspect the module to extract the initial and maximum page count.
        let limits = memory_type.limits();
        match (limits.initial(), limits.maximum()) {
            (initial, Some(maximum)) if initial > maximum => {
                // Requested initial number of pages should not exceed the requested maximum.
                return Err(Error::Memory);
            }
            (_, Some(maximum)) if maximum > config.max_memory_pages => {
                // Maximum number of pages should not exceed the configured maximum.
                return Err(Error::Memory);
            }
            (_, None) => {
                // Maximum number of pages should be always declared.
                // This isn't a hard requirement and can be treated as a maxiumum set
                // to configured maximum.
                return Err(Error::Memory);
            }
            // (initial, maximum) => Memory::init(initial, maximum),
            _ => (),
        }
    }

    contract_module.into_wasm_code()
}

#[cfg(test)]
mod tests {
    use super::*;
    use wabt;

    fn parse_and_prepare_wat(wat: &str) -> Result<Vec<u8>, Error> {
        let wasm = wabt::Wat2Wasm::new().validate(false).convert(wat).unwrap();
        let config = Config::default();
        prepare_contract(wasm.as_ref(), &config)
    }

    #[test]
    fn internal_memory_declaration() {
        let r = parse_and_prepare_wat(r#"(module (memory 1 1))"#);
        assert_matches!(r, Ok(_));
    }

    #[test]
    fn memory() {
        // This test assumes that maximum page number is configured to a certain number.
        assert_eq!(Config::default().max_memory_pages, 32);

        let r = parse_and_prepare_wat(r#"(module (import "env" "memory" (memory 1 1)))"#);
        assert_matches!(r, Ok(_));

        // No memory import
        let r = parse_and_prepare_wat(r#"(module)"#);
        assert_matches!(r, Ok(_));

        // initial exceed maximum
        let r = parse_and_prepare_wat(r#"(module (import "env" "memory" (memory 17 1)))"#);
        assert_matches!(r, Err(Error::Memory));

        // no maximum
        let r = parse_and_prepare_wat(r#"(module (import "env" "memory" (memory 1)))"#);
        assert_matches!(r, Err(Error::Memory));

        // requested maximum exceed configured maximum
        let r = parse_and_prepare_wat(r#"(module (import "env" "memory" (memory 1 33)))"#);
        assert_matches!(r, Err(Error::Memory));
    }

    #[test]
    fn imports() {
        // nothing can be imported from non-"env" module for now.
        let r =
            parse_and_prepare_wat(r#"(module (import "another_module" "memory" (memory 1 1)))"#);
        assert_matches!(r, Err(Error::Instantiate));

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
		*/    }
}
