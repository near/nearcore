//! Legacy validation for very old protocol versions.

use crate::logic::errors::PrepareError;
use crate::logic::VMConfig;
use parity_wasm_41::builder;
use parity_wasm_41::elements::{self, External, MemorySection, Type};
use pwasm_utils_12 as pwasm_utils;

pub(crate) fn prepare_contract(
    original_code: &[u8],
    config: &VMConfig,
) -> Result<Vec<u8>, PrepareError> {
    ContractModule::init(original_code, config)?
        .standardize_mem()
        .ensure_no_internal_memory()?
        .inject_gas_metering()?
        .inject_stack_height_metering()?
        .scan_imports()?
        .into_wasm_code()
}

struct ContractModule<'a> {
    module: elements::Module,
    config: &'a VMConfig,
}

impl<'a> ContractModule<'a> {
    fn init(original_code: &[u8], config: &'a VMConfig) -> Result<Self, PrepareError> {
        let module = elements::deserialize_buffer(original_code).map_err(|e| {
            tracing::debug!(err=?e, "parity_wasm_41 failed decoding a contract");
            PrepareError::Deserialization
        })?;
        Ok(ContractModule { module, config })
    }

    fn standardize_mem(self) -> Self {
        let Self { mut module, config } = self;

        let mut tmp = MemorySection::default();

        module.memory_section_mut().unwrap_or(&mut tmp).entries_mut().pop();

        let entry = elements::MemoryType::new(
            config.limit_config.initial_memory_pages,
            Some(config.limit_config.max_memory_pages),
        );

        let mut builder = builder::from_module(module);
        builder.push_import(elements::ImportEntry::new(
            "env".to_string(),
            "memory".to_string(),
            elements::External::Memory(entry),
        ));

        Self { module: builder.build(), config }
    }

    /// Ensures that module doesn't declare internal memories.
    ///
    /// In this runtime we only allow wasm module to import memory from the environment.
    /// Memory section contains declarations of internal linear memories, so if we find one
    /// we reject such a module.
    fn ensure_no_internal_memory(self) -> Result<Self, PrepareError> {
        if self.module.memory_section().map_or(false, |ms| !ms.entries().is_empty()) {
            Err(PrepareError::InternalMemoryDeclared)
        } else {
            Ok(self)
        }
    }

    fn inject_gas_metering(self) -> Result<Self, PrepareError> {
        let Self { module, config } = self;
        // Free config, no need for gas metering.
        if config.regular_op_cost == 0 {
            return Ok(Self { module, config });
        }
        let gas_rules = pwasm_utils::rules::Set::new(1, Default::default())
            .with_grow_cost(config.grow_mem_cost);
        let module = pwasm_utils::inject_gas_counter(module, &gas_rules)
            .map_err(|_| PrepareError::GasInstrumentation)?;
        Ok(Self { module, config })
    }

    fn inject_stack_height_metering(self) -> Result<Self, PrepareError> {
        let Self { module, config } = self;
        let module =
            pwasm_utils::stack_height::inject_limiter(module, config.limit_config.max_stack_height)
                .map_err(|_| PrepareError::StackHeightInstrumentation)?;
        Ok(Self { module, config })
    }

    /// Scan an import section if any.
    ///
    /// This accomplishes two tasks:
    ///
    /// - checks any imported function against defined host functions set, incl.
    ///   their signatures.
    /// - if there is a memory import, returns it's descriptor
    fn scan_imports(self) -> Result<Self, PrepareError> {
        let Self { module, config } = self;

        let types = module.type_section().map(elements::TypeSection::types).unwrap_or(&[]);
        let import_entries =
            module.import_section().map(elements::ImportSection::entries).unwrap_or(&[]);

        let mut imported_mem_type = None;

        for import in import_entries {
            if import.module() != "env" {
                // This import tries to import something from non-"env" module,
                // but all imports are located in "env" at the moment.
                return Err(PrepareError::Instantiate);
            }

            let type_idx = match *import.external() {
                External::Function(ref type_idx) => type_idx,
                External::Memory(ref memory_type) => {
                    imported_mem_type = Some(memory_type);
                    continue;
                }
                _ => continue,
            };

            let Type::Function(ref _func_ty) =
                types.get(*type_idx as usize).ok_or(PrepareError::Instantiate)?;

            // TODO: Function type check with Env
            /*

            let ext_func = env
                .funcs
                .get(import.field().as_bytes())
                .ok_or_else(|| Error::Instantiate)?;
            if !ext_func.func_type_matches(func_ty) {
                return Err(Error::Instantiate);
            }
            */
        }
        if let Some(memory_type) = imported_mem_type {
            // Inspect the module to extract the initial and maximum page count.
            let limits = memory_type.limits();
            if limits.initial() != config.limit_config.initial_memory_pages
                || limits.maximum() != Some(config.limit_config.max_memory_pages)
            {
                return Err(PrepareError::Memory);
            }
        } else {
            return Err(PrepareError::Memory);
        };
        Ok(Self { module, config })
    }

    fn into_wasm_code(self) -> Result<Vec<u8>, PrepareError> {
        elements::serialize(self.module).map_err(|_| PrepareError::Serialization)
    }
}
