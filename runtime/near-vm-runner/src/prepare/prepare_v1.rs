//! Less legacy validation for old protocol versions.

use near_vm_errors::PrepareError;
use near_vm_logic::VMConfig;
use parity_wasm::builder;
use parity_wasm::elements::{self, External, MemorySection};

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
pub(crate) fn prepare_contract(
    original_code: &[u8],
    config: &VMConfig,
) -> Result<Vec<u8>, PrepareError> {
    ContractModule::init(original_code, config)?
        .scan_imports()?
        .standardize_mem()
        .ensure_no_internal_memory()?
        .inject_gas_metering()?
        .inject_stack_height_metering()?
        .into_wasm_code()
}

struct ContractModule<'a> {
    module: elements::Module,
    config: &'a VMConfig,
}

impl<'a> ContractModule<'a> {
    fn init(original_code: &[u8], config: &'a VMConfig) -> Result<Self, PrepareError> {
        let module = parity_wasm::deserialize_buffer(original_code).map_err(|e| {
            tracing::debug!(err=?e, "parity_wasm failed decoding a contract");
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
        let gas_rules = crate::instrument::rules::Set::new(1, Default::default())
            .with_grow_cost(config.grow_mem_cost);
        let module = crate::instrument::gas::inject_gas_counter(module, &gas_rules, "env")
            .map_err(|_| PrepareError::GasInstrumentation)?;
        Ok(Self { module, config })
    }

    fn inject_stack_height_metering(self) -> Result<Self, PrepareError> {
        let Self { module, config } = self;
        let module = crate::instrument::stack_height::inject_limiter(
            module,
            config.limit_config.max_stack_height,
        )
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

        for import in import_entries {
            if import.module() != "env" {
                // This import tries to import something from non-"env" module,
                // but all imports are located in "env" at the moment.
                return Err(PrepareError::Instantiate);
            }

            let type_idx = match *import.external() {
                External::Function(ref type_idx) => type_idx,
                External::Memory(_) => return Err(PrepareError::Memory),
                _ => continue,
            };

            let elements::Type::Function(ref _func_ty) =
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
        Ok(Self { module, config })
    }

    fn into_wasm_code(self) -> Result<Vec<u8>, PrepareError> {
        elements::serialize(self.module).map_err(|_| PrepareError::Serialization)
    }
}
