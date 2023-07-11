//! Less legacy validation for old protocol versions.

use crate::logic::errors::PrepareError;
use crate::logic::VMConfig;
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

pub(crate) struct ContractModule<'a> {
    module: elements::Module,
    config: &'a VMConfig,
}

impl<'a> ContractModule<'a> {
    pub(crate) fn init(original_code: &[u8], config: &'a VMConfig) -> Result<Self, PrepareError> {
        let module = parity_wasm::deserialize_buffer(original_code).map_err(|e| {
            tracing::debug!(err=?e, "parity_wasm failed decoding a contract");
            PrepareError::Deserialization
        })?;
        Ok(ContractModule { module, config })
    }

    pub(crate) fn standardize_mem(self) -> Self {
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
    pub(crate) fn ensure_no_internal_memory(self) -> Result<Self, PrepareError> {
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
    pub(crate) fn scan_imports(self) -> Result<Self, PrepareError> {
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

    pub(crate) fn into_wasm_code(self) -> Result<Vec<u8>, PrepareError> {
        elements::serialize(self.module).map_err(|_| PrepareError::Serialization)
    }
}

/// Decode and validate the provided WebAssembly code with the `wasmparser` crate.
///
/// This function will return the number of functions defined globally in the provided WebAssembly
/// module as well as the number of locals declared by all functions. If either counter overflows,
/// `None` is returned in its place.
fn wasmparser_decode(
    code: &[u8],
    features: crate::features::WasmFeatures,
) -> Result<(Option<u64>, Option<u64>), wasmparser::BinaryReaderError> {
    use wasmparser::{ImportSectionEntryType, ValidPayload};
    let mut validator = wasmparser::Validator::new();
    validator.wasm_features(features.into());
    let mut function_count = Some(0u64);
    let mut local_count = Some(0u64);
    for payload in wasmparser::Parser::new(0).parse_all(code) {
        let payload = payload?;

        // The validator does not output `ValidPayload::Func` for imported functions.
        if let wasmparser::Payload::ImportSection(ref import_section_reader) = payload {
            let mut import_section_reader = import_section_reader.clone();
            for _ in 0..import_section_reader.get_count() {
                match import_section_reader.read()?.ty {
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

pub(crate) fn validate_contract(
    code: &[u8],
    features: crate::features::WasmFeatures,
    config: &VMConfig,
) -> Result<(), PrepareError> {
    let (function_count, local_count) = wasmparser_decode(code, features).map_err(|e| {
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

#[cfg(test)]
mod test {
    use crate::logic::{ContractPrepareVersion, VMConfig};

    #[test]
    fn v1_preparation_generates_valid_contract() {
        let mut config = VMConfig::test();
        let prepare_version = ContractPrepareVersion::V1;
        config.limit_config.contract_prepare_version = prepare_version;
        let features = crate::features::WasmFeatures::from(prepare_version);
        bolero::check!().for_each(|input: &[u8]| {
            // DO NOT use ArbitraryModule. We do want modules that may be invalid here, if they pass our validation step!
            if let Ok(_) = super::validate_contract(input, features, &config) {
                match super::prepare_contract(input, &config) {
                    Err(_e) => (), // TODO: this should be a panic, but for now it’d actually trigger
                    Ok(code) => {
                        let mut validator = wasmparser::Validator::new();
                        validator.wasm_features(features.into());
                        match validator.validate_all(&code) {
                            Ok(_) => (),
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
