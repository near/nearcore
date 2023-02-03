use crate::internal::VMKind;
use finite_wasm::wasmparser as wp;
use near_vm_errors::PrepareError;
use near_vm_logic::VMConfig;

pub(crate) fn prepare_contract(
    original_code: &[u8],
    _config: &VMConfig,
    kind: VMKind,
) -> Result<Vec<u8>, PrepareError> {
    let original_code = early_prepare(original_code)?;
    if matches!(kind, VMKind::Wasmer2) {
        // Built-in wasmer2 code instruments code for itself.
        return Ok(original_code);
    }

    finite_wasm::Analysis::new()
        .analyze(&original_code)
        .map_err(|_| todo!())?
        // Make sure contracts can’t call the instrumentation functions via `env`.
        .instrument("internal", &original_code)
        .map_err(|_| todo!())
}

/// Early preparation code.
///
/// Must happen before the finite-wasm analysis and is applicable to Wasmer2 just as much as it is
/// applicable to other runtimes.
///
/// This will validate the module, normalize the memories within, apply limits.
fn early_prepare(code: &[u8]) -> Result<Vec<u8>, PrepareError> {
    use super::WASM_FEATURES as F;
    let mut validator = wp::Validator::new_with_features(wp::WasmFeatures {
        mutable_global: true,
        saturating_float_to_int: false,
        sign_extension: false,
        reference_types: F.reference_types,
        multi_value: F.multi_value,
        bulk_memory: F.bulk_memory,
        simd: F.simd,
        relaxed_simd: false,
        threads: F.threads,
        tail_call: F.tail_call,
        floats: true,
        multi_memory: F.multi_memory,
        exceptions: F.exceptions,
        memory64: F.memory64,
        extended_const: false,
        component_model: false,
        memory_control: false,
    });
    let mut func_validator_allocs = Default::default();

    for payload in wp::Parser::new(0).parse_all(code) {
        let payload = payload.expect("TODO");
        match payload {
            wp::Payload::Version { num, encoding, range } => {
                validator.version(num, encoding, &range).expect("TODO");
            }
            wp::Payload::TypeSection(reader) => {
                validator.type_section(&reader).expect("TODO");
            }
            wp::Payload::ImportSection(reader) => {
                validator.import_section(&reader).expect("TODO");
                todo!("verify all imports come from `env`");
            }
            wp::Payload::FunctionSection(reader) => {
                validator.function_section(&reader).expect("TODO");
            }
            wp::Payload::TableSection(reader) => {
                validator.table_section(&reader).expect("TODO");
            }
            wp::Payload::MemorySection(reader) => {
                validator.memory_section(&reader).expect("TODO");
                todo!("standardize memories");
            }
            wp::Payload::TagSection(reader) => {
                validator.tag_section(&reader).expect("TODO");
            }
            wp::Payload::GlobalSection(reader) => {
                validator.global_section(&reader).expect("TODO");
            }
            wp::Payload::ExportSection(reader) => {
                validator.export_section(&reader).expect("TODO");
            }
            wp::Payload::StartSection { func, range } => {
                validator.start_section(func, &range).expect("TODO");
            }
            wp::Payload::ElementSection(reader) => {
                validator.element_section(&reader).expect("TODO");
            }
            wp::Payload::DataCountSection { count, range } => {
                validator.data_count_section(count, &range).expect("TODO");
            }
            wp::Payload::DataSection(reader) => {
                validator.data_section(&reader).expect("TODO");
            }
            wp::Payload::CodeSectionStart { count, range, size } => {
                validator.code_section_start(count, &range).expect("TODO");
            }
            wp::Payload::CodeSectionEntry(body) => {
                let code_validator = validator.code_section_entry(&body).expect("TODO");
                let mut code_validator = code_validator.into_validator(func_validator_allocs);
                code_validator.validate(&body).expect("TODO");
                func_validator_allocs = code_validator.into_allocations();
            }
            wp::Payload::ModuleSection { parser, range } => {
                validator.module_section(&range).expect("TODO");
            }
            wp::Payload::InstanceSection(reader) => {
                validator.instance_section(&reader).expect("TODO");
            }
            wp::Payload::CoreTypeSection(reader) => {
                validator.core_type_section(&reader).expect("TODO");
            }
            wp::Payload::ComponentSection { parser, range } => {
                validator.component_section(&range).expect("TODO");
            }
            wp::Payload::ComponentInstanceSection(reader) => {
                validator.component_instance_section(&reader).expect("TODO");
            }
            wp::Payload::ComponentAliasSection(reader) => {
                validator.component_alias_section(&reader).expect("TODO");
            }
            wp::Payload::ComponentTypeSection(reader) => {
                validator.component_type_section(&reader).expect("TODO");
            }
            wp::Payload::ComponentCanonicalSection(reader) => {
                validator.component_canonical_section(&reader).expect("TODO");
            }
            wp::Payload::ComponentStartSection { start, range } => {
                validator.component_start_section(&start, &range).expect("TODO");
            }
            wp::Payload::ComponentImportSection(reader) => {
                validator.component_import_section(&reader).expect("TODO");
            }
            wp::Payload::ComponentExportSection(reader) => {
                validator.component_export_section(&reader).expect("TODO");
            }
            wp::Payload::CustomSection(_) => {}
            wp::Payload::UnknownSection { id, contents, range } => {
                validator.unknown_section(id, &range).expect("TODO");
            }
            wp::Payload::End(p) => {
                validator.end(p).expect("TODO");
            }
        }
    }

    // Is this necessary anymore?
    todo!("count functions and locals");
    todo!("enforce our own limits, don't rely on wasmparser for it");
    todo!("wasmencoder encode changed module, but only those parts that have actually changed");

    Ok(vec![])

}
