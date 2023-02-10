// This file contains code from external sources.
// Attributions: https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md

//! Translation skeleton that traverses the whole WebAssembly module and call helper functions
//! to deal with each part of it.
use super::environ::ModuleEnvironment;
use super::sections::{
    parse_data_section, parse_element_section, parse_export_section, parse_function_section,
    parse_global_section, parse_import_section, parse_memory_section, parse_name_section,
    parse_start_section, parse_table_section, parse_type_section,
};
use super::state::ModuleTranslationState;
use crate::WasmResult;
use wasmparser::{NameSectionReader, Parser, Payload};

/// Translate a sequence of bytes forming a valid Wasm binary into a
/// parsed ModuleInfo `ModuleTranslationState`.
#[tracing::instrument(skip_all)]
pub fn translate_module<'data>(
    data: &'data [u8],
    environ: &mut ModuleEnvironment<'data>,
) -> WasmResult<ModuleTranslationState> {
    let mut module_translation_state = ModuleTranslationState::new();

    for payload in Parser::new(0).parse_all(data) {
        match payload? {
            Payload::Version { .. } | Payload::End(_) => {}

            Payload::TypeSection(types) => {
                parse_type_section(types, &mut module_translation_state, environ)?;
            }

            Payload::ImportSection(imports) => {
                parse_import_section(imports, environ)?;
            }

            Payload::FunctionSection(functions) => {
                parse_function_section(functions, environ)?;
            }

            Payload::TableSection(tables) => {
                parse_table_section(tables, environ)?;
            }

            Payload::MemorySection(memories) => {
                parse_memory_section(memories, environ)?;
            }

            Payload::GlobalSection(globals) => {
                parse_global_section(globals, environ)?;
            }

            Payload::ExportSection(exports) => {
                parse_export_section(exports, environ)?;
            }

            Payload::StartSection { func, .. } => {
                parse_start_section(func, environ)?;
            }

            Payload::ElementSection(elements) => {
                parse_element_section(elements, environ)?;
            }

            Payload::CodeSectionStart { .. } => {}
            Payload::CodeSectionEntry(code) => {
                let mut code = code.get_binary_reader();
                let size = code.bytes_remaining();
                let offset = code.original_position();
                environ.define_function_body(
                    &module_translation_state,
                    code.read_bytes(size)?,
                    offset,
                )?;
            }

            Payload::DataSection(data) => {
                parse_data_section(data, environ)?;
            }

            Payload::DataCountSection { count, .. } => {
                environ.reserve_passive_data(count)?;
            }

            Payload::InstanceSection(_) => {
                unimplemented!("module linking not implemented yet")
            }

            Payload::TagSection(_) => {
                unimplemented!("exception handling proposal is not implemented yet")
            }

            Payload::CustomSection(reader) => {
                if reader.name() == "name" {
                    parse_name_section(
                        NameSectionReader::new(reader.data(), reader.data_offset()),
                        environ,
                    )?;
                } else {
                    environ.custom_section(reader.name(), reader.data())?;
                }
            }

            Payload::ModuleSection { .. } => unimplemented!("module sections not supported yet"), // which proposal is this coming from?
            Payload::CoreTypeSection { .. } => {
                unimplemented!("component proposal is not implemented yet")
            }
            Payload::ComponentSection { .. } => {
                unimplemented!("component proposal is not implemented yet")
            }
            Payload::ComponentInstanceSection { .. } => {
                unimplemented!("component proposal is not implemented yet")
            }
            Payload::ComponentAliasSection { .. } => {
                unimplemented!("component proposal is not implemented yet")
            }
            Payload::ComponentTypeSection { .. } => {
                unimplemented!("component proposal is not implemented yet")
            }
            Payload::ComponentCanonicalSection { .. } => {
                unimplemented!("component proposal is not implemented yet")
            }
            Payload::ComponentStartSection { .. } => {
                unimplemented!("component proposal is not implemented yet")
            }
            Payload::ComponentImportSection { .. } => {
                unimplemented!("component proposal is not implemented yet")
            }
            Payload::ComponentExportSection { .. } => {
                unimplemented!("component proposal is not implemented yet")
            }

            Payload::UnknownSection { .. } => unreachable!(),
        }
    }

    module_translation_state.build_import_map(&environ.module);

    Ok(module_translation_state)
}
