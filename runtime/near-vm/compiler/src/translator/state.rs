// This file contains code from external sources.
// Attributions: https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md

use near_vm_types::entity::PrimaryMap;
use near_vm_types::{FunctionIndex, ImportIndex, ModuleInfo, SignatureIndex};
use std::boxed::Box;
use std::collections::HashMap;

/// Map of signatures to a function's parameter and return types.
pub(crate) type WasmTypes =
    PrimaryMap<SignatureIndex, (Box<[wasmparser::ValType]>, Box<[wasmparser::ValType]>)>;

/// Contains information decoded from the Wasm module that must be referenced
/// during each Wasm function's translation.
///
/// This is only for data that is maintained by `near-vm-compiler` itself, as
/// opposed to being maintained by the embedder. Data that is maintained by the
/// embedder is represented with `ModuleEnvironment`.
#[derive(Debug)]
pub struct ModuleTranslationState {
    /// A map containing a Wasm module's original, raw signatures.
    ///
    /// This is used for translating multi-value Wasm blocks inside functions,
    /// which are encoded to refer to their type signature via index.
    pub(crate) wasm_types: WasmTypes,

    /// Imported functions names map.
    pub import_map: HashMap<FunctionIndex, String>,
}

impl ModuleTranslationState {
    /// Creates a new empty ModuleTranslationState.
    pub fn new() -> Self {
        Self { wasm_types: PrimaryMap::new(), import_map: HashMap::new() }
    }

    /// Build map of imported functions names for intrinsification.
    #[tracing::instrument(skip_all)]
    pub fn build_import_map(&mut self, module: &ModuleInfo) {
        for key in module.imports.keys() {
            let value = &module.imports[key];
            match value {
                ImportIndex::Function(index) => {
                    self.import_map.insert(*index, key.1.clone());
                }
                _ => {
                    // Non-function import.
                }
            }
        }
    }
}
