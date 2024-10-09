#[allow(dead_code)]
mod opts {
    pub(super) const SIMD: bool = false;
    pub(super) const THREADS: bool = false;
    pub(super) const TAIL_CALL: bool = false;
    pub(super) const MULTI_MEMORY: bool = false;
    pub(super) const MEMORY64: bool = false;
    pub(super) const SATURATING_FLOAT_TO_INT: bool = false;
    pub(super) const EXCEPTIONS: bool = false;
    pub(super) const RELAXED_SIMD: bool = false;
    pub(super) const EXTENDED_COST: bool = false;
    pub(super) const COMPONENT_MODEL: bool = false;
    pub(super) const GC: bool = false;
    pub(super) const FUNCTION_REFERENCES: bool = false;
    pub(super) const MEMORY_CONTROL: bool = false;
}
#[allow(unused_imports)]
use opts::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct WasmFeatures {
    sign_extension: bool,
    reference_types: bool,
    multi_value: bool,
}

impl From<crate::logic::ContractPrepareVersion> for WasmFeatures {
    fn from(version: crate::logic::ContractPrepareVersion) -> Self {
        use crate::logic::ContractPrepareVersion::*;
        let sign_extension = match version {
            V0 => false,
            V1 => false,
            V2 | V3 => true,
        };
        let (multi_value, reference_types) = match version {
            V0 | V1 | V2 => (false, false),
            V3 => (true, true),
        };
        WasmFeatures { sign_extension, multi_value, reference_types }
    }
}

#[cfg(feature = "finite-wasm")]
impl From<WasmFeatures> for finite_wasm::wasmparser::WasmFeatures {
    fn from(f: WasmFeatures) -> Self {
        finite_wasm::wasmparser::WasmFeatures {
            floats: true,
            mutable_global: true,
            sign_extension: f.sign_extension,
            reference_types: f.reference_types,
            bulk_memory: f.reference_types, // bulk_memory is required if reference_types are to be
                                            // enabled
            multi_value: f.multi_value,

            simd: SIMD,
            threads: THREADS,
            tail_call: TAIL_CALL,
            multi_memory: MULTI_MEMORY,
            exceptions: EXCEPTIONS,
            memory64: MEMORY64,
            saturating_float_to_int: SATURATING_FLOAT_TO_INT,
            relaxed_simd: RELAXED_SIMD,
            extended_const: EXTENDED_COST,
            component_model: COMPONENT_MODEL,
            function_references: FUNCTION_REFERENCES,
            memory_control: MEMORY_CONTROL,
            gc: GC,
        }
    }
}

#[cfg(feature = "wasmparser")]
impl From<WasmFeatures> for wasmparser::WasmFeatures {
    fn from(f: WasmFeatures) -> Self {
        // /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\
        //
        // There are features that this version of wasmparser enables by default, but pwasm
        // currently does not and the compilers' support for these features is therefore largely
        // untested if it exists at all. Non exhaustive list of examples:
        //
        // * saturating_float_to_int
        // * sign_extension
        //
        // This is instead ensured by the fact that the V0 and V1 use pwasm utils in preparation
        // and it does not support these extensions.
        //
        // /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\
        wasmparser::WasmFeatures {
            deterministic_only: false,

            module_linking: false, // old version of component model
            reference_types: f.reference_types,
            multi_value: f.multi_value,
            bulk_memory: f.reference_types, // bulk_memory is required by reference_types
            simd: SIMD,
            threads: THREADS,
            tail_call: TAIL_CALL,
            multi_memory: MULTI_MEMORY,
            exceptions: EXCEPTIONS,
            memory64: MEMORY64,
        }
    }
}

#[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
impl From<WasmFeatures> for near_vm_types::Features {
    fn from(f: crate::features::WasmFeatures) -> Self {
        Self {
            mutable_global: true,
            sign_extension: f.sign_extension,

            threads: THREADS,
            reference_types: f.reference_types,
            simd: SIMD,
            bulk_memory: f.reference_types,
            multi_value: f.multi_value,
            tail_call: TAIL_CALL,
            multi_memory: MULTI_MEMORY,
            memory64: MEMORY64,
            exceptions: EXCEPTIONS,
            saturating_float_to_int: SATURATING_FLOAT_TO_INT,
        }
    }
}

#[cfg(all(feature = "wasmer2_vm", target_arch = "x86_64"))]
impl From<WasmFeatures> for wasmer_types::Features {
    fn from(f: crate::features::WasmFeatures) -> Self {
        // /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\
        //
        // There are features that this version of wasmparser enables by default, but pwasm
        // currently does not and the compilers' support for these features is therefore largely
        // untested if it exists at all. Non exhaustive list of examples:
        //
        // * saturating_float_to_int
        // * sign_extension
        //
        // This is instead ensured by the fact that the V0 and V1 use pwasm utils in preparation
        // and it does not support these extensions.
        //
        // /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\ /!\
        Self {
            module_linking: false, // old version of component model
            threads: THREADS,
            reference_types: f.reference_types,
            simd: SIMD,
            bulk_memory: f.reference_types,
            multi_value: f.multi_value,
            tail_call: TAIL_CALL,
            multi_memory: MULTI_MEMORY,
            memory64: MEMORY64,
            exceptions: EXCEPTIONS,
        }
    }
}

#[cfg(feature = "wasmtime_vm")]
impl From<WasmFeatures> for wasmtime::Config {
    fn from(f: WasmFeatures) -> Self {
        let mut config = wasmtime::Config::default();
        config.wasm_threads(THREADS);
        config.wasm_reference_types(f.reference_types);
        config.wasm_simd(SIMD);
        config.wasm_bulk_memory(f.reference_types);
        config.wasm_multi_value(f.multi_value);
        config.wasm_multi_memory(MULTI_MEMORY);
        config.wasm_memory64(MEMORY64);
        config
    }
}
