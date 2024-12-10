#[allow(dead_code)]
mod opts {
    pub(super) const REFERENCE_TYPES: bool = false;
    pub(super) const MULTI_VALUE: bool = false;
    pub(super) const BULK_MEMORY: bool = false;
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
}

impl From<crate::logic::ContractPrepareVersion> for WasmFeatures {
    fn from(version: crate::logic::ContractPrepareVersion) -> Self {
        let sign_extension = match version {
            crate::logic::ContractPrepareVersion::V0 => false,
            crate::logic::ContractPrepareVersion::V1 => false,
            crate::logic::ContractPrepareVersion::V2 => true,
        };
        WasmFeatures { sign_extension }
    }
}

#[cfg(feature = "finite-wasm")]
impl From<WasmFeatures> for finite_wasm::wasmparser::WasmFeatures {
    fn from(f: WasmFeatures) -> Self {
        finite_wasm::wasmparser::WasmFeatures {
            floats: true,
            mutable_global: true,
            sign_extension: f.sign_extension,

            reference_types: REFERENCE_TYPES,
            // wasmer singlepass compiler requires multi_value return values to be disabled.
            multi_value: MULTI_VALUE,
            bulk_memory: BULK_MEMORY,
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
    fn from(_: WasmFeatures) -> Self {
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
            reference_types: REFERENCE_TYPES,
            multi_value: MULTI_VALUE,
            bulk_memory: BULK_MEMORY,
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
            reference_types: REFERENCE_TYPES,
            simd: SIMD,
            bulk_memory: BULK_MEMORY,
            multi_value: MULTI_VALUE,
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
    fn from(_: crate::features::WasmFeatures) -> Self {
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
            reference_types: REFERENCE_TYPES,
            simd: SIMD,
            bulk_memory: BULK_MEMORY,
            multi_value: MULTI_VALUE,
            tail_call: TAIL_CALL,
            multi_memory: MULTI_MEMORY,
            memory64: MEMORY64,
            exceptions: EXCEPTIONS,
        }
    }
}

#[cfg(feature = "wasmtime_vm")]
impl From<WasmFeatures> for wasmtime::Config {
    fn from(_: WasmFeatures) -> Self {
        // preparation code did all the filtering necessary already. Default configuration supports
        // all the necessary features (and, yes, enables more of them.)
        wasmtime::Config::default()
    }
}
