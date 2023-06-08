#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct WasmFeatures {
    sign_extension: bool,
}

impl From<near_vm_logic::ContractPrepareVersion> for WasmFeatures {
    fn from(version: near_vm_logic::ContractPrepareVersion) -> Self {
        let sign_extension = match version {
            near_vm_logic::ContractPrepareVersion::V0 => false,
            near_vm_logic::ContractPrepareVersion::V1 => false,
            near_vm_logic::ContractPrepareVersion::V2 => true,
        };
        WasmFeatures { sign_extension }
    }
}

impl From<WasmFeatures> for finite_wasm::wasmparser::WasmFeatures {
    fn from(f: WasmFeatures) -> Self {
        finite_wasm::wasmparser::WasmFeatures {
            floats: true,
            mutable_global: true,
            sign_extension: f.sign_extension,

            reference_types: false,
            // wasmer singlepass compiler requires multi_value return values to be disabled.
            multi_value: false,
            bulk_memory: false,
            simd: false,
            threads: false,
            tail_call: false,
            multi_memory: false,
            exceptions: false,
            memory64: false,
            saturating_float_to_int: false,
            relaxed_simd: false,
            extended_const: false,
            component_model: false,
            function_references: false,
            memory_control: false,
            gc: false,
        }
    }
}

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
            reference_types: false,
            multi_value: false,
            bulk_memory: false,
            module_linking: false,
            simd: false,
            threads: false,
            tail_call: false,
            deterministic_only: false,
            multi_memory: false,
            exceptions: false,
            memory64: false,
        }
    }
}

#[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
impl From<WasmFeatures> for near_vm_types::Features {
    fn from(f: crate::features::WasmFeatures) -> Self {
        Self {
            mutable_global: true,
            sign_extension: f.sign_extension,

            threads: false,
            reference_types: false,
            simd: false,
            bulk_memory: false,
            multi_value: false,
            tail_call: false,
            multi_memory: false,
            memory64: false,
            exceptions: false,
            saturating_float_to_int: false,
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
            threads: false,
            reference_types: false,
            simd: false,
            bulk_memory: false,
            multi_value: false,
            tail_call: false,
            multi_memory: false,
            memory64: false,
            exceptions: false,
            module_linking: false,
        }
    }
}

#[cfg(feature = "wasmtime_vm")]
impl From<WasmFeatures> for wasmtime::Config {
    fn from(value: WasmFeatures) -> Self {
        let mut config = wasmtime::Config::default();
        config.wasm_threads(false);
        config.wasm_reference_types(false);
        config.wasm_simd(false);
        config.wasm_bulk_memory(false);
        config.wasm_multi_value(false);
        config.wasm_multi_memory(false);
        config.wasm_memory64(false);
        config
    }
}
