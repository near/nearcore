//! This module mainly outputs the `Compiler` trait that custom
//! compilers will need to implement.

use crate::error::CompileError;
use crate::function::Compilation;
use crate::lib::std::boxed::Box;
use crate::module::CompileModuleInfo;
use crate::target::Target;
use crate::FunctionBodyData;
use crate::ModuleTranslationState;
use crate::SectionIndex;
use near_vm_types::entity::PrimaryMap;
use near_vm_types::{Features, FunctionIndex, LocalFunctionIndex, SignatureIndex};
use wasmparser::{Validator, WasmFeatures};

/// The compiler configuration options.
pub trait CompilerConfig {
    /// Enable Position Independent Code (PIC).
    ///
    /// This is required for shared object generation (Native Engine),
    /// but will make the JIT Engine to fail, since PIC is not yet
    /// supported in the JIT linking phase.
    fn enable_pic(&mut self) {
        // By default we do nothing, each backend will need to customize this
        // in case they do something special for emitting PIC code.
    }

    /// Enable compiler IR verification.
    ///
    /// For compilers capable of doing so, this enables internal consistency
    /// checking.
    fn enable_verifier(&mut self) {
        // By default we do nothing, each backend will need to customize this
        // in case they create an IR that they can verify.
    }

    /// Enable NaN canonicalization.
    ///
    /// NaN canonicalization is useful when trying to run WebAssembly
    /// deterministically across different architectures.
    #[deprecated(note = "Please use the canonicalize_nans instead")]
    fn enable_nan_canonicalization(&mut self) {
        // By default we do nothing, each backend will need to customize this
        // in case they create an IR that they can verify.
    }

    /// Enable NaN canonicalization.
    ///
    /// NaN canonicalization is useful when trying to run WebAssembly
    /// deterministically across different architectures.
    fn canonicalize_nans(&mut self, _enable: bool) {
        // By default we do nothing, each backend will need to customize this
        // in case they create an IR that they can verify.
    }

    /// Gets the custom compiler config
    fn compiler(self: Box<Self>) -> Box<dyn Compiler>;

    /// Gets the default features for this compiler in the given target
    fn default_features_for_target(&self, _target: &Target) -> Features {
        Features::default()
    }
}

impl<T> From<T> for Box<dyn CompilerConfig + 'static>
where
    T: CompilerConfig + 'static,
{
    fn from(other: T) -> Self {
        Box::new(other)
    }
}

/// An implementation of a Compiler from parsed WebAssembly module to Compiled native code.
pub trait Compiler: Send {
    /// Validates a module.
    ///
    /// It returns the a succesful Result in case is valid, `CompileError` in case is not.
    fn validate_module<'data>(
        &self,
        features: &Features,
        data: &'data [u8],
    ) -> Result<(), CompileError> {
        let wasm_features = WasmFeatures {
            bulk_memory: features.bulk_memory,
            threads: features.threads,
            reference_types: features.reference_types,
            multi_value: features.multi_value,
            simd: features.simd,
            tail_call: features.tail_call,
            multi_memory: features.multi_memory,
            memory64: features.memory64,
            exceptions: features.exceptions,
            floats: true,
            component_model: false,
            extended_const: false,
            mutable_global: features.mutable_global,
            relaxed_simd: false,
            saturating_float_to_int: features.saturating_float_to_int,
            sign_extension: features.sign_extension,
            memory_control: false,
        };
        let mut validator = Validator::new_with_features(wasm_features);
        validator.validate_all(data).map_err(|e| CompileError::Validate(format!("{}", e)))?;
        Ok(())
    }

    /// Compiles a parsed module.
    ///
    /// It returns the [`Compilation`] or a [`CompileError`].
    fn compile_module<'data, 'module>(
        &self,
        target: &Target,
        module: &'module CompileModuleInfo,
        // The list of function bodies
        function_body_inputs: PrimaryMap<LocalFunctionIndex, FunctionBodyData<'data>>,
        tunables: &dyn near_vm_vm::Tunables,
        instrumentation: &finite_wasm::AnalysisOutcome,
    ) -> Result<Compilation, CompileError>;

    /// Compiles a module into a native object file.
    ///
    /// It returns the bytes as a `&[u8]` or a [`CompileError`].
    fn experimental_native_compile_module<'data, 'module>(
        &self,
        _target: &Target,
        _module: &'module CompileModuleInfo,
        _module_translation: &ModuleTranslationState,
        // The list of function bodies
        _function_body_inputs: &PrimaryMap<LocalFunctionIndex, FunctionBodyData<'data>>,
        _symbol_registry: &dyn SymbolRegistry,
        // The metadata to inject into the near_vm_metadata section of the object file.
        _near_vm_metadata: &[u8],
    ) -> Option<Result<Vec<u8>, CompileError>> {
        None
    }
}

/// The kinds of near_vm_types objects that might be found in a native object file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Symbol {
    /// A function defined in the wasm.
    LocalFunction(LocalFunctionIndex),

    /// A wasm section.
    Section(SectionIndex),

    /// The function call trampoline for a given signature.
    FunctionCallTrampoline(SignatureIndex),

    /// The dynamic function trampoline for a given function.
    DynamicFunctionTrampoline(FunctionIndex),
}

/// This trait facilitates symbol name lookups in a native object file.
pub trait SymbolRegistry: Send + Sync {
    /// Given a `Symbol` it returns the name for that symbol in the object file
    fn symbol_to_name(&self, symbol: Symbol) -> String;

    /// Given a name it returns the `Symbol` for that name in the object file
    ///
    /// This function is the inverse of [`SymbolRegistry::symbol_to_name`]
    fn name_to_symbol(&self, name: &str) -> Option<Symbol>;
}
