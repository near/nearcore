//! Support for compiling with Singlepass.
// Allow unused imports while developing.
#![allow(unused_imports, dead_code)]

use crate::codegen_x64::{
    gen_import_call_trampoline, gen_std_dynamic_import_trampoline, gen_std_trampoline,
    CodegenError, FuncGen,
};
use crate::config::Singlepass;
use near_vm_compiler::{
    Architecture, CallingConvention, Compilation, CompileError, CompileModuleInfo,
    CompiledFunction, Compiler, CompilerConfig, CpuFeature, FunctionBody, FunctionBodyData,
    ModuleTranslationState, OperatingSystem, SectionIndex, Target, TrapInformation,
};
use near_vm_types::entity::{EntityRef, PrimaryMap};
use near_vm_types::{
    FunctionIndex, FunctionType, LocalFunctionIndex, MemoryIndex, ModuleInfo, TableIndex,
};
use near_vm_vm::{TrapCode, VMOffsets};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::sync::Arc;

/// A compiler that compiles a WebAssembly module with Singlepass.
/// It does the compilation in one pass
pub struct SinglepassCompiler {
    config: Singlepass,
}

impl SinglepassCompiler {
    /// Creates a new Singlepass compiler
    pub fn new(config: Singlepass) -> Self {
        Self { config }
    }

    /// Gets the config for this Compiler
    fn config(&self) -> &Singlepass {
        &self.config
    }
}

impl Compiler for SinglepassCompiler {
    /// Compile the module using Singlepass, producing a compilation result with
    /// associated relocations.
    #[tracing::instrument(skip_all)]
    fn compile_module(
        &self,
        target: &Target,
        compile_info: &CompileModuleInfo,
        function_body_inputs: PrimaryMap<LocalFunctionIndex, FunctionBodyData<'_>>,
        tunables: &dyn near_vm_vm::Tunables,
        instrumentation: &finite_wasm::AnalysisOutcome,
    ) -> Result<Compilation, CompileError> {
        /*if target.triple().operating_system == OperatingSystem::Windows {
            return Err(CompileError::UnsupportedTarget(
                OperatingSystem::Windows.to_string(),
            ));
        }*/
        if target.triple().architecture != Architecture::X86_64 {
            return Err(CompileError::UnsupportedTarget(target.triple().architecture.to_string()));
        }
        if !target.cpu_features().contains(CpuFeature::AVX) {
            return Err(CompileError::UnsupportedTarget("x86_64 without AVX".to_string()));
        }
        if compile_info.features.multi_value {
            return Err(CompileError::UnsupportedFeature("multivalue".to_string()));
        }
        let calling_convention = match target.triple().default_calling_convention() {
            Ok(CallingConvention::WindowsFastcall) => CallingConvention::WindowsFastcall,
            Ok(CallingConvention::SystemV) => CallingConvention::SystemV,
            //Ok(CallingConvention::AppleAarch64) => AppleAarch64,
            _ => panic!("Unsupported Calling convention for Singlepass compiler"),
        };

        let table_styles = &compile_info.table_styles;
        let module = &compile_info.module;
        let pointer_width = target
            .triple()
            .pointer_width()
            .map_err(|()| {
                CompileError::UnsupportedTarget("target with unknown pointer width".into())
            })?
            .bytes();
        let vmoffsets = VMOffsets::new(pointer_width).with_module_info(&module);
        let make_assembler = || {
            const KB: usize = 1024;
            dynasmrt::VecAssembler::new_with_capacity(0, 128 * KB, 0, 0, KB, 0, KB)
        };
        let import_idxs = 0..module.import_counts.functions as usize;
        let import_trampolines: PrimaryMap<SectionIndex, _> =
            tracing::info_span!("import_trampolines", n_imports = import_idxs.len()).in_scope(
                || {
                    import_idxs
                        .into_par_iter()
                        .map_init(make_assembler, |assembler, i| {
                            let i = FunctionIndex::new(i);
                            gen_import_call_trampoline(
                                &vmoffsets,
                                i,
                                &module.signatures[module.functions[i]],
                                calling_convention,
                                assembler,
                            )
                        })
                        .collect::<Vec<_>>()
                        .into_iter()
                        .collect()
                },
            );
        let functions = function_body_inputs
            .iter()
            .collect::<Vec<(LocalFunctionIndex, &FunctionBodyData<'_>)>>()
            .into_par_iter()
            .map_init(make_assembler, |assembler, (i, input)| {
                tracing::info_span!("function", i = i.index()).in_scope(|| {
                    let reader =
                        near_vm_compiler::FunctionReader::new(input.module_offset, input.data);
                    let stack_init_gas_cost = tunables
                        .stack_init_gas_cost(instrumentation.function_frame_sizes[i.index()]);
                    let stack_size = instrumentation.function_frame_sizes[i.index()]
                        .checked_add(instrumentation.function_operand_stack_sizes[i.index()])
                        .ok_or_else(|| {
                            CompileError::Codegen(String::from(
                                "got function with frame size going beyond u64::MAX",
                            ))
                        })?;
                    let mut generator = FuncGen::new(
                        assembler,
                        module,
                        &self.config,
                        &target,
                        &vmoffsets,
                        &table_styles,
                        i,
                        calling_convention,
                        stack_init_gas_cost,
                        &instrumentation.gas_offsets[i.index()],
                        &instrumentation.gas_costs[i.index()],
                        &instrumentation.gas_kinds[i.index()],
                        stack_size,
                    )
                    .map_err(to_compile_error)?;

                    let mut local_reader = reader.get_locals_reader()?;
                    for _ in 0..local_reader.get_count() {
                        let (count, ty) = local_reader.read()?;
                        // Overflows feeding a local here have most likely already been caught by the
                        // validator, but it is possible that the validator hasn't been run at all, or
                        // that the validator does not impose any limits on the number of locals.
                        generator.feed_local(count, ty);
                    }

                    generator.emit_head().map_err(to_compile_error)?;

                    let mut operator_reader =
                        reader.get_operators_reader()?.into_iter_with_offsets();
                    while generator.has_control_frames() {
                        let (op, pos) = tracing::info_span!("parsing-next-operator")
                            .in_scope(|| operator_reader.next().unwrap())?;
                        generator.set_srcloc(pos as u32);
                        generator.feed_operator(op).map_err(to_compile_error)?;
                    }

                    Ok(generator.finalize(&input))
                })
            })
            .collect::<Result<Vec<CompiledFunction>, CompileError>>()?
            .into_iter() // TODO: why not just collect to PrimaryMap directly?
            .collect::<PrimaryMap<LocalFunctionIndex, CompiledFunction>>();

        let function_call_trampolines =
            tracing::info_span!("function_call_trampolines").in_scope(|| {
                module
                    .signatures
                    .values()
                    .collect::<Vec<_>>()
                    .into_par_iter()
                    .map_init(make_assembler, |assembler, func_type| {
                        gen_std_trampoline(&func_type, calling_convention, assembler)
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .collect::<PrimaryMap<_, _>>()
            });

        let dynamic_function_trampolines = tracing::info_span!("dynamic_function_trampolines")
            .in_scope(|| {
                module
                    .imported_function_types()
                    .collect::<Vec<_>>()
                    .into_par_iter()
                    .map_init(make_assembler, |assembler, func_type| {
                        gen_std_dynamic_import_trampoline(
                            &vmoffsets,
                            &func_type,
                            calling_convention,
                            assembler,
                        )
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .collect::<PrimaryMap<FunctionIndex, FunctionBody>>()
            });

        Ok(Compilation {
            functions,
            custom_sections: import_trampolines,
            function_call_trampolines,
            dynamic_function_trampolines,
            debug: None,
            trampolines: None,
        })
    }
}

trait ToCompileError {
    fn to_compile_error(self) -> CompileError;
}

impl ToCompileError for CodegenError {
    fn to_compile_error(self) -> CompileError {
        CompileError::Codegen(self.message)
    }
}

fn to_compile_error<T: ToCompileError>(x: T) -> CompileError {
    x.to_compile_error()
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_vm_compiler::{CpuFeature, Features, Triple};
    use near_vm_vm::{MemoryStyle, TableStyle};
    use std::str::FromStr;
    use target_lexicon::triple;

    fn dummy_compilation_ingredients<'a>() -> (
        CompileModuleInfo,
        PrimaryMap<LocalFunctionIndex, FunctionBodyData<'a>>,
        finite_wasm::AnalysisOutcome,
    ) {
        let compile_info = CompileModuleInfo {
            features: Features::new(),
            module: Arc::new(ModuleInfo::new()),
            memory_styles: PrimaryMap::<MemoryIndex, MemoryStyle>::new(),
            table_styles: PrimaryMap::<TableIndex, TableStyle>::new(),
        };
        let function_body_inputs = PrimaryMap::<LocalFunctionIndex, FunctionBodyData<'_>>::new();
        let analysis = finite_wasm::AnalysisOutcome {
            function_frame_sizes: Vec::new(),
            function_operand_stack_sizes: Vec::new(),
            gas_offsets: Vec::new(),
            gas_costs: Vec::new(),
            gas_kinds: Vec::new(),
        };
        (compile_info, function_body_inputs, analysis)
    }

    #[test]
    fn errors_for_unsupported_targets() {
        let compiler = SinglepassCompiler::new(Singlepass::default());

        // Compile for 32bit Linux
        let linux32 = Target::new(triple!("i686-unknown-linux-gnu"), CpuFeature::for_host());
        let (mut info, inputs, analysis) = dummy_compilation_ingredients();
        let result = compiler.compile_module(
            &linux32,
            &mut info,
            inputs,
            &near_vm_vm::TestTunables,
            &analysis,
        );
        match result.unwrap_err() {
            CompileError::UnsupportedTarget(name) => assert_eq!(name, "i686"),
            error => panic!("Unexpected error: {:?}", error),
        };

        // Compile for win32
        let win32 = Target::new(triple!("i686-pc-windows-gnu"), CpuFeature::for_host());
        let (mut info, inputs, analysis) = dummy_compilation_ingredients();
        let result = compiler.compile_module(
            &win32,
            &mut info,
            inputs,
            &near_vm_vm::TestTunables,
            &analysis,
        );
        match result.unwrap_err() {
            CompileError::UnsupportedTarget(name) => assert_eq!(name, "i686"), // Windows should be checked before architecture
            error => panic!("Unexpected error: {:?}", error),
        };
    }
}
