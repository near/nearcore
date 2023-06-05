//! Universal compilation.

use crate::executable::{unrkyv, UniversalExecutableRef};
use crate::{CodeMemory, UniversalArtifact, UniversalExecutable};
#[cfg(feature = "compiler")]
use near_vm_compiler::Compiler;
use near_vm_compiler::{
    CompileError, CustomSectionProtection, CustomSectionRef, FunctionBodyRef, JumpTable,
    SectionIndex, Target,
};
use near_vm_engine::{Engine, EngineId};
use near_vm_types::entity::{EntityRef, PrimaryMap};
use near_vm_types::{
    DataInitializer, ExportIndex, Features, FunctionIndex, FunctionType, FunctionTypeRef,
    GlobalInit, GlobalType, ImportCounts, ImportIndex, LocalFunctionIndex, LocalGlobalIndex,
    MemoryIndex, SignatureIndex, TableIndex,
};
use near_vm_vm::{
    FuncDataRegistry, FunctionBodyPtr, SectionBodyPtr, SignatureRegistry, Tunables,
    VMCallerCheckedAnyfunc, VMFuncRef, VMFunctionBody, VMImportType, VMLocalFunction, VMOffsets,
    VMSharedSignatureIndex, VMTrampoline,
};
use rkyv::de::deserializers::SharedDeserializeMap;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

/// A WebAssembly `Universal` Engine.
#[derive(Clone)]
pub struct UniversalEngine {
    inner: Arc<Mutex<UniversalEngineInner>>,
    /// The target for the compiler
    target: Arc<Target>,
    engine_id: EngineId,
}

impl UniversalEngine {
    /// Create a new `UniversalEngine` with the given config
    #[cfg(feature = "compiler")]
    pub fn new(compiler: Box<dyn Compiler>, target: Target, features: Features) -> Self {
        Self {
            inner: Arc::new(Mutex::new(UniversalEngineInner {
                compiler: Some(compiler),
                code_memory: vec![],
                signatures: SignatureRegistry::new(),
                func_data: Arc::new(FuncDataRegistry::new()),
                features,
            })),
            target: Arc::new(target),
            engine_id: EngineId::default(),
        }
    }

    /// Create a headless `UniversalEngine`
    ///
    /// A headless engine is an engine without any compiler attached.
    /// This is useful for assuring a minimal runtime for running
    /// WebAssembly modules.
    ///
    /// For example, for running in IoT devices where compilers are very
    /// expensive, or also to optimize startup speed.
    ///
    /// # Important
    ///
    /// Headless engines can't compile or validate any modules,
    /// they just take already processed Modules (via `Module::serialize`).
    pub fn headless() -> Self {
        Self {
            inner: Arc::new(Mutex::new(UniversalEngineInner {
                #[cfg(feature = "compiler")]
                compiler: None,
                code_memory: vec![],
                signatures: SignatureRegistry::new(),
                func_data: Arc::new(FuncDataRegistry::new()),
                features: Features::default(),
            })),
            target: Arc::new(Target::default()),
            engine_id: EngineId::default(),
        }
    }

    pub(crate) fn inner(&self) -> std::sync::MutexGuard<'_, UniversalEngineInner> {
        self.inner.lock().unwrap()
    }

    pub(crate) fn inner_mut(&self) -> std::sync::MutexGuard<'_, UniversalEngineInner> {
        self.inner.lock().unwrap()
    }

    /// Compile a WebAssembly binary
    #[cfg(feature = "compiler")]
    #[tracing::instrument(skip_all)]
    pub fn compile_universal(
        &self,
        binary: &[u8],
        tunables: &dyn Tunables,
    ) -> Result<crate::UniversalExecutable, CompileError> {
        // Compute the needed instrumentation
        let instrumentation = finite_wasm::Analysis::new()
            .with_stack(tunables.stack_limiter_cfg())
            .with_gas(tunables.gas_cfg())
            .analyze(binary)
            .map_err(CompileError::Analyze)?;

        let inner_engine = self.inner_mut();
        let features = inner_engine.features();
        let compiler = inner_engine.compiler()?;
        let environ = near_vm_compiler::ModuleEnvironment::new();
        let translation = environ.translate(binary).map_err(CompileError::Wasm)?;

        let memory_styles: PrimaryMap<near_vm_types::MemoryIndex, _> = translation
            .module
            .memories
            .values()
            .map(|memory_type| tunables.memory_style(memory_type))
            .collect();
        let table_styles: PrimaryMap<near_vm_types::TableIndex, _> = translation
            .module
            .tables
            .values()
            .map(|table_type| tunables.table_style(table_type))
            .collect();

        // Compile the Module
        let compile_info = near_vm_compiler::CompileModuleInfo {
            module: Arc::new(translation.module),
            features: features.clone(),
            memory_styles,
            table_styles,
        };
        let near_vm_compiler::Compilation {
            functions,
            custom_sections,
            function_call_trampolines,
            dynamic_function_trampolines,
            debug,
            trampolines,
        } = compiler.compile_module(
            &self.target(),
            &compile_info,
            translation.function_body_inputs,
            tunables,
            &instrumentation,
        )?;
        let data_initializers = translation
            .data_initializers
            .iter()
            .map(near_vm_types::OwnedDataInitializer::new)
            .collect();
        let mut function_frame_info = PrimaryMap::with_capacity(functions.len());
        let mut function_bodies = PrimaryMap::with_capacity(functions.len());
        let mut function_relocations = PrimaryMap::with_capacity(functions.len());
        let mut function_jt_offsets = PrimaryMap::with_capacity(functions.len());
        for (_, func) in functions.into_iter() {
            function_bodies.push(func.body);
            function_relocations.push(func.relocations);
            function_jt_offsets.push(func.jt_offsets);
            function_frame_info.push(func.frame_info);
        }
        let custom_section_relocations = custom_sections
            .iter()
            .map(|(_, section)| section.relocations.clone())
            .collect::<PrimaryMap<SectionIndex, _>>();
        Ok(crate::UniversalExecutable {
            function_bodies,
            function_relocations,
            function_jt_offsets,
            function_frame_info,
            function_call_trampolines,
            dynamic_function_trampolines,
            custom_sections,
            custom_section_relocations,
            debug,
            trampolines,
            compile_info,
            data_initializers,
            cpu_features: self.target().cpu_features().as_u64(),
        })
    }

    /// Load a [`UniversalExecutable`](crate::UniversalExecutable) with this engine.
    #[tracing::instrument(skip_all)]
    pub fn load_universal_executable(
        &self,
        executable: &UniversalExecutable,
    ) -> Result<UniversalArtifact, CompileError> {
        let info = &executable.compile_info;
        let module = &info.module;
        let local_memories = (module.import_counts.memories as usize..module.memories.len())
            .map(|idx| {
                let idx = MemoryIndex::new(idx);
                (module.memories[idx], info.memory_styles[idx].clone())
            })
            .collect();
        let local_tables = (module.import_counts.tables as usize..module.tables.len())
            .map(|idx| {
                let idx = TableIndex::new(idx);
                (module.tables[idx], info.table_styles[idx].clone())
            })
            .collect();
        let local_globals: Vec<(GlobalType, GlobalInit)> = module
            .globals
            .iter()
            .skip(module.import_counts.globals as usize)
            .enumerate()
            .map(|(idx, (_, t))| {
                let init = module.global_initializers[LocalGlobalIndex::new(idx)];
                (*t, init)
            })
            .collect();
        let mut inner_engine = self.inner_mut();

        let local_functions = executable.function_bodies.iter().map(|(_, b)| b.into());
        let function_call_trampolines = &executable.function_call_trampolines;
        let dynamic_function_trampolines = &executable.dynamic_function_trampolines;
        let signatures = module
            .signatures
            .iter()
            .map(|(_, sig)| inner_engine.signatures.register(sig.clone()))
            .collect::<PrimaryMap<SignatureIndex, _>>()
            .into_boxed_slice();
        let (functions, trampolines, dynamic_trampolines, custom_sections) = inner_engine
            .allocate(
                local_functions,
                function_call_trampolines.iter().map(|(_, b)| b.into()),
                dynamic_function_trampolines.iter().map(|(_, b)| b.into()),
                executable.custom_sections.iter().map(|(_, s)| s.into()),
                |idx: LocalFunctionIndex| {
                    let func_idx = module.import_counts.function_index(idx);
                    let sig_idx = module.functions[func_idx];
                    (sig_idx, signatures[sig_idx])
                },
            )?;
        let imports = module
            .imports
            .iter()
            .map(|((module_name, field, idx), entity)| near_vm_vm::VMImport {
                module: String::from(module_name),
                field: String::from(field),
                import_no: *idx,
                ty: match entity {
                    ImportIndex::Function(i) => {
                        let sig_idx = module.functions[*i];
                        VMImportType::Function {
                            sig: signatures[sig_idx],
                            static_trampoline: trampolines[sig_idx],
                        }
                    }
                    ImportIndex::Table(i) => VMImportType::Table(module.tables[*i]),
                    &ImportIndex::Memory(i) => {
                        let ty = module.memories[i];
                        VMImportType::Memory(ty, info.memory_styles[i].clone())
                    }
                    ImportIndex::Global(i) => VMImportType::Global(module.globals[*i]),
                },
            })
            .collect();

        let function_relocations = executable.function_relocations.iter();
        let section_relocations = executable.custom_section_relocations.iter();
        crate::link_module(
            &functions,
            |func_idx, jt_idx| executable.function_jt_offsets[func_idx][jt_idx],
            function_relocations.map(|(i, rs)| (i, rs.iter().cloned())),
            &custom_sections,
            section_relocations.map(|(i, rs)| (i, rs.iter().cloned())),
            &executable.trampolines,
        );

        // Make all code loaded executable.
        inner_engine.publish_compiled_code();
        if let Some(ref d) = executable.debug {
            unsafe {
                // TODO: safety comment
                inner_engine.publish_eh_frame(std::slice::from_raw_parts(
                    *custom_sections[d.eh_frame],
                    executable.custom_sections[d.eh_frame].bytes.len(),
                ))?;
            }
        }
        let exports = module
            .exports
            .iter()
            .map(|(s, i)| (s.clone(), i.clone()))
            .collect::<BTreeMap<String, ExportIndex>>();

        Ok(UniversalArtifact {
            engine: self.clone(),
            import_counts: module.import_counts,
            start_function: module.start_function,
            vmoffsets: VMOffsets::for_host().with_module_info(&*module),
            imports,
            dynamic_function_trampolines: dynamic_trampolines.into_boxed_slice(),
            functions: functions.into_boxed_slice(),
            exports,
            signatures,
            local_memories,
            data_segments: executable.data_initializers.clone(),
            passive_data: module.passive_data.clone(),
            local_tables,
            element_segments: module.table_initializers.clone(),
            passive_elements: module.passive_elements.clone(),
            local_globals,
        })
    }

    /// Load a [`UniversalExecutableRef`](crate::UniversalExecutableRef) with this engine.
    pub fn load_universal_executable_ref(
        &self,
        executable: &UniversalExecutableRef,
    ) -> Result<UniversalArtifact, CompileError> {
        let info = &executable.compile_info;
        let module = &info.module;
        let import_counts: ImportCounts = unrkyv(&module.import_counts);
        let local_memories = (import_counts.memories as usize..module.memories.len())
            .map(|idx| {
                let idx = MemoryIndex::new(idx);
                let mty = &module.memories[&idx];
                (unrkyv(mty), unrkyv(&info.memory_styles[&idx]))
            })
            .collect();
        let local_tables = (import_counts.tables as usize..module.tables.len())
            .map(|idx| {
                let idx = TableIndex::new(idx);
                let tty = &module.tables[&idx];
                (unrkyv(tty), unrkyv(&info.table_styles[&idx]))
            })
            .collect();
        let local_globals: Vec<(GlobalType, GlobalInit)> = module
            .globals
            .iter()
            .skip(import_counts.globals as _)
            .enumerate()
            .map(|(idx, (_, t))| {
                let init = unrkyv(&module.global_initializers[&LocalGlobalIndex::new(idx)]);
                (*t, init)
            })
            .collect();

        let passive_data =
            rkyv::Deserialize::deserialize(&module.passive_data, &mut SharedDeserializeMap::new())
                .map_err(|_| CompileError::Validate("could not deserialize passive data".into()))?;
        let data_segments = executable.data_initializers.iter();
        let data_segments = data_segments.map(|s| DataInitializer::from(s).into()).collect();
        let element_segments = unrkyv(&module.table_initializers);
        let passive_elements: BTreeMap<near_vm_types::ElemIndex, Box<[FunctionIndex]>> =
            unrkyv(&module.passive_elements);

        let import_counts: ImportCounts = unrkyv(&module.import_counts);
        let mut inner_engine = self.inner_mut();

        let local_functions = executable.function_bodies.iter().map(|(_, b)| b.into());
        let call_trampolines = executable.function_call_trampolines.iter();
        let dynamic_trampolines = executable.dynamic_function_trampolines.iter();
        let signatures = module
            .signatures
            .values()
            .map(|sig| {
                let sig_ref = FunctionTypeRef::from(sig);
                inner_engine
                    .signatures
                    .register(FunctionType::new(sig_ref.params(), sig_ref.results()))
            })
            .collect::<PrimaryMap<SignatureIndex, _>>()
            .into_boxed_slice();
        let (functions, trampolines, dynamic_trampolines, custom_sections) = inner_engine
            .allocate(
                local_functions,
                call_trampolines.map(|(_, b)| b.into()),
                dynamic_trampolines.map(|(_, b)| b.into()),
                executable.custom_sections.iter().map(|(_, s)| s.into()),
                |idx: LocalFunctionIndex| {
                    let func_idx = import_counts.function_index(idx);
                    let sig_idx = module.functions[&func_idx];
                    (sig_idx, signatures[sig_idx])
                },
            )?;
        let imports = {
            module
                .imports
                .iter()
                .map(|((module_name, field, idx), entity)| near_vm_vm::VMImport {
                    module: String::from(module_name.as_str()),
                    field: String::from(field.as_str()),
                    import_no: *idx,
                    ty: match entity {
                        ImportIndex::Function(i) => {
                            let sig_idx = module.functions[i];
                            VMImportType::Function {
                                sig: signatures[sig_idx],
                                static_trampoline: trampolines[sig_idx],
                            }
                        }
                        ImportIndex::Table(i) => VMImportType::Table(unrkyv(&module.tables[i])),
                        ImportIndex::Memory(i) => {
                            let ty = unrkyv(&module.memories[i]);
                            VMImportType::Memory(ty, unrkyv(&info.memory_styles[i]))
                        }
                        ImportIndex::Global(i) => VMImportType::Global(unrkyv(&module.globals[i])),
                    },
                })
                .collect()
        };

        let function_relocations = executable.function_relocations.iter();
        let section_relocations = executable.custom_section_relocations.iter();
        crate::link_module(
            &functions,
            |func_idx, jt_idx| {
                let func_idx = rkyv::Archived::<LocalFunctionIndex>::new(func_idx.index());
                let jt_idx = rkyv::Archived::<JumpTable>::new(jt_idx.index());
                executable.function_jt_offsets[&func_idx][&jt_idx]
            },
            function_relocations.map(|(i, r)| (i, r.iter().map(unrkyv))),
            &custom_sections,
            section_relocations.map(|(i, r)| (i, r.iter().map(unrkyv))),
            &unrkyv(&executable.trampolines),
        );

        // Make all code compiled thus far executable.
        inner_engine.publish_compiled_code();
        if let rkyv::option::ArchivedOption::Some(ref d) = executable.debug {
            unsafe {
                // TODO: safety comment
                let s = CustomSectionRef::from(&executable.custom_sections[&d.eh_frame]);
                inner_engine.publish_eh_frame(std::slice::from_raw_parts(
                    *custom_sections[unrkyv(&d.eh_frame)],
                    s.bytes.len(),
                ))?;
            }
        }
        let exports = module
            .exports
            .iter()
            .map(|(s, i)| (unrkyv(s), unrkyv(i)))
            .collect::<BTreeMap<String, ExportIndex>>();
        Ok(UniversalArtifact {
            engine: self.clone(),
            import_counts,
            start_function: unrkyv(&module.start_function),
            vmoffsets: VMOffsets::for_host().with_archived_module_info(&*module),
            imports,
            dynamic_function_trampolines: dynamic_trampolines.into_boxed_slice(),
            functions: functions.into_boxed_slice(),
            exports,
            signatures,
            local_memories,
            data_segments,
            passive_data,
            local_tables,
            element_segments,
            passive_elements,
            local_globals,
        })
    }
}

impl Engine for UniversalEngine {
    /// The target
    fn target(&self) -> &Target {
        &self.target
    }

    /// Register a signature
    fn register_signature(&self, func_type: FunctionType) -> VMSharedSignatureIndex {
        self.inner().signatures.register(func_type)
    }

    fn register_function_metadata(&self, func_data: VMCallerCheckedAnyfunc) -> VMFuncRef {
        self.inner().func_data().register(func_data)
    }

    /// Lookup a signature
    fn lookup_signature(&self, sig: VMSharedSignatureIndex) -> Option<FunctionType> {
        self.inner().signatures.lookup(sig).cloned()
    }

    /// Validates a WebAssembly module
    #[tracing::instrument(skip_all)]
    fn validate(&self, binary: &[u8]) -> Result<(), CompileError> {
        self.inner().validate(binary)
    }

    #[cfg(not(feature = "compiler"))]
    fn compile(
        &self,
        binary: &[u8],
        tunables: &dyn Tunables,
    ) -> Result<Box<dyn near_vm_engine::Executable>, CompileError> {
        return Err(CompileError::Codegen(
            "The UniversalEngine is operating in headless mode, so it can not compile Modules."
                .to_string(),
        ));
    }

    /// Compile a WebAssembly binary
    #[cfg(feature = "compiler")]
    #[tracing::instrument(skip_all)]
    fn compile(
        &self,
        binary: &[u8],
        tunables: &dyn Tunables,
    ) -> Result<Box<dyn near_vm_engine::Executable>, CompileError> {
        self.compile_universal(binary, tunables).map(|ex| Box::new(ex) as _)
    }

    #[tracing::instrument(skip_all)]
    fn load(
        &self,
        executable: &(dyn near_vm_engine::Executable),
    ) -> Result<Arc<dyn near_vm_vm::Artifact>, CompileError> {
        executable.load(self)
    }

    fn id(&self) -> &EngineId {
        &self.engine_id
    }

    fn cloned(&self) -> Arc<dyn Engine + Send + Sync> {
        Arc::new(self.clone())
    }
}

/// The inner contents of `UniversalEngine`
pub struct UniversalEngineInner {
    /// The compiler
    #[cfg(feature = "compiler")]
    compiler: Option<Box<dyn Compiler>>,
    /// The features to compile the Wasm module with
    features: Features,
    /// The code memory is responsible of publishing the compiled
    /// functions to memory.
    code_memory: Vec<CodeMemory>,
    /// The signature registry is used mainly to operate with trampolines
    /// performantly.
    pub(crate) signatures: SignatureRegistry,
    /// The backing storage of `VMFuncRef`s. This centralized store ensures that 2
    /// functions with the same `VMCallerCheckedAnyfunc` will have the same `VMFuncRef`.
    /// It also guarantees that the `VMFuncRef`s stay valid until the engine is dropped.
    func_data: Arc<FuncDataRegistry>,
}

impl UniversalEngineInner {
    /// Gets the compiler associated to this engine.
    #[cfg(feature = "compiler")]
    pub fn compiler(&self) -> Result<&dyn Compiler, CompileError> {
        if self.compiler.is_none() {
            return Err(CompileError::Codegen("The UniversalEngine is operating in headless mode, so it can only execute already compiled Modules.".to_string()));
        }
        Ok(&**self.compiler.as_ref().unwrap())
    }

    /// Validate the module
    #[cfg(feature = "compiler")]
    pub fn validate<'data>(&self, data: &'data [u8]) -> Result<(), CompileError> {
        self.compiler()?.validate_module(self.features(), data)
    }

    /// Validate the module
    #[cfg(not(feature = "compiler"))]
    pub fn validate<'data>(&self, _data: &'data [u8]) -> Result<(), CompileError> {
        Err(CompileError::Validate(
            "The UniversalEngine is not compiled with compiler support, which is required for validating"
                .to_string(),
        ))
    }

    /// The Wasm features
    pub fn features(&self) -> &Features {
        &self.features
    }

    /// Allocate compiled functions into memory
    #[allow(clippy::type_complexity)]
    pub(crate) fn allocate<'a>(
        &mut self,
        local_functions: impl ExactSizeIterator<Item = FunctionBodyRef<'a>>,
        call_trampolines: impl ExactSizeIterator<Item = FunctionBodyRef<'a>>,
        dynamic_trampolines: impl ExactSizeIterator<Item = FunctionBodyRef<'a>>,
        custom_sections: impl ExactSizeIterator<Item = CustomSectionRef<'a>>,
        function_signature: impl Fn(LocalFunctionIndex) -> (SignatureIndex, VMSharedSignatureIndex),
    ) -> Result<
        (
            PrimaryMap<LocalFunctionIndex, VMLocalFunction>,
            PrimaryMap<SignatureIndex, VMTrampoline>,
            PrimaryMap<FunctionIndex, FunctionBodyPtr>,
            PrimaryMap<SectionIndex, SectionBodyPtr>,
        ),
        CompileError,
    > {
        let code_memory = &mut self.code_memory;
        let function_count = local_functions.len();
        let call_trampoline_count = call_trampolines.len();
        let function_bodies =
            call_trampolines.chain(local_functions).chain(dynamic_trampolines).collect::<Vec<_>>();

        // TOOD: this shouldn't be necessary....
        let mut section_types = Vec::with_capacity(custom_sections.len());
        let mut executable_sections = Vec::new();
        let mut data_sections = Vec::new();
        for section in custom_sections {
            if let CustomSectionProtection::ReadExecute = section.protection {
                executable_sections.push(section);
            } else {
                data_sections.push(section);
            }
            section_types.push(section.protection);
        }
        code_memory.push(CodeMemory::new());
        let code_memory = self.code_memory.last_mut().expect("infallible");

        let (mut allocated_functions, allocated_executable_sections, allocated_data_sections) =
            code_memory
                .allocate(
                    function_bodies.as_slice(),
                    executable_sections.as_slice(),
                    data_sections.as_slice(),
                )
                .map_err(|message| {
                    CompileError::Resource(format!(
                        "failed to allocate memory for functions: {}",
                        message
                    ))
                })?;

        let mut allocated_function_call_trampolines: PrimaryMap<SignatureIndex, VMTrampoline> =
            PrimaryMap::new();
        for ptr in allocated_functions.drain(0..call_trampoline_count).map(|slice| slice.as_ptr()) {
            // TODO: What in damnation have you done?! – Bannon
            let trampoline =
                unsafe { std::mem::transmute::<*const VMFunctionBody, VMTrampoline>(ptr) };
            allocated_function_call_trampolines.push(trampoline);
        }

        let allocated_functions_result = allocated_functions
            .drain(0..function_count)
            .enumerate()
            .map(|(index, slice)| -> Result<_, CompileError> {
                let index = LocalFunctionIndex::new(index);
                let (sig_idx, sig) = function_signature(index);
                Ok(VMLocalFunction {
                    body: FunctionBodyPtr(slice.as_ptr()),
                    length: u32::try_from(slice.len()).map_err(|_| {
                        CompileError::Codegen("function body length exceeds 4GiB".into())
                    })?,
                    signature: sig,
                    trampoline: allocated_function_call_trampolines[sig_idx],
                })
            })
            .collect::<Result<PrimaryMap<LocalFunctionIndex, _>, _>>()?;

        let allocated_dynamic_function_trampolines = allocated_functions
            .drain(..)
            .map(|slice| FunctionBodyPtr(slice.as_ptr()))
            .collect::<PrimaryMap<FunctionIndex, _>>();

        let mut exec_iter = allocated_executable_sections.iter();
        let mut data_iter = allocated_data_sections.iter();
        let allocated_custom_sections = section_types
            .into_iter()
            .map(|protection| {
                SectionBodyPtr(
                    if protection == CustomSectionProtection::ReadExecute {
                        exec_iter.next()
                    } else {
                        data_iter.next()
                    }
                    .unwrap()
                    .as_ptr(),
                )
            })
            .collect::<PrimaryMap<SectionIndex, _>>();

        Ok((
            allocated_functions_result,
            allocated_function_call_trampolines,
            allocated_dynamic_function_trampolines,
            allocated_custom_sections,
        ))
    }

    /// Make memory containing compiled code executable.
    pub(crate) fn publish_compiled_code(&mut self) {
        self.code_memory.last_mut().unwrap().publish();
    }

    /// Register DWARF-type exception handling information associated with the code.
    pub(crate) fn publish_eh_frame(&mut self, eh_frame: &[u8]) -> Result<(), CompileError> {
        self.code_memory.last_mut().unwrap().unwind_registry_mut().publish(eh_frame).map_err(
            |e| CompileError::Resource(format!("Error while publishing the unwind code: {}", e)),
        )?;
        Ok(())
    }

    /// Shared func metadata registry.
    pub(crate) fn func_data(&self) -> &Arc<FuncDataRegistry> {
        &self.func_data
    }
}
