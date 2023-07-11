//! Universal compilation.

use super::code_memory::{ARCH_FUNCTION_ALIGNMENT, DATA_SECTION_ALIGNMENT};
use super::executable::{unrkyv, UniversalExecutableRef};
use super::{CodeMemory, UniversalArtifact, UniversalExecutable};
use crate::EngineId;
use near_vm_compiler::Compiler;
use near_vm_compiler::{
    CompileError, CustomSectionProtection, CustomSectionRef, FunctionBodyRef, JumpTable,
    SectionIndex, Target,
};
use near_vm_types::entity::{EntityRef, PrimaryMap};
use near_vm_types::{
    DataInitializer, ExportIndex, Features, FunctionIndex, FunctionType, FunctionTypeRef,
    GlobalInit, GlobalType, ImportCounts, ImportIndex, LocalFunctionIndex, LocalGlobalIndex,
    MemoryIndex, SignatureIndex, TableIndex,
};
use near_vm_vm::{
    FuncDataRegistry, FunctionBodyPtr, SectionBodyPtr, SignatureRegistry, Tunables,
    VMCallerCheckedAnyfunc, VMFuncRef, VMImportType, VMLocalFunction, VMOffsets,
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
    pub fn new(
        compiler: Box<dyn Compiler>,
        target: Target,
        features: Features,
        memory_allocator: super::LimitedMemoryPool,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(UniversalEngineInner {
                compiler: Some(compiler),
                code_memory_pool: memory_allocator,
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
    pub fn headless(memory_allocator: super::LimitedMemoryPool) -> Self {
        Self {
            inner: Arc::new(Mutex::new(UniversalEngineInner {
                compiler: None,
                code_memory_pool: memory_allocator,
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
    #[tracing::instrument(skip_all)]
    pub fn compile_universal(
        &self,
        binary: &[u8],
        tunables: &dyn Tunables,
    ) -> Result<super::UniversalExecutable, CompileError> {
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
        Ok(super::UniversalExecutable {
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
        let (functions, trampolines, dynamic_trampolines, custom_sections, mut code_memory) =
            inner_engine.allocate(
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
        crate::universal::link_module(
            &functions,
            |func_idx, jt_idx| executable.function_jt_offsets[func_idx][jt_idx],
            function_relocations.map(|(i, rs)| (i, rs.iter().cloned())),
            &custom_sections,
            section_relocations.map(|(i, rs)| (i, rs.iter().cloned())),
            &executable.trampolines,
        );

        // Make all code loaded executable.
        unsafe {
            // SAFETY: We finished relocation and linking just above. There should be no write
            // access past this point, though I don’t think we have a good mechanism to ensure this
            // statically at this point..
            code_memory.publish()?;
        }
        let exports = module
            .exports
            .iter()
            .map(|(s, i)| (s.clone(), i.clone()))
            .collect::<BTreeMap<String, ExportIndex>>();

        Ok(UniversalArtifact {
            engine: self.clone(),
            _code_memory: code_memory,
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
        let (functions, trampolines, dynamic_trampolines, custom_sections, mut code_memory) =
            inner_engine.allocate(
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
        crate::universal::link_module(
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
        unsafe {
            // SAFETY: We finished relocation and linking just above. There should be no write
            // access past this point, though I don’t think we have a good mechanism to ensure this
            // statically at this point..
            code_memory.publish()?;
        }
        let exports = module
            .exports
            .iter()
            .map(|(s, i)| (unrkyv(s), unrkyv(i)))
            .collect::<BTreeMap<String, ExportIndex>>();
        Ok(UniversalArtifact {
            engine: self.clone(),
            _code_memory: code_memory,
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

    /// The target
    pub fn target(&self) -> &Target {
        &self.target
    }

    /// Register a signature
    pub fn register_signature(&self, func_type: FunctionType) -> VMSharedSignatureIndex {
        self.inner().signatures.register(func_type)
    }

    /// Register some function metadata
    pub fn register_function_metadata(&self, func_data: VMCallerCheckedAnyfunc) -> VMFuncRef {
        self.inner().func_data().register(func_data)
    }

    /// Lookup a signature
    pub fn lookup_signature(&self, sig: VMSharedSignatureIndex) -> Option<FunctionType> {
        self.inner().signatures.lookup(sig).cloned()
    }

    /// Validates a WebAssembly module
    #[tracing::instrument(skip_all)]
    pub fn validate(&self, binary: &[u8]) -> Result<(), CompileError> {
        self.inner().validate(binary)
    }

    /// Engine ID
    pub fn id(&self) -> &EngineId {
        &self.engine_id
    }
}

/// The inner contents of `UniversalEngine`
pub struct UniversalEngineInner {
    /// The compiler
    compiler: Option<Box<dyn Compiler>>,
    /// Pool from which code memory can be allocated.
    code_memory_pool: super::LimitedMemoryPool,
    /// The features to compile the Wasm module with
    features: Features,
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
    pub fn compiler(&self) -> Result<&dyn Compiler, CompileError> {
        if self.compiler.is_none() {
            return Err(CompileError::Codegen("The UniversalEngine is operating in headless mode, so it can only execute already compiled Modules.".to_string()));
        }
        Ok(&**self.compiler.as_ref().unwrap())
    }

    /// Validate the module
    pub fn validate<'data>(&self, data: &'data [u8]) -> Result<(), CompileError> {
        self.compiler()?.validate_module(self.features(), data)
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
            CodeMemory,
        ),
        CompileError,
    > {
        let code_memory_pool = &mut self.code_memory_pool;
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

        // 1. Calculate the total size, that is:
        // - function body size, including all trampolines
        // -- windows unwind info
        // -- padding between functions
        // - executable section body
        // -- padding between executable sections
        // - padding until a new page to change page permissions
        // - data section body size
        // -- padding between data sections
        let page_size = rustix::param::page_size();
        let total_len = 0;
        let total_len = function_bodies.iter().fold(total_len, |acc, func| {
            round_up(acc, ARCH_FUNCTION_ALIGNMENT.into()) + function_allocation_size(*func)
        });
        let total_len = executable_sections.iter().fold(total_len, |acc, exec| {
            round_up(acc, ARCH_FUNCTION_ALIGNMENT.into()) + exec.bytes.len()
        });
        let total_len = round_up(total_len, page_size);
        let total_len = data_sections.iter().fold(total_len, |acc, data| {
            round_up(acc, DATA_SECTION_ALIGNMENT.into()) + data.bytes.len()
        });

        let mut code_memory = code_memory_pool.get(total_len).map_err(|e| {
            CompileError::Resource(format!("could not allocate code memory: {}", e))
        })?;
        let mut code_writer = unsafe {
            // SAFETY: We just popped out an unused code memory from an allocator pool.
            code_memory.writer()
        };

        let mut allocated_functions = vec![];
        let mut allocated_data_sections = vec![];
        let mut allocated_executable_sections = vec![];
        for func in function_bodies {
            let offset = code_writer
                .write_executable(ARCH_FUNCTION_ALIGNMENT, func.body)
                .expect("incorrectly computed code memory size");
            allocated_functions.push((offset, func.body.len()));
        }
        for section in executable_sections {
            let offset = code_writer.write_executable(ARCH_FUNCTION_ALIGNMENT, section.bytes)?;
            allocated_executable_sections.push(offset);
        }
        if !data_sections.is_empty() {
            for section in data_sections {
                let offset = code_writer
                    .write_data(DATA_SECTION_ALIGNMENT, section.bytes)
                    .expect("incorrectly computed code memory size");
                allocated_data_sections.push(offset);
            }
        }

        let mut allocated_function_call_trampolines: PrimaryMap<SignatureIndex, VMTrampoline> =
            PrimaryMap::new();

        for (offset, _) in allocated_functions.drain(0..call_trampoline_count) {
            // TODO: What in damnation have you done?! – Bannon
            let trampoline = unsafe {
                std::mem::transmute::<_, VMTrampoline>(code_memory.executable_address(offset))
            };
            allocated_function_call_trampolines.push(trampoline);
        }

        let allocated_functions_result = allocated_functions
            .drain(0..function_count)
            .enumerate()
            .map(|(index, (offset, length))| -> Result<_, CompileError> {
                let index = LocalFunctionIndex::new(index);
                let (sig_idx, sig) = function_signature(index);
                Ok(VMLocalFunction {
                    body: FunctionBodyPtr(unsafe { code_memory.executable_address(offset).cast() }),
                    length: u32::try_from(length).map_err(|_| {
                        CompileError::Codegen("function body length exceeds 4GiB".into())
                    })?,
                    signature: sig,
                    trampoline: allocated_function_call_trampolines[sig_idx],
                })
            })
            .collect::<Result<PrimaryMap<LocalFunctionIndex, _>, _>>()?;

        let allocated_dynamic_function_trampolines = allocated_functions
            .drain(..)
            .map(|(offset, _)| {
                FunctionBodyPtr(unsafe { code_memory.executable_address(offset).cast() })
            })
            .collect::<PrimaryMap<FunctionIndex, _>>();

        let mut exec_iter = allocated_executable_sections.iter();
        let mut data_iter = allocated_data_sections.iter();
        let allocated_custom_sections = section_types
            .into_iter()
            .map(|protection| {
                SectionBodyPtr(if protection == CustomSectionProtection::ReadExecute {
                    unsafe { code_memory.executable_address(*exec_iter.next().unwrap()).cast() }
                } else {
                    unsafe { code_memory.writable_address(*data_iter.next().unwrap()).cast() }
                })
            })
            .collect::<PrimaryMap<SectionIndex, _>>();

        Ok((
            allocated_functions_result,
            allocated_function_call_trampolines,
            allocated_dynamic_function_trampolines,
            allocated_custom_sections,
            code_memory,
        ))
    }

    /// Shared func metadata registry.
    pub(crate) fn func_data(&self) -> &Arc<FuncDataRegistry> {
        &self.func_data
    }
}

fn round_up(size: usize, multiple: usize) -> usize {
    debug_assert!(multiple.is_power_of_two());
    (size + (multiple - 1)) & !(multiple - 1)
}

fn function_allocation_size(func: FunctionBodyRef<'_>) -> usize {
    func.body.len()
}
