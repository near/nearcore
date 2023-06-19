//! Define the `Resolver` trait, allowing custom resolution for external
//! references.

use crate::{ImportError, LinkError};
use more_asserts::assert_ge;
use near_vm_types::entity::{BoxedSlice, EntityRef, PrimaryMap};
use near_vm_types::{ExternType, FunctionIndex, ImportCounts, MemoryType, TableType};

use near_vm_vm::{
    Export, ExportFunctionMetadata, FunctionBodyPtr, ImportFunctionEnv, Imports, MemoryStyle,
    Resolver, VMFunctionBody, VMFunctionEnvironment, VMFunctionImport, VMFunctionKind,
    VMGlobalImport, VMImport, VMImportType, VMMemoryImport, VMTableImport,
};

fn is_compatible_table(ex: &TableType, im: &TableType) -> bool {
    (ex.ty == near_vm_types::Type::FuncRef || ex.ty == im.ty)
        && im.minimum <= ex.minimum
        && (im.maximum.is_none()
            || (ex.maximum.is_some() && im.maximum.unwrap() >= ex.maximum.unwrap()))
}

fn is_compatible_memory(ex: &MemoryType, im: &MemoryType) -> bool {
    im.minimum <= ex.minimum
        && (im.maximum.is_none()
            || (ex.maximum.is_some() && im.maximum.unwrap() >= ex.maximum.unwrap()))
        && ex.shared == im.shared
}

/// This function allows to match all imports of a `ModuleInfo` with concrete definitions provided by
/// a `Resolver`.
///
/// If all imports are satisfied returns an `Imports` instance required for a module instantiation.
pub fn resolve_imports(
    engine: &crate::universal::UniversalEngine,
    resolver: &dyn Resolver,
    import_counts: &ImportCounts,
    imports: &[VMImport],
    finished_dynamic_function_trampolines: &BoxedSlice<FunctionIndex, FunctionBodyPtr>,
) -> Result<Imports, LinkError> {
    let mut function_imports = PrimaryMap::with_capacity(import_counts.functions as _);
    let mut host_function_env_initializers =
        PrimaryMap::with_capacity(import_counts.functions as _);
    let mut table_imports = PrimaryMap::with_capacity(import_counts.tables as _);
    let mut memory_imports = PrimaryMap::with_capacity(import_counts.memories as _);
    let mut global_imports = PrimaryMap::with_capacity(import_counts.globals as _);
    for VMImport { import_no, module, field, ty } in imports {
        let resolved = resolver.resolve(*import_no, module, field);
        let import_extern = || match ty {
            &VMImportType::Table(t) => ExternType::Table(t),
            &VMImportType::Memory(t, _) => ExternType::Memory(t),
            &VMImportType::Global(t) => ExternType::Global(t),
            &VMImportType::Function { sig, static_trampoline: _ } => ExternType::Function(
                engine.lookup_signature(sig).expect("VMSharedSignatureIndex is not valid?"),
            ),
        };
        let resolved = match resolved {
            Some(r) => r,
            None => {
                return Err(LinkError::Import(
                    module.to_string(),
                    field.to_string(),
                    ImportError::UnknownImport(import_extern()),
                ));
            }
        };
        let export_extern = || match resolved {
            Export::Function(ref f) => ExternType::Function(
                engine
                    .lookup_signature(f.vm_function.signature)
                    .expect("VMSharedSignatureIndex not registered with engine (wrong engine?)"),
            ),
            Export::Table(ref t) => ExternType::Table(*t.ty()),
            Export::Memory(ref m) => ExternType::Memory(m.ty()),
            Export::Global(ref g) => {
                let global = g.from.ty();
                ExternType::Global(*global)
            }
        };
        match (&resolved, ty) {
            (Export::Function(ex), VMImportType::Function { sig, static_trampoline })
                if ex.vm_function.signature == *sig =>
            {
                let address = match ex.vm_function.kind {
                    VMFunctionKind::Dynamic => {
                        // If this is a dynamic imported function,
                        // the address of the function is the address of the
                        // reverse trampoline.
                        let index = FunctionIndex::new(function_imports.len());
                        finished_dynamic_function_trampolines[index].0 as *mut VMFunctionBody as _

                        // TODO: We should check that the f.vmctx actually matches
                        // the shape of `VMDynamicFunctionImportContext`
                    }
                    VMFunctionKind::Static => ex.vm_function.address,
                };

                // Clone the host env for this `Instance`.
                let env = if let Some(ExportFunctionMetadata { host_env_clone_fn: clone, .. }) =
                    ex.metadata.as_deref()
                {
                    // TODO: maybe start adding asserts in all these
                    // unsafe blocks to prevent future changes from
                    // horribly breaking things.
                    unsafe {
                        assert!(!ex.vm_function.vmctx.host_env.is_null());
                        (clone)(ex.vm_function.vmctx.host_env)
                    }
                } else {
                    // No `clone` function means we're dealing with some
                    // other kind of `vmctx`, not a host env of any
                    // kind.
                    unsafe { ex.vm_function.vmctx.host_env }
                };

                let trampoline = if let Some(t) = ex.vm_function.call_trampoline {
                    Some(t)
                } else if let VMFunctionKind::Static = ex.vm_function.kind {
                    // Look up a trampoline by finding one by the signature and fill it in.
                    Some(*static_trampoline)
                } else {
                    // FIXME: remove this possibility entirely.
                    None
                };

                function_imports.push(VMFunctionImport {
                    body: FunctionBodyPtr(address),
                    signature: *sig,
                    environment: VMFunctionEnvironment { host_env: env },
                    trampoline,
                });

                let initializer = ex.metadata.as_ref().and_then(|m| m.import_init_function_ptr);
                let clone = ex.metadata.as_ref().map(|m| m.host_env_clone_fn);
                let destructor = ex.metadata.as_ref().map(|m| m.host_env_drop_fn);
                let import_function_env =
                    if let (Some(clone), Some(destructor)) = (clone, destructor) {
                        ImportFunctionEnv::Env { env, clone, initializer, destructor }
                    } else {
                        ImportFunctionEnv::NoEnv
                    };

                host_function_env_initializers.push(import_function_env);
            }
            (Export::Table(ex), VMImportType::Table(im)) if is_compatible_table(ex.ty(), im) => {
                let import_table_ty = ex.from.ty();
                if import_table_ty.ty != im.ty {
                    return Err(LinkError::Import(
                        module.to_string(),
                        field.to_string(),
                        ImportError::IncompatibleType(import_extern(), export_extern()),
                    ));
                }
                table_imports
                    .push(VMTableImport { definition: ex.from.vmtable(), from: ex.from.clone() });
            }
            (Export::Memory(ex), VMImportType::Memory(im, import_memory_style))
                if is_compatible_memory(&ex.ty(), im) =>
            {
                // Sanity-check: Ensure that the imported memory has at least
                // guard-page protections the importing module expects it to have.
                let export_memory_style = ex.style();
                if let (
                    MemoryStyle::Static { bound, .. },
                    MemoryStyle::Static { bound: import_bound, .. },
                ) = (export_memory_style.clone(), &import_memory_style)
                {
                    assert_ge!(bound, *import_bound);
                }
                assert_ge!(
                    export_memory_style.offset_guard_size(),
                    import_memory_style.offset_guard_size()
                );
                memory_imports
                    .push(VMMemoryImport { definition: ex.from.vmmemory(), from: ex.from.clone() });
            }

            (Export::Global(ex), VMImportType::Global(im)) if ex.from.ty() == im => {
                global_imports
                    .push(VMGlobalImport { definition: ex.from.vmglobal(), from: ex.from.clone() });
            }
            _ => {
                return Err(LinkError::Import(
                    module.to_string(),
                    field.to_string(),
                    ImportError::IncompatibleType(import_extern(), export_extern()),
                ));
            }
        }
    }
    Ok(Imports::new(
        function_imports,
        host_function_env_initializers,
        table_imports,
        memory_imports,
        global_imports,
    ))
}
