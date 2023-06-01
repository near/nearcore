// This file contains code from external sources.
// Attributions: https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md

//! Offsets and sizes of various structs in near_vm-vm's vmcontext
//! module.

#![deny(rustdoc::broken_intra_doc_links)]

use crate::VMBuiltinFunctionIndex;
use more_asserts::assert_lt;
use near_vm_types::{
    FunctionIndex, GlobalIndex, LocalGlobalIndex, LocalMemoryIndex, LocalTableIndex, MemoryIndex,
    ModuleInfo, SignatureIndex, TableIndex,
};
use std::convert::TryFrom;

#[cfg(target_pointer_width = "32")]
fn cast_to_u32(sz: usize) -> u32 {
    u32::try_from(sz).unwrap()
}
#[cfg(target_pointer_width = "64")]
fn cast_to_u32(sz: usize) -> u32 {
    u32::try_from(sz).expect("overflow in cast from usize to u32")
}

/// Align an offset used in this module to a specific byte-width by rounding up
#[inline]
const fn align(offset: u32, width: u32) -> u32 {
    (offset + (width - 1)) / width * width
}

/// This class computes offsets to fields within [`VMContext`] and other
/// related structs that JIT code accesses directly.
///
/// [`VMContext`]: crate::vmcontext::VMContext
// Invariant: the addresses always fit into an u32 without overflowing
#[derive(Clone, Debug)]
pub struct VMOffsets {
    /// The size in bytes of a pointer on the target.
    pointer_size: u8,
    /// The number of signature declarations in the module.
    num_signature_ids: u32,
    /// The number of imported functions in the module.
    num_imported_functions: u32,
    /// The number of imported tables in the module.
    num_imported_tables: u32,
    /// The number of imported memories in the module.
    num_imported_memories: u32,
    /// The number of imported globals in the module.
    num_imported_globals: u32,
    /// The number of defined tables in the module.
    num_local_tables: u32,
    /// The number of defined memories in the module.
    num_local_memories: u32,
    /// The number of defined globals in the module.
    num_local_globals: u32,
    /// If the module has trap handler.
    has_trap_handlers: bool,

    vmctx_signature_ids_begin: u32,
    vmctx_imported_functions_begin: u32,
    vmctx_imported_tables_begin: u32,
    vmctx_imported_memories_begin: u32,
    vmctx_imported_globals_begin: u32,
    vmctx_tables_begin: u32,
    vmctx_memories_begin: u32,
    vmctx_globals_begin: u32,
    vmctx_builtin_functions_begin: u32,
    vmctx_trap_handler_begin: u32,
    vmctx_gas_limiter_pointer: u32,
    vmctx_stack_limit_begin: u32,
    vmctx_stack_limit_initial_begin: u32,
    size_of_vmctx: u32,
}

impl VMOffsets {
    /// Return a new `VMOffsets` instance, for a given pointer size.
    ///
    /// The returned `VMOffsets` has no entities. Add entities with other builder methods for this
    /// type.
    pub fn new(pointer_size: u8) -> Self {
        Self {
            pointer_size,
            num_signature_ids: 0,
            num_imported_functions: 0,
            num_imported_tables: 0,
            num_imported_memories: 0,
            num_imported_globals: 0,
            num_local_tables: 0,
            num_local_memories: 0,
            num_local_globals: 0,
            has_trap_handlers: false,
            vmctx_signature_ids_begin: 0,
            vmctx_imported_functions_begin: 0,
            vmctx_imported_tables_begin: 0,
            vmctx_imported_memories_begin: 0,
            vmctx_imported_globals_begin: 0,
            vmctx_tables_begin: 0,
            vmctx_memories_begin: 0,
            vmctx_globals_begin: 0,
            vmctx_builtin_functions_begin: 0,
            vmctx_trap_handler_begin: 0,
            vmctx_gas_limiter_pointer: 0,
            vmctx_stack_limit_begin: 0,
            vmctx_stack_limit_initial_begin: 0,
            size_of_vmctx: 0,
        }
    }

    /// Return a new `VMOffsets` instance, for a host's pointer size.
    ///
    /// The returned `VMOffsets` has no entities. Add entities with other builder methods for this
    /// type.
    pub fn for_host() -> Self {
        Self::new(std::mem::size_of::<*const u8>() as u8)
    }

    /// Add imports and locals from the provided ModuleInfo.
    #[tracing::instrument(skip_all)]
    pub fn with_module_info(mut self, module: &ModuleInfo) -> Self {
        self.num_imported_functions = module.import_counts.functions;
        self.num_imported_tables = module.import_counts.tables;
        self.num_imported_memories = module.import_counts.memories;
        self.num_imported_globals = module.import_counts.globals;
        self.num_signature_ids = cast_to_u32(module.signatures.len());
        // FIXME = these should most likely be subtracting the corresponding imports!!?
        self.num_local_tables = cast_to_u32(module.tables.len());
        self.num_local_memories = cast_to_u32(module.memories.len());
        self.num_local_globals = cast_to_u32(module.globals.len());
        self.has_trap_handlers = true;
        self.precompute();
        self
    }

    /// Add imports and locals from the provided ModuleInfo.
    pub fn with_archived_module_info(mut self, module: &rkyv::Archived<ModuleInfo>) -> Self {
        self.num_imported_functions = module.import_counts.functions;
        self.num_imported_tables = module.import_counts.tables;
        self.num_imported_memories = module.import_counts.memories;
        self.num_imported_globals = module.import_counts.globals;
        self.num_signature_ids = cast_to_u32(module.signatures.len());
        // FIXME = these should most likely be subtracting the corresponding imports!!?
        self.num_local_tables = cast_to_u32(module.tables.len());
        self.num_local_memories = cast_to_u32(module.memories.len());
        self.num_local_globals = cast_to_u32(module.globals.len());
        self.has_trap_handlers = true;
        self.precompute();
        self
    }

    /// Number of local tables defined in the module
    pub fn num_local_tables(&self) -> u32 {
        self.num_local_tables
    }

    /// Number of local memories defined in the module
    pub fn num_local_memories(&self) -> u32 {
        self.num_local_memories
    }

    fn precompute(&mut self) {
        use std::mem::align_of;

        /// Offset base by num_items items of size item_size, panicking on overflow
        fn offset_by(
            base: u32,
            num_items: u32,
            prev_item_size: u32,
            next_item_align: usize,
        ) -> u32 {
            align(
                base.checked_add(num_items.checked_mul(prev_item_size).unwrap()).unwrap(),
                next_item_align as u32,
            )
        }

        self.vmctx_signature_ids_begin = 0;
        self.vmctx_imported_functions_begin = offset_by(
            self.vmctx_signature_ids_begin,
            self.num_signature_ids,
            u32::from(self.size_of_vmshared_signature_index()),
            align_of::<crate::VMFunctionImport>(),
        );
        self.vmctx_imported_tables_begin = offset_by(
            self.vmctx_imported_functions_begin,
            self.num_imported_functions,
            u32::from(self.size_of_vmfunction_import()),
            align_of::<crate::VMTableImport>(),
        );
        self.vmctx_imported_memories_begin = offset_by(
            self.vmctx_imported_tables_begin,
            self.num_imported_tables,
            u32::from(self.size_of_vmtable_import()),
            align_of::<crate::VMMemoryImport>(),
        );
        self.vmctx_imported_globals_begin = offset_by(
            self.vmctx_imported_memories_begin,
            self.num_imported_memories,
            u32::from(self.size_of_vmmemory_import()),
            align_of::<crate::VMGlobalImport>(),
        );
        self.vmctx_tables_begin = offset_by(
            self.vmctx_imported_globals_begin,
            self.num_imported_globals,
            u32::from(self.size_of_vmglobal_import()),
            align_of::<crate::VMTableImport>(),
        );
        self.vmctx_memories_begin = offset_by(
            self.vmctx_tables_begin,
            self.num_local_tables,
            u32::from(self.size_of_vmtable_definition()),
            align_of::<crate::VMMemoryDefinition>(),
        );
        self.vmctx_globals_begin = offset_by(
            self.vmctx_memories_begin,
            self.num_local_memories,
            u32::from(self.size_of_vmmemory_definition()),
            align_of::<crate::VMGlobalDefinition>(),
        );
        self.vmctx_builtin_functions_begin = offset_by(
            self.vmctx_globals_begin,
            self.num_local_globals,
            u32::from(self.size_of_vmglobal_local()),
            align_of::<crate::vmcontext::VMBuiltinFunctionsArray>(),
        );
        self.vmctx_trap_handler_begin = offset_by(
            self.vmctx_builtin_functions_begin,
            VMBuiltinFunctionIndex::builtin_functions_total_number(),
            u32::from(self.pointer_size),
            align_of::<fn()>(),
        );
        self.vmctx_gas_limiter_pointer = offset_by(
            self.vmctx_trap_handler_begin,
            if self.has_trap_handlers { 1 } else { 0 },
            u32::from(self.pointer_size),
            align_of::<*mut near_vm_types::FastGasCounter>(),
        );
        self.vmctx_stack_limit_begin = offset_by(
            self.vmctx_gas_limiter_pointer,
            1,
            u32::from(self.pointer_size),
            align_of::<u32>(),
        );
        self.vmctx_stack_limit_initial_begin = self.vmctx_stack_limit_begin.checked_add(4).unwrap();
        self.size_of_vmctx = self.vmctx_stack_limit_begin.checked_add(4).unwrap();
    }
}

/// Offsets for [`VMFunctionImport`].
///
/// [`VMFunctionImport`]: crate::vmcontext::VMFunctionImport
impl VMOffsets {
    /// The offset of the `body` field.
    #[allow(clippy::erasing_op)]
    pub const fn vmfunction_import_body(&self) -> u8 {
        0 * self.pointer_size
    }

    /// The offset of the `vmctx` field.
    #[allow(clippy::identity_op)]
    pub const fn vmfunction_import_vmctx(&self) -> u8 {
        3 * self.pointer_size
    }

    /// Return the size of [`VMFunctionImport`].
    ///
    /// [`VMFunctionImport`]: crate::vmcontext::VMFunctionImport
    pub const fn size_of_vmfunction_import(&self) -> u8 {
        4 * self.pointer_size
    }
}

/// Offsets for [`VMDynamicFunctionContext`].
///
/// [`VMDynamicFunctionContext`]: crate::vmcontext::VMDynamicFunctionContext
impl VMOffsets {
    /// The offset of the `address` field.
    #[allow(clippy::erasing_op)]
    pub const fn vmdynamicfunction_import_context_address(&self) -> u8 {
        0 * self.pointer_size
    }

    /// The offset of the `ctx` field.
    #[allow(clippy::identity_op)]
    pub const fn vmdynamicfunction_import_context_ctx(&self) -> u8 {
        1 * self.pointer_size
    }

    /// Return the size of [`VMDynamicFunctionContext`].
    ///
    /// [`VMDynamicFunctionContext`]: crate::vmcontext::VMDynamicFunctionContext
    pub const fn size_of_vmdynamicfunction_import_context(&self) -> u8 {
        2 * self.pointer_size
    }
}

/// Offsets for `*const VMFunctionBody`.
impl VMOffsets {
    /// The size of the `current_elements` field.
    #[allow(clippy::identity_op)]
    pub const fn size_of_vmfunction_body_ptr(&self) -> u8 {
        1 * self.pointer_size
    }
}

/// Offsets for [`VMTableImport`].
///
/// [`VMTableImport`]: crate::vmcontext::VMTableImport
impl VMOffsets {
    /// The offset of the `definition` field.
    #[allow(clippy::erasing_op)]
    pub const fn vmtable_import_definition(&self) -> u8 {
        0 * self.pointer_size
    }

    /// The offset of the `from` field.
    #[allow(clippy::identity_op)]
    pub const fn vmtable_import_from(&self) -> u8 {
        1 * self.pointer_size
    }

    /// Return the size of [`VMTableImport`].
    ///
    /// [`VMTableImport`]: crate::vmcontext::VMTableImport
    pub const fn size_of_vmtable_import(&self) -> u8 {
        3 * self.pointer_size
    }
}

/// Offsets for [`VMTableDefinition`].
///
/// [`VMTableDefinition`]: crate::vmcontext::VMTableDefinition
impl VMOffsets {
    /// The offset of the `base` field.
    #[allow(clippy::erasing_op)]
    pub const fn vmtable_definition_base(&self) -> u8 {
        0 * self.pointer_size
    }

    /// The offset of the `current_elements` field.
    #[allow(clippy::identity_op)]
    pub const fn vmtable_definition_current_elements(&self) -> u8 {
        1 * self.pointer_size
    }

    /// The size of the `current_elements` field.
    pub const fn size_of_vmtable_definition_current_elements(&self) -> u8 {
        4
    }

    /// Return the size of [`VMTableDefinition`].
    ///
    /// [`VMTableDefinition`]: crate::vmcontext::VMTableDefinition
    pub const fn size_of_vmtable_definition(&self) -> u8 {
        2 * self.pointer_size
    }
}

/// Offsets for [`VMMemoryImport`].
///
/// [`VMMemoryImport`]: crate::vmcontext::VMMemoryImport
impl VMOffsets {
    /// The offset of the `from` field.
    #[allow(clippy::erasing_op)]
    pub const fn vmmemory_import_definition(&self) -> u8 {
        0 * self.pointer_size
    }

    /// The offset of the `from` field.
    #[allow(clippy::identity_op)]
    pub const fn vmmemory_import_from(&self) -> u8 {
        1 * self.pointer_size
    }

    /// Return the size of [`VMMemoryImport`].
    ///
    /// [`VMMemoryImport`]: crate::vmcontext::VMMemoryImport
    pub const fn size_of_vmmemory_import(&self) -> u8 {
        3 * self.pointer_size
    }
}

/// Offsets for [`VMMemoryDefinition`].
///
/// [`VMMemoryDefinition`]: crate::vmcontext::VMMemoryDefinition
impl VMOffsets {
    /// The offset of the `base` field.
    #[allow(clippy::erasing_op)]
    pub const fn vmmemory_definition_base(&self) -> u8 {
        0 * self.pointer_size
    }

    /// The offset of the `current_length` field.
    #[allow(clippy::identity_op)]
    pub const fn vmmemory_definition_current_length(&self) -> u8 {
        1 * self.pointer_size
    }

    /// The size of the `current_length` field.
    pub const fn size_of_vmmemory_definition_current_length(&self) -> u8 {
        4
    }

    /// Return the size of [`VMMemoryDefinition`].
    ///
    /// [`VMMemoryDefinition`]: crate::vmcontext::VMMemoryDefinition
    pub const fn size_of_vmmemory_definition(&self) -> u8 {
        2 * self.pointer_size
    }
}

/// Offsets for [`VMGlobalImport`].
///
/// [`VMGlobalImport`]: crate::vmcontext::VMGlobalImport
impl VMOffsets {
    /// The offset of the `definition` field.
    #[allow(clippy::erasing_op)]
    pub const fn vmglobal_import_definition(&self) -> u8 {
        0 * self.pointer_size
    }

    /// The offset of the `from` field.
    #[allow(clippy::identity_op)]
    pub const fn vmglobal_import_from(&self) -> u8 {
        1 * self.pointer_size
    }

    /// Return the size of [`VMGlobalImport`].
    ///
    /// [`VMGlobalImport`]: crate::vmcontext::VMGlobalImport
    #[allow(clippy::identity_op)]
    pub const fn size_of_vmglobal_import(&self) -> u8 {
        2 * self.pointer_size
    }
}

/// Offsets for a non-null pointer to a [`VMGlobalDefinition`] used as a local global.
///
/// [`VMGlobalDefinition`]: crate::vmcontext::VMGlobalDefinition
impl VMOffsets {
    /// Return the size of a pointer to a [`VMGlobalDefinition`];
    ///
    /// The underlying global itself is the size of the largest value type (i.e. a V128),
    /// however the size of this type is just the size of a pointer.
    ///
    /// [`VMGlobalDefinition`]: crate::vmcontext::VMGlobalDefinition
    pub const fn size_of_vmglobal_local(&self) -> u8 {
        self.pointer_size
    }
}

/// Offsets for [`VMSharedSignatureIndex`].
///
/// [`VMSharedSignatureIndex`]: crate::vmcontext::VMSharedSignatureIndex
impl VMOffsets {
    /// Return the size of [`VMSharedSignatureIndex`].
    ///
    /// [`VMSharedSignatureIndex`]: crate::vmcontext::VMSharedSignatureIndex
    pub const fn size_of_vmshared_signature_index(&self) -> u8 {
        4
    }
}

/// Offsets for [`VMCallerCheckedAnyfunc`].
///
/// [`VMCallerCheckedAnyfunc`]: crate::vmcontext::VMCallerCheckedAnyfunc
impl VMOffsets {
    /// The offset of the `func_ptr` field.
    #[allow(clippy::erasing_op)]
    pub const fn vmcaller_checked_anyfunc_func_ptr(&self) -> u8 {
        0 * self.pointer_size
    }

    /// The offset of the `type_index` field.
    #[allow(clippy::identity_op)]
    pub const fn vmcaller_checked_anyfunc_type_index(&self) -> u8 {
        1 * self.pointer_size
    }

    /// The offset of the `vmctx` field.
    pub const fn vmcaller_checked_anyfunc_vmctx(&self) -> u8 {
        2 * self.pointer_size
    }

    /// Return the size of [`VMCallerCheckedAnyfunc`].
    ///
    /// [`VMCallerCheckedAnyfunc`]: crate::vmcontext::VMCallerCheckedAnyfunc
    pub const fn size_of_vmcaller_checked_anyfunc(&self) -> u8 {
        3 * self.pointer_size
    }
}

/// Offsets for [`VMFuncRef`].
///
/// [`VMFuncRef`]: crate::func_data_registry::VMFuncRef
impl VMOffsets {
    /// The offset to the pointer to the anyfunc inside the ref.
    #[allow(clippy::erasing_op)]
    pub const fn vm_funcref_anyfunc_ptr(&self) -> u8 {
        0 * self.pointer_size
    }

    /// Return the size of [`VMFuncRef`].
    ///
    /// [`VMFuncRef`]: crate::func_data_registry::VMFuncRef
    pub const fn size_of_vm_funcref(&self) -> u8 {
        self.pointer_size
    }
}

/// Offsets for [`VMContext`].
///
/// [`VMContext`]: crate::vmcontext::VMContext
impl VMOffsets {
    /// The offset of the `signature_ids` array.
    #[inline]
    pub fn vmctx_signature_ids_begin(&self) -> u32 {
        self.vmctx_signature_ids_begin
    }

    /// The offset of the `tables` array.
    pub fn vmctx_imported_functions_begin(&self) -> u32 {
        self.vmctx_imported_functions_begin
    }

    /// The offset of the `tables` array.
    #[allow(clippy::identity_op)]
    pub fn vmctx_imported_tables_begin(&self) -> u32 {
        self.vmctx_imported_tables_begin
    }

    /// The offset of the `memories` array.
    pub fn vmctx_imported_memories_begin(&self) -> u32 {
        self.vmctx_imported_memories_begin
    }

    /// The offset of the `globals` array.
    pub fn vmctx_imported_globals_begin(&self) -> u32 {
        self.vmctx_imported_globals_begin
    }

    /// The offset of the `tables` array.
    pub fn vmctx_tables_begin(&self) -> u32 {
        self.vmctx_tables_begin
    }

    /// The offset of the `memories` array.
    pub fn vmctx_memories_begin(&self) -> u32 {
        self.vmctx_memories_begin
    }

    /// The offset of the `globals` array.
    pub fn vmctx_globals_begin(&self) -> u32 {
        self.vmctx_globals_begin
    }

    /// The offset of the builtin functions array.
    pub fn vmctx_builtin_functions_begin(&self) -> u32 {
        self.vmctx_builtin_functions_begin
    }

    /// The offset of the trap handler.
    pub fn vmctx_trap_handler_begin(&self) -> u32 {
        self.vmctx_trap_handler_begin
    }

    /// The offset of the gas limiter pointer.
    pub fn vmctx_gas_limiter_pointer(&self) -> u32 {
        self.vmctx_gas_limiter_pointer
    }

    /// The offset of the current stack limit.
    pub fn vmctx_stack_limit_begin(&self) -> u32 {
        self.vmctx_stack_limit_begin
    }

    /// The offset of the initial stack limit.
    pub fn vmctx_stack_limit_initial_begin(&self) -> u32 {
        self.vmctx_stack_limit_initial_begin
    }

    /// Return the size of the [`VMContext`] allocation.
    ///
    /// [`VMContext`]: crate::vmcontext::VMContext
    pub fn size_of_vmctx(&self) -> u32 {
        self.size_of_vmctx
    }

    /// Return the offset to [`VMSharedSignatureIndex`] index `index`.
    ///
    /// [`VMSharedSignatureIndex`]: crate::vmcontext::VMSharedSignatureIndex
    // Remember updating precompute upon changes
    pub fn vmctx_vmshared_signature_id(&self, index: SignatureIndex) -> u32 {
        assert_lt!(index.as_u32(), self.num_signature_ids);
        self.vmctx_signature_ids_begin
            + index.as_u32() * u32::from(self.size_of_vmshared_signature_index())
    }

    /// Return the offset to [`VMFunctionImport`] index `index`.
    ///
    /// [`VMFunctionImport`]: crate::vmcontext::VMFunctionImport
    // Remember updating precompute upon changes
    pub fn vmctx_vmfunction_import(&self, index: FunctionIndex) -> u32 {
        assert_lt!(index.as_u32(), self.num_imported_functions);
        self.vmctx_imported_functions_begin
            + index.as_u32() * u32::from(self.size_of_vmfunction_import())
    }

    /// Return the offset to [`VMTableImport`] index `index`.
    ///
    /// [`VMTableImport`]: crate::vmcontext::VMTableImport
    // Remember updating precompute upon changes
    pub fn vmctx_vmtable_import(&self, index: TableIndex) -> u32 {
        assert_lt!(index.as_u32(), self.num_imported_tables);
        self.vmctx_imported_tables_begin + index.as_u32() * u32::from(self.size_of_vmtable_import())
    }

    /// Return the offset to [`VMMemoryImport`] index `index`.
    ///
    /// [`VMMemoryImport`]: crate::vmcontext::VMMemoryImport
    // Remember updating precompute upon changes
    pub fn vmctx_vmmemory_import(&self, index: MemoryIndex) -> u32 {
        assert_lt!(index.as_u32(), self.num_imported_memories);
        self.vmctx_imported_memories_begin
            + index.as_u32() * u32::from(self.size_of_vmmemory_import())
    }

    /// Return the offset to [`VMGlobalImport`] index `index`.
    ///
    /// [`VMGlobalImport`]: crate::vmcontext::VMGlobalImport
    // Remember updating precompute upon changes
    pub fn vmctx_vmglobal_import(&self, index: GlobalIndex) -> u32 {
        assert_lt!(index.as_u32(), self.num_imported_globals);
        self.vmctx_imported_globals_begin
            + index.as_u32() * u32::from(self.size_of_vmglobal_import())
    }

    /// Return the offset to [`VMTableDefinition`] index `index`.
    ///
    /// [`VMTableDefinition`]: crate::vmcontext::VMTableDefinition
    // Remember updating precompute upon changes
    pub fn vmctx_vmtable_definition(&self, index: LocalTableIndex) -> u32 {
        assert_lt!(index.as_u32(), self.num_local_tables);
        self.vmctx_tables_begin + index.as_u32() * u32::from(self.size_of_vmtable_definition())
    }

    /// Return the offset to [`VMMemoryDefinition`] index `index`.
    ///
    /// [`VMMemoryDefinition`]: crate::vmcontext::VMMemoryDefinition
    // Remember updating precompute upon changes
    pub fn vmctx_vmmemory_definition(&self, index: LocalMemoryIndex) -> u32 {
        assert_lt!(index.as_u32(), self.num_local_memories);
        self.vmctx_memories_begin + index.as_u32() * u32::from(self.size_of_vmmemory_definition())
    }

    /// Return the offset to the [`VMGlobalDefinition`] index `index`.
    ///
    /// [`VMGlobalDefinition`]: crate::vmcontext::VMGlobalDefinition
    // Remember updating precompute upon changes
    pub fn vmctx_vmglobal_definition(&self, index: LocalGlobalIndex) -> u32 {
        assert_lt!(index.as_u32(), self.num_local_globals);
        self.vmctx_globals_begin + index.as_u32() * u32::from(self.size_of_vmglobal_local())
    }

    /// Return the offset to the `body` field in `*const VMFunctionBody` index `index`.
    // Remember updating precompute upon changes
    pub fn vmctx_vmfunction_import_body(&self, index: FunctionIndex) -> u32 {
        self.vmctx_vmfunction_import(index) + u32::from(self.vmfunction_import_body())
    }

    /// Return the offset to the `vmctx` field in `*const VMFunctionBody` index `index`.
    // Remember updating precompute upon changes
    pub fn vmctx_vmfunction_import_vmctx(&self, index: FunctionIndex) -> u32 {
        self.vmctx_vmfunction_import(index) + u32::from(self.vmfunction_import_vmctx())
    }

    /// Return the offset to the `definition` field in [`VMTableImport`] index `index`.
    ///
    /// [`VMTableImport`]: crate::vmcontext::VMTableImport
    // Remember updating precompute upon changes
    pub fn vmctx_vmtable_import_definition(&self, index: TableIndex) -> u32 {
        self.vmctx_vmtable_import(index) + u32::from(self.vmtable_import_definition())
    }

    /// Return the offset to the `base` field in [`VMTableDefinition`] index `index`.
    ///
    /// [`VMTableDefinition`]: crate::vmcontext::VMTableDefinition
    // Remember updating precompute upon changes
    pub fn vmctx_vmtable_definition_base(&self, index: LocalTableIndex) -> u32 {
        self.vmctx_vmtable_definition(index) + u32::from(self.vmtable_definition_base())
    }

    /// Return the offset to the `current_elements` field in [`VMTableDefinition`] index `index`.
    ///
    /// [`VMTableDefinition`]: crate::vmcontext::VMTableDefinition
    // Remember updating precompute upon changes
    pub fn vmctx_vmtable_definition_current_elements(&self, index: LocalTableIndex) -> u32 {
        self.vmctx_vmtable_definition(index) + u32::from(self.vmtable_definition_current_elements())
    }

    /// Return the offset to the `from` field in [`VMMemoryImport`] index `index`.
    ///
    /// [`VMMemoryImport`]: crate::vmcontext::VMMemoryImport
    // Remember updating precompute upon changes
    pub fn vmctx_vmmemory_import_definition(&self, index: MemoryIndex) -> u32 {
        self.vmctx_vmmemory_import(index) + u32::from(self.vmmemory_import_definition())
    }

    /// Return the offset to the `vmctx` field in [`VMMemoryImport`] index `index`.
    ///
    /// [`VMMemoryImport`]: crate::vmcontext::VMMemoryImport
    // Remember updating precompute upon changes
    pub fn vmctx_vmmemory_import_from(&self, index: MemoryIndex) -> u32 {
        self.vmctx_vmmemory_import(index) + u32::from(self.vmmemory_import_from())
    }

    /// Return the offset to the `base` field in [`VMMemoryDefinition`] index `index`.
    ///
    /// [`VMMemoryDefinition`]: crate::vmcontext::VMMemoryDefinition
    // Remember updating precompute upon changes
    pub fn vmctx_vmmemory_definition_base(&self, index: LocalMemoryIndex) -> u32 {
        self.vmctx_vmmemory_definition(index) + u32::from(self.vmmemory_definition_base())
    }

    /// Return the offset to the `current_length` field in [`VMMemoryDefinition`] index `index`.
    ///
    /// [`VMMemoryDefinition`]: crate::vmcontext::VMMemoryDefinition
    // Remember updating precompute upon changes
    pub fn vmctx_vmmemory_definition_current_length(&self, index: LocalMemoryIndex) -> u32 {
        self.vmctx_vmmemory_definition(index) + u32::from(self.vmmemory_definition_current_length())
    }

    /// Return the offset to the `from` field in [`VMGlobalImport`] index `index`.
    ///
    /// [`VMGlobalImport`]: crate::vmcontext::VMGlobalImport
    // Remember updating precompute upon changes
    pub fn vmctx_vmglobal_import_definition(&self, index: GlobalIndex) -> u32 {
        self.vmctx_vmglobal_import(index) + u32::from(self.vmglobal_import_definition())
    }

    /// Return the offset to builtin function in `VMBuiltinFunctionsArray` index `index`.
    // Remember updating precompute upon changes
    pub fn vmctx_builtin_function(&self, index: VMBuiltinFunctionIndex) -> u32 {
        self.vmctx_builtin_functions_begin + index.index() * u32::from(self.pointer_size)
    }

    /// Return the offset to the trap handler.
    pub fn vmctx_trap_handler(&self) -> u32 {
        // Ensure that we never ask for trap handler offset if it's not enabled.
        assert!(self.has_trap_handlers);
        self.vmctx_trap_handler_begin
    }
}

/// Target specific type for shared signature index.
#[derive(Debug, Copy, Clone)]
pub struct TargetSharedSignatureIndex(u32);

impl TargetSharedSignatureIndex {
    /// Constructs `TargetSharedSignatureIndex`.
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns index value.
    pub const fn index(self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::vmoffsets::align;

    #[test]
    fn alignment() {
        fn is_aligned(x: u32) -> bool {
            x % 16 == 0
        }
        assert!(is_aligned(align(0, 16)));
        assert!(is_aligned(align(32, 16)));
        assert!(is_aligned(align(33, 16)));
        assert!(is_aligned(align(31, 16)));
    }
}
