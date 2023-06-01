use std::sync::Arc;

use crate::{ImportInitializerFuncPtr, VMExtern, VMFunction, VMGlobal, VMMemory, VMTable};

/// The value of an export passed from one instance to another.
#[derive(Debug, Clone)]
pub enum Export {
    /// A function export value.
    Function(ExportFunction),

    /// A table export value.
    Table(VMTable),

    /// A memory export value.
    Memory(VMMemory),

    /// A global export value.
    Global(VMGlobal),
}

impl From<Export> for VMExtern {
    fn from(other: Export) -> Self {
        match other {
            Export::Function(ExportFunction { vm_function, .. }) => Self::Function(vm_function),
            Export::Memory(vm_memory) => Self::Memory(vm_memory),
            Export::Table(vm_table) => Self::Table(vm_table),
            Export::Global(vm_global) => Self::Global(vm_global),
        }
    }
}

impl From<VMExtern> for Export {
    fn from(other: VMExtern) -> Self {
        match other {
            VMExtern::Function(vm_function) => {
                Self::Function(ExportFunction { vm_function, metadata: None })
            }
            VMExtern::Memory(vm_memory) => Self::Memory(vm_memory),
            VMExtern::Table(vm_table) => Self::Table(vm_table),
            VMExtern::Global(vm_global) => Self::Global(vm_global),
        }
    }
}

/// Extra metadata about `ExportFunction`s.
///
/// The metadata acts as a kind of manual virtual dispatch. We store the
/// user-supplied `WasmerEnv` as a void pointer and have methods on it
/// that have been adapted to accept a void pointer.
///
/// This struct owns the original `host_env`, thus when it gets dropped
/// it calls the `drop` function on it.
#[derive(Debug, PartialEq)]
pub struct ExportFunctionMetadata {
    /// This field is stored here to be accessible by `Drop`.
    ///
    /// At the time it was added, it's not accessed anywhere outside of
    /// the `Drop` implementation. This field is the "master copy" of the env,
    /// that is, the original env passed in by the user. Every time we create
    /// an `Instance` we clone this with the `host_env_clone_fn` field.
    ///
    /// Thus, we only bother to store the master copy at all here so that
    /// we can free it.
    ///
    /// See `near_vm_vm::export::VMFunction::vmctx` for the version of
    /// this pointer that is used by the VM when creating an `Instance`.
    pub host_env: *mut std::ffi::c_void,

    /// Function pointer to `WasmerEnv::init_with_instance(&mut self, instance: &Instance)`.
    ///
    /// This function is called to finish setting up the environment after
    /// we create the `api::Instance`.
    // This one is optional for now because dynamic host envs need the rest
    // of this without the init fn
    pub import_init_function_ptr: Option<ImportInitializerFuncPtr>,

    /// A function analogous to `Clone::clone` that returns a leaked `Box`.
    pub host_env_clone_fn: fn(*mut std::ffi::c_void) -> *mut std::ffi::c_void,

    /// The destructor to free the host environment.
    ///
    /// # Safety
    /// - This function should only be called in when properly synchronized.
    /// For example, in the `Drop` implementation of this type.
    pub host_env_drop_fn: unsafe fn(*mut std::ffi::c_void),
}

/// This can be `Send` because `host_env` comes from `WasmerEnv` which is
/// `Send`. Therefore all operations should work on any thread.
unsafe impl Send for ExportFunctionMetadata {}
/// This data may be shared across threads, `drop` is an unsafe function
/// pointer, so care must be taken when calling it.
unsafe impl Sync for ExportFunctionMetadata {}

impl ExportFunctionMetadata {
    /// Create an `ExportFunctionMetadata` type with information about
    /// the exported function.
    ///
    /// # Safety
    /// - the `host_env` must be `Send`.
    /// - all function pointers must work on any thread.
    pub unsafe fn new(
        host_env: *mut std::ffi::c_void,
        import_init_function_ptr: Option<ImportInitializerFuncPtr>,
        host_env_clone_fn: fn(*mut std::ffi::c_void) -> *mut std::ffi::c_void,
        host_env_drop_fn: fn(*mut std::ffi::c_void),
    ) -> Self {
        Self { host_env, import_init_function_ptr, host_env_clone_fn, host_env_drop_fn }
    }
}

// We have to free `host_env` here because we always clone it before using it
// so all the `host_env`s freed at the `Instance` level won't touch the original.
impl Drop for ExportFunctionMetadata {
    fn drop(&mut self) {
        if !self.host_env.is_null() {
            // # Safety
            // - This is correct because we know no other references
            //   to this data can exist if we're dropping it.
            unsafe {
                (self.host_env_drop_fn)(self.host_env);
            }
        }
    }
}

/// A function export value with an extra function pointer to initialize
/// host environments.
#[derive(Debug, Clone, PartialEq)]
pub struct ExportFunction {
    /// The VM function, containing most of the data.
    pub vm_function: VMFunction,
    /// Contains functions necessary to create and initialize host envs
    /// with each `Instance` as well as being responsible for the
    /// underlying memory of the host env.
    pub metadata: Option<Arc<ExportFunctionMetadata>>,
}

impl From<ExportFunction> for Export {
    fn from(func: ExportFunction) -> Self {
        Self::Function(func)
    }
}

impl From<VMTable> for Export {
    fn from(table: VMTable) -> Self {
        Self::Table(table)
    }
}

impl From<VMMemory> for Export {
    fn from(memory: VMMemory) -> Self {
        Self::Memory(memory)
    }
}

impl From<VMGlobal> for Export {
    fn from(global: VMGlobal) -> Self {
        Self::Global(global)
    }
}

///
/// Import resolver connects imports with available exported values.
pub trait Resolver {
    /// Resolves an import a WebAssembly module to an export it's hooked up to.
    ///
    /// The `index` provided is the index of the import in the wasm module
    /// that's being resolved. For example 1 means that it's the second import
    /// listed in the wasm module.
    ///
    /// The `module` and `field` arguments provided are the module/field names
    /// listed on the import itself.
    ///
    /// # Notes:
    ///
    /// The index is useful because some WebAssembly modules may rely on that
    /// for resolving ambiguity in their imports. Such as:
    /// ```ignore
    /// (module
    ///   (import "" "" (func))
    ///   (import "" "" (func (param i32) (result i32)))
    /// )
    /// ```
    fn resolve(&self, _index: u32, module: &str, field: &str) -> Option<Export>;
}

/// Import resolver connects imports with available exported values.
///
/// This is a specific subtrait for [`Resolver`] for those users who don't
/// care about the `index`, but only about the `module` and `field` for
/// the resolution.
pub trait NamedResolver {
    /// Resolves an import a WebAssembly module to an export it's hooked up to.
    ///
    /// It receives the `module` and `field` names and return the [`Export`] in
    /// case it's found.
    fn resolve_by_name(&self, module: &str, field: &str) -> Option<Export>;
}

// All NamedResolvers should extend `Resolver`.
impl<T: NamedResolver> Resolver for T {
    /// By default this method will be calling [`NamedResolver::resolve_by_name`],
    /// dismissing the provided `index`.
    fn resolve(&self, _index: u32, module: &str, field: &str) -> Option<Export> {
        self.resolve_by_name(module, field)
    }
}

impl<T: NamedResolver> NamedResolver for &T {
    fn resolve_by_name(&self, module: &str, field: &str) -> Option<Export> {
        (**self).resolve_by_name(module, field)
    }
}

impl NamedResolver for Box<dyn NamedResolver + Send + Sync> {
    fn resolve_by_name(&self, module: &str, field: &str) -> Option<Export> {
        (**self).resolve_by_name(module, field)
    }
}

impl NamedResolver for () {
    /// Always returns `None`.
    fn resolve_by_name(&self, _module: &str, _field: &str) -> Option<Export> {
        None
    }
}

/// `Resolver` implementation that always resolves to `None`. Equivalent to `()`.
pub struct NullResolver {}

impl Resolver for NullResolver {
    fn resolve(&self, _idx: u32, _module: &str, _field: &str) -> Option<Export> {
        None
    }
}

/// A [`Resolver`] that links two resolvers together in a chain.
pub struct NamedResolverChain<A: NamedResolver + Send + Sync, B: NamedResolver + Send + Sync> {
    a: A,
    b: B,
}

/// A trait for chaining resolvers together.
///
/// ```
/// # use near_vm_vm::{ChainableNamedResolver, NamedResolver};
/// # fn chainable_test<A, B>(imports1: A, imports2: B)
/// # where A: NamedResolver + Sized + Send + Sync,
/// #       B: NamedResolver + Sized + Send + Sync,
/// # {
/// // override duplicates with imports from `imports2`
/// imports1.chain_front(imports2);
/// # }
/// ```
pub trait ChainableNamedResolver: NamedResolver + Sized + Send + Sync {
    /// Chain a resolver in front of the current resolver.
    ///
    /// This will cause the second resolver to override the first.
    ///
    /// ```
    /// # use near_vm_vm::{ChainableNamedResolver, NamedResolver};
    /// # fn chainable_test<A, B>(imports1: A, imports2: B)
    /// # where A: NamedResolver + Sized + Send + Sync,
    /// #       B: NamedResolver + Sized + Send + Sync,
    /// # {
    /// // override duplicates with imports from `imports2`
    /// imports1.chain_front(imports2);
    /// # }
    /// ```
    fn chain_front<U>(self, other: U) -> NamedResolverChain<U, Self>
    where
        U: NamedResolver + Send + Sync,
    {
        NamedResolverChain { a: other, b: self }
    }

    /// Chain a resolver behind the current resolver.
    ///
    /// This will cause the first resolver to override the second.
    ///
    /// ```
    /// # use near_vm_vm::{ChainableNamedResolver, NamedResolver};
    /// # fn chainable_test<A, B>(imports1: A, imports2: B)
    /// # where A: NamedResolver + Sized + Send + Sync,
    /// #       B: NamedResolver + Sized + Send + Sync,
    /// # {
    /// // override duplicates with imports from `imports1`
    /// imports1.chain_back(imports2);
    /// # }
    /// ```
    fn chain_back<U>(self, other: U) -> NamedResolverChain<Self, U>
    where
        U: NamedResolver + Send + Sync,
    {
        NamedResolverChain { a: self, b: other }
    }
}

// We give these chain methods to all types implementing NamedResolver
impl<T: NamedResolver + Send + Sync> ChainableNamedResolver for T {}

impl<A, B> NamedResolver for NamedResolverChain<A, B>
where
    A: NamedResolver + Send + Sync,
    B: NamedResolver + Send + Sync,
{
    fn resolve_by_name(&self, module: &str, field: &str) -> Option<Export> {
        self.a.resolve_by_name(module, field).or_else(|| self.b.resolve_by_name(module, field))
    }
}

impl<A, B> Clone for NamedResolverChain<A, B>
where
    A: NamedResolver + Clone + Send + Sync,
    B: NamedResolver + Clone + Send + Sync,
{
    fn clone(&self) -> Self {
        Self { a: self.a.clone(), b: self.b.clone() }
    }
}
