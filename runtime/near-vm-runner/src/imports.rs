//! Host function interface for smart contracts.
//!
//! Besides native WASM operations, smart contracts can call into runtime to
//! gain access to extra functionality, like operations with store. Such
//! "extras" are called "Host function", and play a role similar to syscalls. In
//! this module, we integrate host functions with various wasm runtimes we
//! support. The actual definitions of host functions live in the `vm-logic`
//! crate.
//!
//! Basically, what the following code does is (in pseudo-code):
//!
//! ```ignore
//! for host_fn in all_host_functions {
//!    wasm_imports.define("env", host_fn.name, |args| host_fn(args))
//! }
//! ```
//!
//! The actual implementation is a bit more complicated, for two reasons. First,
//! host functions have different signatures, so there isn't a trivial single
//! type one can use to hold a host function. Second, we want to use direct
//! calls in the compiled WASM, so we need to avoid dynamic dispatch and hand
//! functions as ZSTs to the WASM runtimes. This basically means that we need to
//! code the above for-loop as a macro.
//!
//! So, the `imports!` macro invocation is the main "public" API -- it just list
//! all host functions with their signatures. `imports! { foo, bar, baz }`
//! expands to roughly
//!
//! ```ignore
//! macro_rules! for_each_available_import {
//!    $($M:ident) => {
//!        $M!(foo);
//!        $M!(bar);
//!        $M!(baz);
//!    }
//! }
//! ```
//!
//! That is, `for_each_available_import` is a high-order macro which takes macro
//! `M` as a parameter, and calls `M!` with each import. Each supported WASM
//! runtime (see submodules of this module) then calls
//! `for_each_available_import` with its own import definition logic.
//!
//! The real `for_each_available_import` takes one more argument --
//! `protocol_version`. We can add new imports, but we must make sure that they
//! are only available to contracts at a specific protocol version -- we can't
//! make imports retroactively available to old transactions. So
//! `for_each_available_import` takes care to invoke `M!` only for currently
//! available imports.

macro_rules! imports {
    (
      $($(#[$stable_feature:ident])? $(#[$feature_name:literal, $feature:ident])*
        $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >,)*
    ) => {
        macro_rules! for_each_available_import {
            ($protocol_version:expr, $M:ident) => {$(
                $(#[cfg(feature = $feature_name)])*
                if true
                    $(&& near_primitives::checked_feature!($feature_name, $feature, $protocol_version))*
                    $(&& near_primitives::checked_feature!("stable", $stable_feature, $protocol_version))?
                {
                    $M!($func < [ $( $arg_name : $arg_type ),* ] -> [ $( $returns ),* ] >);
                }
            )*}
        }
    }
}

imports! {
    // #############
    // # Registers #
    // #############
    read_register<[register_id: u64, ptr: u64] -> []>,
    register_len<[register_id: u64] -> [u64]>,
    write_register<[register_id: u64, data_len: u64, data_ptr: u64] -> []>,
    // ###############
    // # Context API #
    // ###############
    current_account_id<[register_id: u64] -> []>,
    signer_account_id<[register_id: u64] -> []>,
    signer_account_pk<[register_id: u64] -> []>,
    predecessor_account_id<[register_id: u64] -> []>,
    input<[register_id: u64] -> []>,
    // TODO #1903 rename to `block_height`
    block_index<[] -> [u64]>,
    block_timestamp<[] -> [u64]>,
    epoch_height<[] -> [u64]>,
    storage_usage<[] -> [u64]>,
    // #################
    // # Economics API #
    // #################
    account_balance<[balance_ptr: u64] -> []>,
    account_locked_balance<[balance_ptr: u64] -> []>,
    attached_deposit<[balance_ptr: u64] -> []>,
    prepaid_gas<[] -> [u64]>,
    used_gas<[] -> [u64]>,
    // ############
    // # Math API #
    // ############
    random_seed<[register_id: u64] -> []>,
    sha256<[value_len: u64, value_ptr: u64, register_id: u64] -> []>,
    keccak256<[value_len: u64, value_ptr: u64, register_id: u64] -> []>,
    keccak512<[value_len: u64, value_ptr: u64, register_id: u64] -> []>,
    #[MathExtension] ripemd160<[value_len: u64, value_ptr: u64, register_id: u64] -> []>,
    #[MathExtension] ecrecover<[hash_len: u64, hash_ptr: u64, sign_len: u64, sig_ptr: u64, v: u64, malleability_flag: u64, register_id: u64] -> [u64]>,
    // #####################
    // # Miscellaneous API #
    // #####################
    value_return<[value_len: u64, value_ptr: u64] -> []>,
    panic<[] -> []>,
    panic_utf8<[len: u64, ptr: u64] -> []>,
    log_utf8<[len: u64, ptr: u64] -> []>,
    log_utf16<[len: u64, ptr: u64] -> []>,
    abort<[msg_ptr: u32, filename_ptr: u32, line: u32, col: u32] -> []>,
    // ################
    // # Promises API #
    // ################
    promise_create<[
        account_id_len: u64,
        account_id_ptr: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64
    ] -> [u64]>,
    promise_then<[
        promise_index: u64,
        account_id_len: u64,
        account_id_ptr: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64
    ] -> [u64]>,
    promise_and<[promise_idx_ptr: u64, promise_idx_count: u64] -> [u64]>,
    promise_batch_create<[account_id_len: u64, account_id_ptr: u64] -> [u64]>,
    promise_batch_then<[promise_index: u64, account_id_len: u64, account_id_ptr: u64] -> [u64]>,
    // #######################
    // # Promise API actions #
    // #######################
    promise_batch_action_create_account<[promise_index: u64] -> []>,
    promise_batch_action_deploy_contract<[promise_index: u64, code_len: u64, code_ptr: u64] -> []>,
    promise_batch_action_function_call<[
        promise_index: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64
    ] -> []>,
    promise_batch_action_transfer<[promise_index: u64, amount_ptr: u64] -> []>,
    promise_batch_action_stake<[
        promise_index: u64,
        amount_ptr: u64,
        public_key_len: u64,
        public_key_ptr: u64
    ] -> []>,
    promise_batch_action_add_key_with_full_access<[
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        nonce: u64
    ] -> []>,
    promise_batch_action_add_key_with_function_call<[
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        nonce: u64,
        allowance_ptr: u64,
        receiver_id_len: u64,
        receiver_id_ptr: u64,
        method_names_len: u64,
        method_names_ptr: u64
    ] -> []>,
    promise_batch_action_delete_key<[
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64
    ] -> []>,
    promise_batch_action_delete_account<[
        promise_index: u64,
        beneficiary_id_len: u64,
        beneficiary_id_ptr: u64
    ] -> []>,
    // #######################
    // # Promise API results #
    // #######################
    promise_results_count<[] -> [u64]>,
    promise_result<[result_idx: u64, register_id: u64] -> [u64]>,
    promise_return<[promise_idx: u64] -> []>,
    // ###############
    // # Storage API #
    // ###############
    storage_write<[key_len: u64, key_ptr: u64, value_len: u64, value_ptr: u64, register_id: u64] -> [u64]>,
    storage_read<[key_len: u64, key_ptr: u64, register_id: u64] -> [u64]>,
    storage_remove<[key_len: u64, key_ptr: u64, register_id: u64] -> [u64]>,
    storage_has_key<[key_len: u64, key_ptr: u64] -> [u64]>,
    storage_iter_prefix<[prefix_len: u64, prefix_ptr: u64] -> [u64]>,
    storage_iter_range<[start_len: u64, start_ptr: u64, end_len: u64, end_ptr: u64] -> [u64]>,
    storage_iter_next<[iterator_id: u64, key_register_id: u64, value_register_id: u64] -> [u64]>,
    // Function for the injected gas counter. Automatically called by the gas meter.
    gas<[gas_amount: u32] -> []>,
    // ###############
    // # Validator API #
    // ###############
    validator_stake<[account_id_len: u64, account_id_ptr: u64, stake_ptr: u64] -> []>,
    validator_total_stake<[stake_ptr: u64] -> []>,
    // #############
    // # Alt BN128 #
    // #############
    #["protocol_feature_alt_bn128", AltBn128] alt_bn128_g1_multiexp<[value_len: u64, value_ptr: u64, register_id: u64] -> []>,
    #["protocol_feature_alt_bn128", AltBn128] alt_bn128_g1_sum<[value_len: u64, value_ptr: u64, register_id: u64] -> []>,
    #["protocol_feature_alt_bn128", AltBn128] alt_bn128_pairing_check<[value_len: u64, value_ptr: u64] -> [u64]>,
}

#[cfg(feature = "wasmer0_vm")]
pub(crate) mod wasmer {
    use super::str_eq;
    use near_vm_logic::{ProtocolVersion, VMLogic, VMLogicError};
    use std::ffi::c_void;

    #[derive(Clone, Copy)]
    struct ImportReference(pub *mut c_void);
    unsafe impl Send for ImportReference {}
    unsafe impl Sync for ImportReference {}

    pub(crate) fn build(
        memory: wasmer_runtime::memory::Memory,
        logic: &mut VMLogic<'_>,
        protocol_version: ProtocolVersion,
    ) -> wasmer_runtime::ImportObject {
        let raw_ptr = logic as *mut _ as *mut c_void;
        let import_reference = ImportReference(raw_ptr);
        let mut import_object = wasmer_runtime::ImportObject::new_with_data(move || {
            let import_reference = import_reference;
            let dtor = (|_: *mut c_void| {}) as fn(*mut c_void);
            (import_reference.0, dtor)
        });

        let mut ns = wasmer_runtime_core::import::Namespace::new();
        ns.insert("memory", memory);

        macro_rules! add_import {
            (
              $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >
            ) => {
                #[allow(unused_parens)]
                fn $func( ctx: &mut wasmer_runtime::Ctx, $( $arg_name: $arg_type ),* ) -> Result<($( $returns ),*), VMLogicError> {
                    const IS_GAS: bool = str_eq(stringify!($func), "gas");
                    let _span = if IS_GAS {
                        None
                    } else {
                        Some(tracing::trace_span!(target: "host-function", stringify!($func)).entered())
                    };
                    let logic: &mut VMLogic<'_> = unsafe { &mut *(ctx.data as *mut VMLogic<'_>) };
                    logic.$func( $( $arg_name, )* )
                }

                ns.insert(stringify!($func), wasmer_runtime::func!($func));
            };
        }
        for_each_available_import!(protocol_version, add_import);

        import_object.register("env", ns);
        import_object
    }
}

#[cfg(feature = "wasmer2_vm")]
pub(crate) mod wasmer2 {
    use std::sync::Arc;

    use super::str_eq;
    use near_vm_logic::{ProtocolVersion, VMLogic};
    use wasmer_engine::{ExportFunction, ExportFunctionMetadata, Resolver};
    use wasmer_vm::{VMFunction, VMFunctionKind, VMMemory};

    pub(crate) struct Wasmer2Imports<'vmlogic, 'vmlogic_refs> {
        pub(crate) memory: VMMemory,
        // Note: this same object is also referenced by the `metadata` field!
        pub(crate) vmlogic: &'vmlogic mut VMLogic<'vmlogic_refs>,
        pub(crate) metadata: Arc<ExportFunctionMetadata>,
        pub(crate) protocol_version: ProtocolVersion,
    }

    trait Wasmer2Type {
        type Wasmer;
        fn to_wasmer(self) -> Self::Wasmer;
        fn ty() -> wasmer_types::Type;
    }
    macro_rules! wasmer_types {
        ($($native:ty as $wasmer:ty => $type_expr:expr;)*) => {
            $(impl Wasmer2Type for $native {
                type Wasmer = $wasmer;
                fn to_wasmer(self) -> $wasmer {
                    self as _
                }
                fn ty() -> wasmer_types::Type {
                    $type_expr
                }
            })*
        }
    }
    wasmer_types! {
        u32 as i32 => wasmer_types::Type::I32;
        u64 as i64 => wasmer_types::Type::I64;
    }

    macro_rules! return_ty {
        ($return_type: ident = [ ]) => {
            type $return_type = ();
            fn make_ret() -> () {}
        };
        ($return_type: ident = [ $($returns: ident),* ]) => {
            #[repr(C)]
            struct $return_type($(<$returns as Wasmer2Type>::Wasmer),*);
            fn make_ret($($returns: $returns),*) -> Ret { Ret($($returns.to_wasmer()),*) }
        }
    }

    impl<'vmlogic, 'vmlogic_refs> Resolver for Wasmer2Imports<'vmlogic, 'vmlogic_refs> {
        fn resolve(&self, _index: u32, module: &str, field: &str) -> Option<wasmer_engine::Export> {
            if module != "env" {
                return None;
            }
            if field == "memory" {
                return Some(wasmer_engine::Export::Memory(self.memory.clone()));
            }

            macro_rules! add_import {
                (
                  $func:ident <
                    [ $( $arg_name:ident : $arg_type:ident ),* ]
                    -> [ $( $returns:ident ),* ]
                  >
                ) => {
                    return_ty!(Ret = [ $($returns),* ]);

                    extern "C" fn $func(env: *mut VMLogic<'_>, $( $arg_name: $arg_type ),* )
                    -> Ret {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            const IS_GAS: bool = str_eq(stringify!($func), "gas");
                            let _span = if IS_GAS {
                                None
                            } else {
                                Some(tracing::trace_span!(
                                    target: "host-function",
                                    stringify!($func)
                                ).entered())
                            };

                            // SAFETY: This code should only be executable within `'vmlogic`
                            // lifetime and so it is safe to dereference the `env` pointer which is
                            // known to be derived from a valid `&'vmlogic mut VMLogic<'_>` in the
                            // first place.
                            unsafe { (*env).$func( $( $arg_name, )* ) }
                        }));
                        // We want to ensure that the only kind of error that host function calls
                        // return are VMLogicError. This is important because we later attempt to
                        // downcast the `RuntimeError`s into `VMLogicError`.
                        let result: Result<Result<_, near_vm_errors::VMLogicError>, _>  = result;
                        #[allow(unused_parens)]
                        match result {
                            Ok(Ok(($($returns),*))) => make_ret($($returns),*),
                            Ok(Err(trap)) => unsafe {
                                // SAFETY: this can only be called by a WASM contract, so all the
                                // necessary hooks are known to be in place.
                                wasmer_vm::raise_user_trap(Box::new(trap))
                            },
                            Err(e) => unsafe {
                                // SAFETY: this can only be called by a WASM contract, so all the
                                // necessary hooks are known to be in place.
                                wasmer_vm::resume_panic(e)
                            },
                        }
                    }
                    // TODO: a phf hashmap would probably work better here.
                    if field == stringify!($func) {
                        return Some(wasmer_engine::Export::Function(ExportFunction {
                            vm_function: VMFunction {
                                address: $func as *const _,
                                // SAFETY: here we erase the lifetime of the `vmlogic` reference,
                                // but we believe that the lifetimes on `Wasmer2Imports` enforce
                                // sufficiently that it isn't possible to call this exported
                                // function when vmlogic is no loger live.
                                vmctx: wasmer_vm::VMFunctionEnvironment {
                                    host_env: self.vmlogic as *const _ as *mut _
                                },
                                signature: wasmer_types::FunctionType::new([
                                    $(<$arg_type as Wasmer2Type>::ty()),*
                                ], [
                                    $(<$returns as Wasmer2Type>::ty()),*
                                ]),
                                kind: VMFunctionKind::Static,
                                call_trampoline: None,
                                instance_ref: None,
                            },
                            metadata: Some(Arc::clone(&self.metadata)),
                        }));
                    }
                };
            }
            for_each_available_import!(self.protocol_version, add_import);
            return None;
        }
    }

    pub(crate) fn build<'a, 'b>(
        memory: VMMemory,
        logic: &'a mut VMLogic<'b>,
        protocol_version: ProtocolVersion,
    ) -> Wasmer2Imports<'a, 'b> {
        let metadata = unsafe {
            // SAFETY: the functions here are thread-safe. We ensure that the lifetime of `VMLogic`
            // is sufficiently long by tying the lifetime of VMLogic to the return type which
            // contains this metadata.
            ExportFunctionMetadata::new(logic as *mut _ as *mut _, None, |ptr| ptr, |_| {})
        };
        Wasmer2Imports { memory, vmlogic: logic, metadata: Arc::new(metadata), protocol_version }
    }
}

#[cfg(feature = "wasmtime_vm")]
pub(crate) mod wasmtime {
    use super::str_eq;
    use near_vm_logic::{ProtocolVersion, VMLogic, VMLogicError};
    use std::cell::{RefCell, UnsafeCell};
    use std::ffi::c_void;

    thread_local! {
        static CALLER_CONTEXT: UnsafeCell<*mut c_void> = UnsafeCell::new(0 as *mut c_void);
        static EMBEDDER_ERROR: RefCell<Option<VMLogicError>> = RefCell::new(None);
    }

    // Wasm has only i32/i64 types, so Wasmtime 0.17 only accepts
    // external functions taking i32/i64 type.
    // Remove, once using version with https://github.com/bytecodealliance/wasmtime/issues/1829
    // fixed. It doesn't affect correctness, as bit patterns are the same.
    #[cfg(feature = "wasmtime_vm")]
    macro_rules! rust2wasm {
        (u64) => {
            i64
        };
        (u32) => {
            i32
        };
        ( () ) => {
            ()
        };
    }

    pub(crate) fn link(
        linker: &mut wasmtime::Linker,
        memory: wasmtime::Memory,
        raw_logic: *mut c_void,
        protocol_version: ProtocolVersion,
    ) {
        CALLER_CONTEXT.with(|caller_context| unsafe { *caller_context.get() = raw_logic });
        linker.define("env", "memory", memory).expect("cannot define memory");

        macro_rules! add_import {
            (
              $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >
            ) => {
                #[allow(unused_parens)]
                fn $func( $( $arg_name: rust2wasm!($arg_type) ),* ) -> Result<($( rust2wasm!($returns)),*), wasmtime::Trap> {
                    const IS_GAS: bool = str_eq(stringify!($func), "gas");
                    let _span = if IS_GAS {
                        None
                    } else {
                        Some(tracing::trace_span!(target: "host-function", stringify!($func)).entered())
                    };
                    let data = CALLER_CONTEXT.with(|caller_context| {
                        unsafe {
                            *caller_context.get()
                        }
                    });
                    let logic: &mut VMLogic<'_> = unsafe { &mut *(data as *mut VMLogic<'_>) };
                    match logic.$func( $( $arg_name as $arg_type, )* ) {
                        Ok(result) => Ok(result as ($( rust2wasm!($returns) ),* ) ),
                        Err(err) => {
                            // Wasmtime doesn't have proper mechanism for wrapping custom errors
                            // into traps. So, just store error into TLS and use special exit code here.
                            EMBEDDER_ERROR.with(|embedder_error| {
                                *embedder_error.borrow_mut() = Some(err)
                            });
                            Err(wasmtime::Trap::i32_exit(239))
                        }
                    }
                }

                linker.func("env", stringify!($func), $func).expect("cannot link external");
            };
        }
        for_each_available_import!(protocol_version, add_import);
    }

    pub(crate) fn last_error() -> Option<near_vm_logic::VMLogicError> {
        EMBEDDER_ERROR.with(|embedder_error| embedder_error.replace(None))
    }
}

/// Constant-time string equality, work-around for `"foo" == "bar"` not working
/// in const context yet.
const fn str_eq(s1: &str, s2: &str) -> bool {
    let s1 = s1.as_bytes();
    let s2 = s2.as_bytes();
    if s1.len() != s2.len() {
        return false;
    }
    let mut i = 0;
    while i < s1.len() {
        if s1[i] != s2[i] {
            return false;
        }
        i += 1;
    }
    true
}
