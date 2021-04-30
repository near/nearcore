use near_primitives::version::ProtocolVersion;
use near_vm_logic::VMLogic;

use std::ffi::c_void;

#[derive(Clone)]
pub struct ImportReference(pub *mut c_void);
unsafe impl Send for ImportReference {}
unsafe impl Sync for ImportReference {}

#[cfg(feature = "wasmer1_vm")]
use wasmer::{Memory, WasmerEnv};

#[derive(WasmerEnv, Clone)]
#[cfg(feature = "wasmer1_vm")]
pub struct NearWasmerEnv {
    pub memory: Memory,
    pub logic: ImportReference,
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

macro_rules! wrapped_imports {
        ( $($(#[$feature_name:tt, $feature:ident])* $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >, )* ) => {
            #[cfg(feature = "wasmer0_vm")]
            pub mod wasmer_ext {
                use near_vm_logic::VMLogic;
                use wasmer_runtime::Ctx;
                type VMResult<T> = ::std::result::Result<T, near_vm_logic::VMLogicError>;
                $(
                    #[allow(unused_parens)]
                    $(#[cfg(feature = $feature_name)])*
                    pub fn $func( ctx: &mut Ctx, $( $arg_name: $arg_type ),* ) -> VMResult<($( $returns ),*)> {
                        let logic: &mut VMLogic<'_> = unsafe { &mut *(ctx.data as *mut VMLogic<'_>) };
                        logic.$func( $( $arg_name, )* )
                    }
                )*
            }

            #[cfg(feature = "wasmer1_vm")]
            pub mod wasmer1_ext {
            use near_vm_logic::VMLogic;
            use crate::imports::NearWasmerEnv;

            type VMResult<T> = ::std::result::Result<T, near_vm_logic::VMLogicError>;
            $(
                #[allow(unused_parens)]
                $(#[cfg(feature = $feature_name)])*
                pub fn $func(env: &NearWasmerEnv, $( $arg_name: $arg_type ),* ) -> VMResult<($( $returns ),*)> {
                    let logic: &mut VMLogic = unsafe { &mut *(env.logic.0 as *mut VMLogic<'_>) };
                    logic.$func( $( $arg_name, )* )
                }
            )*
            }

            #[cfg(feature = "wasmtime_vm")]
            pub mod wasmtime_ext {
                use near_vm_logic::{VMLogic, VMLogicError};
                use std::ffi::c_void;
                use std::cell::{RefCell, UnsafeCell};
                use wasmtime::Trap;

                thread_local! {
                    pub static CALLER_CONTEXT: UnsafeCell<*mut c_void> = UnsafeCell::new(0 as *mut c_void);
                    pub static EMBEDDER_ERROR: RefCell<Option<VMLogicError>> = RefCell::new(None);
                }

                type VMResult<T> = ::std::result::Result<T, Trap>;
                $(
                    #[allow(unused_parens)]
                    #[cfg(all(feature = "wasmtime_vm" $(, feature = $feature_name)*))]
                    pub fn $func( $( $arg_name: rust2wasm!($arg_type) ),* ) -> VMResult<($( rust2wasm!($returns)),*)> {
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
                                Err(Trap::i32_exit(239))
                            }
                        }
                    }
                )*
            }

            #[allow(unused_variables)]
            #[cfg(feature = "wasmer0_vm")]
            pub(crate) fn build_wasmer(
                memory: wasmer_runtime::memory::Memory,
                logic: &mut VMLogic<'_>,
                protocol_version: ProtocolVersion,
            ) -> wasmer_runtime::ImportObject {
                let raw_ptr = logic as *mut _ as *mut c_void;
                let import_reference = ImportReference(raw_ptr);
                let mut import_object = wasmer_runtime::ImportObject::new_with_data(move || {
                    let dtor = (|_: *mut c_void| {}) as fn(*mut c_void);
                    (import_reference.0, dtor)
                });

                let mut ns = wasmer_runtime_core::import::Namespace::new();
                ns.insert("memory", memory);
                $({
                    $(#[cfg(feature = $feature_name)])*
                    if true $(&& near_primitives::checked_feature!($feature_name, $feature, protocol_version))* {
                        ns.insert(stringify!($func), wasmer_runtime::func!(wasmer_ext::$func));
                    }
                })*

                import_object.register("env", ns);
                import_object
            }

            #[allow(unused_variables)]
            #[cfg(feature = "wasmer1_vm")]
            pub(crate) fn build_wasmer1(
                store: &wasmer::Store,
                memory: wasmer::Memory,
                logic: &mut VMLogic<'_>,
                protocol_version: ProtocolVersion,
            ) -> wasmer::ImportObject {
                let env = NearWasmerEnv {logic: ImportReference(logic as * mut _ as * mut c_void), memory: memory.clone()};
                let mut import_object = wasmer::ImportObject::new();
                let mut namespace = wasmer::Exports::new();
                namespace.insert("memory", memory);
                $({
                    $(#[cfg(feature = $feature_name)])*
                    if true $(&& near_primitives::checked_feature!($feature_name, $feature, protocol_version))* {
                        namespace.insert(stringify!($func), wasmer::Function::new_native_with_env(&store, env.clone(), wasmer1_ext::$func));
                    }
                })*
                import_object.register("env", namespace);
                import_object
            }

            #[cfg(feature = "wasmtime_vm")]
            #[allow(unused_variables)]
            pub(crate) fn link_wasmtime(
                linker: &mut wasmtime::Linker,
                memory: wasmtime::Memory,
                raw_logic: *mut c_void,
                protocol_version: ProtocolVersion,
            ) {
                wasmtime_ext::CALLER_CONTEXT.with(|caller_context| {
                    unsafe {
                        *caller_context.get() = raw_logic
                    }
                });
                linker.define("env", "memory", memory).expect("cannot define memory");
                $({
                    $(#[cfg(feature = $feature_name)])*
                    if true $(&& near_primitives::checked_feature!($feature_name, $feature, protocol_version))* {
                        linker.func("env", stringify!($func), wasmtime_ext::$func).expect("cannot link external");
                    }
                })*
            }

            #[cfg(feature = "wasmtime_vm")]
            pub(crate) fn last_wasmtime_error() -> Option<near_vm_logic::VMLogicError> {
                wasmtime_ext::EMBEDDER_ERROR.with(|embedder_error| {
                   embedder_error.replace(None)
                })
            }
        }
    }

wrapped_imports! {
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
    #["protocol_feature_evm", EVM] ripemd160<[value_len: u64, value_ptr: u64, register_id: u64] -> []>,
    #["protocol_feature_evm", EVM] blake2b<[value_len: u64, value_ptr: u64, register_id: u64] -> []>,
    #["protocol_feature_evm", EVM] blake2b_f<[rounds_ptr: u64, h_ptr: u64, m_ptr: u64, t_ptr: u64, f_ptr: u64, register_id: u64] -> []>,
    #["protocol_feature_evm", EVM] ecrecover<[hash_ptr: u64, sig_ptr: u64, register_id: u64] -> []>,
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
