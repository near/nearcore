use near_vm_logic::VMLogic;

use std::ffi::c_void;

struct ImportReference(*mut c_void);
unsafe impl Send for ImportReference {}
unsafe impl Sync for ImportReference {}

// Wasm has only i32/i64 types, so Wasmtime 0.17 only accepts
// external functions taking i32/i64 type.
// Remove, once using version with https://github.com/bytecodealliance/wasmtime/issues/1829
// fixed. It doesn't affect correctness, as bit patterns are the same.
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
        ( $( $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >, )* ) => {
            pub mod wasmer_ext {
            use near_vm_logic::VMLogic;
            use wasmer_runtime::Ctx;
            type VMResult<T> = ::std::result::Result<T, near_vm_logic::VMLogicError>;
            $(
                #[allow(unused_parens)]
                pub fn $func( ctx: &mut Ctx, $( $arg_name: $arg_type ),* ) -> VMResult<($( $returns ),*)> {
                    let logic: &mut VMLogic<'_> = unsafe { &mut *(ctx.data as *mut VMLogic<'_>) };
                    logic.$func( $( $arg_name, )* )
                }
            )*
            }

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

            pub(crate) fn build_wasmer(memory: wasmer_runtime::memory::Memory, logic: &mut VMLogic<'_>) ->
                wasmer_runtime::ImportObject {
                let raw_ptr = logic as *mut _ as *mut c_void;
                let import_reference = ImportReference(raw_ptr);
                wasmer_runtime::imports! {
                    move || {
                        let dtor = (|_: *mut c_void| {}) as fn(*mut c_void);
                        (import_reference.0, dtor)
                    },
                    "env" => {
                        "memory" => memory,
                        $(
                            stringify!($func) => wasmer_runtime::func!(wasmer_ext::$func),
                        )*
                    },
                }
            }

            pub(crate) fn link_wasmtime(
                    linker: &mut wasmtime::Linker,
                    memory: wasmtime::Memory,
                    raw_logic: *mut c_void,
             ) {
                wasmtime_ext::CALLER_CONTEXT.with(|caller_context| {
                    unsafe {
                        *caller_context.get() = raw_logic
                    }
                });
                linker.define("env", "memory", memory).
                    expect("cannot define memory");
                $(
                   linker.func("env", stringify!($func), wasmtime_ext::$func).
                    expect("cannot link external");
                  )*
            }

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
}
