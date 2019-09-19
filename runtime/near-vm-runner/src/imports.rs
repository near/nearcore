use std::ffi::c_void;

use near_vm_logic::{HostError, VMLogic};
use wasmer_runtime::memory::Memory;
use wasmer_runtime::{func, imports, Ctx, ImportObject};

type Result<T> = ::std::result::Result<T, HostError>;

macro_rules! wrapped_imports {
        ( $( $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >, )* ) => {
            $(
                fn $func( ctx: &mut Ctx, $( $arg_name: $arg_type ),* ) -> Result<($( $returns ),*)> {
                    let logic: &mut VMLogic = unsafe { &mut *(ctx.data as *mut VMLogic) };
                    logic.$func( $( $arg_name, )* )
                }
            )*

            pub(crate) fn build(memory: Memory, raw_ptr: *mut c_void) -> ImportObject {
                let dtor = (|_: *mut c_void| {}) as fn(*mut c_void);
                imports! {
                    move || { (raw_ptr, dtor) },
                    "env" => {
                        "memory" => memory,
                        $(
                            stringify!($func) => func!($func),
                        )*
                    },
                }
            }
        }
    }

wrapped_imports! {
    // #############
    // # Registers #
    // #############
    read_register<[register_id: u64, ptr: u64] -> []>,
    register_len<[register_id: u64] -> [u64]>,
    // ###############
    // # Context API #
    // ###############
    current_account_id<[register_id: u64] -> []>,
    signer_account_id<[register_id: u64] -> []>,
    signer_account_pk<[register_id: u64] -> []>,
    predecessor_account_id<[register_id: u64] -> []>,
    input<[register_id: u64] -> []>,
    block_index<[] -> [u64]>,
    timestamp<[] -> [u64]>,
    storage_usage<[] -> [u64]>,
    // #################
    // # Economics API #
    // #################
    account_balance<[balance_ptr: u64] -> []>,
    attached_deposit<[balance_ptr: u64] -> []>,
    prepaid_gas<[] -> [u64]>,
    used_gas<[] -> [u64]>,
    // ############
    // # Math API #
    // ############
    random_seed<[register_id: u64] -> []>,
    sha256<[value_len: u64, value_ptr: u64, register_id: u64] -> []>,
    // #####################
    // # Miscellaneous API #
    // #####################
    value_return<[value_len: u64, value_ptr: u64] -> []>,
    panic<[] -> []>,
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
    promise_return<[promise_id: u64] -> []>,
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
}
