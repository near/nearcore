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
//! `VMConfig`. We can add new imports, but we must make sure that they
//! are only available to contracts at a specific protocol version -- we can't
//! make imports retroactively available to old transactions. So
//! `for_each_available_import` takes care to invoke `M!` only for currently
//! available imports.
#![cfg(any(
    feature = "wasmer0_vm",
    feature = "wasmer2_vm",
    feature = "near_vm",
    feature = "wasmtime_vm"
))]

macro_rules! call_with_name {
    ( $M:ident => @in $mod:ident : $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] > ) => {
        $M!($mod / $func : $func < [ $( $arg_name : $arg_type ),* ] -> [ $( $returns ),* ] >)
    };
    ( $M:ident => @as $name:ident : $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] > ) => {
        $M!(env / $name : $func < [ $( $arg_name : $arg_type ),* ] -> [ $( $returns ),* ] >)
    };
    ( $M:ident => $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] > ) => {
        $M!(env / $func : $func < [ $( $arg_name : $arg_type ),* ] -> [ $( $returns ),* ] >)
    };
}

macro_rules! imports {
    (
      $($(#[$config_field:ident])? $(##[$feature_name:literal])?
        $( @in $mod:ident : )?
        $( @as $name:ident : )?
        $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >,)*
    ) => {
        macro_rules! for_each_available_import {
            ($config:expr, $M:ident) => {$(
                $(#[cfg(feature = $feature_name)])?
                if true $(&& ($config).$config_field)? {
                    $crate::imports::call_with_name!($M => $( @in $mod : )? $( @as $name : )? $func < [ $( $arg_name : $arg_type ),* ] -> [ $( $returns ),* ] >);
                }
            )*}
        }
    }
}

imports! {
    // #########################
    // # Finite-wasm internals #
    // #########################
    @in internal: finite_wasm_gas<[gas: u64] -> []>,
    @in internal: finite_wasm_stack<[operand_size: u64, frame_size: u64] -> []>,
    @in internal: finite_wasm_unstack<[operand_size: u64, frame_size: u64] -> []>,
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
    #[ed25519_verify] ed25519_verify<[sig_len: u64,
        sig_ptr: u64,
        msg_len: u64,
        msg_ptr: u64,
        pub_key_len: u64,
        pub_key_ptr: u64
    ] -> [u64]>,
    #[math_extension] ripemd160<[value_len: u64, value_ptr: u64, register_id: u64] -> []>,
    #[math_extension] ecrecover<[hash_len: u64, hash_ptr: u64, sign_len: u64, sig_ptr: u64, v: u64, malleability_flag: u64, register_id: u64] -> [u64]>,
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
    #[function_call_weight] promise_batch_action_function_call_weight<[
        promise_index: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
        gas_weight: u64
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
    // # Promise API yield/resume #
    // #######################
    #[yield_resume_host_functions] promise_yield_create<[
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        gas: u64,
        gas_weight: u64,
        register_id: u64
    ] -> [u64]>,
    #[yield_resume_host_functions] promise_yield_resume<[
        data_id_len: u64,
        data_id_ptr: u64,
        payload_len: u64,
        payload_ptr: u64
    ] -> [u32]>,
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
    @as gas: gas_seen_from_wasm<[opcodes: u32] -> []>,
    // ###############
    // # Validator API #
    // ###############
    validator_stake<[account_id_len: u64, account_id_ptr: u64, stake_ptr: u64] -> []>,
    validator_total_stake<[stake_ptr: u64] -> []>,
    // #############
    // # Alt BN128 #
    // #############
    #[alt_bn128] alt_bn128_g1_multiexp<[value_len: u64, value_ptr: u64, register_id: u64] -> []>,
    #[alt_bn128] alt_bn128_g1_sum<[value_len: u64, value_ptr: u64, register_id: u64] -> []>,
    #[alt_bn128] alt_bn128_pairing_check<[value_len: u64, value_ptr: u64] -> [u64]>,
    // #############
    // #  Sandbox  #
    // #############
    ##["sandbox"] sandbox_debug_log<[len: u64, ptr: u64] -> []>,

    // Sleep for the given number of nanoseconds. This is the ultimate
    // undercharging function as it doesn't consume much gas or computes but
    // takes a lot of time to execute. It must always be gated behind a feature
    // flag and it must never be released to production.
    ##["test_features"] sleep_nanos<[duration: u64] -> []>,

    // Burn the given amount of gas. This is the ultimate overcharging function
    // as it doesn't take almost any resources to execute but burns a lot of
    // gas.
    ##["test_features"] burn_gas<[gas: u64] -> []>,
}

pub(crate) use {call_with_name, for_each_available_import};

pub(crate) const fn should_trace_host_function(host_function: &str) -> bool {
    match host_function {
        _ if str_eq(host_function, "gas") => false,
        _ if str_eq(host_function, "finite_wasm_gas") => false,
        _ => true,
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
