#![no_std]
#![feature(core_intrinsics)]

#[panic_handler]
#[no_mangle]
pub fn panic(_info: &::core::panic::PanicInfo) -> ! {
    unsafe {
        ::core::intrinsics::abort();
    }
}

use core::mem::size_of;

#[allow(unused)]
extern "C" {
    // #############
    // # Registers #
    // #############
    fn read_register(register_id: u64, ptr: u64);
    fn register_len(register_id: u64) -> u64;
    // ###############
    // # Context API #
    // ###############
    fn current_account_id(register_id: u64);
    fn signer_account_id(register_id: u64);
    fn signer_account_pk(register_id: u64);
    fn predecessor_account_id(register_id: u64);
    fn input(register_id: u64);
    fn block_index() -> u64;
    fn storage_usage() -> u64;
    // #################
    // # Economics API #
    // #################
    fn account_balance(balance_ptr: u64);
    fn attached_deposit(balance_ptr: u64);
    fn prepaid_gas() -> u64;
    fn used_gas() -> u64;
    // ############
    // # Math API #
    // ############
    fn random_seed(register_id: u64);
    fn sha256(value_len: u64, value_ptr: u64, register_id: u64);
    // #####################
    // # Miscellaneous API #
    // #####################
    fn value_return(value_len: u64, value_ptr: u64);
    fn log_utf8(len: u64, ptr: u64);
    fn log_utf16(len: u64, ptr: u64);
    fn abort(msg_ptr: u32, filename_ptr: u32, line: u32, col: u32);
    // ################
    // # Promises API #
    // ################
    fn promise_create(
        account_id_len: u64,
        account_id_ptr: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
    ) -> u64;
    fn promise_then(
        promise_index: u64,
        account_id_len: u64,
        account_id_ptr: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
    ) -> u64;
    fn promise_and(promise_idx_ptr: u64, promise_idx_count: u64) -> u64;
    fn promise_batch_create(account_id_len: u64, account_id_ptr: u64) -> u64;
    fn promise_batch_then(promise_index: u64, account_id_len: u64, account_id_ptr: u64) -> u64;
    // #######################
    // # Promise API actions #
    // #######################
    fn promise_batch_action_create_account(promise_index: u64);
    fn promise_batch_action_deploy_contract(promise_index: u64, code_len: u64, code_ptr: u64);
    fn promise_batch_action_function_call(
        promise_index: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
    );
    fn promise_batch_action_transfer(promise_index: u64, amount_ptr: u64);
    fn promise_batch_action_stake(
        promise_index: u64,
        amount_ptr: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    );
    fn promise_batch_action_add_key_with_full_access(
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        nonce: u64,
    );
    fn promise_batch_action_add_key_with_function_call(
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        nonce: u64,
        allowance_ptr: u64,
        receiver_id_len: u64,
        receiver_id_ptr: u64,
        method_names_len: u64,
        method_names_ptr: u64,
    );
    fn promise_batch_action_delete_key(
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    );
    fn promise_batch_action_delete_account(
        promise_index: u64,
        beneficiary_id_len: u64,
        beneficiary_id_ptr: u64,
    );
    // #######################
    // # Promise API results #
    // #######################
    fn promise_results_count() -> u64;
    fn promise_result(result_idx: u64, register_id: u64) -> u64;
    fn promise_return(promise_id: u64);
    // ###############
    // # Storage API #
    // ###############
    fn storage_write(
        key_len: u64,
        key_ptr: u64,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> u64;
    fn storage_read(key_len: u64, key_ptr: u64, register_id: u64) -> u64;
    fn storage_remove(key_len: u64, key_ptr: u64, register_id: u64) -> u64;
    fn storage_has_key(key_len: u64, key_ptr: u64) -> u64;
    fn storage_iter_prefix(prefix_len: u64, prefix_ptr: u64) -> u64;
    fn storage_iter_range(start_len: u64, start_ptr: u64, end_len: u64, end_ptr: u64) -> u64;
    fn storage_iter_next(iterator_id: u64, key_register_id: u64, value_register_id: u64) -> u64;
}

/// This function is not doing anything useful, it is just here to make sure the payload is getting
/// compiled into Wasm.
#[cfg(feature = "small_payload")]
#[no_mangle]
pub fn payload() {
    let payload = include_bytes!("../res/small_payload");
    unsafe {
        value_return(1, payload.as_ptr() as *const u64 as u64);
    }
}

#[cfg(feature = "medium_payload")]
#[no_mangle]
pub fn payload() {
    let payload = include_bytes!("../res/medium_payload");
    unsafe {
        value_return(1, payload.as_ptr() as *const u64 as u64);
    }
}

#[cfg(feature = "large_payload")]
#[no_mangle]
pub fn payload() {
    let payload = include_bytes!("../res/large_payload");
    unsafe {
        value_return(1, payload.as_ptr() as *const u64 as u64);
    }
}

/// Function that does not do anything at all.
#[no_mangle]
pub fn noop() {}

/// Call input 1000 times.
#[no_mangle]
pub fn input_1k() {
    for _ in 0..1000 {
        unsafe {
            input(0);
        }
    }
}

/// Call input and register_len 1000 times.
#[no_mangle]
pub fn input_register_len_1k() {
    for _ in 0..1000 {
        unsafe {
            input(0);
            register_len(0);
        }
    }
}

/// Call input, read_register for up to 1M bytes 1000 times.
#[no_mangle]
pub fn input_read_register_1k() {
    let buffer = [0u8; 10240];
    for _ in 0..1000 {
        unsafe {
            input(0);
            read_register(0, buffer.as_ptr() as u64);
        }
    }
}

macro_rules! call_func_10k {
    ($exp_name:ident, $call:expr) => {
        #[no_mangle]
        pub fn $exp_name() {
            for _ in 0..10_000 {
                unsafe {
                    $call;
                }
            }
        }
    };
}

// ###############
// # Context API #
// ###############
call_func_10k!(current_account_id_10k, current_account_id(0));
call_func_10k!(signer_account_id_10k, signer_account_id(0));
call_func_10k!(signer_account_pk_10k, signer_account_pk(0));
call_func_10k!(predecessor_account_id_10k, predecessor_account_id(0));
call_func_10k!(block_index_10k, block_index());
call_func_10k!(storage_usage_10k, storage_usage());

// #################
// # Economics API #
// #################
#[no_mangle]
pub fn account_balance_10k() {
    let mut buffer = [0u8; core::mem::size_of::<u128>()];
    for _ in 0..10_000 {
        unsafe {
            account_balance(buffer.as_ptr() as *const u64 as u64);
        }
    }
}

#[no_mangle]
pub fn attached_deposit_10k() {
    let mut buffer = [0u8; core::mem::size_of::<u128>()];
    for _ in 0..10_000 {
        unsafe {
            attached_deposit(buffer.as_ptr() as *const u64 as u64);
        }
    }
}
call_func_10k!(prepaid_gas_10k, prepaid_gas());
call_func_10k!(used_gas_10k, used_gas());

// ############
// # Math API #
// ############
call_func_10k!(random_seed_10k, random_seed(0));

/// Read input and then call function on it.
macro_rules! read_call_func_10k {
    ($buf:ident, $len:ident, $exp_name:ident, $call:block) => {
        #[no_mangle]
        pub fn $exp_name() {
            unsafe {
                input(0);
                let $len = register_len(0);
                let mut $buf = [0u8; 10240];
                read_register(0, $buf.as_ptr() as u64);
                for _ in 0..10_000 {
                    $call
                }
            }
        }
    };
}
/// Read input and then sha256 it.
read_call_func_10k!(buf, len, sha256_10k, {
    sha256(len as _, buf.as_ptr() as _, 0);
});

// #####################
// # Miscellaneous API #
// #####################

/// Read input and then return it.
read_call_func_10k!(buf, len, value_return_10k, {
    value_return(len as _, buf.as_ptr() as _);
});
/// Read input and log it.
read_call_func_10k!(buf, len, log_utf8_10k, {
    log_utf8(len as _, buf.as_ptr() as _);
});
read_call_func_10k!(buf, len, log_utf16_10k, {
    log_utf16(len as _, buf.as_ptr() as _);
});

// ################
// # Promises API #
// ################

read_call_func_10k!(buf, len, promise_batch_create_10k, {
    promise_batch_create(10, buf.as_ptr() as _);
});

read_call_func_10k!(buf, len, promise_batch_create_then_10k, {
    let id = promise_batch_create(10, buf.as_ptr() as _);
    promise_batch_then(id, 10, buf.as_ptr() as _);
});

read_call_func_10k!(buf, len, promise_batch_create_account_10k, {
    let id = promise_batch_create(10, buf.as_ptr() as _);
    promise_batch_action_create_account(id);
});

read_call_func_10k!(buf, len, promise_batch_create_deploy_10k, {
    let id = promise_batch_create(10, buf.as_ptr() as _);
    promise_batch_action_create_account(id);
    promise_batch_action_deploy_contract(id, len as _, buf.as_ptr() as _);
});

// #######################
// # Promise API results #
// #######################
call_func_10k!(promise_results_count_10k, promise_results_count());

read_call_func_10k!(buf, len, promise_batch_create_return_10k, {
    let id = promise_batch_create(10, buf.as_ptr() as _);
    promise_return(id);
});

// ###############
// # Storage API #
// ###############

macro_rules! storage_100 {
    ($buf:ident, $len:ident, $exp_name:ident, $call1:block, $call2:block) => {
        #[no_mangle]
        pub fn $exp_name() {
            unsafe {
                input(0);
                let $len = register_len(0);
                let mut $buf = [0u8; 10240];
                read_register(0, $buf.as_ptr() as u64);
                for i in 0..100 {
                    // Modify blob so that we write different content.
                    $buf[0] = (i % 256) as u8;
                    $buf[1] = ((i / 256) % 256) as u8;
                    $buf[2] = ((i / 256 / 256) % 256) as u8;
                    $call1
                }

                for i in 0..100 {
                    $buf[0] = (i % 256) as u8;
                    $buf[1] = ((i / 256) % 256) as u8;
                    $buf[2] = ((i / 256 / 256) % 256) as u8;
                    $call2
                }
            }
        }
    };
}

storage_100!(
    buf,
    len,
    storage_write_100,
    {
        storage_write(len as _, buf.as_ptr() as _, len as _, buf.as_ptr() as _, 0);
    },
    {}
);

storage_100!(
    buf,
    len,
    storage_read_100,
    {
        storage_read(len as _, buf.as_ptr() as _, 0);
    },
    {}
);

storage_100!(
    buf,
    len,
    storage_remove_100,
    {
        storage_remove(len as _, buf.as_ptr() as _, 0);
    },
    {}
);

storage_100!(
    buf,
    len,
    storage_has_key_100,
    {
        storage_has_key(len as _, buf.as_ptr() as _);
    },
    {}
);

storage_100!(
    buf,
    len,
    storage_iter_prefix_100,
    {
        storage_iter_prefix(len, buf.as_ptr() as _);
    },
    {}
);

storage_100!(
    buf,
    len,
    storage_iter_range_100,
    {
        storage_iter_range(len, buf.as_ptr() as _, len, buf.as_ptr() as _);
    },
    {}
);

#[no_mangle]
pub fn storage_iter_next_1k() {
    unsafe {
        input(0);
        let len = register_len(0);
        let mut buf = [0u8; 10240];
        read_register(0, buf.as_ptr() as u64);
        for i in 0..1000 {
            buf[0] = (i % 256) as u8;
            buf[1] = ((i / 256) % 256) as u8;
            buf[2] = ((i / 256 / 256) % 256) as u8;
            storage_write(len as _, buf.as_ptr() as _, len as _, buf.as_ptr() as _, 0);
        }

        let start = [0u8, 0u8, 0u8];
        let end = [255u8, 255u8, 255u8];
        let mut id = storage_iter_range(
            start.len() as _,
            start.as_ptr() as _,
            end.len() as _,
            end.as_ptr() as _,
        );
        for _ in 0..1000 {
            if storage_iter_next(id, 1, 2) == 0 {
                id = storage_iter_range(
                    start.len() as _,
                    start.as_ptr() as _,
                    end.len() as _,
                    end.as_ptr() as _,
                );
            }
        }
    }
}

#[no_mangle]
pub fn cpu_ram_soak_test() {
    unsafe {
        input(0);
        let len = register_len(0) as usize;
        let mut buf = [0u8; 10240];
        read_register(0, buf.as_ptr() as u64);
        for i in 0..1_000_000 {
            let j = (i * 3) % len;
            let k = (i * 2 + len / 2) % len;
            let tmp = buf[k];
            buf[k] = buf[j];
            buf[j] = tmp;
        }
    }
}
