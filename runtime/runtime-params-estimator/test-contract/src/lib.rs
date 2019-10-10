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

/// Returns:
/// * `n: u64` -- how many times a certain operation should be repeated;
/// * `blob: &mut [u8]` -- the blob that was also read into the buffer.
#[inline]
fn read_u64input(buffer: &mut [u8]) -> (u64, &mut [u8]) {
    unsafe {
        input(0);
        let len = register_len(0);
        read_register(0, buffer.as_ptr() as u64);
        let mut data = [0u8; size_of::<u64>()];
        data.copy_from_slice(&buffer[..size_of::<u64>()]);
        (
            u64::from_le_bytes(data),
            buffer.split_at_mut(len as usize).0.split_at_mut(size_of::<u64>()).1,
        )
    }
}

#[inline]
fn return_u64(value: u64) {
    unsafe {
        value_return(size_of::<u64>() as u64, &value as *const u64 as u64);
    }
}

const BUFFER_SIZE: usize = 100_000;

/// Call fixture 10 times.
#[no_mangle]
pub fn call_fixture10() {
    for _ in 0..10 {
        let mut buffer = [0u8; BUFFER_SIZE];
        let (n, _) = read_u64input(&mut buffer);
        // Some useful stuff should be happening here.
        return_u64(n);
    }
}

/// Call `input` `n` times.
#[no_mangle]
pub fn call_input() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, _) = read_u64input(&mut buffer);
    for _ in 0..n {
        unsafe {
            input(0);
        }
    }
    return_u64(n);
}

/// Call `input`, `register_len` `n` times.
#[no_mangle]
pub fn call_input_register_len() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, _) = read_u64input(&mut buffer);
    for _ in 0..n {
        unsafe {
            input(0);
            register_len(0);
        }
    }
    return_u64(n);
}

/// Call `input`, `read_register` `n` times.
#[no_mangle]
pub fn call_input_read_register() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, _) = read_u64input(&mut buffer);
    for _ in 0..n {
        unsafe {
            input(0);
            read_register(0, buffer.as_ptr() as *const u64 as u64);
        }
    }
    return_u64(n);
}

macro_rules! call_func {
    ($exp_name:ident, $call:expr) => {
        #[no_mangle]
        pub fn $exp_name() {
            let mut buffer = [0u8; BUFFER_SIZE];
            let (n, _) = read_u64input(&mut buffer);
            for _ in 0..n {
                unsafe {
                    $call;
                }
            }
            return_u64(n);
        }
    };
}

// ###############
// # Context API #
// ###############
call_func!(call_current_account_id, current_account_id(0));
call_func!(call_signer_account_id, signer_account_id(0));
call_func!(call_signer_account_pk, signer_account_pk(0));
call_func!(call_predecessor_account_id, predecessor_account_id(0));
call_func!(call_block_index, block_index());
call_func!(call_storage_usage, storage_usage());

// #################
// # Economics API #
// #################
#[no_mangle]
pub fn call_account_balance() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, _) = read_u64input(&mut buffer);
    for _ in 0..n {
        unsafe {
            account_balance(buffer.as_ptr() as *const u64 as u64);
        }
    }
    return_u64(n);
}

#[no_mangle]
pub fn call_attached_deposit() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, _) = read_u64input(&mut buffer);
    for _ in 0..n {
        unsafe {
            attached_deposit(buffer.as_ptr() as *const u64 as u64);
        }
    }
    return_u64(n);
}
call_func!(call_prepaid_gas, prepaid_gas());
call_func!(call_used_gas, used_gas());

// ############
// # Math API #
// ############
call_func!(call_random_seed, random_seed(0));
#[no_mangle]
pub fn call_sha256() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for _ in 0..n {
        unsafe { sha256(blob.len() as _, blob.as_ptr() as _, 0) }
    }
    return_u64(n);
}

// #####################
// # Miscellaneous API #
// #####################
#[no_mangle]
pub fn call_value_return() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for _ in 0..n {
        unsafe { value_return(blob.len() as _, blob.as_ptr() as _) }
    }
    return_u64(n);
}

#[no_mangle]
pub fn call_log_utf8() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for _ in 0..n {
        let blob = if blob.len() < 200 { &blob } else { &blob[..200] };
        unsafe { log_utf8(blob.len() as _, blob.as_ptr() as _) }
    }
    return_u64(n);
}

#[no_mangle]
pub fn call_log_utf16() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for _ in 0..n {
        let blob = if blob.len() < 200 { &blob } else { &blob[..200] };
        unsafe { log_utf16(blob.len() as _, blob.as_ptr() as _) }
    }
    return_u64(n);
}

// ################
// # Promises API #
// ################

// Most of promises API is different in that it converts incoming blobs of data into Rust structures.
// We don't need to write tests for all of them, and can just test one and extrapolate cost to
// everything else.
#[no_mangle]
pub fn call_promise_batch_create() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for _ in 0..n {
        unsafe {
            promise_batch_create(blob.len() as _, blob.as_ptr() as _);
        }
    }
    return_u64(n);
}

#[no_mangle]
pub fn call_promise_batch_create_promise_batch_then() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for _ in 0..n {
        unsafe {
            let id = promise_batch_create(blob.len() as _, blob.as_ptr() as _);
            promise_batch_then(id, blob.len() as _, blob.as_ptr() as _);
        }
    }
    return_u64(n);
}

#[no_mangle]
pub fn call_promise_batch_create_promise_batch_action_create_account() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for _ in 0..n {
        unsafe {
            let acc_name = if blob.len() < 64 { &blob } else { &blob[..64] };
            let id = promise_batch_create(acc_name.len() as _, acc_name.as_ptr() as _);
            promise_batch_action_create_account(id);
        }
    }
    return_u64(n);
}

#[no_mangle]
pub fn call_promise_batch_create_promise_batch_action_create_account_batch_action_deploy_contract()
{
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for _ in 0..n {
        unsafe {
            let acc_name = if blob.len() < 64 { &blob } else { &blob[..64] };
            let id = promise_batch_create(acc_name.len() as _, acc_name.as_ptr() as _);
            promise_batch_action_create_account(id);
            promise_batch_action_deploy_contract(id, blob.len() as _, blob.as_ptr() as _);
        }
    }
    return_u64(n);
}

// #######################
// # Promise API results #
// #######################
call_func!(call_promise_results_count, promise_results_count());

#[no_mangle]
pub fn call_promise_batch_create_promise_return() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for _ in 0..n {
        unsafe {
            let id = promise_batch_create(blob.len() as _, blob.as_ptr() as _);
            promise_return(id);
        }
    }
    return_u64(n);
}

// ###############
// # Storage API #
// ###############

// We need to measure cost of operation for large&small blobs. Also, we need to measure the
// cost of operation for different depths of the trie.
#[no_mangle]
pub fn call_storage_write() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for i in 0..n {
        unsafe {
            // Modify blob so that we write different content.
            blob[0] = (i % 256) as u8;
            blob[1] = ((i / 256) % 256) as u8;
            storage_write(
                blob.len() as _,
                blob.as_ptr() as _,
                blob.len() as _,
                blob.as_ptr() as _,
                0,
            );
        }
    }
    return_u64(n);
}

#[no_mangle]
pub fn call_storage_read() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for i in 0..n {
        unsafe {
            // Modify blob so that we read different content.
            blob[0] = (i % 256) as u8;
            blob[1] = ((i / 256) % 256) as u8;
            storage_read(blob.len() as _, blob.as_ptr() as _, 0);
        }
    }
    return_u64(n);
}

#[no_mangle]
pub fn call_storage_remove() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for i in 0..n {
        unsafe {
            // Modify blob so that we remove different content.
            blob[0] = (i % 256) as u8;
            blob[1] = ((i / 256) % 256) as u8;
            storage_remove(blob.len() as _, blob.as_ptr() as _, 0);
        }
    }
    return_u64(n);
}

#[no_mangle]
pub fn call_storage_has_key() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for i in 0..n {
        unsafe {
            // Modify blob so that we remove different content.
            blob[0] = (i % 256) as u8;
            blob[1] = ((i / 256) % 256) as u8;
            storage_has_key(blob.len() as _, blob.as_ptr() as _);
        }
    }
    return_u64(n);
}

#[no_mangle]
pub fn call_storage_iter_prefix() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for i in 0..n {
        unsafe {
            storage_iter_prefix(blob.len() as _, blob.as_ptr() as _);
        }
    }
    return_u64(n);
}

#[no_mangle]
pub fn call_storage_iter_range() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);
    for i in 0..n {
        unsafe {
            storage_iter_range(
                blob.len() as _,
                blob.as_ptr() as _,
                blob.len() as _,
                blob.as_ptr() as _,
            );
        }
    }
    return_u64(n);
}

#[no_mangle]
pub fn call_storage_iter_next() {
    let mut buffer = [0u8; BUFFER_SIZE];
    let (n, blob) = read_u64input(&mut buffer);

    let end = [255u8, 255u8];
    unsafe {
        let mut id = storage_iter_range(
            blob.len() as _,
            blob.as_ptr() as _,
            end.len() as _,
            end.as_ptr() as _,
        );
        for i in 0..n {
            if storage_iter_next(id, 1, 2) == 0 {
                id = storage_iter_range(
                    blob.len() as _,
                    blob.as_ptr() as _,
                    end.len() as _,
                    end.as_ptr() as _,
                );
            }
        }
    }
    return_u64(n);
}
