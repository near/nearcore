#![no_std]
#![feature(core_intrinsics)]
#![allow(non_snake_case)]

#[panic_handler]
#[no_mangle]
pub fn panic(_info: &::core::panic::PanicInfo) -> ! {
    unsafe {
        ::core::intrinsics::abort();
    }
}

#[allow(unused)]
extern "C" {
    // #############
    // # Registers #
    // #############
    fn read_register(register_id: u64, ptr: u64);
    fn register_len(register_id: u64) -> u64;
    fn write_register(register_id: u64, data_len: u64, data_ptr: u64);
    // ###############
    // # Context API #
    // ###############
    fn current_account_id(register_id: u64);
    fn signer_account_id(register_id: u64);
    fn signer_account_pk(register_id: u64);
    fn predecessor_account_id(register_id: u64);
    fn input(register_id: u64);
    // TODO #1903 fn block_height() -> u64;
    fn block_index() -> u64;
    fn storage_usage() -> u64;
    fn epoch_height() -> u64;
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
    fn keccak256(value_len: u64, value_ptr: u64, register_id: u64);
    fn keccak512(value_len: u64, value_ptr: u64, register_id: u64);
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
}

// This function is not doing anything useful, it is just here to make sure the payload is getting
// compiled into Wasm.
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

// Function that does not do anything at all.
#[no_mangle]
pub fn noop() {}

// Function that we use to measure `base` cost by calling `block_height` many times.
#[no_mangle]
pub unsafe fn base_1M() {
    for _ in 0..1_000_000 {
        block_index();
    }
}

// Function to measure `read_memory_base` and `read_memory_byte` many times.
// Reads 10b 10k times from memory.
#[no_mangle]
pub unsafe fn read_memory_10b_10k() {
    let buffer = [0u8; 10];
    for _ in 0..10_000 {
        value_return(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `read_memory_base` and `read_memory_byte` many times.
// Reads 1Mib 10k times from memory.
#[no_mangle]
pub unsafe fn read_memory_1Mib_10k() {
    let buffer = [0u8; 1024 * 1024];
    for _ in 0..10_000 {
        value_return(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `write_memory_base` and `write_memory_byte` many times.
// Writes 10b 10k times into memory. Includes `read_register` costs.
#[no_mangle]
pub unsafe fn write_memory_10b_10k() {
    let buffer = [0u8; 10];
    write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    for _ in 0..10_000 {
        read_register(0, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `write_memory_base` and `write_memory_byte` many times.
// Writes 1Mib 10k times into memory. Includes `read_register` costs.
#[no_mangle]
pub unsafe fn write_memory_1Mib_10k() {
    let buffer = [0u8; 1024 * 1024];
    write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    for _ in 0..10_000 {
        read_register(0, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `read_register_base` and `read_register_byte` many times.
// Reads 10b 10k times from register.
#[no_mangle]
pub unsafe fn read_register_10b_10k() {
    let buffer = [0u8; 10];
    write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    for _ in 0..10_000 {
        value_return(core::u64::MAX, 0);
    }
}

// Function to measure `read_register_base` and `read_register_byte` many times.
// Reads 1Mib 10k times from register.
#[no_mangle]
pub unsafe fn read_register_1Mib_10k() {
    let buffer = [0u8; 1024 * 1024];
    write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    for _ in 0..10_000 {
        value_return(core::u64::MAX, 0);
    }
}

// Function to measure `write_register_base` and `write_register_byte` many times.
// Writes 10b 10k times to register.
#[no_mangle]
pub unsafe fn write_register_10b_10k() {
    let buffer = [0u8; 10];
    for _ in 0..10_000 {
        write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `write_register_base` and `write_register_byte` many times.
// Writes 1Mib 10k times to register.
#[no_mangle]
pub unsafe fn write_register_1Mib_10k() {
    let buffer = [0u8; 1024 * 1024];
    for _ in 0..10_000 {
        write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `utf8_decoding_base`, `utf8_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf8 10b 10k times into log.
#[no_mangle]
pub unsafe fn utf8_log_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        log_utf8(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `utf8_decoding_base`, `utf8_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf8 10kib 1k times into log.
#[no_mangle]
pub unsafe fn utf8_log_10kib_10k() {
    let buffer = [65u8; 10240];
    for _ in 0..10_000 {
        log_utf8(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Nul-terminated versions.
// Function to measure `utf8_decoding_base`, `utf8_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf8 10b 10k times into log.
#[no_mangle]
pub unsafe fn nul_utf8_log_10b_10k() {
    let mut buffer = [65u8; 10];
    buffer[buffer.len() - 1] = 0;
    for _ in 0..10_000 {
        log_utf8(core::u64::MAX, buffer.as_ptr() as *const u64 as u64);
    }
}

// Nul-terminated versions.
// Function to measure `utf8_decoding_base`, `utf8_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf8 10kib 1k times into log.
#[no_mangle]
pub unsafe fn nul_utf8_log_10kib_10k() {
    let mut buffer = [65u8; 10240];
    buffer[buffer.len() - 1] = 0;
    for _ in 0..10_000 {
        log_utf8(core::u64::MAX, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `utf16_decoding_base`, `utf16_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf16 10b 10k times into log.
#[no_mangle]
pub unsafe fn utf16_log_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        log_utf16(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `utf16_decoding_base`, `utf16_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf16 10kib 1k times into log.
#[no_mangle]
pub unsafe fn utf16_log_10kib_10k() {
    let buffer = [65u8; 10240];
    for _ in 0..10_000 {
        log_utf16(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Nul-terminated versions.
// Function to measure `utf16_decoding_base`, `utf16_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf16 10b 10k times into log.
#[no_mangle]
pub unsafe fn nul_utf16_log_10b_10k() {
    let mut buffer = [65u8; 10];
    buffer[buffer.len() - 2] = 0;
    buffer[buffer.len() - 1] = 0;
    for _ in 0..10_000 {
        log_utf16(core::u64::MAX, buffer.as_ptr() as *const u64 as u64);
    }
}

// Nul-terminated versions.
// Function to measure `utf16_decoding_base`, `utf16_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf16 10kib 1k times into log.
#[no_mangle]
pub unsafe fn nul_utf16_log_10kib_10k() {
    let mut buffer = [65u8; 10240];
    buffer[buffer.len() - 2] = 0;
    buffer[buffer.len() - 1] = 0;
    for _ in 0..10_000 {
        log_utf16(core::u64::MAX, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `sha256_base` and `sha256_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `sha256` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute sha256 on 10b 10k times.
#[no_mangle]
pub unsafe fn sha256_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        sha256(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}
// Function to measure `sha256_base` and `sha256_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `sha256` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute sha256 on 10kib 10k times.
#[no_mangle]
pub unsafe fn sha256_10kib_10k() {
    let buffer = [65u8; 10240];
    for _ in 0..10_000 {
        sha256(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}

// Function to measure `keccak256_base` and `keccak256_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `keccak256` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute keccak256 on 10b 10k times.
#[no_mangle]
pub unsafe fn keccak256_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        keccak256(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}
// Function to measure `keccak256_base` and `keccak256_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `keccak256` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute keccak256 on 10kib 10k times.
#[no_mangle]
pub unsafe fn keccak256_10kib_10k() {
    let buffer = [65u8; 10240];
    for _ in 0..10_000 {
        keccak256(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}

// Function to measure `keccak512_base` and `keccak512_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `keccak512` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute keccak512 on 10b 10k times.
#[no_mangle]
pub unsafe fn keccak512_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        keccak512(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}
// Function to measure `keccak512_base` and `keccak512_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `keccak512` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute keccak512 on 10kib 10k times.
#[no_mangle]
pub unsafe fn keccak512_10kib_10k() {
    let buffer = [65u8; 10240];
    for _ in 0..10_000 {
        keccak512(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}

// ###############
// # Storage API #
// ###############

macro_rules! storage_bench {
    ($key_buf:ident, $key_len:expr, $value_buf:ident, $value_len:expr, $loop_n:expr, $exp_name:ident,  $call:block) => {
        #[no_mangle]
        pub unsafe fn $exp_name() {
            let mut $key_buf = [0u8; $key_len];
            let mut $value_buf = [0u8; $value_len];
            for i in 0..$loop_n {
                // Modify blob so that we write different content.
                $key_buf[0] = (i % 256) as u8;
                $key_buf[1] = ((i / 256) % 256) as u8;
                $key_buf[2] = ((i / 256 / 256) % 256) as u8;

                $value_buf[0] = (i % 256) as u8;
                $value_buf[1] = ((i / 256) % 256) as u8;
                $value_buf[2] = ((i / 256 / 256) % 256) as u8;
                $call
            }
        }
    };
}

// Storage writing.

// Function to measure `storage_write_base`.
// Writes to storage 1k times.
storage_bench!(key, 10, value, 10, 1000, storage_write_10b_key_10b_value_1k, {
    storage_write(10, key.as_ptr() as _, 10, value.as_ptr() as _, 0);
});

// Function to measure `storage_write_base + storage_write_key_byte`.
// Writes to storage with 10kib key 1000 times.
storage_bench!(key, 10240, value, 10, 1000, storage_write_10kib_key_10b_value_1k, {
    storage_write(10240, key.as_ptr() as _, 10, value.as_ptr() as _, 0);
});

// Function to measure `storage_write_base + storage_write_value_byte`.
// Writes to storage with 10kib value 1000 times.
storage_bench!(key, 10, value, 10240, 1000, storage_write_10b_key_10kib_value_1k, {
    storage_write(10, key.as_ptr() as _, 10240, value.as_ptr() as _, 0);
});

// Storage reading.

// Function to measure `storage_read_base`.
// Writes to storage 1k times.
storage_bench!(key, 10, value, 10, 1000, storage_read_10b_key_10b_value_1k, {
    storage_read(10, key.as_ptr() as _, 0);
});

// Function to measure `storage_read_base + storage_read_key_byte`.
// Writes to storage with 10kib key 1000 times.
storage_bench!(key, 10240, value, 10, 1000, storage_read_10kib_key_10b_value_1k, {
    storage_read(10240, key.as_ptr() as _, 0);
});

// Function to measure `storage_read_base + storage_read_value_byte`.
// Writes to storage with 10kib value 1000 times.
storage_bench!(key, 10, value, 10240, 1000, storage_read_10b_key_10kib_value_1k, {
    storage_read(10, key.as_ptr() as _, 0);
});

// Storage removing.

// Function to measure `storage_remove_base`.
// Writes to storage 1k times.
storage_bench!(key, 10, value, 10, 1000, storage_remove_10b_key_10b_value_1k, {
    storage_remove(10, key.as_ptr() as _, 0);
});

// Function to measure `storage_remove_base + storage_remove_key_byte`.
// Writes to storage with 10kib key 1000 times.
storage_bench!(key, 10240, value, 10, 1000, storage_remove_10kib_key_10b_value_1k, {
    storage_remove(10240, key.as_ptr() as _, 0);
});

// Function to measure `storage_remove_base + storage_remove_value_byte`.
// Writes to storage with 10kib value 1000 times.
storage_bench!(key, 10, value, 10240, 1000, storage_remove_10b_key_10kib_value_1k, {
    storage_remove(10, key.as_ptr() as _, 0);
});

// Storage has key.

// Function to measure `storage_has_key_base`.
// Writes to storage 1k times.
storage_bench!(key, 10, value, 10, 1000, storage_has_key_10b_key_10b_value_1k, {
    storage_has_key(10, key.as_ptr() as _);
});

// Function to measure `storage_has_key_base + storage_has_key_key_byte`.
// Writes to storage with 10kib key 1000 times.
storage_bench!(key, 10240, value, 10, 1000, storage_has_key_10kib_key_10b_value_1k, {
    storage_has_key(10240, key.as_ptr() as _);
});

// Function to measure `storage_has_key_base + storage_has_key_value_byte`.
// Writes to storage with 10kib value 1000 times.
storage_bench!(key, 10, value, 10240, 1000, storage_has_key_10b_key_10kib_value_1k, {
    storage_has_key(10, key.as_ptr() as _);
});

// Function to measure `promise_and_base`.
#[no_mangle]
pub unsafe fn promise_and_100k() {
    let account = b"alice_near";
    let id0 = promise_batch_create(account.len() as _, account.as_ptr() as _);
    let id1 = promise_batch_create(account.len() as _, account.as_ptr() as _);
    let ids = [id0, id1];
    for _ in 0..100_000 {
        promise_and(ids.as_ptr() as _, 2);
    }
}

// Function to measure `promise_and_per_promise`.
#[no_mangle]
pub unsafe fn promise_and_100k_on_1k_and() {
    let account = b"alice_near";
    let mut ids = [0u64; 1000];
    for i in 0..1000 {
        ids[i] = promise_batch_create(account.len() as _, account.as_ptr() as _);
    }
    for _ in 0..100_000 {
        promise_and(ids.as_ptr() as _, ids.len() as _);
    }
}

// Function to measure `promise_return`.
#[no_mangle]
pub unsafe fn promise_return_100k() {
    let account = b"alice_near";
    let id = promise_batch_create(account.len() as _, account.as_ptr() as _);
    for _ in 0..100_000 {
        promise_return(id);
    }
}

// Measuring cost for data_receipt_creation_config.

// Function that emits 10b of data.
#[no_mangle]
pub unsafe fn data_producer_10b() {
    let data = [0u8; 10];
    value_return(data.len() as _, data.as_ptr() as _);
}

// Function that emits 100kib of data.
#[no_mangle]
pub unsafe fn data_producer_100kib() {
    let data = [0u8; 102400];
    value_return(data.len() as _, data.as_ptr() as _);
}

// Function to measure `data_receipt_creation_config`, but we are measure send and execution fee at the same time.
// Produces 1000 10b data receipts.
#[no_mangle]
pub unsafe fn data_receipt_10b_1000() {
    let buf = [0u8; 1000];
    current_account_id(0);
    let buf_len = register_len(0);
    read_register(0, buf.as_ptr() as _);

    let method_name = b"data_producer_10b";
    let args = b"";
    let mut ids = [0u64; 1000];
    let amount = 0u128;
    let gas = prepaid_gas();
    for i in 0..1000 {
        ids[i] = promise_create(
            buf_len,
            buf.as_ptr() as _,
            method_name.len() as _,
            method_name.as_ptr() as _,
            args.len() as _,
            args.as_ptr() as _,
            &amount as *const u128 as *const u64 as u64,
            gas / 2000,
        );
    }
    let id = promise_and(ids.as_ptr() as _, ids.len() as _);
    let method_name = b"noop";
    promise_then(
        id,
        buf_len,
        buf.as_ptr() as _,
        method_name.len() as _,
        method_name.as_ptr() as _,
        args.len() as _,
        args.as_ptr() as _,
        &amount as *const u128 as *const u64 as u64,
        gas / 3,
    );
}

// Function to measure `data_receipt_creation_config`, but we are measure send and execution fee at the same time.
// Produces 1000 10kib data receipts.
#[no_mangle]
pub unsafe fn data_receipt_100kib_1000() {
    let buf = [0u8; 1000];
    current_account_id(0);
    let buf_len = register_len(0);
    read_register(0, buf.as_ptr() as _);

    let method_name = b"data_producer_100kib";
    let args = b"";
    let mut ids = [0u64; 1000];
    let amount = 0u128;
    let gas = prepaid_gas();
    for i in 0..1000 {
        ids[i] = promise_create(
            buf_len,
            buf.as_ptr() as _,
            method_name.len() as _,
            method_name.as_ptr() as _,
            args.len() as _,
            args.as_ptr() as _,
            &amount as *const u128 as *const u64 as u64,
            gas / 2000,
        );
    }
    let id = promise_and(ids.as_ptr() as _, ids.len() as _);
    let method_name = b"noop";
    promise_then(
        id,
        buf_len,
        buf.as_ptr() as _,
        method_name.len() as _,
        method_name.as_ptr() as _,
        args.len() as _,
        args.as_ptr() as _,
        &amount as *const u128 as *const u64 as u64,
        gas / 3,
    );
}

#[no_mangle]
pub unsafe fn cpu_ram_soak_test() {
    let mut buf = [0u8; 100 * 1024];
    let len = buf.len();
    for i in 0..10_000_000 {
        let j = (i * 7 + len / 2) % len;
        let k = (i * 3) % len;
        let tmp = buf[k];
        buf[k] = buf[j];
        buf[j] = tmp;
    }
}
