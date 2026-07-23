// cspell:ignore NDSAXO, kajdlfkjalkfjaklfjdkladjfkljadsk, utjz
// cspell:ignore noooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooop

#![no_std]
#![allow(non_snake_case)]
#![allow(clippy::all)]

#[panic_handler]
pub fn panic(_info: &::core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

mod sys {
    #[allow(unused)]
    extern "C" {
        pub fn read_register(register_id: u64, ptr: u64);
        pub fn account_balance(balance_ptr: u64);
        pub fn attached_deposit(balance_ptr: u64);
    }
}

unsafe fn read_register(register_id: u64, ptr: *mut u8) {
    sys::read_register(register_id, ptr as usize as u64)
}
#[allow(unused)]
unsafe fn account_balance(balance_ptr: *mut u8) {
    sys::account_balance(balance_ptr as usize as u64)
}
#[allow(unused)]
unsafe fn attached_deposit(balance_ptr: *mut u8) {
    sys::attached_deposit(balance_ptr as usize as u64)
}

#[allow(unused)]
extern "C" {
    // #############
    // # Registers #
    // #############
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
    fn block_index() -> u64;
    fn storage_usage() -> u64;
    fn epoch_height() -> u64;
    // #################
    // # Economics API #
    // #################
    fn prepaid_gas() -> u64;
    fn used_gas() -> u64;
    // ############
    // # Math API #
    // ############
    fn alt_bn128_g1_multiexp(value_len: u64, value_ptr: u64, register_id: u64);
    fn alt_bn128_g1_sum(value_len: u64, value_ptr: u64, register_id: u64);
    fn alt_bn128_pairing_check(value_len: u64, value_ptr: u64) -> u64;
    fn bls12381_p1_sum(value_len: u64, value_ptr: u64, register_id: u64) -> u64;
    fn bls12381_p2_sum(value_len: u64, value_ptr: u64, register_id: u64) -> u64;
    fn bls12381_g1_multiexp(value_len: u64, value_ptr: u64, register_id: u64) -> u64;
    fn bls12381_g2_multiexp(value_len: u64, value_ptr: u64, register_id: u64) -> u64;
    fn bls12381_map_fp_to_g1(value_len: u64, value_ptr: u64, register_id: u64) -> u64;
    fn bls12381_map_fp2_to_g2(value_len: u64, value_ptr: u64, register_id: u64) -> u64;
    fn bls12381_pairing_check(value_len: u64, value_ptr: u64) -> u64;
    fn bls12381_p1_decompress(value_len: u64, value_ptr: u64, register_id: u64) -> u64;
    fn bls12381_p2_decompress(value_len: u64, value_ptr: u64, register_id: u64) -> u64;

    fn random_seed(register_id: u64);
    fn sha256(value_len: u64, value_ptr: u64, register_id: u64);
    fn keccak256(value_len: u64, value_ptr: u64, register_id: u64);
    fn keccak512(value_len: u64, value_ptr: u64, register_id: u64);
    fn sha3_256(value_len: u64, value_ptr: u64, register_id: u64);
    fn sha3_384(value_len: u64, value_ptr: u64, register_id: u64);
    fn sha3_512(value_len: u64, value_ptr: u64, register_id: u64);
    fn ripemd160(value_len: u64, value_ptr: u64, register_id: u64);
    fn ecrecover(
        hash_len: u64,
        hash_ptr: u64,
        sig_len: u64,
        sig_ptr: u64,
        v: u64,
        malleability_flag: u64,
        register_id: u64,
    ) -> u64;
    fn ed25519_verify(
        sig_len: u64,
        sig_ptr: u64,
        msg_len: u64,
        msg_ptr: u64,
        pub_key_len: u64,
        pub_key_ptr: u64,
    ) -> u64;
    fn p256_verify(
        sig_len: u64,
        sig_ptr: u64,
        msg_len: u64,
        msg_ptr: u64,
        pub_key_len: u64,
        pub_key_ptr: u64,
    ) -> u64;
    fn ml_dsa_verify(
        sig_len: u64,
        sig_ptr: u64,
        msg_len: u64,
        msg_ptr: u64,
        pub_key_len: u64,
        pub_key_ptr: u64,
    ) -> u64;
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
    fn promise_yield_create(
        method_len: u64,
        method_ptr: u64,
        arg_len: u64,
        arg_ptr: u64,
        gas: u64,
        gas_weight: u64,
        data_id_reg: u64,
    ) -> u64;
    fn promise_yield_resume(
        data_id_len: u64,
        data_id_ptr: u64,
        payload_len: u64,
        payload_ptr: u64,
    ) -> u32;
    fn promise_yield_create_with_id(
        method_len: u64,
        method_ptr: u64,
        arg_len: u64,
        arg_ptr: u64,
        amount_ptr: u64,
        gas: u64,
        gas_weight: u64,
        yield_id_len: u64,
        yield_id_ptr: u64,
    ) -> u64;
    // #######################
    // # Promise API actions #
    // #######################
    fn promise_batch_action_create_account(promise_index: u64);
    fn promise_batch_action_deploy_contract(promise_index: u64, code_len: u64, code_ptr: u64);
    fn promise_batch_action_deploy_global_contract(
        promise_index: u64,
        code_len: u64,
        code_ptr: u64,
    );
    fn promise_batch_action_deploy_global_contract_by_account_id(
        promise_index: u64,
        code_len: u64,
        code_ptr: u64,
    );
    fn promise_batch_action_use_global_contract(
        promise_index: u64,
        code_hash_len: u64,
        code_hash_ptr: u64,
    );
    fn promise_batch_action_use_global_contract_by_account_id(
        promise_index: u64,
        account_id_len: u64,
        account_id_ptr: u64,
    );
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

// Function that does not do anything at all.
#[unsafe(no_mangle)]
pub fn noop() {}

// Function that does not do anything at all. With a shorter name.
#[unsafe(no_mangle)]
pub fn n() {}

// Function that does not do anything at all. With a 100 byte name.
#[unsafe(no_mangle)]
pub fn noooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooop(
) {
}

// Function that we use to measure `base` cost by calling `block_height` many times.
#[unsafe(no_mangle)]
pub unsafe fn base_1M() {
    for _ in 0..1_000_000 {
        block_index();
    }
}

// Function to measure `read_memory_base` and `read_memory_byte` many times.
// Reads 10b 10k times from memory.
#[unsafe(no_mangle)]
pub unsafe fn read_memory_10b_10k() {
    let buffer = [0u8; 10];
    for _ in 0..10_000 {
        value_return(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `read_memory_base` and `read_memory_byte` many times.
// Reads 1Mib 10k times from memory.
#[unsafe(no_mangle)]
pub unsafe fn read_memory_1Mib_10k() {
    let buffer = [0u8; 1024 * 1024];
    for _ in 0..10_000 {
        value_return(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `write_memory_base` and `write_memory_byte` many times.
// Writes 10b 10k times into memory. Includes `read_register` costs.
#[unsafe(no_mangle)]
pub unsafe fn write_memory_10b_10k() {
    let mut buffer = [0u8; 10];
    write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    for _ in 0..10_000 {
        read_register(0, buffer.as_mut_ptr());
    }
}

// Function to measure `write_memory_base` and `write_memory_byte` many times.
// Writes 1Mib 10k times into memory. Includes `read_register` costs.
#[unsafe(no_mangle)]
pub unsafe fn write_memory_1Mib_10k() {
    let mut buffer = [0u8; 1024 * 1024];
    write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    for _ in 0..10_000 {
        read_register(0, buffer.as_mut_ptr());
    }
}

// Function to measure `read_register_base` and `read_register_byte` many times.
// Reads 10b 10k times from register.
#[unsafe(no_mangle)]
pub unsafe fn read_register_10b_10k() {
    let buffer = [0u8; 10];
    write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    for _ in 0..10_000 {
        value_return(core::u64::MAX, 0);
    }
}

// Function to measure `read_register_base` and `read_register_byte` many times.
// Reads 1Mib 10k times from register.
#[unsafe(no_mangle)]
pub unsafe fn read_register_1Mib_10k() {
    let buffer = [0u8; 1024 * 1024];
    write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    for _ in 0..10_000 {
        value_return(core::u64::MAX, 0);
    }
}

// Function to measure `write_register_base` and `write_register_byte` many times.
// Writes 10b 10k times to register.
#[unsafe(no_mangle)]
pub unsafe fn write_register_10b_10k() {
    let buffer = [0u8; 10];
    for _ in 0..10_000 {
        write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `write_register_base` and `write_register_byte` many times.
// Writes 1Mib 10k times to register.
#[unsafe(no_mangle)]
pub unsafe fn write_register_1Mib_10k() {
    let buffer = [0u8; 1024 * 1024];
    for _ in 0..10_000 {
        write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `utf8_decoding_base`, `utf8_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf8 10b 10k times into log.
#[unsafe(no_mangle)]
pub unsafe fn utf8_log_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        log_utf8(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `utf8_decoding_base`, `utf8_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf8 10kib 1k times into log.
#[unsafe(no_mangle)]
pub unsafe fn utf8_log_10kib_1k() {
    let buffer = [65u8; 10240];
    for _ in 0..1_000 {
        log_utf8(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Nul-terminated versions.
// Function to measure `utf8_decoding_base`, `utf8_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf8 10b 10k times into log.
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
pub unsafe fn nul_utf8_log_10kib_1k() {
    let mut buffer = [65u8; 10240];
    buffer[buffer.len() - 1] = 0;
    for _ in 0..1_000 {
        log_utf8(core::u64::MAX, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `utf16_decoding_base`, `utf16_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf16 10b 10k times into log.
#[unsafe(no_mangle)]
pub unsafe fn utf16_log_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        log_utf16(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `utf16_decoding_base`, `utf16_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf16 10kib 1k times into log.
#[unsafe(no_mangle)]
pub unsafe fn utf16_log_10kib_1k() {
    let buffer = [65u8; 10240];
    for _ in 0..1_000 {
        log_utf16(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}

// Nul-terminated versions.
// Function to measure `utf16_decoding_base`, `utf16_decoding_byte`, `log_base`, and `log_byte`;
// It actually measures them together with `read_memory_base` and `read_memory_byte`.
// Write utf16 10b 10k times into log.
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
pub unsafe fn nul_utf16_log_10kib_1k() {
    let mut buffer = [65u8; 10240];
    buffer[buffer.len() - 2] = 0;
    buffer[buffer.len() - 1] = 0;
    for _ in 0..1_000 {
        log_utf16(core::u64::MAX, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `sha256_base` and `sha256_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `sha256` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute sha256 on 10b 10k times.
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
pub unsafe fn keccak512_10kib_10k() {
    let buffer = [65u8; 10240];
    for _ in 0..10_000 {
        keccak512(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}

// Function to measure `sha3_256_base` and `sha3_256_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `sha3_256` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute sha3_256 on 10b 10k times.
#[unsafe(no_mangle)]
pub unsafe fn sha3_256_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        sha3_256(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}
// Function to measure `sha3_256_base` and `sha3_256_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `sha3_256` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute sha3_256 on 10kib 10k times.
#[unsafe(no_mangle)]
pub unsafe fn sha3_256_10kib_10k() {
    let buffer = [65u8; 10240];
    for _ in 0..10_000 {
        sha3_256(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}

// Function to measure `sha3_384_base` and `sha3_384_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `sha3_384` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute sha3_384 on 10b 10k times.
#[unsafe(no_mangle)]
pub unsafe fn sha3_384_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        sha3_384(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}
// Function to measure `sha3_384_base` and `sha3_384_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `sha3_384` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute sha3_384 on 10kib 10k times.
#[unsafe(no_mangle)]
pub unsafe fn sha3_384_10kib_10k() {
    let buffer = [65u8; 10240];
    for _ in 0..10_000 {
        sha3_384(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}
// Function to measure `sha3_512_base` and `sha3_512_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `sha3_512` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute sha3_512 on 10b 10k times.
#[unsafe(no_mangle)]
pub unsafe fn sha3_512_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        sha3_512(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}
// Function to measure `sha3_512_base` and `sha3_512_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `sha3_512` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute sha3_512 on 10kib 10k times.
#[unsafe(no_mangle)]
pub unsafe fn sha3_512_10kib_10k() {
    let buffer = [65u8; 10240];
    for _ in 0..10_000 {
        sha3_512(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}

// Function to measure `ripemd160_base` and `ripemd160_block`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `ripemd160` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute ripemd160 on 10b 10k times.
#[unsafe(no_mangle)]
pub unsafe fn ripemd160_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        ripemd160(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}
// Function to measure `ripemd160_base` and `ripemd160_block`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `ripemd160` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute ripemd160 on 10kib 10k times.
#[unsafe(no_mangle)]
pub unsafe fn ripemd160_10kib_10k() {
    let buffer = [65u8; 10240];
    for _ in 0..10_000 {
        ripemd160(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}

// Function to measure `ecrecover_base`. Also measures `base`, `write_register_base`, and
// `write_register_byte`. However `ecrecover` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute ecrecover 10k times.
#[unsafe(no_mangle)]
pub unsafe fn ecrecover_10k() {
    let hash_buffer: [u8; 32] = [
        0x7d, 0xba, 0xf5, 0x58, 0xb0, 0xa1, 0xa5, 0xdc, 0x7a, 0x67, 0x20, 0x21, 0x17, 0xab, 0x14,
        0x3c, 0x1d, 0x86, 0x05, 0xa9, 0x83, 0xe4, 0xa7, 0x43, 0xbc, 0x06, 0xfc, 0xc0, 0x31, 0x62,
        0xdc, 0x0d,
    ];
    let sig_buffer: [u8; 64] = [
        0x5d, 0x99, 0xb6, 0xf7, 0xf6, 0xd1, 0xf7, 0x3d, 0x1a, 0x26, 0x49, 0x7f, 0x2b, 0x1c, 0x89,
        0xb2, 0x4c, 0x09, 0x93, 0x91, 0x3f, 0x86, 0xe9, 0xa2, 0xd0, 0x2c, 0xd6, 0x98, 0x87, 0xd9,
        0xc9, 0x4f, 0x3c, 0x88, 0x03, 0x58, 0x57, 0x9d, 0x81, 0x1b, 0x21, 0xdd, 0x1b, 0x7f, 0xd9,
        0xbb, 0x01, 0xc1, 0xd8, 0x1d, 0x10, 0xe6, 0x9f, 0x03, 0x84, 0xe6, 0x75, 0xc3, 0x2b, 0x39,
        0x64, 0x3b, 0xe8, 0x92,
    ];

    for _ in 0..10_000 {
        ecrecover(32, hash_buffer.as_ptr() as _, 64, sig_buffer.as_ptr() as _, 0, 0, 0);
    }
}

/// Function to measure `ed25519_verify_base`. Also measures `base`,
/// `write_register_base`, and `write_register_byte`. However
/// `ed25519_verify_base` computation is more expensive than register writing so
/// we are okay overcharging it.
#[unsafe(no_mangle)]
pub unsafe fn ed25519_verify_32b_500() {
    // private key: OReNDSAXOnl-U6Wki95ut01ehQW_9wcAF_utjzRNreg
    // public key: M4QwJx4Sogjr0KcMI_gsvt-lEU6tgd9GWmgejE_JYlA
    let public_key: [u8; 32] = [
        51, 132, 48, 39, 30, 18, 162, 8, 235, 208, 167, 12, 35, 248, 44, 190, 223, 165, 17, 78,
        173, 129, 223, 70, 90, 104, 30, 140, 79, 201, 98, 80,
    ];

    // 32 bytes message ("kajdlfkjalkfjaklfjdkladjfkljadsk")
    let message: [u8; 32] = [
        107, 97, 106, 100, 108, 102, 107, 106, 97, 108, 107, 102, 106, 97, 107, 108, 102, 106, 100,
        107, 108, 97, 100, 106, 102, 107, 108, 106, 97, 100, 115, 107,
    ];

    let signature: [u8; 64] = [
        149, 193, 241, 158, 225, 107, 146, 130, 116, 224, 233, 136, 232, 153, 211, 60, 115, 141,
        183, 174, 15, 52, 27, 186, 34, 68, 124, 158, 81, 3, 8, 76, 93, 28, 91, 68, 252, 151, 172,
        240, 129, 224, 239, 135, 26, 141, 111, 133, 134, 22, 149, 132, 90, 150, 33, 113, 191, 76,
        109, 64, 0, 13, 104, 6,
    ];

    for _ in 0..500 {
        let result = ed25519_verify(
            signature.len() as _,
            signature.as_ptr() as _,
            message.len() as _,
            message.as_ptr() as _,
            public_key.len() as _,
            public_key.as_ptr() as _,
        );
        // check that result was positive, as negative results could have exited
        // early and do not reflect the full cost.
        assert!(result == 1);
    }
}

/// Function to measure `ed25519_verify_bytes`.
#[unsafe(no_mangle)]
pub unsafe fn ed25519_verify_16kib_64() {
    // 16kB bytes message
    let message = [b'a'; 16384];

    // private key: OReNDSAXOnl-U6Wki95ut01ehQW_9wcAF_utjzRNreg
    // public key: M4QwJx4Sogjr0KcMI_gsvt-lEU6tgd9GWmgejE_JYlA
    let public_key: [u8; 32] = [
        51, 132, 48, 39, 30, 18, 162, 8, 235, 208, 167, 12, 35, 248, 44, 190, 223, 165, 17, 78,
        173, 129, 223, 70, 90, 104, 30, 140, 79, 201, 98, 80,
    ];

    let signature: [u8; 64] = [
        137, 224, 108, 168, 192, 229, 57, 250, 232, 231, 200, 55, 155, 62, 134, 111, 71, 124, 174,
        95, 190, 201, 113, 11, 86, 70, 91, 98, 228, 43, 233, 215, 135, 6, 42, 252, 28, 247, 101,
        57, 100, 50, 105, 41, 225, 221, 157, 121, 76, 28, 236, 247, 124, 228, 20, 203, 91, 18, 146,
        99, 254, 153, 41, 6,
    ];

    for _ in 0..64 {
        let result = ed25519_verify(
            signature.len() as _,
            signature.as_ptr() as _,
            message.len() as _,
            message.as_ptr() as _,
            public_key.len() as _,
            public_key.as_ptr() as _,
        );
        // check that result was positive, as negative results could have exited
        // early and do not reflect the full cost.
        assert!(result == 1);
    }
}

/// Function to measure `p256_verify_base`. Also measures `base`,
/// `write_register_base`, and `write_register_byte`. However
/// `p256_verify_base` computation is more expensive than register writing so
/// we are okay overcharging it.
#[unsafe(no_mangle)]
pub unsafe fn p256_verify_32b_500() {
    // P-256 compressed SEC1 public key (33 bytes) derived from the secret key
    // 0xc9afa9d845ba75166b5c215767b1d6934e50c3db36e89b127b8a622b120f6721.
    let public_key: [u8; 33] = [
        3, 96, 254, 212, 186, 37, 90, 157, 49, 201, 97, 235, 116, 198, 53, 109, 104, 192, 73, 184,
        146, 59, 97, 250, 108, 230, 105, 98, 46, 96, 242, 159, 182,
    ];

    // 32-byte prehash (all 0x07). The host does not hash — per NEP-635 the
    // caller supplies a prehashed digest — so this array is what gets signed
    // and verified directly.
    let message: [u8; 32] = [7; 32];

    // ECDSA signature (r || s, 64 bytes) over the prehash using the above key.
    let signature: [u8; 64] = [
        17, 55, 25, 23, 171, 17, 204, 202, 8, 236, 116, 68, 216, 160, 93, 172, 108, 37, 59, 25,
        207, 22, 60, 251, 117, 180, 8, 205, 210, 19, 79, 251, 80, 37, 55, 199, 202, 214, 102, 97,
        212, 16, 80, 5, 254, 23, 162, 155, 94, 39, 255, 35, 168, 31, 146, 54, 165, 72, 178, 237,
        166, 134, 29, 100,
    ];

    for _ in 0..500 {
        let result = p256_verify(
            signature.len() as _,
            signature.as_ptr() as _,
            message.len() as _,
            message.as_ptr() as _,
            public_key.len() as _,
            public_key.as_ptr() as _,
        );
        // check that result was positive, as negative results could have exited
        // early and do not reflect the full cost.
        assert!(result == 1);
    }
}

/// Function to measure `p256_verify_byte`.
#[unsafe(no_mangle)]
pub unsafe fn p256_verify_16kib_64() {
    // 16 KiB prehash input. The host does not hash, so the curve math only
    // sees the leading field-bytes of the input after `bits2field`
    // truncation; `p256_verify_byte` is priced to cover the host-side
    // marshalling of every byte of the declared input length.
    let message = [b'a'; 16384];

    // Same key pair as p256_verify_32b_500.
    let public_key: [u8; 33] = [
        3, 96, 254, 212, 186, 37, 90, 157, 49, 201, 97, 235, 116, 198, 53, 109, 104, 192, 73, 184,
        146, 59, 97, 250, 108, 230, 105, 98, 46, 96, 242, 159, 182,
    ];

    // Signature over the 16 KiB prehash using the same secret key.
    let signature: [u8; 64] = [
        19, 216, 85, 47, 78, 51, 197, 229, 248, 83, 63, 76, 66, 105, 230, 101, 15, 57, 161, 173,
        167, 74, 125, 119, 137, 44, 200, 17, 110, 202, 129, 11, 43, 11, 18, 12, 185, 66, 191, 147,
        75, 162, 163, 237, 197, 169, 172, 178, 92, 201, 144, 78, 232, 171, 46, 74, 221, 11, 63, 51,
        73, 241, 78, 180,
    ];

    for _ in 0..64 {
        let result = p256_verify(
            signature.len() as _,
            signature.as_ptr() as _,
            message.len() as _,
            message.as_ptr() as _,
            public_key.len() as _,
            public_key.as_ptr() as _,
        );
        // check that result was positive, as negative results could have exited
        // early and do not reflect the full cost.
        assert!(result == 1);
    }
}

/// Function to measure `ml_dsa_verify_base`. Also measures `base`,
/// `write_register_base`, and `write_register_byte`. However
/// `ml_dsa_verify_base` computation is more expensive than register writing so
/// we are okay overcharging it. The key pair and signatures are produced from
/// the seed "ml-dsa-host-fn-test"; ML-DSA verification is deterministic so the
/// embedded signature always verifies.
#[unsafe(no_mangle)]
pub unsafe fn ml_dsa_verify_32b_500() {
    let public_key: [u8; 1952] = [
        246, 25, 217, 164, 84, 170, 42, 252, 142, 239, 121, 156, 142, 63, 56, 217, 59, 48, 208,
        252, 157, 174, 217, 170, 142, 210, 100, 124, 23, 226, 25, 242, 193, 22, 104, 63, 248, 232,
        130, 144, 135, 218, 32, 14, 205, 223, 182, 144, 174, 45, 143, 49, 174, 40, 136, 156, 59, 2,
        188, 65, 23, 167, 249, 173, 38, 109, 59, 47, 134, 96, 155, 92, 159, 109, 36, 64, 164, 45,
        73, 232, 19, 83, 91, 69, 76, 237, 202, 6, 249, 253, 211, 5, 139, 88, 240, 116, 117, 171,
        245, 113, 201, 208, 77, 68, 106, 150, 39, 51, 224, 234, 191, 51, 35, 116, 138, 144, 253,
        237, 79, 121, 170, 125, 207, 209, 241, 166, 81, 119, 27, 109, 201, 0, 153, 118, 234, 151,
        30, 14, 188, 220, 80, 23, 177, 160, 133, 56, 123, 148, 217, 80, 244, 8, 4, 189, 129, 246,
        173, 23, 175, 6, 14, 183, 194, 73, 241, 136, 239, 67, 122, 164, 231, 171, 109, 227, 38,
        187, 149, 9, 156, 50, 121, 0, 15, 180, 141, 209, 55, 105, 160, 154, 235, 155, 202, 249,
        206, 128, 104, 149, 106, 173, 179, 217, 148, 57, 20, 92, 123, 158, 90, 235, 227, 192, 114,
        228, 71, 143, 180, 204, 238, 21, 181, 72, 2, 166, 136, 206, 187, 72, 244, 9, 187, 118, 211,
        89, 7, 195, 57, 189, 182, 121, 135, 100, 147, 196, 95, 95, 161, 107, 204, 84, 19, 98, 237,
        89, 162, 82, 20, 241, 20, 138, 121, 100, 44, 119, 206, 45, 191, 157, 51, 224, 91, 141, 28,
        175, 242, 254, 54, 56, 70, 6, 97, 237, 54, 224, 164, 153, 110, 21, 62, 248, 24, 229, 213,
        31, 9, 125, 106, 64, 178, 30, 203, 213, 115, 91, 126, 214, 240, 86, 97, 146, 94, 203, 236,
        111, 124, 137, 145, 230, 253, 25, 207, 48, 161, 211, 17, 158, 18, 206, 86, 68, 253, 71,
        234, 197, 148, 186, 251, 96, 36, 17, 228, 52, 46, 244, 221, 120, 168, 134, 217, 55, 251,
        228, 3, 80, 231, 18, 12, 39, 24, 251, 127, 190, 43, 28, 159, 183, 41, 170, 53, 191, 39,
        180, 21, 169, 49, 0, 169, 66, 160, 138, 188, 7, 226, 65, 211, 120, 142, 195, 111, 2, 2, 15,
        160, 30, 155, 244, 149, 23, 86, 40, 51, 131, 99, 144, 238, 45, 102, 221, 180, 164, 131, 75,
        84, 44, 59, 113, 44, 21, 232, 148, 255, 15, 196, 97, 129, 197, 170, 165, 218, 198, 98, 204,
        56, 42, 43, 7, 1, 30, 234, 60, 119, 14, 81, 101, 54, 7, 188, 82, 109, 197, 54, 4, 207, 222,
        121, 55, 74, 152, 214, 100, 247, 139, 141, 81, 13, 227, 13, 95, 203, 75, 246, 239, 199,
        118, 174, 158, 81, 159, 194, 32, 169, 200, 55, 234, 135, 224, 24, 87, 238, 196, 56, 177,
        243, 169, 132, 247, 56, 119, 116, 244, 115, 236, 51, 20, 111, 81, 161, 67, 246, 96, 92,
        195, 135, 110, 235, 139, 64, 52, 41, 129, 169, 170, 176, 17, 163, 245, 52, 232, 13, 48,
        195, 49, 252, 61, 90, 133, 169, 130, 10, 19, 51, 75, 48, 14, 226, 26, 255, 189, 40, 85,
        109, 71, 201, 186, 231, 176, 177, 137, 172, 58, 251, 91, 10, 207, 11, 60, 181, 65, 192, 64,
        240, 166, 90, 235, 20, 107, 65, 28, 40, 97, 113, 205, 235, 31, 136, 56, 35, 37, 130, 76,
        102, 70, 100, 157, 79, 244, 150, 59, 219, 146, 98, 49, 236, 214, 66, 18, 107, 224, 229,
        160, 254, 77, 199, 94, 212, 245, 3, 196, 5, 108, 40, 37, 88, 167, 78, 203, 49, 72, 237, 68,
        197, 181, 170, 195, 157, 170, 143, 171, 236, 230, 181, 45, 230, 138, 225, 207, 25, 100, 59,
        218, 144, 86, 225, 48, 184, 127, 145, 160, 13, 231, 139, 209, 121, 41, 212, 171, 28, 1,
        203, 194, 242, 219, 68, 82, 71, 185, 171, 55, 52, 164, 187, 218, 151, 8, 127, 52, 220, 248,
        222, 197, 175, 105, 28, 180, 227, 221, 40, 66, 98, 237, 19, 199, 22, 212, 97, 92, 56, 141,
        189, 68, 105, 185, 206, 64, 104, 152, 44, 168, 134, 8, 24, 122, 201, 160, 53, 162, 23, 100,
        54, 106, 226, 249, 240, 111, 184, 5, 160, 34, 37, 184, 193, 16, 190, 89, 216, 174, 67, 98,
        4, 160, 233, 81, 192, 1, 236, 96, 0, 92, 135, 119, 2, 100, 160, 59, 161, 72, 149, 255, 205,
        233, 240, 187, 134, 192, 107, 122, 3, 174, 85, 50, 121, 51, 133, 110, 8, 102, 34, 159, 132,
        246, 237, 72, 14, 81, 52, 67, 229, 171, 156, 203, 227, 193, 159, 104, 236, 18, 149, 113,
        199, 74, 40, 165, 62, 212, 30, 153, 36, 128, 28, 166, 247, 221, 9, 171, 194, 155, 200, 62,
        189, 169, 159, 99, 111, 132, 208, 252, 97, 245, 91, 98, 212, 162, 218, 154, 47, 27, 50,
        153, 156, 25, 180, 231, 151, 127, 136, 95, 205, 54, 0, 46, 102, 57, 45, 109, 239, 191, 200,
        218, 222, 97, 103, 64, 208, 172, 89, 1, 164, 85, 15, 104, 231, 91, 225, 123, 85, 49, 17,
        81, 46, 90, 154, 54, 208, 185, 113, 61, 60, 27, 59, 195, 19, 182, 218, 79, 144, 13, 217,
        71, 200, 10, 28, 44, 213, 184, 220, 219, 31, 69, 129, 70, 173, 5, 157, 45, 169, 20, 66,
        145, 208, 149, 153, 148, 200, 71, 83, 190, 105, 110, 196, 197, 246, 50, 237, 253, 39, 218,
        252, 4, 114, 208, 230, 197, 203, 124, 95, 79, 255, 42, 229, 34, 162, 143, 56, 120, 242,
        105, 245, 218, 105, 9, 250, 70, 109, 163, 18, 82, 191, 206, 252, 99, 65, 49, 193, 176, 49,
        17, 246, 185, 82, 167, 201, 182, 234, 123, 61, 47, 215, 235, 33, 188, 18, 105, 28, 94, 125,
        187, 106, 226, 171, 102, 156, 122, 114, 171, 15, 155, 62, 219, 254, 29, 198, 179, 52, 123,
        133, 76, 50, 87, 48, 229, 212, 35, 56, 183, 38, 48, 238, 169, 212, 60, 141, 5, 88, 216,
        107, 225, 255, 30, 196, 144, 43, 212, 80, 39, 229, 229, 160, 65, 162, 136, 66, 12, 229, 1,
        205, 87, 115, 202, 27, 102, 198, 91, 71, 31, 195, 165, 84, 254, 11, 69, 177, 196, 57, 157,
        62, 128, 213, 53, 86, 14, 1, 255, 107, 137, 53, 8, 222, 158, 66, 181, 59, 223, 121, 139,
        141, 233, 95, 5, 105, 159, 29, 205, 130, 150, 10, 91, 234, 196, 145, 79, 221, 39, 229, 182,
        77, 245, 234, 247, 128, 30, 100, 158, 139, 14, 14, 23, 210, 1, 119, 195, 83, 150, 17, 62,
        27, 99, 191, 253, 71, 186, 192, 56, 142, 65, 17, 113, 39, 186, 170, 48, 101, 94, 102, 60,
        203, 38, 67, 246, 79, 211, 186, 216, 114, 92, 210, 0, 0, 205, 232, 146, 111, 173, 191, 245,
        234, 27, 163, 89, 206, 42, 133, 47, 16, 203, 6, 212, 144, 250, 109, 168, 89, 97, 56, 156,
        111, 84, 251, 104, 93, 46, 165, 126, 11, 196, 131, 250, 225, 227, 152, 239, 153, 21, 81,
        56, 52, 210, 220, 226, 71, 175, 48, 96, 228, 83, 222, 31, 4, 80, 77, 115, 215, 205, 150,
        238, 145, 106, 234, 160, 255, 68, 200, 58, 29, 148, 29, 165, 151, 13, 132, 179, 120, 140,
        20, 17, 231, 1, 82, 1, 78, 189, 224, 231, 146, 118, 231, 138, 238, 106, 66, 155, 229, 75,
        176, 175, 16, 253, 15, 156, 167, 14, 60, 19, 15, 38, 222, 76, 199, 68, 121, 113, 36, 35,
        181, 171, 45, 174, 204, 14, 240, 140, 233, 237, 82, 109, 49, 230, 90, 219, 79, 173, 27,
        200, 241, 56, 5, 5, 246, 88, 127, 158, 18, 188, 74, 204, 98, 196, 202, 17, 169, 236, 20,
        222, 219, 58, 186, 252, 117, 32, 159, 158, 100, 9, 13, 12, 148, 231, 4, 230, 247, 39, 27,
        233, 96, 18, 39, 37, 203, 233, 37, 246, 180, 23, 227, 99, 76, 30, 60, 92, 197, 150, 254,
        93, 214, 184, 142, 212, 134, 179, 30, 27, 239, 47, 16, 119, 162, 19, 153, 137, 143, 135,
        74, 15, 129, 54, 198, 239, 248, 8, 223, 28, 195, 121, 202, 243, 222, 128, 0, 114, 28, 148,
        189, 158, 2, 192, 21, 74, 113, 226, 83, 120, 195, 178, 138, 26, 126, 187, 99, 202, 129, 67,
        173, 117, 225, 16, 179, 60, 60, 97, 197, 133, 30, 169, 71, 65, 186, 131, 83, 201, 0, 62,
        179, 44, 20, 140, 249, 153, 22, 74, 189, 205, 20, 228, 133, 96, 44, 164, 192, 249, 56, 187,
        105, 183, 46, 67, 28, 142, 100, 47, 193, 211, 201, 28, 112, 245, 55, 193, 11, 45, 84, 81,
        142, 47, 90, 149, 221, 34, 110, 14, 125, 45, 246, 206, 62, 245, 190, 111, 48, 154, 107,
        103, 211, 16, 247, 31, 134, 30, 151, 10, 89, 189, 115, 24, 116, 89, 57, 189, 111, 156, 23,
        153, 59, 94, 5, 6, 37, 14, 134, 136, 252, 64, 236, 13, 49, 191, 232, 246, 185, 36, 163, 58,
        43, 167, 22, 123, 136, 127, 50, 215, 238, 155, 21, 21, 79, 192, 46, 71, 196, 229, 154, 190,
        105, 217, 183, 208, 179, 83, 147, 253, 129, 91, 0, 157, 117, 253, 105, 55, 60, 127, 221,
        122, 62, 138, 230, 113, 118, 130, 220, 217, 201, 255, 152, 15, 86, 213, 126, 211, 44, 7,
        183, 5, 250, 152, 135, 92, 221, 196, 246, 52, 33, 143, 92, 22, 61, 177, 95, 29, 164, 36,
        117, 138, 125, 166, 13, 149, 50, 177, 51, 119, 92, 50, 141, 225, 240, 48, 213, 151, 46, 75,
        178, 138, 100, 197, 165, 159, 114, 199, 126, 5, 244, 157, 3, 90, 146, 25, 186, 18, 15, 177,
        35, 247, 174, 37, 164, 172, 29, 253, 192, 27, 92, 110, 118, 219, 79, 26, 197, 61, 139, 171,
        53, 20, 78, 203, 160, 232, 169, 37, 152, 159, 16, 59, 156, 70, 31, 227, 144, 90, 123, 141,
        143, 100, 210, 222, 24, 52, 243, 144, 134, 165, 222, 116, 133, 78, 151, 181, 162, 184, 160,
        123, 247, 111, 180, 231, 84, 127, 167, 153, 171, 100, 32, 182, 143, 87, 103, 204, 79, 192,
        163, 134, 142, 83, 190, 253, 13, 6, 58, 250, 43, 73, 208, 105, 68, 223, 162, 76, 31, 217,
        147, 31, 238, 252, 61, 148, 211, 253, 101, 195, 36, 208, 72, 153, 113, 70, 148, 47, 149,
        69, 54, 138, 214, 181, 111, 59, 235, 216, 21, 38, 16, 196, 44, 39, 150, 125, 238, 148, 65,
        239, 10, 62, 99, 173, 214, 1, 33, 244, 0, 33, 137, 238, 35, 58, 196, 88, 163, 217, 11, 250,
        135, 183, 191, 126, 197, 90, 184, 85, 187, 4, 179, 61, 54, 58, 101, 85, 55, 199, 171, 185,
        73, 148, 166, 132, 151, 51, 148, 148, 5, 191, 156, 160, 47, 221, 106, 173, 138, 13, 144,
        27, 111, 171, 22, 107, 24, 232, 243, 209, 14, 140, 153, 126, 58, 6, 56, 204, 38, 31, 162,
        186, 193, 230, 99, 188, 30, 109, 102, 168, 153, 141, 20, 25, 181, 137, 242, 93, 230, 197,
        31, 131, 5, 184, 222, 145, 129, 64, 137, 122, 127, 6, 230, 134, 234, 26, 114, 70, 77, 182,
        5, 110, 106, 64, 75, 126, 101, 27, 29, 231, 211, 165, 90, 89, 136, 68, 246, 91,
    ];

    // 32-byte message (all 0x07). ML-DSA hashes internally; the host does not
    // pre-hash, so this is signed and verified directly.
    let message: [u8; 32] = [7; 32];

    let signature: [u8; 3309] = [
        45, 143, 216, 129, 147, 240, 8, 38, 52, 76, 138, 138, 235, 76, 155, 150, 91, 7, 202, 94,
        83, 190, 1, 95, 129, 251, 157, 142, 51, 171, 81, 121, 190, 206, 127, 83, 0, 207, 110, 77,
        248, 105, 98, 106, 190, 226, 115, 64, 210, 129, 218, 186, 27, 66, 201, 66, 37, 145, 253,
        72, 209, 182, 231, 17, 96, 95, 58, 87, 238, 61, 196, 98, 228, 31, 149, 236, 130, 218, 177,
        99, 75, 152, 210, 164, 15, 131, 95, 214, 33, 166, 254, 183, 220, 107, 157, 89, 103, 235,
        53, 117, 200, 35, 177, 33, 171, 138, 31, 173, 169, 251, 124, 107, 68, 100, 129, 70, 113,
        66, 201, 150, 154, 39, 138, 7, 203, 55, 26, 32, 91, 106, 94, 13, 172, 217, 148, 199, 157,
        71, 41, 27, 237, 38, 250, 86, 177, 254, 87, 25, 162, 54, 233, 183, 9, 66, 241, 150, 240,
        24, 120, 96, 31, 248, 139, 171, 105, 63, 204, 0, 20, 227, 158, 163, 208, 61, 205, 196, 157,
        213, 21, 126, 238, 255, 40, 193, 47, 177, 110, 224, 253, 198, 234, 176, 205, 5, 214, 202,
        147, 209, 180, 198, 7, 140, 109, 33, 230, 2, 217, 216, 175, 28, 83, 84, 127, 229, 6, 20,
        91, 189, 218, 105, 163, 73, 125, 152, 0, 83, 11, 13, 100, 2, 26, 152, 241, 50, 58, 153,
        246, 5, 49, 118, 233, 174, 249, 86, 3, 98, 142, 102, 210, 160, 242, 47, 185, 130, 173, 18,
        239, 162, 154, 146, 76, 44, 16, 188, 164, 123, 209, 85, 230, 187, 110, 189, 161, 29, 187,
        125, 14, 222, 248, 95, 159, 108, 21, 242, 72, 154, 10, 197, 52, 104, 83, 235, 180, 233, 70,
        180, 91, 15, 214, 172, 59, 74, 225, 138, 179, 78, 236, 41, 183, 169, 101, 108, 239, 171,
        119, 174, 217, 98, 42, 185, 234, 53, 185, 56, 202, 25, 170, 53, 110, 154, 34, 127, 253,
        170, 161, 188, 226, 196, 72, 153, 156, 137, 194, 164, 173, 37, 199, 69, 92, 100, 31, 55,
        97, 218, 125, 124, 10, 200, 22, 123, 67, 136, 219, 145, 173, 147, 76, 200, 102, 176, 76,
        213, 221, 1, 172, 60, 191, 87, 75, 66, 133, 33, 192, 158, 83, 175, 14, 38, 82, 31, 46, 37,
        7, 131, 190, 25, 151, 164, 89, 66, 192, 21, 161, 235, 40, 129, 247, 145, 23, 59, 163, 78,
        238, 217, 246, 110, 43, 234, 69, 187, 133, 208, 212, 107, 167, 44, 74, 240, 33, 165, 37,
        238, 214, 153, 244, 3, 98, 73, 128, 7, 57, 3, 176, 189, 46, 38, 90, 192, 166, 95, 23, 237,
        112, 170, 241, 173, 231, 241, 249, 226, 100, 192, 29, 155, 105, 242, 117, 158, 57, 64, 243,
        194, 24, 208, 122, 187, 251, 140, 182, 73, 197, 216, 9, 79, 36, 214, 116, 94, 135, 146,
        194, 71, 253, 197, 183, 153, 145, 143, 191, 227, 34, 150, 120, 179, 135, 0, 27, 236, 94,
        239, 41, 228, 95, 251, 255, 119, 5, 116, 113, 184, 98, 198, 83, 206, 13, 254, 18, 117, 74,
        236, 207, 13, 183, 116, 252, 13, 164, 218, 110, 77, 106, 207, 137, 231, 21, 176, 53, 225,
        13, 33, 65, 108, 158, 227, 66, 13, 194, 16, 175, 33, 14, 148, 152, 211, 149, 213, 103, 15,
        172, 83, 238, 236, 114, 147, 94, 196, 194, 106, 63, 99, 90, 112, 161, 249, 100, 55, 49,
        215, 204, 123, 110, 216, 28, 66, 232, 146, 117, 24, 95, 136, 102, 22, 71, 68, 54, 143, 51,
        238, 151, 248, 4, 56, 60, 80, 132, 151, 158, 172, 29, 143, 160, 243, 45, 210, 66, 250, 246,
        112, 27, 185, 233, 10, 48, 124, 18, 130, 189, 54, 224, 103, 217, 122, 174, 45, 89, 212,
        134, 111, 107, 60, 10, 158, 143, 214, 104, 242, 70, 166, 164, 207, 226, 148, 226, 137, 79,
        226, 226, 76, 43, 203, 156, 67, 100, 202, 135, 1, 84, 61, 140, 162, 128, 202, 164, 31, 214,
        195, 47, 99, 80, 20, 187, 255, 232, 237, 221, 30, 128, 55, 163, 44, 27, 145, 85, 67, 203,
        253, 68, 49, 185, 103, 66, 22, 181, 101, 97, 209, 91, 78, 4, 214, 245, 144, 197, 79, 172,
        82, 32, 8, 199, 208, 200, 201, 153, 165, 187, 194, 210, 98, 77, 187, 97, 105, 84, 74, 96,
        71, 60, 98, 122, 131, 162, 89, 17, 227, 126, 248, 186, 3, 176, 231, 225, 115, 154, 88, 172,
        187, 176, 250, 162, 68, 29, 129, 227, 188, 179, 215, 23, 84, 118, 229, 150, 65, 245, 179,
        160, 157, 64, 161, 237, 0, 75, 101, 236, 101, 51, 99, 127, 0, 220, 234, 219, 128, 214, 228,
        6, 148, 121, 60, 228, 108, 149, 119, 173, 46, 127, 144, 93, 101, 119, 167, 224, 58, 199,
        113, 203, 247, 124, 177, 122, 21, 101, 185, 148, 115, 91, 164, 13, 206, 56, 139, 152, 139,
        17, 75, 159, 28, 119, 208, 249, 207, 152, 93, 226, 164, 18, 224, 149, 22, 248, 4, 45, 33,
        219, 151, 102, 5, 9, 138, 168, 211, 226, 56, 7, 17, 228, 221, 104, 69, 214, 137, 92, 184,
        142, 106, 80, 205, 241, 66, 163, 80, 148, 128, 135, 21, 89, 231, 73, 254, 170, 100, 29,
        188, 235, 104, 25, 109, 120, 168, 72, 250, 0, 30, 98, 143, 60, 22, 187, 101, 48, 143, 143,
        157, 121, 10, 38, 27, 165, 52, 232, 159, 167, 26, 4, 214, 138, 58, 127, 78, 11, 181, 239,
        176, 69, 192, 46, 58, 130, 192, 90, 81, 189, 9, 171, 47, 166, 49, 81, 118, 77, 24, 30, 88,
        228, 28, 225, 68, 54, 99, 13, 197, 61, 131, 5, 216, 162, 93, 54, 248, 215, 127, 112, 186,
        49, 4, 88, 44, 188, 218, 130, 207, 93, 76, 2, 138, 54, 200, 91, 151, 183, 156, 97, 206, 50,
        146, 132, 236, 146, 116, 226, 180, 47, 235, 215, 80, 211, 4, 116, 120, 200, 239, 207, 2,
        238, 142, 114, 182, 172, 112, 209, 253, 71, 133, 53, 158, 149, 113, 244, 96, 173, 188, 149,
        247, 63, 95, 93, 118, 147, 20, 177, 89, 206, 29, 97, 61, 118, 54, 18, 187, 249, 118, 26, 5,
        37, 206, 19, 75, 240, 52, 17, 234, 251, 84, 232, 55, 213, 247, 17, 35, 219, 60, 36, 39, 70,
        220, 119, 118, 223, 61, 226, 208, 16, 226, 184, 70, 173, 58, 195, 182, 55, 200, 33, 132,
        38, 128, 16, 167, 229, 14, 170, 50, 112, 191, 29, 126, 219, 80, 59, 205, 175, 39, 58, 241,
        203, 142, 90, 119, 254, 136, 43, 246, 26, 17, 148, 5, 252, 249, 174, 7, 204, 159, 158, 154,
        238, 177, 1, 255, 135, 214, 208, 12, 146, 100, 255, 216, 55, 135, 52, 196, 134, 164, 73,
        111, 1, 136, 246, 10, 66, 120, 68, 86, 199, 75, 247, 188, 236, 148, 207, 148, 25, 3, 219,
        109, 15, 244, 32, 26, 224, 125, 40, 175, 163, 172, 216, 127, 106, 114, 111, 121, 71, 192,
        89, 145, 89, 82, 25, 138, 192, 111, 144, 206, 30, 227, 217, 200, 205, 217, 30, 142, 163,
        61, 186, 251, 169, 21, 155, 58, 153, 142, 119, 129, 120, 161, 239, 188, 202, 30, 25, 187,
        91, 191, 72, 64, 195, 122, 101, 74, 159, 5, 226, 128, 171, 64, 170, 170, 148, 34, 170, 23,
        123, 195, 139, 22, 51, 190, 88, 51, 72, 189, 160, 249, 56, 17, 215, 133, 102, 166, 249,
        243, 81, 148, 67, 32, 151, 45, 50, 61, 72, 86, 247, 205, 119, 191, 61, 174, 22, 226, 144,
        15, 138, 13, 86, 149, 11, 223, 142, 253, 70, 49, 4, 127, 185, 228, 57, 203, 185, 29, 4,
        242, 186, 105, 230, 150, 250, 0, 44, 171, 255, 150, 68, 25, 121, 188, 187, 76, 234, 58,
        189, 16, 228, 101, 143, 184, 168, 66, 98, 252, 176, 75, 27, 83, 243, 63, 252, 253, 3, 75,
        76, 165, 133, 212, 235, 208, 170, 14, 160, 176, 148, 55, 114, 161, 7, 29, 7, 90, 133, 85,
        246, 190, 44, 165, 77, 192, 123, 86, 178, 70, 190, 97, 88, 225, 141, 25, 142, 253, 112, 62,
        190, 218, 193, 59, 73, 10, 20, 220, 58, 253, 55, 31, 57, 21, 232, 185, 106, 6, 139, 79,
        120, 68, 110, 110, 127, 124, 169, 6, 217, 197, 252, 76, 142, 226, 76, 141, 208, 239, 73,
        54, 107, 127, 237, 156, 241, 94, 153, 103, 218, 42, 42, 54, 207, 7, 180, 96, 208, 220, 174,
        132, 244, 118, 225, 215, 253, 74, 31, 127, 64, 196, 88, 251, 9, 217, 44, 90, 17, 250, 164,
        137, 247, 98, 45, 64, 200, 185, 14, 216, 14, 216, 7, 37, 160, 46, 80, 210, 231, 25, 108,
        21, 99, 73, 165, 46, 102, 23, 176, 79, 31, 77, 11, 216, 18, 126, 167, 243, 144, 155, 130,
        7, 164, 162, 62, 77, 97, 11, 120, 186, 243, 230, 45, 255, 115, 88, 59, 208, 133, 95, 150,
        36, 67, 11, 47, 230, 147, 179, 172, 44, 83, 62, 148, 187, 111, 49, 171, 248, 178, 231, 167,
        204, 210, 110, 80, 225, 175, 210, 203, 187, 180, 200, 254, 44, 172, 112, 171, 6, 226, 244,
        201, 136, 142, 146, 1, 226, 30, 45, 109, 141, 236, 197, 202, 107, 200, 104, 210, 21, 249,
        156, 248, 230, 189, 180, 38, 53, 82, 144, 182, 34, 24, 251, 249, 173, 251, 86, 34, 87, 229,
        33, 67, 33, 23, 151, 143, 175, 157, 125, 209, 190, 150, 74, 57, 64, 39, 29, 251, 174, 244,
        93, 59, 247, 83, 114, 194, 67, 42, 141, 104, 107, 74, 62, 22, 43, 4, 114, 138, 112, 12,
        164, 212, 166, 229, 76, 162, 128, 217, 233, 124, 239, 32, 181, 7, 134, 252, 230, 62, 120,
        251, 78, 209, 136, 243, 94, 199, 85, 45, 185, 109, 217, 136, 134, 200, 214, 0, 45, 157, 20,
        68, 42, 250, 109, 71, 81, 18, 247, 107, 135, 218, 125, 120, 80, 214, 228, 114, 171, 8, 56,
        224, 4, 253, 174, 157, 74, 66, 78, 93, 104, 14, 25, 69, 214, 200, 173, 227, 84, 0, 79, 98,
        159, 199, 78, 252, 154, 184, 202, 201, 124, 82, 55, 140, 166, 115, 252, 85, 215, 247, 52,
        137, 236, 233, 95, 230, 1, 71, 107, 123, 214, 211, 51, 22, 188, 117, 218, 247, 161, 44,
        137, 86, 96, 21, 41, 207, 161, 168, 29, 105, 66, 99, 242, 21, 158, 243, 105, 27, 49, 241,
        72, 63, 246, 159, 31, 1, 196, 39, 110, 238, 24, 40, 51, 196, 241, 102, 108, 64, 223, 12,
        111, 47, 213, 29, 242, 20, 197, 31, 104, 65, 149, 220, 161, 120, 206, 241, 2, 125, 29, 132,
        16, 169, 73, 78, 34, 3, 176, 66, 109, 66, 203, 160, 248, 169, 139, 237, 230, 37, 228, 149,
        144, 51, 122, 41, 151, 153, 61, 238, 186, 193, 140, 36, 244, 227, 83, 8, 162, 251, 213,
        220, 22, 54, 109, 217, 192, 45, 243, 115, 158, 215, 122, 23, 114, 5, 247, 186, 162, 172,
        70, 187, 143, 81, 2, 205, 36, 237, 193, 16, 158, 203, 182, 169, 115, 24, 39, 89, 207, 116,
        17, 234, 205, 237, 34, 69, 81, 153, 79, 38, 55, 106, 166, 19, 17, 247, 186, 24, 212, 7,
        223, 47, 246, 111, 92, 44, 20, 37, 5, 76, 197, 204, 214, 118, 206, 133, 68, 161, 146, 88,
        157, 228, 128, 21, 86, 104, 1, 168, 117, 97, 44, 55, 168, 108, 128, 47, 20, 243, 27, 112,
        33, 168, 190, 131, 189, 76, 51, 248, 91, 118, 216, 75, 214, 81, 75, 83, 86, 100, 209, 252,
        180, 51, 178, 133, 214, 219, 254, 16, 93, 192, 82, 10, 5, 46, 218, 111, 8, 28, 213, 142,
        172, 227, 194, 136, 140, 35, 137, 45, 149, 144, 34, 228, 182, 146, 85, 123, 5, 225, 131,
        57, 103, 73, 191, 102, 45, 224, 8, 167, 73, 180, 165, 95, 117, 82, 26, 218, 134, 197, 11,
        233, 99, 143, 78, 197, 114, 173, 190, 178, 203, 213, 99, 243, 216, 45, 60, 242, 200, 149,
        163, 152, 149, 73, 103, 160, 3, 89, 155, 51, 255, 220, 180, 98, 93, 157, 18, 45, 65, 197,
        3, 227, 243, 67, 92, 129, 196, 29, 90, 149, 229, 46, 156, 89, 233, 221, 0, 67, 137, 159,
        181, 184, 191, 198, 197, 24, 214, 220, 199, 62, 84, 234, 7, 75, 247, 190, 255, 23, 187, 25,
        247, 188, 115, 113, 55, 32, 240, 40, 233, 231, 97, 115, 6, 67, 52, 208, 53, 240, 123, 163,
        179, 255, 141, 79, 116, 19, 132, 99, 227, 114, 140, 221, 238, 163, 182, 248, 226, 26, 30,
        60, 191, 53, 152, 198, 219, 33, 139, 125, 82, 49, 135, 131, 53, 71, 223, 105, 196, 12, 115,
        93, 104, 112, 107, 117, 84, 174, 36, 49, 79, 193, 53, 255, 44, 18, 202, 62, 50, 138, 141,
        24, 7, 156, 52, 214, 52, 89, 147, 174, 235, 173, 200, 226, 129, 141, 236, 130, 133, 18,
        157, 232, 205, 225, 21, 90, 30, 88, 139, 48, 246, 34, 162, 142, 136, 5, 79, 219, 4, 42,
        126, 155, 206, 203, 221, 83, 118, 213, 209, 175, 210, 238, 86, 15, 35, 236, 149, 115, 2,
        85, 74, 165, 182, 149, 92, 150, 235, 194, 74, 130, 123, 90, 110, 1, 124, 208, 194, 100,
        255, 34, 181, 43, 255, 93, 51, 126, 148, 113, 198, 68, 143, 199, 99, 77, 39, 220, 128, 161,
        89, 208, 207, 120, 234, 128, 218, 102, 188, 119, 143, 63, 11, 8, 191, 38, 207, 203, 79, 87,
        73, 177, 3, 67, 146, 79, 115, 154, 2, 216, 212, 57, 162, 198, 47, 213, 147, 180, 162, 37,
        82, 141, 134, 63, 150, 74, 6, 139, 103, 168, 239, 202, 88, 219, 29, 50, 52, 97, 186, 9, 87,
        206, 136, 248, 127, 181, 199, 225, 211, 126, 92, 228, 236, 203, 114, 119, 173, 232, 230,
        123, 181, 47, 231, 137, 6, 14, 3, 193, 55, 38, 84, 234, 77, 63, 200, 150, 18, 143, 133,
        199, 215, 179, 209, 45, 153, 185, 220, 191, 123, 32, 76, 115, 238, 243, 61, 99, 76, 70,
        181, 12, 55, 45, 117, 116, 47, 205, 99, 123, 116, 86, 39, 205, 127, 210, 167, 58, 150, 254,
        56, 196, 191, 36, 99, 1, 144, 94, 29, 248, 119, 113, 205, 16, 36, 200, 47, 213, 192, 157,
        121, 226, 110, 109, 20, 253, 110, 80, 143, 245, 104, 84, 90, 49, 22, 106, 185, 164, 97,
        186, 57, 59, 90, 189, 240, 249, 36, 113, 246, 111, 46, 246, 83, 154, 149, 138, 239, 34,
        117, 4, 37, 174, 3, 134, 128, 120, 83, 95, 104, 139, 76, 29, 4, 129, 82, 40, 144, 172, 223,
        198, 243, 92, 227, 54, 123, 138, 85, 238, 177, 12, 184, 12, 132, 214, 154, 193, 233, 96,
        110, 63, 47, 166, 186, 16, 255, 99, 65, 89, 208, 35, 51, 197, 216, 197, 92, 190, 176, 57,
        26, 129, 60, 176, 201, 199, 22, 48, 72, 108, 233, 132, 255, 11, 132, 80, 234, 6, 71, 205,
        109, 132, 168, 254, 39, 97, 222, 214, 39, 113, 198, 82, 8, 247, 197, 160, 58, 51, 7, 203,
        60, 69, 224, 66, 58, 216, 77, 7, 227, 56, 74, 242, 239, 90, 2, 156, 29, 244, 153, 138, 245,
        243, 106, 203, 171, 138, 101, 52, 2, 62, 120, 154, 8, 141, 113, 168, 56, 134, 39, 200, 213,
        168, 70, 67, 171, 131, 58, 62, 12, 32, 192, 80, 112, 165, 109, 101, 208, 66, 233, 61, 73,
        23, 221, 111, 133, 30, 128, 96, 17, 54, 71, 110, 216, 29, 96, 105, 205, 35, 27, 154, 246,
        189, 134, 0, 211, 75, 139, 86, 253, 153, 50, 223, 151, 231, 2, 255, 164, 31, 170, 126, 103,
        106, 136, 106, 205, 28, 41, 143, 98, 54, 153, 230, 195, 78, 133, 203, 103, 65, 250, 113,
        144, 64, 13, 162, 28, 162, 226, 104, 59, 88, 13, 22, 101, 132, 79, 84, 120, 50, 85, 183,
        41, 221, 104, 220, 103, 44, 87, 165, 82, 0, 208, 79, 9, 254, 2, 21, 117, 124, 116, 8, 36,
        47, 42, 150, 82, 230, 41, 131, 213, 191, 82, 191, 128, 173, 91, 142, 247, 175, 82, 248, 63,
        142, 38, 118, 181, 51, 85, 243, 44, 161, 171, 236, 116, 211, 214, 87, 98, 204, 36, 15, 62,
        235, 68, 233, 204, 139, 232, 65, 73, 57, 50, 18, 13, 164, 5, 210, 8, 156, 31, 86, 39, 199,
        118, 226, 132, 217, 48, 191, 250, 164, 170, 35, 98, 227, 55, 61, 204, 222, 79, 77, 255, 20,
        208, 70, 150, 33, 136, 210, 44, 154, 146, 71, 11, 125, 172, 124, 153, 192, 75, 138, 75,
        218, 36, 27, 238, 4, 143, 1, 119, 181, 218, 216, 132, 184, 77, 85, 70, 227, 59, 231, 232,
        7, 15, 0, 29, 247, 131, 213, 187, 118, 100, 39, 85, 185, 69, 196, 123, 231, 153, 214, 174,
        42, 157, 253, 255, 4, 225, 80, 124, 181, 186, 85, 218, 155, 188, 158, 192, 101, 38, 168,
        120, 104, 199, 146, 90, 65, 103, 72, 182, 226, 15, 187, 137, 127, 6, 155, 87, 10, 142, 224,
        20, 248, 103, 82, 15, 172, 253, 189, 192, 85, 66, 194, 66, 197, 20, 150, 92, 67, 186, 147,
        72, 115, 46, 239, 21, 231, 55, 226, 147, 1, 43, 57, 211, 83, 91, 138, 255, 244, 240, 130,
        97, 64, 172, 5, 23, 237, 158, 173, 242, 233, 34, 140, 232, 163, 73, 160, 191, 225, 154,
        247, 224, 77, 46, 42, 153, 235, 200, 93, 94, 119, 138, 212, 185, 235, 134, 141, 43, 83,
        164, 52, 81, 235, 44, 11, 106, 174, 7, 54, 172, 77, 138, 226, 164, 19, 196, 28, 179, 178,
        152, 36, 163, 83, 218, 147, 98, 115, 214, 64, 31, 207, 205, 98, 222, 232, 202, 45, 79, 152,
        245, 89, 35, 9, 224, 207, 8, 6, 80, 187, 5, 69, 165, 204, 200, 122, 199, 228, 43, 86, 100,
        193, 74, 166, 85, 10, 9, 204, 152, 79, 104, 87, 133, 255, 152, 57, 33, 37, 35, 127, 49,
        194, 246, 105, 44, 225, 74, 197, 218, 82, 223, 175, 14, 48, 145, 224, 193, 19, 63, 175,
        233, 245, 111, 48, 19, 245, 56, 100, 30, 184, 234, 198, 18, 26, 20, 87, 214, 129, 236, 120,
        230, 183, 70, 8, 142, 103, 122, 100, 84, 180, 68, 115, 80, 180, 168, 197, 61, 157, 47, 230,
        2, 222, 208, 90, 126, 245, 203, 125, 22, 194, 118, 82, 22, 209, 192, 52, 43, 98, 152, 46,
        203, 36, 202, 115, 174, 106, 27, 157, 230, 230, 42, 64, 45, 237, 56, 124, 71, 55, 43, 126,
        195, 196, 133, 83, 238, 120, 92, 33, 6, 145, 113, 67, 240, 62, 38, 121, 116, 17, 235, 192,
        117, 54, 83, 199, 135, 65, 146, 15, 236, 244, 54, 254, 201, 174, 74, 114, 142, 48, 125, 38,
        148, 183, 185, 113, 41, 155, 30, 59, 23, 114, 192, 25, 69, 98, 142, 212, 226, 254, 10, 30,
        37, 80, 93, 185, 187, 41, 114, 124, 143, 171, 194, 26, 66, 82, 90, 116, 179, 189, 250, 18,
        217, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 7, 14,
        20, 28, 30,
    ];

    for _ in 0..500 {
        let result = ml_dsa_verify(
            signature.len() as _,
            signature.as_ptr() as _,
            message.len() as _,
            message.as_ptr() as _,
            public_key.len() as _,
            public_key.as_ptr() as _,
        );
        // check that result was positive, as negative results could have exited
        // early and do not reflect the full cost.
        assert!(result == 1);
    }
}

/// Function to measure `ml_dsa_verify_byte`.
#[unsafe(no_mangle)]
pub unsafe fn ml_dsa_verify_16kib_64() {
    // 16 KiB message. `ml_dsa_verify_byte` is priced to cover the host-side
    // marshalling of every byte of the declared message length.
    let message = [b'a'; 16384];

    // Same key pair as ml_dsa_verify_32b_500.
    let public_key: [u8; 1952] = [
        246, 25, 217, 164, 84, 170, 42, 252, 142, 239, 121, 156, 142, 63, 56, 217, 59, 48, 208,
        252, 157, 174, 217, 170, 142, 210, 100, 124, 23, 226, 25, 242, 193, 22, 104, 63, 248, 232,
        130, 144, 135, 218, 32, 14, 205, 223, 182, 144, 174, 45, 143, 49, 174, 40, 136, 156, 59, 2,
        188, 65, 23, 167, 249, 173, 38, 109, 59, 47, 134, 96, 155, 92, 159, 109, 36, 64, 164, 45,
        73, 232, 19, 83, 91, 69, 76, 237, 202, 6, 249, 253, 211, 5, 139, 88, 240, 116, 117, 171,
        245, 113, 201, 208, 77, 68, 106, 150, 39, 51, 224, 234, 191, 51, 35, 116, 138, 144, 253,
        237, 79, 121, 170, 125, 207, 209, 241, 166, 81, 119, 27, 109, 201, 0, 153, 118, 234, 151,
        30, 14, 188, 220, 80, 23, 177, 160, 133, 56, 123, 148, 217, 80, 244, 8, 4, 189, 129, 246,
        173, 23, 175, 6, 14, 183, 194, 73, 241, 136, 239, 67, 122, 164, 231, 171, 109, 227, 38,
        187, 149, 9, 156, 50, 121, 0, 15, 180, 141, 209, 55, 105, 160, 154, 235, 155, 202, 249,
        206, 128, 104, 149, 106, 173, 179, 217, 148, 57, 20, 92, 123, 158, 90, 235, 227, 192, 114,
        228, 71, 143, 180, 204, 238, 21, 181, 72, 2, 166, 136, 206, 187, 72, 244, 9, 187, 118, 211,
        89, 7, 195, 57, 189, 182, 121, 135, 100, 147, 196, 95, 95, 161, 107, 204, 84, 19, 98, 237,
        89, 162, 82, 20, 241, 20, 138, 121, 100, 44, 119, 206, 45, 191, 157, 51, 224, 91, 141, 28,
        175, 242, 254, 54, 56, 70, 6, 97, 237, 54, 224, 164, 153, 110, 21, 62, 248, 24, 229, 213,
        31, 9, 125, 106, 64, 178, 30, 203, 213, 115, 91, 126, 214, 240, 86, 97, 146, 94, 203, 236,
        111, 124, 137, 145, 230, 253, 25, 207, 48, 161, 211, 17, 158, 18, 206, 86, 68, 253, 71,
        234, 197, 148, 186, 251, 96, 36, 17, 228, 52, 46, 244, 221, 120, 168, 134, 217, 55, 251,
        228, 3, 80, 231, 18, 12, 39, 24, 251, 127, 190, 43, 28, 159, 183, 41, 170, 53, 191, 39,
        180, 21, 169, 49, 0, 169, 66, 160, 138, 188, 7, 226, 65, 211, 120, 142, 195, 111, 2, 2, 15,
        160, 30, 155, 244, 149, 23, 86, 40, 51, 131, 99, 144, 238, 45, 102, 221, 180, 164, 131, 75,
        84, 44, 59, 113, 44, 21, 232, 148, 255, 15, 196, 97, 129, 197, 170, 165, 218, 198, 98, 204,
        56, 42, 43, 7, 1, 30, 234, 60, 119, 14, 81, 101, 54, 7, 188, 82, 109, 197, 54, 4, 207, 222,
        121, 55, 74, 152, 214, 100, 247, 139, 141, 81, 13, 227, 13, 95, 203, 75, 246, 239, 199,
        118, 174, 158, 81, 159, 194, 32, 169, 200, 55, 234, 135, 224, 24, 87, 238, 196, 56, 177,
        243, 169, 132, 247, 56, 119, 116, 244, 115, 236, 51, 20, 111, 81, 161, 67, 246, 96, 92,
        195, 135, 110, 235, 139, 64, 52, 41, 129, 169, 170, 176, 17, 163, 245, 52, 232, 13, 48,
        195, 49, 252, 61, 90, 133, 169, 130, 10, 19, 51, 75, 48, 14, 226, 26, 255, 189, 40, 85,
        109, 71, 201, 186, 231, 176, 177, 137, 172, 58, 251, 91, 10, 207, 11, 60, 181, 65, 192, 64,
        240, 166, 90, 235, 20, 107, 65, 28, 40, 97, 113, 205, 235, 31, 136, 56, 35, 37, 130, 76,
        102, 70, 100, 157, 79, 244, 150, 59, 219, 146, 98, 49, 236, 214, 66, 18, 107, 224, 229,
        160, 254, 77, 199, 94, 212, 245, 3, 196, 5, 108, 40, 37, 88, 167, 78, 203, 49, 72, 237, 68,
        197, 181, 170, 195, 157, 170, 143, 171, 236, 230, 181, 45, 230, 138, 225, 207, 25, 100, 59,
        218, 144, 86, 225, 48, 184, 127, 145, 160, 13, 231, 139, 209, 121, 41, 212, 171, 28, 1,
        203, 194, 242, 219, 68, 82, 71, 185, 171, 55, 52, 164, 187, 218, 151, 8, 127, 52, 220, 248,
        222, 197, 175, 105, 28, 180, 227, 221, 40, 66, 98, 237, 19, 199, 22, 212, 97, 92, 56, 141,
        189, 68, 105, 185, 206, 64, 104, 152, 44, 168, 134, 8, 24, 122, 201, 160, 53, 162, 23, 100,
        54, 106, 226, 249, 240, 111, 184, 5, 160, 34, 37, 184, 193, 16, 190, 89, 216, 174, 67, 98,
        4, 160, 233, 81, 192, 1, 236, 96, 0, 92, 135, 119, 2, 100, 160, 59, 161, 72, 149, 255, 205,
        233, 240, 187, 134, 192, 107, 122, 3, 174, 85, 50, 121, 51, 133, 110, 8, 102, 34, 159, 132,
        246, 237, 72, 14, 81, 52, 67, 229, 171, 156, 203, 227, 193, 159, 104, 236, 18, 149, 113,
        199, 74, 40, 165, 62, 212, 30, 153, 36, 128, 28, 166, 247, 221, 9, 171, 194, 155, 200, 62,
        189, 169, 159, 99, 111, 132, 208, 252, 97, 245, 91, 98, 212, 162, 218, 154, 47, 27, 50,
        153, 156, 25, 180, 231, 151, 127, 136, 95, 205, 54, 0, 46, 102, 57, 45, 109, 239, 191, 200,
        218, 222, 97, 103, 64, 208, 172, 89, 1, 164, 85, 15, 104, 231, 91, 225, 123, 85, 49, 17,
        81, 46, 90, 154, 54, 208, 185, 113, 61, 60, 27, 59, 195, 19, 182, 218, 79, 144, 13, 217,
        71, 200, 10, 28, 44, 213, 184, 220, 219, 31, 69, 129, 70, 173, 5, 157, 45, 169, 20, 66,
        145, 208, 149, 153, 148, 200, 71, 83, 190, 105, 110, 196, 197, 246, 50, 237, 253, 39, 218,
        252, 4, 114, 208, 230, 197, 203, 124, 95, 79, 255, 42, 229, 34, 162, 143, 56, 120, 242,
        105, 245, 218, 105, 9, 250, 70, 109, 163, 18, 82, 191, 206, 252, 99, 65, 49, 193, 176, 49,
        17, 246, 185, 82, 167, 201, 182, 234, 123, 61, 47, 215, 235, 33, 188, 18, 105, 28, 94, 125,
        187, 106, 226, 171, 102, 156, 122, 114, 171, 15, 155, 62, 219, 254, 29, 198, 179, 52, 123,
        133, 76, 50, 87, 48, 229, 212, 35, 56, 183, 38, 48, 238, 169, 212, 60, 141, 5, 88, 216,
        107, 225, 255, 30, 196, 144, 43, 212, 80, 39, 229, 229, 160, 65, 162, 136, 66, 12, 229, 1,
        205, 87, 115, 202, 27, 102, 198, 91, 71, 31, 195, 165, 84, 254, 11, 69, 177, 196, 57, 157,
        62, 128, 213, 53, 86, 14, 1, 255, 107, 137, 53, 8, 222, 158, 66, 181, 59, 223, 121, 139,
        141, 233, 95, 5, 105, 159, 29, 205, 130, 150, 10, 91, 234, 196, 145, 79, 221, 39, 229, 182,
        77, 245, 234, 247, 128, 30, 100, 158, 139, 14, 14, 23, 210, 1, 119, 195, 83, 150, 17, 62,
        27, 99, 191, 253, 71, 186, 192, 56, 142, 65, 17, 113, 39, 186, 170, 48, 101, 94, 102, 60,
        203, 38, 67, 246, 79, 211, 186, 216, 114, 92, 210, 0, 0, 205, 232, 146, 111, 173, 191, 245,
        234, 27, 163, 89, 206, 42, 133, 47, 16, 203, 6, 212, 144, 250, 109, 168, 89, 97, 56, 156,
        111, 84, 251, 104, 93, 46, 165, 126, 11, 196, 131, 250, 225, 227, 152, 239, 153, 21, 81,
        56, 52, 210, 220, 226, 71, 175, 48, 96, 228, 83, 222, 31, 4, 80, 77, 115, 215, 205, 150,
        238, 145, 106, 234, 160, 255, 68, 200, 58, 29, 148, 29, 165, 151, 13, 132, 179, 120, 140,
        20, 17, 231, 1, 82, 1, 78, 189, 224, 231, 146, 118, 231, 138, 238, 106, 66, 155, 229, 75,
        176, 175, 16, 253, 15, 156, 167, 14, 60, 19, 15, 38, 222, 76, 199, 68, 121, 113, 36, 35,
        181, 171, 45, 174, 204, 14, 240, 140, 233, 237, 82, 109, 49, 230, 90, 219, 79, 173, 27,
        200, 241, 56, 5, 5, 246, 88, 127, 158, 18, 188, 74, 204, 98, 196, 202, 17, 169, 236, 20,
        222, 219, 58, 186, 252, 117, 32, 159, 158, 100, 9, 13, 12, 148, 231, 4, 230, 247, 39, 27,
        233, 96, 18, 39, 37, 203, 233, 37, 246, 180, 23, 227, 99, 76, 30, 60, 92, 197, 150, 254,
        93, 214, 184, 142, 212, 134, 179, 30, 27, 239, 47, 16, 119, 162, 19, 153, 137, 143, 135,
        74, 15, 129, 54, 198, 239, 248, 8, 223, 28, 195, 121, 202, 243, 222, 128, 0, 114, 28, 148,
        189, 158, 2, 192, 21, 74, 113, 226, 83, 120, 195, 178, 138, 26, 126, 187, 99, 202, 129, 67,
        173, 117, 225, 16, 179, 60, 60, 97, 197, 133, 30, 169, 71, 65, 186, 131, 83, 201, 0, 62,
        179, 44, 20, 140, 249, 153, 22, 74, 189, 205, 20, 228, 133, 96, 44, 164, 192, 249, 56, 187,
        105, 183, 46, 67, 28, 142, 100, 47, 193, 211, 201, 28, 112, 245, 55, 193, 11, 45, 84, 81,
        142, 47, 90, 149, 221, 34, 110, 14, 125, 45, 246, 206, 62, 245, 190, 111, 48, 154, 107,
        103, 211, 16, 247, 31, 134, 30, 151, 10, 89, 189, 115, 24, 116, 89, 57, 189, 111, 156, 23,
        153, 59, 94, 5, 6, 37, 14, 134, 136, 252, 64, 236, 13, 49, 191, 232, 246, 185, 36, 163, 58,
        43, 167, 22, 123, 136, 127, 50, 215, 238, 155, 21, 21, 79, 192, 46, 71, 196, 229, 154, 190,
        105, 217, 183, 208, 179, 83, 147, 253, 129, 91, 0, 157, 117, 253, 105, 55, 60, 127, 221,
        122, 62, 138, 230, 113, 118, 130, 220, 217, 201, 255, 152, 15, 86, 213, 126, 211, 44, 7,
        183, 5, 250, 152, 135, 92, 221, 196, 246, 52, 33, 143, 92, 22, 61, 177, 95, 29, 164, 36,
        117, 138, 125, 166, 13, 149, 50, 177, 51, 119, 92, 50, 141, 225, 240, 48, 213, 151, 46, 75,
        178, 138, 100, 197, 165, 159, 114, 199, 126, 5, 244, 157, 3, 90, 146, 25, 186, 18, 15, 177,
        35, 247, 174, 37, 164, 172, 29, 253, 192, 27, 92, 110, 118, 219, 79, 26, 197, 61, 139, 171,
        53, 20, 78, 203, 160, 232, 169, 37, 152, 159, 16, 59, 156, 70, 31, 227, 144, 90, 123, 141,
        143, 100, 210, 222, 24, 52, 243, 144, 134, 165, 222, 116, 133, 78, 151, 181, 162, 184, 160,
        123, 247, 111, 180, 231, 84, 127, 167, 153, 171, 100, 32, 182, 143, 87, 103, 204, 79, 192,
        163, 134, 142, 83, 190, 253, 13, 6, 58, 250, 43, 73, 208, 105, 68, 223, 162, 76, 31, 217,
        147, 31, 238, 252, 61, 148, 211, 253, 101, 195, 36, 208, 72, 153, 113, 70, 148, 47, 149,
        69, 54, 138, 214, 181, 111, 59, 235, 216, 21, 38, 16, 196, 44, 39, 150, 125, 238, 148, 65,
        239, 10, 62, 99, 173, 214, 1, 33, 244, 0, 33, 137, 238, 35, 58, 196, 88, 163, 217, 11, 250,
        135, 183, 191, 126, 197, 90, 184, 85, 187, 4, 179, 61, 54, 58, 101, 85, 55, 199, 171, 185,
        73, 148, 166, 132, 151, 51, 148, 148, 5, 191, 156, 160, 47, 221, 106, 173, 138, 13, 144,
        27, 111, 171, 22, 107, 24, 232, 243, 209, 14, 140, 153, 126, 58, 6, 56, 204, 38, 31, 162,
        186, 193, 230, 99, 188, 30, 109, 102, 168, 153, 141, 20, 25, 181, 137, 242, 93, 230, 197,
        31, 131, 5, 184, 222, 145, 129, 64, 137, 122, 127, 6, 230, 134, 234, 26, 114, 70, 77, 182,
        5, 110, 106, 64, 75, 126, 101, 27, 29, 231, 211, 165, 90, 89, 136, 68, 246, 91,
    ];

    // Signature over the 16 KiB message using the same secret key.
    let signature: [u8; 3309] = [
        8, 114, 98, 153, 43, 222, 190, 62, 78, 31, 204, 89, 13, 33, 95, 27, 173, 15, 226, 106, 214,
        249, 30, 152, 173, 121, 230, 164, 132, 96, 3, 229, 146, 13, 168, 8, 28, 120, 11, 115, 173,
        66, 39, 202, 163, 193, 60, 209, 171, 205, 35, 108, 208, 199, 250, 94, 244, 102, 80, 12,
        242, 92, 154, 33, 203, 81, 42, 95, 103, 39, 216, 122, 2, 247, 162, 171, 222, 63, 79, 84,
        110, 140, 217, 203, 197, 72, 241, 7, 69, 120, 52, 139, 3, 69, 78, 17, 50, 188, 224, 112,
        49, 32, 134, 70, 18, 227, 141, 130, 25, 177, 100, 175, 124, 5, 255, 132, 161, 108, 229,
        101, 40, 219, 44, 188, 11, 133, 222, 82, 138, 205, 177, 182, 110, 59, 7, 177, 186, 145, 84,
        192, 233, 163, 191, 139, 177, 149, 73, 153, 9, 58, 218, 90, 246, 165, 0, 12, 190, 8, 75,
        74, 175, 0, 144, 100, 150, 52, 170, 238, 112, 165, 108, 135, 90, 168, 113, 8, 242, 30, 44,
        241, 126, 177, 66, 114, 154, 179, 180, 177, 133, 216, 204, 175, 16, 195, 167, 68, 172, 13,
        105, 34, 18, 178, 179, 220, 162, 128, 232, 108, 155, 188, 129, 227, 61, 210, 137, 45, 63,
        117, 137, 53, 133, 77, 155, 117, 147, 162, 98, 169, 222, 88, 62, 152, 36, 232, 178, 117,
        213, 238, 39, 85, 27, 166, 6, 113, 159, 199, 227, 183, 21, 100, 222, 0, 192, 114, 190, 247,
        65, 64, 130, 196, 23, 77, 176, 54, 62, 181, 118, 150, 102, 16, 116, 189, 222, 219, 86, 55,
        54, 111, 146, 11, 166, 218, 70, 57, 138, 151, 234, 146, 240, 192, 9, 84, 226, 28, 23, 39,
        233, 10, 172, 189, 205, 32, 212, 39, 102, 20, 183, 158, 143, 54, 101, 44, 22, 140, 234,
        224, 245, 23, 198, 57, 231, 23, 150, 243, 152, 43, 80, 216, 132, 134, 213, 96, 182, 185,
        116, 242, 193, 226, 26, 54, 30, 142, 179, 45, 254, 22, 27, 208, 232, 248, 235, 190, 7, 199,
        238, 189, 221, 117, 7, 142, 190, 148, 130, 134, 248, 201, 159, 16, 244, 231, 227, 52, 176,
        203, 71, 222, 29, 151, 42, 166, 223, 65, 28, 104, 33, 90, 85, 10, 100, 54, 68, 59, 125, 56,
        83, 232, 169, 176, 180, 209, 81, 34, 12, 41, 197, 0, 136, 203, 172, 63, 171, 23, 151, 138,
        130, 135, 196, 45, 223, 238, 87, 89, 47, 24, 135, 39, 28, 119, 202, 170, 144, 79, 194, 139,
        26, 245, 81, 189, 159, 224, 16, 182, 58, 240, 153, 48, 189, 172, 31, 142, 186, 81, 138, 13,
        154, 126, 41, 60, 96, 81, 181, 193, 46, 187, 94, 104, 234, 74, 20, 136, 93, 23, 193, 234,
        148, 58, 114, 37, 246, 235, 28, 201, 120, 128, 220, 231, 90, 174, 64, 11, 146, 208, 86, 49,
        60, 206, 210, 148, 69, 135, 197, 84, 228, 0, 239, 73, 233, 104, 87, 134, 164, 237, 144, 87,
        51, 89, 51, 243, 8, 34, 88, 5, 175, 9, 170, 242, 179, 78, 30, 168, 211, 171, 39, 229, 223,
        235, 183, 201, 112, 103, 215, 217, 96, 27, 228, 185, 189, 223, 146, 164, 65, 124, 214, 237,
        131, 176, 79, 171, 47, 49, 151, 197, 102, 25, 156, 248, 7, 205, 81, 18, 98, 26, 51, 172,
        227, 172, 109, 198, 8, 102, 23, 232, 192, 84, 32, 210, 36, 161, 9, 183, 92, 15, 228, 213,
        64, 92, 123, 151, 253, 81, 52, 63, 199, 12, 8, 19, 29, 36, 15, 89, 229, 100, 171, 136, 115,
        164, 147, 176, 238, 9, 173, 29, 236, 35, 218, 170, 203, 188, 225, 252, 64, 58, 31, 21, 146,
        124, 232, 102, 89, 238, 38, 152, 78, 92, 139, 96, 153, 158, 15, 138, 26, 179, 47, 240, 87,
        198, 124, 183, 146, 252, 244, 20, 100, 1, 161, 150, 24, 195, 235, 49, 73, 79, 153, 233,
        133, 42, 47, 43, 80, 141, 69, 111, 144, 15, 100, 23, 84, 205, 156, 206, 121, 35, 106, 166,
        82, 242, 121, 64, 154, 104, 201, 120, 158, 80, 84, 251, 251, 29, 23, 111, 190, 220, 116,
        82, 196, 55, 241, 17, 247, 122, 134, 84, 62, 161, 155, 7, 41, 235, 198, 173, 35, 29, 45,
        88, 177, 210, 60, 33, 81, 145, 245, 68, 117, 233, 13, 224, 134, 127, 249, 42, 71, 131, 162,
        6, 150, 198, 56, 35, 51, 69, 119, 15, 36, 219, 132, 158, 78, 9, 202, 173, 118, 6, 29, 201,
        36, 153, 90, 198, 42, 9, 42, 79, 173, 85, 9, 171, 207, 170, 20, 70, 179, 176, 146, 49, 8,
        134, 43, 204, 102, 134, 23, 65, 152, 188, 209, 230, 236, 215, 208, 91, 255, 18, 6, 23, 76,
        4, 143, 8, 92, 8, 95, 60, 237, 160, 211, 42, 167, 2, 144, 175, 0, 72, 154, 57, 108, 37, 10,
        152, 96, 123, 155, 163, 212, 47, 108, 158, 153, 215, 78, 36, 255, 196, 128, 60, 245, 77,
        187, 187, 92, 200, 216, 44, 53, 201, 107, 127, 74, 68, 78, 54, 147, 182, 117, 125, 246,
        152, 187, 190, 25, 59, 205, 163, 14, 242, 70, 206, 154, 44, 155, 58, 107, 11, 204, 22, 199,
        157, 29, 102, 34, 169, 16, 191, 239, 0, 195, 155, 132, 99, 117, 25, 150, 191, 126, 242,
        212, 95, 130, 133, 146, 96, 203, 126, 241, 31, 4, 169, 154, 61, 60, 80, 153, 46, 49, 228,
        108, 27, 237, 159, 148, 214, 116, 190, 211, 58, 239, 186, 198, 54, 151, 191, 197, 233, 239,
        44, 209, 236, 208, 81, 206, 54, 100, 253, 119, 13, 191, 102, 232, 235, 225, 23, 255, 178,
        86, 199, 41, 13, 252, 250, 24, 170, 134, 206, 157, 42, 7, 191, 173, 35, 17, 21, 117, 84,
        25, 170, 168, 72, 114, 213, 36, 156, 230, 162, 241, 176, 26, 84, 104, 151, 137, 91, 160,
        187, 197, 14, 136, 150, 68, 84, 239, 132, 209, 90, 191, 245, 221, 226, 152, 109, 104, 102,
        54, 210, 91, 19, 217, 65, 62, 194, 62, 234, 48, 255, 63, 230, 57, 241, 232, 239, 253, 128,
        4, 46, 92, 69, 88, 210, 106, 142, 177, 199, 28, 225, 36, 235, 130, 167, 202, 120, 246, 188,
        105, 165, 154, 199, 165, 229, 251, 194, 100, 53, 67, 245, 25, 132, 73, 111, 230, 148, 214,
        42, 5, 66, 121, 64, 47, 166, 231, 81, 21, 22, 7, 164, 148, 27, 166, 96, 86, 245, 136, 196,
        138, 16, 124, 229, 136, 220, 83, 228, 114, 149, 114, 92, 176, 175, 14, 82, 145, 77, 171,
        113, 17, 35, 123, 240, 74, 129, 14, 147, 156, 154, 169, 134, 22, 218, 228, 89, 179, 222,
        34, 160, 169, 250, 46, 123, 178, 187, 198, 145, 225, 198, 4, 177, 187, 72, 68, 134, 60, 37,
        117, 8, 20, 45, 98, 222, 111, 129, 215, 26, 194, 157, 43, 202, 34, 215, 196, 73, 143, 43,
        59, 27, 24, 50, 192, 61, 12, 7, 148, 115, 120, 86, 177, 108, 4, 208, 127, 53, 251, 21, 207,
        6, 56, 198, 68, 181, 53, 26, 186, 130, 129, 53, 18, 61, 13, 160, 223, 96, 221, 81, 138, 50,
        15, 113, 113, 196, 33, 30, 194, 78, 190, 174, 243, 5, 61, 76, 172, 1, 105, 248, 236, 59,
        64, 133, 109, 69, 207, 45, 249, 232, 79, 228, 145, 36, 155, 197, 31, 125, 72, 92, 124, 187,
        206, 34, 59, 187, 196, 251, 100, 255, 243, 42, 96, 66, 80, 9, 82, 184, 215, 236, 130, 253,
        3, 185, 166, 189, 136, 75, 122, 104, 189, 7, 248, 34, 175, 221, 120, 114, 98, 131, 198, 46,
        76, 169, 250, 40, 78, 127, 50, 209, 84, 131, 243, 140, 147, 144, 151, 253, 185, 254, 78,
        155, 151, 95, 15, 151, 128, 146, 198, 100, 88, 231, 104, 64, 99, 231, 50, 138, 187, 157,
        49, 127, 75, 177, 47, 142, 129, 167, 35, 226, 142, 119, 117, 39, 198, 252, 200, 25, 195,
        12, 143, 123, 100, 130, 69, 176, 151, 219, 246, 47, 89, 232, 235, 25, 62, 101, 146, 204,
        70, 245, 129, 217, 250, 109, 109, 122, 152, 86, 197, 178, 41, 0, 158, 83, 85, 120, 243, 69,
        76, 50, 169, 57, 0, 206, 36, 120, 92, 193, 182, 63, 55, 79, 113, 52, 152, 150, 28, 254,
        209, 83, 175, 48, 91, 180, 220, 225, 46, 172, 141, 173, 172, 163, 200, 170, 218, 141, 18,
        51, 7, 11, 56, 51, 196, 82, 91, 139, 112, 86, 48, 124, 43, 76, 158, 60, 113, 71, 181, 51,
        57, 123, 137, 129, 97, 152, 255, 95, 247, 143, 129, 154, 125, 235, 16, 30, 235, 179, 205,
        142, 5, 241, 229, 234, 129, 0, 255, 206, 206, 239, 200, 92, 76, 225, 196, 32, 64, 138, 234,
        213, 91, 138, 194, 87, 146, 177, 220, 156, 206, 104, 238, 65, 239, 222, 37, 198, 142, 231,
        37, 57, 205, 242, 139, 203, 113, 208, 137, 176, 10, 8, 216, 42, 245, 239, 134, 62, 58, 30,
        92, 209, 120, 78, 72, 46, 199, 83, 241, 110, 54, 191, 150, 33, 6, 174, 213, 189, 248, 88,
        228, 23, 165, 153, 116, 46, 140, 24, 158, 13, 64, 159, 191, 176, 29, 76, 163, 84, 103, 62,
        43, 29, 171, 214, 67, 91, 210, 99, 204, 43, 222, 171, 189, 182, 161, 233, 56, 112, 14, 91,
        131, 52, 27, 246, 231, 78, 9, 27, 122, 172, 138, 153, 78, 135, 79, 100, 245, 80, 218, 174,
        153, 27, 172, 150, 38, 160, 68, 26, 224, 92, 128, 18, 97, 48, 197, 156, 193, 49, 39, 83,
        154, 254, 130, 50, 238, 136, 224, 173, 34, 147, 140, 219, 67, 56, 69, 32, 231, 232, 7, 159,
        182, 130, 104, 251, 118, 156, 79, 236, 20, 102, 228, 85, 111, 77, 219, 2, 239, 168, 9, 47,
        27, 206, 232, 39, 108, 12, 54, 124, 96, 170, 146, 234, 245, 179, 87, 122, 138, 11, 99, 12,
        36, 245, 19, 91, 196, 85, 205, 36, 236, 53, 19, 13, 20, 129, 188, 89, 63, 180, 107, 78,
        218, 128, 193, 22, 197, 159, 139, 9, 215, 231, 49, 43, 65, 43, 88, 83, 140, 184, 201, 31,
        171, 39, 103, 215, 249, 50, 143, 97, 1, 179, 232, 41, 38, 100, 122, 137, 25, 238, 151, 30,
        220, 183, 19, 153, 53, 224, 212, 57, 15, 144, 90, 137, 24, 31, 234, 196, 134, 97, 108, 8,
        142, 19, 198, 101, 201, 11, 215, 128, 92, 66, 161, 92, 3, 218, 235, 99, 173, 159, 163, 231,
        9, 83, 9, 122, 91, 20, 147, 0, 234, 136, 228, 123, 206, 128, 66, 158, 247, 19, 24, 83, 154,
        111, 65, 54, 56, 49, 69, 122, 96, 118, 207, 6, 105, 218, 170, 101, 87, 68, 40, 178, 5, 53,
        46, 35, 244, 64, 226, 16, 138, 249, 164, 249, 83, 14, 255, 186, 243, 224, 10, 214, 34, 95,
        237, 16, 206, 51, 41, 121, 202, 49, 24, 22, 220, 41, 60, 242, 99, 41, 148, 213, 61, 52,
        130, 140, 235, 159, 125, 247, 231, 98, 170, 26, 141, 199, 87, 229, 33, 203, 237, 22, 180,
        200, 121, 176, 198, 24, 64, 145, 113, 208, 54, 84, 32, 90, 26, 233, 57, 21, 41, 192, 130,
        147, 171, 198, 227, 233, 108, 151, 251, 97, 237, 251, 140, 101, 164, 63, 143, 47, 247, 27,
        220, 86, 124, 25, 188, 4, 109, 174, 84, 133, 250, 196, 90, 148, 73, 222, 176, 2, 133, 186,
        224, 225, 166, 148, 11, 8, 138, 202, 236, 215, 136, 179, 63, 168, 91, 176, 131, 18, 124,
        117, 13, 187, 8, 184, 207, 211, 147, 193, 25, 165, 128, 222, 15, 243, 110, 221, 234, 238,
        156, 97, 157, 245, 66, 146, 243, 136, 243, 145, 150, 213, 80, 199, 19, 249, 173, 232, 225,
        54, 144, 40, 124, 139, 105, 82, 139, 207, 3, 13, 147, 230, 46, 215, 156, 86, 199, 241, 74,
        118, 98, 95, 190, 3, 92, 98, 77, 99, 243, 186, 43, 62, 196, 52, 182, 148, 132, 22, 75, 169,
        211, 176, 75, 137, 235, 173, 182, 163, 209, 25, 142, 123, 4, 88, 107, 12, 122, 64, 186,
        236, 239, 96, 161, 86, 125, 191, 193, 164, 169, 155, 65, 24, 42, 171, 198, 141, 127, 146,
        147, 6, 33, 124, 89, 213, 115, 134, 243, 27, 141, 111, 22, 130, 147, 220, 176, 21, 246,
        180, 200, 178, 145, 75, 231, 6, 228, 128, 206, 200, 40, 202, 133, 100, 76, 153, 240, 87,
        179, 45, 248, 166, 172, 243, 52, 14, 202, 102, 18, 122, 92, 65, 147, 132, 0, 8, 84, 86, 94,
        218, 75, 107, 168, 60, 167, 121, 188, 62, 234, 34, 82, 172, 72, 184, 92, 107, 99, 191, 108,
        84, 253, 113, 154, 86, 120, 98, 32, 36, 19, 234, 81, 10, 139, 22, 33, 54, 194, 134, 165,
        97, 94, 111, 139, 210, 43, 246, 167, 251, 166, 163, 111, 19, 43, 25, 106, 41, 236, 145, 98,
        22, 73, 238, 152, 96, 184, 175, 226, 192, 203, 215, 189, 197, 129, 241, 210, 161, 230, 50,
        209, 254, 176, 104, 214, 219, 24, 246, 208, 169, 117, 240, 84, 134, 242, 52, 105, 96, 155,
        88, 183, 82, 151, 34, 56, 149, 25, 156, 24, 166, 100, 223, 111, 50, 112, 175, 48, 169, 92,
        51, 47, 62, 5, 240, 40, 13, 194, 156, 196, 180, 75, 94, 106, 232, 175, 233, 213, 77, 105,
        213, 227, 153, 235, 165, 223, 114, 8, 42, 96, 237, 159, 123, 52, 229, 209, 172, 204, 224,
        50, 76, 171, 135, 144, 36, 202, 54, 7, 153, 1, 22, 194, 70, 130, 19, 57, 131, 5, 239, 71,
        192, 38, 240, 120, 96, 207, 255, 73, 255, 87, 5, 192, 24, 114, 119, 176, 249, 68, 225, 28,
        43, 213, 24, 80, 229, 20, 125, 155, 220, 95, 111, 152, 178, 32, 34, 22, 229, 165, 89, 180,
        124, 177, 255, 198, 70, 126, 67, 131, 11, 183, 32, 212, 138, 147, 37, 72, 135, 133, 5, 253,
        35, 156, 200, 85, 161, 96, 246, 60, 121, 119, 2, 141, 4, 133, 123, 72, 180, 253, 81, 72,
        50, 14, 141, 38, 173, 56, 26, 240, 247, 136, 219, 245, 95, 162, 103, 180, 230, 11, 16, 220,
        53, 220, 208, 107, 208, 23, 132, 221, 122, 3, 135, 214, 38, 132, 157, 112, 109, 186, 113,
        205, 199, 76, 185, 133, 230, 200, 219, 147, 163, 104, 30, 92, 105, 95, 64, 217, 237, 246,
        246, 163, 21, 133, 110, 28, 185, 137, 85, 179, 219, 249, 46, 76, 226, 57, 181, 104, 124,
        176, 164, 148, 53, 30, 111, 122, 81, 245, 19, 37, 192, 117, 202, 218, 63, 54, 77, 57, 185,
        218, 124, 47, 94, 50, 113, 132, 189, 115, 146, 109, 253, 200, 24, 154, 98, 87, 227, 124,
        136, 150, 143, 32, 187, 57, 8, 24, 105, 130, 4, 171, 130, 2, 132, 204, 164, 65, 194, 241,
        225, 210, 131, 218, 254, 19, 22, 73, 68, 173, 10, 111, 62, 155, 72, 184, 86, 65, 9, 104,
        158, 236, 197, 7, 185, 113, 233, 66, 23, 83, 179, 121, 127, 197, 221, 21, 139, 246, 128,
        44, 44, 33, 151, 254, 87, 39, 45, 54, 217, 132, 251, 46, 39, 98, 93, 33, 234, 178, 96, 47,
        234, 3, 221, 99, 239, 220, 237, 236, 119, 39, 250, 162, 216, 64, 189, 231, 217, 64, 12,
        160, 102, 241, 85, 161, 234, 136, 219, 0, 240, 102, 167, 239, 110, 211, 57, 63, 114, 230,
        184, 95, 23, 162, 42, 131, 98, 84, 52, 8, 249, 119, 166, 196, 126, 14, 27, 126, 221, 120,
        16, 51, 33, 179, 180, 29, 254, 247, 103, 15, 230, 40, 132, 6, 99, 169, 169, 62, 174, 198,
        210, 39, 89, 187, 214, 131, 81, 156, 145, 221, 133, 227, 85, 35, 209, 155, 70, 193, 168,
        67, 25, 121, 206, 131, 134, 159, 6, 63, 17, 245, 54, 155, 40, 189, 238, 194, 250, 78, 143,
        221, 7, 110, 208, 77, 6, 132, 110, 99, 230, 123, 78, 43, 19, 190, 95, 234, 253, 135, 37,
        186, 38, 32, 200, 211, 85, 8, 48, 157, 172, 191, 27, 29, 170, 70, 179, 108, 191, 134, 217,
        234, 201, 157, 52, 83, 235, 220, 185, 231, 171, 249, 161, 16, 189, 179, 95, 129, 128, 237,
        240, 48, 84, 73, 251, 57, 7, 119, 122, 121, 125, 97, 199, 70, 232, 170, 194, 160, 254, 206,
        47, 223, 2, 145, 173, 158, 203, 200, 100, 203, 111, 31, 169, 81, 29, 131, 189, 87, 141,
        162, 62, 238, 107, 25, 193, 53, 191, 106, 36, 200, 13, 60, 255, 70, 193, 254, 132, 232, 61,
        107, 48, 178, 130, 123, 125, 22, 255, 126, 119, 187, 124, 171, 33, 155, 85, 8, 46, 179,
        128, 62, 152, 49, 161, 91, 39, 9, 26, 165, 8, 132, 46, 192, 57, 13, 214, 74, 84, 127, 213,
        5, 148, 203, 143, 64, 213, 233, 143, 207, 164, 72, 199, 87, 44, 228, 213, 114, 71, 130, 88,
        156, 222, 115, 56, 73, 229, 166, 46, 41, 102, 36, 46, 168, 18, 79, 122, 251, 21, 16, 63,
        115, 61, 115, 159, 5, 130, 104, 66, 209, 94, 77, 193, 135, 84, 243, 214, 223, 194, 78, 201,
        208, 33, 19, 26, 6, 31, 125, 101, 55, 184, 180, 102, 21, 189, 58, 187, 138, 36, 129, 34,
        193, 113, 5, 255, 199, 50, 11, 184, 239, 40, 185, 23, 224, 133, 184, 157, 126, 184, 105,
        73, 72, 33, 69, 188, 160, 168, 10, 253, 173, 63, 23, 81, 76, 86, 39, 215, 189, 78, 236,
        124, 22, 250, 36, 70, 148, 150, 111, 221, 90, 253, 59, 133, 129, 227, 72, 180, 197, 94,
        251, 159, 210, 255, 120, 100, 149, 206, 177, 192, 55, 192, 90, 199, 116, 223, 197, 135,
        235, 122, 63, 26, 35, 14, 115, 198, 136, 233, 173, 242, 58, 154, 94, 114, 227, 95, 244, 79,
        141, 73, 162, 51, 202, 96, 49, 130, 35, 137, 215, 14, 26, 25, 29, 90, 112, 96, 150, 62, 56,
        225, 180, 253, 14, 49, 151, 182, 232, 0, 49, 81, 21, 42, 248, 18, 206, 221, 220, 29, 85,
        211, 20, 35, 236, 108, 23, 119, 124, 195, 64, 119, 200, 43, 235, 184, 152, 45, 12, 137,
        122, 219, 111, 196, 43, 98, 163, 175, 180, 123, 29, 149, 238, 211, 102, 58, 251, 114, 40,
        130, 246, 9, 254, 240, 180, 114, 69, 130, 185, 66, 39, 32, 236, 203, 247, 114, 131, 122,
        31, 171, 71, 59, 35, 204, 134, 200, 247, 177, 22, 23, 20, 165, 78, 24, 178, 250, 4, 227,
        197, 54, 32, 183, 160, 195, 250, 44, 114, 89, 50, 61, 49, 223, 250, 91, 181, 84, 111, 208,
        148, 225, 158, 96, 201, 186, 177, 239, 59, 201, 114, 48, 83, 25, 55, 198, 101, 6, 177, 231,
        224, 233, 7, 54, 174, 89, 235, 167, 84, 189, 170, 212, 230, 2, 161, 184, 241, 32, 234, 86,
        218, 162, 8, 234, 94, 60, 58, 98, 103, 109, 152, 172, 247, 8, 105, 120, 143, 230, 42, 49,
        140, 156, 215, 254, 77, 107, 111, 129, 208, 228, 255, 9, 54, 63, 69, 80, 89, 109, 182, 185,
        216, 250, 26, 63, 97, 166, 217, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 12, 18, 25,
        36, 41,
    ];

    for _ in 0..64 {
        let result = ml_dsa_verify(
            signature.len() as _,
            signature.as_ptr() as _,
            message.len() as _,
            message.as_ptr() as _,
            public_key.len() as _,
            public_key.as_ptr() as _,
        );
        // check that result was positive, as negative results could have exited
        // early and do not reflect the full cost.
        assert!(result == 1);
    }
}

#[repr(C)]
struct MultiexpElem([u8; 64], [u8; 32]);

// Function to measure `alt_bn128_g1_multiexp_base` and `alt_bn128_g1_multiexp_sublinear`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `g1_multiexp` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute g1_multiexp on 1 element 10 times.
#[unsafe(no_mangle)]
pub unsafe fn alt_bn128_g1_multiexp_1_10() {
    #[rustfmt::skip]
    let buffer: [MultiexpElem; 1] = [
        MultiexpElem([16, 238, 91, 161, 241, 22, 172, 158, 138, 252, 202, 212, 136, 37, 110, 231, 118, 220, 8, 45, 14, 153, 125, 217, 227, 87, 238, 238, 31, 138, 226, 8, 238, 185, 12, 155, 93, 126, 144, 248, 200, 177, 46, 245, 40, 162, 169, 80, 150, 211, 157, 13, 10, 36, 44, 232, 173, 32, 32, 115, 123, 2, 9, 47],
                     [190, 148, 181, 91, 69, 6, 83, 40, 65, 222, 251, 70, 81, 73, 60, 142, 130, 217, 176, 20, 69, 75, 40, 167, 41, 180, 244, 5, 142, 215, 135, 35]),
    ];
    for _ in 0..10 {
        alt_bn128_g1_multiexp(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        );
    }
}
// Function to measure `alt_bn128_g1_multiexp_base` and `alt_bn128_g1_multiexp_sublinear`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `g1_multiexp` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute g1_multiexp on 10 elements 10 times.
#[unsafe(no_mangle)]
pub unsafe fn alt_bn128_g1_multiexp_10_10() {
    #[rustfmt::skip]
    let buffer: [MultiexpElem; 10] = [
        MultiexpElem([160, 75, 88, 1, 39, 49, 57, 54, 144, 97, 157, 53, 186, 197, 3, 255, 101, 190, 210, 68, 165, 67, 219, 118, 134, 41, 207, 184, 16, 195, 15, 6, 248, 45, 160, 73, 157, 19, 206, 183, 155, 145, 214, 79, 111, 126, 176, 149, 80, 249, 68, 90, 62, 163, 162, 67, 252, 47, 4, 54, 29, 49, 53, 22],
                     [12, 56, 213, 113, 211, 78, 202, 179, 173, 211, 178, 235, 0, 245, 228, 42, 143, 64, 247, 122, 22, 211, 114, 92, 83, 200, 105, 203, 39, 32, 63, 27]),
        MultiexpElem([37, 7, 26, 98, 65, 31, 35, 122, 129, 78, 25, 86, 148, 57, 116, 70, 153, 93, 194, 58, 97, 220, 203, 215, 224, 39, 69, 225, 33, 127, 203, 22, 208, 87, 64, 209, 160, 50, 233, 197, 79, 248, 194, 81, 89, 254, 17, 41, 109, 220, 150, 155, 5, 95, 228, 40, 12, 165, 190, 58, 247, 134, 209, 14],
                     [244, 210, 233, 80, 103, 48, 162, 222, 169, 166, 178, 7, 251, 120, 170, 5, 196, 223, 179, 12, 240, 219, 64, 15, 210, 140, 180, 12, 98, 193, 51, 35]),
        MultiexpElem([101, 235, 228, 31, 160, 165, 255, 231, 8, 230, 135, 9, 86, 42, 196, 160, 234, 133, 17, 42, 106, 87, 229, 48, 163, 73, 23, 56, 106, 110, 233, 14, 246, 209, 251, 163, 97, 223, 184, 57, 122, 44, 248, 21, 143, 156, 140, 178, 208, 231, 246, 42, 160, 28, 169, 134, 186, 33, 111, 79, 176, 193, 114, 4],
                     [32, 6, 91, 170, 245, 35, 191, 116, 68, 155, 138, 161, 69, 253, 181, 174, 197, 58, 207, 32, 247, 69, 40, 226, 110, 106, 97, 188, 185, 71, 5, 4]),
        MultiexpElem([30, 201, 144, 105, 155, 88, 21, 238, 82, 231, 38, 215, 187, 109, 64, 45, 213, 112, 123, 117, 182, 34, 17, 120, 137, 153, 181, 73, 103, 173, 175, 12, 56, 100, 23, 159, 145, 19, 214, 51, 118, 221, 156, 208, 163, 123, 95, 75, 61, 175, 45, 246, 245, 37, 37, 71, 237, 70, 168, 37, 191, 161, 129, 37],
                     [141, 72, 92, 238, 166, 71, 116, 3, 194, 153, 215, 33, 137, 244, 155, 246, 177, 218, 156, 27, 134, 19, 3, 52, 87, 215, 30, 182, 153, 244, 230, 5]),
        MultiexpElem([130, 175, 69, 182, 170, 81, 194, 158, 15, 103, 192, 202, 12, 245, 20, 187, 241, 59, 70, 185, 207, 220, 215, 180, 202, 0, 201, 132, 203, 112, 218, 23, 190, 111, 131, 109, 225, 24, 24, 109, 165, 174, 0, 243, 229, 234, 191, 178, 12, 139, 167, 219, 113, 35, 210, 187, 238, 171, 58, 57, 92, 204, 80, 13],
                     [68, 230, 101, 222, 230, 122, 127, 221, 26, 145, 121, 2, 186, 153, 57, 195, 155, 167, 252, 162, 74, 29, 145, 71, 152, 182, 106, 222, 251, 31, 18, 14]),
        MultiexpElem([228, 81, 31, 41, 119, 231, 33, 160, 222, 228, 135, 22, 160, 1, 145, 232, 241, 77, 68, 48, 110, 27, 98, 73, 222, 194, 197, 218, 69, 38, 186, 6, 174, 192, 184, 222, 106, 213, 150, 163, 152, 137, 169, 149, 188, 21, 169, 3, 85, 97, 60, 105, 222, 87, 73, 77, 23, 46, 80, 109, 53, 143, 249, 35],
                     [241, 199, 237, 191, 208, 242, 112, 249, 123, 67, 165, 9, 177, 232, 215, 242, 100, 37, 64, 150, 94, 143, 49, 113, 14, 203, 192, 93, 62, 218, 80, 26]),
        MultiexpElem([100, 0, 47, 223, 75, 179, 152, 89, 133, 183, 139, 6, 111, 147, 236, 47, 156, 169, 164, 176, 65, 147, 60, 46, 71, 153, 231, 71, 64, 6, 38, 41, 56, 117, 9, 136, 151, 236, 181, 209, 129, 186, 40, 145, 252, 36, 84, 156, 109, 229, 49, 247, 39, 108, 174, 77, 146, 123, 109, 159, 82, 128, 217, 37],
                     [242, 91, 130, 131, 75, 140, 17, 84, 183, 169, 177, 27, 254, 198, 240, 241, 177, 230, 22, 152, 47, 158, 83, 224, 126, 59, 109, 134, 230, 230, 202, 39]),
        MultiexpElem([235, 0, 171, 107, 245, 236, 211, 78, 253, 88, 231, 142, 235, 226, 72, 251, 47, 190, 102, 103, 42, 243, 147, 160, 94, 110, 252, 188, 127, 208, 129, 21, 113, 64, 101, 51, 63, 70, 181, 9, 13, 27, 129, 6, 132, 11, 122, 0, 14, 141, 42, 104, 214, 180, 72, 237, 163, 4, 25, 169, 236, 244, 6, 1],
                     [49, 147, 150, 137, 36, 231, 125, 113, 18, 113, 80, 86, 72, 98, 64, 52, 199, 187, 237, 189, 196, 37, 57, 172, 59, 169, 228, 72, 130, 172, 79, 43]),
        MultiexpElem([82, 84, 39, 25, 71, 8, 250, 3, 126, 110, 4, 157, 215, 43, 88, 57, 103, 237, 236, 231, 237, 246, 224, 75, 47, 150, 229, 43, 117, 115, 81, 5, 199, 238, 36, 161, 135, 182, 77, 82, 173, 216, 141, 86, 249, 210, 110, 146, 61, 39, 46, 229, 119, 128, 133, 113, 236, 89, 99, 52, 156, 51, 116, 7],
                     [234, 246, 120, 109, 70, 97, 226, 124, 146, 197, 14, 48, 220, 18, 81, 103, 227, 12, 240, 155, 220, 125, 90, 45, 173, 31, 95, 162, 175, 27, 82, 36]),
        MultiexpElem([46, 144, 238, 51, 121, 134, 255, 49, 104, 119, 209, 185, 131, 214, 61, 148, 48, 188, 108, 119, 101, 189, 223, 152, 191, 180, 146, 17, 179, 38, 209, 36, 26, 208, 85, 141, 208, 135, 207, 174, 123, 138, 185, 167, 96, 13, 1, 83, 159, 80, 96, 145, 194, 57, 153, 195, 15, 11, 92, 13, 8, 195, 196, 7],
                     [210, 90, 26, 178, 71, 105, 235, 10, 78, 33, 99, 232, 208, 135, 210, 118, 233, 9, 5, 90, 12, 7, 82, 87, 246, 221, 98, 145, 122, 59, 230, 46]),
    ];
    for _ in 0..10 {
        alt_bn128_g1_multiexp(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        );
    }
}

#[repr(C)]
struct SumElem(u8, [u8; 64]);

// Function to measure `alt_bn128_g1_sum_base` and `alt_bn128_g1_sum_element`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `g1_sum` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute g1_sum on 1 element 1k times.
#[unsafe(no_mangle)]
pub unsafe fn alt_bn128_g1_sum_1_1k() {
    #[rustfmt::skip]
    let buffer: [SumElem; 1] = [
        SumElem(0, [11, 49, 94, 29, 152, 111, 116, 138, 248, 2, 184, 8, 159, 80, 169, 45, 149, 48, 32, 49, 37, 6, 133, 105, 171, 194, 120, 44, 195, 17, 180, 35, 137, 154, 4, 192, 211, 244, 93, 200, 2, 44, 0, 64, 26, 108, 139, 147, 88, 235, 242, 23, 253, 52, 110, 236, 67, 99, 176, 2, 186, 198, 228, 25]),
    ];
    for _ in 0..1_000 {
        alt_bn128_g1_sum(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        );
    }
}

// Function to measure `alt_bn128_g1_sum_base` and `alt_bn128_g1_sum_element`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `g1_sum` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute g1_sum on 10 element 1k times.
#[unsafe(no_mangle)]
pub unsafe fn alt_bn128_g1_sum_10_1k() {
    #[rustfmt::skip]
    let buffer: [SumElem; 10] = [
        SumElem(0, [35, 69, 193, 29, 159, 231, 101, 1, 196, 134, 150, 68, 208, 119, 157, 39, 248, 90, 73, 19, 10, 8, 235, 127, 161, 218, 15, 115, 180, 172, 209, 16, 228, 156, 170, 43, 38, 198, 86, 36, 148, 8, 79, 237, 137, 133, 67, 225, 144, 247, 98, 177, 65, 52, 2, 51, 108, 108, 235, 101, 71, 15, 89, 24]),
        SumElem(0, [61, 2, 168, 116, 29, 2, 121, 230, 138, 133, 50, 222, 81, 136, 130, 255, 228, 187, 237, 97, 226, 24, 24, 225, 228, 221, 102, 101, 88, 154, 127, 43, 21, 183, 14, 55, 239, 203, 66, 188, 194, 60, 232, 17, 100, 73, 170, 174, 44, 131, 89, 176, 26, 43, 123, 173, 38, 238, 5, 204, 224, 211, 6, 38]),
        SumElem(1, [22, 233, 101, 119, 186, 92, 128, 212, 29, 189, 170, 207, 212, 163, 98, 74, 31, 168, 145, 29, 1, 168, 123, 195, 7, 243, 15, 22, 186, 1, 105, 30, 177, 138, 63, 142, 128, 124, 81, 157, 189, 145, 114, 71, 91, 192, 243, 19, 214, 74, 141, 149, 84, 3, 97, 51, 101, 221, 243, 63, 34, 43, 44, 12]),
        SumElem(0, [222, 145, 152, 153, 248, 97, 163, 68, 51, 35, 234, 159, 72, 239, 0, 78, 56, 37, 239, 175, 121, 224, 50, 128, 51, 230, 122, 231, 96, 31, 204, 0, 239, 255, 21, 120, 202, 147, 235, 55, 165, 147, 205, 33, 240, 15, 47, 163, 189, 125, 84, 26, 232, 163, 132, 250, 148, 136, 2, 15, 114, 159, 178, 29]),
        SumElem(1, [175, 114, 55, 82, 223, 6, 219, 68, 103, 204, 9, 215, 220, 64, 99, 224, 12, 186, 46, 90, 74, 93, 66, 74, 211, 119, 163, 10, 36, 239, 175, 31, 80, 201, 192, 101, 188, 227, 193, 181, 51, 8, 239, 157, 158, 28, 92, 28, 186, 197, 20, 161, 153, 183, 226, 236, 231, 56, 22, 141, 20, 194, 202, 6]),
        SumElem(1, [127, 32, 22, 163, 172, 30, 54, 118, 159, 178, 75, 182, 19, 149, 171, 185, 113, 11, 203, 125, 150, 24, 123, 18, 110, 134, 15, 28, 181, 224, 95, 26, 83, 22, 56, 133, 230, 240, 166, 228, 89, 241, 249, 38, 150, 59, 240, 175, 146, 52, 184, 240, 226, 13, 10, 214, 175, 134, 37, 217, 96, 163, 7, 27]),
        SumElem(1, [198, 145, 187, 197, 240, 31, 72, 191, 158, 229, 28, 10, 102, 2, 225, 215, 100, 142, 183, 59, 15, 182, 207, 95, 163, 199, 235, 54, 87, 154, 106, 32, 193, 169, 8, 160, 52, 171, 14, 186, 158, 60, 211, 82, 31, 224, 46, 82, 29, 176, 164, 166, 141, 148, 67, 245, 192, 14, 144, 163, 200, 72, 119, 20]),
        SumElem(1, [146, 85, 5, 23, 90, 242, 183, 140, 72, 71, 210, 248, 1, 106, 24, 44, 12, 88, 216, 245, 83, 140, 157, 142, 220, 215, 110, 181, 223, 192, 104, 9, 255, 142, 7, 108, 36, 73, 219, 134, 59, 163, 13, 120, 153, 208, 66, 18, 91, 48, 15, 151, 121, 11, 183, 176, 89, 170, 89, 187, 165, 219, 130, 34]),
        SumElem(0, [67, 100, 228, 203, 250, 180, 250, 94, 237, 191, 144, 155, 198, 81, 168, 159, 196, 94, 122, 171, 96, 15, 39, 203, 187, 224, 94, 43, 81, 11, 114, 27, 236, 70, 0, 241, 246, 107, 68, 165, 82, 117, 90, 154, 141, 206, 102, 4, 145, 66, 144, 215, 244, 227, 164, 252, 128, 205, 233, 51, 122, 213, 232, 20]),
        SumElem(1, [173, 37, 223, 11, 68, 227, 111, 46, 246, 210, 31, 181, 36, 149, 221, 113, 27, 34, 156, 35, 82, 144, 38, 34, 165, 43, 130, 70, 86, 34, 148, 30, 15, 207, 202, 218, 30, 22, 143, 20, 58, 60, 133, 67, 150, 255, 65, 29, 217, 94, 89, 182, 13, 160, 65, 173, 5, 232, 223, 65, 213, 210, 55, 25]),
    ];
    for _ in 0..1_000 {
        alt_bn128_g1_sum(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        );
    }
}

#[repr(C)]
struct PairingElem([u8; 64], [u8; 128]);

// Function to measure `alt_bn128_pairing_check_base` and `alt_bn128_pairing_check_element`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `g1_multiexp` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute pairing_check on 1 element 10 times.
#[unsafe(no_mangle)]
pub unsafe fn alt_bn128_pairing_check_1_10() {
    #[rustfmt::skip]
    let buffer: [PairingElem; 1] = [
        PairingElem([80, 12, 4, 181, 61, 254, 153, 52, 127, 228, 174, 24, 144, 95, 235, 26, 197, 188, 219, 91, 4, 47, 98, 98, 202, 199, 94, 67, 211, 223, 197, 21, 65, 221, 184, 75, 69, 202, 13, 56, 6, 233, 217, 146, 159, 141, 116, 208, 81, 224, 146, 124, 150, 114, 218, 196, 192, 233, 253, 31, 130, 152, 144, 29],
                    [34, 54, 229, 82, 80, 13, 200, 53, 254, 193, 250, 1, 205, 60, 38, 172, 237, 29, 18, 82, 187, 98, 113, 152, 184, 251, 223, 42, 104, 148, 253, 25, 79, 39, 165, 18, 195, 165, 215, 155, 168, 251, 250, 2, 215, 214, 193, 172, 187, 84, 54, 168, 27, 100, 161, 155, 144, 95, 199, 238, 88, 238, 202, 46, 247, 97, 33, 56, 78, 174, 171, 15, 245, 5, 121, 144, 88, 81, 102, 133, 118, 222, 81, 214, 74, 169, 27, 91, 27, 23, 80, 55, 43, 97, 101, 24, 168, 29, 75, 136, 229, 2, 55, 77, 60, 200, 227, 210, 172, 194, 232, 45, 151, 46, 248, 206, 193, 250, 145, 84, 78, 176, 74, 210, 0, 106, 168, 30]),
    ];
    for _ in 0..10 {
        alt_bn128_pairing_check(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
        );
    }
}
// Function to measure `alt_bn128_g1_multiexp_base` and `alt_bn128_g1_multiexp_sublinear`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `g1_multiexp` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute pairing_check on 10 elements 10 times.
#[unsafe(no_mangle)]
pub unsafe fn alt_bn128_pairing_check_10_10() {
    #[rustfmt::skip]
    let buffer: [PairingElem; 10] = [
        PairingElem([223, 6, 200, 33, 166, 188, 103, 53, 200, 168, 35, 109, 73, 23, 33, 99, 238, 116, 162, 85, 150, 159, 255, 82, 85, 22, 102, 22, 188, 22, 105, 12, 248, 15, 147, 127, 39, 16, 214, 51, 106, 17, 235, 64, 94, 132, 235, 26, 178, 3, 134, 136, 133, 222, 114, 36, 21, 172, 70, 82, 39, 198, 18, 25],
                    [148, 200, 134, 248, 104, 246, 245, 98, 240, 216, 24, 152, 37, 21, 194, 154, 66, 240, 198, 134, 174, 64, 155, 105, 1, 59, 115, 35, 63, 254, 41, 13, 9, 213, 97, 49, 90, 178, 1, 96, 163, 189, 16, 152, 31, 108, 62, 36, 48, 97, 253, 154, 196, 156, 74, 171, 10, 19, 213, 236, 176, 205, 172, 23, 240, 49, 89, 79, 195, 106, 221, 106, 236, 127, 76, 42, 160, 144, 208, 67, 233, 255, 233, 202, 88, 148, 99, 71, 51, 235, 236, 251, 1, 108, 127, 3, 202, 160, 68, 30, 150, 211, 73, 21, 193, 117, 125, 153, 165, 246, 182, 191, 251, 26, 186, 198, 153, 218, 196, 234, 241, 135, 159, 184, 33, 214, 235, 4]),
        PairingElem([229, 38, 210, 21, 178, 61, 170, 167, 16, 18, 51, 233, 225, 101, 198, 72, 53, 5, 223, 97, 78, 49, 54, 101, 37, 6, 196, 16, 9, 255, 173, 6, 6, 141, 167, 242, 31, 118, 141, 162, 125, 98, 127, 242, 9, 144, 222, 175, 9, 217, 39, 246, 105, 3, 244, 130, 192, 39, 67, 47, 134, 150, 185, 2],
                    [0, 3, 183, 195, 187, 101, 121, 183, 52, 251, 254, 149, 175, 97, 17, 209, 129, 221, 149, 4, 175, 47, 128, 5, 253, 131, 89, 231, 88, 69, 161, 32, 236, 15, 165, 1, 226, 253, 65, 154, 122, 61, 175, 78, 146, 202, 39, 228, 214, 15, 229, 88, 122, 165, 40, 247, 52, 154, 251, 14, 129, 223, 11, 24, 90, 186, 200, 9, 91, 165, 129, 250, 69, 190, 83, 250, 209, 188, 250, 16, 124, 179, 106, 66, 228, 50, 142, 82, 119, 98, 193, 50, 251, 92, 141, 37, 194, 136, 10, 231, 216, 165, 140, 225, 143, 243, 23, 238, 11, 153, 112, 148, 78, 52, 22, 19, 65, 198, 84, 212, 199, 1, 2, 176, 190, 45, 113, 47]),
        PairingElem([22, 76, 141, 39, 200, 16, 110, 85, 83, 226, 99, 154, 70, 185, 9, 80, 124, 246, 104, 244, 25, 82, 182, 188, 240, 197, 133, 88, 187, 99, 13, 19, 30, 252, 168, 144, 127, 37, 159, 58, 10, 136, 149, 123, 148, 244, 27, 24, 169, 132, 25, 59, 193, 113, 137, 14, 170, 53, 31, 89, 254, 231, 248, 2],
                    [109, 112, 187, 231, 175, 83, 200, 247, 55, 103, 34, 199, 225, 228, 60, 108, 17, 36, 199, 118, 66, 18, 143, 44, 72, 185, 139, 231, 169, 26, 230, 5, 133, 86, 209, 85, 83, 146, 170, 132, 27, 254, 32, 200, 150, 48, 223, 59, 22, 7, 152, 161, 92, 250, 197, 97, 110, 97, 130, 64, 15, 70, 29, 40, 201, 19, 234, 144, 223, 253, 140, 81, 104, 96, 153, 87, 32, 155, 243, 59, 72, 111, 132, 41, 67, 126, 10, 208, 188, 92, 65, 179, 54, 135, 27, 17, 105, 197, 203, 93, 59, 5, 131, 70, 56, 170, 247, 72, 61, 196, 13, 150, 182, 233, 243, 19, 118, 107, 8, 132, 154, 245, 11, 190, 186, 14, 69, 2]),
        PairingElem([114, 177, 108, 197, 9, 236, 140, 81, 182, 114, 134, 205, 144, 72, 24, 155, 29, 185, 247, 187, 253, 81, 96, 143, 178, 62, 220, 167, 32, 191, 122, 35, 18, 119, 54, 255, 6, 205, 32, 214, 226, 216, 250, 191, 250, 127, 163, 67, 0, 79, 249, 153, 81, 204, 23, 86, 240, 192, 141, 83, 22, 91, 255, 3],
                    [248, 13, 239, 47, 56, 141, 228, 144, 148, 199, 15, 129, 168, 215, 50, 181, 243, 87, 253, 184, 135, 137, 120, 201, 203, 116, 173, 123, 57, 239, 128, 16, 138, 214, 88, 219, 117, 54, 3, 72, 135, 77, 63, 83, 38, 40, 174, 86, 176, 17, 245, 233, 150, 23, 129, 78, 241, 175, 147, 94, 6, 67, 129, 43, 19, 48, 170, 59, 116, 74, 103, 52, 175, 228, 68, 169, 78, 158, 97, 0, 240, 42, 65, 177, 34, 231, 40, 52, 84, 133, 25, 52, 49, 70, 60, 47, 184, 223, 165, 176, 80, 235, 157, 43, 212, 24, 128, 175, 82, 107, 208, 66, 191, 85, 21, 162, 214, 205, 28, 33, 104, 213, 32, 250, 109, 42, 18, 29]),
        PairingElem([170, 231, 186, 171, 126, 67, 218, 47, 205, 136, 168, 13, 190, 186, 202, 200, 239, 184, 31, 99, 98, 201, 226, 94, 205, 77, 164, 32, 238, 244, 183, 7, 130, 196, 74, 163, 122, 189, 118, 4, 121, 28, 183, 243, 104, 226, 144, 141, 42, 215, 93, 251, 83, 207, 229, 37, 154, 231, 64, 162, 209, 56, 190, 37],
                    [22, 4, 93, 106, 34, 167, 39, 254, 224, 210, 205, 5, 62, 77, 150, 86, 109, 161, 250, 136, 75, 218, 159, 26, 85, 109, 215, 194, 20, 142, 174, 25, 215, 255, 214, 125, 44, 2, 176, 114, 9, 241, 236, 64, 124, 185, 132, 142, 190, 187, 227, 198, 240, 89, 47, 222, 130, 27, 242, 48, 157, 33, 150, 16, 42, 220, 107, 124, 189, 43, 191, 62, 17, 211, 40, 243, 233, 26, 247, 205, 68, 253, 105, 132, 129, 167, 169, 231, 109, 154, 144, 73, 234, 55, 45, 42, 6, 118, 244, 125, 13, 42, 146, 138, 199, 195, 104, 254, 221, 172, 220, 19, 98, 176, 166, 227, 9, 159, 205, 50, 55, 21, 92, 205, 86, 224, 189, 45]),
        PairingElem([219, 141, 11, 233, 254, 113, 213, 120, 49, 108, 208, 48, 146, 194, 176, 70, 175, 40, 150, 80, 217, 3, 63, 71, 190, 71, 2, 224, 157, 116, 169, 31, 149, 185, 247, 136, 26, 251, 84, 39, 165, 100, 221, 119, 163, 169, 44, 199, 98, 5, 43, 93, 161, 203, 119, 184, 239, 149, 81, 118, 89, 177, 132, 16],
                    [187, 184, 93, 144, 191, 32, 240, 106, 13, 221, 213, 150, 41, 219, 192, 93, 123, 50, 102, 28, 85, 119, 4, 187, 88, 137, 187, 7, 226, 182, 127, 45, 49, 30, 169, 231, 65, 157, 58, 236, 152, 165, 199, 127, 44, 23, 193, 8, 76, 228, 74, 249, 229, 106, 252, 158, 124, 74, 80, 126, 158, 52, 54, 27, 8, 196, 244, 50, 105, 103, 26, 79, 82, 90, 130, 62, 66, 183, 154, 109, 229, 90, 217, 224, 185, 63, 112, 126, 139, 240, 205, 115, 221, 181, 126, 1, 56, 43, 132, 131, 24, 46, 60, 97, 55, 190, 161, 9, 223, 0, 231, 197, 81, 28, 62, 51, 111, 199, 191, 102, 153, 200, 221, 41, 108, 162, 28, 32]),
        PairingElem([30, 53, 135, 154, 144, 29, 186, 12, 158, 167, 159, 120, 102, 34, 6, 234, 82, 202, 81, 140, 238, 236, 160, 66, 83, 7, 160, 64, 138, 124, 20, 37, 56, 242, 83, 131, 176, 81, 145, 228, 107, 199, 221, 120, 175, 245, 18, 236, 227, 216, 44, 173, 40, 106, 57, 146, 139, 114, 233, 100, 24, 116, 35, 27],
                    [223, 176, 180, 99, 164, 136, 62, 8, 8, 82, 238, 208, 224, 98, 97, 42, 114, 206, 98, 212, 75, 240, 58, 21, 183, 137, 115, 81, 155, 31, 31, 17, 80, 112, 210, 23, 25, 68, 31, 225, 252, 60, 49, 51, 226, 224, 81, 21, 222, 223, 186, 133, 244, 185, 80, 41, 251, 135, 89, 18, 139, 82, 232, 43, 136, 90, 253, 241, 7, 189, 37, 249, 86, 118, 175, 107, 153, 163, 117, 8, 0, 150, 45, 244, 76, 47, 173, 109, 40, 111, 245, 45, 22, 4, 184, 15, 103, 17, 28, 179, 1, 74, 62, 22, 242, 240, 227, 91, 179, 37, 200, 144, 102, 208, 255, 230, 20, 241, 179, 8, 116, 94, 64, 59, 179, 102, 104, 0]),
        PairingElem([15, 85, 82, 165, 6, 84, 191, 14, 237, 31, 189, 18, 247, 6, 164, 106, 224, 231, 24, 196, 118, 52, 45, 51, 249, 19, 100, 2, 126, 100, 224, 16, 188, 81, 64, 1, 130, 153, 112, 55, 135, 138, 0, 31, 89, 27, 51, 208, 84, 98, 235, 218, 106, 229, 138, 27, 232, 252, 53, 183, 27, 90, 19, 11],
                    [40, 240, 97, 126, 62, 32, 190, 177, 113, 254, 3, 136, 251, 27, 61, 148, 65, 195, 47, 94, 18, 66, 163, 169, 231, 122, 2, 158, 183, 37, 105, 33, 59, 108, 132, 78, 184, 105, 25, 221, 5, 76, 192, 207, 127, 194, 208, 92, 50, 197, 84, 215, 146, 92, 150, 23, 207, 90, 90, 166, 39, 206, 253, 42, 251, 16, 74, 4, 176, 247, 209, 65, 131, 177, 6, 248, 111, 156, 152, 123, 77, 197, 141, 132, 169, 93, 79, 95, 195, 92, 175, 229, 29, 186, 20, 42, 85, 130, 115, 201, 145, 81, 5, 152, 86, 225, 6, 226, 70, 106, 161, 229, 158, 245, 168, 47, 183, 104, 24, 217, 165, 70, 154, 93, 111, 149, 70, 33]),
        PairingElem([117, 0, 122, 107, 244, 192, 63, 73, 188, 19, 32, 65, 113, 194, 53, 131, 86, 97, 44, 198, 81, 55, 164, 201, 183, 250, 129, 150, 126, 198, 13, 38, 7, 8, 206, 60, 150, 88, 196, 65, 198, 1, 230, 202, 250, 254, 101, 92, 219, 104, 228, 96, 35, 194, 237, 136, 126, 248, 42, 182, 247, 241, 75, 14],
                    [69, 234, 24, 234, 25, 191, 64, 38, 183, 126, 143, 90, 107, 15, 30, 68, 252, 210, 74, 136, 110, 117, 191, 63, 133, 55, 240, 252, 78, 14, 25, 43, 243, 191, 152, 161, 242, 117, 141, 220, 27, 5, 218, 83, 156, 204, 94, 173, 227, 150, 36, 98, 160, 97, 154, 168, 148, 100, 150, 158, 151, 240, 51, 48, 126, 42, 195, 42, 163, 53, 124, 226, 253, 99, 24, 50, 158, 41, 28, 51, 1, 11, 65, 201, 191, 134, 156, 0, 223, 239, 198, 207, 180, 243, 118, 28, 98, 173, 195, 236, 75, 113, 170, 12, 120, 49, 188, 11, 54, 108, 55, 49, 54, 110, 228, 232, 202, 181, 196, 207, 53, 123, 158, 232, 68, 15, 188, 22]),
        PairingElem([45, 111, 148, 191, 42, 226, 238, 248, 78, 94, 26, 9, 239, 9, 15, 13, 10, 170, 97, 8, 195, 153, 36, 144, 198, 4, 224, 209, 40, 187, 94, 48, 201, 232, 156, 89, 230, 160, 89, 200, 132, 202, 38, 57, 183, 180, 218, 67, 213, 155, 217, 208, 70, 148, 7, 255, 123, 35, 71, 215, 131, 153, 39, 21],
                    [205, 30, 22, 107, 59, 81, 254, 153, 39, 49, 130, 112, 16, 111, 159, 119, 66, 72, 251, 247, 86, 214, 89, 173, 104, 226, 32, 13, 82, 209, 195, 2, 207, 8, 240, 251, 117, 92, 41, 206, 209, 141, 185, 60, 24, 218, 2, 249, 161, 52, 128, 224, 52, 152, 5, 227, 58, 198, 54, 248, 183, 181, 28, 8, 159, 184, 36, 32, 149, 142, 86, 69, 184, 53, 85, 214, 224, 181, 36, 44, 114, 27, 177, 213, 179, 159, 135, 52, 213, 192, 251, 150, 85, 145, 118, 6, 218, 177, 105, 20, 215, 89, 47, 78, 157, 25, 81, 85, 123, 18, 86, 88, 137, 86, 89, 39, 173, 144, 10, 175, 215, 79, 121, 252, 106, 13, 91, 31]),
    ];
    for _ in 0..10 {
        alt_bn128_pairing_check(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p1_sum_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(
            bls12381_p1_sum(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p1_sum_50_100() {
    let buffer: [[u8; 2 * 97]; 25] = [[
        0, 18, 25, 108, 90, 67, 214, 146, 36, 216, 113, 51, 137, 40, 95, 38, 185, 143, 134, 238,
        145, 10, 179, 221, 102, 142, 65, 55, 56, 40, 32, 3, 204, 91, 115, 87, 175, 154, 122, 245,
        75, 183, 19, 214, 34, 85, 232, 15, 86, 6, 186, 129, 2, 191, 190, 234, 68, 22, 183, 16, 199,
        62, 140, 206, 48, 50, 195, 28, 98, 105, 196, 73, 6, 248, 172, 79, 120, 116, 206, 153, 251,
        23, 85, 153, 146, 72, 101, 40, 150, 56, 132, 206, 66, 154, 153, 47, 238, 0, 0, 1, 16, 16,
        152, 245, 195, 152, 147, 118, 87, 102, 175, 69, 18, 160, 199, 78, 27, 184, 155, 199, 230,
        253, 241, 78, 62, 115, 55, 210, 87, 204, 15, 148, 101, 129, 121, 216, 51, 32, 185, 159, 49,
        255, 148, 205, 43, 172, 3, 225, 169, 249, 244, 76, 162, 205, 171, 79, 67, 161, 163, 238,
        52, 112, 253, 249, 11, 47, 194, 40, 235, 59, 112, 159, 205, 114, 240, 20, 131, 138, 200,
        42, 109, 121, 122, 238, 254, 217, 160, 128, 75, 34, 237, 28, 232, 247,
    ]; 25];

    for _ in 0..100 {
        assert_eq!(
            bls12381_p1_sum(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p2_sum_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(
            bls12381_p2_sum(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p2_sum_50_100() {
    let buffer: [[u8; 2 * 193]; 25] = [[
        0, 12, 199, 10, 88, 127, 70, 82, 3, 157, 129, 23, 182, 16, 56, 88, 173, 205, 151, 40, 246,
        174, 190, 35, 5, 120, 56, 154, 98, 218, 0, 66, 183, 98, 59, 28, 4, 54, 115, 79, 70, 60,
        253, 209, 135, 210, 9, 3, 36, 24, 192, 173, 166, 53, 27, 112, 102, 31, 5, 51, 101, 222,
        174, 86, 145, 7, 152, 189, 42, 206, 110, 43, 246, 186, 65, 146, 209, 162, 41, 150, 127,
        106, 246, 202, 28, 154, 138, 17, 235, 192, 162, 50, 52, 78, 224, 246, 214, 7, 155, 165, 13,
        37, 17, 99, 27, 32, 182, 214, 243, 132, 30, 97, 110, 157, 17, 182, 142, 195, 54, 140, 214,
        1, 41, 217, 212, 120, 122, 181, 108, 78, 145, 69, 163, 137, 39, 229, 28, 156, 214, 39, 29,
        73, 61, 147, 136, 9, 245, 11, 215, 190, 237, 178, 51, 40, 129, 143, 159, 253, 175, 219,
        109, 166, 164, 221, 128, 197, 169, 4, 138, 184, 177, 84, 223, 60, 173, 147, 140, 206, 222,
        130, 159, 17, 86, 247, 105, 217, 225, 73, 121, 30, 142, 12, 217, 0, 9, 174, 177, 12, 55,
        43, 94, 241, 1, 6, 117, 198, 164, 118, 47, 218, 51, 99, 100, 137, 194, 59, 88, 28, 117, 34,
        5, 137, 175, 188, 12, 196, 98, 73, 249, 33, 238, 160, 45, 209, 183, 97, 224, 54, 255, 219,
        174, 34, 25, 47, 165, 216, 115, 47, 249, 243, 142, 11, 28, 241, 46, 173, 253, 38, 8, 240,
        199, 163, 154, 206, 215, 116, 104, 55, 131, 58, 226, 83, 187, 87, 239, 156, 13, 152, 164,
        182, 158, 235, 41, 80, 144, 25, 23, 233, 157, 30, 23, 72, 130, 205, 211, 85, 30, 12, 230,
        23, 136, 97, 255, 131, 225, 149, 254, 203, 207, 253, 83, 166, 123, 111, 16, 180, 67, 30,
        66, 62, 40, 164, 128, 50, 127, 235, 231, 2, 118, 3, 111, 96, 187, 156, 153, 207, 118, 51,
        2, 210, 37, 68, 118, 0, 212, 159, 147, 43, 157, 211, 202, 30, 105, 89, 105, 122, 166, 3,
        231, 77, 134, 102, 104, 26, 45, 202, 129, 96, 195, 133, 118, 104, 174, 7, 68, 64, 54, 102,
        25, 235, 137, 32, 37, 108, 78, 74,
    ]; 25];

    for _ in 0..100 {
        assert_eq!(
            bls12381_p2_sum(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_g1_multiexp_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(
            bls12381_g1_multiexp(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_g1_multiexp_50_100() {
    let buffer: [[u8; 96 + 32]; 50] = [[
        23, 241, 211, 167, 49, 151, 215, 148, 38, 149, 99, 140, 79, 169, 172, 15, 195, 104, 140,
        79, 151, 116, 185, 5, 161, 78, 58, 63, 23, 27, 172, 88, 108, 85, 232, 63, 249, 122, 26,
        239, 251, 58, 240, 10, 219, 34, 198, 187, 8, 179, 244, 129, 227, 170, 160, 241, 160, 158,
        48, 237, 116, 29, 138, 228, 252, 245, 224, 149, 213, 208, 10, 246, 0, 219, 24, 203, 44, 4,
        179, 237, 208, 60, 199, 68, 162, 136, 138, 228, 12, 170, 35, 41, 70, 197, 231, 225, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]; 50];

    for _ in 0..100 {
        assert_eq!(
            bls12381_g1_multiexp(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_g2_multiexp_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(
            bls12381_g2_multiexp(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_g2_multiexp_50_100() {
    let buffer: [[u8; 192 + 32]; 50] = [[
        19, 224, 43, 96, 82, 113, 159, 96, 125, 172, 211, 160, 136, 39, 79, 101, 89, 107, 208, 208,
        153, 32, 182, 26, 181, 218, 97, 187, 220, 127, 80, 73, 51, 76, 241, 18, 19, 148, 93, 87,
        229, 172, 125, 5, 93, 4, 43, 126, 2, 74, 162, 178, 240, 143, 10, 145, 38, 8, 5, 39, 45,
        197, 16, 81, 198, 228, 122, 212, 250, 64, 59, 2, 180, 81, 11, 100, 122, 227, 209, 119, 11,
        172, 3, 38, 168, 5, 187, 239, 212, 128, 86, 200, 193, 33, 189, 184, 6, 6, 196, 160, 46,
        167, 52, 204, 50, 172, 210, 176, 43, 194, 139, 153, 203, 62, 40, 126, 133, 167, 99, 175,
        38, 116, 146, 171, 87, 46, 153, 171, 63, 55, 13, 39, 92, 236, 29, 161, 170, 169, 7, 95,
        240, 95, 121, 190, 12, 229, 213, 39, 114, 125, 110, 17, 140, 201, 205, 198, 218, 46, 53,
        26, 173, 253, 155, 170, 140, 189, 211, 167, 109, 66, 154, 105, 81, 96, 209, 44, 146, 58,
        201, 204, 59, 172, 162, 137, 225, 147, 84, 134, 8, 184, 40, 1, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]; 50];

    for _ in 0..100 {
        assert_eq!(
            bls12381_g2_multiexp(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_map_fp_to_g1_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(
            bls12381_map_fp_to_g1(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_map_fp_to_g1_50_100() {
    let buffer: [[u8; 48]; 50] = [[
        20, 64, 110, 91, 251, 146, 9, 37, 106, 56, 32, 135, 154, 41, 172, 47, 98, 214, 172, 168,
        35, 36, 191, 58, 226, 170, 125, 60, 84, 121, 32, 67, 189, 140, 121, 31, 204, 219, 8, 12,
        26, 82, 220, 104, 184, 182, 147, 80,
    ]; 50];

    for _ in 0..100 {
        assert_eq!(
            bls12381_map_fp_to_g1(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_map_fp2_to_g2_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(
            bls12381_map_fp2_to_g2(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_map_fp2_to_g2_10_100() {
    let buffer: [[u8; 96]; 10] = [[
        14, 136, 91, 179, 57, 150, 225, 47, 7, 218, 105, 7, 62, 44, 12, 200, 128, 188, 142, 255,
        38, 210, 167, 36, 41, 158, 177, 45, 84, 244, 188, 242, 111, 71, 72, 187, 2, 14, 128, 167,
        227, 121, 74, 123, 14, 71, 166, 65, 20, 64, 110, 91, 251, 146, 9, 37, 106, 56, 32, 135,
        154, 41, 172, 47, 98, 214, 172, 168, 35, 36, 191, 58, 226, 170, 125, 60, 84, 121, 32, 67,
        189, 140, 121, 31, 204, 219, 8, 12, 26, 82, 220, 104, 184, 182, 147, 80,
    ]; 10];

    for _ in 0..100 {
        assert_eq!(
            bls12381_map_fp2_to_g2(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_pairing_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(
            bls12381_pairing_check(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_pairing_5_100() {
    let buffer: [[u8; 288]; 5] = [[
        23, 241, 211, 167, 49, 151, 215, 148, 38, 149, 99, 140, 79, 169, 172, 15, 195, 104, 140,
        79, 151, 116, 185, 5, 161, 78, 58, 63, 23, 27, 172, 88, 108, 85, 232, 63, 249, 122, 26,
        239, 251, 58, 240, 10, 219, 34, 198, 187, 8, 179, 244, 129, 227, 170, 160, 241, 160, 158,
        48, 237, 116, 29, 138, 228, 252, 245, 224, 149, 213, 208, 10, 246, 0, 219, 24, 203, 44, 4,
        179, 237, 208, 60, 199, 68, 162, 136, 138, 228, 12, 170, 35, 41, 70, 197, 231, 225, 19,
        224, 43, 96, 82, 113, 159, 96, 125, 172, 211, 160, 136, 39, 79, 101, 89, 107, 208, 208,
        153, 32, 182, 26, 181, 218, 97, 187, 220, 127, 80, 73, 51, 76, 241, 18, 19, 148, 93, 87,
        229, 172, 125, 5, 93, 4, 43, 126, 2, 74, 162, 178, 240, 143, 10, 145, 38, 8, 5, 39, 45,
        197, 16, 81, 198, 228, 122, 212, 250, 64, 59, 2, 180, 81, 11, 100, 122, 227, 209, 119, 11,
        172, 3, 38, 168, 5, 187, 239, 212, 128, 86, 200, 193, 33, 189, 184, 6, 6, 196, 160, 46,
        167, 52, 204, 50, 172, 210, 176, 43, 194, 139, 153, 203, 62, 40, 126, 133, 167, 99, 175,
        38, 116, 146, 171, 87, 46, 153, 171, 63, 55, 13, 39, 92, 236, 29, 161, 170, 169, 7, 95,
        240, 95, 121, 190, 12, 229, 213, 39, 114, 125, 110, 17, 140, 201, 205, 198, 218, 46, 53,
        26, 173, 253, 155, 170, 140, 189, 211, 167, 109, 66, 154, 105, 81, 96, 209, 44, 146, 58,
        201, 204, 59, 172, 162, 137, 225, 147, 84, 134, 8, 184, 40, 1,
    ]; 5];

    for _ in 0..100 {
        assert_eq!(
            bls12381_pairing_check(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64
            ),
            2
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p1_decompress_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(
            bls12381_p1_decompress(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p1_decompress_50_100() {
    let buffer: [[u8; 48]; 50] = [[
        185, 110, 35, 139, 110, 142, 126, 177, 120, 97, 234, 41, 91, 204, 20, 203, 207, 103, 224,
        112, 176, 18, 102, 59, 68, 107, 137, 231, 10, 71, 183, 63, 198, 228, 242, 206, 195, 124,
        70, 91, 53, 182, 222, 158, 19, 104, 106, 15,
    ]; 50];

    for _ in 0..100 {
        assert_eq!(
            bls12381_p1_decompress(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p2_decompress_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(
            bls12381_p2_decompress(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p2_decompress_50_100() {
    let buffer: [[u8; 96]; 50] = [[
        143, 150, 139, 210, 67, 144, 143, 243, 229, 250, 26, 179, 243, 30, 7, 129, 151, 229, 138,
        206, 86, 43, 190, 139, 90, 39, 29, 95, 186, 80, 35, 125, 160, 200, 254, 101, 231, 181, 119,
        28, 192, 168, 111, 213, 127, 50, 52, 126, 21, 162, 109, 31, 93, 86, 196, 114, 208, 25, 238,
        162, 83, 158, 88, 219, 0, 196, 154, 165, 208, 169, 102, 56, 56, 144, 63, 221, 190, 67, 107,
        91, 21, 126, 131, 179, 93, 26, 78, 95, 137, 247, 129, 39, 243, 93, 172, 240,
    ]; 50];

    for _ in 0..100 {
        assert_eq!(
            bls12381_p2_decompress(
                core::mem::size_of_val(&buffer) as u64,
                buffer.as_ptr() as *const u64 as u64,
                0,
            ),
            0
        );
    }
}

// ###############
// # Storage API #
// ###############

macro_rules! storage_bench {
    ($key_buf:ident, $key_len:expr, $value_buf:ident, $value_len:expr, $loop_n:expr, $exp_name:ident,  $call:block) => {
        #[unsafe(no_mangle)]
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

// Function to measure `storage_write_base + storage_write_value_byte`.
// Writes to storage with 100kib value 1000 times.
storage_bench!(key, 10, value, 102400, 1000, storage_write_10b_key_100kib_value_1k, {
    storage_write(10, key.as_ptr() as _, 102400, value.as_ptr() as _, 0);
});

// Function to measure `storage_write_base + storage_write_key_byte`.
// Writes to storage with 10kib key and 10kib value 1000 times.
storage_bench!(key, 10240, value, 10240, 1000, storage_write_10kib_key_10kib_value_1k, {
    storage_write(10240, key.as_ptr() as _, 10240, value.as_ptr() as _, 0);
});

// Storage reading.

// Function to measure `storage_read_base`.
// Reads from storage 1k times.
storage_bench!(key, 10, value, 10, 1000, storage_read_10b_key_1k, {
    storage_read(10, key.as_ptr() as _, 0);
});

// Function to measure `storage_read_base + storage_read_key_byte`.
// Reads from storage with 10kib key 1000 times.
storage_bench!(key, 10240, value, 10, 1000, storage_read_10kib_key_1k, {
    storage_read(10240, key.as_ptr() as _, 0);
});

// Storage removing.

// Function to measure `storage_remove_base`.
// Writes to storage 1k times.
storage_bench!(key, 10, value, 10, 1000, storage_remove_10b_key_1k, {
    storage_remove(10, key.as_ptr() as _, 0);
});

// Function to measure `storage_remove_base + storage_remove_key_byte`.
// Writes to storage with 10kib key 1000 times.
storage_bench!(key, 10240, value, 10, 1000, storage_remove_10kib_key_1k, {
    storage_remove(10240, key.as_ptr() as _, 0);
});

// Storage has key.

// Function to measure `storage_has_key_base`.
// Checks key existence from storage 1k times.
storage_bench!(key, 10, value, 10, 1000, storage_has_key_10b_key_1k, {
    storage_has_key(10, key.as_ptr() as _);
});

// Function to measure `storage_has_key_base + storage_has_key_key_byte`.
// Checks key existence from storage with 10kib key 1000 times.
storage_bench!(key, 10240, value, 10, 1000, storage_has_key_10kib_key_1k, {
    storage_has_key(10240, key.as_ptr() as _);
});

// Function to measure `promise_and_base`.
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
pub unsafe fn promise_return_100k() {
    let account = b"alice_near";
    let id = promise_batch_create(account.len() as _, account.as_ptr() as _);
    for _ in 0..100_000 {
        promise_return(id);
    }
}

// Measuring cost for data_receipt_creation_config.

// Function that emits 10b of data.
#[unsafe(no_mangle)]
pub unsafe fn data_producer_10b() {
    let data = [0u8; 10];
    value_return(data.len() as _, data.as_ptr() as _);
}

// Function that emits 100kib of data.
#[unsafe(no_mangle)]
pub unsafe fn data_producer_100kib() {
    let data = [0u8; 102400];
    value_return(data.len() as _, data.as_ptr() as _);
}

// Function to measure `data_receipt_creation_config`, but we are measure send and execution fee at the same time.
// Produces 1000 10b data receipts.
#[unsafe(no_mangle)]
pub unsafe fn data_receipt_10b_1000() {
    data_receipt_10b_n::<1000>();
}

#[unsafe(no_mangle)]
pub unsafe fn data_receipt_10b_40() {
    data_receipt_10b_n::<40>();
}

// Produces `n` data receipts with 10b of data each.
unsafe fn data_receipt_10b_n<const NUM_RECEIPTS: usize>() {
    let mut buf = [0u8; 1000];
    current_account_id(0);
    let buf_len = register_len(0);
    read_register(0, buf.as_mut_ptr());

    let method_name = b"data_producer_10b";
    let args = b"";
    let mut ids = [0u64; NUM_RECEIPTS];
    let amount = 0u128;
    let gas = prepaid_gas();
    for i in 0..NUM_RECEIPTS {
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

// Function to subtract the base from the `data_receipt_creation_config`. This method doesn't
// have a callback on created promises so there is no data dependency.
#[unsafe(no_mangle)]
pub unsafe fn data_receipt_base_10b_1000() {
    let mut buf = [0u8; 1000];
    current_account_id(0);
    let buf_len = register_len(0);
    read_register(0, buf.as_mut_ptr());

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
    let _id = promise_and(ids.as_ptr() as _, ids.len() as _);
}

// Function to measure `data_receipt_creation_config`, but we are measure send and execution fee at
// the same time. Produces 40 100kib data receipts.
#[unsafe(no_mangle)]
pub unsafe fn data_receipt_100kib_40() {
    const NUM_RECEIPTS: usize = 40;
    let mut buf = [0u8; 1000];
    current_account_id(0);
    let buf_len = register_len(0);
    read_register(0, buf.as_mut_ptr());

    let method_name = b"data_producer_100kib";
    let args = b"";
    let mut ids = [0u64; NUM_RECEIPTS];
    let amount = 0u128;
    let gas = prepaid_gas();
    for i in 0..NUM_RECEIPTS {
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

/// Function to measure `yield_create_base` fee.
/// Creates 1000 waiting receipts/yield promises.
#[unsafe(no_mangle)]
pub unsafe fn yield_create_base() {
    const METHOD_NAME: &str = "n";
    for _ in 0..1000 {
        promise_yield_create(METHOD_NAME.len() as u64, METHOD_NAME.as_ptr() as u64, 0, 0, 0, 1, 0);
    }
}

/// Function to measure `yield_create_with_id_base` fee.
/// Creates 1000 waiting receipts/yield promises via `promise_yield_create_with_id`.
/// Each call uses a distinct user-provided yield_id (the loop counter) to avoid
/// duplicate-detection bailouts.
#[unsafe(no_mangle)]
pub unsafe fn yield_create_with_id_base() {
    const METHOD_NAME: &str = "n";
    let amount: u128 = 0;
    for i in 0..1000u64 {
        let mut yield_id = [0u8; 32];
        yield_id[..8].copy_from_slice(&i.to_le_bytes());
        promise_yield_create_with_id(
            METHOD_NAME.len() as u64,
            METHOD_NAME.as_ptr() as u64,
            0,
            0,
            &amount as *const u128 as u64,
            0,
            1,
            yield_id.len() as u64,
            yield_id.as_ptr() as u64,
        );
    }
}

/// Function to measure `yield_create_byte`. Subtract the measurement for `yield_create_base` above,
/// thus obtaining the cost of creating yield promises with a 100 byte method name.
///
/// Creates 1000 waiting receipts/yield promises.
#[unsafe(no_mangle)]
pub unsafe fn yield_create_byte_100b_method_length() {
    const METHOD_NAME: &str = "noooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooop";
    for _ in 0..1000 {
        promise_yield_create(METHOD_NAME.len() as u64, METHOD_NAME.as_ptr() as u64, 0, 0, 0, 1, 0);
    }
}

/// Function to measure `yield_create_byte`. Subtract the measurement for `yield_create_base` above,
/// thus obtaining the cost of creating yield promises with a 1000 byte arguments.
///
/// Creates 1000 waiting receipts/yield promises.
#[unsafe(no_mangle)]
pub unsafe fn yield_create_byte_1000b_argument_length() {
    const ARGUMENTS: [u8; 1000] = [b'a'; 1000];
    const METHOD_NAME: &str = "n";
    for _ in 0..1000 {
        promise_yield_create(
            METHOD_NAME.len() as u64,
            METHOD_NAME.as_ptr() as u64,
            ARGUMENTS.len() as u64,
            ARGUMENTS.as_ptr() as u64,
            0,
            1,
            0,
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe fn yield_resume_base_prepare() {
    const METHOD_NAME: &str = "n";
    for i in 0..255u8 {
        promise_yield_create(METHOD_NAME.len() as u64, METHOD_NAME.as_ptr() as u64, 0, 0, 0, 1, 2);
        storage_write(1, &raw const i as u64, u64::MAX, 2, 3);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn yield_resume_base() {
    for i in 0..255u8 {
        match storage_read(1, &raw const i as u64, 0) {
            0 => panic!("storage_read did not produce data_id"),
            1 => assert_eq!(promise_yield_resume(u64::MAX, 0, 0, 0), 1),
            _ => panic!("unexpected storage_read return"),
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe fn yield_resume_payload() {
    const ARGUMENTS: [u8; 1000] = [b'a'; 1000];
    for i in 0..255u8 {
        match storage_read(1, &raw const i as u64, 0) {
            0 => panic!("storage_read did not produce data_id"),
            1 => assert_eq!(promise_yield_resume(u64::MAX, 0, 1000, ARGUMENTS.as_ptr() as u64), 1),
            _ => panic!("unexpected storage_read return"),
        }
    }
}

#[unsafe(no_mangle)]
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

/// Maximum length that sum of arguments can have.
/// The assumption is that all WASM memory except for 1 page is available.
const MAX_ARG_LEN: u64 = 1024 * 1024 - 4096;

#[unsafe(no_mangle)]
/// Insert or overwrite a value to given key
pub unsafe fn account_storage_insert_key() {
    input(0);
    let mut input_data = [0u8; MAX_ARG_LEN as usize];
    read_register(0, input_data.as_mut_ptr());

    let key_len = u64::from_le_bytes(input_data[..8].try_into().unwrap());
    assert!(key_len < MAX_ARG_LEN - 16);
    let key = &input_data[8..8 + key_len as usize];

    let value_input = &input_data[8 + key_len as usize..];

    let value_len = u64::from_le_bytes(value_input[..8].try_into().unwrap());
    assert!(value_len + key_len + 16 < MAX_ARG_LEN);
    let value = &value_input[8..8 + value_len as usize];

    storage_write(key_len, key.as_ptr() as _, value_len, value.as_ptr() as _, 1);
}

#[unsafe(no_mangle)]
/// Check if key exists for account
pub unsafe fn account_storage_has_key() {
    input(0);
    let mut input_data = [0u8; MAX_ARG_LEN as usize];
    read_register(0, input_data.as_mut_ptr());

    let key_len = u64::from_le_bytes(input_data[..8].try_into().unwrap());
    assert!(key_len < MAX_ARG_LEN - 16);
    let key = &input_data[8..8 + key_len as usize];

    storage_has_key(key_len, key.as_ptr() as _);
}
