#![no_std]
#![allow(non_snake_case)]

#[panic_handler]
#[no_mangle]
pub fn panic(_info: &::core::panic::PanicInfo) -> ! {
    unsafe { core::arch::wasm32::unreachable() }
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
    #[cfg(feature = "protocol_feature_alt_bn128")]
    fn alt_bn128_g1_multiexp(value_len: u64, value_ptr: u64, register_id: u64);
    #[cfg(feature = "protocol_feature_alt_bn128")]
    fn alt_bn128_g1_sum(value_len: u64, value_ptr: u64, register_id: u64);
    #[cfg(feature = "protocol_feature_alt_bn128")]
    fn alt_bn128_pairing_check(value_len: u64, value_ptr: u64) -> u64;
    fn random_seed(register_id: u64);
    fn sha256(value_len: u64, value_ptr: u64, register_id: u64);
    fn keccak256(value_len: u64, value_ptr: u64, register_id: u64);
    fn keccak512(value_len: u64, value_ptr: u64, register_id: u64);
    #[cfg(feature = "protocol_feature_evm")]
    fn ripemd160(value_len: u64, value_ptr: u64, register_id: u64);
    #[cfg(feature = "protocol_feature_evm")]
    fn blake2b(value_len: u64, value_ptr: u64, register_id: u64);
    #[cfg(feature = "protocol_feature_evm")]
    fn blake2b_f(rounds_ptr: u64, h_ptr: u64, m_ptr: u64, t_ptr: u64, f_ptr: u64, register_id: u64);
    #[cfg(feature = "protocol_feature_evm")]
    fn ecrecover(hash_ptr: u64, v: u32, r_ptr: u64, s_ptr: u64, register_id: u64);
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
#[cfg(feature = "payload")]
#[no_mangle]
pub fn payload() {
    let payload = include_bytes!("../res/payload");
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

// Function to measure `ripemd160_base` and `ripemd160_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `ripemd160` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute ripemd160 on 10b 10k times.
#[no_mangle]
#[cfg(feature = "protocol_feature_evm")]
pub unsafe fn ripemd160_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        ripemd160(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}
// Function to measure `ripemd160_base` and `ripemd160_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `ripemd160` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute ripemd160 on 10kib 10k times.
#[no_mangle]
#[cfg(feature = "protocol_feature_evm")]
pub unsafe fn ripemd160_10kib_10k() {
    let buffer = [65u8; 10240];
    for _ in 0..10_000 {
        ripemd160(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}

// Function to measure `blake2b_base` and `blake2b_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `blake2b` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute blake2b on 10b 10k times.
#[no_mangle]
#[cfg(feature = "protocol_feature_evm")]
pub unsafe fn blake2b_10b_10k() {
    let buffer = [65u8; 10];
    for _ in 0..10_000 {
        blake2b(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}
// Function to measure `blake2b_base` and `blake2b_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `blake2b` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute blake2b on 10kib 10k times.
#[no_mangle]
#[cfg(feature = "protocol_feature_evm")]
pub unsafe fn blake2b_10kib_10k() {
    let buffer = [65u8; 10240];
    for _ in 0..10_000 {
        blake2b(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}

/// Function to measure the `blake2b_f_base` and `blake2b_byte`. Also measures `base`,
/// `write_register_base`, and `write_register_byte`. This is done at cost per
/// round.
#[no_mangle]
#[cfg(feature = "protocol_feature_evm")]
pub unsafe fn blake2b_f_1r_10k() {
    let rounds: [u8; 4] = 1_u32.to_le_bytes();
    let h: [u64; 8] = [
        0x6a09e667f2bdc948,
        0xbb67ae8584caa73b,
        0x3c6ef372fe94f82b,
        0xa54ff53a5f1d36f1,
        0x510e527fade682d1,
        0x9b05688c2b3e6c1f,
        0x1f83d9abfb41bd6b,
        0x5be0cd19137e2179,
    ];
    let m: [u64; 16] = [
        0x0000000000636261,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
        0x0000000000000000,
    ];
    let t: [u64; 2] = [3, 0];
    let f: [u8; 1] = 1_u8.to_le_bytes();

    for _ in 0..10_000 {
        blake2b_f(rounds.as_ptr() as u64, h.as_ptr() as u64, m.as_ptr() as u64, t.as_ptr() as u64, f.as_ptr() as u64, 0);
    }
}

// Function to measure `ecrecover_base`. Also measures `base`, `write_register_base`, and
// `write_register_byte`. However `ecrecover` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute ecrecover 10k times.
#[no_mangle]
#[cfg(feature = "protocol_feature_evm")]
pub unsafe fn ecrecover_10k() {
    let hash = [0u8; 32];
    let signature = [0u8; 65];
    let (r, s, v) = (&signature[0..32], &signature[32..64], 27);
    for _ in 0..10_000 {
        ecrecover(hash.as_ptr() as _, v, r.as_ptr() as _, s.as_ptr() as _, 0);
    }
}

// Function to measure `alt_bn128_g1_multiexp_base` and `alt_bn128_g1_multiexp_sublinear`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `g1_multiexp` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute g1_multiexp on 1 element 1k times.
#[cfg(feature = "protocol_feature_alt_bn128")]
#[no_mangle]
pub unsafe fn alt_bn128_g1_multiexp_1_1k() {
    let buffer: [u8; 100] = [
        1, 0, 0, 0, 16, 238, 91, 161, 241, 22, 172, 158, 138, 252, 202, 212, 136, 37, 110, 231,
        118, 220, 8, 45, 14, 153, 125, 217, 227, 87, 238, 238, 31, 138, 226, 8, 238, 185, 12, 155,
        93, 126, 144, 248, 200, 177, 46, 245, 40, 162, 169, 80, 150, 211, 157, 13, 10, 36, 44, 232,
        173, 32, 32, 115, 123, 2, 9, 47, 190, 148, 181, 91, 69, 6, 83, 40, 65, 222, 251, 70, 81,
        73, 60, 142, 130, 217, 176, 20, 69, 75, 40, 167, 41, 180, 244, 5, 142, 215, 135, 35,
    ];
    for _ in 0..1_000 {
        alt_bn128_g1_multiexp(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}
// Function to measure `alt_bn128_g1_multiexp_base` and `alt_bn128_g1_multiexp_sublinear`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `g1_multiexp` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute g1_multiexp on 10 elements 1k times.
#[cfg(feature = "protocol_feature_alt_bn128")]
#[no_mangle]
pub unsafe fn alt_bn128_g1_multiexp_10_1k() {
    let buffer: [u8; 964] = [
        10, 0, 0, 0, 160, 75, 88, 1, 39, 49, 57, 54, 144, 97, 157, 53, 186, 197, 3, 255, 101, 190,
        210, 68, 165, 67, 219, 118, 134, 41, 207, 184, 16, 195, 15, 6, 248, 45, 160, 73, 157, 19,
        206, 183, 155, 145, 214, 79, 111, 126, 176, 149, 80, 249, 68, 90, 62, 163, 162, 67, 252,
        47, 4, 54, 29, 49, 53, 22, 12, 56, 213, 113, 211, 78, 202, 179, 173, 211, 178, 235, 0, 245,
        228, 42, 143, 64, 247, 122, 22, 211, 114, 92, 83, 200, 105, 203, 39, 32, 63, 27, 37, 7, 26,
        98, 65, 31, 35, 122, 129, 78, 25, 86, 148, 57, 116, 70, 153, 93, 194, 58, 97, 220, 203,
        215, 224, 39, 69, 225, 33, 127, 203, 22, 208, 87, 64, 209, 160, 50, 233, 197, 79, 248, 194,
        81, 89, 254, 17, 41, 109, 220, 150, 155, 5, 95, 228, 40, 12, 165, 190, 58, 247, 134, 209,
        14, 244, 210, 233, 80, 103, 48, 162, 222, 169, 166, 178, 7, 251, 120, 170, 5, 196, 223,
        179, 12, 240, 219, 64, 15, 210, 140, 180, 12, 98, 193, 51, 35, 101, 235, 228, 31, 160, 165,
        255, 231, 8, 230, 135, 9, 86, 42, 196, 160, 234, 133, 17, 42, 106, 87, 229, 48, 163, 73,
        23, 56, 106, 110, 233, 14, 246, 209, 251, 163, 97, 223, 184, 57, 122, 44, 248, 21, 143,
        156, 140, 178, 208, 231, 246, 42, 160, 28, 169, 134, 186, 33, 111, 79, 176, 193, 114, 4,
        32, 6, 91, 170, 245, 35, 191, 116, 68, 155, 138, 161, 69, 253, 181, 174, 197, 58, 207, 32,
        247, 69, 40, 226, 110, 106, 97, 188, 185, 71, 5, 4, 30, 201, 144, 105, 155, 88, 21, 238,
        82, 231, 38, 215, 187, 109, 64, 45, 213, 112, 123, 117, 182, 34, 17, 120, 137, 153, 181,
        73, 103, 173, 175, 12, 56, 100, 23, 159, 145, 19, 214, 51, 118, 221, 156, 208, 163, 123,
        95, 75, 61, 175, 45, 246, 245, 37, 37, 71, 237, 70, 168, 37, 191, 161, 129, 37, 141, 72,
        92, 238, 166, 71, 116, 3, 194, 153, 215, 33, 137, 244, 155, 246, 177, 218, 156, 27, 134,
        19, 3, 52, 87, 215, 30, 182, 153, 244, 230, 5, 130, 175, 69, 182, 170, 81, 194, 158, 15,
        103, 192, 202, 12, 245, 20, 187, 241, 59, 70, 185, 207, 220, 215, 180, 202, 0, 201, 132,
        203, 112, 218, 23, 190, 111, 131, 109, 225, 24, 24, 109, 165, 174, 0, 243, 229, 234, 191,
        178, 12, 139, 167, 219, 113, 35, 210, 187, 238, 171, 58, 57, 92, 204, 80, 13, 68, 230, 101,
        222, 230, 122, 127, 221, 26, 145, 121, 2, 186, 153, 57, 195, 155, 167, 252, 162, 74, 29,
        145, 71, 152, 182, 106, 222, 251, 31, 18, 14, 228, 81, 31, 41, 119, 231, 33, 160, 222, 228,
        135, 22, 160, 1, 145, 232, 241, 77, 68, 48, 110, 27, 98, 73, 222, 194, 197, 218, 69, 38,
        186, 6, 174, 192, 184, 222, 106, 213, 150, 163, 152, 137, 169, 149, 188, 21, 169, 3, 85,
        97, 60, 105, 222, 87, 73, 77, 23, 46, 80, 109, 53, 143, 249, 35, 241, 199, 237, 191, 208,
        242, 112, 249, 123, 67, 165, 9, 177, 232, 215, 242, 100, 37, 64, 150, 94, 143, 49, 113, 14,
        203, 192, 93, 62, 218, 80, 26, 100, 0, 47, 223, 75, 179, 152, 89, 133, 183, 139, 6, 111,
        147, 236, 47, 156, 169, 164, 176, 65, 147, 60, 46, 71, 153, 231, 71, 64, 6, 38, 41, 56,
        117, 9, 136, 151, 236, 181, 209, 129, 186, 40, 145, 252, 36, 84, 156, 109, 229, 49, 247,
        39, 108, 174, 77, 146, 123, 109, 159, 82, 128, 217, 37, 242, 91, 130, 131, 75, 140, 17, 84,
        183, 169, 177, 27, 254, 198, 240, 241, 177, 230, 22, 152, 47, 158, 83, 224, 126, 59, 109,
        134, 230, 230, 202, 39, 235, 0, 171, 107, 245, 236, 211, 78, 253, 88, 231, 142, 235, 226,
        72, 251, 47, 190, 102, 103, 42, 243, 147, 160, 94, 110, 252, 188, 127, 208, 129, 21, 113,
        64, 101, 51, 63, 70, 181, 9, 13, 27, 129, 6, 132, 11, 122, 0, 14, 141, 42, 104, 214, 180,
        72, 237, 163, 4, 25, 169, 236, 244, 6, 1, 49, 147, 150, 137, 36, 231, 125, 113, 18, 113,
        80, 86, 72, 98, 64, 52, 199, 187, 237, 189, 196, 37, 57, 172, 59, 169, 228, 72, 130, 172,
        79, 43, 82, 84, 39, 25, 71, 8, 250, 3, 126, 110, 4, 157, 215, 43, 88, 57, 103, 237, 236,
        231, 237, 246, 224, 75, 47, 150, 229, 43, 117, 115, 81, 5, 199, 238, 36, 161, 135, 182, 77,
        82, 173, 216, 141, 86, 249, 210, 110, 146, 61, 39, 46, 229, 119, 128, 133, 113, 236, 89,
        99, 52, 156, 51, 116, 7, 234, 246, 120, 109, 70, 97, 226, 124, 146, 197, 14, 48, 220, 18,
        81, 103, 227, 12, 240, 155, 220, 125, 90, 45, 173, 31, 95, 162, 175, 27, 82, 36, 46, 144,
        238, 51, 121, 134, 255, 49, 104, 119, 209, 185, 131, 214, 61, 148, 48, 188, 108, 119, 101,
        189, 223, 152, 191, 180, 146, 17, 179, 38, 209, 36, 26, 208, 85, 141, 208, 135, 207, 174,
        123, 138, 185, 167, 96, 13, 1, 83, 159, 80, 96, 145, 194, 57, 153, 195, 15, 11, 92, 13, 8,
        195, 196, 7, 210, 90, 26, 178, 71, 105, 235, 10, 78, 33, 99, 232, 208, 135, 210, 118, 233,
        9, 5, 90, 12, 7, 82, 87, 246, 221, 98, 145, 122, 59, 230, 46,
    ];
    for _ in 0..1_000 {
        alt_bn128_g1_multiexp(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}

// Function to measure `alt_bn128_g1_sum_base` and `alt_bn128_g1_sum_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `g1_sum` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute g1_sum on 1 element 1k times.
#[cfg(feature = "protocol_feature_alt_bn128")]
#[no_mangle]
pub unsafe fn alt_bn128_g1_sum_1_1k() {
    let buffer: [u8; 69] = [
        1, 0, 0, 0, 0, 11, 49, 94, 29, 152, 111, 116, 138, 248, 2, 184, 8, 159, 80, 169, 45, 149,
        48, 32, 49, 37, 6, 133, 105, 171, 194, 120, 44, 195, 17, 180, 35, 137, 154, 4, 192, 211,
        244, 93, 200, 2, 44, 0, 64, 26, 108, 139, 147, 88, 235, 242, 23, 253, 52, 110, 236, 67, 99,
        176, 2, 186, 198, 228, 25,
    ];
    for _ in 0..1_000 {
        alt_bn128_g1_sum(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}

// Function to measure `alt_bn128_g1_sum_base` and `alt_bn128_g1_sum_byte`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `g1_sum` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute g1_sum on 10 element 1k times.
#[cfg(feature = "protocol_feature_alt_bn128")]
#[no_mangle]
pub unsafe fn alt_bn128_g1_sum_10_1k() {
    let buffer: [u8; 654] = [
        10, 0, 0, 0, 0, 35, 69, 193, 29, 159, 231, 101, 1, 196, 134, 150, 68, 208, 119, 157, 39,
        248, 90, 73, 19, 10, 8, 235, 127, 161, 218, 15, 115, 180, 172, 209, 16, 228, 156, 170, 43,
        38, 198, 86, 36, 148, 8, 79, 237, 137, 133, 67, 225, 144, 247, 98, 177, 65, 52, 2, 51, 108,
        108, 235, 101, 71, 15, 89, 24, 0, 61, 2, 168, 116, 29, 2, 121, 230, 138, 133, 50, 222, 81,
        136, 130, 255, 228, 187, 237, 97, 226, 24, 24, 225, 228, 221, 102, 101, 88, 154, 127, 43,
        21, 183, 14, 55, 239, 203, 66, 188, 194, 60, 232, 17, 100, 73, 170, 174, 44, 131, 89, 176,
        26, 43, 123, 173, 38, 238, 5, 204, 224, 211, 6, 38, 1, 22, 233, 101, 119, 186, 92, 128,
        212, 29, 189, 170, 207, 212, 163, 98, 74, 31, 168, 145, 29, 1, 168, 123, 195, 7, 243, 15,
        22, 186, 1, 105, 30, 177, 138, 63, 142, 128, 124, 81, 157, 189, 145, 114, 71, 91, 192, 243,
        19, 214, 74, 141, 149, 84, 3, 97, 51, 101, 221, 243, 63, 34, 43, 44, 12, 0, 222, 145, 152,
        153, 248, 97, 163, 68, 51, 35, 234, 159, 72, 239, 0, 78, 56, 37, 239, 175, 121, 224, 50,
        128, 51, 230, 122, 231, 96, 31, 204, 0, 239, 255, 21, 120, 202, 147, 235, 55, 165, 147,
        205, 33, 240, 15, 47, 163, 189, 125, 84, 26, 232, 163, 132, 250, 148, 136, 2, 15, 114, 159,
        178, 29, 1, 175, 114, 55, 82, 223, 6, 219, 68, 103, 204, 9, 215, 220, 64, 99, 224, 12, 186,
        46, 90, 74, 93, 66, 74, 211, 119, 163, 10, 36, 239, 175, 31, 80, 201, 192, 101, 188, 227,
        193, 181, 51, 8, 239, 157, 158, 28, 92, 28, 186, 197, 20, 161, 153, 183, 226, 236, 231, 56,
        22, 141, 20, 194, 202, 6, 1, 127, 32, 22, 163, 172, 30, 54, 118, 159, 178, 75, 182, 19,
        149, 171, 185, 113, 11, 203, 125, 150, 24, 123, 18, 110, 134, 15, 28, 181, 224, 95, 26, 83,
        22, 56, 133, 230, 240, 166, 228, 89, 241, 249, 38, 150, 59, 240, 175, 146, 52, 184, 240,
        226, 13, 10, 214, 175, 134, 37, 217, 96, 163, 7, 27, 1, 198, 145, 187, 197, 240, 31, 72,
        191, 158, 229, 28, 10, 102, 2, 225, 215, 100, 142, 183, 59, 15, 182, 207, 95, 163, 199,
        235, 54, 87, 154, 106, 32, 193, 169, 8, 160, 52, 171, 14, 186, 158, 60, 211, 82, 31, 224,
        46, 82, 29, 176, 164, 166, 141, 148, 67, 245, 192, 14, 144, 163, 200, 72, 119, 20, 1, 146,
        85, 5, 23, 90, 242, 183, 140, 72, 71, 210, 248, 1, 106, 24, 44, 12, 88, 216, 245, 83, 140,
        157, 142, 220, 215, 110, 181, 223, 192, 104, 9, 255, 142, 7, 108, 36, 73, 219, 134, 59,
        163, 13, 120, 153, 208, 66, 18, 91, 48, 15, 151, 121, 11, 183, 176, 89, 170, 89, 187, 165,
        219, 130, 34, 0, 67, 100, 228, 203, 250, 180, 250, 94, 237, 191, 144, 155, 198, 81, 168,
        159, 196, 94, 122, 171, 96, 15, 39, 203, 187, 224, 94, 43, 81, 11, 114, 27, 236, 70, 0,
        241, 246, 107, 68, 165, 82, 117, 90, 154, 141, 206, 102, 4, 145, 66, 144, 215, 244, 227,
        164, 252, 128, 205, 233, 51, 122, 213, 232, 20, 1, 173, 37, 223, 11, 68, 227, 111, 46, 246,
        210, 31, 181, 36, 149, 221, 113, 27, 34, 156, 35, 82, 144, 38, 34, 165, 43, 130, 70, 86,
        34, 148, 30, 15, 207, 202, 218, 30, 22, 143, 20, 58, 60, 133, 67, 150, 255, 65, 29, 217,
        94, 89, 182, 13, 160, 65, 173, 5, 232, 223, 65, 213, 210, 55, 25,
    ];
    for _ in 0..1_000 {
        alt_bn128_g1_sum(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64, 0);
    }
}

// Function to measure `alt_bn128_pairing_check_base` and `alt_bn128_pairing_check_element`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `g1_multiexp` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute pairing_check on 1 element 1k times.
#[cfg(feature = "protocol_feature_alt_bn128")]
#[no_mangle]
pub unsafe fn alt_bn128_pairing_check_1_1k() {
    let buffer: [u8; 196] = [
        1, 0, 0, 0, 80, 12, 4, 181, 61, 254, 153, 52, 127, 228, 174, 24, 144, 95, 235, 26, 197,
        188, 219, 91, 4, 47, 98, 98, 202, 199, 94, 67, 211, 223, 197, 21, 65, 221, 184, 75, 69,
        202, 13, 56, 6, 233, 217, 146, 159, 141, 116, 208, 81, 224, 146, 124, 150, 114, 218, 196,
        192, 233, 253, 31, 130, 152, 144, 29, 34, 54, 229, 82, 80, 13, 200, 53, 254, 193, 250, 1,
        205, 60, 38, 172, 237, 29, 18, 82, 187, 98, 113, 152, 184, 251, 223, 42, 104, 148, 253, 25,
        79, 39, 165, 18, 195, 165, 215, 155, 168, 251, 250, 2, 215, 214, 193, 172, 187, 84, 54,
        168, 27, 100, 161, 155, 144, 95, 199, 238, 88, 238, 202, 46, 247, 97, 33, 56, 78, 174, 171,
        15, 245, 5, 121, 144, 88, 81, 102, 133, 118, 222, 81, 214, 74, 169, 27, 91, 27, 23, 80, 55,
        43, 97, 101, 24, 168, 29, 75, 136, 229, 2, 55, 77, 60, 200, 227, 210, 172, 194, 232, 45,
        151, 46, 248, 206, 193, 250, 145, 84, 78, 176, 74, 210, 0, 106, 168, 30,
    ];
    for _ in 0..1_000 {
        alt_bn128_pairing_check(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    }
}
// Function to measure `alt_bn128_g1_multiexp_base` and `alt_bn128_g1_multiexp_sublinear`. Also measures `base`, `write_register_base`,
// and `write_register_byte`. However `g1_multiexp` computation is more expensive than register writing
// so we are okay overcharging it.
// Compute pairing_check on 10 elements 1k times.
#[cfg(feature = "protocol_feature_alt_bn128")]
#[no_mangle]
pub unsafe fn alt_bn128_pairing_check_10_1k() {
    let buffer: [u8; 1924] = [
        10, 0, 0, 0, 223, 6, 200, 33, 166, 188, 103, 53, 200, 168, 35, 109, 73, 23, 33, 99, 238,
        116, 162, 85, 150, 159, 255, 82, 85, 22, 102, 22, 188, 22, 105, 12, 248, 15, 147, 127, 39,
        16, 214, 51, 106, 17, 235, 64, 94, 132, 235, 26, 178, 3, 134, 136, 133, 222, 114, 36, 21,
        172, 70, 82, 39, 198, 18, 25, 148, 200, 134, 248, 104, 246, 245, 98, 240, 216, 24, 152, 37,
        21, 194, 154, 66, 240, 198, 134, 174, 64, 155, 105, 1, 59, 115, 35, 63, 254, 41, 13, 9,
        213, 97, 49, 90, 178, 1, 96, 163, 189, 16, 152, 31, 108, 62, 36, 48, 97, 253, 154, 196,
        156, 74, 171, 10, 19, 213, 236, 176, 205, 172, 23, 240, 49, 89, 79, 195, 106, 221, 106,
        236, 127, 76, 42, 160, 144, 208, 67, 233, 255, 233, 202, 88, 148, 99, 71, 51, 235, 236,
        251, 1, 108, 127, 3, 202, 160, 68, 30, 150, 211, 73, 21, 193, 117, 125, 153, 165, 246, 182,
        191, 251, 26, 186, 198, 153, 218, 196, 234, 241, 135, 159, 184, 33, 214, 235, 4, 229, 38,
        210, 21, 178, 61, 170, 167, 16, 18, 51, 233, 225, 101, 198, 72, 53, 5, 223, 97, 78, 49, 54,
        101, 37, 6, 196, 16, 9, 255, 173, 6, 6, 141, 167, 242, 31, 118, 141, 162, 125, 98, 127,
        242, 9, 144, 222, 175, 9, 217, 39, 246, 105, 3, 244, 130, 192, 39, 67, 47, 134, 150, 185,
        2, 0, 3, 183, 195, 187, 101, 121, 183, 52, 251, 254, 149, 175, 97, 17, 209, 129, 221, 149,
        4, 175, 47, 128, 5, 253, 131, 89, 231, 88, 69, 161, 32, 236, 15, 165, 1, 226, 253, 65, 154,
        122, 61, 175, 78, 146, 202, 39, 228, 214, 15, 229, 88, 122, 165, 40, 247, 52, 154, 251, 14,
        129, 223, 11, 24, 90, 186, 200, 9, 91, 165, 129, 250, 69, 190, 83, 250, 209, 188, 250, 16,
        124, 179, 106, 66, 228, 50, 142, 82, 119, 98, 193, 50, 251, 92, 141, 37, 194, 136, 10, 231,
        216, 165, 140, 225, 143, 243, 23, 238, 11, 153, 112, 148, 78, 52, 22, 19, 65, 198, 84, 212,
        199, 1, 2, 176, 190, 45, 113, 47, 22, 76, 141, 39, 200, 16, 110, 85, 83, 226, 99, 154, 70,
        185, 9, 80, 124, 246, 104, 244, 25, 82, 182, 188, 240, 197, 133, 88, 187, 99, 13, 19, 30,
        252, 168, 144, 127, 37, 159, 58, 10, 136, 149, 123, 148, 244, 27, 24, 169, 132, 25, 59,
        193, 113, 137, 14, 170, 53, 31, 89, 254, 231, 248, 2, 109, 112, 187, 231, 175, 83, 200,
        247, 55, 103, 34, 199, 225, 228, 60, 108, 17, 36, 199, 118, 66, 18, 143, 44, 72, 185, 139,
        231, 169, 26, 230, 5, 133, 86, 209, 85, 83, 146, 170, 132, 27, 254, 32, 200, 150, 48, 223,
        59, 22, 7, 152, 161, 92, 250, 197, 97, 110, 97, 130, 64, 15, 70, 29, 40, 201, 19, 234, 144,
        223, 253, 140, 81, 104, 96, 153, 87, 32, 155, 243, 59, 72, 111, 132, 41, 67, 126, 10, 208,
        188, 92, 65, 179, 54, 135, 27, 17, 105, 197, 203, 93, 59, 5, 131, 70, 56, 170, 247, 72, 61,
        196, 13, 150, 182, 233, 243, 19, 118, 107, 8, 132, 154, 245, 11, 190, 186, 14, 69, 2, 114,
        177, 108, 197, 9, 236, 140, 81, 182, 114, 134, 205, 144, 72, 24, 155, 29, 185, 247, 187,
        253, 81, 96, 143, 178, 62, 220, 167, 32, 191, 122, 35, 18, 119, 54, 255, 6, 205, 32, 214,
        226, 216, 250, 191, 250, 127, 163, 67, 0, 79, 249, 153, 81, 204, 23, 86, 240, 192, 141, 83,
        22, 91, 255, 3, 248, 13, 239, 47, 56, 141, 228, 144, 148, 199, 15, 129, 168, 215, 50, 181,
        243, 87, 253, 184, 135, 137, 120, 201, 203, 116, 173, 123, 57, 239, 128, 16, 138, 214, 88,
        219, 117, 54, 3, 72, 135, 77, 63, 83, 38, 40, 174, 86, 176, 17, 245, 233, 150, 23, 129, 78,
        241, 175, 147, 94, 6, 67, 129, 43, 19, 48, 170, 59, 116, 74, 103, 52, 175, 228, 68, 169,
        78, 158, 97, 0, 240, 42, 65, 177, 34, 231, 40, 52, 84, 133, 25, 52, 49, 70, 60, 47, 184,
        223, 165, 176, 80, 235, 157, 43, 212, 24, 128, 175, 82, 107, 208, 66, 191, 85, 21, 162,
        214, 205, 28, 33, 104, 213, 32, 250, 109, 42, 18, 29, 170, 231, 186, 171, 126, 67, 218, 47,
        205, 136, 168, 13, 190, 186, 202, 200, 239, 184, 31, 99, 98, 201, 226, 94, 205, 77, 164,
        32, 238, 244, 183, 7, 130, 196, 74, 163, 122, 189, 118, 4, 121, 28, 183, 243, 104, 226,
        144, 141, 42, 215, 93, 251, 83, 207, 229, 37, 154, 231, 64, 162, 209, 56, 190, 37, 22, 4,
        93, 106, 34, 167, 39, 254, 224, 210, 205, 5, 62, 77, 150, 86, 109, 161, 250, 136, 75, 218,
        159, 26, 85, 109, 215, 194, 20, 142, 174, 25, 215, 255, 214, 125, 44, 2, 176, 114, 9, 241,
        236, 64, 124, 185, 132, 142, 190, 187, 227, 198, 240, 89, 47, 222, 130, 27, 242, 48, 157,
        33, 150, 16, 42, 220, 107, 124, 189, 43, 191, 62, 17, 211, 40, 243, 233, 26, 247, 205, 68,
        253, 105, 132, 129, 167, 169, 231, 109, 154, 144, 73, 234, 55, 45, 42, 6, 118, 244, 125,
        13, 42, 146, 138, 199, 195, 104, 254, 221, 172, 220, 19, 98, 176, 166, 227, 9, 159, 205,
        50, 55, 21, 92, 205, 86, 224, 189, 45, 219, 141, 11, 233, 254, 113, 213, 120, 49, 108, 208,
        48, 146, 194, 176, 70, 175, 40, 150, 80, 217, 3, 63, 71, 190, 71, 2, 224, 157, 116, 169,
        31, 149, 185, 247, 136, 26, 251, 84, 39, 165, 100, 221, 119, 163, 169, 44, 199, 98, 5, 43,
        93, 161, 203, 119, 184, 239, 149, 81, 118, 89, 177, 132, 16, 187, 184, 93, 144, 191, 32,
        240, 106, 13, 221, 213, 150, 41, 219, 192, 93, 123, 50, 102, 28, 85, 119, 4, 187, 88, 137,
        187, 7, 226, 182, 127, 45, 49, 30, 169, 231, 65, 157, 58, 236, 152, 165, 199, 127, 44, 23,
        193, 8, 76, 228, 74, 249, 229, 106, 252, 158, 124, 74, 80, 126, 158, 52, 54, 27, 8, 196,
        244, 50, 105, 103, 26, 79, 82, 90, 130, 62, 66, 183, 154, 109, 229, 90, 217, 224, 185, 63,
        112, 126, 139, 240, 205, 115, 221, 181, 126, 1, 56, 43, 132, 131, 24, 46, 60, 97, 55, 190,
        161, 9, 223, 0, 231, 197, 81, 28, 62, 51, 111, 199, 191, 102, 153, 200, 221, 41, 108, 162,
        28, 32, 30, 53, 135, 154, 144, 29, 186, 12, 158, 167, 159, 120, 102, 34, 6, 234, 82, 202,
        81, 140, 238, 236, 160, 66, 83, 7, 160, 64, 138, 124, 20, 37, 56, 242, 83, 131, 176, 81,
        145, 228, 107, 199, 221, 120, 175, 245, 18, 236, 227, 216, 44, 173, 40, 106, 57, 146, 139,
        114, 233, 100, 24, 116, 35, 27, 223, 176, 180, 99, 164, 136, 62, 8, 8, 82, 238, 208, 224,
        98, 97, 42, 114, 206, 98, 212, 75, 240, 58, 21, 183, 137, 115, 81, 155, 31, 31, 17, 80,
        112, 210, 23, 25, 68, 31, 225, 252, 60, 49, 51, 226, 224, 81, 21, 222, 223, 186, 133, 244,
        185, 80, 41, 251, 135, 89, 18, 139, 82, 232, 43, 136, 90, 253, 241, 7, 189, 37, 249, 86,
        118, 175, 107, 153, 163, 117, 8, 0, 150, 45, 244, 76, 47, 173, 109, 40, 111, 245, 45, 22,
        4, 184, 15, 103, 17, 28, 179, 1, 74, 62, 22, 242, 240, 227, 91, 179, 37, 200, 144, 102,
        208, 255, 230, 20, 241, 179, 8, 116, 94, 64, 59, 179, 102, 104, 0, 15, 85, 82, 165, 6, 84,
        191, 14, 237, 31, 189, 18, 247, 6, 164, 106, 224, 231, 24, 196, 118, 52, 45, 51, 249, 19,
        100, 2, 126, 100, 224, 16, 188, 81, 64, 1, 130, 153, 112, 55, 135, 138, 0, 31, 89, 27, 51,
        208, 84, 98, 235, 218, 106, 229, 138, 27, 232, 252, 53, 183, 27, 90, 19, 11, 40, 240, 97,
        126, 62, 32, 190, 177, 113, 254, 3, 136, 251, 27, 61, 148, 65, 195, 47, 94, 18, 66, 163,
        169, 231, 122, 2, 158, 183, 37, 105, 33, 59, 108, 132, 78, 184, 105, 25, 221, 5, 76, 192,
        207, 127, 194, 208, 92, 50, 197, 84, 215, 146, 92, 150, 23, 207, 90, 90, 166, 39, 206, 253,
        42, 251, 16, 74, 4, 176, 247, 209, 65, 131, 177, 6, 248, 111, 156, 152, 123, 77, 197, 141,
        132, 169, 93, 79, 95, 195, 92, 175, 229, 29, 186, 20, 42, 85, 130, 115, 201, 145, 81, 5,
        152, 86, 225, 6, 226, 70, 106, 161, 229, 158, 245, 168, 47, 183, 104, 24, 217, 165, 70,
        154, 93, 111, 149, 70, 33, 117, 0, 122, 107, 244, 192, 63, 73, 188, 19, 32, 65, 113, 194,
        53, 131, 86, 97, 44, 198, 81, 55, 164, 201, 183, 250, 129, 150, 126, 198, 13, 38, 7, 8,
        206, 60, 150, 88, 196, 65, 198, 1, 230, 202, 250, 254, 101, 92, 219, 104, 228, 96, 35, 194,
        237, 136, 126, 248, 42, 182, 247, 241, 75, 14, 69, 234, 24, 234, 25, 191, 64, 38, 183, 126,
        143, 90, 107, 15, 30, 68, 252, 210, 74, 136, 110, 117, 191, 63, 133, 55, 240, 252, 78, 14,
        25, 43, 243, 191, 152, 161, 242, 117, 141, 220, 27, 5, 218, 83, 156, 204, 94, 173, 227,
        150, 36, 98, 160, 97, 154, 168, 148, 100, 150, 158, 151, 240, 51, 48, 126, 42, 195, 42,
        163, 53, 124, 226, 253, 99, 24, 50, 158, 41, 28, 51, 1, 11, 65, 201, 191, 134, 156, 0, 223,
        239, 198, 207, 180, 243, 118, 28, 98, 173, 195, 236, 75, 113, 170, 12, 120, 49, 188, 11,
        54, 108, 55, 49, 54, 110, 228, 232, 202, 181, 196, 207, 53, 123, 158, 232, 68, 15, 188, 22,
        45, 111, 148, 191, 42, 226, 238, 248, 78, 94, 26, 9, 239, 9, 15, 13, 10, 170, 97, 8, 195,
        153, 36, 144, 198, 4, 224, 209, 40, 187, 94, 48, 201, 232, 156, 89, 230, 160, 89, 200, 132,
        202, 38, 57, 183, 180, 218, 67, 213, 155, 217, 208, 70, 148, 7, 255, 123, 35, 71, 215, 131,
        153, 39, 21, 205, 30, 22, 107, 59, 81, 254, 153, 39, 49, 130, 112, 16, 111, 159, 119, 66,
        72, 251, 247, 86, 214, 89, 173, 104, 226, 32, 13, 82, 209, 195, 2, 207, 8, 240, 251, 117,
        92, 41, 206, 209, 141, 185, 60, 24, 218, 2, 249, 161, 52, 128, 224, 52, 152, 5, 227, 58,
        198, 54, 248, 183, 181, 28, 8, 159, 184, 36, 32, 149, 142, 86, 69, 184, 53, 85, 214, 224,
        181, 36, 44, 114, 27, 177, 213, 179, 159, 135, 52, 213, 192, 251, 150, 85, 145, 118, 6,
        218, 177, 105, 20, 215, 89, 47, 78, 157, 25, 81, 85, 123, 18, 86, 88, 137, 86, 89, 39, 173,
        144, 10, 175, 215, 79, 121, 252, 106, 13, 91, 31,
    ];
    for _ in 0..1_000 {
        alt_bn128_pairing_check(buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
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

// Function to subtract the base from the `data_receipt_creation_config`. This method doesn't
// have a callback on created promises so there is no data dependency.
#[no_mangle]
pub unsafe fn data_receipt_base_10b_1000() {
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
    let _id = promise_and(ids.as_ptr() as _, ids.len() as _);
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
