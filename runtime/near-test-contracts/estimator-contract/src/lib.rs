// cspell:ignore NDSAXO, kajdlfkjalkfjaklfjdkladjfkljadsk, utjz
// cspell:ignore noooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooop

#![no_std]
#![allow(non_snake_case)]
#![allow(clippy::all)]

#[panic_handler]
pub fn panic(_info: &::core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
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
    let buffer = [0u8; 10];
    write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    for _ in 0..10_000 {
        read_register(0, buffer.as_ptr() as *const u64 as u64);
    }
}

// Function to measure `write_memory_base` and `write_memory_byte` many times.
// Writes 1Mib 10k times into memory. Includes `read_register` costs.
#[unsafe(no_mangle)]
pub unsafe fn write_memory_1Mib_10k() {
    let buffer = [0u8; 1024 * 1024];
    write_register(0, buffer.len() as u64, buffer.as_ptr() as *const u64 as u64);
    for _ in 0..10_000 {
        read_register(0, buffer.as_ptr() as *const u64 as u64);
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
        assert_eq!(bls12381_p1_sum(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p1_sum_50_100() {
    let buffer: [[u8; 2*97]; 25] = [[0, 18, 25, 108, 90, 67, 214, 146, 36, 216, 113, 51, 137, 40, 95, 38, 185, 143, 134, 238, 145, 10, 179, 221, 102, 142, 65, 55, 56, 40, 32, 3, 204, 91, 115, 87, 175, 154, 122, 245, 75, 183, 19, 214, 34, 85, 232, 15, 86, 6, 186, 129, 2, 191, 190, 234, 68, 22, 183, 16, 199, 62, 140, 206, 48, 50, 195, 28, 98, 105, 196, 73, 6, 248, 172, 79, 120, 116, 206, 153, 251, 23, 85, 153, 146, 72, 101, 40, 150, 56, 132, 206, 66, 154, 153, 47, 238,
        0, 0, 1, 16, 16, 152, 245, 195, 152, 147, 118, 87, 102, 175, 69, 18, 160, 199, 78, 27, 184, 155, 199, 230, 253, 241, 78, 62, 115, 55, 210, 87, 204, 15, 148, 101, 129, 121, 216, 51, 32, 185, 159, 49, 255, 148, 205, 43, 172, 3, 225, 169, 249, 244, 76, 162, 205, 171, 79, 67, 161, 163, 238, 52, 112, 253, 249, 11, 47, 194, 40, 235, 59, 112, 159, 205, 114, 240, 20, 131, 138, 200, 42, 109, 121, 122, 238, 254, 217, 160, 128, 75, 34, 237, 28, 232, 247]; 25];

    for _ in 0..100 {
        assert_eq!(bls12381_p1_sum(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p2_sum_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(bls12381_p2_sum(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p2_sum_50_100() {
    let buffer: [[u8; 2*193]; 25] = [
        [0, 12, 199, 10, 88, 127, 70, 82, 3, 157, 129, 23, 182, 16, 56, 88, 173, 205, 151, 40, 246, 174, 190, 35, 5, 120, 56, 154, 98, 218, 0, 66, 183, 98, 59, 28, 4, 54, 115, 79, 70, 60, 253, 209, 135, 210, 9, 3, 36, 24, 192, 173, 166, 53, 27, 112, 102, 31, 5, 51, 101, 222, 174, 86, 145, 7, 152, 189, 42, 206, 110, 43, 246, 186, 65, 146, 209, 162, 41, 150, 127, 106, 246, 202, 28, 154, 138, 17, 235, 192, 162, 50, 52, 78, 224, 246, 214, 7, 155, 165, 13, 37, 17, 99, 27, 32, 182, 214, 243, 132, 30, 97, 110, 157, 17, 182, 142, 195, 54, 140, 214, 1, 41, 217, 212, 120, 122, 181, 108, 78, 145, 69, 163, 137, 39, 229, 28, 156, 214, 39, 29, 73, 61, 147, 136, 9, 245, 11, 215, 190, 237, 178, 51, 40, 129, 143, 159, 253, 175, 219, 109, 166, 164, 221, 128, 197, 169, 4, 138, 184, 177, 84, 223, 60, 173, 147, 140, 206, 222, 130, 159, 17, 86, 247, 105, 217, 225, 73, 121, 30, 142, 12, 217,
         0, 9, 174, 177, 12, 55, 43, 94, 241, 1, 6, 117, 198, 164, 118, 47, 218, 51, 99, 100, 137, 194, 59, 88, 28, 117, 34, 5, 137, 175, 188, 12, 196, 98, 73, 249, 33, 238, 160, 45, 209, 183, 97, 224, 54, 255, 219, 174, 34, 25, 47, 165, 216, 115, 47, 249, 243, 142, 11, 28, 241, 46, 173, 253, 38, 8, 240, 199, 163, 154, 206, 215, 116, 104, 55, 131, 58, 226, 83, 187, 87, 239, 156, 13, 152, 164, 182, 158, 235, 41, 80, 144, 25, 23, 233, 157, 30, 23, 72, 130, 205, 211, 85, 30, 12, 230, 23, 136, 97, 255, 131, 225, 149, 254, 203, 207, 253, 83, 166, 123, 111, 16, 180, 67, 30, 66, 62, 40, 164, 128, 50, 127, 235, 231, 2, 118, 3, 111, 96, 187, 156, 153, 207, 118, 51, 2, 210, 37, 68, 118, 0, 212, 159, 147, 43, 157, 211, 202, 30, 105, 89, 105, 122, 166, 3, 231, 77, 134, 102, 104, 26, 45, 202, 129, 96, 195, 133, 118, 104, 174, 7, 68, 64, 54, 102, 25, 235, 137, 32, 37, 108, 78, 74]; 25];

    for _ in 0..100 {
        assert_eq!(bls12381_p2_sum(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_g1_multiexp_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(bls12381_g1_multiexp(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_g1_multiexp_50_100() {
    let buffer: [[u8; 96 + 32]; 50] = [[23, 241, 211, 167, 49, 151, 215, 148, 38, 149, 99, 140, 79, 169, 172, 15, 195, 104, 140, 79, 151, 116, 185, 5, 161, 78, 58, 63, 23, 27, 172, 88, 108, 85, 232, 63, 249, 122, 26, 239, 251, 58, 240, 10, 219, 34, 198, 187, 8, 179, 244, 129, 227, 170, 160, 241, 160, 158, 48, 237, 116, 29, 138, 228, 252, 245, 224, 149, 213, 208, 10, 246, 0, 219, 24, 203, 44, 4, 179, 237, 208, 60, 199, 68, 162, 136, 138, 228, 12, 170, 35, 41, 70, 197, 231,
        225, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]; 50];

    for _ in 0..100 {
        assert_eq!(bls12381_g1_multiexp(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_g2_multiexp_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(bls12381_g2_multiexp(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_g2_multiexp_50_100() {
    let buffer: [[u8; 192 + 32]; 50] = [[19, 224, 43, 96, 82, 113, 159, 96, 125, 172, 211, 160, 136, 39, 79, 101, 89, 107, 208, 208, 153, 32, 182, 26, 181, 218, 97, 187, 220, 127, 80, 73, 51, 76, 241, 18, 19, 148, 93, 87, 229, 172, 125, 5, 93, 4, 43, 126, 2, 74, 162, 178, 240, 143, 10, 145, 38, 8, 5, 39, 45, 197, 16, 81, 198, 228, 122, 212, 250, 64, 59, 2, 180, 81, 11, 100, 122, 227, 209, 119, 11, 172, 3, 38, 168, 5, 187, 239, 212, 128, 86, 200, 193, 33, 189, 184, 6, 6, 196, 160, 46, 167, 52, 204, 50, 172, 210, 176, 43, 194, 139, 153, 203, 62, 40, 126, 133, 167, 99, 175, 38, 116, 146, 171, 87, 46, 153, 171, 63, 55, 13, 39, 92, 236, 29, 161, 170, 169, 7, 95, 240, 95, 121, 190, 12, 229, 213, 39, 114, 125, 110, 17, 140, 201, 205, 198, 218, 46, 53, 26, 173, 253, 155, 170, 140, 189, 211, 167, 109, 66, 154, 105, 81, 96, 209, 44, 146, 58, 201, 204, 59, 172, 162, 137, 225, 147, 84, 134, 8, 184, 40, 1,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]; 50];

    for _ in 0..100 {
        assert_eq!(bls12381_g2_multiexp(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_map_fp_to_g1_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(bls12381_map_fp_to_g1(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_map_fp_to_g1_50_100() {
    let buffer: [[u8; 48]; 50] = [[20, 64, 110, 91, 251, 146, 9, 37, 106, 56, 32, 135, 154, 41, 172, 47, 98, 214, 172, 168, 35, 36, 191, 58, 226, 170, 125, 60, 84, 121, 32, 67, 189, 140, 121, 31, 204, 219, 8, 12, 26, 82, 220, 104, 184, 182, 147, 80]; 50];

    for _ in 0..100 {
        assert_eq!(bls12381_map_fp_to_g1(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}


#[unsafe(no_mangle)]
pub unsafe fn bls12381_map_fp2_to_g2_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(bls12381_map_fp2_to_g2(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_map_fp2_to_g2_10_100() {
    let buffer: [[u8; 96]; 10] = [[14, 136, 91, 179, 57, 150, 225, 47, 7, 218, 105, 7, 62, 44, 12, 200, 128, 188, 142, 255, 38, 210, 167, 36, 41, 158, 177, 45, 84, 244, 188, 242, 111, 71, 72, 187, 2, 14, 128, 167, 227, 121, 74, 123, 14, 71, 166, 65, 20, 64, 110, 91, 251, 146, 9, 37, 106, 56, 32, 135, 154, 41, 172, 47, 98, 214, 172, 168, 35, 36, 191, 58, 226, 170, 125, 60, 84, 121, 32, 67, 189, 140, 121, 31, 204, 219, 8, 12, 26, 82, 220, 104, 184, 182, 147, 80]; 10];

    for _ in 0..100 {
        assert_eq!(bls12381_map_fp2_to_g2(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_pairing_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(bls12381_pairing_check(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_pairing_5_100() {
    let buffer: [[u8; 288]; 5] = [[23, 241, 211, 167, 49, 151, 215, 148, 38, 149, 99, 140, 79, 169, 172, 15, 195, 104, 140, 79, 151, 116, 185, 5, 161, 78, 58, 63, 23, 27, 172, 88, 108, 85, 232, 63, 249, 122, 26, 239, 251, 58, 240, 10, 219, 34, 198, 187, 8, 179, 244, 129, 227, 170, 160, 241, 160, 158, 48, 237, 116, 29, 138, 228, 252, 245, 224, 149, 213, 208, 10, 246, 0, 219, 24, 203, 44, 4, 179, 237, 208, 60, 199, 68, 162, 136, 138, 228, 12, 170, 35, 41, 70, 197, 231, 225, 19, 224, 43, 96, 82, 113, 159, 96, 125, 172, 211, 160, 136, 39, 79, 101, 89, 107, 208, 208, 153, 32, 182, 26, 181, 218, 97, 187, 220, 127, 80, 73, 51, 76, 241, 18, 19, 148, 93, 87, 229, 172, 125, 5, 93, 4, 43, 126, 2, 74, 162, 178, 240, 143, 10, 145, 38, 8, 5, 39, 45, 197, 16, 81, 198, 228, 122, 212, 250, 64, 59, 2, 180, 81, 11, 100, 122, 227, 209, 119, 11, 172, 3, 38, 168, 5, 187, 239, 212, 128, 86, 200, 193, 33, 189, 184, 6, 6, 196, 160, 46, 167, 52, 204, 50, 172, 210, 176, 43, 194, 139, 153, 203, 62, 40, 126, 133, 167, 99, 175, 38, 116, 146, 171, 87, 46, 153, 171, 63, 55, 13, 39, 92, 236, 29, 161, 170, 169, 7, 95, 240, 95, 121, 190, 12, 229, 213, 39, 114, 125, 110, 17, 140, 201, 205, 198, 218, 46, 53, 26, 173, 253, 155, 170, 140, 189, 211, 167, 109, 66, 154, 105, 81, 96, 209, 44, 146, 58, 201, 204, 59, 172, 162, 137, 225, 147, 84, 134, 8, 184, 40, 1]; 5];


    for _ in 0..100 {
        assert_eq!(bls12381_pairing_check(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64
        ), 2);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p1_decompress_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(bls12381_p1_decompress(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p1_decompress_50_100() {
    let buffer: [[u8; 48]; 50] = [[185, 110, 35, 139, 110, 142, 126, 177, 120, 97, 234, 41, 91, 204, 20, 203, 207, 103, 224, 112, 176, 18, 102, 59, 68, 107, 137, 231, 10, 71, 183, 63, 198, 228, 242, 206, 195, 124, 70, 91, 53, 182, 222, 158, 19, 104, 106, 15]; 50];

    for _ in 0..100 {
        assert_eq!(bls12381_p1_decompress(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p2_decompress_0_100() {
    let buffer: [u8; 0] = [];

    for _ in 0..100 {
        assert_eq!(bls12381_p2_decompress(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
    }
}

#[unsafe(no_mangle)]
pub unsafe fn bls12381_p2_decompress_50_100() {
    let buffer: [[u8; 96]; 50] = [[143, 150, 139, 210, 67, 144, 143, 243, 229, 250, 26, 179, 243, 30, 7, 129, 151, 229, 138, 206, 86, 43, 190, 139, 90, 39, 29, 95, 186, 80, 35, 125, 160, 200, 254, 101, 231, 181, 119, 28, 192, 168, 111, 213, 127, 50, 52, 126, 21, 162, 109, 31, 93, 86, 196, 114, 208, 25, 238, 162, 83, 158, 88, 219, 0, 196, 154, 165, 208, 169, 102, 56, 56, 144, 63, 221, 190, 67, 107, 91, 21, 126, 131, 179, 93, 26, 78, 95, 137, 247, 129, 39, 243, 93, 172, 240]; 50];

    for _ in 0..100 {
        assert_eq!(bls12381_p2_decompress(
            core::mem::size_of_val(&buffer) as u64,
            buffer.as_ptr() as *const u64 as u64,
            0,
        ), 0);
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
    let buf = [0u8; 1000];
    current_account_id(0);
    let buf_len = register_len(0);
    read_register(0, buf.as_ptr() as _);

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

// Function to measure `data_receipt_creation_config`, but we are measure send and execution fee at
// the same time. Produces 40 100kib data receipts.
#[unsafe(no_mangle)]
pub unsafe fn data_receipt_100kib_40() {
    const NUM_RECEIPTS: usize = 40;
    let buf = [0u8; 1000];
    current_account_id(0);
    let buf_len = register_len(0);
    read_register(0, buf.as_ptr() as _);

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
    let input_data = [0u8; MAX_ARG_LEN as usize];
    read_register(0, input_data.as_ptr() as _);

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
    let input_data = [0u8; MAX_ARG_LEN as usize];
    read_register(0, input_data.as_ptr() as _);

    let key_len = u64::from_le_bytes(input_data[..8].try_into().unwrap());
    assert!(key_len < MAX_ARG_LEN - 16);
    let key = &input_data[8..8 + key_len as usize];

    storage_has_key(key_len, key.as_ptr() as _);
}
