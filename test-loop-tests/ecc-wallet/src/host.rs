//! Safe wrappers over the host functions the wallet uses. The contract calls them directly
//! rather than through near-sdk, which has no `__external_call` support.

/// The register every wrapper uses. Each wrapper copies the register out before returning.
const REGISTER: u64 = 0;

mod sys {
    unsafe extern "C" {
        // Registers.
        pub fn read_register(register_id: u64, ptr: u64);
        pub fn register_len(register_id: u64) -> u64;

        // Context.
        pub fn input(register_id: u64);
        pub fn current_account_id(register_id: u64);
        pub fn chain_id(register_id: u64);
        pub fn block_index() -> u64;

        // Storage.
        pub fn storage_read(key_len: u64, key_ptr: u64, register_id: u64) -> u64;
        pub fn storage_write(
            key_len: u64,
            key_ptr: u64,
            value_len: u64,
            value_ptr: u64,
            register_id: u64,
        ) -> u64;

        // Cryptography.
        pub fn sha256(value_len: u64, value_ptr: u64, register_id: u64);
        pub fn p256_verify(
            sig_len: u64,
            sig_ptr: u64,
            msg_len: u64,
            msg_ptr: u64,
            pub_key_len: u64,
            pub_key_ptr: u64,
        ) -> u64;

        // Promises.
        pub fn promise_batch_create(account_id_len: u64, account_id_ptr: u64) -> u64;
        pub fn promise_batch_action_transfer(promise_index: u64, amount_ptr: u64);
        pub fn promise_batch_action_function_call(
            promise_index: u64,
            method_name_len: u64,
            method_name_ptr: u64,
            arguments_len: u64,
            arguments_ptr: u64,
            amount_ptr: u64,
            gas: u64,
        );
        pub fn promise_batch_action_delete_account(
            promise_index: u64,
            beneficiary_id_len: u64,
            beneficiary_id_ptr: u64,
        );

        // Return values, logs and refusals.
        pub fn value_return(value_len: u64, value_ptr: u64);
        pub fn log_utf8(len: u64, ptr: u64);
        pub fn panic_utf8(len: u64, ptr: u64) -> !;
    }
}

fn ptr(bytes: &[u8]) -> u64 {
    bytes.as_ptr() as u64
}

fn len(bytes: &[u8]) -> u64 {
    bytes.len() as u64
}

/// The register's contents. Only called on a register a host function has just written.
fn read_register() -> Vec<u8> {
    // SAFETY: the buffer is as long as the register.
    unsafe {
        let mut buffer = vec![0; sys::register_len(REGISTER) as usize];
        sys::read_register(REGISTER, buffer.as_mut_ptr() as u64);
        buffer
    }
}

/// The call's payload.
pub fn input() -> Vec<u8> {
    unsafe { sys::input(REGISTER) };
    read_register()
}

pub fn current_account_id() -> Vec<u8> {
    unsafe { sys::current_account_id(REGISTER) };
    read_register()
}

pub fn chain_id() -> Vec<u8> {
    unsafe { sys::chain_id(REGISTER) };
    read_register()
}

/// The block height the call sees: in an external call, the height of the chunk's parent block.
pub fn block_height() -> u64 {
    unsafe { sys::block_index() }
}

pub fn storage_read(key: &[u8]) -> Option<Vec<u8>> {
    match unsafe { sys::storage_read(len(key), ptr(key), REGISTER) } {
        1 => Some(read_register()),
        _ => None,
    }
}

pub fn storage_write(key: &[u8], value: &[u8]) {
    unsafe { sys::storage_write(len(key), ptr(key), len(value), ptr(value), REGISTER) };
}

pub fn sha256(value: &[u8]) -> [u8; 32] {
    let mut digest = [0; 32];
    // SAFETY: the host writes a 32-byte digest to the register.
    unsafe {
        sys::sha256(len(value), ptr(value), REGISTER);
        sys::read_register(REGISTER, digest.as_mut_ptr() as u64);
    }
    digest
}

/// Whether `signature` is a P-256 signature by `public_key` over `digest`. A key that isn't
/// 33 bytes fails the call in the host function, which refuses it too.
pub fn p256_verify(signature: &[u8; 64], digest: &[u8; 32], public_key: &[u8]) -> bool {
    unsafe {
        sys::p256_verify(
            len(signature),
            ptr(signature),
            len(digest),
            ptr(digest),
            len(public_key),
            ptr(public_key),
        ) == 1
    }
}

pub fn promise_batch_create(receiver_id: &str) -> u64 {
    unsafe { sys::promise_batch_create(len(receiver_id.as_bytes()), ptr(receiver_id.as_bytes())) }
}

pub fn promise_batch_action_transfer(promise: u64, deposit: u128) {
    let deposit = deposit.to_le_bytes();
    unsafe { sys::promise_batch_action_transfer(promise, ptr(&deposit)) };
}

pub fn promise_batch_action_function_call(
    promise: u64,
    method_name: &str,
    args: &[u8],
    deposit: u128,
    gas: u64,
) {
    let deposit = deposit.to_le_bytes();
    let method_name = method_name.as_bytes();
    unsafe {
        sys::promise_batch_action_function_call(
            promise,
            len(method_name),
            ptr(method_name),
            len(args),
            ptr(args),
            ptr(&deposit),
            gas,
        )
    };
}

pub fn promise_batch_action_delete_account(promise: u64, beneficiary_id: &str) {
    let beneficiary_id = beneficiary_id.as_bytes();
    unsafe {
        sys::promise_batch_action_delete_account(promise, len(beneficiary_id), ptr(beneficiary_id))
    };
}

pub fn value_return(value: &[u8]) {
    unsafe { sys::value_return(len(value), ptr(value)) };
}

pub fn log(message: &str) {
    unsafe { sys::log_utf8(len(message.as_bytes()), ptr(message.as_bytes())) };
}

/// Refuses the call with `message`. A plain Rust panic would abort without a message.
pub fn refuse(message: &str) -> ! {
    unsafe { sys::panic_utf8(len(message.as_bytes()), ptr(message.as_bytes())) }
}
