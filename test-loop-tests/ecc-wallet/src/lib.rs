//! An example wallet contract for external contract calls (ECCs).
//!
//! NOT FOR PRODUCTION USE: see the README.
//!
//! This is a stub. Its `__external_call` accepts every call and does nothing.

/// The host functions the wallet uses. The contract calls them directly rather than through
/// near-sdk, which has no `__external_call` support.
#[allow(dead_code)]
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

        // Logs and refusals.
        pub fn log_utf8(len: u64, ptr: u64);
        pub fn panic_utf8(len: u64, ptr: u64) -> !;
    }
}

/// The method an external call runs. Returning accepts the call, and panicking refuses it.
#[unsafe(no_mangle)]
pub extern "C" fn __external_call() {}
