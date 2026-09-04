//! Contract for the bandwidth scheduler tests, which are sensitive to contract size.
//!
//! The workload deploys this on every account and then calls
//! `do_function_call_with_args_of_size` to emit cross-shard receipts of a chosen
//! size. Contract code is loaded (and charged for) on every one of those calls, so
//! the contract's byte size is an input to how much work fits in a chunk, and the
//! fairness and utilization figures the tests assert on move with it. Using the
//! shared `test-contract-rs` here coupled the tests to a fixture that every new
//! host function grows: adding one moved `link_imbalance_ratio` from 1.53 to 1.81
//! and broke the test. Hence this contract, which nothing else deploys.
//!
//! Keep it small, and do not add methods for unrelated tests.

#![allow(clippy::all)]

#[allow(unused)]
unsafe extern "C" {
    fn input(register_id: u64);
    fn register_len(register_id: u64) -> u64;
    fn read_register(register_id: u64, ptr: u64);
    fn promise_batch_create(account_id_len: u64, account_id_ptr: u64) -> u64;
    fn promise_batch_action_function_call_weight(
        promise_index: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
        gas_weight: u64,
    );
}

/// The method the emitted receipts call on the receiver. Does nothing; the point
/// is the receipt, not its effect.
#[unsafe(no_mangle)]
pub unsafe fn noop() {}

/// Send a receipt of a caller-chosen size to another account.
///
/// Arguments are a fixed binary layout rather than JSON, so that the contract
/// needs no deserializer and stays small:
///
/// ```text
/// [0..8]                    args_size, little-endian u64
/// [8]                       account_id length in bytes
/// [9..9 + account_id_len]   account_id
/// [9 + account_id_len..]    method name to call on the receiver
/// ```
///
/// It attaches a fixed 1 gas so the receipt keeps congestion low, and no gas
/// weight, so the receiver does no work beyond existing.
#[unsafe(no_mangle)]
pub unsafe fn do_function_call_with_args_of_size() {
    input(0);
    let mut params = vec![0u8; register_len(0) as usize];
    read_register(0, params.as_mut_ptr() as u64);

    let args_size = u64::from_le_bytes(params[0..8].try_into().unwrap());
    let account_id_len = params[8] as usize;
    let account_id = &params[9..9 + account_id_len];
    let method_name = &params[9 + account_id_len..];

    // The receipt's size is what the test controls; the bytes themselves are padding.
    let args = vec![0u8; args_size as usize];
    let amount = 0u128;
    let gas_fixed = 1;
    let gas_weight = 0;

    let promise_idx = promise_batch_create(account_id.len() as u64, account_id.as_ptr() as u64);
    promise_batch_action_function_call_weight(
        promise_idx,
        method_name.len() as u64,
        method_name.as_ptr() as u64,
        args_size,
        args.as_ptr() as u64,
        &amount as *const u128 as u64,
        gas_fixed,
        gas_weight,
    );
}
