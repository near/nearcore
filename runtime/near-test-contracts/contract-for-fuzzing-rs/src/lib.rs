#![allow(clippy::all)]

use std::mem::size_of;

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
    fn block_timestamp() -> u64;
    fn epoch_height() -> u64;
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
    fn panic();
    fn panic_utf8(len: u64, ptr: u64);
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
    // ###############
    // # Validator API #
    // ###############
    fn validator_stake(account_id_len: u64, account_id_ptr: u64, stake_ptr: u64);
    fn validator_total_stake(stake_ptr: u64);
    // #################
    // # alt_bn128 API #
    // #################
    fn alt_bn128_g1_multiexp(value_len: u64, value_ptr: u64, register_id: u64);
    fn alt_bn128_g1_sum(value_len: u64, value_ptr: u64, register_id: u64);
    fn alt_bn128_pairing_check(value_len: u64, value_ptr: u64) -> u64;
}

#[no_mangle]
pub unsafe fn number_from_input() {
    input(0);
    let value = [0u8; size_of::<u64>()];
    read_register(0, value.as_ptr() as u64);
    value_return(value.len() as u64, &value as *const u8 as u64);
}

#[no_mangle]
pub unsafe fn count_sum() {
    input(0);
    let data = [0u8; size_of::<u64>()];
    read_register(0, data.as_ptr() as u64);

    let number_of_numbers = u64::from_le_bytes(data);

    let mut sum: u64 = 0;

    for i in 0..number_of_numbers {
        promise_result(i, 0);

        let data = [0u8; size_of::<u64>()];
        read_register(0, data.as_ptr() as u64);

        let number = u64::from_le_bytes(data);
        sum += number;
    }

    let value = sum.to_le_bytes();

    value_return(value.len() as u64, &value as *const u8 as u64);
}

#[no_mangle]
pub unsafe fn sum_of_numbers() {
    input(0);
    let data = [0u8; size_of::<u64>()];
    read_register(0, data.as_ptr() as u64);

    let number_of_numbers = u64::from_le_bytes(data);
    let mut promise_ids = vec![];

    for _ in 1..=number_of_numbers {
        let i: u64 = 1;
        let method_name = "number_from_input".as_bytes();
        let account_id = {
            signer_account_id(0);
            let result = vec![0; register_len(0) as usize];
            read_register(0, result.as_ptr() as *const u64 as u64);
            result
        };
        let arguments = i.to_le_bytes();

        promise_ids.push(promise_create(
            account_id.len() as u64,
            account_id.as_ptr() as u64,
            method_name.len() as u64,
            method_name.as_ptr() as u64,
            arguments.len() as u64,
            arguments.as_ptr() as u64,
            0,
            3_000_000_000,
        ));
    }

    let promise_idx = promise_and(promise_ids.as_ptr() as u64, promise_ids.len() as u64);

    {
        let method_name = "count_sum".as_bytes();
        let account_id = {
            signer_account_id(0);
            let result = vec![0; register_len(0) as usize];
            read_register(0, result.as_ptr() as *const u64 as u64);
            result
        };
        let arguments = number_of_numbers.to_le_bytes();

        promise_then(
            promise_idx,
            account_id.len() as u64,
            account_id.as_ptr() as u64,
            method_name.len() as u64,
            method_name.as_ptr() as u64,
            arguments.len() as u64,
            arguments.as_ptr() as u64,
            0,
            3_000_000_000_000,
        );
    }
}

// Function that does not do anything at all.
#[no_mangle]
pub fn noop() {}

#[no_mangle]
pub unsafe fn data_producer() {
    input(0);
    let data = [0u8; size_of::<u64>()];
    read_register(0, data.as_ptr() as u64);
    let size = u64::from_le_bytes(data);

    let data = vec![0u8; size as usize];
    value_return(data.len() as _, data.as_ptr() as _);
}

#[no_mangle]
pub unsafe fn data_receipt_with_size() {
    input(0);
    let data = [0u8; size_of::<u64>()];
    read_register(0, data.as_ptr() as u64);
    let size = u64::from_le_bytes(data);

    let buf = [0u8; 1000];
    current_account_id(0);
    let buf_len = register_len(0);
    read_register(0, buf.as_ptr() as _);

    let method_name = b"data_producer";
    let args = size.to_le_bytes();
    let amount = 0u128;
    let gas = prepaid_gas();
    let id = promise_create(
        buf_len,
        buf.as_ptr() as _,
        method_name.len() as _,
        method_name.as_ptr() as _,
        args.len() as _,
        args.as_ptr() as _,
        &amount as *const u128 as *const u64 as u64,
        gas / 20,
    );

    let method_name = b"noop";
    let args = b"";
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
