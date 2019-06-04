type DataTypeIndex = u32;

pub const DATA_TYPE_ORIGINATOR_ACCOUNT_ID: DataTypeIndex = 1;
pub const DATA_TYPE_CURRENT_ACCOUNT_ID: DataTypeIndex = 2;
pub const DATA_TYPE_STORAGE: DataTypeIndex = 3;
pub const DATA_TYPE_INPUT: DataTypeIndex = 4;
pub const DATA_TYPE_RESULT: DataTypeIndex = 5;
pub const DATA_TYPE_STORAGE_ITER: DataTypeIndex = 6;

extern "C" {
    pub fn storage_write(
        key_len: usize,
        key_ptr: *const u8,
        value_len: usize,
        value_ptr: *const u8,
    );
    pub fn storage_remove(key_len: usize, key_ptr: *const u8);
    pub fn storage_has_key(key_len: usize, key_ptr: *const u8) -> bool;

    fn result_count() -> u32;
    fn result_is_ok(index: u32) -> bool;

    pub fn return_value(value_len: usize, value_ptr: *const u8);
    fn return_promise(promise_index: u32);

    fn data_read(
        data_type_index: u32,
        key_len: usize,
        key_ptr: *const u8,
        max_buf_len: usize,
        buf_ptr: *mut u8,
    ) -> usize;

    // AccountID is just 32 bytes without the prefix length.
    fn promise_create(
        account_id_len: usize,
        account_id_ptr: *const u8,
        method_name_len: usize,
        method_name_ptr: *const u8,
        arguments_len: usize,
        arguments_ptr: *const u8,
        amount: u64,
    ) -> u32;

    fn promise_then(
        promise_index: u32,
        method_name_len: usize,
        method_name_ptr: *const u8,
        arguments_len: usize,
        arguments_ptr: *const u8,
        amount: u64,
    ) -> u32;

    fn promise_and(promise_index1: u32, promise_index2: u32) -> u32;

    fn check_ethash(
        block_number: u64,
        header_hash_ptr: *const u8,
        header_hash_len: usize,
        nonce: u64,
        mix_hash_ptr: *const u8,
        mix_hash_len: usize,
        difficulty: u64,
    ) -> u32;

    fn frozen_balance() -> u64;
    fn liquid_balance() -> u64;
    fn deposit(min_amout: u64, max_amount: u64) -> u64;
    fn withdraw(min_amout: u64, max_amount: u64) -> u64;
    fn storage_usage() -> u64;
    fn received_amount() -> u64;
    fn assert(expr: bool);

    /// Hash buffer is 32 bytes
    fn hash(value_len: usize, value_ptr: *const u8, buf_ptr: *mut u8);
    fn hash32(value_len: usize, value_ptr: *const u8) -> u32;

    // Fills given buffer with random u8.
    fn random_buf(buf_len: u32, buf_ptr: *mut u8);
    fn random32() -> u32;

    fn block_index() -> u64;

    /// Log using utf-8 string format.
    pub fn debug(msg_len: usize, msg_ptr: *const u8);
}

const MAX_BUF_SIZE: usize = 1 << 16;
static mut SCRATCH_BUF: Vec<u8> = Vec::new();

fn read(type_index: u32, key_len: usize, key: *const u8) -> Vec<u8> {
    unsafe {
        if SCRATCH_BUF.len() == 0 {
            SCRATCH_BUF.resize(MAX_BUF_SIZE, 0);
        }
        let len = data_read(type_index, key_len, key, MAX_BUF_SIZE, SCRATCH_BUF.as_mut_ptr());
        assert(len <= MAX_BUF_SIZE);
        SCRATCH_BUF[..len as usize].to_vec()
    }
}

pub fn storage_read(key_len: usize, key: *const u8) -> Vec<u8> {
    read(DATA_TYPE_STORAGE, key_len, key)
}

pub fn input_read() -> Vec<u8> {
    read(DATA_TYPE_INPUT, 0, 0 as (*const u8))
}

pub fn my_log(msg: &[u8]) {
    unsafe {
        debug(msg.len(), msg.as_ptr());
    }
}

pub fn result_read(index: u32) -> Vec<u8> {
    read(DATA_TYPE_RESULT, 0, index as (*const u8))
}

pub fn originator_id() -> Vec<u8> {
    read(DATA_TYPE_ORIGINATOR_ACCOUNT_ID, 0, 0 as (*const u8))
}

pub fn account_id() -> Vec<u8> {
    read(DATA_TYPE_CURRENT_ACCOUNT_ID, 0, 0 as (*const u8))
}
