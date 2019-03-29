// We aren't using the standard library.
#![no_std]
#![feature(alloc_error_handler)]
#![feature(alloc)]
#![feature(allocator_api)]
#![feature(const_vec_new)]

use core::panic::PanicInfo;

#[allow(unused)]
#[macro_use]
extern crate alloc;

extern crate wee_alloc;

extern crate byteorder;

use alloc::vec::Vec;
use byteorder::{ByteOrder, LittleEndian};

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

type DataTypeIndex = u32;

pub const DATA_TYPE_ORIGINATOR_ACCOUNT_ID: DataTypeIndex = 1;
pub const DATA_TYPE_CURRENT_ACCOUNT_ID: DataTypeIndex = 2;
pub const DATA_TYPE_STORAGE: DataTypeIndex = 3;
pub const DATA_TYPE_INPUT: DataTypeIndex = 4;
pub const DATA_TYPE_RESULT: DataTypeIndex = 5;
pub const DATA_TYPE_STORAGE_ITER: DataTypeIndex = 6;

#[allow(unused)]
extern "C" {
    fn storage_write(key_len: usize, key_ptr: *const u8, value_len: usize, value_ptr: *const u8);
    fn storage_remove(key_len: usize, key_ptr: *const u8);
    fn storage_has_key(key_len: usize, key_ptr: *const u8) -> bool;

    fn result_count() -> u32;
    fn result_is_ok(index: u32) -> bool;

    fn return_value(value_len: usize, value_ptr: *const u8);
    fn return_promise(promise_index: u32);

    fn data_read(data_type_index: u32, key_len: usize, key_ptr: *const u8, max_buf_len: usize, buf_ptr: *mut u8) -> usize;

    // AccountID is just 32 bytes without the prefix length.
    fn promise_create(
        account_id_len: usize, account_id_ptr: *const u8,
        method_name_len: usize, method_name_ptr: *const u8,
        arguments_len: usize, arguments_ptr: *const u8,
        mana: u32,
        amount: u64,
    ) -> u32;

    fn promise_then(
        promise_index: u32,
        method_name_len: usize, method_name_ptr: *const u8,
        arguments_len: usize, arguments_ptr: *const u8,
        mana: u32,
    ) -> u32;

    fn promise_and(promise_index1: u32, promise_index2: u32) -> u32;

    fn balance() -> u64;
    fn mana_left() -> u32;
    fn gas_left() -> u64;
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
    fn debug(msg_len: usize, msg_ptr: *const u8);
}

const MAX_BUF_SIZE: usize = 1<<16;
static mut SCRATCH_BUF: Vec<u8> = Vec::new();

fn read(type_index: u32, key_len: usize, key: *const u8) -> Vec<u8> {
unsafe {
    if SCRATCH_BUF.len() == 0 {
        SCRATCH_BUF.resize(MAX_BUF_SIZE, 0);
    }
    let len = data_read(
        type_index,
        key_len,
        key,
        MAX_BUF_SIZE,
        SCRATCH_BUF.as_mut_ptr());
    assert(len <= MAX_BUF_SIZE);
    SCRATCH_BUF[..len as usize].to_vec()
}
}

fn storage_read(key_len: usize, key: *const u8) -> Vec<u8> {
    read(DATA_TYPE_STORAGE, key_len, key)
}

fn input_read() -> Vec<u8> {
    read(DATA_TYPE_INPUT, 0, 0 as (*const u8))
}

fn my_log(msg: &[u8]) {
unsafe {
    debug(msg.len(), msg.as_ptr());
}
}

fn result_read(index: u32) -> Vec<u8> {
    read(DATA_TYPE_RESULT, 0, index as (*const u8))
}

fn return_i32(res: i32) {
unsafe {
    let mut buf = [0u8; 4];
    LittleEndian::write_i32(&mut buf, res);
    return_value(4, buf.as_ptr())
}
}

fn return_u64(res: u64) {
unsafe {
    let mut buf = [0u8; 8];
    LittleEndian::write_u64(&mut buf, res);
    return_value(8, buf.as_ptr())
}
}

fn originator_id() -> Vec<u8> {
    read(DATA_TYPE_ORIGINATOR_ACCOUNT_ID, 0, 0 as (*const u8))
}

fn account_id() -> Vec<u8> {
    read(DATA_TYPE_CURRENT_ACCOUNT_ID, 0, 0 as (*const u8))
}

#[no_mangle]
fn key_to_str(key: u32) -> Vec<u8> {
    let mut str_key = b"key: 0000000000".to_vec();
    let mut pos = str_key.len() - 1;
    let mut mkey = key;
    while mkey > 0 {
        str_key[pos] = b'0' as u8 + (mkey % 10) as u8;
        pos -= 1;
        mkey /= 10;
    }
    str_key
}

#[no_mangle]
pub fn put_int(key: u32, value: i32) {
unsafe {
    let mut val_bytes = [0u8; 4];
    LittleEndian::write_i32(&mut val_bytes, value);
    let key = key_to_str(key);
    storage_write(key.len(), key.as_ptr(), 4, val_bytes.as_ptr());
}
}

#[no_mangle]
pub fn get_int(key: u32) -> i32 {
unsafe {
    let key = key_to_str(key);
    let val = storage_read(key.len(), key.as_ptr());
    assert(val.len() == 4);
    LittleEndian::read_i32(&val[..])
}
}

#[no_mangle]
pub fn remove_int(key: u32) {
unsafe {
    let key = key_to_str(key);
    storage_remove(key.len(), key.as_ptr());
}
}

#[no_mangle]
pub fn has_int(key: u32) -> bool {
unsafe {
    let key = key_to_str(key);
    storage_has_key(key.len(), key.as_ptr())
}
}


#[no_mangle]
pub fn log_something() {
    my_log(b"hello");
}

#[no_mangle]
pub fn run_test() {
    return_i32(10)
}

#[no_mangle]
pub fn run_test_with_storage_change() {
unsafe {
    put_int(10, 20);
    put_int(50, 150);
    assert(has_int(50));
    remove_int(50);
    assert(!has_int(50));
    let res = get_int(10);
    return_i32(res)
}
}

#[no_mangle]
pub fn sum_with_input() {
unsafe {
    let input = input_read();
    assert(input.len() == 8);
    let a = LittleEndian::read_i32(&input[..4]);
    let b = LittleEndian::read_i32(&input[4..]);
    let sum = a + b;
    return_i32(sum)
}
}

#[no_mangle]
pub fn get_account_id() {
unsafe {
    let acc_id = account_id();
    return_value(acc_id.len(), acc_id.as_ptr())
}
}

#[no_mangle]
pub fn get_originator_id() {
unsafe {
    let acc_id = originator_id();
    return_value(acc_id.len(), acc_id.as_ptr())
}
}

#[no_mangle]
pub fn sum_with_multiple_results() {
unsafe {
    let cnt = result_count();
    if cnt == 0 {
        return return_i32(-100);
    }
    let mut sum = 0;
    for index in 0..cnt {
        if !result_is_ok(index) {
            return return_i32(-100);
        }
        sum += LittleEndian::read_i32(&result_read(index));
    }
    return_i32(sum)
}
}

#[no_mangle]
pub fn create_promises_and_join() {
unsafe {
    let promise1 = promise_create(
        5, b"test1".to_vec().as_ptr(),
        4, b"run1".to_vec().as_ptr(),
        5, b"args1".to_vec().as_ptr(),
        0,
        0,
    );
    let promise2 = promise_create(
        5, b"test2".to_vec().as_ptr(),
        4, b"run2".to_vec().as_ptr(),
        5, b"args2".to_vec().as_ptr(),
        0,
        0,
    );
    let promise_joined = promise_and(promise1, promise2);
    let callback = promise_then(
        promise_joined,
        8, b"run_test".to_vec().as_ptr(),
        0, 0 as (*const u8),
        0,
    );
    return_promise(callback);
}
}

#[no_mangle]
pub fn answer_to_life() {
    return_i32(43);
}

#[no_mangle]
pub fn transfer_to_bob() {
unsafe {
    let promise1 = promise_create(
        3, b"bob".to_vec().as_ptr(),
        5, b"deposit".to_vec().as_ptr(),
        0, 0 as (*const u8),
        0,
        1u64,
    );
    return_promise(promise1);
}
}

#[no_mangle]
pub fn get_prev_balance() {
unsafe {
    let bal = balance();
    let amount = received_amount();
    return_u64(bal - amount);
}
}

#[no_mangle]
pub fn get_gas_left() {
unsafe {
    let my_gas = gas_left();
    return_u64(my_gas);
}
}

#[no_mangle]
pub fn get_mana_left() {
unsafe {
    let my_mana = mana_left();
    return_i32(my_mana as i32);
}
}

#[no_mangle]
pub fn get_block_index() {
unsafe {
    let bi = block_index();
    return_u64(bi);
}
}

#[no_mangle]
pub fn assert_sum() {
unsafe {
    let input = input_read();
    assert(input.len() == 12);
    let a = LittleEndian::read_i32(&input[..4]);
    let b = LittleEndian::read_i32(&input[4..8]);
    let sum = LittleEndian::read_i32(&input[8..]);
    assert(a + b == sum);
}
}

#[no_mangle]
pub fn get_random_32() {
unsafe {
    return_i32(random32() as i32)
}
}

#[no_mangle]
pub fn get_random_buf() {
unsafe {
    let input = input_read();
    assert(input.len() == 4);
    let len = LittleEndian::read_u32(&input[..4]);
    let mut buf = vec![0u8; len as usize];
    random_buf(len, buf.as_mut_ptr());
    return_value(buf.len(), buf.as_ptr())
}
}

#[no_mangle]
pub fn hash_given_input() {
unsafe {
    let input = input_read();
    let mut buf = [0u8; 32];
    hash(input.len(), input.as_ptr(), buf.as_mut_ptr());
    return_value(buf.len(), buf.as_ptr())
}
}


#[no_mangle]
pub fn hash32_given_input() {
unsafe {
    let input = input_read();
    return_i32(hash32(input.len(), input.as_ptr()) as i32)
}
}

#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}

#[alloc_error_handler]
fn foo(_: core::alloc::Layout) -> ! {
    loop {}
}
