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

#[allow(unused)]
extern "C" {
    // First 4 bytes are the length of the remaining buffer.
    fn storage_write(key: *const u8, value: *const u8);
    fn storage_read_len(key: *const u8) -> u32;
    fn storage_read_into(key: *const u8, value: *mut u8);

    fn input_read_len() -> u32;
    fn input_read_into(value: *mut u8);

    fn result_count() -> u32;
    fn result_is_ok(index: u32) -> bool;
    fn result_read_len(index: u32) -> u32;
    fn result_read_into(index: u32, value: *mut u8);

    fn return_value(value: *const u8);
    fn return_promise(promise_index: u32);

    // TODO(#316): Fix sender_id/account_id to work with dynamic length of the account id.
    // Sender's account id.
    fn sender_id(account_id: *mut u8);
    // Current account id.
    fn account_id(account_id: *mut u8);

    // AccountID is just 32 bytes without the prefix length.
    fn promise_create(
        account_id: *const u8,
        method_name: *const u8,
        arguments: *const u8,
        mana: u32,
        amount: u64,
    ) -> u32;

    fn promise_then(
        promise_index: u32,
        method_name: *const u8,
        arguments: *const u8,
        mana: u32,
    ) -> u32;

    fn promise_and(promise_index1: u32, promise_index2: u32) -> u32;

    fn balance() -> u64;
    fn mana_left() -> u32;
    fn gas_left() -> u64;
    fn received_amount() -> u64;
    fn assert(expr: bool);
}

fn storage_read(key: *const u8) -> Vec<u8> {
    unsafe {
        let len = storage_read_len(key);
        let mut vec = vec![0u8; len as usize];
        storage_read_into(key, vec.as_mut_ptr());
        vec
    }
}

fn input_read() -> Vec<u8> {
    unsafe {
        let len = input_read_len();
        let mut vec = vec![0u8; len as usize];
        input_read_into(vec.as_mut_ptr());
        vec
    }
}

fn result_read(index: u32) -> Vec<u8> {
    unsafe {
        let len = result_read_len(index);
        let mut vec = vec![0u8; len as usize];
        result_read_into(index, vec.as_mut_ptr());
        vec
    }
}

fn return_i32(res: i32) {
    let mut buf = [0u8; 8];
    LittleEndian::write_u32(&mut buf[..4], 4);
    LittleEndian::write_i32(&mut buf[4..], res);
    unsafe {
        return_value(buf.as_ptr())
    }
}

fn return_u64(res: u64) {
    let mut buf = [0u8; 12];
    LittleEndian::write_u32(&mut buf[..4], 8);
    LittleEndian::write_u64(&mut buf[4..], res);
    unsafe {
        return_value(buf.as_ptr())
    }
}

fn serialize(buf: &[u8]) -> Vec<u8> {
    let mut vec = vec![0u8; buf.len() + 4];
    LittleEndian::write_u32(&mut vec[..4], buf.len() as u32);
    vec[4..].clone_from_slice(buf);
    return vec
}

#[no_mangle]
fn key_to_str(key: u32) -> [u8; 19] {
    let mut str_key = [0u8; 19];
    LittleEndian::write_u32(&mut str_key[..4], 15);
    str_key[4..].clone_from_slice(&b"key: 0000000000"[..]);
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
    let mut val_bytes = [0u8; 8];
    LittleEndian::write_u32(&mut val_bytes[..4], 4);
    LittleEndian::write_i32(&mut val_bytes[4..], value);
    unsafe {
        storage_write(key_to_str(key).as_ptr(), val_bytes.as_ptr());
    }
}

#[no_mangle]
pub fn get_int(key: u32) -> i32 {
    let val = storage_read(key_to_str(key).as_ptr());
    unsafe {
        assert(val.len() == 4);
    }
    LittleEndian::read_i32(&val[..])
}

#[no_mangle]
pub fn run_test() {
    put_int(10, 20);
    put_int(50, 150);
    let res = get_int(10);
    return_i32(res)
}

#[no_mangle]
pub fn sum_with_input() {
    let input = input_read();
    unsafe {
        assert(input.len() == 8);
    }
    let a = LittleEndian::read_i32(&input[..4]);
    let b = LittleEndian::read_i32(&input[4..]);
    let sum = a + b;
    return_i32(sum)
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
            serialize(b"test1").as_ptr(),
            serialize(b"run1").as_ptr(),
            serialize(b"args1").as_ptr(),
            0,
            0,
        );
        let promise2 = promise_create(
            serialize(b"test2").as_ptr(),
            serialize(b"run2").as_ptr(),
            serialize(b"args2").as_ptr(),
            0,
            0,
        );
        let promise_joined = promise_and(promise1, promise2);
        let callback = promise_then(
            promise_joined,
            serialize(b"run_test").as_ptr(),
            serialize(b"").as_ptr(),
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
            serialize(b"bob").as_ptr(),
            serialize(b"deposit").as_ptr(),
            serialize(b"").as_ptr(),
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


#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}

#[alloc_error_handler]
fn foo(_: core::alloc::Layout) -> ! {
    loop {}
}
