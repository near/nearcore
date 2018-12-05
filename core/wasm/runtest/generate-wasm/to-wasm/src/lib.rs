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

    fn promise_create(
        account_alias: *const u8,
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

fn return_int(res: i32) {
    let mut buf = [0u8; 8];
    LittleEndian::write_u32(&mut buf[..4], 4);
    LittleEndian::write_i32(&mut buf[4..], res);
    unsafe {
        return_value(buf.as_ptr())
    }
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
    assert!(val.len() == 4);
    LittleEndian::read_i32(&val[..])
}

#[no_mangle]
pub fn run_test() {
    put_int(10, 20);
    put_int(50, 150);
    let res = get_int(10);
    return_int(res)
}

#[no_mangle]
pub fn sum_with_input() {
    let input = input_read();
    assert!(input.len() == 8);
    let a = LittleEndian::read_i32(&input[..4]);
    let b = LittleEndian::read_i32(&input[4..]);
    let sum = a + b;
    return_int(sum)
}

#[no_mangle]
pub fn sum_with_multiple_results() {
    unsafe {
        let cnt = result_count();
        if cnt == 0 {
            return return_int(-100);
        }
        let mut sum = 0;
        for index in 0..cnt {
            if !result_is_ok(index) {
                return return_int(-100);
            }
            sum += LittleEndian::read_i32(&result_read(index));
        }
        return_int(sum)
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
