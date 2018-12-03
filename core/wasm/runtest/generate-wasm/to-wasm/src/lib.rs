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

static mut FINAL_RESULT: Vec<u8> = Vec::new();

extern "C" {
    // First 4 bytes are the length of the remaining buffer.
    fn storage_write(key: *const u8, value: *const u8);
    fn storage_read_len(key: *const u8) -> u32;
    fn storage_read_into(key: *const u8, value: *mut u8);
}

pub fn storage_read(key: *const u8) -> Vec<u8> {
    unsafe {
        let len = storage_read_len(key);
        let mut vec = vec![0u8; len as usize];
        storage_read_into(key, vec.as_mut_ptr());
        vec
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
pub fn run_test() -> *const u8 {
    put_int(10, 20);
    put_int(50, 150);
    let int20 = get_int(10);
    unsafe {
        FINAL_RESULT.resize(8, 0);
        LittleEndian::write_u32(&mut FINAL_RESULT[..4], 4);
        LittleEndian::write_i32(&mut FINAL_RESULT[4..], int20);
        FINAL_RESULT.as_ptr()
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
