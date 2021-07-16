#![no_std]
#![allow(non_snake_case)]

#[panic_handler]
#[no_mangle]
pub fn panic(_info: &::core::panic::PanicInfo) -> ! {
    unsafe { core::arch::wasm32::unreachable() }
}

#[allow(unused)]
extern "C" {
    fn input(register_id: u64);
}

#[no_mangle]
pub unsafe fn sum(a: u32, b: u32) -> u32 {
    input(0);
    a + b
}
