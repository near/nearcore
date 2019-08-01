use std::mem::size_of;

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[allow(unused)]
extern "C" {
    fn read_register(register_id: u64, ptr: u64);
    fn register_len(register_id: u64) -> u64;

    fn storage_write(
        key_len: u64,
        key_ptr: u64,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> u64;
    fn storage_read(key_len: u64, key_ptr: u64, register_id: u64) -> u64;

    fn input(register_id: u64);
    fn value_return(value_len: u64, value_ptr: u64);
    fn panic();
}

#[no_mangle]
pub fn write_key_value() {
    unsafe {
        input(0);
        if register_len(0) != 2 * size_of::<u64>() as u64 {
            panic()
        }
        let data = [0u8; 2 * size_of::<u64>()];
        read_register(0, data.as_ptr() as u64);

        let key = &data[0..size_of::<u64>()];
        let value = &data[size_of::<u64>()..];
        let result = storage_write(
            key.len() as u64,
            key.as_ptr() as u64,
            value.len() as u64,
            value.as_ptr() as u64,
            1,
        );
        value_return(size_of::<u64>() as u64, &result as *const u64 as u64);
    }
}

#[no_mangle]
pub fn read_value() {
    unsafe {
        input(0);
        if register_len(0) != size_of::<u64>() as u64 {
            panic()
        }
        let key = [0u8; size_of::<u64>()];
        read_register(0, key.as_ptr() as u64);
        let result = storage_read(key.len() as u64, key.as_ptr() as u64, 1);
        if result == 1 {
            let value = [0u8; size_of::<u64>()];
            read_register(1, value.as_ptr() as u64);
            value_return(value.len() as u64, &value as *const u8 as u64);
        }
    }
}
