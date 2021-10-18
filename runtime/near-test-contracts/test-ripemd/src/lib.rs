#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[allow(unused)]
extern "C" {
    // ###################
    // # Math Extensions #
    // ###################
    fn ripemd160(value_len: u64, value_ptr: u64, register_id: u64);
}

#[no_mangle]
fn do_ripemd() {
    let data = b"tesdsst";
    unsafe {
        ripemd160(data.len() as _, data.as_ptr() as _, 0);
    }
}
