@external("env", "read_register")
declare function read_register(register_id: u64, ptr: u64): void;
@external("env", "register_len")
declare function register_len(register_id: u64): u64;

@external("env", "storage_write")
declare function storage_write(key_len: u64, key_ptr: u64, value_len: u64, value_ptr: u64, register_id: u64): u64;
@external("env", "storage_read")
declare function storage_read(key_len: u64, key_ptr: u64, register_id: u64): u64;

@external("env", "input")
declare function input(register_id: u64): void;
@external("env", "value_return")
declare function value_return(value_len: u64, value_ptr: u64): void;

@external("env", "panic")
declare function panic(): void;

export function try_storage_write(): void {
    input(0);
    let len = register_len(0);
    if (len == U64.MAX_VALUE) {
        panic();
    }
    let buffer = new Uint8Array(len as i32);
    read_register(0, buffer.buffer as u64);
    let input_str = String.UTF8.decode(buffer.buffer);
    let input_split = input_str.split(" ");
    let key_str = input_split[0];
    let value_str = input_split[1]

    let key = String.UTF8.encode(key_str);
    let value = String.UTF8.encode(value_str);
    storage_write(key.byteLength, key as u64, value.byteLength, value as u64, 0);
}

export function try_storage_read(): void {
    input(0);
    let key_len = register_len(0);
    if (key_len == U64.MAX_VALUE) {
        panic();
    }
    let key = new Uint8Array(key_len as i32);
    read_register(0, key.buffer as u64);

    let res = storage_read(key.buffer.byteLength, key.buffer as u64, 1);
    if (res == 1) {
        let value_len = register_len(1);
        let value = new Uint8Array(value_len as i32);
        read_register(1, value.buffer as u64);
        value_return(value.buffer.byteLength, value.buffer as u64);
    }
}

export function try_panic(): void {
    panic();
}
