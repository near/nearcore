use std::mem::size_of;

use byteorder::{ByteOrder, LittleEndian};

pub fn u64_as_bytes(num: u64) -> Vec<u8> {
    let mut buffer = vec![0u8; size_of::<u64>()];
    LittleEndian::write_u64(buffer.as_mut(), num);
    buffer
}
