use std::convert::TryFrom;
use std::io::Cursor;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;

use crate::uint128 as uint128_proto;

impl TryFrom<uint128_proto::Uint128> for u128 {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: uint128_proto::Uint128) -> Result<Self, Self::Error> {
        let len = value.number.len();
        if len > 16 {
            return Err(format!("uint128 proto has {} bytes, but expected up to 16.", len).into());
        }
        let mut rdr = Cursor::new(value.number);
        rdr.read_uint128::<LittleEndian>(len).map_err(|err| err.into())
    }
}

impl From<u128> for uint128_proto::Uint128 {
    fn from(value: u128) -> Self {
        uint128_proto::Uint128 {
            number: value.to_le_bytes().to_vec(),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}
