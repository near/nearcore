use byteorder::{LittleEndian, ReadBytesExt};
use near_primitives_core::hash::{hash, CryptoHash};
use std::io::{Cursor, Read};

// to specify a part we always specify both part_id and num_parts together
#[derive(Copy, Clone)]
pub struct PartId {
    pub idx: u64,
    pub total: u64,
}
impl PartId {
    pub fn new(part_id: u64, num_parts: u64) -> PartId {
        assert!(part_id < num_parts);
        PartId { idx: part_id, total: num_parts }
    }
}

/// State value reference. Used to charge fees for value length before retrieving the value itself.
pub struct ValueRef {
    /// Value length in bytes.
    pub length: u32,
    /// Unique value hash.
    pub hash: CryptoHash,
}

impl ValueRef {
    /// Create serialized value reference by the value.
    /// Resulting array stores 4 bytes of length and then 32 bytes of hash.
    pub fn create_serialized(value: &[u8]) -> [u8; 36] {
        let mut result = [0u8; 36];
        result[0..4].copy_from_slice(&(value.len() as u32).to_le_bytes());
        result[4..36].copy_from_slice(&hash(value).0);
        result
    }

    /// Decode value reference from the raw byte array.
    /// TODO (#7327): use &[u8; 36] and get rid of Cursor; also check that there are no leftover bytes
    pub fn decode(bytes: &[u8]) -> Result<Self, std::io::Error> {
        let mut cursor = Cursor::new(bytes);
        let value_length = cursor.read_u32::<LittleEndian>()?;
        let mut arr = [0; 32];
        cursor.read_exact(&mut arr)?;
        let value_hash = CryptoHash(arr);
        Ok(ValueRef { length: value_length, hash: value_hash })
    }
}

#[cfg(test)]
mod tests {
    use crate::state::ValueRef;
    use near_primitives_core::hash::hash;

    #[test]
    fn test_encode_decode() {
        let value = vec![1, 2, 3];
        let value_ref_ser = ValueRef::create_serialized(&value);
        let value_ref = ValueRef::decode(&value_ref_ser).unwrap();
        assert_eq!(value_ref.length, value.len() as u32);
        assert_eq!(value_ref.hash, hash(&value));
    }
}
