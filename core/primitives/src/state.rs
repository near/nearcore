use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives_core::hash::{hash, CryptoHash};

/// State value reference. Used to charge fees for value length before retrieving the value itself.
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug)]
pub struct ValueRef {
    /// Value length in bytes.
    pub length: u32,
    /// Unique value hash.
    pub hash: CryptoHash,
}

impl ValueRef {
    /// Create serialized value reference by the value.
    /// Resulting array stores 4 bytes of length and then 32 bytes of hash.
    /// TODO (#7327): consider passing hash here to avoid double computation
    pub fn new(value: &[u8]) -> Self {
        Self { length: value.len() as u32, hash: hash(value) }
    }

    /// Decode value reference from the raw byte array.
    pub fn decode(bytes: &[u8; 36]) -> Self {
        let (length, hash) = stdx::split_array(bytes);
        let length = u32::from_le_bytes(*length);
        ValueRef { length, hash: CryptoHash(*hash) }
    }
}

#[cfg(test)]
mod tests {
    use crate::state::ValueRef;
    use near_primitives_core::hash::hash;

    #[test]
    fn test_encode_decode() {
        let value = vec![1, 2, 3];
        let old_value_ref = ValueRef::new(&value);
        let mut value_ref_ser = [0u8; 36];
        value_ref_ser[0..4].copy_from_slice(&old_value_ref.length.to_le_bytes());
        value_ref_ser[4..36].copy_from_slice(&old_value_ref.hash.0);
        let value_ref = ValueRef::decode(&value_ref_ser);
        assert_eq!(value_ref.length, value.len() as u32);
        assert_eq!(value_ref.hash, hash(&value));
    }
}
