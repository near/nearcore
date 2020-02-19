use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, BorshDeserialize, BorshSerialize)]
pub enum KeyType {
    /// Key type
    Regular,
    Foundation,
}

impl KeyType {
    pub fn allowed_methods(&self) -> Vec<u8> {
        match self {
            KeyType::Regular => b"stake,transfer".to_vec(),
            KeyType::Foundation => b"permanently_unstake,terminate".to_vec(),
        }
    }
}

/// A key is a sequence of bytes, potentially including the prefix determining the cryptographic type
/// of the key. For forward compatibility we do not enforce any specific length.
pub type PublicKey = Vec<u8>;
