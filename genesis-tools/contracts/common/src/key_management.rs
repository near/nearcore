use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
/// Different types of keys can add/remove other types of keys. Here is the alignment matrix:
/// <vertical> can be added/removed by <horizontal>:
///              regular     privileged      foundation
/// regular      -           +               +         
/// privileged   -           +               +         
/// foundation   -           -               +         
#[derive(Clone, Copy, Debug, Deserialize, Serialize, BorshDeserialize, BorshSerialize)]
pub enum KeyType {
    Regular,
    Privileged,
    Foundation,
}

impl KeyType {
    /// Check whether this key can add or remove the other key.
    pub fn check_can_add_remove(&self, other: &Self) {
        let ok = match (self, other) {
            (KeyType::Regular, _) => false,
            (KeyType::Privileged, KeyType::Regular) => true,
            (KeyType::Privileged, KeyType::Privileged) => true,
            (KeyType::Privileged, KeyType::Foundation) => false,
            (KeyType::Foundation, _) => true,
        };
        if !ok {
            panic!("{:?} key type cannot add or remove {:?} key type", self, other);
        }
    }

    pub fn allowed_methods(&self) -> &[u8] {
        match self {
            KeyType::Regular => b"stake,transfer",
            KeyType::Privileged => b"add_key,remove_key",
            KeyType::Foundation => b"add_key,remove_key,permanently_unstake,terminate",
        }
    }
}

/// A key is a sequence of bytes, potentially including the prefix determining the cryptographic type
/// of the key. For forward compatibility we do not enforce any specific length.
pub type PublicKey = Vec<u8>;
