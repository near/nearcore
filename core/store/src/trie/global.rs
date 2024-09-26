use near_primitives::{errors::StorageError, hash::CryptoHash, types::AccountId};

use crate::Trie;

pub struct GlobalShard(Trie);

impl GlobalShard {
    pub fn new(trie: Trie) -> Self {
        Self(trie)
    }

    pub fn trie(self) -> Trie {
        self.0
    }

    pub fn get(&self, key: &GlobalTrieKey) -> Result<Option<Vec<u8>>, StorageError> {
        self.0.get(&key.to_vec())
    }

    pub fn contains_key(&self, key: &GlobalTrieKey) -> Result<bool, StorageError> {
        self.0.contains_key(&key.to_vec())
    }
}

pub enum GlobalTrieKey {
    /// Account -> Contract hash
    Account { account_id: AccountId },
    /// Contract hash -> Contract code
    ContractHash { hash: CryptoHash },
}

impl GlobalTrieKey {
    /// Convert trie key to a byte array with a prefix that indicates the type of the key.
    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            GlobalTrieKey::Account { account_id } => {
                let mut res = vec![0];
                res.extend_from_slice(account_id.as_bytes());
                res
            },
            GlobalTrieKey::ContractHash { hash } =>  {
                let mut res = vec![1];
                res.extend_from_slice(hash.as_ref());
                res
            }
        }
    }
}