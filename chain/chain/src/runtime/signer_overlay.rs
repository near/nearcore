use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::types::{AccountId, Nonce, NonceIndex};
use near_store::{StorageError, TrieAccess, get_access_key, get_account, get_gas_key_nonce};
use std::collections::HashMap;
use std::collections::hash_map::Entry;

/// Per-(account, public_key) state in the overlay.
pub(crate) struct KeyEntry {
    pub access_key: AccessKey,
    pub gas_key_nonces: HashMap<NonceIndex, Nonce>,
}

/// Per-account state in the overlay: the account itself plus per-key entries.
struct AccountEntry {
    account: Account,
    keys: HashMap<PublicKey, KeyEntry>,
}

/// Ephemeral overlay for signer account and access key data during a single
/// `prepare_transactions` call. Loads from the trie on first access and
/// accumulates mutations (balance deductions, nonce increments) locally
/// without writing back to the trie.
///
/// Account state is keyed by `AccountId` alone so that multiple public keys
/// for the same account share one account state (e.g., balance), preventing
/// double-spend across those keys.
pub(crate) struct SignerOverlay {
    entries: HashMap<AccountId, AccountEntry>,
}

impl SignerOverlay {
    pub fn new() -> Self {
        Self { entries: HashMap::new() }
    }

    /// Returns the current nonce from the overlay if available. For gas key
    /// transactions (nonce_index is Some), returns the gas key nonce;
    /// otherwise returns the access key nonce. Returns `None` on cache miss.
    pub fn cached_nonce(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
        nonce_index: Option<NonceIndex>,
    ) -> Option<Nonce> {
        let key_entry = self.entries.get(account_id)?.keys.get(public_key)?;
        if let Some(idx) = nonce_index {
            key_entry.gas_key_nonces.get(&idx).copied()
        } else {
            Some(key_entry.access_key.nonce)
        }
    }

    /// Returns mutable references to the account and per-key state, loading
    /// from the trie on first access. `Ok(None)` signals that the requested
    /// account, access key, or gas-key nonce does not exist in state. Storage
    /// errors propagate as `Err`.
    pub fn get_or_load_entry_mut(
        &mut self,
        trie: &dyn TrieAccess,
        account_id: &AccountId,
        public_key: &PublicKey,
        nonce_index: Option<NonceIndex>,
    ) -> Result<Option<(&mut Account, &mut KeyEntry)>, StorageError> {
        // Ensure the account is loaded.
        let entry = match self.entries.entry(account_id.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let Some(account) = get_account(trie, account_id)? else {
                    return Ok(None);
                };
                entry.insert(AccountEntry { account, keys: HashMap::new() })
            }
        };

        // Destructure to split the borrow between account and keys.
        let AccountEntry { account, keys } = entry;

        // Ensure the key entry is loaded.
        let key_entry = match keys.entry(public_key.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let Some(access_key) = get_access_key(trie, account_id, public_key)? else {
                    return Ok(None);
                };
                entry.insert(KeyEntry { access_key, gas_key_nonces: HashMap::new() })
            }
        };

        // Ensure the requested gas key nonce is loaded.
        if let Some(idx) = nonce_index {
            if let Entry::Vacant(e) = key_entry.gas_key_nonces.entry(idx) {
                let Some(nonce) = get_gas_key_nonce(trie, account_id, public_key, idx)? else {
                    return Ok(None);
                };
                e.insert(nonce);
            }
        }

        Ok(Some((account, key_entry)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use borsh::to_vec;
    use near_crypto::KeyType;
    use near_primitives::account::AccountContract;
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::Balance;
    use near_store::trie::AccessOptions;

    struct MockTrie {
        values: HashMap<Vec<u8>, Result<Vec<u8>, StorageError>>,
    }

    impl TrieAccess for MockTrie {
        fn get(
            &self,
            key: &TrieKey,
            _opts: AccessOptions,
        ) -> Result<Option<Vec<u8>>, StorageError> {
            match self.values.get(&key.to_vec()) {
                None => Ok(None),
                Some(Ok(v)) => Ok(Some(v.clone())),
                Some(Err(e)) => Err(e.clone()),
            }
        }

        fn contains_key(&self, _: &TrieKey, _: AccessOptions) -> Result<bool, StorageError> {
            unimplemented!()
        }
    }

    fn alice() -> AccountId {
        "alice".parse().unwrap()
    }

    fn pk() -> PublicKey {
        PublicKey::empty(KeyType::ED25519)
    }

    fn account_bytes() -> Vec<u8> {
        let account =
            Account::new(Balance::from_yoctonear(1), Balance::ZERO, AccountContract::None, 0);
        to_vec(&account).unwrap()
    }

    fn access_key_bytes() -> Vec<u8> {
        to_vec(&AccessKey::full_access()).unwrap()
    }

    #[test]
    fn returns_none_when_gas_key_nonce_missing() {
        let mut values = HashMap::new();
        values.insert(TrieKey::Account { account_id: alice() }.to_vec(), Ok(account_bytes()));
        values.insert(TrieKey::access_key(alice(), pk()).to_vec(), Ok(access_key_bytes()));
        let trie = MockTrie { values };
        let mut overlay = SignerOverlay::new();
        let result = overlay.get_or_load_entry_mut(&trie, &alice(), &pk(), Some(0));
        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn propagates_storage_error_from_account_lookup() {
        let mut values = HashMap::new();
        values.insert(
            TrieKey::Account { account_id: alice() }.to_vec(),
            Err(StorageError::StorageInternalError),
        );
        let trie = MockTrie { values };
        let mut overlay = SignerOverlay::new();
        let result = overlay.get_or_load_entry_mut(&trie, &alice(), &pk(), None);
        assert!(matches!(result, Err(StorageError::StorageInternalError)));
    }
}
