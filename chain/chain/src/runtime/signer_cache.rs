use crate::Error;
use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::types::{AccountId, Nonce, NonceIndex};
use near_store::{TrieAccess, get_access_key, get_account, get_gas_key_nonce};
use std::collections::HashMap;
use std::collections::hash_map::Entry;

/// Key for looking up per-key cached state.
pub(crate) type SignerKey = (AccountId, PublicKey);

/// Per-(account, public_key) cached state.
struct KeyEntry {
    access_key: AccessKey,
    gas_key_nonces: HashMap<NonceIndex, Nonce>,
}

/// Combined mutable view returned by [`SignerCache::get_or_load`].
/// The `account` is shared across all public keys for the same account_id,
/// while `access_key` and `gas_key_nonces` are per-(account_id, public_key).
pub(crate) struct SignerCacheView<'a> {
    pub account: &'a mut Account,
    pub access_key: &'a mut AccessKey,
    pub gas_key_nonces: &'a mut HashMap<NonceIndex, Nonce>,
}

/// Caches signer account and access key data across transaction groups within
/// a single `prepare_transactions` call. Avoids redundant trie reads and
/// eliminates the need to write intermediate state back to the trie overlay.
///
/// Account state is keyed by `AccountId` alone so that multiple public keys
/// for the same account share one balance/nonce, preventing double-spend.
pub(crate) struct SignerCache {
    accounts: HashMap<AccountId, Account>,
    key_entries: HashMap<SignerKey, KeyEntry>,
}

impl SignerCache {
    pub fn new() -> Self {
        Self { accounts: HashMap::new(), key_entries: HashMap::new() }
    }

    /// Returns a combined view of account + per-key state, loading from the
    /// trie on first access.
    pub fn get_or_load(
        &mut self,
        trie: &dyn TrieAccess,
        key: SignerKey,
        nonce_index: Option<NonceIndex>,
    ) -> Result<SignerCacheView<'_>, Error> {
        let (account_id, public_key) = &key;

        // Ensure the account is loaded.
        if let Entry::Vacant(e) = self.accounts.entry(account_id.clone()) {
            let account = get_account(trie, account_id)
                .map_err(|_| Error::InvalidTransactions)?
                .ok_or(Error::InvalidTransactions)?;
            e.insert(account);
        }

        // Ensure the key entry is loaded (access key + any requested gas key nonce).
        match self.key_entries.entry(key.clone()) {
            Entry::Occupied(mut e) => {
                if let Some(idx) = nonce_index {
                    if !e.get().gas_key_nonces.contains_key(&idx) {
                        let nonce = get_gas_key_nonce(trie, account_id, public_key, idx)
                            .map_err(|_| Error::InvalidTransactions)?
                            .ok_or(Error::InvalidTransactions)?;
                        e.get_mut().gas_key_nonces.insert(idx, nonce);
                    }
                }
            }
            Entry::Vacant(e) => {
                let access_key = get_access_key(trie, account_id, public_key)
                    .map_err(|_| Error::InvalidTransactions)?
                    .ok_or(Error::InvalidTransactions)?;
                let gas_key_nonces = if let Some(idx) = nonce_index {
                    let nonce = get_gas_key_nonce(trie, account_id, public_key, idx)
                        .map_err(|_| Error::InvalidTransactions)?
                        .ok_or(Error::InvalidTransactions)?;
                    HashMap::from([(idx, nonce)])
                } else {
                    HashMap::new()
                };
                e.insert(KeyEntry { access_key, gas_key_nonces });
            }
        }

        let account = self.accounts.get_mut(&key.0).expect("inserted above");
        let key_entry = self.key_entries.get_mut(&key).expect("inserted above");
        Ok(SignerCacheView {
            account,
            access_key: &mut key_entry.access_key,
            gas_key_nonces: &mut key_entry.gas_key_nonces,
        })
    }
}
