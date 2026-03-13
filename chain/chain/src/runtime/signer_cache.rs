use crate::Error;
use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::types::{AccountId, Nonce, NonceIndex};
use near_store::{TrieAccess, get_access_key, get_account, get_gas_key_nonce};
use std::collections::HashMap;
use std::collections::hash_map::Entry;

/// Per-(account, public_key) cached state.
struct KeyEntry {
    access_key: AccessKey,
    gas_key_nonces: HashMap<NonceIndex, Nonce>,
}

/// Combined mutable view returned by [`SignerCache::get_or_load_entry_mut`].
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
/// for the same account share one account state (e.g., balance), preventing
/// double-spend across those keys.
pub(crate) struct SignerCache {
    accounts: HashMap<AccountId, Account>,
    key_entries: HashMap<AccountId, HashMap<PublicKey, KeyEntry>>,
}

impl SignerCache {
    pub fn new() -> Self {
        Self { accounts: HashMap::new(), key_entries: HashMap::new() }
    }

    /// Returns a combined view of account + per-key state, loading from the
    /// trie on first access.
    pub fn get_or_load_entry_mut(
        &mut self,
        trie: &dyn TrieAccess,
        account_id: &AccountId,
        public_key: &PublicKey,
        nonce_index: Option<NonceIndex>,
    ) -> Result<SignerCacheView<'_>, Error> {
        // Ensure the account is loaded.
        if !self.accounts.contains_key(account_id) {
            let account = get_account(trie, account_id)
                .map_err(|_| Error::InvalidTransactions)?
                .ok_or(Error::InvalidTransactions)?;
            self.accounts.insert(account_id.clone(), account);
        }
        let account = self.accounts.get_mut(account_id).expect("inserted above");

        // Ensure the key entry is loaded.
        let per_key = self.key_entries.entry(account_id.clone()).or_default();
        if !per_key.contains_key(public_key) {
            let access_key = get_access_key(trie, account_id, public_key)
                .map_err(|_| Error::InvalidTransactions)?
                .ok_or(Error::InvalidTransactions)?;
            per_key.insert(
                public_key.clone(),
                KeyEntry { access_key, gas_key_nonces: HashMap::new() },
            );
        }
        let key_entry = per_key.get_mut(public_key).expect("inserted above");

        // Ensure the requested gas key nonce is loaded.
        if let Some(idx) = nonce_index {
            if let Entry::Vacant(e) = key_entry.gas_key_nonces.entry(idx) {
                let nonce = get_gas_key_nonce(trie, account_id, public_key, idx)
                    .map_err(|_| Error::InvalidTransactions)?
                    .ok_or(Error::InvalidTransactions)?;
                e.insert(nonce);
            }
        }

        Ok(SignerCacheView {
            account,
            access_key: &mut key_entry.access_key,
            gas_key_nonces: &mut key_entry.gas_key_nonces,
        })
    }
}
