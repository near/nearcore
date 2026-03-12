use crate::Error;
use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::types::{AccountId, Nonce, NonceIndex};
use near_store::{TrieAccess, get_access_key, get_account, get_gas_key_nonce};
use std::collections::HashMap;
use std::collections::hash_map::Entry;

/// Key for looking up cached signer state. All transaction groups with the
/// same (account, public_key) share one cache entry.
pub(crate) type SignerKey = (AccountId, PublicKey);

/// Cached signer state: account, access key, and any gas key nonces that
/// have been loaded or updated during this block's transaction preparation.
pub(crate) struct SignerCacheEntry {
    pub account: Account,
    pub access_key: AccessKey,
    pub gas_key_nonces: HashMap<NonceIndex, Nonce>,
}

/// Caches signer account and access key data across transaction groups within
/// a single `prepare_transactions` call. Avoids redundant trie reads and
/// eliminates the need to write intermediate state back to the trie overlay.
pub(crate) struct SignerCache {
    entries: HashMap<SignerKey, SignerCacheEntry>,
}

impl SignerCache {
    pub fn new() -> Self {
        Self { entries: HashMap::new() }
    }

    /// Returns the cached entry for the given signer, loading from the trie
    /// if not yet cached.
    pub fn get_or_load_entry_mut(
        &mut self,
        trie: &dyn TrieAccess,
        key: SignerKey,
        nonce_index: Option<NonceIndex>,
    ) -> Result<&mut SignerCacheEntry, Error> {
        let entry = match self.entries.entry(key) {
            Entry::Occupied(mut e) => {
                if let Some(idx) = nonce_index {
                    if !e.get().gas_key_nonces.contains_key(&idx) {
                        let (account_id, public_key) = e.key();
                        let nonce = get_gas_key_nonce(trie, account_id, public_key, idx)
                            .map_err(|_| Error::InvalidTransactions)?
                            .ok_or(Error::InvalidTransactions)?;
                        e.get_mut().gas_key_nonces.insert(idx, nonce);
                    }
                }
                e.into_mut()
            }
            Entry::Vacant(e) => {
                let (account_id, public_key) = e.key();
                let account = get_account(trie, account_id)
                    .map_err(|_| Error::InvalidTransactions)?
                    .ok_or(Error::InvalidTransactions)?;
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
                e.insert(SignerCacheEntry { account, access_key, gas_key_nonces })
            }
        };
        Ok(entry)
    }
}
