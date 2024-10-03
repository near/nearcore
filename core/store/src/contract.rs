use crate::TrieStorage;
use near_primitives::hash::CryptoHash;
use near_vm_runner::ContractCode;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Reads contract code from the trie by its hash.
///
/// Cloning is cheap.
#[derive(Clone)]
pub struct ContractStorage {
    storage: Arc<dyn TrieStorage>,

    /// During an apply of a single chunk contracts may be deployed through the
    /// `Action::DeployContract`.
    ///
    /// Unfortunately `TrieStorage` does not have a way to write to the underlying storage, and the
    /// `TrieUpdate` will only write the contract once the whole transaction is committed at the
    /// end of the chunk's apply.
    ///
    /// As a temporary work-around while we're still involving `Trie` for `ContractCode` storage,
    /// we'll keep a list of such deployed contracts here. Once the contracts are no longer part of
    /// The State this field should be removed, and the `Storage::store` function should be
    /// adjusted to write out the contract into the relevant part of the database immediately
    /// (without going through transactional storage operations and such).
    uncommitted_deploys: Arc<Mutex<BTreeMap<CryptoHash, ContractCode>>>,
}

impl ContractStorage {
    pub fn new(storage: Arc<dyn TrieStorage>) -> Self {
        Self { storage, uncommitted_deploys: Default::default() }
    }

    pub fn get(&self, code_hash: CryptoHash) -> Option<ContractCode> {
        {
            let guard = self.uncommitted_deploys.lock().expect("no panics");
            if let Some(v) = guard.get(&code_hash) {
                return Some(ContractCode::new(v.code().to_vec(), Some(code_hash)));
            }
        }

        match self.storage.retrieve_raw_bytes(&code_hash) {
            Ok(raw_code) => Some(ContractCode::new(raw_code.to_vec(), Some(code_hash))),
            Err(_) => None,
        }
    }

    pub fn store(&self, code: ContractCode) {
        let mut guard = self.uncommitted_deploys.lock().expect("no panics");
        guard.insert(*code.hash(), code);
    }
}
