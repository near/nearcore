use crate::TrieStorage;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_vm_runner::ContractCode;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, RwLock};

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
    uncommitted_deploys: Arc<RwLock<BTreeMap<CodeHash, ContractCode>>>,

    /// List of code-hashes for the contracts called.
    uncommitted_calls: Arc<Mutex<BTreeSet<CodeHash>>>,
}

/// Result of finalizing the contract storage.
pub struct ContractStorageResult {
    /// List of code-hashes for the contracts called while applying the chunk.
    pub contract_calls: Vec<CodeHash>,
    /// List of code-hashes for the contracts deployed while applying the chunk.
    pub contract_deploys: Vec<CodeHash>,
}

impl ContractStorage {
    pub fn new(storage: Arc<dyn TrieStorage>) -> Self {
        Self {
            storage,
            uncommitted_calls: Arc::new(Mutex::new(BTreeSet::new())),
            uncommitted_deploys: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Retrieves the contract code by its hash.
    ///
    /// This is called when preparing (eg. compiling) the contract for execution optimistically, and
    /// the function call for the returned contract may not be included in the currently applied chunk.
    /// Thus, the `record_call` method should also be called to indicate that the calling the contract
    /// is actually included in the chunk application.
    pub fn get(&self, code_hash: CryptoHash) -> Option<ContractCode> {
        {
            let guard = self.uncommitted_deploys.read().expect("no panics");
            if let Some(v) = guard.get(&code_hash.into()) {
                return Some(ContractCode::new(v.code().to_vec(), Some(code_hash)));
            }
        }

        match self.storage.retrieve_raw_bytes(&code_hash) {
            Ok(raw_code) => Some(ContractCode::new(raw_code.to_vec(), Some(code_hash))),
            Err(_) => None,
        }
    }

    /// Records a call to a contract by code-hash.
    ///
    /// This is used to capture the contracts that are called when applying a chunk.
    /// Calling `rollback` clears this recording.
    pub fn record_call(&self, code_hash: CryptoHash) {
        let mut guard = self.uncommitted_calls.lock().expect("no panics");
        guard.insert(code_hash.into());
    }

    /// Stores the contract code as an uncommitted deploy.
    ///
    /// Subsequent calls to `get` will return the code that was stored here.
    /// Calling `rollback` clears this uncommitted deploy.
    pub fn store(&self, code: ContractCode) {
        let mut guard = self.uncommitted_deploys.write().expect("no panics");
        guard.insert((*code.hash()).into(), code);
    }

    /// Rolls back the previous recording of contract calls and deployments.
    pub(crate) fn rollback(&mut self) {
        {
            let mut guard = self.uncommitted_calls.lock().expect("no panics");
            guard.clear();
        }
        {
            let mut guard = self.uncommitted_deploys.write().expect("no panics");
            guard.clear();
        }
    }

    /// Returns the list of contract calls and deployments collected so far.
    ///
    /// This should be called when there will be no further deployments or calls to contracts.
    /// Note: This also serves as the commit operation, so there is no other explicit commit method.
    pub(crate) fn finalize(self) -> ContractStorageResult {
        let contract_calls = {
            let guard = self.uncommitted_calls.lock().expect("no panics");
            guard.iter().cloned().collect()
        };
        // NOTE: We do not destruct the `uncommitted_deploys` here, since other copies of this storage may still
        // be used by contract preparation pipeline (possibly running in separate thread) after this call.
        let contract_deploys = {
            let guard = self.uncommitted_deploys.write().expect("no panics");
            guard.keys().cloned().collect()
        };
        ContractStorageResult { contract_deploys, contract_calls }
    }
}
