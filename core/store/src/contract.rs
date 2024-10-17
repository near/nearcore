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
    uncommitted_deploys: Arc<RwLock<Option<BTreeMap<CodeHash, ContractCode>>>>,

    /// List of code-hashes for the contracts retrieved from the storage.
    /// This does not include the contracts deployed and then read.
    // TODO(#11099): For collecting deployed code, consolidate this with uncommitted_deploys.
    uncommitted_accesses: Arc<Mutex<Option<BTreeSet<CodeHash>>>>,
}

/// Result of finalizing the contract storage.
pub struct ContractStorageResult {
    /// List of code-hashes for the contracts retrieved from the storage while applying the chunk.
    pub contract_accesses: Vec<CodeHash>,
    /// List of code-hashes for the contracts deployed while applying the chunk.
    pub contract_deploys: Vec<CodeHash>,
}

// TODO(#11099): Implement commit and rollback.
impl ContractStorage {
    pub fn new(storage: Arc<dyn TrieStorage>) -> Self {
        Self {
            storage,
            uncommitted_deploys: Arc::new(RwLock::new(Some(BTreeMap::new()))),
            uncommitted_accesses: Arc::new(Mutex::new(Some(BTreeSet::new()))),
        }
    }

    pub fn get(&self, code_hash: CryptoHash) -> Option<ContractCode> {
        {
            let guard = self.uncommitted_deploys.read().expect("no panics");
            let deploys = guard.as_ref().expect("must not be called after finalized");
            if let Some(v) = deploys.get(&CodeHash(code_hash)) {
                return Some(ContractCode::new(v.code().to_vec(), Some(code_hash)));
            }
        }

        let contract_code = match self.storage.retrieve_raw_bytes(&code_hash) {
            Ok(raw_code) => Some(ContractCode::new(raw_code.to_vec(), Some(code_hash))),
            Err(_) => None,
        };

        if contract_code.is_some() {
            let mut guard = self.uncommitted_accesses.lock().expect("no panics");
            guard.as_mut().expect("must not be called after finalize").insert(CodeHash(code_hash));
        }

        contract_code
    }

    pub fn store(&self, code: ContractCode) {
        let mut guard = self.uncommitted_deploys.write().expect("no panics");
        let deploys = guard.as_mut().expect("must not be called after finalized");
        deploys.insert(CodeHash(*code.hash()), code);
    }

    /// Rolls back the previous recording of accesses and deployments.
    pub(crate) fn rollback(&mut self) {
        {
            let mut guard = self.uncommitted_accesses.lock().expect("no panics");
            let _discarded_reads = guard.replace(BTreeSet::new());
        }
        {
            let mut guard = self.uncommitted_deploys.write().expect("no panics");
            let _discarded_deploys = guard.replace(BTreeMap::new());
        }
    }

    /// Destructs the ContractStorage and returns the list of contract deployments and accesses.
    /// This serves as the commit operation, so there is no other explicit commit method.
    pub(crate) fn finalize(self) -> ContractStorageResult {
        let contract_accesses = {
            let mut guard = self.uncommitted_accesses.lock().expect("no panics");
            guard.replace(BTreeSet::new()).unwrap().into_iter().collect()
        };
        // TODO(#11099): Change `replace` to `take` after investigating why `get` is called after the TrieUpdate
        // is finalizing in the yield-resume tests.
        let contract_deploys = {
            let mut guard = self.uncommitted_deploys.write().expect("no panics");
            guard.replace(BTreeMap::new()).unwrap().into_keys().collect()
        };
        ContractStorageResult { contract_deploys, contract_accesses }
    }
}
