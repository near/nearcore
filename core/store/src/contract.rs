use crate::TrieStorage;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_vm_runner::ContractCode;
use std::collections::{BTreeMap, HashSet};
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

    /// List of code-hashes for the contracts retrieved from the storage.
    /// This does not include the contracts deployed and then read.
    // TODO(#11099): For collecting deployed code, consolidate this with uncommitted_deploys.
    storage_reads: Arc<Mutex<Option<HashSet<CodeHash>>>>,
}

/// Result of finalizing the contract storage.
pub struct ContractStorageResult {
    pub contract_accesses: Vec<CodeHash>,
}

impl ContractStorage {
    pub fn new(storage: Arc<dyn TrieStorage>) -> Self {
        Self {
            storage,
            uncommitted_deploys: Default::default(),
            storage_reads: Arc::new(Mutex::new(Some(HashSet::new()))),
        }
    }

    pub fn get(&self, code_hash: CryptoHash) -> Option<ContractCode> {
        {
            let guard = self.uncommitted_deploys.lock().expect("no panics");
            if let Some(v) = guard.get(&code_hash) {
                return Some(ContractCode::new(v.code().to_vec(), Some(code_hash)));
            }
        }

        let contract_code = match self.storage.retrieve_raw_bytes(&code_hash) {
            Ok(raw_code) => Some(ContractCode::new(raw_code.to_vec(), Some(code_hash))),
            Err(_) => None,
        };

        if contract_code.is_some() {
            let mut guard = self.storage_reads.lock().expect("no panics");
            guard.as_mut().expect("must not be called after finalize").insert(CodeHash(code_hash));
        }

        contract_code
    }

    pub fn store(&self, code: ContractCode) {
        let mut guard = self.uncommitted_deploys.lock().expect("no panics");
        guard.insert(*code.hash(), code);
    }

    /// Destructs the ContractStorage and returns the list of storage reads.
    pub(crate) fn finalize(self) -> ContractStorageResult {
        let mut guard = self.storage_reads.lock().expect("no panics");
        // TODO(#11099): Change `replace` to `take` after investigating why `get` is called after the TrieUpdate
        // is finalizing in the yield-resume tests.
        ContractStorageResult {
            contract_accesses: guard.replace(HashSet::new()).unwrap().into_iter().collect(),
        }
    }
}
