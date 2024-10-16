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
    uncommitted_deploys: Arc<RwLock<Option<BTreeMap<CryptoHash, ContractCode>>>>,

    committed_deploys: Option<BTreeMap<CryptoHash, ContractCode>>,

    /// List of code-hashes for the contracts retrieved from the storage.
    /// This does not include the contracts deployed and then read.
    // TODO(#11099): For collecting deployed code, consolidate this with uncommitted_deploys.
    uncommitted_accesses: Arc<Mutex<Option<BTreeSet<CodeHash>>>>,

    committed_accesses: Option<BTreeSet<CodeHash>>>,
}

/// Result of finalizing the contract storage.
pub struct ContractStorageResult {
    pub contract_accesses: Vec<CodeHash>,
    pub contract_deploys: Vec<ContractCode>,
}

// TODO(#11099): Implement commit and rollback.
impl ContractStorage {
    pub fn new(storage: Arc<dyn TrieStorage>) -> Self {
        Self {
            storage,
            uncommitted_deploys: Arc::new(RwLock::new(Some(BTreeMap::new()))),
            committed_deploys: None,
            uncommitted_accesses: Arc::new(Mutex::new(Some(BTreeSet::new()))),
            committed_accesses: None,
        }
    }

    pub fn get(&self, code_hash: CryptoHash) -> Option<ContractCode> {
        {
            let guard = self.uncommitted_deploys.read().expect("no panics");
            let deploys = guard.as_ref().expect("must not be called after finalized");
            if let Some(v) = deploys.get(&code_hash) {
                return Some(ContractCode::new(v.code().to_vec(), Some(code_hash)));
            }
        }

        if cfg!(feature = "contract_distribution") {
            let mut guard = self.uncommitted_accesses.lock().expect("no panics");
            let accesses = guard.as_mut().expect("must not be called after finalized");
            accesses.insert(CodeHash(code_hash));
        }

        match self.storage.retrieve_raw_bytes(&code_hash) {
            Ok(raw_code) => Some(ContractCode::new(raw_code.to_vec(), Some(code_hash))),
            Err(_) => None,
        }
    }

    pub fn store(&self, code: ContractCode) {
        let mut guard = self.uncommitted_deploys.write().expect("no panics");
        let deploys = guard.as_mut().expect("must not be called after finalized");
        deploys.insert(*code.hash(), code);
    }

    pub(crate) fn commit(&mut self) {
        {
        let mut guard = self.uncommitted_deploys.write().expect("no panics");
        // TODO(#11099): We should leave the uncommitted_deploys None after commit.
        // However there is code that calls commit multiple times before finalizing or
        // re-initializing, so we leave an empty map here.
        let _discarded_deploys = self.committed_deploys.replace(guard.replace(BTreeMap::new()));
        }
        {
            let mut guard = self.uncommitted_accesses.lock().expect("no panics");
            // TODO(#11099): We should leave the uncommitted_deploys None after commit.
            // However there is code that calls commit multiple times before finalizing or
            // re-initializing, so we leave an empty map here.
            let _discarded_accesses = self.committed_accesses.replace(guard.replace(BTreeSet::new()));
        }
    } 

    pub(crate) fn rollback(&mut self) {
        {
            debug_assert!(self.committed_deploys.is_none(), "Must not rollback after commit");
        let mut guard = self.uncommitted_deploys.write().expect("no panics");
        let _discarded_deploys = guard.replace(BTreeMap::new());
        }
        {
            debug_assert!(self.committed_accesses.is_none(), "Must not rollback after commit");
            let mut guard = self.uncommitted_accesses.lock().expect("no panics");
            let _discarded_reads = guard.replace(BTreeSet::new());
            }
    } 

    /// Destructs the ContractStorage and returns the list of storage reads.
    pub(crate) fn finalize(self) -> ContractStorageResult {
         // TODO(#11099): This should not happen but there is code that omits calling commit before finalizing.
         if self.committed_deploys.is_none(){
            debug_assert!(self.committed_accesses.is_none());
            self.commit();
        }
        let contract_accesses = self.committed_deploys.take().unwrap();
        let contract_deploys = self.committed_deploys.take().unwrap();
        ContractStorageResult { contract_accesses, contract_deploys }
    }
}
