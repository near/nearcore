use crate::TrieStorage;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_vm_runner::ContractCode;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// Tracks the uncommitted and committed contract calls and deployments, while applying the receipts in a chunk.
#[derive(Default)]
struct ContractsTracker {
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
    uncommitted_deploys: BTreeMap<CodeHash, ContractCode>,

    /// List of code-hashes for the contracts called,
    /// before committing the receipt the call belongs to.
    uncommitted_calls: BTreeSet<CodeHash>,

    /// Deployments moved from `uncommitted_deploys` by calling `commit`.
    /// These are returned from `finalize`.
    committed_deploys: BTreeMap<CodeHash, ContractCode>,

    /// Calls moved from `uncommitted_calls` by calling `commit`.
    /// These are returned from `finalize`.
    committed_calls: BTreeSet<CodeHash>,
}

impl ContractsTracker {
    fn get(&self, code_hash: CodeHash) -> Option<ContractCode> {
        self.uncommitted_deploys
            .get(&code_hash)
            .or_else(|| self.committed_deploys.get(&code_hash))
            .and_then(|contract| {
                Some(ContractCode::new(contract.code().to_vec(), Some(code_hash.into())))
            })
    }

    fn call(&mut self, code_hash: CodeHash) {
        self.uncommitted_calls.insert(code_hash);
    }

    fn deploy(&mut self, code: ContractCode) {
        self.uncommitted_deploys.insert((*code.hash()).into(), code);
    }

    fn clear_uncommitted(&mut self) {
        self.uncommitted_deploys.clear();
        self.uncommitted_calls.clear();
    }

    fn commit(&mut self) {
        let deploys = std::mem::take(&mut self.uncommitted_deploys);
        for (code_hash, contract) in deploys.into_iter() {
            self.committed_deploys.insert(code_hash, contract);
        }

        let calls = std::mem::take(&mut self.uncommitted_calls);
        for code_hash in calls.into_iter() {
            self.committed_calls.insert(code_hash);
        }
    }

    fn get_committed(&self) -> ContractStorageResult {
        ContractStorageResult {
            contract_calls: self.committed_calls.iter().cloned().collect(),
            contract_deploys: self.committed_deploys.keys().cloned().collect(),
        }
    }
}

/// Result of finalizing the contract storage, containing the committed calls and deployments.
pub struct ContractStorageResult {
    /// List of code-hashes for the contracts called while applying the chunk.
    pub contract_calls: Vec<CodeHash>,
    /// List of code-hashes for the contracts deployed while applying the chunk.
    pub contract_deploys: Vec<CodeHash>,
}

/// Reads contract code from the trie by its hash.
///
/// Cloning is cheap.
#[derive(Clone)]
pub struct ContractStorage {
    storage: Arc<dyn TrieStorage>,
    /// Tracker that tracks the contract calls and deployments that are not yet committed.
    tracker: Arc<Mutex<ContractsTracker>>,
}

impl ContractStorage {
    pub fn new(storage: Arc<dyn TrieStorage>) -> Self {
        Self { storage, tracker: Arc::new(Mutex::new(ContractsTracker::default())) }
    }

    /// Retrieves the contract code by its hash.
    ///
    /// This is called when preparing (eg. compiling) the contract for execution optimistically, and
    /// the function call for the returned contract may not be included in the currently applied chunk.
    /// Thus, the `record_call` method should also be called to indicate that the calling the contract
    /// is actually included in the chunk application.
    pub fn get(&self, code_hash: CryptoHash) -> Option<ContractCode> {
        {
            let tracker = self.tracker.lock().expect("no panics");
            if let Some(contract) = tracker.get(code_hash.into()) {
                return Some(ContractCode::new(contract.code().to_vec(), Some(code_hash)));
            }
        }

        match self.storage.retrieve_raw_bytes(&code_hash) {
            Ok(raw_code) => Some(ContractCode::new(raw_code.to_vec(), Some(code_hash))),
            Err(_) => None,
        }
    }

    /// Records an uncommitted call to a contract by code-hash.
    ///
    /// This is used to capture the contracts that are called when applying a chunk.
    /// Calling `rollback` clears this recording.
    pub fn record_call(&self, code_hash: CryptoHash) {
        let mut tracker = self.tracker.lock().expect("no panics");
        tracker.call(code_hash.into());
    }

    /// Stores the contract code as an uncommitted deploy.
    ///
    /// Subsequent calls to `get` will return the code that was stored here.
    /// Calling `rollback` clears this uncommitted deploy.
    pub fn record_deploy(&self, code: ContractCode) {
        let mut tracker = self.tracker.lock().expect("no panics");
        tracker.deploy(code);
    }

    /// Commits the uncommitted recording of contract calls and deployments.
    /// 
    /// This adds the uncommitted calls and deployments to the committed list and clears the uncommitted list.
    /// Note that there can be multiple calls to `commit`.
    pub(crate) fn commit(&mut self) {
        let mut tracker = self.tracker.lock().expect("no panics");
        tracker.commit();
    }

    /// Rolls back the uncommitted recording of contract calls and deployments.
    pub(crate) fn clear_uncommitted(&mut self) {
        let mut tracker = self.tracker.lock().expect("no panics");
        tracker.clear_uncommitted();
    }

    /// Finalizes this instance and returns the list of committed contract calls and deployments collected so far.
    ///
    /// This should be called when there will be no further deployments or calls to contracts.
    pub(crate) fn finalize(self) -> ContractStorageResult {
        let tracker = self.tracker.lock().expect("no panics");
        tracker.get_committed()
    }
}
