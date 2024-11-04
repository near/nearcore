use crate::{metrics, TrieStorage};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::contract_distribution::{CodeHash, ContractUpdates};
use near_vm_runner::ContractCode;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

/// Tracks the uncommitted and committed deployments and calls to contracts, while applying the receipts in a chunk.
///
/// Provides methods to record deployments as uncommitted and then commit or rollback them.
/// Committing deployments appends them to a committed list and rollback clears the uncommitted list.
/// The contract calls and committed deployments are retrieved by calling `finalize`.
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

    /// Deployments moved from `uncommitted_deploys` by calling `commit`.
    /// These are returned from `finalize`.
    committed_deploys: BTreeMap<CodeHash, ContractCode>,

    /// List of code-hashes for the contracts called.
    ///
    /// We do not distinguish between committed and uncommitted calls, because we need
    /// to record all calls to validate both successful and failing function calls.
    contract_calls: HashSet<CodeHash>,
}

impl ContractsTracker {
    fn get(&self, code_hash: CodeHash) -> Option<ContractCode> {
        self.uncommitted_deploys
            .get(&code_hash)
            .or_else(|| self.committed_deploys.get(&code_hash))
            .map(|contract| ContractCode::new(contract.code().to_vec(), Some(code_hash.into())))
    }

    fn call(&mut self, code_hash: CodeHash) {
        self.contract_calls.insert(code_hash);
    }

    fn deploy(&mut self, code: ContractCode) {
        self.uncommitted_deploys.insert((*code.hash()).into(), code);
    }

    /// Rollsback the uncommitted deployments.
    fn rollback_deploys(&mut self) {
        self.uncommitted_deploys.clear();
    }

    /// Commits the uncommitted deployments by moving them to the set of committed deployments and clearing the uncommitted list.
    fn commit_deploys(&mut self) {
        let deploys = std::mem::take(&mut self.uncommitted_deploys);
        for (code_hash, contract) in deploys.into_iter() {
            self.committed_deploys.insert(code_hash, contract);
        }
    }

    /// Finalizes this tracker and returns the calls and committed deployments.
    fn finalize(mut self) -> ContractUpdates {
        ContractUpdates {
            contract_accesses: std::mem::take(&mut self.contract_calls),
            contract_deploys: self.committed_deploys.into_values().collect(),
        }
    }
}

/// Reads contract code from the trie by its hash.
///
/// Cloning is cheap.
#[derive(Clone)]
pub struct ContractStorage {
    storage: Arc<dyn TrieStorage>,
    /// Tracker that tracks the contract calls and committed/uncommitted deployments.
    ///
    /// Calling `finalize`` on any instance of `ContractStorage` will finalize the tracker and
    /// replace the value of this field with `None`, so it is not usable after that.
    tracker: Arc<Mutex<Option<ContractsTracker>>>,
}

impl ContractStorage {
    pub fn new(storage: Arc<dyn TrieStorage>) -> Self {
        Self { storage, tracker: Arc::new(Mutex::new(Some(ContractsTracker::default()))) }
    }

    /// Retrieves the contract code by its hash.
    ///
    /// This is called when preparing (eg. compiling) the contract for execution optimistically, and
    /// the function call for the returned contract may not be included in the currently applied chunk.
    /// Thus, the `record_call` method should also be called to indicate that the calling the contract
    /// is actually included in the chunk application.
    pub fn get(&self, code_hash: CryptoHash) -> Option<ContractCode> {
        {
            let guard = self.tracker.lock().expect("no panics");
            // The tracker may be finalized before the receipt preparation pipeline calls this function.
            // In this case we should skip checking the tracker and directly read from the storage.
            // Note that this does not cause any correctness issue, because the pipeline stops processing when
            // it hits a new deployment, so the contract requested by the pipeline will not be in the committed or
            // uncommitted deployment list.
            if let Some(tracker) = guard.as_ref() {
                if let Some(contract) = tracker.get(code_hash.into()) {
                    return Some(ContractCode::new(contract.code().to_vec(), Some(code_hash)));
                }
            }
        }

        match self.storage.retrieve_raw_bytes(&code_hash) {
            Ok(raw_code) => Some(ContractCode::new(raw_code.to_vec(), Some(code_hash))),
            Err(StorageError::MissingTrieValue(context, _)) => {
                metrics::STORAGE_MISSING_CONTRACTS_COUNT
                    .with_label_values(&[context.metrics_label()])
                    .inc();
                None
            }
            Err(_) => None,
        }
    }

    /// Records a call to a contract by code-hash.
    ///
    /// This is used to capture the contracts that are called when applying a chunk.
    pub fn record_call(&self, code_hash: CryptoHash) {
        let mut guard = self.tracker.lock().expect("no panics");
        let tracker = guard.as_mut().expect("must not be called after finalizing");
        tracker.call(code_hash.into());
    }

    /// Stores the contract code as an uncommitted deploy.
    ///
    /// Subsequent calls to `get` will return the code that was stored here. Calling `rollback_deploys` clears
    /// this uncommitted deploy and calling `commit_deploys` moves it to the committed list.
    pub fn record_deploy(&self, code: ContractCode) {
        let mut guard = self.tracker.lock().expect("no panics");
        let tracker = guard.as_mut().expect("must not be called after finalizing");
        tracker.deploy(code);
    }

    /// Commits the uncommitted recording of contract deployments.
    ///
    /// Note that there can be multiple calls to `commit_deploys`. Each commit moves the uncommitted deployments
    /// to the committed list and clears the uncommitted list.
    pub(crate) fn commit_deploys(&mut self) {
        let mut guard = self.tracker.lock().expect("no panics");
        let tracker = guard.as_mut().expect("must not be called after finalizing");
        tracker.commit_deploys();
    }

    /// Rolls back the uncommitted recording of contract deployments.
    ///
    /// Note that there can be multiple calls to `rollback_deploys`. Each rollback clears the uncommitted deployments
    /// but does not modify the list of committed deployments.
    pub(crate) fn rollback_deploys(&mut self) {
        let mut guard = self.tracker.lock().expect("no panics");
        let tracker = guard.as_mut().expect("must not be called after finalizing");
        tracker.rollback_deploys();
    }

    /// Finalizes this instance and returns the list of contract calls and committed deployments recorded so far.
    ///
    /// It also finalizes and destructs the inner`ContractsTracker` so there must be no other deployments or
    /// calls to contracts after this returns.
    ///
    /// NOTE: The same contract may be deployed multiple times to different accounts. Thus, if a contract that was previously
    /// deployed to an account is now deployed to a different account, we still include the contract in the list of `contracts_deployed`.
    /// This can be optimized later by checking if the deployed contract already exists in the storage and excluding from the returned list.
    pub(crate) fn finalize(self) -> ContractUpdates {
        let mut guard = self.tracker.lock().expect("no panics");
        let tracker = guard.take().expect("finalize must be called only once");
        tracker.finalize()
    }
}
