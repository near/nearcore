use crate::contract_code_cache::ContractCodeCache;
use crate::{TrieStorage, metrics};
use near_primitives::errors::{MissingTrieValue, StorageError};
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::contract_distribution::{CodeHash, ContractUpdates};
use near_primitives_core::code::ContractCode;
use parking_lot::Mutex;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ContractType {
    /// Local contract, deployed to a specific account
    Local,
    /// Global contract
    Global,
}

/// Tracks deployments of this contract.
struct DeployedContract {
    /// The deployed contract
    code: ContractCode,
    /// List of all types with which this contract was deployed.
    types: BTreeSet<ContractType>,
}

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
    uncommitted_deploys: BTreeMap<CodeHash, DeployedContract>,

    /// Deployments moved from `uncommitted_deploys` by calling `commit`.
    /// These are returned from `finalize`.
    committed_deploys: BTreeMap<CodeHash, DeployedContract>,

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
            .map(|contract| {
                ContractCode::new(contract.code.code().to_vec(), Some(code_hash.into()))
            })
    }

    fn call(&mut self, code_hash: CodeHash) {
        self.contract_calls.insert(code_hash);
    }

    fn deploy(&mut self, code: ContractCode, contract_type: ContractType) {
        let deployed_contract =
            DeployedContract { code, types: std::iter::once(contract_type).collect() };
        Self::add_deployed_contract(deployed_contract, &mut self.uncommitted_deploys);
    }

    /// Add a DeployedContract to the given BTreeMap.
    /// If the map already contains a DeployedContract for this contract hash, it merges the two structs by merging the `types` lists.
    fn add_deployed_contract(
        deploys: DeployedContract,
        map: &mut BTreeMap<CodeHash, DeployedContract>,
    ) {
        match map.entry((*deploys.code.hash()).into()) {
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(deploys);
            }
            Entry::Occupied(mut occupied_entry) => {
                occupied_entry.get_mut().types.extend(deploys.types);
            }
        }
    }

    /// Rollback the uncommitted deployments.
    fn rollback_deploys(&mut self) {
        self.uncommitted_deploys.clear();
    }

    /// Commits the uncommitted deployments by moving them to the set of committed deployments and clearing the uncommitted list.
    fn commit_deploys(&mut self) {
        let deploys = std::mem::take(&mut self.uncommitted_deploys);
        for (_code_hash, deployed) in deploys {
            Self::add_deployed_contract(deployed, &mut self.committed_deploys);
        }
    }

    /// Finalizes this tracker and returns the calls and committed deployments.
    /// The contract_deploys field doesn't contain deployed global contracts, those are distributed separately.
    fn finalize(mut self) -> ContractUpdates {
        let contract_deploys = self
            .committed_deploys
            .into_values()
            .filter_map(|deployed| {
                if deployed.types.iter().all(|t| *t == ContractType::Global) {
                    // Global contracts are distributed using a different mechanism, don't include them in contract_deploys
                    None
                } else {
                    Some(deployed.code)
                }
            })
            .collect();

        ContractUpdates {
            contract_accesses: std::mem::take(&mut self.contract_calls),
            contract_deploys,
        }
    }
}

/// Reads contract code by its hash: from the node's contract code cache when it has it,
/// otherwise from the trie, keeping what the trie returned in the cache.
///
/// Cloning is cheap.
#[derive(Clone)]
pub struct ContractStorage {
    storage: Arc<dyn TrieStorage>,
    /// Node-local, shard-independent copy of raw contract code. Only ever a cache: the trie
    /// decides whether a contract exists, the cache just supplies the bytes for its hash.
    /// `NoContractCodeCache` where nothing is kept.
    code_cache: Arc<dyn ContractCodeCache>,
    /// Tracker that tracks the contract calls and committed/uncommitted deployments.
    ///
    /// Calling `finalize`` on any instance of `ContractStorage` will finalize the tracker and
    /// replace the value of this field with `None`, so it is not usable after that.
    tracker: Arc<Mutex<Option<ContractsTracker>>>,
}

impl ContractStorage {
    /// `code_cache` is consulted ahead of `storage` and receives the code read from it.
    pub fn new(storage: Arc<dyn TrieStorage>, code_cache: Arc<dyn ContractCodeCache>) -> Self {
        Self {
            storage,
            code_cache,
            tracker: Arc::new(Mutex::new(Some(ContractsTracker::default()))),
        }
    }

    pub fn code_cache(&self) -> Arc<dyn ContractCodeCache> {
        self.code_cache.clone()
    }

    /// Retrieves the contract code by its hash.
    ///
    /// This is called when preparing (eg. compiling) the contract for execution optimistically, and
    /// the function call for the returned contract may not be included in the currently applied chunk.
    /// Thus, the `record_call` method should also be called to indicate that the calling the contract
    /// is actually included in the chunk application.
    pub fn get(&self, code_hash: CryptoHash) -> Option<ContractCode> {
        {
            let guard = self.tracker.lock();
            // The tracker may be finalized before the receipt preparation pipeline calls this function.
            // In this case we should skip checking the tracker and directly read from the storage.
            // Note that this does not cause any correctness issue, because the pipeline stops processing when
            // it hits a new deployment, so the contract requested by the pipeline will not be in the committed or
            // uncommitted deployment list.
            if let Some(tracker) = guard.as_ref()
                && let Some(contract) = tracker.get(code_hash.into())
            {
                return Some(ContractCode::new(contract.code().to_vec(), Some(code_hash)));
            }
        }

        if let Some(code) = self.code_cache.get(&code_hash) {
            return Some(code);
        }

        match self.storage.retrieve_raw_bytes(&code_hash) {
            Ok(raw_code) => {
                let code = ContractCode::new(raw_code.to_vec(), Some(code_hash));
                // Code found in the trie (or in a witness) is global: keep it so the next
                // compile of this contract, under any config, does not need the trie again.
                self.code_cache.put(&code);
                Some(code)
            }
            Err(StorageError::MissingTrieValue(MissingTrieValue { context, hash: _ })) => {
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
        let mut guard = self.tracker.lock();
        let tracker = guard.as_mut().expect("must not be called after finalizing");
        tracker.call(code_hash.into());
    }

    /// Stores the contract code as an uncommitted deploy.
    ///
    /// Subsequent calls to `get` will return the code that was stored here. Calling `rollback_deploys` clears
    /// this uncommitted deploy and calling `commit_deploys` moves it to the committed list.
    pub fn record_deploy(&self, code: ContractCode) {
        let mut guard = self.tracker.lock();
        let tracker = guard.as_mut().expect("must not be called after finalizing");
        tracker.deploy(code, ContractType::Local);
    }

    /// Same as record_deploy, used for global contracts.
    pub fn record_global_contract_deploy(&self, code: ContractCode) {
        let mut guard = self.tracker.lock();
        let tracker = guard.as_mut().expect("must not be called after finalizing");
        tracker.deploy(code, ContractType::Global);
    }

    /// Commits the uncommitted recording of contract deployments.
    ///
    /// Note that there can be multiple calls to `commit_deploys`. Each commit moves the uncommitted deployments
    /// to the committed list and clears the uncommitted list.
    pub(crate) fn commit_deploys(&self) {
        let mut guard = self.tracker.lock();
        let tracker = guard.as_mut().expect("must not be called after finalizing");
        tracker.commit_deploys();
    }

    /// Rolls back the uncommitted recording of contract deployments.
    ///
    /// Note that there can be multiple calls to `rollback_deploys`. Each rollback clears the uncommitted deployments
    /// but does not modify the list of committed deployments.
    pub(crate) fn rollback_deploys(&self) {
        let mut guard = self.tracker.lock();
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
    /// NOTE: Contract_deploys doesn't contain deployed global contracts, those are distributed separately.
    pub(crate) fn finalize(self) -> ContractUpdates {
        let mut guard = self.tracker.lock();
        let tracker = guard.take().expect("finalize must be called only once");
        // Deployed code, local or global, is kept in the cache whether this node tracks the
        // shard or only validated the chunk.
        for deployed in tracker.committed_deploys.values() {
            self.code_cache.put(&deployed.code);
        }
        tracker.finalize()
    }
}

#[cfg(test)]
mod tests {
    use crate::contract_code_cache::{ContractCodeCache, NoContractCodeCache};
    use crate::{TrieStorage, contract::ContractStorage};
    use itertools::Itertools;
    use near_primitives::{
        errors::{MissingTrieValue, MissingTrieValueContext, StorageError},
        hash::CryptoHash,
        stateless_validation::contract_distribution::CodeHash,
    };
    use near_primitives_core::code::ContractCode;
    use parking_lot::Mutex;
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    struct MockTrieStorage {
        store: HashMap<CryptoHash, Arc<[u8]>>,
    }

    impl MockTrieStorage {
        fn new() -> Self {
            Self { store: HashMap::new() }
        }

        fn insert(&mut self, hash: CryptoHash, data: Arc<[u8]>) {
            self.store.insert(hash, data);
        }
    }

    impl TrieStorage for MockTrieStorage {
        fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
            match self.store.get(hash) {
                Some(data) => Ok(data.clone()),
                None => Err(StorageError::MissingTrieValue(MissingTrieValue {
                    context: MissingTrieValueContext::TrieStorage,
                    hash: *hash,
                })),
            }
        }
    }

    fn empty_contract_storage() -> ContractStorage {
        ContractStorage::new(Arc::new(MockTrieStorage::new()), Arc::new(NoContractCodeCache))
    }

    /// Tests a scenario with old (already existing in the storage) and new contracts and finalizing after rolling back some deploys and committing others.
    #[test]
    fn test_contract_storage_finalize_after_rollback_and_commit() {
        let contracts = (0..4).map(|i| ContractCode::new(vec![i], None)).collect_vec();
        let (old_contracts, new_contracts) = contracts.split_at(2);

        // Insert old contracts (already deployed contracts) into the storage.
        let mut mock_storage = MockTrieStorage::new();
        for contract in old_contracts {
            mock_storage.insert(*contract.hash(), contract.code().to_vec().into());
        }

        let contract_storage =
            ContractStorage::new(Arc::new(mock_storage), Arc::new(NoContractCodeCache));

        contract_storage.record_deploy(old_contracts[0].clone_for_tests());
        contract_storage.record_deploy(new_contracts[0].clone_for_tests());

        contract_storage.record_call(*old_contracts[0].hash());
        contract_storage.record_call(*new_contracts[0].hash());

        contract_storage.rollback_deploys();

        contract_storage.record_deploy(old_contracts[1].clone_for_tests());
        contract_storage.record_deploy(new_contracts[1].clone_for_tests());

        contract_storage.record_call(*old_contracts[1].hash());
        contract_storage.record_call(*new_contracts[1].hash());

        contract_storage.commit_deploys();

        let updates = contract_storage.finalize();
        assert_eq!(
            updates.contract_accesses,
            HashSet::from_iter(vec![
                CodeHash(*old_contracts[0].hash()),
                CodeHash(*old_contracts[1].hash()),
                CodeHash(*new_contracts[0].hash()),
                CodeHash(*new_contracts[1].hash())
            ])
        );
        assert_eq!(
            updates.contract_deploy_hashes(),
            HashSet::from_iter(vec![
                CodeHash(*old_contracts[1].hash()),
                CodeHash(*new_contracts[1].hash())
            ])
        );
    }

    /// Tests a scenario with old (already existing in the storage) and new contracts and calling `get` after committing some deploys.
    #[test]
    fn test_contract_storage_get_after_new_deploys_and_commit() {
        let contracts = (0..4).map(|i| ContractCode::new(vec![i], None)).collect_vec();
        let (old_contracts, new_contracts) = contracts.split_at(2);

        // Insert old contracts (already deployed contracts) into the storage.
        let mut mock_storage = MockTrieStorage::new();
        for contract in old_contracts {
            mock_storage.insert(*contract.hash(), contract.code().to_vec().into());
        }

        let contract_storage =
            ContractStorage::new(Arc::new(mock_storage), Arc::new(NoContractCodeCache));

        // Only existing contracts should be returned by `get` before deploying the new ones.
        for contract in old_contracts {
            assert_eq!(contract_storage.get(*contract.hash()).unwrap().hash(), contract.hash());
        }
        for contract in new_contracts {
            assert!(contract_storage.get(*contract.hash()).is_none());
        }

        contract_storage.record_deploy(new_contracts[0].clone_for_tests());
        contract_storage.record_deploy(new_contracts[1].clone_for_tests());

        // Make the same `get` calls before and after commit. Both should return the same results.

        for contract in old_contracts {
            assert_eq!(contract_storage.get(*contract.hash()).unwrap().hash(), contract.hash());
        }
        for contract in new_contracts {
            assert_eq!(contract_storage.get(*contract.hash()).unwrap().hash(), contract.hash());
        }

        contract_storage.commit_deploys();

        for contract in old_contracts {
            assert_eq!(contract_storage.get(*contract.hash()).unwrap().hash(), contract.hash());
        }
        for contract in new_contracts {
            assert_eq!(contract_storage.get(*contract.hash()).unwrap().hash(), contract.hash());
        }
    }

    /// Tests a scenario with old (already existing in the storage) and new contracts and calling `get` after rolling back some deploys.
    #[test]
    fn test_contract_storage_get_after_new_deploys_and_rollback() {
        let contracts = (0..4).map(|i| ContractCode::new(vec![i], None)).collect_vec();
        let (old_contracts, new_contracts) = contracts.split_at(2);

        // Insert old contracts (already deployed contracts) into the storage.
        let mut mock_storage = MockTrieStorage::new();
        for contract in old_contracts {
            mock_storage.insert(*contract.hash(), contract.code().to_vec().into());
        }

        let contract_storage =
            ContractStorage::new(Arc::new(mock_storage), Arc::new(NoContractCodeCache));

        contract_storage.record_deploy(new_contracts[0].clone_for_tests());
        contract_storage.record_deploy(new_contracts[1].clone_for_tests());

        // Make the same `get` calls before and after rollback. Calls before rollback return the new contracts
        // but those after rollback should return only the old contracts.

        for contract in old_contracts {
            assert_eq!(contract_storage.get(*contract.hash()).unwrap().hash(), contract.hash());
        }
        for contract in new_contracts {
            assert_eq!(contract_storage.get(*contract.hash()).unwrap().hash(), contract.hash());
        }

        contract_storage.rollback_deploys();

        for contract in old_contracts {
            assert_eq!(contract_storage.get(*contract.hash()).unwrap().hash(), contract.hash());
        }
        for contract in new_contracts {
            assert!(contract_storage.get(*contract.hash()).is_none());
        }
    }

    /// Tests a scenario with existing and missing contracts, and calling `get` after finalizing the storage.
    #[test]
    fn test_contract_storage_get_after_finalize() {
        let contracts = (0..4).map(|i| ContractCode::new(vec![i], None)).collect_vec();
        let (existing_contracts, missing_contracts) = contracts.split_at(2);

        // Insert old contracts (already deployed contracts) into the storage.
        let mut mock_storage = MockTrieStorage::new();
        for contract in existing_contracts {
            mock_storage.insert(*contract.hash(), contract.code().to_vec().into());
        }

        let contract_storage =
            ContractStorage::new(Arc::new(mock_storage), Arc::new(NoContractCodeCache));

        // Make the same `get` calls before and after finalizing the tracker.
        // Both should return the same results for existing and missing contracts.

        for contract in existing_contracts {
            assert_eq!(contract_storage.get(*contract.hash()).unwrap().hash(), contract.hash());
        }
        for contract in missing_contracts {
            assert!(contract_storage.get(*contract.hash()).is_none());
        }

        let contract_storage_clone = contract_storage.clone();
        let _ = contract_storage.finalize();

        for contract in existing_contracts {
            assert_eq!(
                contract_storage_clone.get(*contract.hash()).unwrap().hash(),
                contract.hash()
            );
        }
        for contract in missing_contracts {
            assert!(contract_storage_clone.get(*contract.hash()).is_none());
        }
    }

    /// A global contract is recorded in ContractTracker like a local deploy, but is filtered out of
    /// the `contract_deploys` broadcast.
    #[test]
    fn test_contract_storage_filters_global_deploys_from_broadcast() {
        let local = ContractCode::new(vec![1], None);
        let global = ContractCode::new(vec![2], None);
        let contract_storage = ContractStorage::new(
            Arc::new(MockTrieStorage { store: HashMap::new() }),
            Arc::new(NoContractCodeCache),
        );

        contract_storage.record_deploy(local.clone_for_tests());
        contract_storage.record_global_contract_deploy(global.clone_for_tests());
        contract_storage.commit_deploys();

        // Both contracts can be accessed from the contract storage.
        assert!(contract_storage.get(*local.hash()).is_some());
        assert!(contract_storage.get(*global.hash()).is_some());

        // Only the local deploy is broadcast to chunk validators.
        let updates = contract_storage.finalize();
        assert_eq!(
            updates.contract_deploy_hashes(),
            HashSet::from([CodeHash(*local.hash())]),
            "global contracts must not be re-broadcast as deploys"
        );
    }

    /// Deploys are keyed by code hash, so the same code deployed both to an account and as a
    /// global contract shares a single entry. The local deploy still has to be broadcast, no
    /// matter which order the two are recorded in or whether they land in the same commit.
    #[test]
    fn test_contract_storage_broadcasts_code_deployed_both_locally_and_globally() {
        let code = ContractCode::new(vec![1], None);
        let expected = HashSet::from([CodeHash(*code.hash())]);

        // Same commit, local deploy recorded first.
        let contract_storage = empty_contract_storage();
        contract_storage.record_deploy(code.clone_for_tests());
        contract_storage.record_global_contract_deploy(code.clone_for_tests());
        contract_storage.commit_deploys();
        assert_eq!(contract_storage.finalize().contract_deploy_hashes(), expected);

        // Same commit, global deploy recorded first.
        let contract_storage = empty_contract_storage();
        contract_storage.record_global_contract_deploy(code.clone_for_tests());
        contract_storage.record_deploy(code.clone_for_tests());
        contract_storage.commit_deploys();
        assert_eq!(contract_storage.finalize().contract_deploy_hashes(), expected);

        // Separate commits: the global deploy is already committed when the local one arrives.
        let contract_storage = empty_contract_storage();
        contract_storage.record_global_contract_deploy(code.clone_for_tests());
        contract_storage.commit_deploys();
        contract_storage.record_deploy(code.clone_for_tests());
        contract_storage.commit_deploys();
        assert_eq!(contract_storage.finalize().contract_deploy_hashes(), expected);
    }

    /// In-memory `ContractCodeCache` that records every put, for checking what the storage
    /// serves from where.
    #[derive(Default)]
    struct MockCodeCache {
        codes: Mutex<HashMap<CryptoHash, Vec<u8>>>,
        puts: Mutex<Vec<CryptoHash>>,
    }

    impl ContractCodeCache for MockCodeCache {
        fn get(&self, code_hash: &CryptoHash) -> Option<ContractCode> {
            let codes = self.codes.lock();
            let code = codes.get(code_hash)?;
            Some(ContractCode::new(code.clone(), Some(*code_hash)))
        }

        fn has(&self, code_hash: &CryptoHash) -> bool {
            self.codes.lock().contains_key(code_hash)
        }

        fn put(&self, code: &ContractCode) {
            self.puts.lock().push(*code.hash());
            self.codes.lock().insert(*code.hash(), code.code().to_vec());
        }

        #[cfg(feature = "test_features")]
        fn test_only_clear(&self) {
            self.codes.lock().clear();
        }
    }

    /// Code in the cache is served without touching the trie storage, which here lacks it.
    #[test]
    fn test_contract_storage_serves_code_cache_before_trie() {
        let contract = ContractCode::new(vec![1], None);
        let code_cache = Arc::new(MockCodeCache::default());
        code_cache.put(&contract);
        code_cache.puts.lock().clear();
        let contract_storage =
            ContractStorage::new(Arc::new(MockTrieStorage::new()), code_cache.clone());

        let read = contract_storage.get(*contract.hash()).unwrap();
        assert_eq!(read.hash(), contract.hash());
        assert_eq!(read.code(), contract.code());
        assert!(code_cache.puts.lock().is_empty(), "a cache hit must not write back");
    }

    /// Code read from the trie storage is kept in the cache; code the tracker holds for a
    /// pending deploy is not, as that deploy may still be rolled back.
    #[test]
    fn test_contract_storage_keeps_trie_code_in_store() {
        let in_trie = ContractCode::new(vec![1], None);
        let pending = ContractCode::new(vec![2], None);
        let mut mock_storage = MockTrieStorage::new();
        mock_storage.insert(*in_trie.hash(), in_trie.code().to_vec().into());
        let code_cache = Arc::new(MockCodeCache::default());
        let contract_storage = ContractStorage::new(Arc::new(mock_storage), code_cache.clone());
        contract_storage.record_deploy(pending.clone_for_tests());

        assert!(contract_storage.get(*pending.hash()).is_some());
        assert!(!code_cache.has(pending.hash()));

        assert!(contract_storage.get(*in_trie.hash()).is_some());
        assert!(code_cache.has(in_trie.hash()));
        assert_eq!(*code_cache.puts.lock(), vec![*in_trie.hash()]);

        // The second read is served by the cache: the trie storage is not asked again.
        assert!(contract_storage.get(*in_trie.hash()).is_some());
        assert_eq!(code_cache.puts.lock().len(), 1);
    }

    /// Finalizing keeps committed deploys, local and global, in the cache, and drops the
    /// rolled back one.
    #[test]
    fn test_contract_storage_finalize_keeps_committed_deploys_in_store() {
        let local = ContractCode::new(vec![1], None);
        let global = ContractCode::new(vec![2], None);
        let rolled_back = ContractCode::new(vec![3], None);
        let code_cache = Arc::new(MockCodeCache::default());
        let contract_storage =
            ContractStorage::new(Arc::new(MockTrieStorage::new()), code_cache.clone());

        contract_storage.record_deploy(rolled_back.clone_for_tests());
        contract_storage.rollback_deploys();
        contract_storage.record_deploy(local.clone_for_tests());
        contract_storage.record_global_contract_deploy(global.clone_for_tests());
        contract_storage.commit_deploys();
        assert!(code_cache.puts.lock().is_empty(), "nothing is stored before finalize");

        let updates = contract_storage.finalize();
        assert_eq!(updates.contract_deploy_hashes(), HashSet::from([CodeHash(*local.hash())]));
        assert!(code_cache.has(local.hash()));
        assert!(code_cache.has(global.hash()));
        assert!(!code_cache.has(rolled_back.hash()));
    }
}
