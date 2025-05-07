use crate::receipt_manager::ReceiptManager;
use near_parameters::vm::StorageGetMode;
use near_primitives::account::Account;
use near_primitives::account::id::AccountType;
use near_primitives::errors::{EpochError, StorageError};
use near_primitives::hash::CryptoHash;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, Balance, BlockHeight, EpochId, EpochInfoProvider, Gas};
use near_primitives::utils::create_receipt_id_from_action_hash;
use near_primitives::version::ProtocolVersion;
use near_store::contract::ContractStorage;
use near_store::trie::{AccessOptions, AccessTracker};
use near_store::{KeyLookupMode, TrieUpdate, TrieUpdateValuePtr, has_promise_yield_receipt};
use near_vm_runner::logic::errors::{AnyError, InconsistentStateError, VMLogicError};
use near_vm_runner::logic::types::ReceiptIndex;
use near_vm_runner::logic::{External, StorageAccessTracker, ValuePtr};
use near_vm_runner::{Contract, ContractCode};
use near_wallet_contract::wallet_contract;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct RuntimeExt<'a> {
    pub(crate) trie_update: &'a mut TrieUpdate,
    pub(crate) receipt_manager: &'a mut ReceiptManager,
    account_id: AccountId,
    account: Account,
    action_hash: CryptoHash,
    data_count: u64,
    epoch_id: EpochId,
    last_block_hash: CryptoHash,
    block_height: BlockHeight,
    epoch_info_provider: &'a dyn EpochInfoProvider,
    current_protocol_version: ProtocolVersion,
    storage_access_mode: StorageGetMode,
    trie_access_tracker: AccountingAccessTracker,
}

/// Error used by `RuntimeExt`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExternalError {
    /// Unexpected error which is typically related to the node storage corruption.
    /// It's possible the input state is invalid or malicious.
    StorageError(StorageError),
    /// Error when accessing validator information. Happens inside epoch manager.
    ValidatorError(EpochError),
}

impl From<ExternalError> for VMLogicError {
    fn from(err: ExternalError) -> Self {
        VMLogicError::ExternalError(AnyError::new(err))
    }
}

pub struct RuntimeExtValuePtr<'a, 'b> {
    value_ptr: TrieUpdateValuePtr<'a>,
    deref_options: AccessOptions<'b>,
    accounting_state: Arc<AccountingState>,
}

impl<'a, 'b> ValuePtr for RuntimeExtValuePtr<'a, 'b> {
    fn len(&self) -> u32 {
        self.value_ptr.len()
    }

    fn deref(&self, access_tracker: &mut dyn StorageAccessTracker) -> ExtResult<Vec<u8>> {
        match &self.value_ptr {
            TrieUpdateValuePtr::MemoryRef(data) => Ok(data.to_vec()),
            TrieUpdateValuePtr::Ref(trie, optimized_value_ref) => {
                let start_ttn = self.accounting_state.get_counts();
                let result = trie.deref_optimized(self.deref_options, &optimized_value_ref);
                self.accounting_state.commit_counts_since(start_ttn, access_tracker)?;
                Ok(result.map_err(wrap_storage_error)?)
            }
        }
    }
}

impl<'a> RuntimeExt<'a> {
    pub fn new(
        trie_update: &'a mut TrieUpdate,
        receipt_manager: &'a mut ReceiptManager,
        account_id: AccountId,
        account: Account,
        action_hash: CryptoHash,
        epoch_id: EpochId,
        last_block_hash: CryptoHash,
        block_height: BlockHeight,
        epoch_info_provider: &'a dyn EpochInfoProvider,
        current_protocol_version: ProtocolVersion,
        storage_access_mode: StorageGetMode,
        trie_access_tracker_state: Arc<AccountingState>,
    ) -> Self {
        RuntimeExt {
            trie_update,
            receipt_manager,
            account_id,
            account,
            action_hash,
            data_count: 0,
            epoch_id,
            last_block_hash,
            block_height,
            epoch_info_provider,
            current_protocol_version,
            storage_access_mode,
            trie_access_tracker: AccountingAccessTracker {
                allow_insert: true,
                state: trie_access_tracker_state,
            },
        }
    }

    #[inline]
    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    #[inline]
    pub fn account(&self) -> &Account {
        &self.account
    }

    pub fn create_storage_key(&self, key: &[u8]) -> TrieKey {
        TrieKey::ContractData { account_id: self.account_id.clone(), key: key.to_vec() }
    }

    #[inline]
    pub fn protocol_version(&self) -> ProtocolVersion {
        self.current_protocol_version
    }

    pub fn chain_id(&self) -> String {
        self.epoch_info_provider.chain_id()
    }
}

fn wrap_storage_error(error: StorageError) -> VMLogicError {
    VMLogicError::from(ExternalError::StorageError(error))
}

type ExtResult<T> = ::std::result::Result<T, VMLogicError>;

impl<'a> External for RuntimeExt<'a> {
    fn storage_set<'b>(
        &'b mut self,
        access_tracker: &mut dyn StorageAccessTracker,
        key: &[u8],
        value: &[u8],
    ) -> ExtResult<Option<Vec<u8>>> {
        // SUBTLE: Storage writes don't actually touch anything in the trie, at least not during
        // the contract execution -- they just put a value as a prospective in a map in
        // `TrieUpdate`. However we do need to grab the value for the evicted result and for all
        // intents and purposes we need to both account for TTN fees and record the path to the
        // value for the state witness. For that reason the lookup below has to happen through the
        // Trie.
        let start_ttn = self.trie_access_tracker.state.get_counts();
        let storage_key = self.create_storage_key(key);
        let options = AccessOptions::contract_runtime(&self.trie_access_tracker);
        let evicted_ptr = self
            .trie_update
            .get_ref(&storage_key, KeyLookupMode::MemOrTrie, options)
            .map_err(wrap_storage_error)?;
        let evicted = match evicted_ptr {
            None => None,
            Some(ptr) => {
                access_tracker.deref_write_evicted_value_bytes(u64::from(ptr.len()))?;
                Some(ptr.deref_value(options).map_err(wrap_storage_error)?)
            }
        };
        let _delta =
            self.trie_access_tracker.state.commit_counts_since(start_ttn, access_tracker)?;
        #[cfg(feature = "io_trace")]
        tracing::trace!(
            target: "io_tracer",
            storage_op = "write",
            key = base64(&key),
            size = value.len(),
            evicted_len = evicted.as_ref().map(Vec::len),
            tn_mem_reads = _delta.mem_reads,
            tn_db_reads = _delta.db_reads,
        );
        self.trie_update.set(storage_key, Vec::from(value));
        Ok(evicted)
    }

    fn storage_get<'b>(
        &'b self,
        access_tracker: &mut dyn StorageAccessTracker,
        key: &[u8],
    ) -> ExtResult<Option<Box<dyn ValuePtr + 'b>>> {
        let start_ttn = self.trie_access_tracker.state.get_counts();
        let storage_key = self.create_storage_key(key);
        let mode = match self.storage_access_mode {
            StorageGetMode::FlatStorage => KeyLookupMode::MemOrFlatOrTrie,
            StorageGetMode::Trie => KeyLookupMode::MemOrTrie,
        };
        let deref_options = AccessOptions::contract_runtime(&self.trie_access_tracker);
        // SUBTLE: unlike `write` or `remove` which does not record TTN fees if the read operations
        // fail for the evicted values, this will record the TTN fees unconditionally.
        let result = self
            .trie_update
            .get_ref(&storage_key, mode, deref_options)
            .map_err(wrap_storage_error)
            .map(|option| {
                option.map(|value_ptr| {
                    Box::new(RuntimeExtValuePtr {
                        value_ptr,
                        deref_options,
                        accounting_state: Arc::clone(&self.trie_access_tracker.state),
                    }) as Box<dyn ValuePtr>
                })
            });
        let _delta =
            self.trie_access_tracker.state.commit_counts_since(start_ttn, access_tracker)?;
        #[cfg(feature = "io_trace")]
        if let Ok(read) = &result {
            tracing::trace!(
                target: "io_tracer",
                storage_op = "read",
                key = base64(&key),
                size = read.as_ref().map(|v| v.len()),
                tn_db_reads = _delta.db_reads,
                tn_mem_reads = _delta.mem_reads,
            );
        }

        Ok(result?)
    }

    fn storage_remove(
        &mut self,
        access_tracker: &mut dyn StorageAccessTracker,
        key: &[u8],
    ) -> ExtResult<Option<Vec<u8>>> {
        // SUBTLE: Storage removals don't actually touch anything in the trie, at least not during
        // the contract execution -- they just put a value as a prospective in a map in
        // `TrieUpdate`. However we do need to grab the value for the evicted result and for all
        // intents and purposes we need to both account for TTN fees and record the path to the
        // value for the state witness. For that reason the lookup below has to happen through the
        // Trie.
        let start_ttn = self.trie_access_tracker.state.get_counts();
        let storage_key = self.create_storage_key(key);
        let options = AccessOptions::contract_runtime(&self.trie_access_tracker);
        let removed = self
            .trie_update
            .get_ref(&storage_key, KeyLookupMode::MemOrTrie, options)
            .map_err(wrap_storage_error)?;
        let removed = match removed {
            None => None,
            Some(ptr) => {
                access_tracker.deref_removed_value_bytes(u64::from(ptr.len()))?;
                Some(ptr.deref_value(options).map_err(wrap_storage_error)?)
            }
        };
        self.trie_update.remove(storage_key);
        let _delta =
            self.trie_access_tracker.state.commit_counts_since(start_ttn, access_tracker)?;
        #[cfg(feature = "io_trace")]
        tracing::trace!(
            target: "io_tracer",
            storage_op = "remove",
            key = base64(&key),
            evicted_len = removed.as_ref().map(Vec::len),
            tn_mem_reads = _delta.mem_reads,
            tn_db_reads = _delta.db_reads,
        );

        Ok(removed)
    }

    fn storage_has_key(
        &mut self,
        access_tracker: &mut dyn StorageAccessTracker,
        key: &[u8],
    ) -> ExtResult<bool> {
        let start_ttn = self.trie_access_tracker.state.get_counts();
        let storage_key = self.create_storage_key(key);
        let mode = match self.storage_access_mode {
            StorageGetMode::FlatStorage => KeyLookupMode::MemOrFlatOrTrie,
            StorageGetMode::Trie => KeyLookupMode::MemOrTrie,
        };
        let result = self
            .trie_update
            .get_ref(&storage_key, mode, AccessOptions::contract_runtime(&self.trie_access_tracker))
            .map(|x| x.is_some())
            .map_err(wrap_storage_error);
        let _delta =
            self.trie_access_tracker.state.commit_counts_since(start_ttn, access_tracker)?;
        #[cfg(feature = "io_trace")]
        tracing::trace!(
            target: "io_tracer",
            storage_op = "exists",
            key = base64(&key),
            tn_mem_reads = _delta.mem_reads,
            tn_db_reads = _delta.db_reads,
        );
        Ok(result?)
    }

    fn generate_data_id(&mut self) -> CryptoHash {
        let data_id = create_receipt_id_from_action_hash(
            self.current_protocol_version,
            &self.action_hash,
            &self.last_block_hash,
            self.block_height,
            self.data_count,
        );
        self.data_count += 1;
        data_id
    }

    fn get_recorded_storage_size(&self) -> usize {
        // `recorded_storage_size()` doesn't provide the exact size of storage proof
        // as it doesn't cover some corner cases (see https://github.com/near/nearcore/issues/10890),
        // so we use the `upper_bound` version to estimate how much storage proof
        // could've been generated by the receipt. As long as upper bound is
        // under the limit we can be sure that the actual value is also under the limit.
        self.trie_update.trie().recorded_storage_size_upper_bound()
    }

    fn validator_stake(&self, account_id: &AccountId) -> ExtResult<Option<Balance>> {
        self.epoch_info_provider
            .validator_stake(&self.epoch_id, account_id)
            .map_err(|e| ExternalError::ValidatorError(e).into())
    }

    fn validator_total_stake(&self) -> ExtResult<Balance> {
        self.epoch_info_provider
            .validator_total_stake(&self.epoch_id)
            .map_err(|e| ExternalError::ValidatorError(e).into())
    }

    fn create_action_receipt(
        &mut self,
        receipt_indices: Vec<ReceiptIndex>,
        receiver_id: AccountId,
    ) -> Result<ReceiptIndex, VMLogicError> {
        let data_ids = std::iter::from_fn(|| Some(self.generate_data_id()))
            .take(receipt_indices.len())
            .collect();
        self.receipt_manager.create_action_receipt(data_ids, receipt_indices, receiver_id)
    }

    fn create_promise_yield_receipt(
        &mut self,
        receiver_id: AccountId,
    ) -> Result<(ReceiptIndex, CryptoHash), VMLogicError> {
        let input_data_id = self.generate_data_id();
        self.receipt_manager
            .create_promise_yield_receipt(input_data_id, receiver_id)
            .map(|receipt_index| (receipt_index, input_data_id))
    }

    fn submit_promise_resume_data(
        &mut self,
        data_id: CryptoHash,
        data: Vec<u8>,
    ) -> Result<bool, VMLogicError> {
        // If the yielded promise was created by a previous transaction, we'll find it in the trie
        if has_promise_yield_receipt(self.trie_update, self.account_id.clone(), data_id)
            .map_err(wrap_storage_error)?
        {
            self.receipt_manager.create_promise_resume_receipt(data_id, data)?;
            return Ok(true);
        }

        // If the yielded promise was created by the current transaction, we'll find it in the
        // receipt manager.
        self.receipt_manager.checked_resolve_promise_yield(data_id, data)
    }

    fn append_action_create_account(
        &mut self,
        receipt_index: ReceiptIndex,
    ) -> Result<(), VMLogicError> {
        self.receipt_manager.append_action_create_account(receipt_index)
    }

    fn append_action_deploy_contract(
        &mut self,
        receipt_index: ReceiptIndex,
        code: Vec<u8>,
    ) -> Result<(), VMLogicError> {
        self.receipt_manager.append_action_deploy_contract(receipt_index, code)
    }

    fn append_action_function_call_weight(
        &mut self,
        receipt_index: ReceiptIndex,
        method_name: Vec<u8>,
        args: Vec<u8>,
        attached_deposit: Balance,
        prepaid_gas: Gas,
        gas_weight: near_primitives::types::GasWeight,
    ) -> Result<(), VMLogicError> {
        self.receipt_manager.append_action_function_call_weight(
            receipt_index,
            method_name,
            args,
            attached_deposit,
            prepaid_gas,
            gas_weight,
        )
    }

    fn append_action_transfer(
        &mut self,
        receipt_index: ReceiptIndex,
        deposit: Balance,
    ) -> Result<(), VMLogicError> {
        self.receipt_manager.append_action_transfer(receipt_index, deposit)
    }

    fn append_action_stake(
        &mut self,
        receipt_index: ReceiptIndex,
        stake: Balance,
        public_key: near_crypto::PublicKey,
    ) {
        self.receipt_manager.append_action_stake(receipt_index, stake, public_key)
    }

    fn append_action_add_key_with_full_access(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: near_crypto::PublicKey,
        nonce: near_primitives::types::Nonce,
    ) {
        self.receipt_manager.append_action_add_key_with_full_access(
            receipt_index,
            public_key,
            nonce,
        )
    }

    fn append_action_add_key_with_function_call(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: near_crypto::PublicKey,
        nonce: near_primitives::types::Nonce,
        allowance: Option<Balance>,
        receiver_id: AccountId,
        method_names: Vec<Vec<u8>>,
    ) -> Result<(), VMLogicError> {
        self.receipt_manager.append_action_add_key_with_function_call(
            receipt_index,
            public_key,
            nonce,
            allowance,
            receiver_id,
            method_names,
        )
    }

    fn append_action_delete_key(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: near_crypto::PublicKey,
    ) {
        self.receipt_manager.append_action_delete_key(receipt_index, public_key)
    }

    fn append_action_delete_account(
        &mut self,
        receipt_index: ReceiptIndex,
        beneficiary_id: AccountId,
    ) -> Result<(), VMLogicError> {
        self.receipt_manager.append_action_delete_account(receipt_index, beneficiary_id)
    }

    fn get_receipt_receiver(&self, receipt_index: ReceiptIndex) -> &AccountId {
        self.receipt_manager.get_receipt_receiver(receipt_index)
    }
}

pub(crate) struct RuntimeContractExt<'a> {
    pub(crate) storage: ContractStorage,
    pub(crate) account_id: &'a AccountId,
    pub(crate) code_hash: CryptoHash,
}

impl<'a> Contract for RuntimeContractExt<'a> {
    fn hash(&self) -> CryptoHash {
        // For eth implicit accounts return the wallet contract code hash.
        // The account.code_hash() contains hash of the magic bytes, not the contract hash.
        if self.account_id.get_account_type() == AccountType::EthImplicitAccount {
            // There are old eth implicit accounts without magic bytes in the code hash.
            // Result can be None and it's a valid option. See https://github.com/near/nearcore/pull/11606
            if let Some(wallet_contract) = wallet_contract(self.code_hash) {
                return *wallet_contract.hash();
            }
        }

        self.code_hash
    }

    fn get_code(&self) -> Option<Arc<ContractCode>> {
        let account_id = self.account_id;
        if account_id.get_account_type() == AccountType::EthImplicitAccount {
            // Accounts that look like eth implicit accounts and have existed prior to the
            // eth-implicit accounts protocol change (these accounts are discussed in the
            // description of #11606) may have something else deployed to them. Only return
            // something here if the accounts have a wallet contract hash. Otherwise use the
            // regular path to grab the deployed contract.
            if let Some(wc) = wallet_contract(self.code_hash) {
                return Some(wc);
            }
        }
        self.storage.get(self.code_hash).map(Arc::new)
    }
}

/// Deterministic cache to store trie nodes that have been accessed so far
/// during the cache's lifetime. It is used for deterministic gas accounting
/// so that previously accessed trie nodes and values are charged at a
/// cheaper gas cost.
///
/// This cache's correctness is critical as it contributes to the gas accounting of storage
/// operations during contract execution. For that reason, a new `AccountingState` must be
/// created at the beginning of a chunk's execution, and the db_read_nodes and mem_read_nodes must
/// be taken into account whenever a contract storage operation is performed to calculate what kind
/// of operation it was.
///
/// The latter is easy as the only way a contract storage operation can happen is through the
/// implementation of `Externals`.
///
/// Note that we don't have a size limit for values in the accounting cache.
/// There are two reasons:
///   - for nodes, value size is an implementation detail. If we change
///     internal representation of a node (e.g. change `memory_usage` field
///     from `RawTrieNodeWithSize`), this would have to be a protocol upgrade.
///   - total size of all values is limited by the runtime fees. More
///     thoroughly:
///       - number of nodes is limited by receipt gas limit / touching trie
///         node fee ~= 500 Tgas / 16 Ggas = 31_250;
///       - size of trie keys and values is limited by receipt gas limit /
///         lowest per byte fee (`storage_read_value_byte`) ~=
///         (500 * 10**12 / 5611005) / 2**20 ~= 85 MB.
/// All values are given as of 16/03/2022. We may consider more precise limit
/// for the accounting cache as well.
///
/// Note that in general, it is NOT true that all storage access is either a db read or mem read.
/// It can also be a flat storage read, which is not tracked via `AccountingAccessTracker`, except
/// for value dereferences that ultimately go out to trie anyway.
// FIXME(nagisa): equalize fees for different types of accesses and eventually remove this code.
struct AccountingAccessTracker {
    allow_insert: bool,
    state: Arc<AccountingState>,
}

// FIXME(nagisa): (un-pub) this!
#[derive(Default, Debug)]
pub struct AccountingState {
    mem_reads: AtomicU64,
    db_reads: AtomicU64,
    cache: Mutex<BTreeMap<CryptoHash, Arc<[u8]>>>,
}

impl AccountingState {
    fn get_counts(&self) -> TrieNodesCount {
        TrieNodesCount {
            db_reads: self.db_reads.load(Ordering::Relaxed),
            mem_reads: self.mem_reads.load(Ordering::Relaxed),
        }
    }

    fn commit_counts_since(
        &self,
        snapshot: TrieNodesCount,
        into: &mut dyn StorageAccessTracker,
    ) -> Result<TrieNodesCount, VMLogicError> {
        let db_read_delta = self
            .db_reads
            .load(Ordering::Relaxed)
            .checked_sub(snapshot.db_reads)
            .ok_or(InconsistentStateError::IntegerOverflow)?;
        let mem_read_delta = self
            .mem_reads
            .load(Ordering::Relaxed)
            .checked_sub(snapshot.mem_reads)
            .ok_or(InconsistentStateError::IntegerOverflow)?;
        into.trie_node_touched(db_read_delta)?;
        into.cached_trie_node_access(mem_read_delta)?;
        Ok(TrieNodesCount { db_reads: db_read_delta, mem_reads: mem_read_delta })
    }
}

pub struct TrieNodesCount {
    /// Potentially expensive trie node reads which are served from disk in the worst case.
    pub db_reads: u64,
    /// Cheap trie node reads which are guaranteed to be served from RAM.
    pub mem_reads: u64,
}

impl Debug for AccountingAccessTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountingAccessTracker")
            .field("allow_insert", &self.allow_insert)
            .field("db_reads", &self.state.db_reads)
            .field("mem_reads", &self.state.mem_reads)
            .field("cache.len", &self.state.cache.lock().len())
            .finish()
    }
}

impl AccessTracker for AccountingAccessTracker {
    fn track_mem_lookup(&self, key: &CryptoHash) -> Option<Arc<[u8]>> {
        let value = Arc::clone(self.state.cache.lock().get(key)?);
        self.state.mem_reads.fetch_add(1, Ordering::Relaxed);
        Some(value)
    }

    fn track_disk_lookup(&self, key: CryptoHash, value: Arc<[u8]>) {
        self.state.db_reads.fetch_add(1, Ordering::Relaxed);
        if self.allow_insert {
            self.state.cache.lock().insert(key, value);
        }
    }
}

#[cfg(feature = "io_trace")]
fn base64(s: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(s)
}
