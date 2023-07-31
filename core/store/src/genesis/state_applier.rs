use crate::flat::FlatStateChanges;
use crate::{
    get_account, get_received_data, set, set_access_key, set_account, set_code,
    set_delayed_receipt, set_postponed_receipt, set_received_data, ShardTries, TrieUpdate,
};
use borsh::BorshSerialize;
use near_chain_configs::Genesis;
use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::contract::ContractCode;
use near_primitives::receipt::{DelayedReceiptIndices, Receipt, ReceiptEnum, ReceivedData};
use near_primitives::runtime::fees::StorageUsageConfig;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state_record::{state_record_to_account_id, StateRecord};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, Balance, ShardId, StateChangeCause, StateRoot};
use std::collections::{HashMap, HashSet};
use std::sync::atomic;

/// Computes the expected storage per account for a given stream of StateRecord(s).
/// For example: the storage for Contract depends on its length, we don't charge storage for receipts
/// and we compute a fixed (config-configured) number of bytes for each account (to store account id).
struct StorageComputer<'a> {
    /// Map from account id to number of storage bytes used.
    result: HashMap<AccountId, u64>,
    /// Configuration that keeps information like 'how many bytes should accountId consume' etc.
    config: &'a StorageUsageConfig,
}

impl<'a> StorageComputer<'a> {
    fn new(config: &'a StorageUsageConfig) -> Self {
        Self { result: HashMap::new(), config: &config }
    }

    /// Updates user's storage info based on the StateRecord.
    fn process_record(&mut self, record: &StateRecord) {
        // Note: It's okay to use unsafe math here, because this method should only be called on the trusted
        // state records (e.g. at launch from genesis)
        let account_and_storage = match record {
            StateRecord::Account { account_id, .. } => {
                Some((account_id.clone(), self.config.num_bytes_account))
            }
            StateRecord::Data { account_id, data_key, value } => {
                let storage_usage =
                    self.config.num_extra_bytes_record + data_key.len() as u64 + value.len() as u64;
                Some((account_id.clone(), storage_usage))
            }
            StateRecord::Contract { account_id, code } => {
                Some((account_id.clone(), code.len() as u64))
            }
            StateRecord::AccessKey { account_id, public_key, access_key } => {
                let public_key: PublicKey = public_key.clone();
                let access_key: AccessKey = access_key.clone();
                let storage_usage = self.config.num_extra_bytes_record
                    + public_key.try_to_vec().unwrap().len() as u64
                    + access_key.try_to_vec().unwrap().len() as u64;
                Some((account_id.clone(), storage_usage))
            }
            StateRecord::PostponedReceipt(_) => None,
            StateRecord::ReceivedData { .. } => None,
            StateRecord::DelayedReceipt(_) => None,
        };
        if let Some((account_id, storage_usage)) = account_and_storage {
            *self.result.entry(account_id).or_default() += storage_usage;
        }
    }

    /// Adds multiple StateRecords to the users' storage info.
    fn process_records(&mut self, records: &[StateRecord]) {
        for record in records {
            self.process_record(record);
        }
    }

    /// Returns the current storage use for each user.
    fn finalize(self) -> HashMap<AccountId, u64> {
        self.result
    }
}

/// The number of buffered storage modifications stored in memory.
///
/// Note that the actual number of changes stored in memory may be higher, and this is a very rough
/// limit on the memory usage. For instance – 100k writes of 4MiB (maximum size) contracts is still
/// going to be half a terabyte worth of memory, but in practice this does not seem to be a problem
/// and the memory usage stays fairly consistent throughout the process.
const TARGET_OUTSTANDING_WRITES: usize = 100_000;

struct AutoFlushingTrieUpdate<'a> {
    tries: &'a ShardTries,
    shard_uid: ShardUId,
    state_root: StateRoot,
    state_update: Option<TrieUpdate>,
    changes: usize,
    active_writers: &'a atomic::AtomicUsize,
}

impl<'a> AutoFlushingTrieUpdate<'a> {
    fn new(
        active_writers: &'a atomic::AtomicUsize,
        state_root: StateRoot,
        tries: &'a ShardTries,
        uid: ShardUId,
    ) -> Self {
        active_writers.fetch_add(1, atomic::Ordering::Relaxed);
        Self {
            active_writers,
            state_root,
            tries,
            changes: 0,
            state_update: Some(tries.new_trie_update(uid, state_root)),
            shard_uid: uid,
        }
    }

    fn modify<R>(&mut self, process_callback: impl FnOnce(&mut TrieUpdate) -> R) -> R {
        let Self { ref mut changes, ref mut state_update, .. } = self;
        // See if we should consider flushing.
        let result = process_callback(state_update.as_mut().expect("state update should be set"));
        let writers = self.active_writers.load(atomic::Ordering::Relaxed);
        let target_updates = TARGET_OUTSTANDING_WRITES / writers;
        *changes += 1;
        if *changes >= target_updates {
            self.flush();
        }
        result
    }

    fn flush(&mut self) -> StateRoot {
        let Self { ref mut changes, ref mut state_root, ref mut state_update, .. } = self;
        tracing::info!(
            target: "runtime",
            shard_uid=?self.shard_uid,
            %state_root,
            %changes,
            "flushing changes"
        );
        let mut old_state_update = state_update.take().expect("state update should be set");
        old_state_update.commit(StateChangeCause::InitialState);
        let (_, trie_changes, state_changes) =
            old_state_update.finalize().expect("Genesis state update failed");
        let mut store_update = self.tries.store_update();
        *state_root = self.tries.apply_all(&trie_changes, self.shard_uid, &mut store_update);
        FlatStateChanges::from_state_changes(&state_changes)
            .apply_to_flat_state(&mut store_update, self.shard_uid);
        store_update.commit().expect("Store update failed on genesis initialization");
        *state_update = Some(self.tries.new_trie_update(self.shard_uid, *state_root));
        *changes = 0;
        *state_root
    }
}

impl<'a> Drop for AutoFlushingTrieUpdate<'a> {
    fn drop(&mut self) {
        self.flush();
        self.active_writers.fetch_sub(1, atomic::Ordering::Relaxed);
    }
}

pub struct GenesisStateApplier {}

impl GenesisStateApplier {
    fn apply_batch(
        storage: &mut AutoFlushingTrieUpdate,
        delayed_receipts_indices: &mut DelayedReceiptIndices,
        shard_uid: ShardUId,
        validators: &[(AccountId, PublicKey, Balance)],
        config: &StorageUsageConfig,
        genesis: &Genesis,
        account_ids: HashSet<AccountId>,
    ) {
        let mut postponed_receipts: Vec<Receipt> = vec![];
        let mut storage_computer = StorageComputer::new(config);
        tracing::info!(
            target: "runtime",
            ?shard_uid,
            "processing records…"
        );
        genesis.for_each_record(|record: &StateRecord| {
            if !account_ids.contains(state_record_to_account_id(record)) {
                return;
            }
            storage_computer.process_record(record);
            match record {
                StateRecord::Account { account_id, account } => storage.modify(|state_update| {
                    set_account(state_update, account_id.clone(), account);
                }),
                StateRecord::Data { account_id, data_key, value } => {
                    storage.modify(|state_update| {
                        state_update.set(
                            TrieKey::ContractData {
                                key: data_key.clone().into(),
                                account_id: account_id.clone(),
                            },
                            value.clone().into(),
                        );
                    })
                }
                StateRecord::Contract { account_id, code } => {
                    storage.modify(|state_update| {
                        // Recompute contract code hash.
                        let code = ContractCode::new(code.clone(), None);
                        if let Some(acc) =
                            get_account(state_update, account_id).expect("Failed to read state")
                        {
                            set_code(state_update, account_id.clone(), &code);
                            assert_eq!(*code.hash(), acc.code_hash());
                        } else {
                            tracing::error!(
                                target: "runtime",
                                %account_id,
                                code_hash = %code.hash(),
                                message = "code for non-existent account",
                            );
                        }
                    })
                }
                StateRecord::AccessKey { account_id, public_key, access_key } => {
                    storage.modify(|state_update| {
                        set_access_key(
                            state_update,
                            account_id.clone(),
                            public_key.clone(),
                            access_key,
                        );
                    })
                }
                StateRecord::PostponedReceipt(receipt) => {
                    // Delaying processing postponed receipts, until we process all data first
                    postponed_receipts.push(*receipt.clone());
                }
                StateRecord::ReceivedData { account_id, data_id, data } => {
                    storage.modify(|state_update| {
                        set_received_data(
                            state_update,
                            account_id.clone(),
                            *data_id,
                            // FIXME: this clone is not necessary!
                            &ReceivedData { data: data.clone() },
                        );
                    })
                }
                StateRecord::DelayedReceipt(receipt) => storage.modify(|state_update| {
                    set_delayed_receipt(state_update, delayed_receipts_indices, &*receipt);
                }),
            }
        });

        tracing::info!(
            target: "runtime",
            ?shard_uid,
            "processing account storage…"
        );
        for (account_id, storage_usage) in storage_computer.finalize() {
            storage.modify(|state_update| {
                let mut account = get_account(state_update, &account_id)
                    .expect("Genesis storage error")
                    .expect("Account must exist");
                account.set_storage_usage(storage_usage);
                set_account(state_update, account_id, &account);
            });
        }

        // Processing postponed receipts after we stored all received data
        tracing::info!(
            target: "runtime",
            ?shard_uid,
            "processing postponed receipts…"
        );
        for receipt in postponed_receipts {
            let account_id = &receipt.receiver_id;
            let action_receipt = match &receipt.receipt {
                ReceiptEnum::Action(a) => a,
                _ => panic!("Expected action receipt"),
            };
            // Logic similar to `apply_receipt`
            let mut pending_data_count: u32 = 0;
            for data_id in &action_receipt.input_data_ids {
                storage.modify(|state_update| {
                    if get_received_data(state_update, account_id, *data_id)
                        .expect("Genesis storage error")
                        .is_none()
                    {
                        pending_data_count += 1;
                        set(
                            state_update,
                            TrieKey::PostponedReceiptId {
                                receiver_id: account_id.clone(),
                                data_id: *data_id,
                            },
                            &receipt.receipt_id,
                        )
                    }
                });
            }
            if pending_data_count == 0 {
                panic!("Postponed receipt should have pending data")
            } else {
                storage.modify(|state_update| {
                    set(
                        state_update,
                        TrieKey::PendingDataCount {
                            receiver_id: account_id.clone(),
                            receipt_id: receipt.receipt_id,
                        },
                        &pending_data_count,
                    );
                    set_postponed_receipt(state_update, &receipt);
                });
            }
        }

        for (account_id, _, amount) in validators {
            if !account_ids.contains(account_id) {
                continue;
            }
            storage.modify(|state_update| {
                let mut account: Account = get_account(state_update, account_id)
                    .expect("Genesis storage error")
                    .expect("account must exist");
                account.set_locked(*amount);
                set_account(state_update, account_id.clone(), &account);
            });
        }
    }

    fn apply_delayed_receipts(
        storage: &mut AutoFlushingTrieUpdate,
        delayed_receipts_indices: DelayedReceiptIndices,
    ) {
        if delayed_receipts_indices != DelayedReceiptIndices::default() {
            storage.modify(|state_update| {
                set(state_update, TrieKey::DelayedReceiptIndices, &delayed_receipts_indices);
            });
        }
    }

    pub fn apply(
        op_limit: &atomic::AtomicUsize,
        tries: ShardTries,
        shard_id: ShardId,
        validators: &[(AccountId, PublicKey, Balance)],
        config: &StorageUsageConfig,
        genesis: &Genesis,
        shard_account_ids: HashSet<AccountId>,
    ) -> StateRoot {
        let mut delayed_receipts_indices = DelayedReceiptIndices::default();
        let shard_uid =
            ShardUId { version: genesis.config.shard_layout.version(), shard_id: shard_id as u32 };
        let mut storage =
            AutoFlushingTrieUpdate::new(op_limit, StateRoot::default(), &tries, shard_uid);
        Self::apply_batch(
            &mut storage,
            &mut delayed_receipts_indices,
            shard_uid,
            validators,
            config,
            genesis,
            shard_account_ids,
        );
        Self::apply_delayed_receipts(&mut storage, delayed_receipts_indices);
        // At this point we have written all we wanted, but there may be outstanding writes left.
        // We flush those writes and return the new state root to the caller.
        storage.flush()
    }
}

/// Computes the expected storage per account for a given set of StateRecord(s).
pub fn compute_storage_usage(
    records: &[StateRecord],
    config: &StorageUsageConfig,
) -> HashMap<AccountId, u64> {
    let mut storage_computer = StorageComputer::new(config);
    storage_computer.process_records(records);
    storage_computer.finalize()
}

/// Compute the expected storage per account for genesis records.
pub fn compute_genesis_storage_usage(
    genesis: &Genesis,
    config: &StorageUsageConfig,
) -> HashMap<AccountId, u64> {
    let mut storage_computer = StorageComputer::new(config);
    genesis.for_each_record(|record| {
        storage_computer.process_record(record);
    });
    storage_computer.finalize()
}
