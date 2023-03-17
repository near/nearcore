use crate::config::RuntimeConfig;
use crate::Runtime;
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
use near_store::flat::FlatStateChanges;
use near_store::{
    get_account, get_received_data, set, set_access_key, set_account, set_code,
    set_postponed_receipt, set_received_data, ShardTries, TrieUpdate,
};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{atomic, Mutex};

/// Computes the expected storage per account for a given stream of StateRecord(s).
/// For example: the storage for Contract depends on its length, we don't charge storage for receipts
/// and we compute a fixed (config-configured) number of bytes for each account (to store account id).
pub(crate) struct StorageComputer<'a> {
    /// Map from account id to number of storage bytes used.
    result: HashMap<AccountId, u64>,
    /// Configuration that keeps information like 'how many bytes should accountId consume' etc.
    config: &'a StorageUsageConfig,
}

impl<'a> StorageComputer<'a> {
    pub(crate) fn new(config: &'a RuntimeConfig) -> Self {
        Self { result: HashMap::new(), config: &config.fees.storage_usage_config }
    }

    /// Updates user's storage info based on the StateRecord.
    pub(crate) fn process_record(&mut self, record: &StateRecord) {
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
    pub(crate) fn process_records(&mut self, records: &[StateRecord]) {
        for record in records {
            self.process_record(record);
        }
    }

    /// Returns the current storage use for each user.
    pub(crate) fn finalize(self) -> HashMap<AccountId, u64> {
        self.result
    }
}

type State = (usize, StateRoot, TrieUpdate);

pub(crate) struct AutoFlushingTrieUpdate<'a> {
    remaining_ops: &'a atomic::AtomicUsize,
    tries: &'a ShardTries,
    shard_uid: ShardUId,
    state: Mutex<State>,
}

impl<'a> AutoFlushingTrieUpdate<'a> {
    pub(crate) fn new(
        remaining_ops: &'a atomic::AtomicUsize,
        state_root: StateRoot,
        tries: &'a ShardTries,
        uid: ShardUId,
    ) -> Self {
        let state = Mutex::new((0, state_root, tries.new_trie_update(uid, state_root)));
        Self { remaining_ops, state, tries, shard_uid: uid }
    }

    fn lock(&self) -> std::sync::MutexGuard<State> {
        self.state.lock().unwrap_or_else(|g| g.into_inner())
    }

    fn modify<R>(&self, cb: impl FnOnce(&mut StateRoot, &mut TrieUpdate) -> R) -> R {
        let mut guard = self.lock();
        let (ref mut update_count, ref mut state_root, ref mut state_update) = &mut *guard;
        let result = cb(state_root, state_update);
        *update_count += 1;
        let old = self.remaining_ops.fetch_update(
            atomic::Ordering::Relaxed,
            atomic::Ordering::Relaxed,
            |old| {
                if old == 0 {
                    Some(*update_count)
                } else {
                    Some(old - 1)
                }
            },
        );
        if old == Ok(0) {
            tracing::info!(
                target: "runtime",
                messages="write-count triggered flush",
                update_count
            );
            *update_count = 0;
            self.flush(guard);
        }
        result
    }

    fn flush(&self, mut guard: std::sync::MutexGuard<State>) {
        let (_, ref mut state_root, ref mut state_update) = &mut *guard;
        state_update.commit(StateChangeCause::InitialState);
        // Temporarily store a trie update with the "wrong" state root. This will be replaced
        // shortly as soon as we know the new state root.
        let old_state_update = std::mem::replace(
            state_update,
            self.tries.new_trie_update(self.shard_uid, *state_root),
        );
        let (_, trie_changes, state_changes) =
            old_state_update.finalize().expect("Genesis state update failed");
        let mut store_update = self.tries.store_update();
        *state_root = self.tries.apply_all(&trie_changes, self.shard_uid, &mut store_update);
        if cfg!(feature = "protocol_feature_flat_state") {
            FlatStateChanges::from_state_changes(&state_changes)
                .apply_to_flat_state(&mut store_update, self.shard_uid);
        }
        drop(state_changes); // silence compiler when not protocol_feature_flat_state
        store_update.commit().expect("Store update failed on genesis initialization");
        tracing::info!(
            target: "runtime",
            shard_uid=?self.shard_uid,
            %state_root,
            "creating genesis, flushed state"
        );
        *state_update = self.tries.new_trie_update(self.shard_uid, *state_root);
    }
}

impl<'a> Drop for AutoFlushingTrieUpdate<'a> {
    fn drop(&mut self) {
        self.flush(self.lock());
    }
}

pub struct GenesisStateApplier {}

impl GenesisStateApplier {
    fn apply_batch(
        storage: &AutoFlushingTrieUpdate,
        delayed_receipts_indices: &mut DelayedReceiptIndices,
        shard_uid: ShardUId,
        validators: &[(AccountId, PublicKey, Balance)],
        config: &RuntimeConfig,
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
                StateRecord::Account { account_id, account } => {
                    storage.modify(|_, state_update| {
                        set_account(state_update, account_id.clone(), account);
                    })
                }
                StateRecord::Data { account_id, data_key, value } => {
                    storage.modify(|_, state_update| {
                        state_update.set(
                            TrieKey::ContractData {
                                key: data_key.clone(),
                                account_id: account_id.clone(),
                            },
                            value.clone(),
                        );
                    })
                }
                StateRecord::Contract { account_id, code } => {
                    storage.modify(|_, state_update| {
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
                    storage.modify(|_, state_update| {
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
                    storage.modify(|_, state_update| {
                        set_received_data(
                            state_update,
                            account_id.clone(),
                            *data_id,
                            // FIXME: this clone is not necessary!
                            &ReceivedData { data: data.clone() },
                        );
                    })
                }
                StateRecord::DelayedReceipt(receipt) => storage.modify(|_, state_update| {
                    Runtime::delay_receipt(state_update, delayed_receipts_indices, &*receipt)
                        .unwrap();
                }),
            }
        });

        tracing::info!(
            target: "runtime",
            ?shard_uid,
            "processing account storage…"
        );
        for (account_id, storage_usage) in storage_computer.finalize() {
            storage.modify(|_, state_update| {
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

        postponed_receipts.into_par_iter().for_each(|receipt| {
            storage.modify(|_, state_update| {
                let account_id = &receipt.receiver_id;
                let action_receipt = match &receipt.receipt {
                    ReceiptEnum::Action(a) => a,
                    _ => panic!("Expected action receipt"),
                };
                // Logic similar to `apply_receipt`
                let mut pending_data_count: u32 = 0;
                for data_id in &action_receipt.input_data_ids {
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
                }
                if pending_data_count == 0 {
                    panic!("Postponed receipt should have pending data")
                } else {
                    set(
                        state_update,
                        TrieKey::PendingDataCount {
                            receiver_id: account_id.clone(),
                            receipt_id: receipt.receipt_id,
                        },
                        &pending_data_count,
                    );
                    set_postponed_receipt(state_update, &receipt);
                }
            });
        });

        for (account_id, _, amount) in validators {
            if !account_ids.contains(account_id) {
                continue;
            }
            storage.modify(|_, state_update| {
                let mut account: Account = get_account(state_update, account_id)
                    .expect("Genesis storage error")
                    .expect("account must exist");
                account.set_locked(*amount);
                set_account(state_update, account_id.clone(), &account);
            });
        }
    }

    fn apply_delayed_receipts(
        storage: &AutoFlushingTrieUpdate,
        delayed_receipts_indices: DelayedReceiptIndices,
    ) {
        if delayed_receipts_indices != DelayedReceiptIndices::default() {
            storage.modify(|_, state_update| {
                set(state_update, TrieKey::DelayedReceiptIndices, &delayed_receipts_indices);
            });
        }
    }

    pub fn apply(
        op_limit: &atomic::AtomicUsize,
        tries: ShardTries,
        shard_id: ShardId,
        validators: &[(AccountId, PublicKey, Balance)],
        config: &RuntimeConfig,
        genesis: &Genesis,
        shard_account_ids: HashSet<AccountId>,
    ) -> StateRoot {
        let mut delayed_receipts_indices = DelayedReceiptIndices::default();
        let shard_uid =
            ShardUId { version: genesis.config.shard_layout.version(), shard_id: shard_id as u32 };
        let storage =
            AutoFlushingTrieUpdate::new(op_limit, StateRoot::default(), &tries, shard_uid);
        Self::apply_batch(
            &storage,
            &mut delayed_receipts_indices,
            shard_uid,
            validators,
            config,
            genesis,
            shard_account_ids,
        );
        Self::apply_delayed_receipts(&storage, delayed_receipts_indices);
        storage.flush(storage.lock());
        storage.modify(|root, _| *root)
    }
}
