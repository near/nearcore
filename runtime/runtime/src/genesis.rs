use std::collections::{HashMap, HashSet};

use borsh::BorshSerialize;

use near_chain_configs::Genesis;
use near_crypto::PublicKey;
use near_primitives::runtime::fees::StorageUsageConfig;
use near_primitives::shard_layout::ShardUId;
use near_primitives::{
    account::{AccessKey, Account},
    contract::ContractCode,
    receipt::{DelayedReceiptIndices, Receipt, ReceiptEnum, ReceivedData},
    state_record::{state_record_to_account_id, StateRecord},
    trie_key::TrieKey,
    types::{AccountId, Balance, MerkleHash, ShardId, StateChangeCause, StateRoot},
};
use near_store::{
    get_account, get_received_data, set, set_access_key, set_account, set_code,
    set_postponed_receipt, set_received_data, ShardTries, TrieUpdate,
};

use crate::config::RuntimeConfig;
use crate::Runtime;
/// Computes the expected storage per account for a given stream of StateRecord(s).
/// For example: the storage for Contract depends on its length, we don't charge storage for receipts
/// and we compute a fixed (config-configured) number of bytes for each account (to store account id).
pub struct StorageComputer<'a> {
    /// Map from account id to number of storage bytes used.
    result: HashMap<AccountId, u64>,
    /// Configuration that keeps information like 'how many bytes should accountId consume' etc.
    config: &'a StorageUsageConfig,
}

impl<'a> StorageComputer<'a> {
    pub fn new(config: &'a RuntimeConfig) -> Self {
        Self { result: HashMap::new(), config: &config.transaction_costs.storage_usage_config }
    }

    /// Updates user's storage info based on the StateRecord.
    pub fn process_record(&mut self, record: &StateRecord) {
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
    pub fn process_records(&mut self, records: &[StateRecord]) {
        for record in records {
            self.process_record(record);
        }
    }

    /// Returns the current storage use for each user.
    pub fn finalize(self) -> HashMap<AccountId, u64> {
        self.result
    }
}

pub struct GenesisStateApplier {}

impl GenesisStateApplier {
    fn commit(
        mut state_update: TrieUpdate,
        current_state_root: &mut StateRoot,
        tries: &mut ShardTries,
        shard_uid: ShardUId,
    ) {
        state_update.commit(StateChangeCause::InitialState);
        let (trie_changes, mut state_changes) =
            state_update.finalize().expect("Genesis state update failed");

        let (mut store_update, new_state_root) = tries.apply_all(&trie_changes, shard_uid);
        for change in state_changes.drain(..) {
            store_update.apply_change_to_flat_state(&change);
        }
        store_update.commit().expect("Store update failed on genesis initialization");
        *current_state_root = new_state_root;
    }

    fn apply_batch(
        current_state_root: &mut StateRoot,
        delayed_receipts_indices: &mut DelayedReceiptIndices,
        tries: &mut ShardTries,
        shard_uid: ShardUId,
        validators: &[(AccountId, PublicKey, Balance)],
        config: &RuntimeConfig,
        genesis: &Genesis,
        batch_account_ids: HashSet<&AccountId>,
    ) {
        let mut state_update = tries.new_trie_update(shard_uid, *current_state_root);
        let mut postponed_receipts: Vec<Receipt> = vec![];

        let mut storage_computer = StorageComputer::new(config);

        genesis.for_each_record(|record: &StateRecord| {
            if !batch_account_ids.contains(state_record_to_account_id(record)) {
                return;
            }

            storage_computer.process_record(record);

            match record.clone() {
                StateRecord::Account { account_id, account } => {
                    set_account(&mut state_update, account_id, &account);
                }
                StateRecord::Data { account_id, data_key, value } => {
                    state_update.set(TrieKey::ContractData { key: data_key, account_id }, value);
                }
                StateRecord::Contract { account_id, code } => {
                    let acc = get_account(&state_update, &account_id).expect("Failed to read state").expect("Code state record should be preceded by the corresponding account record");
                    // Recompute contract code hash.
                    let code = ContractCode::new(code, None);
                    set_code(&mut state_update, account_id, &code);
                    assert_eq!(*code.hash(), acc.code_hash());
                }
                StateRecord::AccessKey { account_id, public_key, access_key } => {
                    set_access_key(&mut state_update, account_id, public_key, &access_key);
                }
                StateRecord::PostponedReceipt(receipt) => {
                    // Delaying processing postponed receipts, until we process all data first
                    postponed_receipts.push(*receipt);
                }
                StateRecord::ReceivedData { account_id, data_id, data } => {
                    set_received_data(
                        &mut state_update,
                        account_id,
                        data_id,
                        &ReceivedData { data },
                    );
                }
                StateRecord::DelayedReceipt(receipt) => {
                    Runtime::delay_receipt(
                        &mut state_update,
                        delayed_receipts_indices,
                        &*receipt,
                    )
                        .unwrap();
                }
            }
        });

        for (account_id, storage_usage) in storage_computer.finalize() {
            let mut account = get_account(&state_update, &account_id)
                .expect("Genesis storage error")
                .expect("Account must exist");
            account.set_storage_usage(storage_usage);
            set_account(&mut state_update, account_id, &account);
        }

        // Processing postponed receipts after we stored all received data
        for receipt in postponed_receipts {
            let account_id = &receipt.receiver_id;
            let action_receipt = match &receipt.receipt {
                ReceiptEnum::Action(a) => a,
                _ => panic!("Expected action receipt"),
            };
            // Logic similar to `apply_receipt`
            let mut pending_data_count: u32 = 0;
            for data_id in &action_receipt.input_data_ids {
                if get_received_data(&state_update, account_id, *data_id)
                    .expect("Genesis storage error")
                    .is_none()
                {
                    pending_data_count += 1;
                    set(
                        &mut state_update,
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
                    &mut state_update,
                    TrieKey::PendingDataCount {
                        receiver_id: account_id.clone(),
                        receipt_id: receipt.receipt_id,
                    },
                    &pending_data_count,
                );
                set_postponed_receipt(&mut state_update, &receipt);
            }
        }

        for (account_id, _, amount) in validators {
            if !batch_account_ids.contains(account_id) {
                continue;
            }
            let mut account: Account = get_account(&state_update, account_id)
                .expect("Genesis storage error")
                .expect("account must exist");
            account.set_locked(*amount);
            set_account(&mut state_update, account_id.clone(), &account);
        }

        Self::commit(state_update, current_state_root, tries, shard_uid);
    }

    fn apply_delayed_receipts(
        delayed_receipts_indices: DelayedReceiptIndices,
        current_state_root: &mut StateRoot,
        tries: &mut ShardTries,
        shard_uid: ShardUId,
    ) {
        let mut state_update = tries.new_trie_update(shard_uid, *current_state_root);

        if delayed_receipts_indices != DelayedReceiptIndices::default() {
            set(&mut state_update, TrieKey::DelayedReceiptIndices, &delayed_receipts_indices);
            Self::commit(state_update, current_state_root, tries, shard_uid);
        }
    }

    pub fn apply(
        mut tries: ShardTries,
        shard_id: ShardId,
        validators: &[(AccountId, PublicKey, Balance)],
        config: &RuntimeConfig,
        genesis: &Genesis,
        shard_account_ids: HashSet<AccountId>,
    ) -> StateRoot {
        let mut current_state_root = MerkleHash::default();
        let mut delayed_receipts_indices = DelayedReceiptIndices::default();
        let shard_uid =
            ShardUId { version: genesis.config.shard_layout.version(), shard_id: shard_id as u32 };
        for batch_account_ids in
            shard_account_ids.into_iter().collect::<Vec<AccountId>>().chunks(300_000)
        {
            Self::apply_batch(
                &mut current_state_root,
                &mut delayed_receipts_indices,
                &mut tries,
                shard_uid,
                validators,
                config,
                genesis,
                HashSet::from_iter(batch_account_ids),
            );
        }
        Self::apply_delayed_receipts(
            delayed_receipts_indices,
            &mut current_state_root,
            &mut tries,
            shard_uid,
        );
        current_state_root
    }
}
