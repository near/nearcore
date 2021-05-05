use std::collections::{HashMap, HashSet};

use borsh::BorshSerialize;

use near_chain_configs::Genesis;
use near_crypto::PublicKey;
use near_primitives::runtime::fees::StorageUsageConfig;
use near_primitives::{
    account::{AccessKey, Account},
    contract::ContractCode,
    hash::CryptoHash,
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

use std::iter::FromIterator;

pub struct StorageComputer<'a> {
    result: HashMap<String, u64>,
    config: &'a StorageUsageConfig,
}

impl<'a> StorageComputer<'a> {
    pub fn new(config: &'a RuntimeConfig) -> Self {
        Self { result: HashMap::new(), config: &config.transaction_costs.storage_usage_config }
    }

    pub fn process_record(&mut self, record: &StateRecord) {
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
                let access_key: AccessKey = access_key.clone().into();
                let storage_usage = self.config.num_extra_bytes_record
                    + public_key.try_to_vec().unwrap().len() as u64
                    + access_key.try_to_vec().unwrap().len() as u64;
                Some((account_id.clone(), storage_usage))
            }
            StateRecord::PostponedReceipt(_) => None,
            StateRecord::ReceivedData { .. } => None,
            StateRecord::DelayedReceipt(_) => None,
        };
        if let Some((account, storage_usage)) = account_and_storage {
            *self.result.entry(account).or_default() += storage_usage;
        }
    }

    pub fn process_records(&mut self, records: &[StateRecord]) {
        for record in records {
            self.process_record(record);
        }
    }

    pub fn finalize(self) -> HashMap<String, u64> {
        self.result
    }
}

pub struct GenesisStateApplier<'a> {
    tries: ShardTries,
    shard_id: ShardId,
    validators: &'a [(AccountId, PublicKey, Balance)],
    config: &'a RuntimeConfig,
    current_state_root: CryptoHash,
    delayed_receipts_indices: DelayedReceiptIndices,
}

impl<'a> GenesisStateApplier<'a> {
    pub fn new(
        tries: ShardTries,
        shard_id: ShardId,
        validators: &'a [(AccountId, PublicKey, Balance)],
        config: &'a RuntimeConfig,
    ) -> Self {
        Self {
            tries,
            shard_id,
            validators,
            config,
            current_state_root: MerkleHash::default(),
            delayed_receipts_indices: DelayedReceiptIndices::default(),
        }
    }

    fn commit(&mut self, mut state_update: TrieUpdate) {
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize_genesis().expect("Genesis state update failed");

        let (store_update, new_state_root) = self
            .tries
            .apply_all(&trie_changes, self.shard_id)
            .expect("Failed to apply genesis chunk");
        store_update.commit().expect("Store update failed on genesis initialization");
        self.current_state_root = new_state_root;
    }

    fn apply_batch(&mut self, genesis: &Genesis, batch_account_ids: HashSet<&AccountId>) {
        let mut state_update = self.tries.new_trie_update(self.shard_id, self.current_state_root);
        let mut postponed_receipts: Vec<Receipt> = vec![];

        let mut storage_computer = StorageComputer::new(self.config);

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
                    assert_eq!(code.get_hash(), acc.code_hash());
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
                        &mut self.delayed_receipts_indices,
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

        for (account_id, _, amount) in self.validators {
            if !batch_account_ids.contains(account_id) {
                continue;
            }
            let mut account: Account = get_account(&state_update, account_id)
                .expect("Genesis storage error")
                .expect("account must exist");
            account.set_locked(*amount);
            set_account(&mut state_update, account_id.clone(), &account);
        }

        self.commit(state_update);
    }

    pub fn apply_delayed_receipts(&mut self) {
        let mut state_update = self.tries.new_trie_update(self.shard_id, self.current_state_root);

        if self.delayed_receipts_indices != DelayedReceiptIndices::default() {
            set(&mut state_update, TrieKey::DelayedReceiptIndices, &self.delayed_receipts_indices);
            self.commit(state_update);
        }
    }

    pub fn apply(&mut self, genesis: &Genesis, shard_account_ids: HashSet<AccountId>) -> StateRoot {
        for batch_account_ids in
            shard_account_ids.into_iter().collect::<Vec<AccountId>>().chunks(300_000)
        {
            self.apply_batch(genesis, HashSet::from_iter(batch_account_ids));
        }
        self.apply_delayed_receipts();
        self.current_state_root
    }
}
