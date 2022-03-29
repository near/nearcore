use crate::NightshadeRuntime;
use borsh::BorshSerialize;
use near_chain::RuntimeAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::state_record::{state_record_to_account_id, StateRecord};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId, ShardId, StateRoot};
use near_store::TrieIterator;
use std::cell::Cell;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::sync::Mutex;
use tracing::debug;

pub struct SnapshotGrabber {
    epoch_id: Mutex<Cell<Option<CryptoHash>>>,
    accounts_to_grab: HashSet<AccountId>,
}

impl SnapshotGrabber {
    pub fn new(accounts: &[AccountId]) -> Self {
        Self {
            epoch_id: Mutex::new(Cell::new(None)),
            accounts_to_grab: accounts.iter().cloned().collect(),
        }
    }

    pub fn on_epoch_start(
        &self,
        epoch_id: &EpochId,
        runtime: &NightshadeRuntime,
        state_root: &StateRoot,
        prev_block_hash: &CryptoHash,
        shard_id: ShardId,
    ) {
        let lock = self.epoch_id.lock().unwrap();
        if lock.get().is_none() {
            debug!("Snapshot grabbing started");
            File::create("transactions").unwrap();
            lock.set(Some(epoch_id.0));
        }
        if lock.get().unwrap() == epoch_id.0 {
            self.grab_state(runtime, state_root, prev_block_hash, shard_id);
        } else {
            debug!("Snapshot grabbing completed for shard {}", shard_id);
        }
    }

    fn grab_state(
        &self,
        runtime: &NightshadeRuntime,
        state_root: &StateRoot,
        prev_block_hash: &CryptoHash,
        shard_id: ShardId,
    ) {
        let trie = runtime.get_trie_for_shard(shard_id as u64, prev_block_hash).unwrap();
        let trie = TrieIterator::new(&trie, state_root).unwrap();
        let mut records = Vec::new();
        for item in trie {
            let (key, value) = item.unwrap();
            if let Some(sr) = StateRecord::from_raw_key_value(key, value) {
                if self.accounts_to_grab.contains(state_record_to_account_id(&sr)) {
                    records.push(sr);
                }
            }
        }
        serde_json::to_writer(File::create(format!("records{}.json", shard_id)).unwrap(), &records)
            .unwrap();
        debug!("State grabbed for shard {}", shard_id);
    }

    pub fn grab_transactions(&self, epoch_id: &EpochId, transactions: &[SignedTransaction]) {
        if self.epoch_id.lock().unwrap().get() == Some(epoch_id.0) {
            let mut file =
                fs::OpenOptions::new().write(true).append(true).open("transactions").unwrap();
            for transaction in transactions {
                if self.accounts_to_grab.contains(&transaction.transaction.signer_id) {
                    transaction.serialize(&mut file).unwrap();
                }
            }
        }
    }
}
