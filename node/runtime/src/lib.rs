extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
#[macro_use]
extern crate log;

extern crate byteorder;
extern crate primitives;
extern crate storage;

use byteorder::{LittleEndian, WriteBytesExt};
use primitives::signature::PublicKey;
use primitives::types::{AccountId, MerkleHash, SignedTransaction, ViewCall, ViewCallResult};
use storage::{StateDb, StateDbUpdate};
use storage::{MemoryDB, TestBackend, TestBackendTransaction, TestChangesTrieStorage, TestExt};
use storage::{Externalities, OverlayedChanges, Backend};

#[derive(Serialize, Deserialize)]
pub struct Account {
    pub public_keys: Vec<PublicKey>,
    pub nonce: u64,
    pub amount: u64,
}

#[derive(Default)]
pub struct Runtime {}

fn account_id_to_bytes(account_key: AccountId) -> Vec<u8> {
    let mut bytes = vec![];
    bytes
        .write_u64::<LittleEndian>(account_key)
        .expect("writing to bytes failed");
    bytes
}

/// TODO: runtime must include balance / staking / WASM modules.
impl Runtime {
    pub fn get_account(
        &self,
        state_update: &mut TestExt,
        account_key: AccountId,
    ) -> Option<Account> {
        state_update
            .storage(&account_id_to_bytes(account_key))
            .and_then(|data| bincode::deserialize(&data).ok())
    }

    pub fn set_account(
        &self,
        state_update: &mut TestExt,
        account_key: AccountId,
        account: &Account,
    ) {
        match bincode::serialize(&account) {
            Ok(data) => state_update.set_storage(account_id_to_bytes(account_key), data),
            Err(e) => {
                error!("error occurred while encoding: {:?}", e);
            }
        }
    }
    fn apply_transaction(
        &self,
        state_update: &mut TestExt,
        transaction: &SignedTransaction,
    ) -> bool {
        let sender = self.get_account(state_update, transaction.body.sender);
        let receiver = self.get_account(state_update, transaction.body.receiver);
        match (sender, receiver) {
            (Some(mut sender), Some(mut receiver)) => {
                sender.amount -= transaction.body.amount;
                sender.nonce = transaction.body.nonce;
                receiver.amount += transaction.body.amount;
                self.set_account(state_update, transaction.body.sender, &sender);
                self.set_account(state_update, transaction.body.sender, &receiver);
                true
            }
            _ => false,
        }
    }

    pub fn apply(
        &self,
        backend: &TestBackend,
        transactions: Vec<SignedTransaction>,
    ) -> (Vec<SignedTransaction>, TestBackendTransaction, MerkleHash) {
        let mut filtered_transactions = vec![];
        let mut overlay = OverlayedChanges::default();

        for t in transactions.into_iter() {
            let success = {
                let mut state_update = TestExt::new(&mut overlay, backend, None);
                self.apply_transaction(&mut state_update, &t)
            };

            if success {
                overlay.commit_prospective();
            } else {
                overlay.discard_prospective();
            }
        }

        {
            let mut state_update = TestExt::new(&mut overlay, backend, None);

            let root = state_update.storage_root();
            let (storage_transaction, _changes_trie_transaction) = state_update.transaction();

            (filtered_transactions, storage_transaction, root)
        }
    }

    pub fn view(
        &self,
        backend: &TestBackend,
        view_call: &ViewCall,
    ) -> ViewCallResult {
        let mut overlay = OverlayedChanges::default();
        let mut state_update = TestExt::new(&mut overlay, backend, None);
        match self.get_account(&mut state_update, view_call.account) {
            Some(account) => ViewCallResult {
                account: view_call.account,
                amount: account.amount,
            },
            None => ViewCallResult {
                account: view_call.account,
                amount: 0,
            },
        }
    }
}
