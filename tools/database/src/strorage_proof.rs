use std::collections::HashMap;
use std::sync::Arc;

use near_crypto::{KeyType, PublicKey, SecretKey};
use near_primitives::account::{AccessKey, AccessKeyPermission, Account};
use near_primitives::challenge::PartialState;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, StateChangeCause, StateRoot};
use near_primitives::utils::derive_near_implicit_account_id;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::adapter::trie_store::TrieStoreAdapter;
use near_store::db::TestDB;
use near_store::{Store, Trie, TrieDBStorage, TrieUpdate};

#[derive(clap::Args)]
pub(crate) struct StorageProofCommand {
    #[clap(long)]
    users: usize,
    #[clap(long)]
    transfers: usize,
}

impl StorageProofCommand {
    pub(crate) fn run(&self) -> anyhow::Result<()> {
        let shard_layout = ShardLayout::single_shard();
        let shard_uid = shard_layout.shard_uids().next().unwrap();
        let store = Store::new(TestDB::new());
        let db_storage =
            Arc::new(TrieDBStorage::new(TrieStoreAdapter::new(store.clone()), shard_uid));
        let trie = Trie::new(db_storage.clone(), StateRoot::new(), None);
        let mut trie_update = TrieUpdate::new(trie);
        let user_accounts: HashMap<AccountId, (Account, PublicKey)> = (0..self.users)
            .map(|index| {
                let pk = SecretKey::from_seed(KeyType::ED25519, format!("user{index}").as_str())
                    .public_key();
                let account_id = derive_near_implicit_account_id(pk.unwrap_as_ed25519());
                let account =
                    Account::new(1000_000_000, 0, 0, CryptoHash::default(), 0, PROTOCOL_VERSION);
                (account_id, (account, pk))
            })
            .collect();
        for (account_id, (account, pk)) in &user_accounts {
            trie_update.set(
                TrieKey::Account { account_id: account_id.clone() },
                borsh::to_vec(&account.clone()).unwrap(),
            );
            let access_key = AccessKey { nonce: 42, permission: AccessKeyPermission::FullAccess };
            trie_update.set(
                TrieKey::AccessKey { account_id: account_id.clone(), public_key: pk.clone() },
                borsh::to_vec(&access_key).unwrap(),
            );
        }
        trie_update.commit(StateChangeCause::InitialState);
        let update_res = trie_update.finalize().unwrap();

        let mut store_update = store.store_update();
        for upd in update_res.trie_changes.insertions() {
            let mut key = [0; 40];
            key[0..8].copy_from_slice(&shard_uid.to_bytes());
            key[8..].copy_from_slice(upd.hash().as_ref());
            store_update.increment_refcount(near_store::DBCol::State, &key, upd.payload());
        }
        store_update.commit().unwrap();

        let new_trie = Trie::new(db_storage.clone(), update_res.trie_changes.new_root, None);
        let recording_trie = new_trie.recording_reads_new_recorder();

        // Debit accounts -> convert to receipt
        let mut last_accout = None;
        for (account_id, (expected_account, pk)) in user_accounts.iter().take(self.transfers) {
            let account: Account =
                borsh::from_slice(&recording_trie.get(&trie_key_bytes(TrieKey::Account { account_id: account_id.clone() })).unwrap().unwrap()).unwrap();
            last_accout = Some(account.clone());
            let access_key: AccessKey =
                borsh::from_slice(&recording_trie.get(&trie_key_bytes(TrieKey::AccessKey { account_id: account_id.clone(), public_key: pk.clone() } )).unwrap().unwrap()).unwrap();
            assert_eq!(&account, expected_account);
            assert_eq!(access_key.nonce, 42);
        }
        println!("acc len = {}", borsh::to_vec(&last_accout.unwrap()).unwrap().len());
        // Credit accounts
        for account_id in user_accounts.keys().skip(self.transfers).take(self.transfers) {
            let _account: Account =
                borsh::from_slice(&recording_trie.get(&trie_key_bytes(TrieKey::Account { account_id: account_id.clone() })).unwrap().unwrap()).unwrap();
        }
        let PartialState::TrieValues(values) = recording_trie.recorded_storage().unwrap().nodes;
        println!("cnt: {}", values.len());
        let tot: usize = values.iter().map(|v| v.len()).sum();
        println!("total size: {}", bytesize::ByteSize::b(tot as u64));
        Ok(())
    }
}

fn trie_key_bytes(key: TrieKey) -> Vec<u8> {
    let mut key_bytes = Vec::new();
    key.append_into(&mut key_bytes);
    key_bytes
}
