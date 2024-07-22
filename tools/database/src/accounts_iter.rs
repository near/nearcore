use borsh::BorshDeserialize;
use clap::Parser;
use indicatif::{ProgressBar, ProgressIterator};
use near_primitives::account::Account;
use near_primitives::trie_key::trie_key_parsers;
use std::collections::BTreeMap;
use std::fmt::{Display, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::state::ValueRef;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

use near_store::flat::store_helper::{decode_flat_state_db_key, iter_flat_state_entries};
use near_store::{Store, TrieStorage};

use crate::utils::open_rocksdb;

#[derive(Parser)]
pub(crate) struct AccountsIterCommand {
}

impl AccountsIterCommand {
    pub(crate) fn run(&self, home: &Path) -> anyhow::Result<()> {
        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadOnly)?);
        let store = near_store::NodeStorage::new(rocksdb).get_hot_store();
        eprintln!("Start accounts iter");
        iter_accounts(store);
        Ok(())
    }
}

fn iter_accounts(store: Store) {
    let shard_uids = ShardLayout::get_simple_nightshade_layout_v3().shard_uids().collect::<Vec<_>>();
    for shard_uid in shard_uids {
        for trie_key in iter_flat_state_entries(shard_uid, &store, None, None)
            .flat_map(|res| res.map(|(key, _)| key))
        {
            let account_id = trie_key_parsers::parse_account_id_from_raw_key(&trie_key).unwrap().unwrap();
            eprintln!("{} -> {}", shard_uid.shard_id, account_id);
            break;
        }
    }
    eprintln!("Finished requests generation");
}
