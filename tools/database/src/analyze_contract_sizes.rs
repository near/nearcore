use std::collections::BTreeMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;

use bytesize::ByteSize;
use clap::Parser;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::EpochManager;
use near_primitives::trie_key::col;
use near_primitives::types::AccountId;
use near_store::{ShardUId, Trie, TrieDBStorage};
use nearcore::{load_config, open_storage};

#[derive(Parser)]
pub(crate) struct AnalyzeContractSizesCommand {
    /// Show top N contracts by size.
    #[arg(short, long, default_value_t = 50)]
    topn: usize,

    /// Compress contract code before calculating size.
    #[arg(long, default_value_t = false)]
    compressed: bool,
}

struct ContractSizeStats {
    topn: usize,
    top_accounts: BTreeMap<ByteSize, AccountId>,
    total_accounts: usize,
    shard_accounts: BTreeMap<ShardUId, usize>,
}

impl ContractSizeStats {
    pub fn new(topn: usize) -> ContractSizeStats {
        ContractSizeStats {
            topn,
            top_accounts: BTreeMap::new(),
            total_accounts: 0,
            shard_accounts: BTreeMap::new(),
        }
    }

    pub fn add_info(&mut self, shard_uid: ShardUId, account_id: AccountId, contract_size: usize) {
        self.total_accounts += 1;
        *self.shard_accounts.entry(shard_uid).or_insert(0) += 1;

        self.top_accounts.insert(ByteSize::b(contract_size as u64), account_id);
        if self.top_accounts.len() > self.topn {
            self.top_accounts.pop_first();
        }
    }

    pub fn print_stats(&self, compressed: bool) {
        println!("");
        println!("Analyzed {} accounts", self.total_accounts);
        println!("Accounts per shard:");
        for (shard_uid, count) in self.shard_accounts.iter() {
            println!("{}: {}", shard_uid, count);
        }
        println!("");
        if compressed {
            println!("Top {} accounts by compressed contract size:", self.topn);
        } else {
            println!("Top {} accounts by contract size:", self.topn);
        }
        for (size, account_id) in self.top_accounts.iter().rev() {
            println!("{}: {}", size, account_id);
        }
    }
}

impl AnalyzeContractSizesCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let mut near_config = load_config(home, GenesisValidationMode::Full).unwrap();
        let node_storage = open_storage(&home, &mut near_config).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store = Rc::new(ChainStore::new(
            store.clone(),
            near_config.genesis.config.genesis_height,
            false,
        ));

        let head = chain_store.head().unwrap();
        let epoch_manager =
            EpochManager::new_from_genesis_config(store.clone(), &near_config.genesis.config)
                .unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&head.epoch_id).unwrap();

        let mut stats = ContractSizeStats::new(self.topn);
        for shard_uid in shard_layout.shard_uids() {
            println!("Analyzing chunk with uid: {}", shard_uid);

            let chunk_extra =
                chain_store.get_chunk_extra(&head.last_block_hash, &shard_uid).unwrap();

            let state_root = chunk_extra.state_root();
            let trie_storage = Rc::new(TrieDBStorage::new(store.clone(), shard_uid));
            let trie = Trie::new(trie_storage, *state_root, None);

            let mut iterator = trie.disk_iter().unwrap();
            iterator.seek_prefix(&[col::CONTRACT_CODE]).unwrap();

            for (i, item) in iterator.enumerate() {
                let (key, value) = item.unwrap();
                if key.is_empty() || key[0] != col::CONTRACT_CODE {
                    break;
                }
                let account_id_bytes = &key[1..];
                let account_id_str = std::str::from_utf8(&account_id_bytes).unwrap();
                let account_id = AccountId::from_str(account_id_str).unwrap();

                let contract_size = if self.compressed {
                    zstd::encode_all(value.as_slice(), 3).unwrap().len()
                } else {
                    value.len()
                };

                stats.add_info(shard_uid, account_id, contract_size);
                if i % 1000 == 0 {
                    println!("Processed {} contracts...", i);
                }
            }
        }

        stats.print_stats(self.compressed);
        Ok(())
    }
}
