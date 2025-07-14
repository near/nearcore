use std::collections::VecDeque;
use std::io::{self, Write};
use std::path::Path;

use anyhow::anyhow;
use near_chain::Block;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::trie::mem::node::MemTrieNodeView;
use near_store::{DBCol, ShardTries, ShardUId};
use strum::IntoEnumIterator;

pub(crate) fn get_user_confirmation(message: &str) -> bool {
    print!("{}\nAre you sure? (y/N): ", message);
    io::stdout().flush().expect("Failed to flush stdout");
    let mut input = String::new();
    io::stdin().read_line(&mut input).expect("Failed to read input");
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

pub(crate) fn open_rocksdb(
    home: &Path,
    mode: near_store::Mode,
) -> anyhow::Result<near_store::db::RocksDB> {
    let config = nearcore::config::Config::from_file_skip_validation(
        &home.join(nearcore::config::CONFIG_FILENAME),
    )?;
    let store_config = &config.store;
    let db_path = store_config.path.as_ref().cloned().unwrap_or_else(|| home.join("data"));
    let rocksdb =
        near_store::db::RocksDB::open(&db_path, store_config, mode, near_store::Temperature::Hot)?;
    Ok(rocksdb)
}

pub(crate) fn resolve_column(col_name: &str) -> anyhow::Result<DBCol> {
    DBCol::iter()
        .filter(|db_col| <&str>::from(db_col) == col_name)
        .next()
        .ok_or_else(|| anyhow!("column {col_name} does not exist"))
}

pub struct MemtrieSizeCalculator<'a, 'b> {
    chain_store: ChainStoreAdapter,
    shard_tries: &'a ShardTries,
    block: &'b Block,
}

impl<'a, 'b> MemtrieSizeCalculator<'a, 'b> {
    pub fn new(chain_store: ChainStoreAdapter, shard_tries: &'a ShardTries, block: &'b Block) -> Self {
        Self { chain_store, shard_tries, block }
    }

    /// Get RAM usage of a shard trie
    /// Does a BFS of the whole memtrie
    pub fn get_shard_trie_size(&self, shard_uid: ShardUId, non_inlined: bool) -> anyhow::Result<(u64, u64)> {
        let chunk_extra = self.chain_store.get_chunk_extra(self.block.hash(), &shard_uid)?;
        let state_root = chunk_extra.state_root();
        //println!("Shard {shard_uid}: state root: {state_root}");

        let memtries = self
            .shard_tries
            .get_memtries(shard_uid)
            .ok_or_else(|| anyhow::anyhow!("Cannot get memtrie"))?;
        let read_guard = memtries.read();
        let root_ptr = read_guard.get_root(state_root)?;

        let mut queue = VecDeque::new();
        queue.push_back(root_ptr);
        let mut total_size = 0;
        let mut total_leaves_size = 0;

        while let Some(node_ptr) = queue.pop_front() {
            let alloc_size = node_ptr.size_of_allocation(non_inlined) as u64;
            total_size += alloc_size;

            match node_ptr.view() {
                MemTrieNodeView::Leaf { .. } => {
                    total_leaves_size += alloc_size;
                }
                MemTrieNodeView::Extension { child, .. } => queue.push_back(child),
                MemTrieNodeView::Branch { children, .. }
                | MemTrieNodeView::BranchWithValue { children, .. } => {
                    queue.extend(children.iter())
                }
            }
        }

        Ok((total_size, total_leaves_size))
    }
}
