use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;
use std::usize;

use bytesize::ByteSize;
use clap::Parser;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout};
use near_primitives::types::RawStateChangesWithTrieKey;
use near_store::adapter::flat_store::decode_flat_state_db_key;
use near_store::{DBCol, KeyForStateChanges, ShardUId, Store};

use crate::utils::open_rocksdb;

#[derive(Parser)]
pub(crate) struct AnalyseArchivalCommand {
    /// Number entries to consider, corresponds to iterator `.take`
    #[arg(short, long)]
    limit: Option<usize>,

    /// Flat state compression level
    #[arg(long, default_value_t = 3)]
    compression_level: usize,

    /// Columns to analyse
    #[arg(long)]
    columns: Vec<AnalyseColumn>,
}

#[derive(clap::ValueEnum, Clone)]
pub enum AnalyseColumn {
    FlatState,
    StateChanges,
}

impl AnalyseArchivalCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadOnly)?);
        let store = near_store::NodeStorage::new(rocksdb).get_hot_store();
        eprintln!("Start archival analysis");
        for column in &self.columns {
            match column {
                AnalyseColumn::FlatState => {
                    self.analyse_flat_state(store.clone());
                }
                AnalyseColumn::StateChanges => {
                    self.analyse_state_changes(store.clone());
                }
            }
        }
        Ok(())
    }

    fn analyse_flat_state(&self, store: Store) {
        eprintln!("Analyse FlatState column");
        let shard_layout = ShardLayout::get_simple_nightshade_layout_v3();
        let shard_uids = shard_layout.shard_uids();
        let mut total_stats = SizeStats::default();
        let mut total_compressed = 0;
        for shard_uid in shard_uids {
            eprintln!("Analysing shard {}", shard_uid.shard_id());
            let mut shard_stats = SizeStats::default();
            let db_key_from = shard_uid.to_bytes().to_vec();
            let db_key_to = ShardUId::get_upper_bound_db_key(&shard_uid.to_bytes()).to_vec();
            let mut shard_entries = Vec::new();
            for res in store
                .iter_range(DBCol::FlatState, Some(&db_key_from), Some(&db_key_to))
                .take(self.limit())
            {
                let (key, value) = res.unwrap();
                let (key_shard_uid, trie_key) = decode_flat_state_db_key(&key).unwrap();
                assert_eq!(shard_uid, key_shard_uid);
                total_stats.update(&trie_key, &value);
                shard_stats.update(&trie_key, &value);
                shard_entries.push((trie_key, value));
            }
            eprintln!("Shard stats: {}", shard_stats);
            let data = borsh::to_vec(&shard_entries).unwrap();
            std::mem::drop(shard_entries);
            let compressed =
                zstd::encode_all(data.as_slice(), self.compression_level as i32).unwrap();
            eprintln!(
                "Raw size: {}, compressed size: {}",
                ByteSize::b(data.len() as u64),
                ByteSize::b(compressed.len() as u64)
            );
            total_compressed += compressed.len();
        }
        eprintln!("Overall stats: {}", total_stats);
        eprintln!("Overall compressed size: {}", ByteSize::b(total_compressed as u64));
    }

    fn analyse_state_changes(&self, store: Store) {
        eprintln!("Analyse StateChanges column");
        let mut blocks = HashSet::new();
        let mut shard_changes =
            HashMap::<ShardUId, HashMap<CryptoHash, Vec<RawStateChangesWithTrieKey>>>::new();
        let shard_layout = ShardLayout::get_simple_nightshade_layout_v3();
        for res in store.iter(DBCol::StateChanges).take(self.limit()) {
            let (key, value) = res.unwrap();
            let changes: RawStateChangesWithTrieKey = borsh::from_slice(value.as_ref()).unwrap();
            // See KeyForStateChanges for key structure
            let block_hash = CryptoHash::try_from(key.split_at(CryptoHash::LENGTH).0).unwrap();
            blocks.insert(block_hash);
            let shard_uid = if let Some(account_id) = changes.trie_key.get_account_id() {
                account_id_to_shard_uid(&account_id, &shard_layout)
            } else {
                KeyForStateChanges::delayed_receipt_key_decode_shard_uid(
                    &key,
                    &block_hash,
                    &changes.trie_key,
                )
                .unwrap()
            };
            shard_changes
                .entry(shard_uid)
                .or_default()
                .entry(block_hash)
                .or_default()
                .push(changes);
        }
        eprintln!("Blocks count: {}", blocks.len());
        let mut total_compressed = 0;
        for shard_uid in shard_layout.shard_uids() {
            eprintln!("Analyse changes for shard {}", shard_uid.shard_id());
            let Some(block_changes) = shard_changes.remove(&shard_uid) else {
                eprintln!("Missing changes for shard {}", shard_uid.shard_id());
                continue;
            };
            let bytes = borsh::to_vec(&block_changes).unwrap();
            let avg_per_block = bytes.len() / block_changes.len();
            let compressed =
                zstd::encode_all(bytes.as_slice(), self.compression_level as i32).unwrap();
            total_compressed += compressed.len();
            eprintln!(
                "Changes raw size: {}, compressed size {}, avg per block: {}",
                ByteSize::b(bytes.len() as u64),
                ByteSize::b(compressed.len() as u64),
                ByteSize::b(avg_per_block as u64)
            );
        }
        eprintln!("Total compressed size {}", ByteSize::b(total_compressed as u64));
    }

    fn limit(&self) -> usize {
        self.limit.unwrap_or(usize::MAX)
    }
}

#[derive(Default)]
struct SizeStats {
    cnt: usize,
    keys: ByteSize,
    values: ByteSize,
}

impl SizeStats {
    fn update(&mut self, key: &[u8], value: &[u8]) {
        self.cnt += 1;
        self.keys += ByteSize::b(key.len() as u64);
        self.values += ByteSize::b(value.len() as u64);
    }
}

impl Display for SizeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cnt: {}, keys: {}, values: {}, total: {}",
            self.cnt,
            self.keys,
            self.values,
            self.keys + self.values
        )
    }
}
