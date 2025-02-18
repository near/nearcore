use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;
use std::usize;

use borsh::BorshDeserialize;
use bytesize::ByteSize;
use clap::Parser;
use near_chain::Block;
use near_primitives::block::MaybeNew;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{account_id_to_shard_uid, get_block_shard_uid, ShardLayout};
use near_primitives::sharding::{ChunkHash, ReceiptProof, ShardChunk};
use near_primitives::state_sync::{ShardStateSyncResponseHeader, StateHeaderKey};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{RawStateChangesWithTrieKey, ShardId};
use near_primitives::utils::get_block_shard_id;
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
    BlockRelatedData,
    ChunkRelatedData,
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
                AnalyseColumn::BlockRelatedData => {
                    self.analyse_block_related_data(store.clone());
                }
                AnalyseColumn::ChunkRelatedData => {
                    self.analyse_chunk_related_data(store.clone());
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

    fn analyse_block_related_data(&self, store: Store) {
        eprintln!("Analyse block-related data");
        let mut block_data = HashMap::<CryptoHash, Vec<Box<[u8]>>>::new();
        for col in [DBCol::Block, DBCol::BlockExtra, DBCol::BlockInfo, DBCol::TransactionResultForBlock] {
            eprintln!("- Analysing column: {col}");
            let mut stats = SizeStats::default();
            let mut skipped_cnt = 0;
            let limit = if col == DBCol::Block { self.limit() } else { usize::MAX };
            for res in store
                .iter(col)
                .take(limit)
            {
                let (key, value) = res.unwrap();
                let block_hash = if col == DBCol::TransactionResultForBlock {
                    CryptoHash::try_from(key.split_at(CryptoHash::LENGTH).1).unwrap()
                } else {
                    CryptoHash::try_from_slice(&key).unwrap()
                };
                let entry = if col == DBCol::Block {
                    block_data
                        .entry(block_hash)
                        .or_default()
                } else if let Some(entry) = block_data.get_mut(&block_hash) {
                    entry
                } else {
                    skipped_cnt += 1;
                    continue;
                };
                stats.update(&key, &value);
                entry.push(value);
            }
            if skipped_cnt > 0 {
                eprintln!("Skipped {skipped_cnt} entries");
            }
            eprintln!("Stats: {}", stats);
        }

        let block_data = borsh::to_vec(&block_data).unwrap();
        let compressed =
            zstd::encode_all(block_data.as_slice(), self.compression_level as i32).unwrap();
        eprintln!(
            "Total raw size: {}, Total compressed size: {}",
            ByteSize::b(block_data.len() as u64),
            ByteSize::b(compressed.len() as u64)
        );
    }

    fn get_chunk_related_data(store: &Store, block_hash: &CryptoHash, chunk_hash: &CryptoHash, shard_uid: &ShardUId) -> Result<Vec<u8>, anyhow::Error> {
        let chunk = store.get_ser::<ShardChunk>(DBCol::Chunks, chunk_hash.as_bytes()).unwrap().unwrap();
        let block_shard_uid = get_block_shard_uid(block_hash, shard_uid);
        let chunk_extra = store.get_ser::<ChunkExtra>(DBCol::ChunkExtra, &block_shard_uid).unwrap().unwrap();
        let block_shard_id = get_block_shard_id(block_hash, shard_uid.shard_id());
        let incoming_receipts = store.get_ser::<Vec<ReceiptProof>>(DBCol::IncomingReceipts, &block_shard_id).unwrap().unwrap();
        let outgoing_receipts = store.get_ser::<Vec<Receipt>>(DBCol::OutgoingReceipts, &block_shard_id).unwrap().unwrap();
        let outcome_ids = store.get_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds, &block_shard_id).unwrap().unwrap();
        // StateChanges are excluded because they are analysed separately.
        let shard_id_block = borsh::to_vec(&StateHeaderKey(shard_uid.shard_id(), *block_hash))?;
        let state_headers = store.get_ser::<ShardStateSyncResponseHeader>(DBCol::StateHeaders, &shard_id_block).unwrap();
        if state_headers.is_some() {
            eprintln!("Found ShardStateSyncResponseHeader");
        }
        let chunk_related_data = (chunk, chunk_extra, incoming_receipts, outgoing_receipts, outcome_ids, state_headers);
        // Transactions are excluded because they are already inside `ShardChunk`.
        Ok(borsh::to_vec(&chunk_related_data).unwrap())
    }

    fn analyse_chunk_related_data(&self, store: Store) {
        eprintln!("Analyse chunk-related data");
        let mut shard_data = HashMap::<ShardId, HashMap::<ChunkHash, Vec<Vec<u8>>>>::new();
        let mut stats = HashMap::<ShardId, SizeStats>::new();
        let shard_layout = ShardLayout::get_simple_nightshade_layout_v3();
        for res in store
            .iter_ser::<Block>(DBCol::Block)
            .take(self.limit())
        {
            let (key, block) = res.unwrap();
            let block_hash = CryptoHash::try_from_slice(&key).unwrap();
            for chunk in block.chunks().iter() {
                let MaybeNew::New(chunk) = chunk else { continue; };
                let chunk_hash = chunk.chunk_hash();
                let shard_id = chunk.shard_id();
                let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
                let chunk_related_data = Self::get_chunk_related_data(&store, &block_hash, &chunk_hash.0, &shard_uid);
                let Ok(data) = chunk_related_data else {
                    continue;
                };
                stats.entry(shard_id).or_default().update(chunk_hash.as_bytes(), &data);
                shard_data.entry(shard_id).or_default().entry(chunk_hash).or_default().push(data);
            }
        }
        let mut overall_size = 0;
        let mut overall_compressed_size = 0;
        for shard_id in shard_layout.shard_ids() {
            let stats = stats.get(&shard_id).unwrap();
            eprintln!("* Stats for shard {shard_id}\n{stats}");
            let data = borsh::to_vec(&shard_data.get(&shard_id).unwrap()).unwrap();
            let compressed =
                zstd::encode_all(data.as_slice(), self.compression_level as i32).unwrap();
            eprintln!(
                "Total raw size: {}, Total compressed size: {}",
                ByteSize::b(data.len() as u64),
                ByteSize::b(compressed.len() as u64)
            );
            overall_size += data.len();
            overall_compressed_size += compressed.len();
        }
        eprintln!(
            "\nOverall size: {}, Overall compressed size: {}",
            ByteSize::b(overall_size as u64),
            ByteSize::b(overall_compressed_size as u64)
        );
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
            "cnt: {}, keys: {}, values: {}, total: {}, avg: {}",
            self.cnt,
            self.keys,
            self.values,
            self.keys + self.values,
            ByteSize::b((self.keys.0 + self.values.0) / self.cnt as u64),
        )
    }
}
