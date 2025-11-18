use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;
use std::usize;

use anyhow::Context;
use borsh::BorshDeserialize;
use bytesize::ByteSize;
use humantime::format_duration;
use clap::Parser;
use near_async::time::{Clock, Instant};
use near_chain::types::RuntimeAdapter;
use near_chain::{Block, Chain, ChainGenesis, ChainStore, DoomslugThresholdMode};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::block::ChunkType;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{ShardLayout, get_block_shard_uid};
use near_primitives::sharding::{ChunkHash, ReceiptProof, ShardChunk};
use near_primitives::state_part::PartId;
use near_primitives::state_sync::{ShardStateSyncResponseHeader, StateHeaderKey};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{RawStateChangesWithTrieKey, ShardId};
use near_primitives::utils::get_block_shard_id;
use near_store::adapter::StoreAdapter;
use near_store::adapter::flat_store::decode_flat_state_db_key;
use near_store::{DBCol, KeyForStateChanges, ShardUId, Store, TrieChanges};
use nearcore::{NightshadeRuntime, NightshadeRuntimeExt};

use crate::utils::{MemtrieSizeCalculator, open_rocksdb};

/// Example usage: neard database analyse-archival memtrie --shard-id 0,1,2
#[derive(Parser)]
pub(crate) struct AnalyseArchivalCommand {
    /// Number entries to consider, corresponds to iterator `.take`
    #[arg(short, long)]
    limit: Option<usize>,

    /// Flat state compression level
    #[arg(long, default_value_t = 3)]
    compression_level: usize,

    /// Stuff to analyze
    #[arg(long, use_value_delimiter = true, value_delimiter = ',')]
    targets: Vec<AnalyseTarget>,

    #[clap(long, use_value_delimiter = true, value_delimiter = ',')]
    shard_id: Option<Vec<ShardId>>,
}

#[derive(clap::ValueEnum, Clone)]
pub enum AnalyseTarget {
    BlockRelatedData,
    ChunkRelatedData,
    FlatState,
    StateChanges,
    Memtrie,
    StateParts,
    TrieChanges,
    Transactions,
    Receipts,
    Stats,
}

impl AnalyseArchivalCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadOnly)?);
        let store = near_store::NodeStorage::new(rocksdb).get_hot_store();
        let near_config = nearcore::config::load_config(&home, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
        let genesis_config = &near_config.genesis.config;
        let chain_store = ChainStore::new(
            store.clone(),
            true,
            near_config.genesis.config.transaction_validity_period,
        );
        let final_head = chain_store.final_head()?;
        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &genesis_config, Some(home));
        let shard_tracker = ShardTracker::new(
            near_config.client_config.tracked_shards_config.clone(),
            epoch_manager.clone(),
            near_config.validator_signer.clone(),
        );
        let runtime = NightshadeRuntime::from_config(
            home,
            store.clone(),
            &near_config,
            epoch_manager.clone(),
        )
        .context("could not create the transaction runtime")?;
        let chain_genesis = ChainGenesis::new(&near_config.genesis.config);
        let mut chain = Chain::new_for_view_client(
            Clock::real(),
            epoch_manager.clone(),
            shard_tracker,
            runtime.clone(),
            &chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            false,
            near_config.validator_signer.clone(),
        )
        .unwrap();

        let block = chain_store.get_block(&final_head.prev_block_hash)?;
        let shard_layout = epoch_manager.get_shard_layout(&final_head.epoch_id)?;
        let shard_uids: Vec<ShardUId> = match &self.shard_id {
            None => shard_layout.shard_uids().collect(),
            Some(shard_ids) => shard_layout
                .shard_uids()
                .filter(|uid| shard_ids.contains(&uid.shard_id()))
                .map(|uid| uid)
                .collect(),
        };
        let shard_ids = shard_uids.iter().map(|uid| uid.shard_id()).collect();
        eprintln!("Start archival analysis for shards: {shard_uids:?}");
        for target in &self.targets {
            match target {
                AnalyseTarget::FlatState => {
                    self.analyse_flat_state(store.clone(), &shard_uids);
                }
                AnalyseTarget::StateChanges => {
                    self.analyse_state_changes(store.clone(), &shard_layout);
                }
                AnalyseTarget::TrieChanges => {
                    self.analyse_trie_changes(store.clone(), &shard_ids);
                }
                AnalyseTarget::BlockRelatedData => {
                    self.analyse_block_related_data(store.clone());
                }
                AnalyseTarget::ChunkRelatedData => {
                    self.analyse_chunk_related_data(store.clone(), &shard_layout);
                }
                AnalyseTarget::Memtrie => {
                    self.analyze_memtrie(store.clone(), &runtime, &shard_uids, &block);
                }
                AnalyseTarget::StateParts => {
                    self.analyze_state_parts(&mut chain, &shard_uids, &block.header().prev_hash());
                }
                AnalyseTarget::Transactions => {
                    self.analyse_txs(store.clone(), &shard_ids);
                }
                AnalyseTarget::Receipts => {
                    self.analyse_receipts(store.clone(), &shard_ids);
                }
                AnalyseTarget::Stats => {
                    self.print_stats(store.clone());
                }
            }
        }
        Ok(())
    }

    fn analyze_state_parts(
        &self,
        chain: &mut Chain,
        shards: &Vec<ShardUId>,
        block_hash: &CryptoHash,
    ) {
        eprintln!("Analyse State parts");
        let sync_hash = chain.get_sync_hash(&block_hash).unwrap().unwrap();
        let sync_block_header = chain.get_block_header(&sync_hash).unwrap();
        let sync_prev_header = chain.get_previous_header(&sync_block_header).unwrap();
        let sync_prev_prev_hash = sync_prev_header.prev_hash();

        for shard_uid in shards {
            let shard_id = shard_uid.shard_id();
            eprintln!("Analysing shard {shard_id}");
            let state_header = chain
                .state_sync_adapter
                .compute_state_response_header(shard_id, sync_hash)
                .unwrap();
            let state_root = state_header.chunk_prev_state_root();
            let num_parts = state_header.num_state_parts();

            let mut total_size = 0;
            let mut total_compressed = 0;
            let mut state_parts = Vec::new();
            let mut total_elapsed_sec = std::time::Duration::ZERO;
            for part_id in 0..num_parts {
                let state_part = chain
                    .runtime_adapter
                    .obtain_state_part(
                        shard_id,
                        sync_prev_prev_hash,
                        &state_root,
                        PartId::new(part_id, num_parts),
                    )
                    .unwrap();
                let data = borsh::to_vec(&state_part).unwrap();
                state_parts.push(state_part);
                let timer = Instant::now();
                let compressed =
                    zstd::encode_all(data.as_slice(), self.compression_level as i32).unwrap();
                let elapsed_sec = timer.elapsed();
                if part_id == 0 {
                    eprintln!(
                        "compressed {} to {} in {}",
                        data.len(), compressed.len(), format_duration(elapsed_sec),
                    );
                }
                total_elapsed_sec += elapsed_sec;
                total_size += data.len();
                total_compressed += compressed.len();
            }
            eprintln!(
                "Avg raw size: {}, Avg compressed size: {}",
                ByteSize::b((total_size / num_parts as usize) as u64),
                ByteSize::b((total_compressed / num_parts as usize) as u64),
            );
            eprintln!(
                "Parts num: {}, Avg compression time: {}",
                num_parts, format_duration(total_elapsed_sec / num_parts as u32),
            );

            let data = borsh::to_vec(&state_parts).unwrap();
            std::mem::drop(state_parts);
            let timer = Instant::now();
            let compressed =
                zstd::encode_all(data.as_slice(), self.compression_level as i32).unwrap();
            let elapsed_sec = timer.elapsed();
            eprintln!(
                "Total size: {}, Total compressed size: {}, compression took {}",
                ByteSize::b(data.len() as u64),
                ByteSize::b(compressed.len() as u64),
                format_duration(elapsed_sec),
            );
        }
    }

    fn analyze_memtrie(
        &self,
        store: Store,
        runtime: &Arc<NightshadeRuntime>,
        shards: &Vec<ShardUId>,
        block: &Block,
    ) {
        eprintln!("Analyse Trie");
        let mut total_inlined_memtries = 0;
        let mut total_inlined_leaves = 0;
        let mut total_non_inlined_memtries = 0;
        let mut total_non_inlined_leaves = 0;
        for shard_uid in shards {
            eprintln!("Analysing shard {}", shard_uid.shard_id());
            let shard_tries = runtime.get_tries();
            shard_tries.load_memtrie(&shard_uid, None, false).unwrap();

            let size_calculator =
                MemtrieSizeCalculator::new(store.chain_store(), &shard_tries, &block);
            let (memtrie_size, leaves_size) =
                size_calculator.get_shard_trie_size(*shard_uid, false).unwrap();
            println!(
                "[Inlined] trie size: {} bytes, leaves: {}",
                ByteSize::b(memtrie_size as u64),
                ByteSize::b(leaves_size as u64),
            );
            total_inlined_memtries += memtrie_size;
            total_inlined_leaves += leaves_size;
            let (memtrie_size, leaves_size) =
                size_calculator.get_shard_trie_size(*shard_uid, true).unwrap();
            println!(
                "[Non-inlined] trie size: {} bytes, leaves: {}",
                ByteSize::b(memtrie_size as u64),
                ByteSize::b(leaves_size as u64),
            );
            total_non_inlined_memtries += memtrie_size;
            total_non_inlined_leaves += leaves_size;
            shard_tries.unload_memtrie(shard_uid);
        }
        println!(
            "Total inlined memtries: {}, leaves: {}",
            ByteSize::b(total_inlined_memtries as u64),
            ByteSize::b(total_inlined_leaves as u64),
        );
        println!(
            "Total non-inlined memtries: {}, leaves: {}",
            ByteSize::b(total_non_inlined_memtries as u64),
            ByteSize::b(total_non_inlined_leaves as u64),
        );
    }

    fn analyse_flat_state(&self, store: Store, shards: &Vec<ShardUId>) {
        eprintln!("Analyse FlatState column");
        let mut total_size = 0;
        let mut total_compressed = 0;
        for shard_uid in shards {
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
                assert_eq!(*shard_uid, key_shard_uid);
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
            total_size += data.len();
            total_compressed += compressed.len();
        }
        eprintln!("Total size: {}", ByteSize::b(total_size as u64));
        eprintln!("Total compressed size: {}", ByteSize::b(total_compressed as u64));
    }

    fn analyse_state_changes(&self, store: Store, shard_layout: &ShardLayout) {
        eprintln!("Analyse StateChanges column");
        let mut blocks = HashSet::new();
        let mut shard_changes =
            HashMap::<ShardUId, HashMap<CryptoHash, Vec<RawStateChangesWithTrieKey>>>::new();
        for res in store.iter(DBCol::StateChanges).take(self.limit()) {
            let (key, value) = res.unwrap();
            let changes: RawStateChangesWithTrieKey = borsh::from_slice(value.as_ref()).unwrap();
            // See KeyForStateChanges for key structure
            let block_hash = CryptoHash::try_from(key.split_at(CryptoHash::LENGTH).0).unwrap();
            blocks.insert(block_hash);
            let shard_uid = if let Some(account_id) = changes.trie_key.get_account_id() {
                shard_layout.account_id_to_shard_uid(&account_id)
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
        let mut total_size = 0;
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
            total_size += bytes.len();
            total_compressed += compressed.len();
            eprintln!(
                "Changes raw size: {}, compressed size {}, avg per block: {}",
                ByteSize::b(bytes.len() as u64),
                ByteSize::b(compressed.len() as u64),
                ByteSize::b(avg_per_block as u64),
            );
        }
        eprintln!("Blocks count: {}", blocks.len());
        eprintln!("Total size {}", ByteSize::b(total_size as u64));
        eprintln!("Total compressed size {}", ByteSize::b(total_compressed as u64));
    }

  fn analyse_txs(&self, store: Store, shards: &Vec<ShardId>) {
        eprintln!("Analyse txs data");
        let mut shard_data = HashMap::<ShardId, HashMap<CryptoHash, SignedTransaction>>::new();
        let mut stats = HashMap::<ShardId, SizeStats>::new();
        let mut block_cnt = 0;
        for res in store.iter_ser::<Block>(DBCol::Block).take(self.limit()) {
            let (_, block) = res.unwrap();
            if block.header().is_genesis() {
                continue;
            }
            block_cnt += 1;
            for chunk in block.chunks().iter() {
                let ChunkType::New(chunk) = chunk else {
                    continue;
                };
                let shard_id = chunk.shard_id();
                if !shards.contains(&shard_id) {
                    continue;
                }
                let chunk_hash = chunk.chunk_hash();
                let chunk =
                    store.get_ser::<ShardChunk>(DBCol::Chunks, chunk_hash.as_bytes()).unwrap().unwrap();
                let txs = chunk.to_transactions();
                //let receipts = chunk.prev_outgoing_receipts();
                for tx in txs {
                    let data = borsh::to_vec(tx).unwrap();
                    stats.entry(shard_id).or_default().update(tx.hash().as_bytes(), &data);
                    shard_data
                        .entry(shard_id)
                        .or_default()
                        .entry(tx.hash().clone())
                        .insert_entry(tx.clone());
                }
            }
        }
        let mut overall_size = 0;
        let mut overall_compressed_size = 0;
        for shard_id in shards {
            let Some(stats) = stats.get(&shard_id) else {
                eprintln!("* Missing stats for shard {shard_id}");
                continue;
            };
            eprintln!("* Stats for shard {shard_id}\n{stats}");
            let shard_map = shard_data.get(&shard_id).unwrap();
            let data = borsh::to_vec(&shard_map).unwrap();
            let compressed =
                zstd::encode_all(data.as_slice(), self.compression_level as i32).unwrap();
            let txs_per_block = shard_map.len() as f32 / block_cnt as f32;
            eprintln!(
                "Txs per block: {:.1}",
                txs_per_block,
            );
            eprintln!(
                "Total raw size: {}, Total compressed size: {}\n",
                ByteSize::b(data.len() as u64),
                ByteSize::b(compressed.len() as u64)
            );
            overall_size += data.len();
            overall_compressed_size += compressed.len();
        }
        eprintln!(
            "\nOverall size: {}, Overall compressed size: {}\nblocks: {}, total size per block (uncompressed): {}\n",
            ByteSize::b(overall_size as u64),
            ByteSize::b(overall_compressed_size as u64),
            block_cnt,
            ByteSize::b((overall_size / block_cnt) as u64),
        );
    }

    fn print_stats(&self, store: Store) {
        println!("Store statistics:\n");
        let stats = store.get_store_statistics().unwrap();
        for stat in stats.data {
            println!("{stat:?}");
        }
    }

    fn analyse_receipts(&self, store: Store, shards: &Vec<ShardId>) {
        eprintln!("Analyse receipts");
        let mut shard_data = HashMap::<ShardId, HashMap<CryptoHash, Receipt>>::new();
        let mut stats = HashMap::<ShardId, SizeStats>::new();
        let mut block_cnt = 0;
        for res in store.iter_ser::<Block>(DBCol::Block).take(self.limit()) {
            let (_, block) = res.unwrap();
            if block.header().is_genesis() {
                continue;
            }
            block_cnt += 1;
            for chunk in block.chunks().iter() {
                let ChunkType::New(chunk) = chunk else {
                    continue;
                };
                let shard_id = chunk.shard_id();
                if !shards.contains(&shard_id) {
                    continue;
                }
                let chunk_hash = chunk.chunk_hash();
                let chunk =
                    store.get_ser::<ShardChunk>(DBCol::Chunks, chunk_hash.as_bytes()).unwrap().unwrap();
                let receipts = chunk.prev_outgoing_receipts();
                for r in receipts {
                    let data = borsh::to_vec(r).unwrap();
                    stats.entry(shard_id).or_default().update(r.receipt_id().as_bytes(), &data);
                    shard_data
                        .entry(shard_id)
                        .or_default()
                        .entry(r.receipt_id().clone())
                        .insert_entry(r.clone());
                }
            }
        }
        let mut overall_size = 0;
        let mut overall_compressed_size = 0;
        for shard_id in shards {
            let Some(stats) = stats.get(&shard_id) else {
                eprintln!("* Missing stats for shard {shard_id}");
                continue;
            };
            eprintln!("* Stats for shard {shard_id}\n{stats}");
            let shard_map = shard_data.get(&shard_id).unwrap();
            let data = borsh::to_vec(&shard_map).unwrap();
            let compressed =
                zstd::encode_all(data.as_slice(), self.compression_level as i32).unwrap();
            let rs_per_block = shard_map.len() as f32 / block_cnt as f32;
            eprintln!(
                "Receipts per block: {:.1}",
                rs_per_block,
            );
            eprintln!(
                "Total raw size: {}, Total compressed size: {}\n",
                ByteSize::b(data.len() as u64),
                ByteSize::b(compressed.len() as u64)
            );
            overall_size += data.len();
            overall_compressed_size += compressed.len();
        }
        eprintln!(
            "\nOverall size: {}, Overall compressed size: {}\nblocks: {}, total size per block (uncompressed): {}\n",
            ByteSize::b(overall_size as u64),
            ByteSize::b(overall_compressed_size as u64),
            block_cnt,
            ByteSize::b((overall_size / block_cnt) as u64),
        );
    }

    fn analyse_trie_changes(&self, store: Store, shards: &Vec<ShardId>) {
        eprintln!("Analyse TrieChanges column (insertions)");
        let mut blocks = HashMap::<ShardId, HashSet<CryptoHash>>::new();
        let mut trie_changes =
            HashMap::<ShardId, HashMap<CryptoHash, Vec<u8>>>::new();
        let mut stats = HashMap::<ShardId, SizeStats>::new();
        for res in store.iter(DBCol::TrieChanges).take(self.limit()) {
            let (key, value) = res.unwrap();
            let suffix: &[u8] = key.split_at(CryptoHash::LENGTH).1.try_into().unwrap();
            let shard_id = ShardUId::try_from(suffix).unwrap().shard_id();
            if !shards.contains(&shard_id) {
                continue;
            }
            let block_hash = CryptoHash::try_from(key.split_at(CryptoHash::LENGTH).0).unwrap();
            let changes: TrieChanges = borsh::from_slice(value.as_ref()).unwrap();
            blocks.entry(shard_id).or_default().insert(block_hash);
            for change in changes.insertions() {
                stats.entry(shard_id).or_default().update(change.hash().as_bytes(), change.payload());
                trie_changes
                    .entry(shard_id)
                    .or_default()
                    .entry(change.hash().clone())
                    .insert_entry(change.payload().to_vec());
            }
        }
        let mut total_count = 0;
        let mut total_size = 0;
        let mut total_compressed = 0;
        for shard_id in shards {
            eprintln!("Analyse changes for shard {}", shard_id);
            let Some(block_changes) = trie_changes.remove(&shard_id) else {
                eprintln!("Missing changes for shard {}", shard_id);
                continue;
            };
            let count = block_changes.len();
            total_count += count;
            let bytes = borsh::to_vec(&block_changes).unwrap();
            let avg_per_block = bytes.len() / block_changes.len();
            let compressed =
                zstd::encode_all(bytes.as_slice(), self.compression_level as i32).unwrap();
            total_size += bytes.len();
            total_compressed += compressed.len();
            let block_count = blocks.get(shard_id).unwrap().len();
            let avg_changes_per_block = count / block_count;
            eprintln!(
                "Blocks: {}, avg trie changes per block: {}, avg trie change size: {}",
                block_count,
                avg_changes_per_block,
                ByteSize::b(avg_per_block as u64),
            );
            eprintln!(
                "Changes raw size: {}, compressed size {}",
                ByteSize::b(bytes.len() as u64),
                ByteSize::b(compressed.len() as u64),
            );
            eprintln!("Stats: {}\n", stats[shard_id]);
        }
        eprintln!("Trie changes count: {}", total_count);
        eprintln!("Total size {}", ByteSize::b(total_size as u64));
        eprintln!("Total compressed size {}", ByteSize::b(total_compressed as u64));
    }

    fn analyse_block_related_data(&self, store: Store) {
        eprintln!("Analyse block-related data");
        let mut block_data = HashMap::<CryptoHash, Vec<Box<[u8]>>>::new();
        for col in [DBCol::Block, DBCol::BlockInfo, DBCol::TransactionResultForBlock] {
            eprintln!("- Analysing column: {col}");
            let mut stats = SizeStats::default();
            let mut skipped_cnt = 0;
            let limit = if col == DBCol::Block { self.limit() } else { usize::MAX };
            for res in store.iter(col).take(limit) {
                let (key, value) = res.unwrap();
                let block_hash = if col == DBCol::TransactionResultForBlock {
                    CryptoHash::try_from(key.split_at(CryptoHash::LENGTH).1).unwrap()
                } else {
                    CryptoHash::try_from_slice(&key).unwrap()
                };
                let entry = if col == DBCol::Block {
                    block_data.entry(block_hash).or_default()
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

    fn get_chunk_related_data(
        store: &Store,
        block_hash: &CryptoHash,
        chunk_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let chunk =
            store.get_ser::<ShardChunk>(DBCol::Chunks, chunk_hash.as_bytes()).unwrap().unwrap();
        let block_shard_uid = get_block_shard_uid(block_hash, shard_uid);
        let chunk_extra =
            store.get_ser::<ChunkExtra>(DBCol::ChunkExtra, &block_shard_uid).unwrap().unwrap();
        let block_shard_id = get_block_shard_id(block_hash, shard_uid.shard_id());
        let incoming_receipts = store
            .get_ser::<Vec<ReceiptProof>>(DBCol::IncomingReceipts, &block_shard_id)
            .unwrap()
            .unwrap();
        let outgoing_receipts = store
            .get_ser::<Vec<Receipt>>(DBCol::OutgoingReceipts, &block_shard_id)
            .unwrap()
            .unwrap();
        let outcome_ids =
            store.get_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds, &block_shard_id).unwrap().unwrap();
        // StateChanges are excluded because they are analysed separately.
        let shard_id_block = borsh::to_vec(&StateHeaderKey(shard_uid.shard_id(), *block_hash))?;
        let state_headers = store
            .get_ser::<ShardStateSyncResponseHeader>(DBCol::StateHeaders, &shard_id_block)
            .unwrap();
        if state_headers.is_some() {
            eprintln!("Found ShardStateSyncResponseHeader");
        }
        let chunk_related_data =
            (chunk, chunk_extra, incoming_receipts, outgoing_receipts, outcome_ids, state_headers);
        // Transactions are excluded because they are already inside `ShardChunk`.
        Ok(borsh::to_vec(&chunk_related_data).unwrap())
    }

    fn analyse_chunk_related_data(&self, store: Store, shard_layout: &ShardLayout) {
        eprintln!("Analyse chunk-related data");
        let mut shard_data = HashMap::<ShardId, HashMap<ChunkHash, Vec<Vec<u8>>>>::new();
        let mut stats = HashMap::<ShardId, SizeStats>::new();
        for res in store.iter_ser::<Block>(DBCol::Block).take(self.limit()) {
            let (key, block) = res.unwrap();
            if block.header().is_genesis() {
                continue;
            }
            let block_hash = CryptoHash::try_from_slice(&key).unwrap();
            for chunk in block.chunks().iter() {
                let ChunkType::New(chunk) = chunk else {
                    continue;
                };
                let chunk_hash = chunk.chunk_hash();
                let shard_id = chunk.shard_id();
                let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
                let chunk_related_data =
                    Self::get_chunk_related_data(&store, &block_hash, &chunk_hash.0, &shard_uid);
                let Ok(data) = chunk_related_data else {
                    continue;
                };
                stats.entry(shard_id).or_default().update(chunk_hash.as_bytes(), &data);
                shard_data
                    .entry(shard_id)
                    .or_default()
                    .entry(chunk_hash.clone())
                    .or_default()
                    .push(data);
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
            ByteSize::b((self.keys.0 + self.values.0) / (self.cnt + 1) as u64),
        )
    }
}
