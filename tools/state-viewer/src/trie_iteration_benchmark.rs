use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Instant};

use near_chain::{ChainStore, ChainStoreAccess};
use near_epoch_manager::EpochManager;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::state_record::{state_record_to_account_id, StateRecord};
use near_primitives::trie_key::col;
use near_primitives::trie_key::trie_key_parsers::{
    parse_account_id_from_access_key_key, parse_account_id_from_trie_key_with_separator,
};
use near_primitives_core::types::ShardId;
use near_store::{ShardUId, Store, Trie, TrieDBStorage};
use nearcore::NearConfig;

#[derive(Clone)]
pub enum TrieIterationType {
    Full,
    Shallow,
}

impl clap::ValueEnum for TrieIterationType {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Full, Self::Shallow]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        match self {
            Self::Full => Some(clap::builder::PossibleValue::new("full")),
            Self::Shallow => Some(clap::builder::PossibleValue::new("shallow")),
        }
    }
}

#[derive(Default)]
struct ColumnCountMap(HashMap<u8, usize>);

impl ColumnCountMap {
    fn col_to_string(col: u8) -> &'static str {
        match col {
            col::ACCOUNT => "ACCOUNT",
            col::CONTRACT_CODE => "CONTRACT_CODE",
            col::DELAYED_RECEIPT => "DELAYED_RECEIPT",
            col::DELAYED_RECEIPT_INDICES => "DELAYED_RECEIPT_INDICES",
            col::ACCESS_KEY => "ACCESS_KEY",
            col::CONTRACT_DATA => "CONTRACT_DATA",
            col::RECEIVED_DATA => "RECEIVED_DATA",
            col::POSTPONED_RECEIPT_ID => "POSTPONED_RECEIPT_ID",
            col::PENDING_DATA_COUNT => "PENDING_DATA_COUNT",
            col::POSTPONED_RECEIPT => "POSTPONED_RECEIPT",
            _ => unreachable!(),
        }
    }
}

impl std::fmt::Debug for ColumnCountMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for (col, count) in &self.0 {
            map.entry(&Self::col_to_string(*col), &count);
        }
        map.finish()
    }
}

#[derive(Debug)]
pub struct TrieIterationBenchmarkStats {
    visited_map: ColumnCountMap,
    pruned_map: ColumnCountMap,
}

impl TrieIterationBenchmarkStats {
    pub fn new() -> Self {
        Self { visited_map: ColumnCountMap::default(), pruned_map: ColumnCountMap::default() }
    }

    pub fn bump_visited(&mut self, col: u8) {
        let entry = self.visited_map.0.entry(col).or_insert(0);
        *entry += 1;
    }

    pub fn bump_pruned(&mut self, col: u8) {
        let entry = self.pruned_map.0.entry(col).or_insert(0);
        *entry += 1;
    }
}

#[derive(clap::Parser)]
pub struct TrieIterationBenchmarkCmd {
    /// The type of trie iteration.
    /// - Full will iterate over all trie keys.
    /// - Shallow will only iterate until full account id prefix can be parsed
    ///   in the trie key. Most notably this will skip any keys or data
    ///   belonging to accounts.
    #[clap(long, default_value = "full")]
    iteration_type: TrieIterationType,

    /// Limit the number of trie nodes to be iterated.
    #[clap(long)]
    limit: Option<u32>,
}

impl TrieIterationBenchmarkCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        let genesis_config = &near_config.genesis.config;
        let chain_store = ChainStore::new(
            store.clone(),
            genesis_config.genesis_height,
            near_config.client_config.save_trie_changes,
        );
        let head = chain_store.head().unwrap();
        let block = chain_store.get_block(&head.last_block_hash).unwrap();
        let epoch_manager =
            EpochManager::new_from_genesis_config(store.clone(), &genesis_config).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();

        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            if chunk_header.height_included() != block.header().height() {
                println!("chunk for shard {shard_id} is missing and will be skipped");
            }
        }

        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if chunk_header.height_included() != block.header().height() {
                println!("chunk for shard {shard_id} is missing, skipping it");
                continue;
            }
            let trie = self.get_trie(shard_id, &shard_layout, &chunk_header, &store);

            println!("shard id    {shard_id:#?} benchmark starting");
            self.iter_trie(&trie);
            println!("shard id    {shard_id:#?} benchmark finished");
        }
    }

    fn get_trie(
        &self,
        shard_id: ShardId,
        shard_layout: &ShardLayout,
        chunk_header: &ShardChunkHeader,
        store: &Store,
    ) -> Trie {
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, shard_layout);
        // Note: here we get the previous state root but the shard layout
        // corresponds to the current epoch id. In practice shouldn't
        // matter as the shard layout doesn't change.
        let state_root = chunk_header.prev_state_root();
        let storage = TrieDBStorage::new(store.clone(), shard_uid);
        let flat_storage_chunk_view = None;
        Trie::new(Rc::new(storage), state_root, flat_storage_chunk_view)
    }

    fn iter_trie(&self, trie: &Trie) {
        let stats = Rc::new(RefCell::new(TrieIterationBenchmarkStats::new()));
        let stats_clone = Rc::clone(&stats);

        let prune_condition: Option<Box<dyn Fn(&Vec<u8>) -> bool>> = match &self.iteration_type {
            TrieIterationType::Full => None,
            TrieIterationType::Shallow => Some(Box::new(move |key_nibbles| -> bool {
                Self::shallow_iter_prune_condition(key_nibbles, &stats_clone)
            })),
        };

        let start = Instant::now();
        let mut node_count = 0;
        let mut error_count = 0;
        let iter = trie.iter_with_prune_condition(prune_condition);
        let iter = match iter {
            Ok(iter) => iter,
            Err(err) => {
                tracing::error!("iter error {err:#?}");
                return;
            }
        };
        for item in iter {
            node_count += 1;

            let (key, value) = match item {
                Ok((key, value)) => (key, value),
                Err(err) => {
                    tracing::error!("Failed to iterate node with error: {err}.");
                    error_count += 1;
                    continue;
                }
            };

            stats.borrow_mut().bump_visited(key[0]);

            let state_record = StateRecord::from_raw_key_value(key.clone(), value);
            let state_record = match state_record {
                Some(state_record) => state_record,
                None => {
                    println!("Failed to parse state record.");
                    error_count += 1;
                    continue;
                }
            };
            tracing::trace!(
                target: "trie-iteration-benchmark",
                "visiting column {} account id {}",
                &state_record.get_type_string(),state_record_to_account_id(&state_record)
            );

            if let Some(limit) = self.limit {
                if limit <= node_count {
                    break;
                }
            }
        }
        let duration = start.elapsed();
        println!("node count  {node_count}");
        println!("error count {error_count}");
        println!("time        {duration:?}");
        println!("stats\n{:#?}", stats.borrow());
    }

    fn shallow_iter_prune_condition(
        key_nibbles: &Vec<u8>,
        stats: &Rc<RefCell<TrieIterationBenchmarkStats>>,
    ) -> bool {
        // Need at least 2 nibbles for the column type byte.
        if key_nibbles.len() < 2 {
            return false;
        }

        // The key method will drop the last nibble if there is an odd number of
        // them. This is on purpose because the interesting keys have even length.
        let key = Self::key(key_nibbles);
        let col: u8 = key[0];
        let result = match col {
            // key for account only contains account id, nothing to prune
            col::ACCOUNT => false,
            // key for contract code only contains account id, nothing to prune
            col::CONTRACT_CODE => false,
            // key for delayed receipt only contains account id, nothing to prune
            col::DELAYED_RECEIPT => false,
            // key for delayed receipt indices is a shard singleton, nothing to prune
            col::DELAYED_RECEIPT_INDICES => false,

            // Most columns use the ACCOUNT_DATA_SEPARATOR to indicate the end
            // of the accound id in the trie key. For those columns the
            // partial_parse_account_id method should be used.
            // The only exception is the ACCESS_KEY and dedicated method
            // partial_parse_account_id_from_access_key should be used.
            col::ACCESS_KEY => Self::partial_parse_account_id_from_access_key(&key, "ACCESS KEY"),
            col::CONTRACT_DATA => Self::partial_parse_account_id(col, &key, "CONTRACT DATA"),
            col::RECEIVED_DATA => Self::partial_parse_account_id(col, &key, "RECEIVED DATA"),
            col::POSTPONED_RECEIPT_ID => {
                Self::partial_parse_account_id(col, &key, "POSTPONED RECEIPT ID")
            }
            col::PENDING_DATA_COUNT => {
                Self::partial_parse_account_id(col, &key, "PENDING DATA COUNT")
            }
            col::POSTPONED_RECEIPT => {
                Self::partial_parse_account_id(col, &key, "POSTPONED RECEIPT")
            }
            _ => unreachable!(),
        };

        if result {
            stats.borrow_mut().bump_pruned(col);
        }

        result

        // TODO - this can be optimized, we really only need to look at the last
        // byte of the key and check if it is the separator. This only works
        // when doing full iteration as seeking inside of the trie would break
        // the invariant that parent node key was already checked.
    }

    fn key(key_nibbles: &Vec<u8>) -> Vec<u8> {
        // Intentionally ignoring the odd nibble at the end.
        let mut result = <Vec<u8>>::with_capacity(key_nibbles.len() / 2);
        for i in (1..key_nibbles.len()).step_by(2) {
            result.push(key_nibbles[i - 1] * 16 + key_nibbles[i]);
        }
        result
    }

    fn partial_parse_account_id(col: u8, key: &Vec<u8>, col_name: &str) -> bool {
        match parse_account_id_from_trie_key_with_separator(col, &key, "") {
            Ok(account_id) => {
                tracing::trace!(
                    target: "trie-iteration-benchmark",
                    "pruning column {col_name} account id {account_id:?}"
                );
                true
            }
            Err(_) => false,
        }
    }

    // returns true if the partial key contains full account id
    fn partial_parse_account_id_from_access_key(key: &Vec<u8>, col_name: &str) -> bool {
        match parse_account_id_from_access_key_key(&key) {
            Ok(account_id) => {
                tracing::trace!(
                    target: "trie-iteration-benchmark",
                    "pruning column {col_name} account id {account_id:?}"
                );
                true
            }
            Err(_) => false,
        }
    }
}
