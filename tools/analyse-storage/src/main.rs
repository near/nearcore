use clap::Parser;
use near_store::db::RocksDB;
use near_store::{col_name, DBCol};
use rayon::prelude::*;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::collections::HashMap;
use std::{println, panic};
use std::sync::{Arc, Mutex};
use strum::IntoEnumIterator;

#[derive(Parser)]
struct Cli {
    db_path: String,

    #[arg(short, long)]
    column: Option<String>,

    #[arg(short, long)]
    limit: Option<usize>,
}

fn is_col_name_rc(col: &str) -> bool {
    for db_col in DBCol::iter() {
        if format!("{}", db_col) == col {
            return db_col.is_rc();
        }
    }
    return false;
}

fn get_all_col_familiy_names() -> Vec<DBCol> {
    Vec::from([
        DBCol::BlockMisc,
        DBCol::Block,
        DBCol::DbVersion,
        DBCol::BlockHeader,
        DBCol::BlockHeight,
        DBCol::State,
        DBCol::ChunkExtra,
        DBCol::_TransactionResult,
        DBCol::OutgoingReceipts,
        DBCol::IncomingReceipts,
        DBCol::_Peers,
        DBCol::EpochInfo,
        DBCol::BlockInfo,
        DBCol::Chunks,
        DBCol::PartialChunks,
        DBCol::BlocksToCatchup,
        DBCol::StateDlInfos,
        DBCol::ChallengedBlocks,
        DBCol::StateHeaders,
        DBCol::InvalidChunks,
        DBCol::BlockExtra,
        DBCol::BlockPerHeight,
        DBCol::StateParts,
        DBCol::EpochStart,
        DBCol::AccountAnnouncements,
        DBCol::NextBlockHashes,
        DBCol::EpochLightClientBlocks,
        DBCol::ReceiptIdToShardId,
        DBCol::_NextBlockWithNewChunk,
        DBCol::_LastBlockWithNewChunk,
        DBCol::PeerComponent,
        DBCol::ComponentEdges,
        DBCol::LastComponentNonce,
        DBCol::Transactions,
        DBCol::_ChunkPerHeightShard,
        DBCol::StateChanges,
        DBCol::BlockRefCount,
        DBCol::TrieChanges,
        DBCol::BlockMerkleTree,
        DBCol::ChunkHashesByHeight,
        DBCol::BlockOrdinal,
        DBCol::_GCCount,
        DBCol::OutcomeIds,
        DBCol::_TransactionRefCount,
        DBCol::ProcessedBlockHeights,
        DBCol::Receipts,
        DBCol::CachedContractCode,
        DBCol::EpochValidatorInfo,
        DBCol::HeaderHashesByHeight,
        DBCol::StateChangesForSplitStates,
    ])
}

fn print_results(
    sizes_count: &Vec<(usize, usize)>,
    size_count_type: &str,
    limit: usize,
    total_num_of_pairs: usize,
) {
    println!(
        "Total number of pairs read {}\n",
        sizes_count.into_iter().map(|(_, count)| count).sum::<usize>()
    );

    // Print out distributions
    println!("{} Size Distribution:", size_count_type);
    println!("Minimum size {}: {:?}", size_count_type, sizes_count.first().unwrap());
    println!("Maximum size {}: {:?}", size_count_type, sizes_count.last().unwrap());
    let total_sizes_bytes_sum = sizes_count.iter().map(|a| a.0 * a.1).sum::<usize>();
    println!(
        "Average size {}: {:?}",
        size_count_type,
        total_sizes_bytes_sum as f64 / total_num_of_pairs as f64
    );
    let mut size_bytes_median = 0;
    let mut median_index = total_num_of_pairs / 2;
    for (size, count) in sizes_count.iter().take(limit) {
        if median_index < *count {
            size_bytes_median = *size;
            break;
        } else {
            median_index -= count;
        }
    }
    println!("Median size {} {}", size_count_type, size_bytes_median);
    for (size, count) in sizes_count.iter().take(limit) {
        println!("Size: {}, Count: {}", size, count);
    }
    println!("");
}

fn rocksdb_column_options(col: DBCol) -> Options {
    let mut opts = Options::default();
    opts.set_level_compaction_dynamic_level_bytes(true);

    if col.is_rc() {
        opts.set_merge_operator("refcount merge", RocksDB::refcount_merge, RocksDB::refcount_merge);
        opts.set_compaction_filter("empty value filter", RocksDB::empty_value_compaction_filter);
    }
    opts
}

fn read_all_pairs(
    db: &DB,
    col_families: &Vec<String>,
) -> (Vec<(usize, usize)>, Vec<(usize, usize)>) {
    // Initialize counters
    let key_sizes: Arc<Mutex<HashMap<usize, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let value_sizes: Arc<Mutex<HashMap<usize, usize>>> = Arc::new(Mutex::new(HashMap::new()));

    // Iterate over key-value pairs
    let update_map = |global_map: &Arc<Mutex<HashMap<usize, usize>>>,
                      local_map: &HashMap<usize, usize>| {
        let mut key_sizes_guard = global_map.lock().unwrap();
        for (key, value) in local_map {
            *key_sizes_guard.entry(*key).or_insert(0) += *value;
        }
    };
    col_families.par_iter().for_each(|col_family| {
        let mut local_key_sizes: HashMap<usize, usize> = HashMap::new();
        let mut local_value_sizes: HashMap<usize, usize> = HashMap::new();

        let cf_handle = db.cf_handle(col_family).unwrap();
        for res in db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start) {
            match res {
                Ok(tuple) => {
                    // Count key sizes
                    let key_len = tuple.0.len();
                    *local_key_sizes.entry(key_len).or_insert(0) += 1;

                    // Count value sizes
                    let value_len = tuple.1.len();
                    *local_value_sizes.entry(value_len).or_insert(0) += 1;
                }
                Err(err) => {
                    panic!("Error occured during iteration of {}: {}", col_family, err);
                }
            }
        }
        println!(
            "In column family {} there are {} number of pairs, and col size is {}",
            col_family,
            local_key_sizes.values().sum::<usize>(),
            local_key_sizes.iter().map(|(&size, &count)| size * count).sum::<usize>()
        );
        update_map(&key_sizes, &local_key_sizes);
        update_map(&value_sizes, &local_value_sizes);
    });

    let mut key_sizes_sorted: Vec<(usize, usize)> =
        key_sizes.lock().unwrap().clone().into_iter().collect();
    key_sizes_sorted.sort_by(|a, b| b.1.cmp(&a.1));
    let mut value_sizes_sorted: Vec<(usize, usize)> =
        value_sizes.lock().unwrap().clone().into_iter().collect();
    value_sizes_sorted.sort_by(|a, b| b.1.cmp(&a.1));
    (key_sizes_sorted, value_sizes_sorted)
}

fn get_column_family_options(
    input_col: Option<String>,
) -> (Vec<String>, Vec<ColumnFamilyDescriptor>) {
    match input_col {
        Some(col_name) => {
            let mut opts = Options::default();
            if is_col_name_rc(&col_name)  {
                opts.set_merge_operator(
                    "refcount merge",
                    RocksDB::refcount_merge,
                    RocksDB::refcount_merge,
                );
                opts.set_compaction_filter(
                    "empty value filter",
                    RocksDB::empty_value_compaction_filter,
                );
            }
            (vec![col_name.clone()], vec![ColumnFamilyDescriptor::new(col_name, opts)])
        }
        None => {
            let col_families = get_all_col_familiy_names();
            (
                col_families.iter().map(|db_col| col_name(*db_col).to_string()).collect(),
                col_families
                    .iter()
                    .map(|db_col| {
                        ColumnFamilyDescriptor::new(
                            col_name(*db_col),
                            rocksdb_column_options(*db_col),
                        )
                    })
                    .collect(),
            )
        }
    }
}

fn main() {
    let args = Cli::parse();

    // Set db options
    let mut opts = Options::default();
    opts.set_max_open_files(20_000);
    opts.increase_parallelism(std::cmp::max(1, num_cpus::get() as i32 / 2));
 
    // Define column families
    let (col_families, col_families_cf) = get_column_family_options(args.column);

    // Open db
    let db =
        DB::open_cf_descriptors_read_only(&opts, args.db_path, col_families_cf, false).unwrap();
    let (key_sizes_sorted, value_sizes_sorted) = read_all_pairs(&db, &col_families);
    let total_num_of_pairs = key_sizes_sorted.iter().map(|(_, count)| count).sum::<usize>();

    let limit = match args.limit {
        Some(limit) => limit,
        None => 100,
    };
    print_results(&key_sizes_sorted, "Key", limit, total_num_of_pairs);
    print_results(&value_sizes_sorted, "Value", limit, total_num_of_pairs);
}
