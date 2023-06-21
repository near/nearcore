use clap::Parser;
use near_store::db::RocksDB;
use near_store::{col_name, DBCol};
use rayon::prelude::*;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::{panic, println};
use strum::IntoEnumIterator;
use nearcore::get_default_home;
use once_cell::sync::Lazy;

static DEFAULT_HOME: Lazy<PathBuf> = Lazy::new(get_default_home);

#[derive(Parser)]
struct Cli {
    #[clap(long, value_parser, default_value_os = DEFAULT_HOME.as_os_str())]
    home: PathBuf, 

    #[arg(short, long)]
    column: Option<String>,

    #[arg(short, long)]
    limit: Option<usize>,
}

pub fn get_db_col(col: &str) -> Option<DBCol> {
    match col {
        "col0" => Some(DBCol::DbVersion),
        "col1" => Some(DBCol::BlockMisc),
        "col2" => Some(DBCol::Block),
        "col3" => Some(DBCol::BlockHeader),
        "col4" => Some(DBCol::BlockHeight),
        "col5" => Some(DBCol::State),
        "col6" => Some(DBCol::ChunkExtra),
        "col7" => Some(DBCol::_TransactionResult),
        "col8" => Some(DBCol::OutgoingReceipts),
        "col9" => Some(DBCol::IncomingReceipts),
        "col10" => Some(DBCol::_Peers),
        "col11" => Some(DBCol::EpochInfo),
        "col12" => Some(DBCol::BlockInfo),
        "col13" => Some(DBCol::Chunks),
        "col14" => Some(DBCol::PartialChunks),
        "col15" => Some(DBCol::BlocksToCatchup),
        "col16" => Some(DBCol::StateDlInfos),
        "col17" => Some(DBCol::ChallengedBlocks),
        "col18" => Some(DBCol::StateHeaders),
        "col19" => Some(DBCol::InvalidChunks),
        "col20" => Some(DBCol::BlockExtra),
        "col21" => Some(DBCol::BlockPerHeight),
        "col22" => Some(DBCol::StateParts),
        "col23" => Some(DBCol::EpochStart),
        "col24" => Some(DBCol::AccountAnnouncements),
        "col25" => Some(DBCol::NextBlockHashes),
        "col26" => Some(DBCol::EpochLightClientBlocks),
        "col27" => Some(DBCol::ReceiptIdToShardId),
        "col28" => Some(DBCol::_NextBlockWithNewChunk),
        "col29" => Some(DBCol::_LastBlockWithNewChunk),
        "col30" => Some(DBCol::PeerComponent),
        "col31" => Some(DBCol::ComponentEdges),
        "col32" => Some(DBCol::LastComponentNonce),
        "col33" => Some(DBCol::Transactions),
        "col34" => Some(DBCol::_ChunkPerHeightShard),
        "col35" => Some(DBCol::StateChanges),
        "col36" => Some(DBCol::BlockRefCount),
        "col37" => Some(DBCol::TrieChanges),
        "col38" => Some(DBCol::BlockMerkleTree),
        "col39" => Some(DBCol::ChunkHashesByHeight),
        "col40" => Some(DBCol::BlockOrdinal),
        "col41" => Some(DBCol::_GCCount),
        "col42" => Some(DBCol::OutcomeIds),
        "col43" => Some(DBCol::_TransactionRefCount),
        "col44" => Some(DBCol::ProcessedBlockHeights),
        "col45" => Some(DBCol::Receipts),
        "col46" => Some(DBCol::CachedContractCode),
        "col47" => Some(DBCol::EpochValidatorInfo),
        "col48" => Some(DBCol::HeaderHashesByHeight),
        "col49" => Some(DBCol::StateChangesForSplitStates),
        _ => {
            for db_col in DBCol::iter() {
                if col_name(db_col) == col {
                    return Some(db_col);
                }
            }
            return None;
        }
    }
}

fn get_all_col_family_names() -> Vec<DBCol> {
    DBCol::iter().collect()
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
    println!(
        "Minimum size {}: {:?}",
        size_count_type,
        sizes_count.iter().map(|(size, _)| size).min().unwrap()
    );
    println!(
        "Maximum size {}: {:?}",
        size_count_type,
        sizes_count.iter().map(|(size, _)| size).max().unwrap()
    );
    println!("Most occurring size {}: {:?}", size_count_type, sizes_count.first().unwrap());
    println!("Least occurring size {}: {:?}", size_count_type, sizes_count.last().unwrap());

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
        let mut global_sizes_guard = global_map.lock().unwrap();
        for (key, value) in local_map {
            *global_sizes_guard.entry(*key).or_insert(0) += *value;
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
                    panic!("Error occurred during iteration of {}: {}", col_family, err);
                }
            }
        }
        println!(
            "In column family {} there are {} pairs, and col size is {}",
            col_family,
            local_key_sizes.values().sum::<usize>(),
            local_key_sizes.iter().map(|(&size, &count)| size * count).sum::<usize>()
        );
        update_map(&key_sizes, &local_key_sizes);
        update_map(&value_sizes, &local_value_sizes);
    });

    // The reason we sort here is because we want to display sorted
    // output that shows the most occurring sizes (the ones with the 
    // biggest count) in descending order, to have histogram like order
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
            let maybe_db_col = get_db_col(&col_name);
            if let Some(db_col) = maybe_db_col {
                if db_col.is_rc() {
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
            }
            (vec![col_name.clone()], vec![ColumnFamilyDescriptor::new(col_name, opts)])
        }
        None => {
            let col_families = get_all_col_family_names();
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
        DB::open_cf_descriptors_read_only(&opts, args.home.to_str().unwrap(), col_families_cf, false).unwrap();
    let (key_sizes_sorted, value_sizes_sorted) = read_all_pairs(&db, &col_families);
    let total_num_of_pairs = key_sizes_sorted.iter().map(|(_, count)| count).sum::<usize>();

    let limit = match args.limit {
        Some(limit) => limit,
        None => 100,
    };
    print_results(&key_sizes_sorted, "Key", limit, total_num_of_pairs);
    print_results(&value_sizes_sorted, "Value", limit, total_num_of_pairs);
}
