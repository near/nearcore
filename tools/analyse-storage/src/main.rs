use clap::Parser;
use near_store::{col_name, DBCol};
use plotters::prelude::*;
use rayon::prelude::*;
use rocksdb::{Options, DB};
use std::collections::HashMap;
use std::println;
use std::sync::{Arc, Mutex};

#[derive(Parser)]
struct Cli {
    db_path: String,

    #[arg(short, long)]
    column: Option<String>,

    #[arg(short, long)]
    draw_histogram: bool,

    #[arg(short, long)]
    limit: Option<usize>,
}

// Function to draw a histogram
// TODO(jbajic) This fails...
fn draw_histogram(
    data: &Vec<(usize, usize)>,
    title: &str,
    filename: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = SVGBackend::new(filename, (640, 480)).into_drawing_area();
    root.fill(&WHITE)?;

    let max_size = *data.into_iter().map(|(size, _)| size).max().unwrap();
    let max_count = *data.into_iter().map(|(_, count)| count).max().unwrap();

    let mut chart = ChartBuilder::on(&root)
        .caption(title, ("Arial", 20).into_font())
        .margin(10)
        .x_label_area_size(50)
        .y_label_area_size(50)
        .build_cartesian_2d(0..max_size, 0..max_count)?;

    chart.configure_mesh().draw()?;
    let series = data.iter().map(|(size, count)| (*size, *count));
    chart.draw_series(Histogram::vertical(&chart).style(BLUE.filled()).data(series))?;

    Ok(())
}

fn get_all_col_familiy_names() -> Vec<String> {
    Vec::from([
        col_name(DBCol::BlockMisc),
        col_name(DBCol::Block),
        col_name(DBCol::DbVersion),
        col_name(DBCol::BlockHeader),
        col_name(DBCol::BlockHeight),
        col_name(DBCol::State),
        col_name(DBCol::ChunkExtra),
        col_name(DBCol::_TransactionResult),
        col_name(DBCol::OutgoingReceipts),
        col_name(DBCol::IncomingReceipts),
        col_name(DBCol::_Peers),
        col_name(DBCol::EpochInfo),
        col_name(DBCol::BlockInfo),
        col_name(DBCol::Chunks),
        col_name(DBCol::PartialChunks),
        col_name(DBCol::BlocksToCatchup),
        col_name(DBCol::StateDlInfos),
        col_name(DBCol::ChallengedBlocks),
        col_name(DBCol::StateHeaders),
        col_name(DBCol::InvalidChunks),
        col_name(DBCol::BlockExtra),
        col_name(DBCol::BlockPerHeight),
        col_name(DBCol::StateParts),
        col_name(DBCol::EpochStart),
        col_name(DBCol::AccountAnnouncements),
        col_name(DBCol::NextBlockHashes),
        col_name(DBCol::EpochLightClientBlocks),
        col_name(DBCol::ReceiptIdToShardId),
        col_name(DBCol::_NextBlockWithNewChunk),
        col_name(DBCol::_LastBlockWithNewChunk),
        col_name(DBCol::PeerComponent),
        col_name(DBCol::ComponentEdges),
        col_name(DBCol::LastComponentNonce),
        col_name(DBCol::Transactions),
        col_name(DBCol::_ChunkPerHeightShard),
        col_name(DBCol::StateChanges),
        col_name(DBCol::BlockRefCount),
        col_name(DBCol::TrieChanges),
        col_name(DBCol::BlockMerkleTree),
        col_name(DBCol::ChunkHashesByHeight),
        col_name(DBCol::BlockOrdinal),
        col_name(DBCol::_GCCount),
        col_name(DBCol::OutcomeIds),
        col_name(DBCol::_TransactionRefCount),
        col_name(DBCol::ProcessedBlockHeights),
        col_name(DBCol::Receipts),
        col_name(DBCol::CachedContractCode),
        col_name(DBCol::EpochValidatorInfo),
        col_name(DBCol::HeaderHashesByHeight),
        col_name(DBCol::StateChangesForSplitStates),
    ])
    .into_iter()
    .map(|s| s.to_string())
    .collect()
}

fn print_results(
    sizes_count: &Vec<(usize, usize)>,
    size_count_type: &str,
    limit: usize,
    total_num_of_pairs: usize,
) {
    println!(
        "Total number of pairs read {}",
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


fn read_all_pairs(db: &DB, col_families: &Vec<String>) -> (Vec<(usize, usize)>, Vec<(usize, usize)>) {
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
                Err(_) => {
                    println!("Error occured during iteration");
                }
            }
        }
        println!("In column family {} there are {} number of pairs", col_family, local_key_sizes.len());
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

fn main() {
    let args = Cli::parse();

    // Set db options
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_max_open_files(10_000);
    opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::SkipAnyCorruptedRecord);
    opts.increase_parallelism(std::cmp::max(1, 32));

    // Define column families
    let col_families = match args.column {
        Some(col_name) => vec![col_name],
        None => get_all_col_familiy_names(),
    };

    // Open db
    let db = DB::open_cf_for_read_only(&opts, args.db_path, col_families.clone(), false).unwrap();
    let (key_sizes_sorted, value_sizes_sorted) = read_all_pairs(&db, &col_families);
    let total_num_of_pairs = key_sizes_sorted.iter().map(|(_, count)| count).sum::<usize>();
    
    let limit = match args.limit {
        Some(limit) => limit,
        None => 100,
    };
    print_results(&key_sizes_sorted, "Key", limit, total_num_of_pairs);
    print_results(&value_sizes_sorted, "Value", limit, total_num_of_pairs);

    // Draw histograms
    if args.draw_histogram && !key_sizes_sorted.is_empty() {
        draw_histogram(
            &key_sizes_sorted.into_iter().take(limit).collect(),
            "Key size distribution",
            "key_sizes.svg",
        )
        .unwrap();
        draw_histogram(
            &value_sizes_sorted.into_iter().take(limit).collect(),
            "Value size distribution",
            "value_sizes.svg",
        )
        .unwrap();
    }
}
