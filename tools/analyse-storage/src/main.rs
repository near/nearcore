use clap::Parser;
use near_store::{col_name, DBCol};
use plotters::prelude::*;
use rocksdb::{Options, DB};
use std::collections::HashMap;

#[derive(Parser)]
struct Cli {
    db_path: String,

    #[arg(short, long)]
    column: Option<String>,

    #[arg(short, long)]
    draw_histogram: bool,
}

// Function to draw a histogram
fn draw_histogram(
    data: &HashMap<usize, usize>,
    title: &str,
    filename: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = SVGBackend::new(filename, (640, 480)).into_drawing_area();
    root.fill(&WHITE)?;

    let max_size = *data.keys().max().unwrap();
    let max_count = *data.values().max().unwrap();

    let mut chart = ChartBuilder::on(&root)
        .caption(title, ("Arial", 20).into_font())
        .margin(10)
        .x_label_area_size(50)
        .y_label_area_size(50)
        .build_cartesian_2d(0..max_size, 0..max_count)?;

    chart.configure_mesh().draw()?;
    let series = data.iter().map(|(&size, &count)| (size, count));
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

fn main() {
    let args = Cli::parse();

    // Set db options
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_max_open_files(10_000);
    opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::SkipAnyCorruptedRecord);

    // Define column families
    let col_families = match args.column {
        Some(col_name) => vec![col_name],
        None => get_all_col_familiy_names(),
    };

    // Open db
    let db = DB::open_cf_for_read_only(&opts, args.db_path, col_families.clone(), false).unwrap();

    // Initialize counters
    let mut key_sizes: HashMap<usize, usize> = HashMap::new();
    let mut value_sizes: HashMap<usize, usize> = HashMap::new();

    // Iterate over key-value pairs
    for col_family in col_families.iter() {
        let cf_handle = db.cf_handle(col_family).unwrap();
        let iter = db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
        for res in iter {
            match res {
                Ok(tuple) => {
                    // Count key sizes
                    let key_len = tuple.0.len();
                    *key_sizes.entry(key_len).or_insert(0) += 1;

                    // Count value sizes
                    let value_len = tuple.1.len();
                    *value_sizes.entry(value_len).or_insert(0) += 1;
                }
                Err(_) => {
                    println!("Error occured during iteration");
                }
            }
        }
    }

    println!("Total number of pairs read {}", key_sizes.values().sum::<usize>());
    // Print out distributions
    println!("Key Size Distribution:");
    for (size, count) in key_sizes.iter() {
        println!("Size: {}, Count: {}", size, count);
    }

    println!("Value Size Distribution:");
    for (size, count) in value_sizes.iter() {
        println!("Size: {}, Count: {}", size, count);
    }

    // Draw histograms
    let check_for_draw = |sizes_map: HashMap<usize, usize>, size_type: &str| {
        if sizes_map.is_empty() {
            println!("{} have not been read!", size_type);
        } else {
            if args.draw_histogram {
                draw_histogram(
                    &sizes_map,
                    &format!("{} Size Distribution", size_type),
                    &format!("{}_sizes.svg", size_type),
                )
                .unwrap();
            }
        }
    };
    check_for_draw(key_sizes, "key");
    check_for_draw(value_sizes, "value");
}
