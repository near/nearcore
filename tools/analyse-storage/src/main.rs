use clap::Parser;
use plotters::prelude::*;
use rocksdb::{Options, DB};
use std::collections::HashMap;
use near_store::DBCol;

#[derive(Parser)]
struct Cli {
    db_path: String,

    #[arg(short, long)]
    column: Option<String>,

    #[arg(short, long)]
    draw_histogram: bool,
}

pub fn col_name(col: DBCol) -> &'static str {
    match col {
        DBCol::DbVersion => "col0",
        DBCol::BlockMisc => "col1",
        DBCol::Block => "col2",
        DBCol::BlockHeader => "col3",
        DBCol::BlockHeight => "col4",
        DBCol::State => "col5",
        DBCol::ChunkExtra => "col6",
        DBCol::_TransactionResult => "col7",
        DBCol::OutgoingReceipts => "col8",
        DBCol::IncomingReceipts => "col9",
        DBCol::_Peers => "col10",
        DBCol::EpochInfo => "col11",
        DBCol::BlockInfo => "col12",
        DBCol::Chunks => "col13",
        DBCol::PartialChunks => "col14",
        DBCol::BlocksToCatchup => "col15",
        DBCol::StateDlInfos => "col16",
        DBCol::ChallengedBlocks => "col17",
        DBCol::StateHeaders => "col18",
        DBCol::InvalidChunks => "col19",
        DBCol::BlockExtra => "col20",
        DBCol::BlockPerHeight => "col21",
        DBCol::StateParts => "col22",
        DBCol::EpochStart => "col23",
        DBCol::AccountAnnouncements => "col24",
        DBCol::NextBlockHashes => "col25",
        DBCol::EpochLightClientBlocks => "col26",
        DBCol::ReceiptIdToShardId => "col27",
        DBCol::_NextBlockWithNewChunk => "col28",
        DBCol::_LastBlockWithNewChunk => "col29",
        DBCol::PeerComponent => "col30",
        DBCol::ComponentEdges => "col31",
        DBCol::LastComponentNonce => "col32",
        DBCol::Transactions => "col33",
        DBCol::_ChunkPerHeightShard => "col34",
        DBCol::StateChanges => "col35",
        DBCol::BlockRefCount => "col36",
        DBCol::TrieChanges => "col37",
        DBCol::BlockMerkleTree => "col38",
        DBCol::ChunkHashesByHeight => "col39",
        DBCol::BlockOrdinal => "col40",
        DBCol::_GCCount => "col41",
        DBCol::OutcomeIds => "col42",
        DBCol::_TransactionRefCount => "col43",
        DBCol::ProcessedBlockHeights => "col44",
        DBCol::Receipts => "col45",
        DBCol::CachedContractCode => "col46",
        DBCol::EpochValidatorInfo => "col47",
        DBCol::HeaderHashesByHeight => "col48",
        DBCol::StateChangesForSplitStates => "col49",
        // If you’re adding a new column, do *not* create a new case for it.
        // All new columns are handled by this default case:
        #[allow(unreachable_patterns)]
        _ => <&str>::from(col),
    }
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

fn main() {
    let args = Cli::parse();

    // Set db options
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_max_open_files(10_000);

    // Open db
    let db = match &args.column {
        Some(col) => {
            let cf = vec![col];
            DB::open_cf_for_read_only(&opts, args.db_path, cf, false).unwrap()
        },
        None => {
            let column_families = Vec::from([
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
            ]);
            DB::open_cf_for_read_only(&opts, args.db_path, column_families, false).unwrap()
        }
    };

    // Initialize counters
    let mut key_sizes: HashMap<usize, usize> = HashMap::new();
    let mut value_sizes: HashMap<usize, usize> = HashMap::new();

    // Get iterator
    let iter = match &args.column {
        Some(col) => {
            let cf_handle = db.cf_handle(col).unwrap();
            db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start)
        },
        None => {
            db.iterator(rocksdb::IteratorMode::Start)
        }
    };
    // Iterate over key-value pairs
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
