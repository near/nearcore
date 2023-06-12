use clap::Parser;
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

fn main() {
    let args = Cli::parse();

    // Set db options
    let mut opts = Options::default();
    opts.create_if_missing(true);

    // Open the RocksDB database
    let db = match &args.column {
        Some(col) => {
            let cf = vec![col];
            DB::open_cf_for_read_only(&opts, args.db_path, cf, false).unwrap()
        }
        None => DB::open_for_read_only(&opts, args.db_path, false).unwrap(),
    };

    // Initialize counters
    let mut key_sizes: HashMap<usize, usize> = HashMap::new();
    let mut value_sizes: HashMap<usize, usize> = HashMap::new();

    // Iterate over all key-value pairs
    let cf_handle = db.cf_handle(&args.column.unwrap()).unwrap();
    for res in db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start) {
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
