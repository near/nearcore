use clap::Parser;
use near_store::db::{Database, RocksDB};
use near_store::DBCol;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::{panic, println};
use strum::IntoEnumIterator;

use crate::utils::open_rocksdb;

#[derive(Parser)]
pub(crate) struct AnalyseDataSizeDistributionCommand {
    /// If specified only this column will be analysed
    #[arg(short, long)]
    column: Option<String>,

    /// Number of count sizes to output
    #[arg(short, long, default_value_t = 100)]
    top_k: usize,
}

fn resolve_db_col(col: &str) -> Option<DBCol> {
    DBCol::iter().filter(|db_col| <&str>::from(db_col) == col).next()
}

#[derive(Clone)]
struct ColumnFamilyCountAndSize {
    number_of_pairs: usize,
    size: usize,
}

struct DataSizeDistribution {
    key_sizes: Vec<(usize, usize)>,
    value_sizes: Vec<(usize, usize)>,
    total_num_of_pairs: usize,
    column_families_data: Vec<(String, ColumnFamilyCountAndSize)>,
}

impl DataSizeDistribution {
    fn new(
        mut key_sizes: Vec<(usize, usize)>,
        mut value_sizes: Vec<(usize, usize)>,
        col_families_data: Vec<(String, ColumnFamilyCountAndSize)>,
    ) -> Self {
        // The reason we sort here is because we want to display sorted
        // output that shows the most occurring sizes (the ones with the
        // biggest count) in descending order, to have histogram like order
        key_sizes.sort_by(|a, b| b.1.cmp(&a.1));
        value_sizes.sort_by(|a, b| b.1.cmp(&a.1));
        let total_num_of_pairs = key_sizes.iter().map(|(_, count)| count).sum::<usize>();

        Self {
            key_sizes: key_sizes,
            value_sizes: value_sizes,
            total_num_of_pairs: total_num_of_pairs,
            column_families_data: col_families_data,
        }
    }

    fn print_results(&self, top_k: usize) {
        self.print_column_family_data();
        self.print_sizes_count(&self.key_sizes, "Key", top_k);
        self.print_sizes_count(&self.value_sizes, "Value", top_k);
    }

    fn print_column_family_data(&self) {
        for (column_family_name, column_family_data) in self.column_families_data.iter() {
            println!(
                "Column family {} has {} number of pairs and {} bytes size",
                column_family_name, column_family_data.number_of_pairs, column_family_data.size
            );
        }
    }

    fn print_sizes_count(
        &self,
        sizes_count: &Vec<(usize, usize)>,
        size_count_type: &str,
        top_k: usize,
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
            total_sizes_bytes_sum as f64 / self.total_num_of_pairs as f64
        );
        let mut size_bytes_median = 0;
        let mut median_index = self.total_num_of_pairs / 2;
        for (size, count) in sizes_count.iter().take(top_k) {
            if median_index < *count {
                size_bytes_median = *size;
                break;
            } else {
                median_index -= count;
            }
        }
        println!("Median size {} {}", size_count_type, size_bytes_median);
        for (size, count) in sizes_count.iter().take(top_k) {
            println!("Size: {}, Count: {}", size, count);
        }
        println!("");
    }
}

fn read_all_pairs(db: &RocksDB, col_families: &Vec<DBCol>) -> DataSizeDistribution {
    // Initialize counters
    let key_sizes: Arc<Mutex<HashMap<usize, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let value_sizes: Arc<Mutex<HashMap<usize, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let column_families_data: Arc<Mutex<HashMap<String, ColumnFamilyCountAndSize>>> =
        Arc::new(Mutex::new(HashMap::new()));

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

        //let cf_handle = db.cf_handle(col_family).unwrap();
        for res in db.iter_raw_bytes(*col_family) {
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

        {
            let mut guard = column_families_data.lock().unwrap();
            let column_number_of_pairs = local_key_sizes.values().sum::<usize>();
            let column_size =
                local_key_sizes.iter().map(|(&size, &count)| size * count).sum::<usize>();
            let column_family = ColumnFamilyCountAndSize {
                number_of_pairs: column_number_of_pairs,
                size: column_size,
            };
            guard.insert(col_family.to_string(), column_family);
        }

        update_map(&key_sizes, &local_key_sizes);
        update_map(&value_sizes, &local_value_sizes);
    });

    let key_sizes: Vec<(usize, usize)> = key_sizes.lock().unwrap().clone().into_iter().collect();
    let value_sizes: Vec<(usize, usize)> =
        value_sizes.lock().unwrap().clone().into_iter().collect();
    let column_families: Vec<(String, ColumnFamilyCountAndSize)> =
        column_families_data.lock().unwrap().clone().into_iter().collect();

    DataSizeDistribution::new(key_sizes, value_sizes, column_families)
}

fn get_column_families(input_col: &Option<String>) -> Vec<DBCol> {
    match input_col {
        Some(column_name) => {
            vec![resolve_db_col(&column_name).unwrap()]
        }
        None => DBCol::iter().collect(),
    }
}

impl AnalyseDataSizeDistributionCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let db = open_rocksdb(home)?;
        let column_families = get_column_families(&self.column);
        let results = read_all_pairs(&db, &column_families);
        results.print_results(self.top_k);

        Ok(())
    }
}
