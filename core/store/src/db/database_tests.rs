#![cfg(test)]
//! Set of tests over the 'Database' interface, that we can run over multiple implementations
//! to make sure that they are working correctly.
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

use itertools::Itertools;
use tempfile::Builder;

use crate::db::{DBTransaction, TestDB};
use crate::{DBCol, NodeStorage};

/// Tests the behavior of the iterators. Iterators don't really work over cold storage, so we're not testing it here.
#[test]
fn test_db_iter() {
    let (_tmp_dir, opener) = NodeStorage::test_opener();
    let store = opener.open().unwrap().get_hot_store();
    let test_db = TestDB::new();
    for db in [&*test_db as _, store.database()] {
        let mut transaction = DBTransaction::new();
        transaction.insert(DBCol::Block, "a".into(), "val_a".into());
        transaction.insert(DBCol::Block, "aa".into(), "val_aa".into());
        transaction.insert(DBCol::Block, "aa1".into(), "val_aa1".into());
        transaction.insert(DBCol::Block, "bb1".into(), "val_bb1".into());
        transaction.insert(DBCol::Block, "cc1".into(), "val_cc1".into());
        db.write(transaction).unwrap();

        let keys: Vec<_> = db
            .iter(DBCol::Block)
            .map(|data| String::from_utf8(data.unwrap().0.to_vec()).unwrap())
            .collect();
        assert_eq!(keys, vec!["a", "aa", "aa1", "bb1", "cc1"]);

        let keys: Vec<_> = db
            .iter_range(DBCol::Block, Some("aa".as_bytes()), Some("bb1".as_bytes()))
            .map(|data| String::from_utf8(data.unwrap().0.to_vec()).unwrap())
            .collect();
        assert_eq!(keys, vec!["aa", "aa1"]);
    }
}

#[test]
fn test_print_batch() {
    let file = "/tmp/1752257522372545000-BlockMisc.batch";
    let mut file = std::fs::File::open(file).expect("Failed to open file");
    let transaction: DBTransaction =
        borsh::de::from_reader(&mut file).expect("Failed to deserialize transaction");
    eprintln!("Transaction: {:#?}", transaction);
}

// cspell:words tikv jemallocator Jemalloc
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[test]
fn test_replay_batches() {
    // Get environment variable BATCHES_DIR
    let Ok(batches_dir) = std::env::var("BATCHES_DIR") else {
        // skip the test if the environment variable is not set
        println!("Skipping test_replay_batches: BATCHES_DIR not set");
        return;
    };

    let home = env::var("HOME").expect("HOME not set");
    let base_dir = PathBuf::from(home).join("tmp-db");
    std::fs::create_dir_all(&base_dir).unwrap();
    let tmp_dir = Builder::new().prefix("mytemp-").tempdir_in(&base_dir).unwrap();

    // let tmp_dir = tempfile::tempdir().unwrap();

    // Use default StoreConfig rather than NodeStorage::test_opener so we’re using the
    // same configuration as in production.
    let mut store = NodeStorage::opener(tmp_dir.path(), &Default::default(), None)
        .open()
        .unwrap()
        .get_hot_store();
    store.set_output_batches(false);

    // Iterate over all files in the directory, alphabetically
    let mut entries: Vec<_> = std::fs::read_dir(batches_dir)
        .expect("Failed to read directory")
        .filter_map(Result::ok)
        .collect();
    entries.sort_by_key(|entry| entry.path());

    let mut nanos_last: Option<u64> = None;
    let mut last_block_processed_at: Option<std::time::Instant> = None;
    let mut total_blocking_time = std::time::Duration::ZERO;

    for entry in entries {
        // Parse the filename as {nanos}-{string}.batch
        let filename = entry.file_name();
        let filename_str = filename.to_string_lossy();
        let Some((nanos, cols)) = filename_str.split_once('-') else {
            // skip files that do not match the expected format
            println!("Skipping file: {} (does not match expected format)", filename_str);
            continue;
        };

        let path = entry.path();
        // Check if the file is a regular file
        if !path.is_file() {
            continue;
        }

        let file_len = entry.metadata().expect("Failed to get metadata").len();
        let mut reader = std::fs::File::open(&path).expect("Failed to open file");
        let transaction: DBTransaction = match borsh::de::from_reader(&mut reader) {
            Ok(tx) => tx,
            Err(e) => {
                println!("Failed to deserialize transaction from {}: {}", filename_str, e);
                continue;
            }
        };

        // Critical path for postprocess_ready_block goes like:
        // Block-BlockHeader-...-State-... etc, (main chain update)
        // Flat-FlatState-... etc, (flat state update)
        // Try to wait as long as it took between blocks during actual processing,
        // to allow rocksdb background work to be more realistic.
        if cols.starts_with("Block-") {
            let nanos = nanos.parse::<u64>().expect("Failed to parse nanos");
            match (nanos_last, last_block_processed_at) {
                (Some(last), Some(processed_at)) => {
                    let elapsed =
                        std::time::Instant::now().duration_since(processed_at).as_nanos() as u64;
                    if elapsed < nanos - last {
                        let delay = std::time::Duration::from_nanos(std::cmp::min(
                            1_000_000_000, // at most 1 second delay
                            nanos - last - elapsed,
                        ));
                        eprintln!(
                            "Waiting for {} milliseconds (raw delta {})  before processing {}",
                            delay.as_millis(),
                            (nanos - last - elapsed) / 1_000_000,
                            filename_str
                        );
                        std::thread::sleep(delay);
                    }
                }
                (_, _) => {
                    eprintln!("Processing first file: {} at {} nanoseconds", filename_str, nanos);
                }
            };
            nanos_last = Some(nanos);
            last_block_processed_at = Some(std::time::Instant::now());
        }

        let tx_ops_len = transaction.ops.len();
        if cols.starts_with("Block-") || cols.starts_with("FlatState-FlatStateChanges-") {
            //print_batch_stats(cols, &transaction);
        }
        let now = std::time::Instant::now();
        store.write(transaction).expect("Failed to write transaction to store");
        let elapsed = now.elapsed();

        if cols.starts_with("Block-") || cols.starts_with("FlatState-FlatStateChanges-") {
            total_blocking_time += elapsed;
        }

        if elapsed > std::time::Duration::from_millis(1) {
            println!(
                "Processing file: {}\t(total blocking: {}ms)\t{} items\t{} KBs\telapsed: {:?}ms\tcols: {}",
                nanos,
                total_blocking_time.as_millis(),
                tx_ops_len,
                file_len / 1024,
                elapsed.as_millis(),
                cols
            );
        }
    }
}

fn print_batch_stats(cols: &str, transaction: &DBTransaction) {
    let mut col_ops: HashMap<&str, usize> = HashMap::new();
    let mut col_size: HashMap<&str, usize> = HashMap::new();
    for op in &transaction.ops {
        let name: &'static str = op.col().into();
        *col_ops.entry(name).or_default() += 1;
        *col_size.entry(name).or_default() += op.bytes();
    }
    let col_ops_str = col_ops.iter().map(|(col, count)| format!("{}: {}", col, count)).join(", ");
    let col_size_str =
        col_size.iter().map(|(col, size)| format!("{}: {} bytes", col, size)).join(", ");
    eprintln!("Transaction: {} cols: [{}] ops: [{}]", cols, col_ops_str, col_size_str);
}
