use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use borsh::BorshDeserialize;
use clap::Parser;
use indicatif::HumanBytes;
use near_chain_configs::GenesisValidationMode;
use near_primitives::state::FlatStateValue;
use near_store::{
    flat::{
        delta::KeyForFlatStateDelta,
        store_helper::{decode_flat_state_db_key, encode_flat_state_db_key},
        FlatStateChanges,
    },
    DBCol, Store,
};
use nearcore::{load_config, open_storage};

use crate::parallel_iter::RocksDBParallelIterator;

/// Analyze delayed receipts in a piece of history of the blockchain to understand congestion of each shard
#[derive(Parser)]
pub(crate) struct AggressiveTrimmingCommand {
    #[clap(long)]
    state_deletion_batch_size: Option<usize>,
}

impl AggressiveTrimmingCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let mut near_config = load_config(home, GenesisValidationMode::UnsafeFast).unwrap();
        let node_storage = open_storage(&home, &mut near_config).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());

        // Self::delete_small_state_from_store(
        //     store.clone(),
        //     self.state_deletion_batch_size.unwrap_or(100000),
        // )?;
        Self::keep_only_flat_state(store.clone())?;

        Ok(())
    }

    fn keep_only_flat_state(store: Store) -> anyhow::Result<()> {
        let keys_to_read = Arc::new(Mutex::new(Vec::<(usize, Vec<u8>)>::new()));

        {
            let keys_to_read = keys_to_read.clone();
            RocksDBParallelIterator::for_each_parallel(
                store.clone(),
                DBCol::FlatState,
                vec![3, 0, 0, 0],
                vec![3, 0, 0, 1],
                6,
                move |key, value| {
                    let value = FlatStateValue::try_from_slice(value.unwrap()).unwrap();
                    match value {
                        FlatStateValue::Ref(r) => {
                            let (shard_uid, _) = decode_flat_state_db_key(key).unwrap();
                            let mut keys = keys_to_read.lock().unwrap();
                            keys.push((
                                r.length as usize,
                                encode_flat_state_db_key(shard_uid, &r.hash.0),
                            ));
                            if keys.len() % 10000 == 0 {
                                println!("Found {} keys to read", keys.len());
                            }
                        }
                        FlatStateValue::Inlined(_) => {}
                    }
                },
            );
        }

        {
            let keys_to_read = keys_to_read.lock().unwrap();
            println!(
                "Found {} keys to read from FlatState, total size {}",
                keys_to_read.len(),
                keys_to_read.iter().map(|(size, _)| size).sum::<usize>()
            );
        }

        {
            let keys_to_read = keys_to_read.clone();
            RocksDBParallelIterator::for_each_parallel(
                store.clone(),
                DBCol::FlatStateChanges,
                Vec::new(),
                Vec::new(),
                6,
                move |key, value| {
                    let key = KeyForFlatStateDelta::try_from_slice(key).unwrap();
                    if key.shard_uid.version != 3 {
                        return;
                    }
                    let changes = FlatStateChanges::try_from_slice(value.unwrap()).unwrap();
                    for (_, value) in changes.0 {
                        if let Some(FlatStateValue::Ref(r)) = value {
                            let mut keys = keys_to_read.lock().unwrap();
                            keys.push((
                                r.length as usize,
                                encode_flat_state_db_key(key.shard_uid, &r.hash.0),
                            ));
                            if keys.len() % 10000 == 0 {
                                println!("Found {} keys to read", keys.len());
                            }
                        }
                    }
                },
            );
        }

        {
            let keys_to_read = keys_to_read.lock().unwrap();
            println!(
                "Found {} keys to read from FlatState and FlatStateChanges, total size {}",
                keys_to_read.len(),
                keys_to_read.iter().map(|(size, _)| size).sum::<usize>()
            );
        }

        let values_read = Arc::new(Mutex::new(BTreeMap::<Vec<u8>, Vec<u8>>::new()));
        {
            let keys =
                keys_to_read.lock().unwrap().drain(..).map(|(_, key)| key).collect::<Vec<_>>();
            let values_read = values_read.clone();
            RocksDBParallelIterator::lookup_parallel(
                store.clone(),
                DBCol::State,
                keys,
                6,
                move |key, value| {
                    assert!(value.is_some(), "Key not found: {}", hex::encode(&key));
                    let value = value.unwrap().to_vec();
                    let mut values = values_read.lock().unwrap();
                    values.insert(key.to_vec(), value);
                },
            )
        }

        let mut update = store.store_update();
        update.delete_all(DBCol::State);
        update.commit()?;

        let mut update = store.store_update();
        let mut total_bytes_written = 0;
        let mut total_bytes_written_this_batch = 0;
        let mut total_keys_written = 0;
        let values_read = values_read.lock().unwrap();
        let total_bytes_to_write =
            values_read.iter().map(|(key, value)| key.len() + value.len()).sum::<usize>();
        let total_keys_to_write = values_read.len();
        for (key, value) in values_read.iter() {
            update.set_raw_bytes(DBCol::State, key, value);
            total_bytes_written += key.len() + value.len();
            total_bytes_written_this_batch += key.len() + value.len();
            total_keys_written += 1;
            if total_bytes_written_this_batch > 100_000_000 {
                update.commit()?;
                update = store.store_update();
                total_bytes_written_this_batch = 0;
                println!(
                    "Written {} / {} keys ({} / {})",
                    total_keys_written,
                    total_keys_to_write,
                    HumanBytes(total_bytes_written as u64),
                    HumanBytes(total_bytes_to_write as u64)
                );
            }
        }
        update.commit()?;
        println!(
            "Done. Written {} / {} keys ({} / {})",
            total_keys_written,
            total_keys_to_write,
            HumanBytes(total_bytes_written as u64),
            HumanBytes(total_bytes_to_write as u64)
        );

        println!("Compacting store");
        store.compact()?;
        println!("Done");

        Ok(())
    }
}
