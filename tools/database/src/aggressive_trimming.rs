use borsh::BorshDeserialize;
use clap::Parser;
use indicatif::HumanBytes;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::{GenesisConfig, GenesisValidationMode};
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::state::FlatStateValue;
use near_store::flat::delta::KeyForFlatStateDelta;
use near_store::flat::store_helper::{decode_flat_state_db_key, encode_flat_state_db_key};
use near_store::flat::FlatStateChanges;
use near_store::parallel_iter::StoreParallelIterator;
use near_store::{DBCol, Store};
use nearcore::{load_config, open_storage};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// For developers only. Aggressively trims the database for testing purposes.
#[derive(Parser)]
pub(crate) struct AggressiveTrimmingCommand {
    #[clap(long)]
    obliterate_disk_trie: bool,
}

impl AggressiveTrimmingCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let mut near_config = load_config(home, GenesisValidationMode::UnsafeFast).unwrap();
        let node_storage = open_storage(&home, &mut near_config).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        if self.obliterate_disk_trie {
            Self::obliterate_disk_trie(store, &near_config.genesis.config)?;
        }
        Ok(())
    }

    /// Delete the entire State column except those that are not inlined by flat storage.
    /// This is used to TEST that we are able to rely on memtries only to run a node.
    /// It is NOT safe for production. Do not trim your nodes like this. It will break your node.
    fn obliterate_disk_trie(store: Store, genesis_config: &GenesisConfig) -> anyhow::Result<()> {
        let sharding_version = {
            let epoch_manager = EpochManager::new_arc_handle(store.clone(), genesis_config);
            let chain = ChainStore::new(store.clone(), genesis_config.genesis_height, false);
            let final_head = chain.final_head()?;
            let epoch_id = final_head.epoch_id;
            let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
            shard_layout.version()
        };

        // Find all keys that are not inlined by flat storage. This includes both the
        // FlatState column and FlatStateChanges column.
        let non_inlined_keys = Arc::new(Mutex::new(Vec::<(usize, Vec<u8>)>::new()));
        {
            let non_inlined_keys = non_inlined_keys.clone();
            StoreParallelIterator::for_each_in_range(
                store.clone(),
                DBCol::FlatState,
                sharding_version.to_le_bytes().to_vec(),
                (sharding_version + 1).to_le_bytes().to_vec(),
                6,
                move |key, value| {
                    let value = FlatStateValue::try_from_slice(value).unwrap();
                    match value {
                        FlatStateValue::Ref(r) => {
                            let (shard_uid, _) = decode_flat_state_db_key(key).unwrap();
                            let mut keys = non_inlined_keys.lock().unwrap();
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
                true,
            );
        }

        {
            let non_inlined_keys = non_inlined_keys.lock().unwrap();
            println!(
                "Found {} non-inlined keys from FlatState, total size {}",
                non_inlined_keys.len(),
                non_inlined_keys.iter().map(|(size, _)| size).sum::<usize>()
            );
        }

        {
            let non_inlined_keys = non_inlined_keys.clone();
            StoreParallelIterator::for_each_in_range(
                store.clone(),
                DBCol::FlatStateChanges,
                Vec::new(),
                Vec::new(),
                6,
                move |key: &[u8], value| {
                    let key = KeyForFlatStateDelta::try_from_slice(key).unwrap();
                    if key.shard_uid.version != sharding_version {
                        return;
                    }
                    let changes = FlatStateChanges::try_from_slice(value).unwrap();
                    for (_, value) in changes.0 {
                        if let Some(FlatStateValue::Ref(r)) = value {
                            let mut keys = non_inlined_keys.lock().unwrap();
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
                true,
            );
        }

        {
            let non_inlined_keys = non_inlined_keys.lock().unwrap();
            println!(
                "Found {} non-inlined keys from FlatState and FlatStateChanges, total size {}",
                non_inlined_keys.len(),
                non_inlined_keys.iter().map(|(size, _)| size).sum::<usize>()
            );
        }

        // Now read the non-inlined keys from the State column.
        let non_inlined_entries = Arc::new(Mutex::new(BTreeMap::<Vec<u8>, Vec<u8>>::new()));
        {
            let keys =
                non_inlined_keys.lock().unwrap().drain(..).map(|(_, key)| key).collect::<Vec<_>>();
            let values_read = non_inlined_entries.clone();
            StoreParallelIterator::lookup_keys(
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
                true,
            )
        }

        // Now that we've read all the non-inlined keys into memory, delete the State column, and
        // write back these values.
        let mut update = store.store_update();
        update.delete_all(DBCol::State);
        update.commit()?;

        let mut update = store.store_update();
        let mut total_bytes_written = 0;
        let mut total_bytes_written_this_batch = 0;
        let mut total_keys_written = 0;
        let values_read = non_inlined_entries.lock().unwrap();
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

        Ok(())
    }
}
