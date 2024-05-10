use indicatif::HumanBytes;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisConfig;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::{borsh::BorshDeserialize, state::FlatStateValue};
use near_store::{
    flat::store_helper::{decode_flat_state_db_key, encode_flat_state_db_key},
    parallel_iter::StoreParallelIterator,
    DBCol, Store,
};

pub fn trim_database(old: Store, genesis_config: &GenesisConfig, new: Store) -> anyhow::Result<()> {
    let sharding_version = {
        let epoch_manager = EpochManager::new_arc_handle(old.clone(), genesis_config);
        let chain = ChainStore::new(old.clone(), genesis_config.genesis_height, false);
        let final_head = chain.final_head()?;
        let epoch_id = final_head.epoch_id;
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
        shard_layout.version()
    };

    let (tx, rx) = std::sync::mpsc::sync_channel(1000000);
    let iter_handle = {
        let old = old.clone();
        std::thread::spawn(move || {
            StoreParallelIterator::for_each_in_range(
                old,
                DBCol::FlatState,
                sharding_version.to_le_bytes().to_vec(),
                (sharding_version + 1).to_le_bytes().to_vec(),
                6,
                move |key, value| {
                    tx.send((key.to_vec(), value.to_vec())).unwrap();
                },
                true,
            );
        })
    };

    let mut update = new.store_update();
    let mut keys_to_read = Vec::<Vec<u8>>::new();
    let mut keys_copied = 0;
    let mut bytes_copied = 0;
    let mut bytes_this_batch = 0;
    let mut last_print = std::time::Instant::now();
    for (key, value) in rx {
        update.set_raw_bytes(DBCol::FlatState, &key, &value);
        keys_copied += 1;
        bytes_copied += key.len() + value.len();
        bytes_this_batch += key.len() + value.len();
        if bytes_this_batch > 100 * 1024 * 1024 {
            update.commit()?;
            update = new.store_update();
            bytes_this_batch = 0;
        }

        let value = FlatStateValue::try_from_slice(&value)?;
        if let FlatStateValue::Ref(r) = value {
            let (shard_uid, _) = decode_flat_state_db_key(&key).unwrap();
            keys_to_read.push(encode_flat_state_db_key(shard_uid, &r.hash.0));
        }
        if last_print.elapsed().as_secs() > 3 {
            println!(
                "FlatState: Copied {} keys, {} bytes, {} large keys to read from State",
                keys_copied,
                HumanBytes(bytes_copied as u64),
                keys_to_read.len()
            );
            last_print = std::time::Instant::now();
        }
    }
    update.commit()?;
    println!(
        "Done copying FlatState: Copied {} keys, {} bytes, {} large keys to read from State",
        keys_copied,
        HumanBytes(bytes_copied as u64),
        keys_to_read.len()
    );
    iter_handle.join().unwrap();

    let (tx, rx) = std::sync::mpsc::sync_channel(1000000);
    let iter_handle = {
        let old = old.clone();
        std::thread::spawn(move || {
            StoreParallelIterator::lookup_keys(
                old,
                DBCol::State,
                keys_to_read,
                6,
                move |key, value| {
                    tx.send((key.to_vec(), value.map(|value| value.to_vec()))).unwrap();
                },
                true,
            );
        })
    };

    let mut update = new.store_update();
    let mut keys_copied = 0;
    let mut bytes_copied = 0;
    let mut bytes_this_batch = 0;
    let mut last_print = std::time::Instant::now();
    for (key, value) in rx {
        let Some(value) = value else {
            anyhow::bail!("Key not found in State column: {}", hex::encode(&key));
        };
        update.set_raw_bytes(DBCol::State, &key, &value);
        keys_copied += 1;
        bytes_copied += key.len() + value.len();
        bytes_this_batch += key.len() + value.len();
        if bytes_this_batch > 100 * 1024 * 1024 {
            update.commit()?;
            update = new.store_update();
            bytes_this_batch = 0;
        }
        if last_print.elapsed().as_secs() > 3 {
            println!(
                "State: Copied {} keys, {} bytes",
                keys_copied,
                HumanBytes(bytes_copied as u64)
            );
            last_print = std::time::Instant::now();
        }
    }
    update.commit()?;
    iter_handle.join().unwrap();
    println!(
        "Done copying State: Copied {} keys, {} bytes",
        keys_copied,
        HumanBytes(bytes_copied as u64)
    );

    // Finally copy over some metadata columns
    let mut update = new.store_update();
    for item in old.iter_raw_bytes(DBCol::DbVersion) {
        let (key, value) = item?;
        update.set_raw_bytes(DBCol::DbVersion, &key, &value);
    }
    for item in old.iter_raw_bytes(DBCol::Misc) {
        let (key, value) = item?;
        update.set_raw_bytes(DBCol::Misc, &key, &value);
    }
    update.commit()?;
    println!("Done constructing trimmed database");

    Ok(())
}
