use indicatif::ParallelProgressIterator;
use near_chain_configs::GenesisConfig;
use near_database_tool::utils::{prepare_memtrie_state_trimming, write_state_column};
use near_store::{DBCol, Store};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

/// Trims the database for forknet by rebuilding the entire database with only the FlatState
/// and the small part of State that is needed for memtrie operations, i.e. large non-inlined
/// trie values as well as some trie nodes near the root.
///
/// Also copies over the basic DbVersion and Misc columns.
///
/// Does not mutate the old store, but populates the new store with the trimmed data.
///
/// Note that only the state in the FlatState is copied. All other state in FlatStateChanges is
/// ignored. For this reason, this is only suitable for forknet testing.
pub fn trim_database(old: Store, genesis_config: &GenesisConfig, new: Store) -> anyhow::Result<()> {
    let state_needed = prepare_memtrie_state_trimming(old.clone(), genesis_config, false)?;

    tracing::info!("Copying FlatState to new database...");
    state_needed
        .flat_state_ranges
        .par_iter()
        .progress_count(state_needed.flat_state_ranges.len() as u64)
        .for_each(|(start, end)| {
            let mut update = new.store_update();
            for item in old.iter_range(DBCol::FlatState, Some(start), Some(end)) {
                let (key, value) = item.unwrap();
                update.set_raw_bytes(DBCol::FlatState, &key, &value);
            }
            update.commit().unwrap();
        });
    tracing::info!("Done copying FlatState to new database.");

    tracing::info!("Writing important State column keys to new database...");
    write_state_column(&new, state_needed.state_entries)?;
    tracing::info!("Done writing State column to new database.");

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
