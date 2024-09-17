use std::fs;
use std::path::Path;

use anyhow::anyhow;
use near_store::{DBCol, NodeStorage, Store};
use strum::IntoEnumIterator;

/// Opens RocksDB with Hot data from the `home` directory.
/// This function ignores cold storage in the same home directory; to open RocksDB with cold data, use `cold_rocksdb_cold`.
pub(crate) fn open_rocksdb(
    home: &Path,
    mode: near_store::Mode,
) -> anyhow::Result<near_store::db::RocksDB> {
    let config = nearcore::config::Config::from_file_skip_validation(
        &home.join(nearcore::config::CONFIG_FILENAME),
    )?;
    let store_config = &config.store;
    // TODO: Use DBOpener to avoid duplicating the way we open RocksDB.
    let db_path = home.join(store_config.path.as_ref().cloned().unwrap_or_else(|| home.join("data")));
    let rocksdb =
        near_store::db::RocksDB::open(&db_path, store_config, mode, near_store::Temperature::Hot)?;
    Ok(rocksdb)
}

/// Opens RocksDB with Cold data from the `home` directory.
/// This function ignores hot storage in the same home directory; to open RocksDB with hot data, use `cold_rocksdb`.
pub(crate) fn open_rocksdb_cold(
    home: &Path,
    mode: near_store::Mode,
) -> anyhow::Result<near_store::db::RocksDB> {
    let config = nearcore::config::Config::from_file_skip_validation(
        &home.join(nearcore::config::CONFIG_FILENAME),
    )?;
    let store_config = config.cold_store.as_ref().ok_or_else(|| {
        anyhow!("Cold store is not configured, please configure it in the config file")
    })?;
    // TODO: Use DBOpener to avoid duplicating the way we open RocksDB.
    let db_path = home.join(store_config.path.as_ref().cloned().unwrap_or_else(|| home.join("cold-data")));
    let rocksdb =
        near_store::db::RocksDB::open(&db_path, store_config, mode, near_store::Temperature::Cold)?;
    Ok(rocksdb)
}

pub(crate) fn open_state_snapshot(home: &Path, mode: near_store::Mode) -> anyhow::Result<Store> {
    let config = nearcore::config::Config::from_file_skip_validation(
        &home.join(nearcore::config::CONFIG_FILENAME),
    )?;
    let store_config = &config.store;
    let db_path = store_config.path.as_ref().cloned().unwrap_or_else(|| home.join("data"));

    let state_snapshot_dir = db_path.join("state_snapshot");
    let snapshots: Result<Vec<_>, _> = fs::read_dir(state_snapshot_dir)?.into_iter().collect();
    let snapshots = snapshots?;
    let &[snapshot_dir] = &snapshots.as_slice() else {
        return Err(anyhow!("found more than one snapshot"));
    };

    let path = snapshot_dir.path();
    println!("state snapshot path {path:?}");

    let opener = NodeStorage::opener(&path, false, &store_config, None);
    let storage = opener.open_in_mode(mode)?;
    let store = storage.get_hot_store();

    Ok(store)
}

pub(crate) fn resolve_column(col_name: &str) -> anyhow::Result<DBCol> {
    DBCol::iter()
        .filter(|db_col| <&str>::from(db_col) == col_name)
        .next()
        .ok_or_else(|| anyhow!("column {col_name} does not exist"))
}
