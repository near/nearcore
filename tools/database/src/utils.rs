use std::path::Path;

pub(crate) fn open_rocksdb(home: &Path) -> anyhow::Result<near_store::db::RocksDB> {
    let config = nearcore::config::Config::from_file_skip_validation(
        &home.join(nearcore::config::CONFIG_FILENAME),
    )?;
    let store_config = &config.store;
    let db_path = store_config.path.as_ref().cloned().unwrap_or_else(|| home.join("data"));
    let rocksdb = near_store::db::RocksDB::open(
        &db_path,
        store_config,
        near_store::Mode::ReadOnly,
        near_store::Temperature::Hot,
    )?;
    Ok(rocksdb)
}
