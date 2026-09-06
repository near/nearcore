use anyhow::anyhow;
use near_store::DBCol;
use std::io::{self, Write};
use std::path::Path;
use strum::IntoEnumIterator;

pub(crate) fn get_user_confirmation(message: &str) -> bool {
    print!("{}\nAre you sure? (y/N): ", message);
    io::stdout().flush().expect("Failed to flush stdout");
    let mut input = String::new();
    io::stdin().read_line(&mut input).expect("Failed to read input");
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

pub(crate) fn open_rocksdb(
    home: &Path,
    mode: near_store::Mode,
) -> anyhow::Result<near_store::db::RocksDB> {
    let config = nearcore::config::Config::from_file_skip_validation(
        &home.join(nearcore::config::CONFIG_FILENAME),
    )?;
    let store_config = &config.store;
    // `StoreOpener` resolves a relative `store.path` against the home directory, so an
    // archival node whose config says `hot-data` means `<home>/hot-data`. Joining an
    // absolute path leaves it as it is.
    let db_path = home.join(store_config.path.as_deref().unwrap_or_else(|| Path::new("data")));
    let rocksdb =
        near_store::db::RocksDB::open(&db_path, store_config, mode, near_store::Temperature::Hot)?;
    Ok(rocksdb)
}

pub(crate) fn resolve_column(col_name: &str) -> anyhow::Result<DBCol> {
    DBCol::iter()
        .filter(|db_col| <&str>::from(db_col) == col_name)
        .next()
        .ok_or_else(|| anyhow!("column {col_name} does not exist"))
}
