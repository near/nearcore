use crate::utils::{open_rocksdb, open_rocksdb_cold, resolve_column};
use clap::Parser;
use near_store::{db::Database, Mode, Temperature};
use std::path::PathBuf;

#[derive(Parser)]
pub(crate) struct RunCompactionCommand {
    /// If specified only this column will compacted
    #[arg(short, long)]
    column: Option<String>,
    /// What store temperature should compaction run on. Allowed values are hot and cold but
    /// cold is only available when cold_store is configured.
    #[clap(long, short = 't', default_value = "hot")]
    store_temperature: Temperature,
}

impl RunCompactionCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let db = match self.store_temperature {
            Temperature::Hot => open_rocksdb(home, Mode::ReadWrite)?,
            Temperature::Cold => open_rocksdb_cold(home, Mode::ReadWrite)?,
        };
        if let Some(col_name) = &self.column {
            db.compact_column(resolve_column(col_name)?)?;
        } else {
            db.compact()?;
        }
        eprintln!("Compaction is finished!");
        Ok(())
    }
}
