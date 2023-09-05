use crate::utils::{open_rocksdb, resolve_column};
use clap::Parser;
use near_store::db::Database;
use std::path::PathBuf;

#[derive(Parser)]
pub(crate) struct RunCompactionCommand {
    /// If specified only this column will compacted
    #[arg(short, long)]
    column: Option<String>,
}

impl RunCompactionCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let db = open_rocksdb(home, near_store::Mode::ReadWrite)?;
        if let Some(col_name) = &self.column {
            db.compact_column(resolve_column(col_name)?)?;
        } else {
            db.compact()?;
        }
        eprintln!("Compaction is finished!");
        Ok(())
    }
}
