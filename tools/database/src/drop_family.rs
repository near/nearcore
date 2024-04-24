use crate::utils::{open_rocksdb, resolve_column};
use clap::Parser;
use near_store::db::Database;
use std::path::PathBuf;

#[derive(Parser)]
pub(crate) struct DropColumnFamilyCommand {
    /// This column will dropped
    #[arg(short, long)]
    column: String,
}

impl DropColumnFamilyCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let mut db = open_rocksdb(home, near_store::Mode::ReadWrite)?;
        db.drop_column_family(resolve_column(&self.column)?);
        eprintln!("Compaction is finished!");
        Ok(())
    }
}
