use crate::utils::open_rocksdb;
use clap::Parser;
use near_store::db::Database;
use std::path::PathBuf;

#[derive(Parser)]
pub(crate) struct RunCompactionCommand {}

impl RunCompactionCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let db = open_rocksdb(home, near_store::Mode::ReadWrite)?;
        db.compact()?;
        eprintln!("Compaction is finished!");
        Ok(())
    }
}
