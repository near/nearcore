use clap::Parser;
use near_store::db::Database;
use std::path::PathBuf;

use crate::utils::open_rocksdb;

#[derive(Parser)]
pub(crate) struct RunCompactionCommand {}

impl RunCompactionCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let db = open_rocksdb(home)?;
        db.compact()?;
        Ok(())
    }
}
