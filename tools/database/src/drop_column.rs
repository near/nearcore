use crate::utils::{open_rocksdb, resolve_column};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
pub(crate) struct DropColumnCommand {
    /// Column name, e.g. 'ChunkApplyStats'.
    #[clap(long)]
    column: String,
}

impl DropColumnCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let mut db = open_rocksdb(home, near_store::Mode::ReadWrite)?;
        let column = resolve_column(&self.column)?;
        db.drop_column(column)?;
        println!("Dropped column: {}", self.column);
        Ok(())
    }
}
