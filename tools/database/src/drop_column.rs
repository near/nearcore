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
        let column = match resolve_column(&self.column) {
            Ok(col) => {
                println!("Column {} is recognized by the binary. Note that it will be automatically recreated each time neard is run.", self.column);
                Some(col)
            }
            Err(_) => {
                println!("Column {} is not recognized by the binary. Will attempt to remove it.", self.column);
                None
            }
        };
        db.drop_column(&self.column, column)?;
        println!("Dropped column: {}", self.column);
        Ok(())
    }
}
