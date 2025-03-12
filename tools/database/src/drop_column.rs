use crate::utils::{open_rocksdb, resolve_column};
use clap::Parser;
use dialoguer::Confirm;
use std::path::PathBuf;

#[derive(Parser)]
pub(crate) struct DropColumnCommand {
    /// Column name, e.g. 'ChunkApplyStats'.
    #[clap(long)]
    column: String,
}

impl DropColumnCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        if !Confirm::new()
            .with_prompt(format!(
                "WARNING: You are about to drop the column '{}'.\n\
                That would break the database, unless you know what you are doing.\n\
                Also, the column may be automatically restored (empty) the next time you run neard.\n\
                Are you sure?",
                self.column
            ))
            .default(false)
            .interact()?
        {
            println!("Operation canceled.");
            return Ok(());
        }

        let mut db = open_rocksdb(home, near_store::Mode::ReadWrite)?;
        let column = resolve_column(&self.column)?;
        db.drop_column(column)?;
        println!("Dropped column: {}", self.column);
        Ok(())
    }
}
