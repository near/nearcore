use crate::utils::{get_user_confirmation, resolve_column};
use clap::Parser;
use near_chain_configs::GenesisValidationMode;
use near_store::DBCol;
use nearcore::load_config;
use std::path::PathBuf;

// TODO: remove this cmd once we have a proper way to rollback migration
#[derive(Parser)]
pub(crate) struct DropColumnCommand {
    /// Column name, e.g. 'ChunkApplyStats'.
    #[clap(long, required = true)]
    column: Vec<String>,
}

impl DropColumnCommand {
    pub(crate) fn run(
        &self,
        home_dir: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let unwanted_columns: Vec<DBCol> =
            self.column.iter().map(|col| resolve_column(col)).collect::<Result<Vec<_>, _>>()?;

        if !get_user_confirmation(&format!(
            "WARNING: You are about to drop columns '{:?}'.\n\
            That would break the database, unless you know what you are doing.\n\
            Also, columns may be automatically restored (empty) the next time you run neard.",
            self.column
        )) {
            println!("Operation canceled.");
            return Ok(());
        }

        println!("Dropping columns: {unwanted_columns:?}");
        let near_config = load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {e:#}"));

        near_store::clear_columns(
            home_dir,
            &near_config.config.store,
            near_config.config.archival_config(),
            &unwanted_columns,
            false,
        )?;
        println!("Dropped columns");
        Ok(())
    }
}
