use clap::Parser;
use near_chain_configs::GenesisValidationMode;
use near_store::NodeStorage;
use near_store::metadata::DB_VERSION;
use std::path::Path;

use crate::utils::get_user_confirmation;

// TODO: remove this cmd once we have a proper way to rollback migration
#[derive(Parser)]
pub(crate) struct ResetVersionCommand;

impl ResetVersionCommand {
    pub(crate) fn run(
        &self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        if !get_user_confirmation(&format!(
            "WARNING: You are about to manually set the database version to {}.\n\
            It is not a rollback migration and it might break the database. Do it at your own risk!",
            DB_VERSION,
        )) {
            println!("Operation canceled.");
            return Ok(());
        }

        println!("Setting database version to: {DB_VERSION}");
        let near_config = nearcore::config::load_config(&home_dir, genesis_validation)?;
        let opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.archival_config(),
        );
        let storage = opener.open_unsafe()?;
        let store = storage.get_hot_store();
        store.set_db_version(DB_VERSION)?;
        if let Some(cold_store) = storage.get_cold_store() {
            cold_store.set_db_version(DB_VERSION)?;
        }

        println!("Database version set to {DB_VERSION}");
        Ok(())
    }
}
