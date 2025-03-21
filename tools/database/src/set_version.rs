use clap::Parser;
use near_chain_configs::GenesisValidationMode;
use near_store::NodeStorage;
use std::path::Path;

use crate::utils::get_user_confirmation;

// TODO: remove this cmd once we have a proper way to rollback migration
#[derive(Parser)]
pub(crate) struct SetVersionCommand {
    /// Version to set
    #[clap(long)]
    version: u32,
}

impl SetVersionCommand {
    pub(crate) fn run(
        &self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(&home_dir, genesis_validation)?;
        let opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.archival_config(),
        );
        let storage = opener.open_unsafe()?;
        let store = storage.get_hot_store();

        println!("Current hot db version is: {:?}", store.get_db_version()?);
        if let Some(cold_store) = storage.get_cold_store() {
            println!("Current cold db version is: {:?}", cold_store.get_db_version()?);
        }

        if !get_user_confirmation(&format!(
            "WARNING: You are about to manually set the database version to {}.\n\
            It is not a rollback migration and it might break the database. Do it at your own risk!",
            self.version,
        )) {
            println!("Operation canceled.");
            return Ok(());
        }

        println!("Setting hot db version to {}... ", self.version);
        store.set_db_version(self.version)?;
        if let Some(cold_store) = storage.get_cold_store() {
            println!("Setting cold db version to {}... ", self.version);
            cold_store.set_db_version(self.version)?;
        }

        println!("Database version set to {}", self.version);
        Ok(())
    }
}
