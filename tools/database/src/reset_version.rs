use clap::Parser;
use near_chain_configs::GenesisValidationMode;
use near_store::NodeStorage;
use near_store::metadata::DB_VERSION;
use std::path::Path;

#[derive(Parser)]
pub(crate) struct ResetVersionCommand;

impl ResetVersionCommand {
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
        store.set_db_version(DB_VERSION)?;
        println!("Database version set to {DB_VERSION}");
        Ok(())
    }
}
