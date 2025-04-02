use clap::Parser;
use near_chain_configs::GenesisValidationMode;
use std::path::Path;

/// Rollback the database from format used by neard 2.6 to the format used by neard 2.5.
/// Removes ChunkApplyStats column and sets DB version to 43.
#[derive(Parser)]
pub(crate) struct RollbackTo25Command;

impl RollbackTo25Command {
    pub(crate) fn run(
        &self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let config = nearcore::config::load_config(&home_dir, genesis_validation)?;
        near_store::rollback_database_from_26_to_25(
            home_dir,
            &config.config.store,
            config.config.archival_config(),
        )?;
        Ok(())
    }
}
