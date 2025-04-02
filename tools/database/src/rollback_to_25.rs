use clap::Parser;
use near_chain_configs::GenesisValidationMode;
use std::path::Path;

use crate::utils::get_user_confirmation;

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

        let get_confirmation = || {
            get_user_confirmation(
                "You are about to roll back the database from format used by neard 2.6 to the format used by neard 2.5.
                Do not interrupt the rollback operation, stopping the process in the middle of it could corrupt the database.
                Do you want to continue?",
            )
        };

        near_store::rollback_database_from_26_to_25(
            home_dir,
            &config.config.store,
            config.config.archival_config(),
            get_confirmation,
        )?;
        Ok(())
    }
}
