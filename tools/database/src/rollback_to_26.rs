use clap::Parser;
use near_chain_configs::GenesisValidationMode;
use std::path::Path;

use crate::utils::get_user_confirmation;

/// Rollback the database from format used by neard 2.7 to the format used by neard 2.6.
/// Sets DB version to 44.
#[derive(Parser)]
pub(crate) struct RollbackTo26Command {
    /// Don't ask for confirmation - use this flag to skip the confirmation prompt.
    #[clap(long, action)]
    no_confirmation: bool,
}

impl RollbackTo26Command {
    pub(crate) fn run(
        &self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let config = nearcore::config::load_config(&home_dir, genesis_validation)?;

        let get_confirmation: Box<dyn FnOnce() -> bool> = if self.no_confirmation {
            Box::new(|| true)
        } else {
            Box::new(|| {
                get_user_confirmation(
                    "You are about to roll back the database from format used by neard 2.7 to the format used by neard 2.6.
Do not interrupt the rollback operation, stopping the process in the middle of it could corrupt the database.
Do you want to continue?",
                )
            })
        };

        near_store::rollback_database_from_27_to_26(
            home_dir,
            &config.config.store,
            config.config.archival_config(),
            get_confirmation,
        )?;
        Ok(())
    }
}
