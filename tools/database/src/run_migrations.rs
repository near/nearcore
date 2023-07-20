use near_store::metadata::DbKind;
use near_store::{Mode, NodeStorage};
use nearcore::{migrations, NearConfig, open_storage};
use std::path::Path;

/// This can potentially support db specified not in config, but in command line.
/// `ChangeRelative { path: Path, archive: bool }`
/// But it is a pain to implement, because of all the current storage possibilities.
/// So, I'll leave it as a TODO(posvyatokum): implement relative path DbSelector.
/// This can be useful workaround for config modification.
#[derive(clap::Subcommand)]
enum DbSelector {
    ChangeHot,
    ChangeCold,
}

#[derive(clap::Args)]
pub(crate) struct RunMigrationsCommand { }

impl RunMigrationsCommand {
    pub(crate) fn run(&self, home_dir: &Path) -> anyhow::Result<()> {
        let mut near_config = nearcore::config::load_config(
            &home_dir,
            near_chain_configs::GenesisValidationMode::UnsafeFast,
        )
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
        open_storage(home_dir, &mut near_config)?;
        Ok(())
    }
}
