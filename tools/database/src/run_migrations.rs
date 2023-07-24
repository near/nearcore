use std::path::Path;

#[derive(clap::Args)]
pub(crate) struct RunMigrationsCommand {}

impl RunMigrationsCommand {
    pub(crate) fn run(&self, home_dir: &Path) -> anyhow::Result<()> {
        let mut near_config = nearcore::config::load_config(
            &home_dir,
            near_chain_configs::GenesisValidationMode::UnsafeFast,
        )
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
        nearcore::open_storage(home_dir, &mut near_config)?;
        Ok(())
    }
}
