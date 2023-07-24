use std::path::Path;

#[derive(clap::Args)]
pub(crate) struct RunMigrationsCommand {}

impl RunMigrationsCommand {
    pub(crate) fn run(
        &self,
        home_dir: &Path,
        near_config: &mut nearcore::NearConfig,
    ) -> anyhow::Result<()> {
        nearcore::open_storage(home_dir, near_config)?;
        Ok(())
    }
}
