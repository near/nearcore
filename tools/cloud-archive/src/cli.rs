use crate::status::StatusCmd;
use near_chain_configs::GenesisValidationMode;
use std::path::Path;

#[derive(clap::Parser)]
pub struct CloudArchiveCommand {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(clap::Parser)]
enum SubCommand {
    /// Show cloud archive head positions in external and local storage.
    Status(StatusCmd),
}

impl CloudArchiveCommand {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        match self.subcmd {
            SubCommand::Status(cmd) => cmd.run(home_dir, genesis_validation),
        }
    }
}
