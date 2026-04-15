use crate::bootstrap_reader::BootstrapReaderCmd;
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
    /// Bootstrap local store from cloud-archived block data.
    BootstrapReader(BootstrapReaderCmd),
}

impl CloudArchiveCommand {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        match self.subcmd {
            SubCommand::Status(cmd) => cmd.run(home_dir, genesis_validation),
            SubCommand::BootstrapReader(cmd) => cmd.run(home_dir, genesis_validation),
        }
    }
}
