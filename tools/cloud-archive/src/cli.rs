use crate::historical_reader::BootstrapCmd;
use crate::recent_reader::FollowCmd;
use crate::status::StatusCmd;
use crate::utils::ServeCmd;
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
    /// Build a local store from bucket data for a given height range.
    Bootstrap(BootstrapCmd),
    /// Run the recent reader: pull from the bucket and answer queries from the local store.
    Follow(FollowCmd),
    /// Answer queries from a local store nothing is writing to.
    Serve(ServeCmd),
}

impl CloudArchiveCommand {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        match self.subcmd {
            SubCommand::Status(cmd) => cmd.run(home_dir, genesis_validation),
            SubCommand::Bootstrap(cmd) => cmd.run(home_dir, genesis_validation),
            SubCommand::Follow(cmd) => cmd.run(home_dir, genesis_validation),
            SubCommand::Serve(cmd) => cmd.run(home_dir, genesis_validation),
        }
    }
}
