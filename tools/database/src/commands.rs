use clap::Parser;
use std::path::PathBuf;
use crate::analyse::AnalyseDatabaseCommand;

#[derive(Parser)]
pub struct DatabaseCommand {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
enum SubCommand {
    /// Analyse data size distribution in RocksDB
    Analyse(AnalyseDatabaseCommand),
}

impl DatabaseCommand {
    pub fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        match &self.subcmd {
            SubCommand::Analyse(cmd) => cmd.run(home),
        }
    }
}
