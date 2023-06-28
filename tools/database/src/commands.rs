use crate::analyse_data_size_distribution::AnalyseDataSizeDistributionCommand;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
pub struct DatabaseCommand {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
enum SubCommand {
    /// Analyse data size distribution in RocksDB
    AnalyseDataSizeDistribution(AnalyseDataSizeDistributionCommand),
}

impl DatabaseCommand {
    pub fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        match &self.subcmd {
            SubCommand::AnalyseDataSizeDistribution(cmd) => cmd.run(home),
        }
    }
}
