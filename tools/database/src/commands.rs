use crate::adjust_database::ChangeDbKindCommand;
use crate::aggressive_trimming::AggressiveTrimmingCommand;
use crate::analyse_data_size_distribution::AnalyseDataSizeDistributionCommand;
use crate::analyse_gas_usage::AnalyseGasUsageCommand;
use crate::analyse_high_load::HighLoadStatsCommand;
use crate::analyze_delayed_receipt::AnalyzeDelayedReceiptCommand;
use crate::compact::RunCompactionCommand;
use crate::corrupt::CorruptStateSnapshotCommand;
use crate::make_snapshot::MakeSnapshotCommand;
use crate::memtrie::LoadMemTrieCommand;
use crate::run_migrations::RunMigrationsCommand;
use crate::state_perf::StatePerfCommand;
use crate::write_to_db::WriteCryptoHashCommand;
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

    /// Analyse gas usage in a chosen sequnce of blocks
    AnalyseGasUsage(AnalyseGasUsageCommand),

    /// Change DbKind of hot or cold db.
    ChangeDbKind(ChangeDbKindCommand),

    /// Run SST file compaction on database
    CompactDatabase(RunCompactionCommand),

    /// Corrupt the state snapshot.
    CorruptStateSnapshot(CorruptStateSnapshotCommand),

    /// Make snapshot of the database
    MakeSnapshot(MakeSnapshotCommand),

    /// Run migrations,
    RunMigrations(RunMigrationsCommand),

    /// Run performance test for State column reads.
    /// Uses RocksDB data specified via --home argument.
    StatePerf(StatePerfCommand),

    /// Loads an in-memory trie for research purposes.
    LoadMemTrie(LoadMemTrieCommand),
    /// Write CryptoHash to DB
    WriteCryptoHash(WriteCryptoHashCommand),
    /// Outputs stats that are needed to analise high load
    /// for a block range and account.
    HighLoadStats(HighLoadStatsCommand),
    // Analyze congestion through delayed receipts
    AnalyzeDelayedReceipt(AnalyzeDelayedReceiptCommand),

    AggressiveTrimming(AggressiveTrimmingCommand),
}

impl DatabaseCommand {
    pub fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        match &self.subcmd {
            SubCommand::AnalyseDataSizeDistribution(cmd) => cmd.run(home),
            SubCommand::AnalyseGasUsage(cmd) => cmd.run(home),
            SubCommand::ChangeDbKind(cmd) => cmd.run(home),
            SubCommand::CompactDatabase(cmd) => cmd.run(home),
            SubCommand::CorruptStateSnapshot(cmd) => cmd.run(home),
            SubCommand::MakeSnapshot(cmd) => {
                let near_config = nearcore::config::load_config(
                    &home,
                    near_chain_configs::GenesisValidationMode::UnsafeFast,
                )
                .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
                cmd.run(home, near_config.config.archive, &near_config.config.store)
            }
            SubCommand::RunMigrations(cmd) => cmd.run(home),
            SubCommand::StatePerf(cmd) => cmd.run(home),
            SubCommand::LoadMemTrie(cmd) => {
                let near_config = nearcore::config::load_config(
                    &home,
                    near_chain_configs::GenesisValidationMode::UnsafeFast,
                )
                .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
                cmd.run(near_config, home)
            }
            SubCommand::WriteCryptoHash(cmd) => cmd.run(home),
            SubCommand::HighLoadStats(cmd) => cmd.run(home),
            SubCommand::AnalyzeDelayedReceipt(cmd) => cmd.run(home),
            SubCommand::AggressiveTrimming(cmd) => cmd.run(home),
        }
    }
}
