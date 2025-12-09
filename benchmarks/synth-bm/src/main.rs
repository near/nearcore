use clap::{Parser, Subcommand};

mod account;
use account::{CreateSubAccountsArgs, create_sub_accounts};
mod block_service;
mod contract;
use contract::BenchmarkMpcSignArgs;
mod metrics;
mod native_transfer;
mod rpc;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Creates sub accounts for the signer.
    CreateSubAccounts(CreateSubAccountsArgs),
    BenchmarkNativeTransfers(native_transfer::BenchmarkArgs),
    BenchmarkMpcSign(BenchmarkMpcSignArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    // TODO increase file descriptor limit, if required.

    match &cli.command {
        Commands::CreateSubAccounts(args) => {
            create_sub_accounts(args).await?;
        }
        Commands::BenchmarkNativeTransfers(args) => {
            native_transfer::benchmark(args).await?;
        }
        Commands::BenchmarkMpcSign(args) => {
            contract::benchmark_mpc_sign(args).await?;
        }
    }
    Ok(())
}
