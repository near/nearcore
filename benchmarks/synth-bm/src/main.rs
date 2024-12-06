use clap::{Parser, Subcommand};

mod account;
use account::{create_sub_accounts, CreateSubAccountsArgs};
mod block_service;
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    // TODO increase file descriptor limit, if required.

    match &cli.command {
        Commands::CreateSubAccounts(args) => {
            create_sub_accounts(args).await?;
        }
        Commands::BenchmarkNativeTransfers(args) => {
            native_transfer::benchmark(args).await?;
        }
    }
    Ok(())
}
