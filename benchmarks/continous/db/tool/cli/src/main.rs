use std::fs;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use orm::{check_connection, establish_connection, insert_ft_transfer};

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    match args.command {
        Command::CheckConnection => {
            let connection = &mut establish_connection()?;
            check_connection(connection)?;
        }
        Command::InsertFtTransfer { in_path } => {
            let file_content = fs::read_to_string(in_path)?;
            let new_ft_transfer = serde_json::from_str(&file_content)?;
            let connection = &mut establish_connection()?;
            insert_ft_transfer(connection, &new_ft_transfer)?;
        }
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[command(
    about = "A CLI to interact with the db storing contiuous benchmark data. Commands that connect to the db require the env var DATABASE_URL_CLI to be set in a format compatible with the diesel crate. Consider sourcing the dbprofile file in the repository.",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Connects to the db and runs a SELECT query to check if a connection can be established.
    CheckConnection,
    /// Insert data related to an ft transfer benchmark run.
    #[command(arg_required_else_help = true)]
    InsertFtTransfer {
        /// Path to a file that contains the [`NewFtTransfer`](orm::models::NewFtTransfer) to insert serialized as JSON.
        in_path: PathBuf,
    },
}
