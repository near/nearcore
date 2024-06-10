use anyhow::Context;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};

use orm::{check_connection, establish_connection, insert_ft_transfer, models::NewFtTransfer};

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    match args.command {
        Command::CheckConnection => {
            let connection = &mut establish_connection()?;
            check_connection(connection)?;
        }
        Command::InsertFtTransfer {
            time,
            git_commit_hash,
            git_commit_time,
            num_nodes,
            node_hardware,
            num_traffic_gen_machines,
            disjoint_workloads,
            num_shards,
            num_unique_users,
            size_state_bytes,
            tps,
            total_transactions,
        } => {
            let connection = &mut establish_connection()?;
            let node_hardware: Vec<&str> = node_hardware.iter().map(String::as_str).collect();
            let new_ft_transfer = NewFtTransfer {
                time,
                git_commit_hash: &git_commit_hash,
                git_commit_time,
                num_nodes,
                node_hardware: &node_hardware,
                num_traffic_gen_machines,
                disjoint_workloads,
                num_shards,
                num_unique_users,
                size_state_bytes,
                tps,
                total_transactions,
            };
            insert_ft_transfer(connection, &new_ft_transfer)?;
        }
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[command(
    about = "A CLI to interact with the db storing contiouos benchmark data",
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
        /// String representation of UTC datetime when the benchmark was run, e.g. '2024-06-07T11:30:44Z'
        #[arg(long, value_parser = parse_datetime_utc)]
        time: DateTime<Utc>,
        #[arg(long)]
        git_commit_hash: String,
        #[arg(long, value_parser = parse_datetime_utc)]
        git_commit_time: DateTime<Utc>,
        #[arg(long)]
        num_nodes: i32,
        #[arg(long)]
        node_hardware: Vec<String>,
        #[arg(long)]
        num_traffic_gen_machines: i32,
        #[arg(long)]
        disjoint_workloads: bool,
        #[arg(long)]
        num_shards: i32,
        #[arg(long)]
        num_unique_users: i32,
        #[arg(long)]
        size_state_bytes: i32,
        #[arg(long)]
        tps: i32,
        #[arg(long)]
        total_transactions: i32,
    },
}

fn parse_datetime_utc(src: &str) -> anyhow::Result<DateTime<Utc>> {
    src.parse().with_context(|| format!("Failed to parse UTC datetime from {src}"))
}
