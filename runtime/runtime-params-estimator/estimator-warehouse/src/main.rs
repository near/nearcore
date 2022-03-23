use std::{io, path::PathBuf};

use check::{check, CheckConfig};
use clap::Clap;
use db::{EstimationRow, ParameterRow, DB};
use import::ImportConfig;

mod check;
mod db;
mod import;
mod zulip;

#[derive(Clap)]
struct CliArgs {
    #[clap(subcommand)]
    cmd: SubCommand,
    /// File path for either an existing SQLite3 DB or the path where a new DB
    /// will be created.
    #[clap(long, default_value = "db.sqlite")]
    db: PathBuf,
}

#[derive(Clap, Debug)]
enum SubCommand {
    /// Read estimations in JSON format from STDIN and store it in the warehouse.
    Import(ImportConfig),
    /// Compares parameters, estimations, and how estimations changed over time.
    /// Reports any deviations from the norm to STDOUT. Combine with `--zulip`
    /// to send notifications to a Zulip stream
    Check(CheckConfig),
    /// Prints a summary of the current data in the warehouse.
    Stats,
}

fn main() -> anyhow::Result<()> {
    let cli_args = CliArgs::parse();
    let db = DB::open(&cli_args.db)?;

    match cli_args.cmd {
        SubCommand::Import(config) => {
            db.import_json_lines(&config, io::stdin().lock())?;
        }
        SubCommand::Check(config) => {
            check(&db, &config)?;
        }
        SubCommand::Stats => {
            print_stats(&db)?;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ArgEnum)]
enum Metric {
    #[clap(name = "icount")]
    ICount,
    Time,
}

fn print_stats(db: &DB) -> anyhow::Result<()> {
    eprintln!("");
    eprintln!("{:=^72}", " Warehouse statistics ");
    eprintln!("");
    eprintln!("{:>24}{:>24}{:>24}", "metric", "records", "last updated");
    eprintln!("{:>24}{:>24}{:>24}", "------", "-------", "------------");
    eprintln!(
        "{:>24}{:>24}{:>24}",
        "icount",
        EstimationRow::count_by_metric(&db, Metric::ICount)?,
        EstimationRow::last_updated(&db, Metric::ICount)?
            .map(|dt| dt.to_string())
            .as_deref()
            .unwrap_or("never")
    );
    eprintln!(
        "{:>24}{:>24}{:>24}",
        "time",
        EstimationRow::count_by_metric(&db, Metric::Time)?,
        EstimationRow::last_updated(&db, Metric::Time)?
            .map(|dt| dt.to_string())
            .as_deref()
            .unwrap_or("never")
    );
    eprintln!(
        "{:>24}{:>24}{:>24}",
        "parameter",
        ParameterRow::count(&db)?,
        ParameterRow::latest_protocol_version(&db)?
            .map(|version| format!("v{version}"))
            .as_deref()
            .unwrap_or("never")
    );
    eprintln!("");
    eprintln!("{:=^72}", " END STATS ");

    Ok(())
}
