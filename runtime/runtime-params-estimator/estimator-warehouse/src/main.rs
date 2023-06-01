use check::{check, CheckConfig};
use db::{Db, EstimationRow, ParameterRow};
use estimate::{run_estimation, EstimateConfig};
use import::ImportConfig;
use std::fmt::Write;
use std::io::{self, Read};
use std::path::PathBuf;

mod check;
mod db;
mod estimate;
mod import;
mod zulip;

#[derive(clap::Parser)]
struct CliArgs {
    #[clap(subcommand)]
    cmd: SubCommand,
    /// File path for either an existing SQLite3 DB or the path where a new DB
    /// will be created.
    #[clap(long, default_value = "db.sqlite")]
    db: PathBuf,
}

#[derive(clap::Subcommand, Debug)]
enum SubCommand {
    /// Call runtime-params-estimator for all metrics and import the results.
    Estimate(EstimateConfig),
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
    let cli_args: CliArgs = clap::Parser::parse();
    let db = Db::open(&cli_args.db)?;

    match cli_args.cmd {
        SubCommand::Estimate(config) => {
            run_estimation(&db, &config)?;
        }
        SubCommand::Import(config) => {
            let mut buf = String::new();
            io::stdin().read_to_string(&mut buf)?;
            db.import_json_lines(&config, &buf)?;
        }
        SubCommand::Check(config) => {
            check(&db, &config)?;
        }
        SubCommand::Stats => {
            let stats = generate_stats(&db)?;
            eprintln!("{stats}");
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
enum Metric {
    #[clap(name = "icount")]
    ICount,
    Time,
}

fn generate_stats(db: &Db) -> anyhow::Result<String> {
    let mut buf = String::new();
    writeln!(&mut buf)?;
    writeln!(&mut buf, "{:=^72}", " Warehouse statistics ")?;
    writeln!(&mut buf)?;
    writeln!(&mut buf, "{:>24}{:>24}{:>24}", "metric", "records", "last updated")?;
    writeln!(&mut buf, "{:>24}{:>24}{:>24}", "------", "-------", "------------")?;
    writeln!(
        &mut buf,
        "{:>24}{:>24}{:>24}",
        "icount",
        EstimationRow::count_by_metric(db, Metric::ICount)?,
        EstimationRow::last_updated(db, Metric::ICount)?
            .map(|dt| dt.to_string())
            .as_deref()
            .unwrap_or("never")
    )?;
    writeln!(
        &mut buf,
        "{:>24}{:>24}{:>24}",
        "time",
        EstimationRow::count_by_metric(db, Metric::Time)?,
        EstimationRow::last_updated(db, Metric::Time)?
            .map(|dt| dt.to_string())
            .as_deref()
            .unwrap_or("never")
    )?;
    writeln!(
        &mut buf,
        "{:>24}{:>24}{:>24}",
        "parameter",
        ParameterRow::count(db)?,
        ParameterRow::latest_protocol_version(db)?
            .map(|version| format!("v{version}"))
            .as_deref()
            .unwrap_or("never")
    )?;
    writeln!(&mut buf)?;
    writeln!(&mut buf, "{:=^72}", " END STATS ")?;

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::generate_stats;
    use crate::db::Db;

    #[test]
    fn test_stats() {
        let input = r#"
        ae7f1cd2
        {"computed_in":{"nanos":222,"secs":1},"name":"LogBase","result":{"gas":1002000000.0,"time_ns":1002,"metric":"time","uncertain_reason":null}}
        {"computed_in":{"nanos":671,"secs":18},"name":"LogBase","result":{"gas":1003000000.0,"instructions":8024.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}

        be7f1cd2
        {"computed_in":{"nanos":38,"secs":16},"name":"LogBase","result":{"gas":4000000000.0,"instructions":32000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":38,"secs":16},"name":"LogBase","result":{"gas":4000000000.0,"time_ns":4000,"metric":"time","uncertain_reason":null}}
        {"computed_in":{"nanos":637,"secs":17},"name":"LogByte","result":{"gas":20100000.0,"instructions":160.8,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}

        ce7f1cd2
        {"computed_in":{"nanos":988,"secs":51},"name":"LogBase","result":{"gas":10000000000.0,"time_ns":10000,"metric":"time","uncertain_reason":null}}
        {"computed_in":{"nanos":441,"secs":54},"name":"LogByte","result":{"gas":20000000.0,"instructions":160.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}"#;

        let db = Db::test_with_data(input);
        insta::assert_snapshot!(generate_stats(&db).unwrap());
    }
}
