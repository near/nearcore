use std::{io, path::PathBuf};

use check::{check, CheckConfig};
use clap::Clap;
use db::DB;
use import::ImportInfo;

use crate::db::{EstimationRow, ParameterRow};

mod check;
mod db;
mod import;

#[derive(Clap)]
struct CliArgs {
    #[clap(subcommand)]
    cmd: SubCommand,
    /// File path for either an existing SQLite3 DB or the path where a new DB
    /// will be created.
    #[clap(long, default_value = "db.sqlite")]
    db: PathBuf,
    /// For importing estimation data, which source code commit it should be associated with.
    #[clap(long)]
    commit_hash: Option<String>,
    /// For importing parameter values, which protocol version it should be associated with.
    #[clap(long)]
    protocol_version: Option<u32>,
    /// <domain:stream> (for example --zulip near.zulipchat.com:mystream)
    /// Send notifications from checks to specified server and stream.
    #[clap(long)]
    zulip: Option<String>,
}

#[derive(Clap, Debug)]
enum SubCommand {
    /// Read estimations in JSON format from STDIN and store it in the warehouse.
    Import,
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
        SubCommand::Import => {
            let info = ImportInfo {
                commit_hash: cli_args.commit_hash,
                protocol_version: cli_args.protocol_version,
            };
            db.import_json_lines(&info, io::stdin().lock())?;
        }
        SubCommand::Check(check_config) => {
            check(&db, &check_config)?;
        }
        SubCommand::Stats => {
            print_stats(&db)?;
        }
    }

    Ok(())
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

#[derive(Debug, Clone, Copy, PartialEq, clap::ArgEnum)]
enum Metric {
    #[clap(name = "icount")]
    ICount,
    Time,
}

#[cfg(test)]
mod test {
    use crate::{
        db::{EstimationRow, DB},
        import::ImportInfo,
    };

    #[test]
    fn test_import_time() {
        let input = r#"
            {"computed_in":{"nanos":826929296,"secs":0},"name":"LogBase","result":{"gas":441061948,"metric":"time","time_ns":441.061948,"uncertain_reason":null}}
            {"computed_in":{"nanos":983235753,"secs":0},"name":"LogByte","result":{"gas":2743748,"metric":"time","time_ns":2.7437486640625,"uncertain_reason":"HIGH-VARIANCE"}}
        "#;
        let expected = [
            EstimationRow {
                name: "LogBase".to_owned(),
                gas: 441061948.0,
                parameter: None,
                wall_clock_time: Some(441.061948),
                icount: None,
                io_read: None,
                io_write: None,
                uncertain_reason: None,
                commit_hash: "53a3ccf3ef07".to_owned(),
            },
            EstimationRow {
                name: "LogByte".to_owned(),
                gas: 2743748.0,
                parameter: None,
                wall_clock_time: Some(2.7437486640625),
                icount: None,
                io_read: None,
                io_write: None,
                uncertain_reason: Some("HIGH-VARIANCE".to_owned()),
                commit_hash: "53a3ccf3ef07".to_owned(),
            },
        ];
        let info =
            ImportInfo { commit_hash: Some("53a3ccf3ef07".to_owned()), protocol_version: Some(0) };
        assert_import_time(input, &info, &expected);
    }
    #[test]
    fn test_import_icount() {
        let input = r#"
        {"computed_in":{"nanos":107762511,"secs":17},"name":"ActionReceiptCreation","result":{"gas":240650158750,"instructions":1860478.51,"io_r_bytes":0.0,"io_w_bytes":1377.08,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":50472,"secs":0},"name":"ApplyBlock","result":{"gas":9059500000,"instructions":71583.0,"io_r_bytes":0.0,"io_w_bytes":19.0,"metric":"icount","uncertain_reason":"HIGH-VARIANCE"}}
        "#;
        let expected = [
            EstimationRow {
                name: "ActionReceiptCreation".to_owned(),
                gas: 240650158750.0,
                parameter: None,
                wall_clock_time: None,
                icount: Some(1860478.51),
                io_read: Some(0.0),
                io_write: Some(1377.08),
                uncertain_reason: None,
                commit_hash: "53a3ccf3ef07".to_owned(),
            },
            EstimationRow {
                name: "ApplyBlock".to_owned(),
                gas: 9059500000.0,
                parameter: None,
                wall_clock_time: None,
                icount: Some(71583.0),
                io_read: Some(0.0),
                io_write: Some(19.0),
                uncertain_reason: Some("HIGH-VARIANCE".to_owned()),
                commit_hash: "53a3ccf3ef07".to_owned(),
            },
        ];
        let info =
            ImportInfo { commit_hash: Some("53a3ccf3ef07".to_owned()), protocol_version: Some(0) };
        assert_import_icount(input, &info, &expected);
    }
    #[track_caller]
    fn assert_import_time(input: &str, info: &ImportInfo, expected_output: &[EstimationRow]) {
        let db = DB::test();
        db.import_json_lines(info, input.as_bytes()).unwrap();
        let output = EstimationRow::all_time_based(&db).unwrap();
        assert_eq!(expected_output, output);
    }
    #[track_caller]
    fn assert_import_icount(input: &str, info: &ImportInfo, expected_output: &[EstimationRow]) {
        let db = DB::test();
        db.import_json_lines(info, input.as_bytes()).unwrap();
        let output = EstimationRow::all_icount_based(&db).unwrap();
        assert_eq!(expected_output, output);
    }
}
