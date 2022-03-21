use std::{io, path::PathBuf};

use clap::Clap;
use import::ImportInfo;
use rusqlite::Connection;

mod data_representations;
mod import;
mod queries;

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
}

#[derive(Clap, Debug)]
enum SubCommand {
    /// Read estimations in JSON format from STDIN and store it in the warehouse.
    Import,
}

fn main() -> anyhow::Result<()> {
    let cli_args = CliArgs::parse();

    let db = Connection::open(cli_args.db)?;
    let init_sql = include_str!("init.sql");
    db.execute(init_sql, [])?;

    match cli_args.cmd {
        SubCommand::Import => {
            let info = ImportInfo {
                commit_hash: cli_args.commit_hash,
                protocol_version: cli_args.protocol_version,
            };
            import::json_from_bufread(&db, &info, io::stdin().lock())?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use rusqlite::Connection;

    use crate::{
        data_representations::GasFeeRow,
        import::{json_from_bufread, ImportInfo},
    };

    #[test]
    fn test_import_time() {
        let input = r#"
            {"computed_in":{"nanos":826929296,"secs":0},"name":"LogBase","result":{"gas":441061948,"metric":"time","time_ns":441.061948,"uncertain_reason":null}}
            {"computed_in":{"nanos":983235753,"secs":0},"name":"LogByte","result":{"gas":2743748,"metric":"time","time_ns":2.7437486640625,"uncertain_reason":"HIGH-VARIANCE"}}
        "#;
        let expected = [
            GasFeeRow {
                name: "LogBase".to_owned(),
                gas: 441061948.0,
                wall_clock_time: Some(441.061948),
                icount: None,
                io_read: None,
                io_write: None,
                uncertain_reason: None,
                protocol_version: None,
                commit_hash: Some("53a3ccf3ef07".to_owned()),
            },
            GasFeeRow {
                name: "LogByte".to_owned(),
                gas: 2743748.0,
                wall_clock_time: Some(2.7437486640625),
                icount: None,
                io_read: None,
                io_write: None,
                uncertain_reason: Some("HIGH-VARIANCE".to_owned()),
                protocol_version: None,
                commit_hash: Some("53a3ccf3ef07".to_owned()),
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
            GasFeeRow {
                name: "ActionReceiptCreation".to_owned(),
                gas: 240650158750.0,
                wall_clock_time: None,
                icount: Some(1860478.51),
                io_read: Some(0.0),
                io_write: Some(1377.08),
                uncertain_reason: None,
                protocol_version: None,
                commit_hash: Some("53a3ccf3ef07".to_owned()),
            },
            GasFeeRow {
                name: "ApplyBlock".to_owned(),
                gas: 9059500000.0,
                wall_clock_time: None,
                icount: Some(71583.0),
                io_read: Some(0.0),
                io_write: Some(19.0),
                uncertain_reason: Some("HIGH-VARIANCE".to_owned()),
                protocol_version: None,
                commit_hash: Some("53a3ccf3ef07".to_owned()),
            },
        ];
        let info =
            ImportInfo { commit_hash: Some("53a3ccf3ef07".to_owned()), protocol_version: Some(0) };
        assert_import_icount(input, &info, &expected);
    }
    #[track_caller]
    fn assert_import_time(input: &str, info: &ImportInfo, expected_output: &[GasFeeRow]) {
        let db = Connection::open_in_memory().unwrap();
        let init_sql = include_str!("init.sql");
        db.execute(init_sql, []).unwrap();
        json_from_bufread(&db, info, input.as_bytes()).unwrap();
        let output = GasFeeRow::all_time_based(&db).unwrap();
        assert_eq!(expected_output, output);
    }
    #[track_caller]
    fn assert_import_icount(input: &str, info: &ImportInfo, expected_output: &[GasFeeRow]) {
        let db = Connection::open_in_memory().unwrap();
        let init_sql = include_str!("init.sql");
        db.execute(init_sql, []).unwrap();
        json_from_bufread(&db, info, input.as_bytes()).unwrap();
        let output = GasFeeRow::all_icount_based(&db).unwrap();
        assert_eq!(expected_output, output);
    }
}
