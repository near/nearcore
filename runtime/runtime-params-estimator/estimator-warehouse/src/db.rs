//! Database connection and struct representing rows in the data tables.

use std::path::Path;

use chrono::NaiveDateTime;
use rusqlite::{params, Connection, Row};

use crate::Metric;

/// Wrapper around database connection
pub(crate) struct Db {
    conn: Connection,
}

impl Db {
    pub(crate) fn new(conn: Connection) -> Self {
        Self { conn }
    }

    /// Opens an existing SQLite Db or creates it
    pub(crate) fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        let init_sql = include_str!("init.sql");
        conn.execute_batch(init_sql)?;
        Ok(Self::new(conn))
    }
}

/// A single data row in the estimation table
#[derive(Debug, PartialEq)]
pub(crate) struct EstimationRow {
    /// Name of the estimation / parameter
    pub name: String,
    /// The estimation result converted to gas units
    pub gas: f64,
    /// Parameter for which this estimation is used
    pub parameter: Option<String>,
    /// The estimated time in nanoseconds, (if time-based estimation)
    pub wall_clock_time: Option<f64>,
    /// The number of operations counted (if icount-based estimation)
    pub icount: Option<f64>,
    /// The number of IO read bytes counted (if icount-based estimation)
    pub io_read: Option<f64>,
    /// The number of IO write bytes counted (if icount-based estimation)
    pub io_write: Option<f64>,
    /// For measurements that had some kind of inaccuracies or problems
    pub uncertain_reason: Option<String>,
    /// Which git commit this has been estimated on
    pub commit_hash: String,
}

/// A single data row in the parameter table
#[derive(Debug, PartialEq)]
pub(crate) struct ParameterRow {
    /// Name of the estimation / parameter
    pub name: String,
    /// The estimation result converted to gas units
    pub gas: f64,
    /// Protocol version for which the parameter is valid
    pub protocol_version: u32,
}

impl EstimationRow {
    const SELECT_ALL: &'static str =
        "name,gas,parameter,wall_clock_time,icount,io_read,io_write,uncertain_reason,commit_hash";
    pub fn get(db: &Db, name: &str, commit: &str, metric: Metric) -> anyhow::Result<Vec<Self>> {
        Ok(Self::get_any_metric(db, name, commit)?
            .into_iter()
            .filter(|row| row.is_metric(metric))
            .collect())
    }
    pub fn get_any_metric(db: &Db, name: &str, commit: &str) -> anyhow::Result<Vec<Self>> {
        let select = Self::SELECT_ALL;
        let mut stmt = db.conn.prepare(&format!(
            "SELECT {select} FROM estimation WHERE name = ?1 AND commit_hash = ?2;"
        ))?;
        let data = stmt
            .query_map([name, commit], Self::from_row)?
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;
        Ok(data)
    }
    pub(crate) fn insert(&self, db: &Db) -> anyhow::Result<()> {
        db.conn.execute(
            "INSERT INTO estimation(name,gas,parameter,wall_clock_time,icount,io_read,io_write,uncertain_reason,commit_hash) values (?1,?2,?3,?4,?,?6,?7,?8,?9)",
            params![
                self.name,
                self.gas,
                self.parameter,
                self.wall_clock_time,
                self.icount,
                self.io_read,
                self.io_write,
                self.uncertain_reason,
                self.commit_hash,
            ],
        )?;
        Ok(())
    }
    pub fn select_by_commit_and_metric(
        db: &Db,
        commit: &str,
        metric: Metric,
    ) -> anyhow::Result<Vec<Self>> {
        let select = Self::SELECT_ALL;
        let metric_condition = metric.condition();
        let mut stmt = db.conn.prepare(&format!(
            "SELECT {select} FROM estimation WHERE commit_hash = ?1 AND {metric_condition};"
        ))?;
        let data = stmt
            .query_map([commit], Self::from_row)?
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;
        Ok(data)
    }

    /// Returns one (commit_hash,date) tuple for each commit in store,
    /// optionally filtered by estimation metric. The output is sorted by the
    /// date, in ascending order. Note that the date is not the committed-date
    /// but instead the earliest date associated with an estimation of that
    /// commit, as recorded in the warehouse. This is usually the date the
    /// estimation has been performed.
    pub fn commits_sorted_by_date(
        db: &Db,
        metric: Option<Metric>,
    ) -> anyhow::Result<Vec<(String, NaiveDateTime)>> {
        let metric_condition = match metric {
            Some(m) => format!("WHERE {}", m.condition()),
            None => "".to_owned(),
        };
        let sql = format!("SELECT commit_hash,min(date) FROM estimation {metric_condition} GROUP BY commit_hash ORDER BY date ASC;");
        let mut stmt = db.conn.prepare(&sql)?;
        let data = stmt
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, NaiveDateTime>(1)?)))?
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;
        Ok(data)
    }
    pub fn count_by_metric(db: &Db, metric: Metric) -> anyhow::Result<u64> {
        let sql = match metric {
            Metric::ICount => "SELECT COUNT(*) FROM estimation WHERE icount IS NOT NULL;",
            Metric::Time => "SELECT COUNT(*) FROM estimation WHERE wall_clock_time IS NOT NULL;",
        };
        let count = db.conn.query_row::<i32, _, _>(sql, [], |row| row.get(0))?;
        Ok(count as u64)
    }
    pub fn last_updated(db: &Db, metric: Metric) -> anyhow::Result<Option<NaiveDateTime>> {
        let sql = match metric {
            Metric::ICount => "SELECT MAX(date) FROM estimation WHERE icount IS NOT NULL;",
            Metric::Time => "SELECT MAX(date) FROM estimation WHERE wall_clock_time IS NOT NULL;",
        };
        let dt = db.conn.query_row::<Option<NaiveDateTime>, _, _>(sql, [], |row| row.get(0))?;
        Ok(dt)
    }
    fn is_metric(&self, metric: Metric) -> bool {
        match metric {
            Metric::ICount => self.icount.is_some(),
            Metric::Time => self.icount.is_none() && self.wall_clock_time.is_some(),
        }
    }
    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            name: row.get(0)?,
            gas: row.get(1)?,
            parameter: row.get(2)?,
            wall_clock_time: row.get(3)?,
            icount: row.get(4)?,
            io_read: row.get(5)?,
            io_write: row.get(6)?,
            uncertain_reason: row.get(7)?,
            commit_hash: row.get(8)?,
        })
    }
}

impl ParameterRow {
    pub fn count(db: &Db) -> anyhow::Result<u64> {
        let sql = "SELECT COUNT(*) FROM parameter;";
        let count = db.conn.query_row::<u64, _, _>(sql, [], |row| row.get(0))?;
        Ok(count)
    }
    pub fn latest_protocol_version(db: &Db) -> anyhow::Result<Option<u32>> {
        let sql = "SELECT MAX(protocol_version) FROM parameter;";
        let max = db.conn.query_row::<Option<u32>, _, _>(sql, [], |row| row.get(0))?;
        Ok(max)
    }
}

impl Metric {
    fn condition(&self) -> &'static str {
        match self {
            Metric::ICount => "icount IS NOT NULL",
            Metric::Time => "wall_clock_time IS NOT NULL",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Db;
    use crate::import::ImportConfig;
    use chrono::{NaiveDate, NaiveDateTime};
    use rusqlite::{functions::FunctionFlags, Connection};

    impl Db {
        /// Create a new in-memory test database with a mocked `datetime()` function.
        pub(crate) fn test() -> Self {
            let conn = Connection::open_in_memory().unwrap();
            let init_sql = include_str!("init.sql");
            conn.execute_batch(init_sql).unwrap();
            let db = Self::new(conn);
            db.mock_time(
                NaiveDate::from_ymd_opt(2015, 5, 15).unwrap().and_hms_opt(11, 22, 33).unwrap(),
            );
            db
        }

        /// Create a new in-memory test database with data defined by the input.
        ///
        /// The test data is expected to come in blocks of JSON lines with a commit header.
        /// WAIT statements can be used as barriers to make sure DB timestamps differ.
        pub(crate) fn test_with_data(input: &str) -> Self {
            let db = Self::test();
            for block in input.split("\n\n") {
                if block.trim() == "WAIT" {
                    // Update mocked time to ensure the following data is considered to be later.
                    let mocked_now: NaiveDateTime =
                        db.conn.query_row("SELECT datetime('now')", [], |r| r.get(0)).unwrap();
                    db.mock_time(mocked_now + chrono::Duration::seconds(1));
                } else {
                    let (commit_hash, input) = block.split_once("\n").unwrap();
                    let conf = ImportConfig {
                        commit_hash: Some(commit_hash.to_string()),
                        protocol_version: None,
                    };
                    db.import_json_lines(&conf, input).unwrap();
                }
            }
            db
        }

        pub(crate) fn mock_time(&self, dt: NaiveDateTime) {
            let string_rep = dt.format("%Y-%m-%d %H:%M:%S").to_string();
            self.conn
                .create_scalar_function("datetime", 1, FunctionFlags::SQLITE_UTF8, move |_ctx| {
                    Ok(string_rep.clone())
                })
                .unwrap();
        }
    }
}
