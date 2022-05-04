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
mod test {
    use crate::db::{Db, EstimationRow};
    use crate::Metric;
    use chrono::NaiveDateTime;
    use rusqlite::params;

    #[track_caller]
    fn check_get_single(db: &Db, name: &str, commit: &str, metric: Metric, expected_gas: f64) {
        let row = &EstimationRow::get(db, name, commit, metric).unwrap()[0];
        assert_eq!(row.name, name);
        assert_eq!(row.commit_hash, commit);
        assert!(row.is_metric(metric));
        assert_eq!(row.gas, expected_gas);
    }

    #[test]
    fn test_get() {
        let db = Db::test();
        for metric in [Metric::Time, Metric::ICount] {
            EstimationRow::insert_test_data_set(&db, "LogBase", 1e9, 1e6, metric, 3);
            EstimationRow::insert_test_data_set(&db, "LogByte", 2e7, 5e4, metric, 3);
        }

        check_get_single(&db, "LogBase", "0001beef", Metric::ICount, 1_001e6);
        check_get_single(&db, "LogBase", "0000beef", Metric::ICount, 1_000e6);
        check_get_single(&db, "LogBase", "0002beef", Metric::ICount, 1_002e6);
        check_get_single(&db, "LogBase", "0001beef", Metric::Time, 1_001e6);
        check_get_single(&db, "LogBase", "0000beef", Metric::Time, 1_000e6);
        check_get_single(&db, "LogBase", "0002beef", Metric::Time, 1_002e6);

        check_get_single(&db, "LogByte", "0000beef", Metric::ICount, 20_000_000.0);
        check_get_single(&db, "LogByte", "0001beef", Metric::ICount, 20_050_000.0);
        check_get_single(&db, "LogByte", "0002beef", Metric::ICount, 20_100_000.0);
        check_get_single(&db, "LogByte", "0000beef", Metric::Time, 20_000_000.0);
        check_get_single(&db, "LogByte", "0001beef", Metric::Time, 20_050_000.0);
        check_get_single(&db, "LogByte", "0002beef", Metric::Time, 20_100_000.0);
    }

    #[track_caller]
    fn check_get_any_metric(
        db: &Db,
        name: &str,
        commit: &str,
        expected_metric_gas: &[(Metric, f64)],
    ) {
        let rows = &EstimationRow::get_any_metric(db, name, commit).unwrap();
        assert_eq!(rows.len(), expected_metric_gas.len());

        for row in rows {
            assert_eq!(row.name, name);
            assert_eq!(row.commit_hash, commit);
            assert!(expected_metric_gas.contains(&(row.metric(), row.gas)))
        }
    }

    #[test]
    fn test_get_any_metric() {
        let db = Db::test();
        EstimationRow::insert_test_data_set(&db, "LogBase", 1e9, 1e6, Metric::ICount, 2);
        EstimationRow::insert_test_data_set(&db, "LogBase", 10e9, 1e6, Metric::Time, 2);
        // extra data, not actually read
        EstimationRow::insert_test_data_set(&db, "LogByte", 2e7, 5e4, Metric::ICount, 10);

        check_get_any_metric(
            &db,
            "LogBase",
            "0000beef",
            &[(Metric::ICount, 1_000e6), (Metric::Time, 10_000e6)],
        );
        check_get_any_metric(
            &db,
            "LogBase",
            "0001beef",
            &[(Metric::ICount, 1_001e6), (Metric::Time, 10_001e6)],
        );
    }

    #[track_caller]
    fn check_select_by_commit_and_metric(
        db: &Db,
        commit: &str,
        metric: Metric,
        expected_name_gas: &[(&str, f64)],
    ) {
        let rows = &EstimationRow::select_by_commit_and_metric(db, commit, metric).unwrap();
        assert_eq!(rows.len(), expected_name_gas.len());

        for row in rows {
            assert!(row.is_metric(metric));
            assert_eq!(row.commit_hash, commit);
            assert!(expected_name_gas.contains(&(&row.name, row.gas)))
        }
    }

    #[test]
    fn test_select_by_commit_and_metric() {
        let db = Db::test();
        EstimationRow::insert_test_data_set(&db, "LogBase", 1e9, 1e6, Metric::ICount, 3);
        EstimationRow::insert_test_data_set(&db, "LogByte", 2e7, 5e4, Metric::ICount, 3);
        EstimationRow::insert_test_data_set(&db, "LogBase", 10e9, 1e6, Metric::Time, 1);

        check_select_by_commit_and_metric(
            &db,
            "0000beef",
            Metric::ICount,
            &[("LogBase", 1_000_e6), ("LogByte", 20_e6)],
        );
        check_select_by_commit_and_metric(
            &db,
            "0002beef",
            Metric::ICount,
            &[("LogBase", 1_002_e6), ("LogByte", 20_100_e3)],
        );
        check_select_by_commit_and_metric(&db, "0000beef", Metric::Time, &[("LogBase", 10e9)]);
        check_select_by_commit_and_metric(&db, "0001beef", Metric::Time, &[]);
    }

    #[track_caller]
    fn check_commits_sorted_by_date(db: &Db, metric: Option<Metric>, expected: &[&str]) {
        let rows = EstimationRow::commits_sorted_by_date(db, metric).unwrap();
        assert_eq!(rows.len(), expected.len());

        for i in 0..rows.len() {
            assert_eq!(rows[i].0, expected[i]);
        }
    }

    #[test]
    fn test_commits_sorted_by_date() {
        let db = Db::test();
        EstimationRow::insert_test_data_set(&db, "LogBase", 10e9, 1e6, Metric::Time, 2);
        EstimationRow::insert_test_data_set(&db, "LogBase", 1e9, 1e6, Metric::ICount, 5);
        EstimationRow::insert_test_data_set(&db, "LogByte", 2e7, 5e4, Metric::ICount, 3);

        check_commits_sorted_by_date(&db, Some(Metric::Time), &["0000beef", "0001beef"]);
        check_commits_sorted_by_date(
            &db,
            Some(Metric::ICount),
            &["0000beef", "0001beef", "0002beef", "0003beef", "0004beef"],
        );
        check_commits_sorted_by_date(
            &db,
            None,
            &["0000beef", "0001beef", "0002beef", "0003beef", "0004beef"],
        );

        // Add an extra estimation with a later timestamp than all previous estimations
        EstimationRow::insert_test_data_set_ex(
            &db,
            "LogByte",
            2e7,
            5e4,
            Metric::ICount,
            1,
            "cafe",
            9,
        );
        check_commits_sorted_by_date(
            &db,
            None,
            &["0000beef", "0001beef", "0002beef", "0003beef", "0004beef", "0000cafe"],
        );
    }

    #[test]
    fn test_count_by_metric() {
        let db = Db::test();
        EstimationRow::insert_test_data_set(&db, "LogBase", 1e9, 1e6, Metric::ICount, 3);
        EstimationRow::insert_test_data_set(&db, "LogByte", 2e7, 5e4, Metric::ICount, 3);

        assert_eq!(6, EstimationRow::count_by_metric(&db, Metric::ICount).unwrap());
        assert_eq!(0, EstimationRow::count_by_metric(&db, Metric::Time).unwrap());
    }

    impl EstimationRow {
        pub(crate) fn metric(&self) -> Metric {
            if self.is_metric(Metric::ICount) {
                Metric::ICount
            } else {
                assert!(self.is_metric(Metric::Time));
                Metric::Time
            }
        }

        /// Create estimation rows with differing gas values and commit hashes.
        ///
        /// Commit hashes are "0000beef", "0001beef" and so on. The timestamps
        /// are one second apart from each other.
        ///
        /// Gas values go up by the defined step size.
        pub(crate) fn insert_test_data_set(
            db: &Db,
            name: &str,
            gas: f64,
            gas_step_size: f64,
            metric: Metric,
            num: usize,
        ) {
            Self::insert_test_data_set_ex(db, name, gas, gas_step_size, metric, num, "beef", 0)
        }

        /// Exhaustive arguments for `insert_test_data_set`.
        ///
        /// `commit_suffix` allows to change the commit hashes that data is associated with.
        /// `timestamp_offset_seconds` adds a constant to the generated timestamps.
        pub(crate) fn insert_test_data_set_ex(
            db: &Db,
            name: &str,
            gas: f64,
            gas_step_size: f64,
            metric: Metric,
            num: usize,
            commit_suffix: &str,
            timestamp_offset_seconds: i64,
        ) {
            for i in 0..num {
                let gas = gas + i as f64 * gas_step_size;
                let timestamp =
                    NaiveDateTime::from_timestamp(i as i64 + timestamp_offset_seconds, 0);

                let mut wall_clock_time = None;
                let mut icount = None;
                let mut io_read = None;
                let mut io_write = None;

                match metric {
                    Metric::ICount => {
                        icount = Some(gas / 125_000.0);
                        io_read = Some(0.0);
                        io_write = Some(0.0);
                    }
                    Metric::Time => {
                        wall_clock_time = Some(gas / 1e6);
                    }
                }
                EstimationRow {
                    name: name.to_owned(),
                    gas,
                    parameter: None,
                    wall_clock_time,
                    icount,
                    io_read,
                    io_write,
                    uncertain_reason: None,
                    commit_hash: format!("{i:04}{commit_suffix}"),
                }
                .insert_with_timestamp(db, timestamp)
                .unwrap();
            }
        }

        fn insert_with_timestamp(
            &self,
            db: &Db,
            timestamp: chrono::NaiveDateTime,
        ) -> anyhow::Result<()> {
            db.conn.execute(
                "INSERT INTO estimation(name,gas,parameter,wall_clock_time,icount,io_read,io_write,uncertain_reason,commit_hash,date) values (?1,?2,?3,?4,?,?6,?7,?8,?9,?10)",
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
                    timestamp,
                ],
            )?;
            Ok(())
        }
    }
}
