//! Database connection and struct representing rows in the data tables.

use std::path::Path;

use rusqlite::{params, Connection};

/// Wrapper around database connection
pub(crate) struct DB {
    conn: Connection,
}

impl DB {
    /// Opens an existing SQLite DB or creates it
    pub(crate) fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        let init_sql = include_str!("init.sql");
        conn.execute(init_sql, [])?;
        Ok(Self { conn })
    }
    #[cfg(test)]
    pub(crate) fn test() -> Self {
        let conn = Connection::open_in_memory().unwrap();
        let init_sql = include_str!("init.sql");
        conn.execute(init_sql, []).unwrap();
        Self { conn }
    }
    pub(crate) fn insert_estimation(&self, row: &EstimationRow) -> anyhow::Result<()> {
        self.conn.execute(
            "INSERT INTO estimation(name,gas,parameter,wall_clock_time,icount,io_read,io_write,uncertain_reason,commit_hash) values (?1,?2,?3,?4,?,?6,?7,?8,?9)",
            params![
                row.name,
                row.gas,
                row.parameter,
                row.wall_clock_time,
                row.icount,
                row.io_read,
                row.io_write,
                row.uncertain_reason,
                row.commit_hash,
            ],
        )?;
        Ok(())
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

impl EstimationRow {
    pub fn all_time_based(db: &DB) -> anyhow::Result<Vec<Self>> {
        let mut stmt = db.conn.prepare(
            "SELECT name,gas,wall_clock_time,uncertain_reason,commit_hash FROM estimation WHERE wall_clock_time IS NOT NULL;",
        )?;

        let data = stmt
            .query_map([], |row| {
                Ok(Self {
                    name: row.get(0)?,
                    gas: row.get(1)?,
                    parameter: None,
                    wall_clock_time: row.get(2)?,
                    icount: None,
                    io_read: None,
                    io_write: None,
                    uncertain_reason: row.get(3)?,
                    commit_hash: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;
        Ok(data)
    }
    pub fn all_icount_based(db: &DB) -> anyhow::Result<Vec<Self>> {
        let mut stmt = db.conn.prepare(
            "SELECT name,gas,icount,io_read,io_write,uncertain_reason,commit_hash FROM estimation WHERE icount IS NOT NULL;",
        )?;

        let data = stmt
            .query_map([], |row| {
                Ok(Self {
                    name: row.get(0)?,
                    gas: row.get(1)?,
                    parameter: None,
                    wall_clock_time: None,
                    icount: row.get(2)?,
                    io_read: row.get(3)?,
                    io_write: row.get(4)?,
                    uncertain_reason: row.get(5)?,
                    commit_hash: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;
        Ok(data)
    }
}
