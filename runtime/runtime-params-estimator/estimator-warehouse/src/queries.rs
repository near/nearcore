//! Data queries to fetch data from the database

use rusqlite::Connection;

use crate::data_representations::GasFeeRow;

impl GasFeeRow {
    pub fn all_time_based(db: &Connection) -> anyhow::Result<Vec<Self>> {
        let mut stmt = db.prepare(
            "SELECT name,gas,wall_clock_time,uncertain_reason,commit_hash FROM gas_fee WHERE wall_clock_time IS NOT NULL;",
        )?;

        let data = stmt
            .query_map([], |row| {
                Ok(GasFeeRow {
                    name: row.get(0)?,
                    gas: row.get(1)?,
                    wall_clock_time: row.get(2)?,
                    icount: None,
                    io_read: None,
                    io_write: None,
                    uncertain_reason: row.get(3)?,
                    protocol_version: None,
                    commit_hash: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;
        Ok(data)
    }
    pub fn all_icount_based(db: &Connection) -> anyhow::Result<Vec<Self>> {
        let mut stmt = db.prepare(
            "SELECT name,gas,icount,io_read,io_write,uncertain_reason,commit_hash FROM gas_fee WHERE icount IS NOT NULL;",
        )?;

        let data = stmt
            .query_map([], |row| {
                Ok(GasFeeRow {
                    name: row.get(0)?,
                    gas: row.get(1)?,
                    wall_clock_time: None,
                    icount: row.get(2)?,
                    io_read: row.get(3)?,
                    io_write: row.get(4)?,
                    uncertain_reason: row.get(5)?,
                    protocol_version: None,
                    commit_hash: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;
        Ok(data)
    }
}
