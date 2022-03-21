use std::io::prelude::*;

use anyhow::Context;
use rusqlite::{params, Connection};

use crate::data_representations::EstimatorOutput;

/// Additional information required for import
pub(crate) struct ImportInfo {
    /// Required for importing estimation results
    pub commit_hash: Option<String>,
    /// Required for importing parameter values
    pub protocol_version: Option<u32>,
}

pub(crate) fn json_from_bufread(
    db: &Connection,
    info: &ImportInfo,
    input: impl BufRead,
) -> anyhow::Result<()> {
    for line in input.lines() {
        import(db, info, &line?)?;
    }
    Ok(())
}

fn import(db: &Connection, info: &ImportInfo, line: &str) -> anyhow::Result<()> {
    if let Ok(estimator_output) = serde_json::from_str::<EstimatorOutput>(line) {
        let commit_hash = info.commit_hash.as_ref().with_context(|| {
            "Missing --commit-hash argument while importing estimation data".to_owned()
        })?;
        db.execute(
            "INSERT INTO gas_fee(name,gas,wall_clock_time,icount,io_read,io_write,uncertain_reason,commit_hash) values (?1,?2,?3,?4,?,?6,?7,?8)",
            params![
                &estimator_output.name,
                &estimator_output.result.gas,
                &estimator_output.result.time_ns,
                &estimator_output.result.instructions,
                &estimator_output.result.io_r_bytes,
                &estimator_output.result.io_w_bytes,
                &estimator_output.result.uncertain_reason,
                commit_hash,
            ],
        )?;
    }
    Ok(())
}
