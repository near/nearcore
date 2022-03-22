use std::io::prelude::*;

use anyhow::Context;

use serde::Deserialize;
use std::time::Duration;

use crate::db::{EstimationRow, DB};

/// Additional information required for import
pub(crate) struct ImportInfo {
    /// Required for importing estimation results
    pub commit_hash: Option<String>,
    /// Required for importing parameter values
    pub protocol_version: Option<u32>,
}

/// Estimation result as produced by the params-estimator
#[derive(Deserialize, Debug, PartialEq)]
struct EstimatorOutput {
    name: String,
    result: EstimationResult,
    computed_in: Duration,
}
#[derive(Deserialize, Debug, PartialEq)]
struct EstimationResult {
    gas: f64,
    time_ns: Option<f64>,
    instructions: Option<f64>,
    io_r_bytes: Option<f64>,
    io_w_bytes: Option<f64>,
    uncertain_reason: Option<String>,
}

impl DB {
    pub(crate) fn import_json_lines(
        &self,
        info: &ImportInfo,
        input: impl BufRead,
    ) -> anyhow::Result<()> {
        for line in input.lines() {
            self.import(info, &line?)?;
        }
        Ok(())
    }

    fn import(&self, info: &ImportInfo, line: &str) -> anyhow::Result<()> {
        if let Ok(estimator_output) = serde_json::from_str::<EstimatorOutput>(line) {
            let commit_hash = info.commit_hash.as_ref().with_context(|| {
                "Missing --commit-hash argument while importing estimation data".to_owned()
            })?;
            let row = EstimationRow {
                name: estimator_output.name,
                gas: estimator_output.result.gas,
                parameter: None, // TODO
                wall_clock_time: estimator_output.result.time_ns,
                icount: estimator_output.result.instructions,
                io_read: estimator_output.result.io_r_bytes,
                io_write: estimator_output.result.io_w_bytes,
                uncertain_reason: estimator_output.result.uncertain_reason,
                commit_hash: commit_hash.clone(),
            };
            self.insert_estimation(&row)?;
        }
        Ok(())
    }
}
