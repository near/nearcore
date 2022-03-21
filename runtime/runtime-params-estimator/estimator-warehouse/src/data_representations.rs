//! The warehouse deals with different data representations for data sources,
//! the database, and data consumers. (To keep them decoupled from each other.)
//! This module defines the structs that represent the data and has the code to
//! convert between them.

use serde::Deserialize;
use std::time::Duration;

/// A single data row in the gas_fee table, which can either be measured
/// estimation or a parameter value.
#[derive(Debug, PartialEq)]
pub(crate) struct GasFeeRow {
    /// Name of the estimation / parameter
    pub name: String,
    /// The estimation result converted to gas units
    pub gas: f64,
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
    /// For parameter values, the protocol version for which it was recorded
    pub protocol_version: Option<u32>,
    /// Which git commit this has been estimated on (only for estimations)
    pub commit_hash: Option<String>,
}

/// Estimation result as produced by the params-estimator
#[derive(Deserialize, Debug, PartialEq)]
pub(crate) struct EstimatorOutput {
    pub name: String,
    pub result: EstimationResult,
    pub computed_in: Duration,
}
#[derive(Deserialize, Debug, PartialEq)]
pub(crate) struct EstimationResult {
    pub gas: f64,
    pub time_ns: Option<f64>,
    pub instructions: Option<f64>,
    pub io_r_bytes: Option<f64>,
    pub io_w_bytes: Option<f64>,
    pub uncertain_reason: Option<String>,
}
