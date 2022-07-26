//! Types for telemetry reporting. Can be received by any telemetry dashboard to display
//! node count and their status across the network.
use serde::{Deserialize, Serialize};

use crate::types::BlockHeight;

use crate::types::AccountId;

#[derive(Serialize, Deserialize, Debug)]
pub struct TelemetryAgentInfo {
    pub name: String,
    pub version: String,
    pub build: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TelemetrySystemInfo {
    pub bandwidth_download: u64,
    pub bandwidth_upload: u64,
    pub cpu_usage: f32,
    pub memory_usage: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TelemetryChainInfo {
    pub node_id: String,
    pub account_id: Option<AccountId>,
    pub is_validator: bool,
    pub status: String,
    pub latest_block_hash: String,
    pub latest_block_height: BlockHeight,
    pub num_peers: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TelemetryInfo {
    pub agent: TelemetryAgentInfo,
    pub system: TelemetrySystemInfo,
    pub chain: TelemetryChainInfo,
    // Extra telemetry information that will be ignored by the explorer frontend.
    pub extra_info: String,
}
