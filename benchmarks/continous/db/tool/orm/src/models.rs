use chrono::{DateTime, Utc};
use diesel::prelude::Insertable;
use serde::Deserialize;

use crate::schema::ft_transfers;

#[derive(Insertable, Deserialize)]
#[diesel(table_name = ft_transfers)]
pub struct NewFtTransfer {
    // TODO store start and time in two separate columns
    /// String representation of UTC datetime when the benchmark was run, e.g. '2024-06-07T11:30:44Z'
    pub time: DateTime<Utc>,
    pub git_commit_hash: String,
    /// See `time` for formatting.
    pub git_commit_time: DateTime<Utc>,
    pub num_nodes: i32,
    pub node_hardware: Vec<String>,
    pub num_traffic_gen_machines: i32,
    pub disjoint_workloads: bool,
    pub num_shards: i32,
    pub num_unique_users: i32,
    // TODO next to fields should be i64 (and sql bigint)
    pub size_state_bytes: i32,
    pub tps: i32,
    pub total_transactions: i32,
}
