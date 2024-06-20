use chrono::{DateTime, Utc};
use diesel::prelude::Insertable;
use serde::Deserialize;

use crate::schema::ft_transfers;

#[derive(Insertable, Deserialize)]
#[diesel(table_name = ft_transfers)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewFtTransfer {
    /// String representation of UTC datetime when the benchmark run was started, e.g.
    /// '2024-06-07T11:30:44Z'.The name `time` (as opposed to `time_start`) is chosen to enable
    /// Grafana [time series queries].
    ///
    /// [time series queries]: https://grafana.com/docs/grafana/latest/datasources/postgres/#time-series-queries
    pub time: DateTime<Utc>,
    /// See `time` for formatting.
    pub time_end: DateTime<Utc>,
    pub git_commit_hash: String,
    /// See `time` for formatting.
    pub git_commit_time: DateTime<Utc>,
    pub num_nodes: i32,
    pub node_hardware: Vec<String>,
    pub num_traffic_gen_machines: i32,
    pub disjoint_workloads: bool,
    pub num_shards: i32,
    pub num_unique_users: i32,
    pub size_state_bytes: i64,
    pub tps: i32,
    pub total_transactions: i64,
    /// Specifies who ran the benchmark.
    pub initiator: String,
    /// Describes the context, e.g. *scheduled continuous benchmark run*.
    pub context: String,
}
