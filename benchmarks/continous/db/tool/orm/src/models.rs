use chrono::{DateTime, Utc};
use diesel::prelude::Insertable;

use crate::schema::ft_transfers;

#[derive(Insertable)]
#[diesel(table_name = ft_transfers)]
pub struct NewFtTransfer<'a> {
    // TODO store start and time in two separate columns
    pub time: DateTime<Utc>,
    pub git_commit_hash: &'a str,
    pub git_commit_time: DateTime<Utc>,
    pub num_nodes: i32,
    pub node_hardware: &'a [&'a str],
    pub num_traffic_gen_machines: i32,
    pub disjoint_workloads: bool,
    pub num_shards: i32,
    pub num_unique_users: i32,
    // TODO next to fields should be i64 (and sql bigint)
    pub size_state_bytes: i32,
    pub tps: i32,
    pub total_transactions: i32,
}
