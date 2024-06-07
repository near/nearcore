use chrono::Utc;

use orm::{establish_connection, insert_ft_transfer, models::NewFtTransfer};

fn main() -> anyhow::Result<()> {
    let new_ft_transfer = NewFtTransfer {
        time: Utc::now(),
        git_commit_hash: "foo",
        git_commit_time: Utc::now(),
        num_nodes: 1,
        node_hardware: &["foo"],
        num_traffic_gen_machines: 1,
        disjoint_workloads: true,
        num_shards: 6,
        num_unique_users: 1000,
        size_state_bytes: 100_000_000,
        tps: 800,
        total_transactions: 800_000,
    };

    let connection = &mut establish_connection()?;
    insert_ft_transfer(connection, &new_ft_transfer)?;
    Ok(())
}
