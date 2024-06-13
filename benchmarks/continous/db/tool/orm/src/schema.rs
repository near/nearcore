// @generated automatically by Diesel CLI.

diesel::table! {
    ft_transfers (id) {
        id -> Int4,
        time -> Timestamptz,
        git_commit_hash -> Text,
        git_commit_time -> Timestamptz,
        num_nodes -> Int4,
        node_hardware -> Array<Nullable<Text>>,
        num_traffic_gen_machines -> Int4,
        disjoint_workloads -> Bool,
        num_shards -> Int4,
        num_unique_users -> Int4,
        size_state_bytes -> Int8,
        tps -> Int4,
        total_transactions -> Int8,
        time_end -> Timestamptz,
    }
}
