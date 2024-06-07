create table ft_transfers (
    id serial primary key,

    -- Metadata related to the execution of the benchmark.

    -- Timestamp of benchmark execution start.
    -- Enables Grafana time series queries.
    -- https://grafana.com/docs/grafana/latest/datasources/postgres
    -- /#time-series-queries
    time timestamp with time zone not null,
    -- Hash of the git commit off which the run's neard was compiled.
    git_commit_hash text not null,
    -- Time when the commit was made. Store it in the db to easily check how
    --  recent the commit is.
    git_commit_time timestamp with time zone not null,
    -- Number of (physically separate) NEAR nodes participating in the run.
    num_nodes integer not null,
    -- Descriptors of NEAR nodes' hardware, e.g. GCP machine types.
    node_hardware text [] not null,
    -- Number of machines used for traffic generation.
    num_traffic_gen_machines integer not null,
    -- Whether the set of machines running nodes and generating transactions
    -- was disjoint.
    disjoint_workloads boolean not null,
    -- Number of shards where users sending ft transfers are located.
    num_shards integer not null,
    -- Number of unique users that sent ft transfers.
    num_unique_users integer not null,
    -- Size of the synthetic state at the beginning of the run in bytes.
    size_state_bytes integer not null,

    -- Bencmark results.

    -- FT transfer transactions per second aggregated over the run of the
    -- benchmark.
    tps integer not null,
    -- Total number of ft transfer transactions executed during the run.
    total_transactions integer not null
);

grant select on ft_transfers to grafana_reader;
grant select on ft_transfers to benchmark_runner;
grant insert on ft_transfers to benchmark_runner;
