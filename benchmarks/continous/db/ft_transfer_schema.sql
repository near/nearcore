create table ft_transfer (
    id serial primary key,

    -- Metada related to the execution of the benchmark.

    -- Timestamp of benchmark execution start.
    -- Enables Grafana time series queries.
    -- https://grafana.com/docs/grafana/latest/datasources/postgres
    -- /#time-series-queries
    time timestamp not null,
    -- Hash of the git commit off which the run's neard was compiled.
    git_commit_hash varchar(40) not null,
    -- Time when the commit was made. Store it in the db to easily check how
    --  recent the commit is.
    git_commit_time timestamp not null,
    -- Number of shards where users sending ft transfers are located.
    num_shards integer not null,
    -- Number of unique users that sent ft transfers.
    num_unique_users integer not null,
    -- Size of the synthetic state at the beginning of the run in bytes.
    size_state_bytes integer not null,
    -- Whether the set of machines running nodes and generating transactions
    -- was disjoint.
    disjoint_workloads boolean not null,

    -- Bencmark results.

    -- FT transfer transactions per second aggregated over the run of the
    -- benchmark.
    tps integer not null,
    -- Total number of ft transfer transactions executed during the run.
    total_transactions integer not null
);
