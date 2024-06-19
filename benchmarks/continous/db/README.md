# DB for data related to continuous benchmarks

This document describes the setup and administration of the database which stores data related to continuous benchmarks. Each benchmark, for instance ft transfer throughput, has its own table storing data like measured throughput and metadata related to the execution of the benchmark.

## Data store selection

The data collected for continuous benchmarks shall be displayed in Grafana dashboards to visualize how `nearcore` performance metrics evolve. Data is pushed after benchmark execution and therefore stored in a SQL database. In particular, we use a PostgreSQL database since there is prior knowledge about adding them to Grafana as a data source.

## Cloud SQL set up and Grafana integration

We set up the Cloud SQL instance [`crt-benchmark`](https://console.cloud.google.com/sql/instances/crt-benchmarks/overview?project=nearone-crt) in the `nearone-crt` project. Within that instance data is stored in the `benchmarks` table.

### Network

In Grafana the database is available as data source `crt_benchmarks`. Since the database does not contain sensitive data, we open the instance for connections from any IPv4 and delegate authentication to PostgreSQL:

```
gcloud sql instances patch crt-benchmarks \
  --authorized-networks=0.0.0.0/0
```

This simplifies remote connections.

### Role

A role with read-only permissions is created for Grafana:

```sql
create role grafana_reader login password 'store_it_in_1password';
grant connect on database benchmarks to grafana_reader;

-- Execute this statement when connected to the benchmarks db.
grant pg_read_all_data to grafana_reader;
```

The `grafana_reader` may use up to 20 connections. To verify that the PostgreSQL instance allows a sufficient number of connections you can run:

```sql
select setting from pg_settings where name = 'max_connections';
```

## Remote connection

To connect to the database remotely, you can execute the `psql` recipe in the [`Justfile`](./Justfile).


# Adding results of you benchmark automatically.

Once you finished with accesses setup (or you can just use on of our GCP where everything already setted up), you can post results of your benchmark to database just by running (from `nearcore/`):
```bash
python3 scripts/run-ft-benchmark.py --time <BENCHMARK_DURATION> --users <AMOUNT_OF_USERS> --shards <SHARDS> --nodes <NODES> --rump-up <RUMP_UP_RATE> --user <ACTOR_NAME>
```

Where `BENCHMARK_DURATION` is duration of experiment (setup time not included) in format `\d+[smh]` (for example `30s`, `15m`, `2h`).
<RUMP_UP_RATE> is integer which means "how many users should be added every second".

Currently everything (nodes and locust) is running on same machine you run this command, so be careful increasing `NODES`.

For first try you can just let script to use default parameters:
```bash
python3 scripts/run-ft-benchmark.py --user alice
```