# FT bencmark scripts

## Quick start

### Prerequirements

First, make sure that you setted up your db access. About it you can read [here](https://github.com/near/nearcore/blob/master/benchmarks/continous/db/tool/README.md#requirements).

For scripts themselves, just install necessary python packages: `python -m pip install -r pytest/requirements.txt`

## Run your benchmark

For running a benchmark you can just use (from `nearcore/`) `python3 benchmarks/continous/scripts/run-ft-benchmark.py`. This command will launch neard, locust traffic generator, collect data and send it to our db connected to grafana. Using this script you also can change parameters of your run:

```bash
python3 benchmarks/continous/scripts/run-ft-benchmark.py --time <BENCHMARK_DURATION> --users <AMOUNT_OF_USERS> --shards <SHARDS> --nodes <NODES> --rump-up <RUMP_UP_RATE> --user <ACTOR_NAME>
```

Where `BENCHMARK_DURATION` is duration of experiment (setup time not included) in format `\d+[smh]` (for example `30s`, `15m`, `2h`).
<RUMP_UP_RATE> is integer which means "how many users should be added every second".

Currently everything (nodes and locust) is running on same machine you run this command, so be careful increasing `NODES`.