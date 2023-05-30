# Locust based load testing

WIP: This is work in progress, motivation described in https://github.com/near/nearcore/issues/8999

TLDR: Use [locust](https://locust.io/) to generate transactions against a Near chain and produce statistics.

## Install
```sh
pip3 install locust
```

## Run a first load test

The load generator needs access to a FT contract WASM and it needs access to an account key with plenty of tokens.
For a local test setup, this works just fine.
```sh
# This assumes your shell is are in nearcore directory
CONTRACT="${PWD}/runtime/near-test-contracts/res/fungible_token.wasm"
# This assumes you are running against localnet
KEY=~/.near/localnet/node0/validator_key.json
```

For a quick demo, you can also run a localnet using [nearup](https://github.com/near/nearup).
```sh
nearup run localnet --binary-path ../nearcore/target/release/ --num-nodes 4 --num-shards 4 --override
```

Then to actually run it, this is the command. (Update ports and IP according to your localnet, nearup will print it.)
```sh
cd pytest/tests/loadtest/locust/
locust -H 127.0.0.1:3030 \
  --fungible-token-wasm=$CONTRACT \
  --funding-key=$KEY
```

This will print a link to a web UI where the loadtest can be started and statistics & graphs can be observed.
But if you only want the pure numbers, it can also be run headless.
(add `--headless` and something like `-t 120s -u 1000` to the command to specify total time and max concurrent users)

## Running with full load (multi-threaded or even multi-machine)

Each locust process will only use a single core. This leads to unwanted
throttling on the load generator side if you try to use more than a couple
hundred of users.

Luckily, locust has the ability to swarm the load generation across many processes.
To use it, start one process with the `--master` argument and as many as you
like with `--worker`. (If they run on different machines, you also need to
provide `--master-host` and `--master-port`, if running on the same machine it
will work automagically.)

Start the master
```sh
locust -H 127.0.0.1:3030 \
  --fungible-token-wasm=$CONTRACT \
  --funding-key=$KEY \
  --master
```

On the worker
```sh
# Increase soft limit of open files for the current OS user
# (each locust user opens a separate socket = file)
ulimit -S -n 100000
# Run the worker, key must include the same account id as used on master
locust -H 127.0.0.1:3030 \
  --fungible-token-wasm=$CONTRACT \
  --funding-key=$KEY \
  --worker
```

Spawning N workers in a single shell:
```sh
for i in {1..16}
do
   locust -H 127.0.0.1:3030 \
    --fungible-token-wasm=$CONTRACT \
    --funding-key=$KEY \
    --worker &
done
```

Use `pkill -P $$` to stop all workers.
