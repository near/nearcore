# Locust based load testing

TLDR: Use [locust](https://locust.io/) to generate transactions against a Near chain and produce statistics.

Locust is a python library which we are using to send load to an RPC node. How
to set up the network under test is outside the scope of this document. This is
only about generating the load.

## Install
```sh
pip3 install locust
# Run in nearcore directory.
pip3 install -r pytest/requirements.txt
```

*Note: You will need a working python3 / pip3 environment. While the code is
written in a backwards compatible way, modern OSs with modern python are
preferred. Completely independent of locust, you may run into problems with
PyOpenSSL with an old OS, `pip3 install pyopenssl --upgrade` may help if you see
error messages involving `X509_V_FLAG_CB_ISSUER_CHECK`.*

## Run a first load test

The load generator needs access to an account key with plenty of tokens.
For a local test setup, this works just fine.
```sh
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
  -f locustfiles/ft.py \
  --funding-key=$KEY
```

This will print a link to a web UI where the loadtest can be started and statistics & graphs can be observed.
But if you only want the pure numbers, it can also be run headless.
(add `--headless` and something like `-t 120s -u 1000` to the command to specify total time and max concurrent users)

## Running with full load (multi-threaded or even multi-machine)

Each locust process will only use a single core. This leads to unwanted
throttling on the load generator side if you try to use more than a couple
hundred of users.

In the Locust UI, check the "Workers" tab to see CPU and memory usage. If this
approaches anything close to 100%, you should use more workers.

Luckily, locust has the ability to swarm the load generation across many processes.
To use it, start one process with the `--master` argument and as many as you
like with `--worker`. (If they run on different machines, you also need to
provide `--master-host` and `--master-port`, if running on the same machine it
will work automagically.)

Start the master:

```sh
locust -H 127.0.0.1:3030 \
  -f locustfiles/ft.py \
  --funding-key=$KEY \
  --master
```

On the worker:

```sh
# Increase soft limit of open files for the current OS user
# (each locust user opens a separate socket = file)
ulimit -S -n 100000
# Run the worker, key must include the same account id as used on master
locust -H 127.0.0.1:3030 \
  -f locustfiles/ft.py \
  --funding-key=$KEY \
  --worker
```

Note that you have to start each worker process individually. But you can, for
example, use the `&` operator to spawn N workers in a single shell.

```sh
for i in {1..16}
do
   locust -H 127.0.0.1:3030 \
    -f locustfiles/ft.py \
    --funding-key=$KEY \
    --worker &
done
```

Use `pkill -P $$` to stop all workers spawned in the current shell.
Stopping the master will also terminate all workers.

### Funding Key

The funding key (`--funding-key=$KEY`) and the account associated with it are
used to pay for all on-chain transactions. When running in the distributed mode,
it will first create a funding account for each worker instance and those are
used to pay for transactions.

As the load generator is currently configured, it will consume 1M NEAR from the
master funding account for each worker instance spawned. So, if you run with 64
workers with 1000 users, it will consume 64M Near. It's recommended to have a
funding key with billions of Near.

Also, the account name associated with this account should not bee too long.
Keep it below 20 characters, and it should be fine. The reason is that we create
sub accounts on this account, which become Sweat users. If the parent account
name is too long, children names will breach the 64 bytes max length.

# Available load types

Different locust files can be passed as an argument to `locust` to run a specific load test.
All available workloads can be found in `locustfiles` folder.

Currently supported load types:

| load type | file | args | description |
|---|---|---|---|
| Fungible Token | ft.py | (`--fungible-token-wasm $WASM_PATH`) <br> (`--num-ft-contracts $N`) |  Creates `$N` FT contracts per worker, registers each user in one of them. Users transfer FTs between each other. |
| Social DB  | social.py | (`--social-db-wasm $WASM_PATH`) | Creates a single instance of SocialDB and registers users to it. Users post messages and follow other users. (More workload TBD) |
| Congestion | congestion.py | (`--congestion-wasm $WASM_PATH`) | Creates a single instance of Congestion contract. Users run large and long transactions. |
| Sweat (normal load) | sweat.py | (`--sweat-wasm $WASM_PATH`) | Creates a single instance of the SWEAT contract. A mix of FT transfers and batch minting with batch sizes comparable to mainnet observations in summer 2023. |
| Sweat (storage stress test) | sweat.py | `--tags=storage-stress-test` <br> (`--sweat-wasm $WASM_PATH`) | Creates a single instance of the SWEAT contract. Sends maximally large batches to mint more tokens, thereby touching many storage nodes per receipt. This load will take a while to initialize enough Sweat users on chain. |
| Sweat (claim) | sweat.py | `--tags=claim-test` <br> (`--sweat-wasm $WASM_PATH`) <br> (`--sweat-claim-wasm $WASM_PATH`) | Creates a single instance of the SWEAT and SWEAT.CLAIM contract. Sends deferred batches to mint more tokens, thereby touching many storage nodes per receipt. Then calls balance checks that iterate through populated state. |

## Notes on Storage Stress Test

The Sweat based storage stress test is special. While the other workloads send
innocent traffic with many assumptions, this test pushes the storage accesses
per chunk to the limit. As such, it is a bit more fragile.

### Slow Start

First, you will notice that for several minutes after start, it will only be
registering new accounts to the Sweat contract on chain. You will see these
requests show up as "Init FT Account" in the statistics tab of the Locust UI.

To make sure you are not waiting forever, you want to have enough locust users
per worker instance. For example, 100 users in one worker will be 100 times
faster than 100 users distributed over 100 workers.

Once enough accounts have been registered, large batches of users get new steps
added, which translates to new tokens being minted for them. You will see them
as `Sweat record batch (stress test)` requests.

When you restart your workers, they reset their known registered user accounts.
Hence on restart they add new accounts again. And you have to wait again. To
avoid this, you can stop and restart tests from within the UI. This way, they
will remember the account list and start the next test immediately, without long
setup.


### Master Key Requirements

The `--funding-key` provided must always have enough balance to fund many users.
But this is even more extreme for this load, as we are creating many accounts
per worker.

Also, the account name limits are even tighter for this load. Other loads
probably work with lengths up to 40 character, here it really has to be below 20
characters or else we hit the log output limit when writing all the JSON events
for updated Sweat balances.
