# How to start developing nearcore

## Running a local node

If you're looking to run a validator or RPC node in mainnet or testnet, you can follow this [article](https://near-nodes.io/validator/running-a-node) that is using nearup or [this article](https://docs.near.org/pt-BR/docs/develop/node/validator/compile-and-run-a-node/) that shows you how to compile and run it.

In this section, we'll cover the preferred setup that you might want to have when you do the local nearcore development.

### Step 1 - get the code

```shell
git clone https://github.com/near/nearcore

git checkout master
```

### Step 2 - compilation

There are 3 options:

* default (debug) binary - faster compilation, slower running
```shell
cargo build --package neard
```

* quick-release - medium compilation speed, faster running
```shell
cargo build --package neard --profile quick-release
```

* release - slow compilation time, even faster running
```shell
cargo build --package neard --release
```

IMPORTANT: each one of the methods above, is putting a binary in a different location (``target/debug``, ``target/quick-release``, ``target/release``) - so make sure that you adjust other scripts accordingly.

For local development, we'd recomment using the default (debug) build.


### Step 3 - starting the node

There are couple ways to start the locanet, but the way I'd recommend is to use the nearup. This way you'll be able to automatically start 4 nodes on your local machine, therefore also testing the networking code, synchronization etc.

First: get the nearup:

```
git clone https://github.com/near/nearup
cd nearup
```

And then you can start the node using the command below (from the nearup directory):

```
python3 nearup run localnet --binary-path ../nearcore/target/debug/ --interactive
```

The command above, is going to ask you a bunch of questions.

* How many nodes -- the default is 4 - and I'd recommend that if your machine can handle it.
* How many shards - the default is 1 - but I'd recomment actually running with 4 shards.
* Would you like a fixed accounts - I'd say yes - this way you'll automatically have a bunch of accounts that are spread between different shards (for example shard0 will be on shard 0, shard1 will be on shard1 etc).
* Should nodes be archival - up to you (archival nodes will take some more disk space - but in most localnet cases, thats negligible difference).

Then you can go to http://localhost:3030/debug and congratulations - you are officially running a localnet instance.

If you want to shut it down, you can run:

```shell
python3 nearup stop
```

The history of the node will be persisted - and if you re-run the nearup run, it will ask you whether you want to reuse it or remove it.

The localnet will put the home directories in ``~/.near/localnet/nodeX``. Logs will be put in ``~/.nearup/logs/localnet/nodeX.log``

### Step 4 - Next steps

Now you can actually play with your network. If you've created the fixed accounts, you can try sending tokens between them:

```shell
NEAR_ENV=local near send shard0 shard1 500
```

You can create sub accounts:

```shell
NEAR_ENV=local near create-account 0.shard0 --masterAccount shard0
```

#### Load testing

You can also run a larger loadtests to put some load on the system.

Running this command will deploy the example contract (https://github.com/near/nearcore/blob/master/pytest/tests/loadtest/contract/src/lib.rs) - in this example only to account ``shard0``.

```
python3 pytest/tests/loadtest/setup.py --home=~/.near/localnet/node0 --num_accounts=1 
```

And then you can start sending different requests to it:

```
python3 pytest/tests/loadtest/loadtest.py --home=~/.near/localnet/node0 --num_accounts=1 --num_requests=1000 --contract_type=compute
```

See the [documentation](https://github.com/near/nearcore/tree/master/pytest/tests/loadtest) for more info.

### Restarting after code change

When I do some code change, I'd compile it (cargo build -p nearcore) and then deploy by killing the node0 and running it again.

So, the first time:
``` shell
ps aux | grep neard | grep node0
```
Find the pid of the node0 - and kill the process

Start the node manually:

```shell
../nearcore/target/debug/neard --home /Users/michalski/.near/localnet/node0 run
```




