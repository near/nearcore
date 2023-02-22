# Run a Node

This chapter focuses on the basics of running a node you've just built from
source. It tries to explain how the thing works under the hood and pays
relatively little attention to the various shortcuts we have.

## Building the Node

Start with the following command:

```console
$ cargo run --profile quick-release -p neard -- --help
```

This command builds `neard` and asks it to show `--help`. Building `neard` takes
a while, take a look at [Fast Builds](../fast_builds.md) chapter to learn how to
speed it up.

Let's dissect the command:

- `cargo run` asks `Cargo`, the package manager/build tool, to run our
  application. If you don't have `cargo`, install it via <https://rustup.rs>
- `--profile quick-release` is our
  [custom profile](https://doc.rust-lang.org/cargo/reference/profiles.html#custom-profiles)
  to build a somewhat optimized version of the code. The default debug
  profile is faster to compile, but produces a node that is too slow to
  participate in a real network. The `--release` profile produces a fully
  optimized node, but that's very slow to compile. So `--quick-release`
  is a sweet spot for us!
- `-p neard` asks to build the `neard` package. We use
  [cargo workspaces](https://doc.rust-lang.org/cargo/reference/workspaces.html)
  to organize our code. The `neard` package in the top-level `/neard` directory
  is the final binary that ties everything together.
- `--` tells cargo to pass the rest of the arguments through to `neard`.
- `--help` instructs `neard` to list available CLI arguments and subcommands.

**Note:** Building `neard` might fail with an openssl or CC error. This means
that you lack some non-rust dependencies we use (openssl and rocksdb mainly). We
currently don't have docs on how to install those, but (basically) you want to
`sudo apt install` (or whichever distro/package manager you use) missing bits.

## Preparing Tiny Network

Typically, you want `neard` to connect to some network, like `mainnet` or
`testnet`. We'll get there in time, but we'll start small. For the current
chapter, we will run a network consisting of just a single node -- our own.

The first step there is creating the required configuration. Run the `init`
command to create config files:

```console
$ cargo run --profile quick-release -p neard -- init
INFO neard: version="trunk" build="1.1.0-3091-ga8964d200-modified" latest_protocol=57
INFO near: Using key ed25519:B41GMfqE2jWHVwrPLbD7YmjZxxeQE9WA9Ua2jffP5dVQ for test.near
INFO near: Using key ed25519:34d4aFJEmc2A96UXMa9kQCF8g2EfzZG9gCkBAPcsVZaz for node
INFO near: Generated node key, validator key, genesis file in ~/.near
```

As the log output says, we are just generating _some things_ in `~/.near`.
Let's take a look:

```console
$ ls ~/.near
config.json
genesis.json
node_key.json
validator_key.json
```

The most interesting file here is perhaps `genesis.json` -- it specifies the
initial state of our blockchain. There are a bunch of hugely important fields
there, which we'll ignore here. The part we'll look at is the `.records`, which
contains the actual initial data:

```console
$ cat ~/.near/genesis.json | jq '.records'
[
  {
    "Account": {
      "account_id": "test.near",
      "account": {
        "amount": "1000000000000000000000000000000000",
        "locked": "50000000000000000000000000000000",
        "code_hash": "11111111111111111111111111111111",
        "storage_usage": 0,
        "version": "V1"
      }
    }
  },
  {
    "AccessKey": {
      "account_id": "test.near",
      "public_key": "ed25519:B41GMfqE2jWHVwrPLbD7YmjZxxeQE9WA9Ua2jffP5dVQ",
      "access_key": {
        "nonce": 0,
        "permission": "FullAccess"
      }
    }
  },
  {
    "Account": {
      "account_id": "near",
      "account": {
        "amount": "1000000000000000000000000000000000",
        "locked": "0",
        "code_hash": "11111111111111111111111111111111",
        "storage_usage": 0,
        "version": "V1"
      }
    }
  },
  {
    "AccessKey": {
      "account_id": "near",
      "public_key": "ed25519:546XB2oHhj7PzUKHiH9Xve3Ze5q1JiW2WTh6abXFED3c",
      "access_key": {
        "nonce": 0,
        "permission": "FullAccess"
      }
    }
  }
```

(I am using the [jq](https://stedolan.github.io/jq/) utility here)

We see that we have two accounts here, and we also see their public keys (but
not the private ones).

One of these accounts is a validator:

```
$ cat ~/.near/genesis.json | jq '.validators'
[
  {
    "account_id": "test.near",
    "public_key": "ed25519:B41GMfqE2jWHVwrPLbD7YmjZxxeQE9WA9Ua2jffP5dVQ",
    "amount": "50000000000000000000000000000000"
  }
]
```

Now, if we

```console
$ cat ~/.near/validator_key.json
```

we'll see

```json
{
  "account_id": "test.near",
  "public_key": "ed25519:B41GMfqE2jWHVwrPLbD7YmjZxxeQE9WA9Ua2jffP5dVQ",
  "secret_key": "ed25519:3x2dUQgBoEqNvKwPjfDE8zDVJgM8ysqb641PYHV28mGPu61WWv332p8keMDKHUEdf7GVBm4f6z4D1XRgBxnGPd7L"
}
```

That is, we have a secret key for the sole validator in our network, how
convenient.

To recap, `neard init` without arguments creates a config for a new network
that starts with a single validator, for which we have the keys.

You might be wondering what `~/.near/node_key.json` is. That's not too
important, but, in our network, there's no 1-1 correspondence between machines
participating in the peer-to-peer network and accounts on the blockchain. So the
`node_key` specifies the keypair we'll use when signing network packets. These
packets internally will contain messages signed with the validator's key, and
these internal messages will drive the evolution of the blockchain state.

Finally, `~/.near/config.json` contains various configs for the node itself.
These are configs that don't affect the rules guiding the evolution of the
blockchain state, but rather things like timeouts, database settings and
such.

The only field we'll look at is `boot_nodes`:

```console
$ cat ~/.near/config.json | jq '.network.boot_nodes'
""
```

It's empty! The `boot_nodes` specify IPs of the initial nodes our node will
try to connect to on startup. As we are looking into running a single-node
network, we want to leave it empty. But, if you would like to connect to
mainnet, you'd have to set this to some nodes from the mainnet you already know.
You'd also have to ensure that you use the same genesis as the mainnet though
-- if the node tries to connect to a network with a different genesis, it
is rejected.

## Running the Network

Finally,

```console
$ cargo run --profile quick-release -p neard -- run
INFO neard: version="trunk" build="1.1.0-3091-ga8964d200-modified" latest_protocol=57
INFO near: Creating a new RocksDB database path=/home/matklad/.near/data
INFO db: Created a new RocksDB instance. num_instances=1
INFO stats: #       0 4xecSHqTKx2q8JNQNapVEi5jxzewjxAnVFhMd4v5LqNh Validator | 1 validator 0 peers ⬇ 0 B/s ⬆ 0 B/s NaN bps 0 gas/s CPU: 0%, Mem: 50.8 MB
INFO near_chain::doomslug: ready to produce block @ 1, has enough approvals for 59.907µs, has enough chunks
INFO near_chain::doomslug: ready to produce block @ 2, has enough approvals for 40.732µs, has enough chunks
INFO near_chain::doomslug: ready to produce block @ 3, has enough approvals for 65.341µs, has enough chunks
INFO near_chain::doomslug: ready to produce block @ 4, has enough approvals for 51.916µs, has enough chunks
INFO near_chain::doomslug: ready to produce block @ 5, has enough approvals for 37.155µs, has enough chunks
...
```

🎉 it's alive!

So, what's going on here?

Our node is running a single-node network. As the network only has a single
validator, and the node has the keys for the validator, the node can produce
blocks by itself. Note the increasing `@ 1`, `@ 2`, ... numbers. That
means that our network grows.

Let's stop the node with `^C` and look around

```console
INFO near_chain::doomslug: ready to produce block @ 42, has enough approvals for 56.759µs, has enough chunks
^C WARN neard: SIGINT, stopping... this may take a few minutes.
INFO neard: Waiting for RocksDB to gracefully shutdown
INFO db: Waiting for remaining RocksDB instances to shut down num_instances=1
INFO db: All RocksDB instances shut down
$
```

The main change now is that we have a `~/.near/data` directory which holds the
state of the network in various rocksdb tables:

```console
$ ls ~/.near/data
 000004.log
 CURRENT
 IDENTITY
 LOCK
 LOG
 MANIFEST-000005
 OPTIONS-000107
 OPTIONS-000109
```

It doesn't matter what those are, "rocksdb stuff" is a fine level of understanding
here. The important bit here is that the node remembers the state of the network,
so, when we restart it, it continues from around the last block:

```console
$ cargo run --profile quick-release -p neard -- run
INFO neard: version="trunk" build="1.1.0-3091-ga8964d200-modified" latest_protocol=57
INFO db: Created a new RocksDB instance. num_instances=1
INFO db: Dropped a RocksDB instance. num_instances=0
INFO near: Opening an existing RocksDB database path=/home/matklad/.near/data
INFO db: Created a new RocksDB instance. num_instances=1
INFO stats: #       5 Cfba39eH7cyNfKn9GoKTyRg8YrhoY1nQxQs66tLBYwRH Validator | 1 validator 0 peers ⬇ 0 B/s ⬆ 0 B/s NaN bps 0 gas/s CPU: 0%, Mem: 49.4 MB
INFO near_chain::doomslug: not ready to produce block @ 43, need to wait 366.58789ms, has enough approvals for 78.776µs
INFO near_chain::doomslug: not ready to produce block @ 43, need to wait 265.547148ms, has enough approvals for 101.119518ms
INFO near_chain::doomslug: not ready to produce block @ 43, need to wait 164.509153ms, has enough approvals for 202.157513ms
INFO near_chain::doomslug: not ready to produce block @ 43, need to wait 63.176926ms, has enough approvals for 303.48974ms
INFO near_chain::doomslug: ready to produce block @ 43, has enough approvals for 404.41498ms, does not have enough chunks
INFO near_chain::doomslug: ready to produce block @ 44, has enough approvals for 50.07µs, has enough chunks
INFO near_chain::doomslug: ready to produce block @ 45, has enough approvals for 45.093µs, has enough chunks
```

## Interacting With the Node

Ok, now our node is running, let's poke it! The node exposes a JSON RPC interface
which can be used to interact with the node itself (to, e.g., do a health check)
or with the blockchain (to query information about the blockchain state or to
submit a transaction).

```console
$ http get http://localhost:3030/status
HTTP/1.1 200 OK
access-control-allow-credentials: true
access-control-expose-headers: accept-encoding, accept, connection, host, user-agent
content-length: 1010
content-type: application/json
date: Tue, 15 Nov 2022 13:58:13 GMT
vary: Origin, Access-Control-Request-Method, Access-Control-Request-Headers

{
    "chain_id": "test-chain-rR8Ct",
    "latest_protocol_version": 57,
    "node_key": "ed25519:71QRP9qKcYRUYXTLNnrmRc1NZSdBaBo9nKZ88DK5USNf",
    "node_public_key": "ed25519:5A5QHyLayA9zksJZGBzveTgBRecpsVS4ohuxujMAFLLa",
    "protocol_version": 57,
    "rpc_addr": "0.0.0.0:3030",
    "sync_info": {
        "earliest_block_hash": "6gJLCnThQENYFbnFQeqQvFvRsTS5w87bf3xf8WN1CMUX",
        "earliest_block_height": 0,
        "earliest_block_time": "2022-11-15T13:45:53.062613669Z",
        "epoch_id": "6gJLCnThQENYFbnFQeqQvFvRsTS5w87bf3xf8WN1CMUX",
        "epoch_start_height": 501,
        "latest_block_hash": "9JC9o3rZrDLubNxVr91qMYvaDiumzwtQybj1ZZR9dhbK",
        "latest_block_height": 952,
        "latest_block_time": "2022-11-15T13:58:13.185721125Z",
        "latest_state_root": "9kEYQtWczrdzKCCuFzPDX3Vtar1pFPXMdLU5HJyF8Ght",
        "syncing": false
    },
    "uptime_sec": 570,
    "validator_account_id": "test.near",
    "validator_public_key": "ed25519:71QRP9qKcYRUYXTLNnrmRc1NZSdBaBo9nKZ88DK5USNf",
    "validators": [
        {
            "account_id": "test.near",
            "is_slashed": false
        }
    ],
    "version": {
        "build": "1.1.0-3091-ga8964d200-modified",
        "rustc_version": "1.65.0",
        "version": "trunk"
    }
}
```

(I am using [HTTPie here](https://httpie.io/cli))

Note how `"latest_block_height": 952` corresponds to `@ 952` we see in the logs.

Let's query the blockchain state:

```
$ http post http://localhost:3030/ method=query jsonrpc=2.0 id=1 \
     params:='{"request_type": "view_account", "finality": "final", "account_id": "test.near"}'
λ http post http://localhost:3030/ method=query jsonrpc=2.0 id=1 \
           params:='{"request_type": "view_account", "finality": "final", "account_id": "test.near"}'

HTTP/1.1 200 OK
access-control-allow-credentials: true
access-control-expose-headers: content-length, accept, connection, user-agent, accept-encoding, content-type, host
content-length: 294
content-type: application/json
date: Tue, 15 Nov 2022 14:04:54 GMT
vary: Origin, Access-Control-Request-Method, Access-Control-Request-Headers

{
    "id": "1",
    "jsonrpc": "2.0",
    "result": {
        "amount": "1000000000000000000000000000000000",
        "block_hash": "Hn4v5CpfWf141AJi166gdDK3e3khCxgfeDJ9dSXGpAVi",
        "block_height": 1611,
        "code_hash": "11111111111111111111111111111111",
        "locked": "50003138579594550524246699058859",
        "storage_paid_at": 0,
        "storage_usage": 182
    }
}
```

Note how we use an HTTP `post` method when we interact with the blockchain RPC.
The full set of RPC endpoints is documented at

<https://docs.near.org/api/rpc/introduction>

## Sending Transactions

Transactions are submitted via RPC as well. Submitting a transaction manually
with `http` is going to be cumbersome though — transactions are borsh encoded
to bytes, then signed, then encoded in base64 for JSON.

So we will use the official [NEAR CLI] utility.

[NEAR CLI]: https://docs.near.org/tools/near-cli

Install it via `npm`:

```console
$ npm install -g near-cli
$ near -h
Usage: near <command> [options]

Commands:
  near create-account <accountId>    create a new developer account
....
```

Note that, although you install `near-cli`, the name of the utility is `near`.

As a first step, let's redo the `view_account` call we did with raw `httpie`
with `near-cli`:

```console
$ NEAR_ENV=local near state test.near
Loaded master account test.near key from ~/.near/validator_key.json with public key = ed25519:71QRP9qKcYRUYXTLNnrmRc1NZSdBaBo9nKZ88DK5USNf
Account test.near
{
  amount: '1000000000000000000000000000000000',
  block_hash: 'ESGN7H1kVLp566CTQ9zkBocooUFWNMhjKwqHg4uCh2Sg',
  block_height: 2110,
  code_hash: '11111111111111111111111111111111',
  locked: '50005124762657986708532525400812',
  storage_paid_at: 0,
  storage_usage: 182,
  formattedAmount: '1,000,000,000'
}
```

`NEAR_ENV=local` tells `near-cli` to use our local network, rather than the
`mainnet`.

Now, let's create a couple of accounts and send tokes between them:

```
$ NEAR_ENV=local near create-account alice.test.near --masterAccount test.near
NOTE: In most cases, when connected to network "local", masterAccount will end in ".node0"
Loaded master account test.near key from /home/matklad/.near/validator_key.json with public key = ed25519:71QRP9qKcYRUYXTLNnrmRc1NZSdBaBo9nKZ88DK5USNf
Saving key to 'undefined/local/alice.test.near.json'
Account alice.test.near for network "local" was created.

$ NEAR_ENV=local near create-account bob.test.near --masterAccount test.near
NOTE: In most cases, when connected to network "local", masterAccount will end in ".node0"
Loaded master account test.near key from /home/matklad/.near/validator_key.json with public key = ed25519:71QRP9qKcYRUYXTLNnrmRc1NZSdBaBo9nKZ88DK5USNf
Saving key to 'undefined/local/bob.test.near.json'
Account bob.test.near for network "local" was created.

$ NEAR_ENV=local near send alice.test.near bob.test.near 10
Sending 10 NEAR to bob.test.near from alice.test.near
Loaded master account test.near key from /home/matklad/.near/validator_key.json with public key = ed25519:71QRP9qKcYRUYXTLNnrmRc1NZSdBaBo9nKZ88DK5USNf
Transaction Id BBPndo6gR4X8pzoDK7UQfoUXp5J8WDxkf8Sq75tK5FFT
To see the transaction in the transaction explorer, please open this url in your browser
http://localhost:9001/transactions/BBPndo6gR4X8pzoDK7UQfoUXp5J8WDxkf8Sq75tK5FFT
```

**Note:** You can export the variable `NEAR_ENV` in your shell if you are planning
to do multiple commands to avoid repetition:

```console
$ export NEAR_ENV=local
```

NEAR CLI printouts are not always the most useful or accurate, but this seems to
work.

Note that `near` automatically creates keypairs and stores them at
`.near-credentials`:

```console
$ ls ~/.near-credentials/local
  alice.test.near.json
  bob.test.near.json
```

To verify that this did work, and that `near-cli` didn't cheat us, let's
query the state of accounts manually:

```console
$ http post http://localhost:3030/ method=query jsonrpc=2.0 id=1 \
    params:='{"request_type": "view_account", "finality": "final", "account_id": "alice.test.near"}' \
    | jq '.result.amount'
"89999955363487500000000000"

14:30:52|~
λ http post http://localhost:3030/ method=query jsonrpc=2.0 id=1 \
    params:='{"request_type": "view_account", "finality": "final", "account_id": "bob.test.near"}' \
    | jq '.result.amount'
"110000000000000000000000000"
```

Indeed, some amount of tokes was transferred from `alice` to `bob`, and then
some amount of tokens was deducted to account for transaction fees.

## Recap

Great! So we've learned how to run our very own single-node NEAR network using a
binary we've built from source. The steps are:

- Create configs with `cargo run --profile quick-release -p neard -- init`
- Run the node with `cargo run --profile quick-release -p neard -- run`
- Poke the node with `httpie` or
- Install `near-cli` via `npm install -g near-cli`
- Submit transactions via `NEAR_ENV=local near create-account ...`

In the [next chapter](./deploy_a_contract.md), we'll learn how to deploy a simple
WASM contract.
