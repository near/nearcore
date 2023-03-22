# Python Tests

To simplify writing integration tests for nearcore we have a python
infrastructure that allows writing a large variety of tests that run small local
clusters, remove clusters, or run against full-scale live deployments.

Such tests are written in python and not in Rust (in which the nearcore itself,
and most of the sanity and fuzz tests, are written) due to the availability of
libraries to easily connect to, remove nodes and orchestrate cloud instances.

Nearcore itself has several features guarded by a
[feature flag](https://doc.rust-lang.org/1.29.0/book/first-edition/conditional-compilation.html)
that allows the python tests to invoke behaviors otherwise impossible to be
exercised by an honest actor.

# Basics

The infrastructure is located in `{nearcore}/pytest/lib` and the tests themselves
are in subdirectories of `{nearcore}/pytest/tests`. To prepare the local machine to
run the tests you'd need python3 (python 3.7), and have several dependencies
installed, for which we recommend using virtualenv:

```
cd pytest
virtualenv .env --python=python3
. .env/bin/activate
pip install -r requirements.txt
```

The tests are expected to be ran from the `pytest` dir itself. For example, once
the virtualenv is configured:

```
cd pytest
. .env/bin/activate
python tests/sanity/block_production.py
```

This will run the most basic tests that spin up a small cluster locally and wait
until it produces several blocks.

## Compiling the client for tests

The local tests by default expect the binary to be in the default location for a
debug build (`{nearcore}/target/debug`). Some tests might also expect
test-specific features guarded by a feature flag to be available. To compile the
binary with such features run:

```
cargo build -p neard --features=adversarial
```

The feature is called `adversarial` to highlight that the many functions it enables,
outside of tests, would constitute malicious behavior. The node compiled with
such a flag will not start unless an environment variable `ADVERSARY_CONSENT=1`
is set and prints a noticeable warning when it starts, thus minimizing the chance
that an honest participant accidentally launches a node compiled with such
functionality.

You can change the way the tests run (locally or using Google Cloud), and where
the local tests look for the binary by supplying a config file. For example, if you
want to run tests against a release build, you can create a file with the
following config:

```json
{"local": True, "near_root": "../target/release/"}
```

and run the test with the following command:

```shell
NEAR_PYTEST_CONFIG=<path to config> python tests/sanity/block_production.py
```

# Writing tests

We differentiate between "regular" tests, or tests that spin up their cluster,
either local or on the cloud, and "mocknet" tests, or tests that run against
an existing live deployment of NEAR.

In both cases, the test starts by importing the infrastructure and starting or
connecting to a cluster

## Starting a cluster

In the simplest case a regular test starts by starting a cluster. The cluster
will run locally by default but can be spun up on the cloud by supplying the
corresponding config.

```python
import sys
sys.path.append('lib')
from cluster import start_cluster

nodes = start_cluster(4, 0, 4, None, [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})
```

In the example above the first three parameters are `num_validating_nodes`,
`num_observers` and `num_shards`. The third parameter is a config, which generally
should be `None`, in which case the config is picked up from the environment
variable as shown above.

`start_cluster` will spin up `num_validating_nodes` nodes that are block
producers (with pre-staked tokens), `num_observers` non-validating nodes and
will configure the system to have `num_shards` shards. The fifth argument
changes the genesis config. Each element is a list of some length `n` where the
first `n-1` elements are a path in the genesis JSON file, and the last element
is the value. You'd often want to significantly reduce the epoch length, so that
your test triggers epoch switches, and reduce the kick-out threshold since with
shorter epochs it is easier for a block producer to get kicked out.

The last parameter is a dictionary from the node ordinal to changes to their
local config.

Note that `start_cluster` spins up all the nodes right away. Some tests (e.g.
tests that test syncing) might want to configure the nodes but delay their
start. In such a case you will initialize the cluster by calling
`init_cluster` and will run the nodes manually, for example, see
[`state_sync.py`](https://github.com/nearprotocol/nearcore/blob/master/pytest/tests/sanity/state_sync.py)

## Connecting to a mocknet

Nodes that run against a mocknet would connect to an existing cluster instead of
running their own.

```python
import sys
sys.path.append('lib')
from cluster import connect_to_mocknet

nodes, accounts = connect_to_mocknet(None)
```

The only parameter is a config, with `None` meaning to use the config from the
environment variable. The config should have the following format:

```json
{
    "nodes": [
        {"ip": "(some_ip)", "port": 3030},
        {"ip": "(some_ip)", "port": 3030},
        {"ip": "(some_ip)", "port": 3030},
        {"ip": "(some_ip)", "port": 3030}
    ],
    "accounts": [
        {"account_id": "node1", "pk": "ed25519:<public key>", "sk": "edd25519:<secret key>"},
        {"account_id": "node2", "pk": "ed25519:<public key>", "sk": "edd25519:<secret key>"}
    ]
}
```

## Manipulating nodes

The nodes returned by `start_cluster` and `init_cluster` have certain
convenience functions. You can see the full interface in
`{nearcore}/pytest/lib/cluster.py`.

`start(boot_public_key, (boot_ip, boot_port))` starts the node. If both
arguments are `None`, the node will start as a boot node (note that the concept
of a "boot node" is relatively vague in a decentralized system, and from the
perspective of the tests the only requirement is that the graph of "node A
booted from node B" is connected).

The particular way to get the `boot_ip` and `boot_port` when launching `node1`
with `node2` being its boot node is the following:

```python
node1.start(node2.node_key.pk, node2.addr())
```

`kill()` shuts down the node by sending it `SIGKILL`

`reset_data()` cleans up the data dir, which could be handy between the calls to
`kill` and `start` to see if a node can start from a clean state.

Nodes on the mocknet do not expose `start`, `kill` and `reset_data`.

## Issuing RPC calls

Nodes in both regular and mocknet tests expose an interface to issue RPC calls.
In the most generic case, one can just issue raw JSON RPC calls by calling the
`json_rpc` method:

```python
validator_info = nodes[0].json_rpc('validators', [<some block_hash>])
```

For the most popular calls, there are convenience functions:

* `send_tx` sends a signed transaction asynchronously
* `send_tx_and_waits` sends a signed transaction synchronously
* `get_status` returns the current status (the output of the `/status/endpoint`),
  which contains e.g. last block hash and height
* `get_tx` returns a transaction by the transaction hash and the recipient ID.

See all the methods in `{nearcore}/pytest/lib/cluster.rs` after the definition
of the `json_rpc` method.

### Signing and sending transactions

There are two ways to send a transaction. A synchronous way (`send_tx_and_wait`)
sends a tx and blocks the test execution until either the TX is finished, or the
timeout is hit. An asynchronous way (`send_tx` + `get_tx`) sends a TX and then
verifies its result later. Here's an end-to-end example of sending a
transaction:

```python
# the tx needs to include one of the recent hashes
last_block_hash = nodes[0].get_status()['sync_info']['latest_block_hash']
last_block_hash_decoded = base58.b58decode(last_block_hash.encode('utf8'))

# sign the actual transaction
# `fr` and `to` in this case are instances of class `Key`.
# In mocknet tests the list `Key`s for all the accounts are returned by `connect_to_mocknet`
# In regular tests each node is associated with a single account, and its key is stored in the
# `signer_key` field (e.g. `nodes[0].signer_key`)
# `15` in the example below is the nonce. Nonces needs to increase for consecutive transactions
# for the same sender account.
tx = sign_payment_tx(fr, to.account_id, 100, 15, last_block_hash_decoded)

# Sending the transaction synchronously. `10` is the timeout in seconds. If after 10 seconds the
# outcome is not ready, throws an exception
if want_sync:
    outcome = nodes[0].send_tx_and_wait(tx, 10)

# Sending the transaction asynchronously.
if want_async:
    tx_hash = nodes[from_ordinal % len(nodes)].send_tx(tx)['result']

    # and then sometime later fetch the result...
    resp = nodes[0].get_tx(tx_hash, to.account_id, timeout=1)
    # and see if the tx has finished
    finished = 'result' in resp and 'receipts_outcome' in resp['result'] and len(resp['result']['receipts_outcome']) > 0
```

See
[rpc_tx_forwarding.py](https://github.com/nearprotocol/nearcore/blob/master/pytest/tests/sanity/rpc_tx_forwarding.py)
for an example of signing and submitting a transaction.

## Adversarial behavior

Some tests need certain nodes in the cluster to exercise behavior that is
impossible to be invoked by an honest node. For such tests, we provide
functionality that is protected by an "adversarial" feature flag.

It's an advanced feature, and more thorough documentation is a TODO. Most of the
tests that depend on the feature flag enabled are under
`{nearcore}/pytest/tests/adversarial`, refer to them for how such features can
be used. Search for code in the `nearcore` codebase guarded by the "adversarial"
feature flag for an example of how such features are added and exposed.

## Interfering with the network

We have a library that allows running a proxy in front of each node that would
intercept all the messages between nodes, deserialize them in python and run a
handler on each one. The handler can then either let the message pass (`return
True`), drop it (`return False`) or replace it (`return <new message>`).

This technique can be used to both interfere with the network (by dropping or
replacing messages), and to inspect messages that flow through the network
without interfering with them. For the latter, note that the handler for each node
runs in a separate `Process`, and thus you need to use `multiprocessing`
primitives if you want the handlers to exchange information with the main test
process, or between each other.

See the tests that match `tests/sanity/proxy_*.py` for examples.

# Contributing tests

We always welcome new tests, especially python tests that use the above
infrastructure. We have a list of test requests
[here](https://github.com/nearprotocol/nearcore/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+test%22+),
but also welcome any other tests that test aspects of the network we haven't
thought about.
