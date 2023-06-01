# General principles

1. Every PR needs to have test coverage in place. Sending the code change and
   deferring tests for a future change is not acceptable.
2. Tests need to either be sufficiently simple to follow or have good
   documentation to explain why certain actions are made and conditions are
   expected.
3. When implementing a PR, **make sure to run the new tests with the change
   disabled and confirm that they fail**! It is extremely common to have tests
   that pass without the change that is being tested.
4. The general rule of thumb for a reviewer is to first review the tests, and
   ensure that they can convince themselves that the code change that passes the
   tests must be correct. Only then the code should be reviewed.
5. Have the assertions in the tests as specific as possible,
   however do not make the tests change-detectors of the concrete implementation.
   (assert only properties which are required for correctness).
   For example, do not do `assert!(result.is_err())`, expect the specific error instead.

# Tests hierarchy

In the NEAR Reference Client we largely split tests into three categories:

1. **Relatively cheap sanity or fast fuzz tests:** It includes all the `#[test]`
   Rust tests not decorated by features. Our repo is configured in such a way
   that all such tests are run on every PR and failing at least one of them is
   blocking the PR from being merged.

To run such tests locally run `cargo nextest run --all`.
It requires nextest harness which can be installed by running `cargo install cargo-nextest` first.

2. **Expensive tests:** This includes all the fuzzy tests that run many iterations,
   as well as tests that spin up multiple nodes and run them until they reach a
   certain condition. Such tests are decorated with
   `#[cfg(feature="expensive-tests")]`. It is not trivial to enable features
   that are not declared in the top-level crate, and thus the easiest way to run
   such tests is to enable all the features by passing `--all-features` to
   `cargo nextest run`, e.g:

`cargo nextest run --package near-client --test cross_shard_tx
tests::test_cross_shard_tx --all-features`

3. **Python tests:** We have an infrastructure to spin up nodes, both locally and
   remotely, in python, and interact with them using RPC. The infrastructure and
   the tests are located in the `pytest` folder. The infrastructure is relatively
   straightforward, see for example `block_production.py`
   [here](https://github.com/nearprotocol/nearcore/blob/master/pytest/tests/sanity/block_production.py).
   See the `Test infrastructure` section below for details.

Expensive and python tests are not part of CI, and are run by a custom nightly
runner. The results of the latest runs are available
[here](http://nightly.neartest.com/). Today, test runs launch approximately
every 5-6 hours. For the latest results look at the **second** run, since the
first one has some tests still scheduled to run.

# Test infrastructure

Different levels of the reference implementation have different infrastructures
available to test them.

## Client

The Client is separated from the runtime via a `RuntimeAdapter` trait.
In production, it uses `NightshadeRuntime` which uses real runtime and epoch managers.
To test the client without instantiating runtime and epoch manager, we have a mock runtime
`KeyValueRuntime`.

Most of the tests in the client work by setting up either a single node (via
`setup_mock()`) or multiple nodes (via `setup_mock_all_validators()`) and then
launching the nodes and waiting for a particular message to occur, with a
predefined timeout.

For the most basic example of using this infrastructure see `produce_two_blocks`
in
[`tests/process_blocks.rs`](https://github.com/nearprotocol/nearcore/blob/master/chain/client/tests/process_blocks.rs).

1. The callback (`Box::new(move |msg, _ctx, _| { ...`) is what is executed
   whenever the client sends a message. The return value of the callback is sent
   back to the client, which allows for testing relatively complex scenarios. The
   tests generally expect a particular message to occur, in this case, the tests
   expect two blocks to be produced. `System::current().stop();` is the way to
   stop the test and mark it as passed.
2. `near_network::test_utils::wait_or_panic(5000);` is how the timeout for the
   test is set (in milliseconds).

For an example of a test that launches multiple nodes, see
`chunks_produced_and_distributed_common` in
[tests/chunks_management.rs](https://github.com/nearprotocol/nearcore/blob/master/chain/client/tests/chunks_management.rs).
The `setup_mock_all_validators` function is the key piece of infrastructure here.

## Runtime

Tests for Runtime are listed in
[tests/test_cases_runtime.rs](https://github.com/near/nearcore/blob/master/tests/test_cases_runtime.rs).

To run a test, usually, a mock `RuntimeNode` is created via `create_runtime_node()`.
In its constructor, the `Runtime` is created in the
`get_runtime_and_trie_from_genesis` function.

Inside a test, an abstracted `User` is used for sending specific actions to the
runtime client. The helper functions `function_call`, `deploy_contract`, etc.
eventually lead to the `Runtime.apply` method call.

For setting usernames during playing with transactions, use default names
`alice_account`, `bob_account`, `eve_dot_alice_account`, etc.

## Network

<!-- TODO: Explain the `runner` here -->

## Chain, Epoch Manager, Runtime and other low-level changes

When building new features in the `chain`, `epoch_manager` and `network` crates,
make sure to build new components sufficiently abstract so that they can be tested
without relying on other components.

For example, see tests for doomslug
[here](https://github.com/nearprotocol/nearcore/blob/master/chain/chain/tests/doomslug.rs),
for network cache
[here](https://github.com/nearprotocol/nearcore/blob/master/chain/network/tests/cache_edges.rs),
or for promises in runtime
[here](https://github.com/nearprotocol/nearcore/blob/master/runtime/near-vm-logic/tests/test_promises.rs).

## Python tests

See
[this page](https://github.com/nearprotocol/nearcore/wiki/Writing-integration-tests-for-nearcore)
for detailed coverage of how to write a python test.

We have a python library that allows one to create and run python tests.

To run python tests, from the `nearcore` repo the first time, do the following:

```shell
cd pytest
virtualenv . --python=python3
. .env/bin/activate
pip install -r requirements.txt
python tests/sanity/block_production.py
```

This will create a python virtual environment, activate the environment, install
all the required packages specified in the `requirements.txt` file and run the
`tests/sanity/block_production.py` file. After the first time, we only need to
activate the environment and can then run the tests:

```shell
cd pytest
. .env/bin/activate
python tests/sanity/block_production.py
```

Use `pytest/tests/sanity/block_production.py` as the basic example of starting a
cluster with multiple nodes, and doing RPC calls.

See `pytest/tests/sanity/deploy_call_smart_contract.py` to see how contracts can
be deployed, or transactions called.

See `pytest/tests/sanity/staking1.py` to see how staking transactions can be
issued.

See `pytest/tests/sanity/state_sync.py` to see how to delay the launch of the
whole cluster by using `init_cluster` instead of `start_cluster`, and then
launching nodes manually.

### Enabling adversarial behavior

To allow testing adversarial behavior, or generally, behaviors that a node should
not normally exercise, we have certain features in the code decorated with
`#[cfg(feature="adversarial")]`. The binary normally is compiled with the
feature disabled, and when compiled with the feature enabled, it traces a
warning on launch.

The nightly runner runs all the python tests against the binary compiled with
the feature enabled, and thus the python tests can make the binary perform
actions that it normally would not perform.

The actions can include lying about the known chain height, producing multiple
blocks for the same height, or disabling doomslug.

See all the tests under `pytest/tests/adversarial` for some examples.
