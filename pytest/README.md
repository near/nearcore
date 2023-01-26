# Python-based tests

The directory contains Python-based tests.  The tests are run as part
of nightly testing on NayDuck though they can be run locally as well.

There is no set format of what the tests do but they typical start
a local test cluster using neard binary at `../target/debug/neard`.
There is also some capacity of starting the cluster on remote
machines.


## Running tests

### Running tests locally

To run tests locally first compile a debug build of the nearcore
package, make sure that all required Python packages are installed and
then execute the test file using python.  For example:

    cargo build
    cd pytest
    python3 -m pip install -U -r requirements.txt
    python3 tests/sanity/one_val.py

After the test finishes, log files and other result data from running
each node will be located in a `~/.near/test#_finished` directory
(where `#` is index of the node starting with zero).

Note that running the tests using `pytest` command is not supported
and won’t work reliably.

Furthermore, running multiple tests at once is not supported either
because tests often use hard-coded paths (e.g. `~/.node/test#` for
node home directories) and port numbers

### Ruining tests on NayDuck

As mentioned, the tests are normally run nightly on NayDuck.  To
schedule a run on NayDuck manual the `../scripts/nayduck.py` script is
used.  The `../nightly/README.md` file describes this is more detail.

### Running pytest remotely

The test library has code for executing tests while running the nodes
on remote Google Cloud machines.  Presumably that code worked in the
past but I, mina86, haven’t tried it and am a bit sceptical as to
whether it is still functional.  Regardless, for anyone who wants to
try it out, the instructions are as follows:

Prerequisites:

1. Same as local pytest
2. gcloud cli in PATH

Steps:

1. Choose or upload a near binary here: https://console.cloud.google.com/storage/browser/nearprotocol_nearcore_release?project=near-core
2. Fill the binary filename in remote.json.  Modify zones as needed,
   they’ll be used in round-robin manner.
3. `NEAR_PYTEST_CONFIG=remote.json python tests/...`
4. Run `python tests/delete_remote_nodes.py` to make sure the remote
   nodes are shut down properly (especially if tests failed early).


## Creating new tests

To add a test simply create a Python script inside of the `tests`
directory and add it to a test set file in `../nightly` directory.
See `../nightly/README.md` file for detailed documentation of the test
set files.  Note that if you add a test file but don’t include it in
nightly test set the pull request check will fail.

Even though this directory is called `pytest`, the tests need to work
when executed via `python3`.  This means that they need to execute the
tests when run as the main module rather than just defining the tests
function.  To make that happen it’s best to implement the tests using
the python's unittest framework but trigger them manually from within
the `__main__` condition like so:

    if __name__ == "__main__":
        unittest.main()

Alternatively, using the legacy way, the tests can be defined as
`test_<foo>` functions with test bodies and than executed in
a code fragment guarded by `if __name__ == '__main__'` condition.

If the test operates on the nodes running in a cluster, it will very
likely want to make use of `start_cluster` function defined in the
`lib/cluster.py` module.

Rather than assuming location a temporary directory, well-behaved test
should use `tempfile` module instead which will automatically take
`TEMPDIR` variable into consideration.  This is especially important
for NayDuck which will automatically cleanup after a test which
respects `TEMPDIR` directory even if that tests ends up not cleaning
up its temporary files.

For example, a simple test for checking implementation of
`max_gas_burnt_view` could be located in
`tests/sanity/rpc_max_gas_burnt.py` and look as follows:

    """Test max_gas_burnt_view client configuration.

    Spins up two nodes with different max_gas_burnt_view client
    configuration, deploys a smart contract and finally calls a view
    function against both nodes expecting the one with low
    max_gas_burnt_view limit to fail.
    """

    import sys
    import base58
    import base64
    import json
    import pathlib

    sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
    from cluster import start_cluster
    from utils import load_binary_file
    import transaction


    def test_max_gas_burnt_view():
        nodes = start_cluster(2, 0, 1,
                              config=None,
                              genesis_config_changes=[],
                              client_config_changes={
                                  1: {'max_gas_burnt_view': int(5e10)}
                              })

        contract_key = nodes[0].signer_key
        contract = load_binary_file(
            '../runtime/near-test-contracts/res/test_contract_rs.wasm')

        # Deploy the fib smart contract
        latest_block_hash = nodes[0].get_latest_block().hash
        deploy_contract_tx = transaction.sign_deploy_contract_tx(
            contract_key, contract, 10,
            base58.b58decode(latest_block_hash.encode('utf8')))
        deploy_contract_response = (
            nodes[0].send_tx_and_wait(deploy_contract_tx, 10))

        def call_fib(node, n):
            args = base64.b64encode(bytes([n])).decode('ascii')
            return node.call_function(
                contract_key.account_id, 'fibonacci', args,
                timeout=10
            ).get('result')

        # Call view function of the smart contract via the first
        # node.  This should succeed.
        result = call_fib(nodes[0], 25)
        assert 'result' in result and 'error' not in result, (
            'Expected "result" and no "error" in response, got: {}'
                .format(result))

        # Same but against the second node.  This should fail.
        result = call_fib(nodes[1], 25)
        assert 'result' not in result and 'error' in result, (
            'Expected "error" and no "result" in response, got: {}'
                .format(result))
        error = result['error']
        assert 'HostError(GasLimitExceeded)' in error, (
            'Expected error due to GasLimitExceeded but got: {}'.format(error))


    if __name__ == '__main__':
        test_max_gas_burnt_view()

### NayDuck environment

When executed on NayDuck, tests have access to `neard`,
`genesis-populate` and `restaked` binaries in `../target/debug` or
`../target/release` directory (depending if the test has been
scheduled with `--release` flag) just as if they were executed on
local machine.  Similarly, freshly built NEAR test contracts will be
located in `../runtime/near-test-contracts/res` directory.

The `NAYDUCK=1`, `NIGHTLY_RUNNER=1` and `NAYDUCK_TIMEOUT=<timeout>`
environment variables are set when tests are run on NayDuck.  If
necessary and no other option exists, the first two can be used to
change test’s behaviour to accommodate it running on the testing
infrastructure as opposed to local machine.  Meanwhile,
`NAYDUCK_TIMEOUT` specifies how much time in seconds test has to run
before NayDuck decides the test failed.

### Code Style

To automate formatting and avoid excessive bike shedding, we're using
YAPF to format Python source code in the pytest directory.  It can be
installed from Python Package Index (PyPI) using `pip` tool:

    python3 -m pip install yapf

Once installed, it can be run either on a single file, for example
with the following command:

    python3 -m yapf -pi lib/cluster.py

or the entire directory with command as seen below:

    python3 -m yapf -pir .

The `-p` switch enables parallelism and `-i` applies the changes in
place.  Without the latter switch the tool will write formatted file
to standard output instead.

The command should be executed in the `pytest` directory so that it’ll
pick up configuration from the `.style.yapf` file.
