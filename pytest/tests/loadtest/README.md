# Loadtest

This test requires a few steps. Firstly, build the binary:

```shell
make neard-release
```

Secondly, initialise your own localnet:

```shell
./target/release/neard --home ~/.near_tmp init --chain-id localnet --num-shards=5
```

Thirdly, create accounts and deploy the contract:

```shell
python3 pytest/tests/loadtest/setup.py --home ~/.near_tmp --num_accounts=5
```

And lastly, run the test:

```shell
python3 pytest/tests/loadtest/loadtest.py --home ~/.near_tmp --num_accounts=5 --num_requests=1000
```

# Load Test version 2

The newer loadtest2.py script currently runs an intense load test with the FT contract.

Much like with the earlier version you will want to build a `neard`. This script can set up a (2
node) cluster for you (nice for testing):

```
env NEAR_ROOT=../target/release/ python3 tests/loadtest/loadtest2.py --fungible-token-wasm=$PWD/../../FT/res/fungible_token.wasm --setup-cluster --accounts=1000 --executors=4
```

Or, you can set up a network yourself, and point the script at your local node’s RPC endpoint:

```
env NEAR_ROOT=../target/release/ python3 tests/stress/perf_ft_transfer.py --fungible-token-wasm=$PWD/../../FT/res/fungible_token.wasm --accounts=1000 --executors=4 --contract-key=~/.near/node.json
```

As seen in commands above, you will need a fungible token contract to test with. There's one you
can get from the `near/near-examples` repository.
