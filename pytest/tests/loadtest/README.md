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
