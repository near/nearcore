# Loadtest 

This test consists of two parts:


Init your own localnet:

```shell
./target/release/neard --home ~/.near_tmp init --chain-id localnet --num-shards=5
```


Create accounts and deploy the contract:

```shell
python3 pytest/tests/loadtest/setup.py --home=~/.near_tmp --num_accounts=5
```

Run the test:

```shell
python3 pytest/tests/loadtest/loadtest.py --home=~/.near_tmp --num_accounts=5 --num_requests=1000
```
