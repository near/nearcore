# Mirror Guide

## Prerequisites

* GCP access (SRE)
* Infra-ops access (SRE)
* Install gcloud tools locally [link](https://cloud.google.com/sdk/docs/install)

## Creating the infrastructure

Running terraform apply take about 10 minutes, depending on how many hosts you need.

To setup the GCP hosts that you want to use in your mocknet, you need to create a Terraform recipe. 

Start in Infra-ops repo, in `infra-ops/provisioning/terraform/infra/network/mocknet` [folder (link)](https://github.com/Near-One/infra-ops/tree/main/provisioning/terraform/infra/network/mocknet/). Make a copy of `example-forknet` folder. 

Find the forknet image that you are going to use in your test by running `sh mirror-base/util.sh ls-test-cases`. If you are not sure, use the latest `START_HEIGHT`.

In your newly created folder, edit the `main.tf` file:

* **unique_id**: You will identify your forknet by this string. i.e. `my-test`, but not too long. GCP has a limit on names.
* **start_height**: `START_HEIGHT` **# the number from above**
* state_dumper : true/false #  When set to `true`, it creates the infrastructure needed for state sync, including a state part dumper and state parts bucket. If you don't plan to have your nodes change tracked shards, keep it `false`.
* **nodes_location** : here you add your nodes in different locations. Only use [these locations](https://github.com/Near-One/infra-ops/blob/main/ansible/prometheus-scrapper/update-config-prometheus.yml) .
* **tracing_server**: If you want to gather traces on a dedicated server.
* **machine_type** : Depending on your need "n2d-standard-16" has 64GB RAM while"n2d-standard-8" has 32GB.

Edit `resources.tf`:

* Set **prefix** = "state/infra/network/mocknet/**unique_id-start_height**"

  This is to allow other people to modify this network by pushing the state file to a gcp bucket

Once you have these files run `terraform init`, `terraform apply` (make sure to run these commands from the newly created directory, not from `mocknet` directory), and push your changes to a branch in the repo. This will allow others to amend / delete this network.

To save resources, destroy the infrastructure if you do not use it. You can do this with `terraform destroy`

## Configure the nodes

To configure and operate the nodes we use the `mirror.py` script from `nearcore` repo.

### Alias

Create an alias to the `mirror.py` tool with to make the commands shorter by replacing `YOUR_ID` and `YOUR_START_HEIGHT` with the values set in terraform.

```javascript
alias mirror="python3 tests/mocknet/mirror.py --chain-id mainnet --start-height YOUR_START_HEIGHT --unique-id YOUR_ID"
```


All `mirror` commands should be run from the `nearcore/pytest` directory, otherwise relative directories will break.

### Binary

Get a link to the binary that you want to test:

```javascript
NODE_BINARY_URL=https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux/master/neard
```

### Initialize the testing environment

This will upload the orchestrator (\`neard-runner\`) to all the hosts and create the necessary folders: 

```javascript
mirror init-neard-runner --neard-binary-url $NODE_BINARY_URL --neard-upgrade-binary-url ""
```

### \[Optional\] genesis configs

Reduce the min gas price to reduce the chance of running out of NEAR.

```javascript
mirror --host-type traffic run-cmd --cmd 'cp ~/.near/target/setup/genesis.json g.json ; jq ".min_gas_price = \"10000000\"" g.json > ~/.near/target/setup/genesis.json'
mirror --host-type nodes run-cmd --cmd 'cp ~/.near/setup/genesis.json g.json ; jq ".min_gas_price = \"10000000\"" g.json > ~/.near/setup/genesis.json'
```

### Create the a new test

```javascript
mirror new-test \
  --epoch-length 200 \
  --genesis-protocol-version 71 \
  --num-validators 20 \
  --num-seats 20 \
  --new-chain-id mocknet \
  --gcs-state-sync
```

`--epoch-length` If you need state sync, allow at least 5500.

`--genesis-protocol-version` Can be 1 less than the binary version. If you want to do 2 upgrades, you need to set a voting schedule in an ENV variable.

`--num-validators 20` How many of your GCP hosts you want to be added to the validator pool in genesis.

`--num-seats 20` How many block producers do you want in your network. (WIP. Currently a dummy parameter)

`--new-chain-id mocknet` Set the name of your chain id in genesis. Do not use `mainnet` or `testnet`. Typical value for this argument is `mocknet`. The chain_id in the grafana metrics is not set by this value, it is set by a value in terraform file. `chain_id` in metrics is always set to `mocknet.`

 `--gcs-state-sync` If you want to benefit from state sync, your nodes will be configured to sync from the bucket dedicated to your mocknet. You need to enable `state_dumper` in [terraform](https://docs.nearone.org/doc/mocknet-tips-7VnYUXjs2A#h-creating-the-infrastructure).


First time you run `new-test` it will take less than 1 minute for the nodes to be ready. Check the status with `mirror status`. If you run `new-test` on an existing network the process will take slightly longer because it needs to delete the existing `data` folder (2-3 minutes). 

### \[Optional\] Custom shard tracking

If you need to set full shard tracking on any node, you can change the configs like this:

```javascript
mirror --host-filter "<nodes regex>" update-config --set 'store.load_mem_tries_for_tracked_shards=false,tracked_shards=[0]'
```

### \[Optional\] Set logs

Note that `RUST_LOG`s are controlled by `log_config.json`.

```javascript
RUST_LOG="debug,network=info"
mirror --host-type nodes run-cmd --cmd "jq '.opentelemetry = \"${RUST_LOG}\" | .rust_log = \"${RUST_LOG}\"' /home/ubuntu/.near/log_config.json > tmp && mv tmp /home/ubuntu/.near/log_config.json"
mirror --host-type traffic run-cmd --cmd "jq '.opentelemetry = \"${RUST_LOG}\" | .rust_log = \"${RUST_LOG}\"' /home/ubuntu/.near/target/log_config.json > tmp && mv tmp /home/ubuntu/.near/target/log_config.json"
```


### \[Optional\] Set tracing server

```javascript
UNIQUE_ID=your unique id
SEARCH_STRING="$UNIQUE_ID-tracing"
SERVER_IP=$(gcloud compute instances list --project=nearone-mocknet | awk -v search="$SEARCH_STRING" '$0 ~ search {print $5}')
mirror env --key-value "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://$SERVER_IP:4317"
```

## Operating the network

### Start the network

```none
# Check if is ready with mirror status
# Start nodes with traffic
mirror start-traffic
# Or start without traffic
mirror start-nodes
```

### Stop the network

```none
# Stop all nodes and traffic
mirror stop-nodes

# Stop traffic 
mirror stop-traffic
```

### Reset the network

```none
mirror stop-nodes
mirror reset --backup-id start --yes
```

### Update the binary

Stop the nodes before altering the running binary.

If you are using the same URL: make a new build and run `update-binaries`:

```none
update-binaries     Update the neard binaries by re-downloading them. The same urls are used. If you plan to restart the network multiple times, it is recommended to use URLs that only depend on the branch name. This way, every time you
                    build, you will not need to amend the URL but just run update-binaries.
```

If you plan to use a binary from another branch, you need to alter the URL with `amend-binaries`:

```none
 amend-binaries      Add or override the neard URLs by specifying the epoch height or index if you have multiple binaries. If the network was started with 2 binaries, the epoch height for the second binary can be randomly assigned on each
                     host. Use caution when updating --epoch-height so that it will not add a binary in between the upgrade window for another binary.
```

```bash
URL=https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux/some_branch/neard
mirror amend-binaries --neard-binary-url $URL --binary-idx 0
```

The following option is not recommended, but, if the binary isn't available at a public URL, but you have it on your machine/laptop, you can upload it like:`gcloud --project near-mocknet compute scp {local_neard_path} ubuntu@{instance_name}:/home/ubuntu/.near/neard-runner/binaries/neard0`

### Run commands on the nodes

```javascript
mirror run-cmd --cmd "cat .near/neard-runner/.env"
```

### Target specific hosts

There are three selectors that can be combined

```javascript
  --host-type {all,nodes,traffic}
                        Type of hosts to select
  --host-filter HOST_FILTER
                        Filter through the selected nodes using regex.
  --select-partition SELECT_PARTITION
                        Input should be in the form of "i/n" where 0 < i <= n. Select a group of hosts based on the division provided. For i/n, it will split the selected hosts into n groups and select the i-th group. Use this if you want to
                        target just a partition of the hosts.
```

### Get mapped account data

In Forknet, all accounts are set up with a full-access key, allowing full control over all transactions made by those accounts. To retrieve this injected key, you can use the neard mirror show-keys tool on any mainnet database. This can be done using either a local database copy or an accessible RPC endpoint.  This setup is useful for testing purposes since having a full-access key on all accounts enables precise control over transactions and allows for easy testing and debugging across network simulations.

<!-- cspell:words poolv -->
```javascript
neard mirror show-keys from-rpc  --account-id "astro-stakers.poolv1.near" --rpc-url https://rpc.mainnet.near.org/

If it exists, this account probably has an extra full access key added:
mapped secret key: ed25519:5GnmuWueJptLxKYoirp6rHHDJpu7vLgM1BCXwfvc8CJ8cmoettg9vYVaN2mqJZPbiRcrqFuPb7AXjf2jCJyVpyNQ
public key: ed25519:93zQfXQsfWEkDG2n5qKfbTQUxLZdMrvGpBtwpezWpWTJ
```

### Check the logs

To get the logs of the `neard_runner.py` and see what commands were executed on the host during setup or operation, look at the `journalctl -ru neard-runner`

To check the `neard` logs look at the files in `~/neard-logs/logs.txt`. This file is rotated so it does not become too big.