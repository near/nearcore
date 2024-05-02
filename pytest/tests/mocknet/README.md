Mirror transactions from a given network into a custom mocktest network and add load

1. Setup a custom mocknet network following the [README](https://github.com/Near-One/infra-ops/blob/main/provisioning/terraform/infra/network/mocknet/mirror/README) in the `provisioning/terraform/infra/network/mocknet/mirror/` directory of the [Near-One/infra-ops repository](https://github.com/Near-One/infra-ops).
    - An example setup command should look like the following: `terraform apply -var="unique_id=stateless" -var="chain_id=mainnet" -var="start_height=116991260" -var="size=small"`

    - Use the same values of `unique_id`, `chain_id`, and `start_height` from this setup when running the mirror.py the commands below. 

2. Run `python3 tests/mocknet/mirror.py --chain-id {chain_id} --start-height {start_height} --unique-id {unique_id} init-neard-runner`, replacing the `{}`s with appropriate values from the `nearcore/pytest` directory. This starts a helper program on each node that will be in charge of the test state and neard process.

3. Run `python3 tests/mocknet/mirror.py --chain-id {chain_id} --start-height {start_height} --unique-id {unique_id} new-test`. This will take a few hours.

4. Run `python3 tests/mocknet/mirror.py --chain-id {chain_id} --start-height {start_height} --unique-id {unique_id} start-traffic` replacing the `{}`s with appropriate values

5. Monitoring
    - See metrics on grafana mocknet https://grafana.near.org/d/jHbiNgSnz/mocknet?orgId=1&refresh=30s&var-chain_id=All&var-node_id=.*unique_id.*&var-account_id=All replacing the "unique_id" with the value from earlier

If there's ever a problem with the neard runners on each node, for example if you get a connection error running the `status` command, run the `restart-neard-runner` command to restart them, which should be safe to do.

To run a locust load test on the mocknet network, run `python3 tests/mocknet/locust.py init --instance-names {}`, where
the instance names are VMs that have been prepared for this purpose, and then run `python3 tests/mocknet/locust.py run --master {master_instance_name} --workers {worker_instance_name0,worker_instance_name1,etc...} --funding-key {key.json} --node-ip-port {mocknet_node_ip}:3030`, where `mocknet_node_ip` is an IP address of a node that's been setup by the mirror.py script, and `key.json` is an account key that contains lots of NEAR for this load test. TODO: add extra accounts for load testing purposes during the mocknet setup step

# Running mocknet locally

If you want to set up a mocknet instance locally, the script in local_test_node.py can set this up. First you'll need a neard home directory with some transactions in it (there's no actual requirement that it have any transactions, but it might be more interesting if it does). To get this, you can run one of the pytests that generates transactions, or you can follow the instructions in the README in `pytest/tests/loadtest/locust`.

Suppose we've done that and the directory in `~/.near/localnet/node0` contains the state of one of our nodes. First, find the head of the chain:

```
neard --home ~/.near/localnet/node0 view-state view-chain
```

Then find a height of the chain that's a bit behind the head. There's no easy way to find this programatically at the timie of this writing, but you might try `neard --home ~/.near/localnet/node0 view-state view-chain --height {HEIGHT}` where `$HEIGHT` is maybe 100 blocks behind the head. For this earlier height that we choose as the fork height, there also happens to be a requirement that the first block of the epoch it belongs to should be included in the home directory as well. (This is required by the dump-state command, and maybe could be removed in the future) So if you choose some height and the below setup command fails at `view-state dump-state`, you might need to choose a later height. So, say the head of the chain is 400, and we want to fork at height 300. Then to set up a local mocknet, run:

```
python3 tests/mocknet/local_test_node.py local-test-setup --yes --num-nodes 2 --source-home-dir ~/.near/localnet/node0 --neard-binary-path ~/nearcore/target/debug/neard --fork-height 300 --legacy-records
```

Then you can run the normal `mirror.py` commands to control this mocknet instance, passing it `--local-test` instead of the usual `--start-height`, `--chain-id` and `--unique-id` flags:

```
python3 tests/mocknet/mirror.py --local-test new-test
python3 tests/mocknet/mirror.py --local-test start-traffic
```

The above `local_test_node.py` command will set up directories in `~/.near/local-mocknet` where state has been forked from `~/.near/localnet/node0` at height 300. The `--legacy-records` argument tells us to use the older records.json method of forking state instead of the newer `neard fork-network` method. If you want to use that one, it's a bit more involved since you'll need two NEAR directories, where one has some head height, say N, and the other should have a head height M > N, *and* it should include height N as well. This will be the directory we use for the traffic generation, which is why it should include height N so that we can start sending transactions from that point. If you're using locust traffic sent to a localnet for the source chain, one way to achieve this is to start a localnet with, say, four nodes (at least that many so the chain doesn't stall when you stop one of them). Then as you're sending locust traffic, stop one of the nodes and leave the others running for a little while longer (but not so long that they end up garbage collecting the block at which the stopped node went offline). Then if `~/.near/localnet/node1` is the home directory for the node that was stopped early, and `~/.near/localnet/node0` is the home directory for one of the other nodes, you can set up the local mocknet with:

```
python3 tests/mocknet/local_test_node.py local-test-setup --yes --num-nodes 2 --source-home-dir ~/.near/localnet/node0 --neard-binary-path ~/nearcore/target/debug/neard --target-home-dir ~/.near/localnet/node1
```

And then run the mirror.py commands as explained above.
