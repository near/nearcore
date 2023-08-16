Mirror transactions from a given network into a custom mocktest network and add load

1. Setup a custom mocknet network following the README in the `provisioning/terraform/network/mocknet/mirror/` directory of the [near-ops](https://github.com/near/near-ops) repo.
- you'll need the `unique_id`, `chain_id`, and `start_height` from this setup when running the mirror.py test script in 2.
2. Run `python3 tests/mocknet/mirror.py --chain-id {chain_id} --start-height {start_height} --unique-id {unique_id} init-neard-runner`, replacing the `{}`s with appropriate values from the `nearcore/pytest` directory. This starts a helper program on each node that will be in charge of the test state and neard process.
3. Run `python3 tests/mocknet/mirror.py --chain-id {chain_id} --start-height {start_height} --unique-id {unique_id} new-test`. This will take a few hours.
4. Run `python3 tests/mocknet/mirror.py --chain-id {chain_id} --start-height {start_height} --unique-id {unique_id} start-traffic` replacing the `{}`s with appropriate values
4. Monitoring
- See metrics on grafana mocknet https://grafana.near.org/d/jHbiNgSnz/mocknet?orgId=1&refresh=30s&var-chain_id=All&var-node_id=.*unique_id.*&var-account_id=All replacing the "unique_id" with the value from earlier

If there's ever a problem with the neard runners on each node, for example if you get a connection error running the `status` command, run the `restart-neard-runner` command to restart them, which should be safe to do.

To run a locust load test on the mocknet network, run `python3 tests/mocknet/locust.py init --instance-names {}`, where
the instance names are VMs that have been prepared for this purpose, and then run `python3 tests/mocknet/locust.py run --master {master_instance_name} --workers {worker_instance_name0,worker_instance_name1,etc...} --funding-key {key.json} --node-ip-port {mocknet_node_ip}:3030`, where `mocknet_node_ip` is an IP address of a node that's been setup by the mirror.py script, and `key.json` is an account key that contains lots of NEAR for this load test. TODO: add extra accounts for load testing purposes during the mocknet setup step