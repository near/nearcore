#!/usr/bin/env python3
"""
Runs a loadtest on mocknet.

The setup requires you to have a few nodes that will be validators and at least one node that will be an RPC node.
Specify the number of validator nodes using the `--num-nodes` flag.

Use https://github.com/near/near-ops/tree/master/provisioning/terraform/network/mocknet to bring up a set of VM
instances for the test. It also takes care of downloading necessary binaries and smart contracts.

Depending on the `--chain-id` flag, the nodes will be initialized with an empty state, or with the most recent state
dump of `mainnet`, `testnet` and `betanet`. Note that `mainnet` and `testnet` state dumps are huge. Starting a node with
`testnet` state dump takes about 1.5 hours.

You can run multiple tests in parallel, use `--pattern` to disambiguate.

Configure the desirable generated load with the `--max-tps` flag, or disable load altogether with `--skip-load`.

Example from the recent loadtest run:
1) terraform apply -var="chain_id=mainnet" -var="size=big" -var="override_chain_id=rc3-22" -var="neard_binary_url=https://s3.us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux/1.23.0/1eaa01d6abc76757b2ef50a1a127f98576b750c4/neard" -var="upgrade_neard_binary_url=https://near-protocol-public.s3.ca-central-1.amazonaws.com/mocknet/neard.rc3-22"
2) python3 tests/mocknet/load_test_spoon.py --chain-id=mainnet-spoon --pattern=rc3-22 --epoch-length=1000 --num-nodes=120 --max-tps=100 --script=add_and_delete --increasing-stakes=1 --progressive-upgrade --num-seats=100

Things to look out for when running the test:
1) Test init phase completes before any binary upgrades start.
2) At the beginning of each of the first epochs some nodes get upgraded.
3) If the protocol upgrades becomes effective at epoch T, check that binaries upgraded at epochs T-1, T-2 have started successfully.
4) BPS and TPS need to be at some sensible values.
5) CPU usage and RAM usage need to be reasonable as well.
6) Ideally we should check the percentage of generated transactions that succeed, but there is no easy way to do that. Maybe replay some blocks using `neard view_state apply_range --help`.

Other notes:
1) This grafana dashboard can help: https://grafana.near.org/d/jHbiNgSnz/mocknet
2) Logs are in /home/ubuntu/neard.log and /home/ubuntu/neard.upgrade.log

For the terraform command to work, please do the following:
1) Install terraform cli: https://learn.hashicorp.com/tutorials/terraform/install-cli
2) git clone https://github.com/near/near-ops
3) cd provisioning/terraform/network/mocknet
4) Run `terraform init`
5) Run `terraform apply` as specified above and check the output.
6) If you don't have permissions for the GCP project `near-mocknet`, please ask your friendly SREs to give you permissions.
7) Make the pair of neard binaries available for download. The pair of binaries is the baseline version, e.g. the current mainnet binary version, and your experimental binary. The simplest way to make the binaries available is to upload them to https://s3.console.aws.amazon.com/s3/buckets/near-protocol-public?region=ca-central-1&prefix=mocknet/
8) If you don't have access to S3 AWS, please ask your friendly SREs to give you access.
9) A reliable way to make an experimental binary compatible with mocknet instances, is to build it on machine "builder" in project "near-mocknet". You may need to "Resume" the instance in the GCP console: https://console.cloud.google.com/compute/instancesDetail/zones/us-central1-a/instances/builder?project=near-mocknet

For the `load_test_spoon.py` to work, please do the following:
1) git clone https://github.com/near/nearcore
2) cd pytest
3) Follow instructions in https://github.com/near/nearcore/blob/master/pytest/README.md to setup your Python environment
4) Run load_test_spoon.py as specified above.

"""
import argparse
import random
import sys
import time
from rc import pmap
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from helpers import load_test_spoon_helper
from helpers import load_testing_add_and_delete_helper
import mocknet
import data

from metrics import Metrics
from configured_logger import logger


def measure_tps_bps(nodes, tx_filename):
    input_tx_events = mocknet.get_tx_events(nodes, tx_filename)
    # drop first and last 5% of events to avoid edges of test
    n = int(0.05 * len(input_tx_events))
    input_tx_events = input_tx_events[n:-n]
    input_tps = data.compute_rate(input_tx_events)
    measurement = mocknet.chain_measure_bps_and_tps(nodes[-1],
                                                    input_tx_events[0],
                                                    input_tx_events[-1])
    result = {
        'bps': measurement['bps'],
        'in_tps': input_tps,
        'out_tps': measurement['tps']
    }
    logger.info(f'{result}')
    return result


def check_tps(measurement, expected_in, expected_out=None, tolerance=0.05):
    if expected_out is None:
        expected_out = expected_in
    almost_equal = lambda x, y: (abs(x - y) / y) <= tolerance
    return (almost_equal(measurement['in_tps'], expected_in) and
            almost_equal(measurement['out_tps'], expected_out))


def check_memory_usage(node):
    metrics = mocknet.get_metrics(node)
    mem_usage = metrics.memory_usage / 1e6
    logger.info(f'Memory usage (MB) = {mem_usage}')
    return mem_usage < 4500


def check_slow_blocks(initial_metrics, final_metrics):
    delta = Metrics.diff(final_metrics, initial_metrics)
    slow_process_blocks = delta.block_processing_time[
        'le +Inf'] - delta.block_processing_time['le 1']
    logger.info(
        f'Number of blocks processing for more than 1s: {slow_process_blocks}')
    return slow_process_blocks == 0


if __name__ == '__main__':
    logger.info('Starting Load test.')
    parser = argparse.ArgumentParser(description='Run a load test')
    parser.add_argument('--chain-id', required=True)
    parser.add_argument('--pattern', required=False)
    parser.add_argument('--epoch-length', type=int, required=True)
    parser.add_argument('--num-nodes', type=int, required=True)
    parser.add_argument('--max-tps', type=float, required=True)
    parser.add_argument('--increasing-stakes', type=float, default=0.0)
    parser.add_argument('--progressive-upgrade',
                        default=False,
                        action='store_true')
    parser.add_argument('--skip-load', default=False, action='store_true')
    parser.add_argument('--skip-setup', default=False, action='store_true')
    parser.add_argument('--skip-restart', default=False, action='store_true')
    parser.add_argument('--no-sharding', default=False, action='store_true')
    parser.add_argument('--script', required=True)
    parser.add_argument('--num-seats', type=int, required=True)

    args = parser.parse_args()

    chain_id = args.chain_id
    pattern = args.pattern
    epoch_length = args.epoch_length
    assert epoch_length > 0
    num_nodes = args.num_nodes
    assert num_nodes > 0
    max_tps = args.max_tps
    assert max_tps > 0

    all_nodes = mocknet.get_nodes(pattern=pattern)
    random.shuffle(all_nodes)
    assert len(all_nodes) > num_nodes, 'Need at least one RPC node'
    validator_nodes = all_nodes[:num_nodes]
    logger.info(f'validator_nodes: {validator_nodes}')
    rpc_nodes = all_nodes[num_nodes:]
    logger.info(
        f'Starting Load of {chain_id} test using {len(validator_nodes)} validator nodes and {len(rpc_nodes)} RPC nodes.'
    )

    upgrade_schedule = mocknet.create_upgrade_schedule(rpc_nodes,
                                                       validator_nodes,
                                                       args.progressive_upgrade,
                                                       args.increasing_stakes,
                                                       args.num_seats)
    logger.info(f'upgrade_schedule: %s' % str(upgrade_schedule))

    if not args.skip_setup:
        logger.info('Setting remote python environments')
        mocknet.setup_python_environments(all_nodes,
                                          'add_and_delete_state.wasm')
        logger.info('Setting remote python environments -- done')

    if not args.skip_restart:
        logger.info(f'Restarting')
        # Make sure nodes are running by restarting them.
        mocknet.stop_nodes(all_nodes)
        time.sleep(10)
        node_pks = pmap(lambda node: mocknet.get_node_keys(node)[0],
                        validator_nodes)
        all_node_pks = pmap(lambda node: mocknet.get_node_keys(node)[0],
                            all_nodes)
        node_ips = [node.machine.ip for node in all_nodes]
        mocknet.create_and_upload_genesis(
            validator_nodes,
            chain_id,
            rpc_nodes=rpc_nodes,
            epoch_length=epoch_length,
            node_pks=node_pks,
            increasing_stakes=args.increasing_stakes,
            num_seats=args.num_seats,
            single_shard=args.no_sharding,
            all_node_pks=all_node_pks,
            node_ips=node_ips)
        mocknet.start_nodes(all_nodes, upgrade_schedule)
        time.sleep(60)

    mocknet.wait_all_nodes_up(all_nodes)

    archival_node = rpc_nodes[0]
    logger.info(f'Archival node: {archival_node.instance_name}')
    initial_validator_accounts = mocknet.list_validators(archival_node)
    logger.info(f'initial_validator_accounts: {initial_validator_accounts}')
    test_passed = True

    script, deploy_time, test_timeout = (None, None, None)
    if args.script == 'skyward':
        script = 'load_testing_add_and_delete_helper.py'
        deploy_time = load_testing_add_and_delete_helper.CONTRACT_DEPLOY_TIME
        test_timeout = load_testing_add_and_delete_helper.TEST_TIMEOUT
    elif args.script == 'add_and_delete':
        script = 'load_test_spoon_helper.py'
        deploy_time = load_test_spoon_helper.CONTRACT_DEPLOY_TIME
        test_timeout = load_test_spoon_helper.TEST_TIMEOUT
    else:
        assert False, f'Unsupported --script={args.script}'

    if not args.skip_load:
        logger.info('Starting transaction spamming scripts.')
        mocknet.start_load_test_helpers(validator_nodes,
                                        script,
                                        rpc_nodes,
                                        num_nodes,
                                        max_tps,
                                        get_node_key=True)

        initial_epoch_height = mocknet.get_epoch_height(rpc_nodes, -1)
        logger.info(f'initial_epoch_height: {initial_epoch_height}')
        assert initial_epoch_height >= 0
        initial_metrics = mocknet.get_metrics(archival_node)
        start_time = time.monotonic()
        logger.info(
            f'Waiting for contracts to be deployed for {deploy_time} seconds.')
        prev_epoch_height = initial_epoch_height
        EPOCH_HEIGHT_CHECK_DELAY = 30
        while time.monotonic() - start_time < deploy_time:
            epoch_height = mocknet.get_epoch_height(rpc_nodes,
                                                    prev_epoch_height)
            if epoch_height > prev_epoch_height:
                mocknet.upgrade_nodes(epoch_height - initial_epoch_height,
                                      upgrade_schedule, all_nodes)
                prev_epoch_height = epoch_height
            time.sleep(EPOCH_HEIGHT_CHECK_DELAY)

        logger.info(
            f'Waiting for the loadtest to complete: {test_timeout} seconds')
        while time.monotonic() - start_time < test_timeout:
            epoch_height = mocknet.get_epoch_height(rpc_nodes,
                                                    prev_epoch_height)
            if epoch_height > prev_epoch_height:
                mocknet.upgrade_nodes(epoch_height - initial_epoch_height,
                                      upgrade_schedule, all_nodes)
                prev_epoch_height = epoch_height
            time.sleep(EPOCH_HEIGHT_CHECK_DELAY)

        final_metrics = mocknet.get_metrics(archival_node)
        logger.info('All transaction types results:')
        all_tx_measurement = measure_tps_bps(validator_nodes,
                                             f'{mocknet.TX_OUT_FILE}.0')
        if all_tx_measurement['bps'] < 0.5:
            test_passed = False
            logger.error(f'bps is below 0.5: {all_tx_measurement["bps"]}')
        if not check_memory_usage(validator_nodes[0]):
            test_passed = False
            logger.error('Memory usage too large')
        if not check_slow_blocks(initial_metrics, final_metrics):
            test_passed = False
            logger.error('Too many slow blocks')

    time.sleep(5)

    final_validator_accounts = mocknet.list_validators(validator_nodes[0])
    logger.info(f'final_validator_accounts: {final_validator_accounts}')
    if initial_validator_accounts != final_validator_accounts:
        test_passed = False
        logger.error(
            f'Mismatching set of validators:\n{initial_validator_accounts}\n{final_validator_accounts}'
        )

    assert test_passed
    logger.info('Load test complete.')
