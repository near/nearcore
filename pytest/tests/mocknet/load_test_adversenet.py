#!/usr/bin/env python3
"""
Runs a loadtest on adversenet.

The setup requires you to have a few nodes that will be validators and at least one node that will be an RPC node.
The script will recognize any node that has "validator" in its name as a validator, of which any node that has a
"bad" in its name as an adversarial node.

Use https://github.com/near/near-ops/tree/master/provisioning/terraform/network/adversenet to bring up a set of VM
instances for the test.

This script will always initializes the network from scratch, and that is the intended purpose of adversenet.
(Because the point of adversenet is to break the network, it is likely the network is in a bad state, so that's
why it's a good idea to just always start from scratch.)
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

def override_config(node, config):
    # Add config here depending on the specific node build.
    pass

if __name__ == '__main__':
    logger.info('Starting Load test.')
    parser = argparse.ArgumentParser(description='Run a load test')
    parser.add_argument('--chain-id', required=False, default="adversenet")
    parser.add_argument('--pattern', required=False, default="adversenet-node-")
    parser.add_argument('--epoch-length', type=int, required=False, default=60)
    parser.add_argument('--skip-setup', default=False, action='store_true')
    parser.add_argument('--skip-reconfigure', default=False, action='store_true')
    parser.add_argument('--num-seats', type=int, required=False, default=100)
    parser.add_argument('--binary-url', required=False)
    parser.add_argument('--clear-data', required=False, default=False, action='store_true')

    args = parser.parse_args()

    chain_id = args.chain_id
    pattern = args.pattern
    epoch_length = args.epoch_length
    assert epoch_length > 0

    all_nodes = mocknet.get_nodes(pattern=pattern)
    validator_nodes = [node for node in all_nodes if 'validator' in node.instance_name]
    logger.info(f'validator_nodes: {validator_nodes}')
    rpc_nodes = [node for node in all_nodes if 'validator' not in node.instance_name]
    logger.info(
        f'Starting Load of {chain_id} test using {len(validator_nodes)} validator nodes and {len(rpc_nodes)} RPC nodes.'
    )

    mocknet.stop_nodes(all_nodes)
    time.sleep(10)
    if not args.skip_reconfigure:
        logger.info(f'Reconfiguring nodes')
        # Make sure nodes are running by restarting them.

        if args.clear_data:
            mocknet.clear_data(all_nodes)
        mocknet.create_and_upload_genesis_file_from_empty_genesis(
            # Give bad validators less stake.
            [(node, 1 if 'bad' in node.instance_name else 5) for node in validator_nodes],
            rpc_nodes,
            chain_id,
            epoch_length=epoch_length,
            num_seats=args.num_seats)
        mocknet.create_and_upload_config_file_from_default(all_nodes, chain_id, override_config)
    if args.binary_url:
        mocknet.redownload_neard(all_nodes, args.binary_url)
    mocknet.start_nodes(all_nodes)
    mocknet.wait_all_nodes_up(all_nodes)

    # TODO: send loadtest traffic.