#!/usr/bin/env python3

help_str = """
This script starts or updates a network of adversenet, in which some validators may be malicious.

The setup requires you to have a few nodes that will be validators and at least one node that will be an RPC node.
The script will recognize any node that has "validator" in its name as a validator, of which any node that has a
"bad" in its name as an adversarial node.

Use https://github.com/near/near-ops/tree/master/provisioning/terraform/network/adversenet to bring up a set of VM
instances for the test.
"""

import argparse
import sys
import time
from enum import Enum
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

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
    """
    if "bad" in node.instance_name:
        config["adversarial"] = {
            "produce_duplicate_blocks": True
        }
    """


class Role(Enum):
    Rpc = 0
    GoodValidator = 1
    BadValidator = 2


def get_role(node):
    if "validator" not in node.instance_name:
        return Role.Rpc
    elif "bad" in node.instance_name:
        return Role.BadValidator
    else:
        return Role.GoodValidator


if __name__ == '__main__':
    logger.info('Starting adversenet.')
    parser = argparse.ArgumentParser(description=help_str)
    parser.add_argument(
        'mode',
        choices=["new", "update"],
        help=
        "new: start a new network from scratch, update: update existing network"
    )
    parser.add_argument('--chain-id', required=False, default="adversenet")
    parser.add_argument('--pattern',
                        required=False,
                        default="adversenet-node-",
                        help="pattern to filter the gcp instance names")
    parser.add_argument(
        '--epoch-length',
        type=int,
        required=False,
        default=60,
        help="epoch length of the network. Only used when mode == new")
    parser.add_argument(
        '--num-seats',
        type=int,
        required=False,
        default=100,
        help="number of validator seats. Only used when mode == new")
    parser.add_argument('--bad-stake',
                        required=False,
                        default=5,
                        type=int,
                        help="Total stake percentage for bad validators")
    parser.add_argument('--binary-url',
                        required=False,
                        help="url to download neard binary")

    args = parser.parse_args()

    chain_id = args.chain_id
    pattern = args.pattern
    epoch_length = args.epoch_length
    assert epoch_length > 0

    all_nodes = mocknet.get_nodes(pattern=pattern)
    rpcs = [n.instance_name for n in all_nodes if get_role(n) == Role.Rpc]
    good_validators = [
        n.instance_name for n in all_nodes if get_role(n) == Role.GoodValidator
    ]
    bad_validators = [
        n.instance_name for n in all_nodes if get_role(n) == Role.BadValidator
    ]
    TOTAL_STAKE = 1000000
    bad_validator_stake = int(TOTAL_STAKE * args.bad_stake /
                              (100 * len(bad_validators)))
    good_validator_stake = int(TOTAL_STAKE * (100 - args.bad_stake) /
                               (100 * len(good_validators)))

    logger.info(f'Starting chain {chain_id} with {len(all_nodes)} nodes. \n\
        Good validators: {good_validators} each with stake {good_validator_stake} NEAR\n\
        Bad validators: {bad_validators} each with stake {bad_validator_stake} NEAR\n\
        RPC nodes: {rpcs}\n')

    answer = input("Enter y to continue: ")
    if answer != "y":
        exit(0)

    mocknet.stop_nodes(all_nodes)
    time.sleep(10)
    validator_nodes = [n for n in all_nodes if get_role(n) != Role.Rpc]
    rpc_nodes = [n for n in all_nodes if get_role(n) == Role.Rpc]
    if args.binary_url:
        mocknet.redownload_neard(all_nodes, args.binary_url)
    if args.mode == "new":
        logger.info(f'Configuring nodes from scratch')
        mocknet.clear_data(all_nodes)
        mocknet.create_and_upload_genesis_file_from_empty_genesis(
            # Give bad validators less stake.
            [(node, bad_validator_stake * mocknet.ONE_NEAR if get_role(node)
              == Role.BadValidator else good_validator_stake * mocknet.ONE_NEAR)
             for node in validator_nodes],
            rpc_nodes,
            chain_id,
            epoch_length=epoch_length,
            num_seats=args.num_seats)
        mocknet.create_and_upload_config_file_from_default(
            all_nodes, chain_id, override_config)
    else:
        mocknet.update_existing_config_file(all_nodes, override_config)
    mocknet.start_nodes(all_nodes)
    mocknet.wait_all_nodes_up(all_nodes)

    # TODO: send loadtest traffic.
