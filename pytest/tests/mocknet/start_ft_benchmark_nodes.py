#!/usr/bin/env python3
"""
Starts nodes for FT transfer benchmark.
"""
import argparse
import random
import sys
import time
from rc import pmap
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import mocknet

from configured_logger import logger


if __name__ == '__main__':
    logger.info('Starting Load test.')
    parser = argparse.ArgumentParser(description='Start nodes for FT benchmark')
    parser.add_argument('--chain-id', required=True)
    parser.add_argument('--pattern', required=False)
    parser.add_argument('--epoch-length', type=int, default=1000)
    parser.add_argument('--num-nodes', type=int, required=True)
    parser.add_argument('--skip-setup', default=False, action='store_true')
    parser.add_argument('--skip-restart', default=False, action='store_true')
    parser.add_argument('--no-sharding', default=False, action='store_true')
    parser.add_argument('--num-seats', type=int, required=True)

    args = parser.parse_args()

    chain_id = args.chain_id
    pattern = args.pattern
    epoch_length = args.epoch_length
    assert epoch_length > 0
    num_nodes = args.num_nodes
    assert num_nodes > 0

    all_nodes = mocknet.get_nodes(pattern=pattern)
    random.shuffle(all_nodes)
    assert len(all_nodes) > num_nodes, 'Need at least one RPC node'
    validator_nodes = all_nodes[:num_nodes]
    logger.info(f'validator_nodes: {validator_nodes}')
    rpc_nodes = all_nodes[num_nodes:]
    logger.info(
        f'Starting Load of {chain_id} test using {len(validator_nodes)} validator nodes and {len(rpc_nodes)} RPC nodes.'
    )

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
        pmap(lambda node: mocknet.init_validator_key(node), all_nodes)
        node_ips = [node.machine.ip for node in all_nodes]
        mocknet.create_and_upload_genesis(
            validator_nodes,
            chain_id,
            rpc_nodes=rpc_nodes,
            epoch_length=epoch_length,
            node_pks=node_pks,
            num_seats=args.num_seats,
            single_shard=args.no_sharding,
            all_node_pks=all_node_pks,
            node_ips=node_ips)
        mocknet.start_nodes(all_nodes)
        time.sleep(60)

    mocknet.wait_all_nodes_up(all_nodes)
