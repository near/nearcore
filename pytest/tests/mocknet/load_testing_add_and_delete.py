# Force each node to submit many transactions for
# about 20 minutes. Monitor the block production time
# stays consistent.

import sys
import time
import random

sys.path.append('lib')

from rc import pmap

import load_testing_add_and_delete_helper
import mocknet
import data, utils

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


def check_tps(measurement, expected_in, expected_out=None, tolarance=0.05):
    if expected_out is None:
        expected_out = expected_in
    within_tolarance = lambda x, y: (abs(x - y) / y) <= tolarance
    return within_tolarance(measurement['in_tps'],
                            expected_in) and within_tolarance(
                                measurement['out_tps'], expected_out)


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
    all_nodes = mocknet.get_nodes(pattern='mocknet4-mocknet-')
    random.shuffle(all_nodes)
    assert len(all_nodes) >= load_testing_add_and_delete_helper.NUM_NODES, "%d %s" % (len(all_nodes),[node.instance_name for node in all_nodes])
    nodes = all_nodes[:load_testing_add_and_delete_helper.NUM_NODES]
    rpc_nodes = all_nodes[load_testing_add_and_delete_helper.NUM_NODES:]
    logger.info(
        f'Starting Load test using {len(nodes)} validator nodes and {len(rpc_nodes)} RPC nodes.'
    )

    if '--skip_restart' not in sys.argv:
        # Make sure nodes are running by restarting them.
        mocknet.stop_nodes(all_nodes)
        time.sleep(10)
        mocknet.create_and_upload_genesis(nodes,
                                          '../nearcore/res/genesis_config.json',
                                          rpc_nodes=rpc_nodes)
        mocknet.start_nodes(all_nodes)
        time.sleep(60)

    archival_node = rpc_nodes[0] if rpc_nodes else all_nodes[-1]
    logger.info(f'Archival node: {archival_node.instance_name}')
    initial_validator_accounts = mocknet.list_validators(archival_node)
    test_passed = True

    logger.info('Setting remote python environments.')
    mocknet.setup_python_environments(nodes, 'add_and_delete_state.wasm')
    logger.info('Starting transaction spamming scripts.')
    mocknet.start_load_test_helpers(nodes,
                                    'load_testing_add_and_delete_helper.py',
                                    rpc_nodes=rpc_nodes, get_node_key=True)

    initial_metrics = mocknet.get_metrics(archival_node)
    logger.info(
        f'Waiting for the skyward contract to be initialized for {load_testing_add_and_delete_helper.SKYWARD_INIT_TIME} seconds.'
    )
    time.sleep(load_testing_add_and_delete_helper.SKYWARD_INIT_TIME)
    logger.info(
        f'Waiting for contracts to be deployed for {load_testing_add_and_delete_helper.CONTRACT_DEPLOY_TIME} seconds.'
    )
    time.sleep(load_testing_add_and_delete_helper.CONTRACT_DEPLOY_TIME)
    logger.info(
        f'Waiting for the loadtest to complete: {load_testing_add_and_delete_helper.TEST_TIMEOUT} seconds'
    )
    time.sleep(load_testing_add_and_delete_helper.TEST_TIMEOUT)
    final_metrics = mocknet.get_metrics(archival_node)

    time.sleep(5)

    logger.info('All transaction types results:')
    all_tx_measurement = measure_tps_bps(nodes, f'{mocknet.TX_OUT_FILE}.0')
    test_passed = (all_tx_measurement['bps'] > 0.5) and test_passed
    test_passed = check_memory_usage(nodes[0]) and test_passed
    test_passed = check_slow_blocks(initial_metrics,
                                    final_metrics) and test_passed

    final_validator_accounts = mocknet.list_validators(nodes[0])
    assert initial_validator_accounts == final_validator_accounts

    assert test_passed

    logger.info('Load test complete.')
