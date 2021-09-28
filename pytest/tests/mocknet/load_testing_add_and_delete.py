# Force each node to submit many transactions for
# about 20 minutes. Monitor the block production time
# stays consistent.

import sys, time

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
    nodes = mocknet.get_nodes()
    logger.info(f'Starting Load test using {len(nodes)} nodes.')

    if '--skip_restart' not in sys.argv:
        # Make sure nodes are running by restarting them.
        mocknet.stop_nodes(nodes)
        time.sleep(10)
        mocknet.create_and_upload_genesis(
            nodes, '../nearcore/res/genesis_config.json')
        mocknet.start_nodes(nodes)
        time.sleep(60)

    archival_node = nodes[-1]
    logger.info(f'Archival node: {archival_node.instance_name}')
    initial_validator_accounts = mocknet.list_validators(archival_node)
    test_passed = True

    logger.info('Performing baseline block time measurement')
    # We do not include tps here because there are no transactions on mocknet normally.
    if '--skip_initial_sleep' not in sys.argv:
        time.sleep(120)
    baseline_measurement = mocknet.chain_measure_bps_and_tps(
        archival_node=archival_node,
        start_time=None,
        end_time=None,
        duration=120)
    baseline_measurement['in_tps'] = 0.0
    # quit test early if the baseline is poor.
    assert baseline_measurement['bps'] > 1.0
    logger.info(f'{baseline_measurement}')
    logger.info('Baseline block time measurement complete')

    logger.info('Setting remote python environments.')
    mocknet.setup_python_environments(nodes, 'add_and_delete_state.wasm')
    logger.info('Starting transaction spamming scripts.')
    mocknet.start_load_test_helpers(nodes,
                                    'load_testing_add_and_delete_helper.py')

    initial_metrics = mocknet.get_metrics(archival_node)
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
