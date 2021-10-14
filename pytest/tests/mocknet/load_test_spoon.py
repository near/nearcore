# Force each node to submit many transactions for
# about 20 minutes. Monitor the block production time
# stays consistent.

import sys
import time
import random

sys.path.append('lib')

from rc import pmap

import load_test_spoon_helper
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


def check_memory_usage(node):
    metrics = mocknet.get_metrics(node)
    mem_usage = metrics.memory_usage / 1e6
    logger.info(f'Memory usage (MB) = {mem_usage}')
    return mem_usage < 4500


if __name__ == '__main__':
    logger.info('Starting Load test.')

    chain_id = None
    for arg in sys.argv:
        if '--chain_id' in arg:
            assert not chain_id
            chain_id = arg.split('=')[1]
    assert chain_id, 'Please add an argument --chain_id=testnet-spoon or similar'

    pattern = None
    for arg in sys.argv:
        if '--pattern' in arg:
            assert not pattern
            pattern = arg.split('=')[1]
    assert pattern, 'Please add an argument --pattern=mocknet2 or similar'

    epoch_length = None
    for arg in sys.argv:
        if '--epoch_length' in arg:
            assert not epoch_length
            epoch_length = int(arg.split('=')[1])
    assert epoch_length, 'Please add an argument --epoch_length=10000 or similar'

    num_nodes = None
    for arg in sys.argv:
        if '--num_nodes' in arg:
            assert not num_nodes
            num_nodes = int(arg.split('=')[1])
    assert num_nodes, 'Please add an argument --num_nodes=100 or similar'

    max_tps = None
    for arg in sys.argv:
        if '--max_tps' in arg:
            assert not max_tps
            max_tps = int(arg.split('=')[1])
    assert max_tps, 'Please add an argument --num_nodes=100 or similar'

    all_nodes = mocknet.get_nodes(pattern=pattern)
    random.shuffle(all_nodes)
    assert len(all_nodes) > num_nodes
    validator_nodes = all_nodes[:num_nodes]
    rpc_nodes = all_nodes[num_nodes:]
    logger.info(
        f'Starting Load of {chain_id} test using {len(validator_nodes)} validator nodes and {len(rpc_nodes)} RPC nodes.'
    )

    if '--skip_setup' not in sys.argv:
        logger.info('Setting remote python environments')
        mocknet.setup_python_environments(all_nodes,
                                          'add_and_delete_state.wasm')
        logger.info('Setting remote python environments -- done')

    if '--skip_restart' not in sys.argv:
        # Make sure nodes are running by restarting them.
        mocknet.stop_nodes(all_nodes)
        time.sleep(10)
        mocknet.create_and_upload_genesis(validator_nodes,
                                          genesis_template_filename=None,
                                          rpc_nodes=rpc_nodes,
                                          chain_id=chain_id,
                                          update_genesis_on_machine=True,
                                          epoch_length=epoch_length)
        mocknet.start_nodes(all_nodes)
        time.sleep(60)

    mocknet.wait_all_nodes_up(all_nodes)

    archival_node = rpc_nodes[0]
    logger.info(f'Archival node: {archival_node.instance_name}')
    initial_validator_accounts = mocknet.list_validators(archival_node)
    logger.info(f'initial_validator_accounts: {initial_validator_accounts}')
    test_passed = True

    if '--skip_load' not in sys.argv:
        logger.info('Starting transaction spamming scripts.')
        mocknet.start_load_test_helpers(validator_nodes,
                                        'load_test_spoon_helper.py', rpc_nodes,
                                        num_nodes, max_tps)

        initial_metrics = mocknet.get_metrics(archival_node)
        logger.info(
            f'Waiting for contracts to be deployed for {load_test_spoon_helper.CONTRACT_DEPLOY_TIME} seconds.'
        )
        time.sleep(load_test_spoon_helper.CONTRACT_DEPLOY_TIME)
        logger.info(
            f'Waiting for the loadtest to complete: {load_test_spoon_helper.TEST_TIMEOUT} seconds'
        )
        time.sleep(load_test_spoon_helper.TEST_TIMEOUT)
        final_metrics = mocknet.get_metrics(archival_node)
        logger.info('All transaction types results:')
        all_tx_measurement = measure_tps_bps(validator_nodes,
                                             f'{mocknet.TX_OUT_FILE}.0')
        test_passed = (all_tx_measurement['bps'] > 0.5) and test_passed
        test_passed = check_memory_usage(validator_nodes[0]) and test_passed

    time.sleep(5)

    final_validator_accounts = mocknet.list_validators(validator_nodes[0])
    logger.info(f'final_validator_accounts: {final_validator_accounts}')
    assert initial_validator_accounts == final_validator_accounts

    assert test_passed

    logger.info('Load test complete.')
