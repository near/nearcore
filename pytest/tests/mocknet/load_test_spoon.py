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
"""
import argparse
import random
import sys
import time

sys.path.append('lib')

from helpers import load_test_spoon_helper
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
    parser.add_argument('--skip-load', default=False, action='store_true')
    parser.add_argument('--skip-setup', default=False, action='store_true')
    parser.add_argument('--skip-restart', default=False, action='store_true')

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

    if not args.skip_load:
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
