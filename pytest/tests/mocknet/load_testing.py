# Force each node to submit many transactions for
# about 20 minutes. Monitor the block production time
# stays consistent.

import sys, time
sys.path.append('lib')

from rc import pmap
from load_testing_helper import (ALL_TX_TIMEOUT, TRANSFER_ONLY_TIMEOUT,
                                 CONTRACT_DEPLOY_TIME, MAX_TPS)

import mocknet
import data, utils

from metrics import Metrics
from configured_logger import logger


def wasm_contract():
    return utils.compile_rust_contract('''
const N: u32 = 100;

metadata! {
    #[near_bindgen]
    #[derive(Default, BorshSerialize, BorshDeserialize)]
    pub struct LoadContract {}
}

#[near_bindgen]
impl LoadContract {
    pub fn do_work(&self) {
        // Do some pointless work.
        // In this case we bubble sort a reversed list.
        // Thus, this is O(N) in space and O(N^2) in time.
        let xs: Vec<u32> = (0..N).rev().collect();
        let _ = Self::bubble_sort(xs);
        env::log(b"Done.");
    }

    fn bubble_sort(mut xs: Vec<u32>) -> Vec<u32> {
        let n = xs.len();
        for i in 0..n {
            for j in 1..(n - i) {
                if xs[j - 1] > xs[j] {
                    let tmp = xs[j - 1];
                    xs[j - 1] = xs[j];
                    xs[j] = tmp;
                }
            }
        }
        xs
    }
}''')


def measure_tps_bps(nodes):
    input_tx_events = mocknet.get_tx_events(nodes)
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
    logger.info(f'INFO: {result}')
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
    logger.info(f'INFO: Memory usage (MB) = {mem_usage}')
    return mem_usage < 4500


def check_slow_blocks(initial_metrics, final_metrics):
    delta = Metrics.diff(final_metrics, initial_metrics)
    slow_process_blocks = delta.block_processing_time[
        'le +Inf'] - delta.block_processing_time['le 1']
    logger.info(
        f'INFO: Number of blocks processing for more than 1s: {slow_process_blocks}'
    )
    return slow_process_blocks == 0


if __name__ == '__main__':
    logger.info('INFO: Starting Load test.')
    nodes = mocknet.get_nodes()
    initial_validator_accounts = mocknet.list_validators(nodes[0])
    test_passed = True

    logger.info('INFO: Performing baseline block time measurement')
    # We do not include tps here because there are no transactions on mocknet normally.
    time.sleep(120)
    baseline_measurement = mocknet.chain_measure_bps_and_tps(
        archival_node=nodes[-1], start_time=None, end_time=None, duration=120)
    baseline_measurement['in_tps'] = 0.0
    # quit test early if the baseline is poor.
    assert baseline_measurement['bps'] > 1.0
    logger.info(f'INFO: {baseline_measurement}')
    logger.info('INFO: Baseline block time measurement complete')

    logger.info('INFO: Setting remote python environments.')
    mocknet.setup_python_environments(nodes, wasm_contract())
    logger.info('INFO: Starting transaction spamming scripts.')
    mocknet.start_load_test_helpers(nodes, 'load_testing_helper.py')

    initial_metrics = mocknet.get_metrics(nodes[-1])
    logger.info('INFO: Waiting for transfer only period to complete.')
    time.sleep(TRANSFER_ONLY_TIMEOUT)
    transfer_final_metrics = mocknet.get_metrics(nodes[-1])

    # wait before doing measurement to ensure tx_events written by helpers
    time.sleep(5)
    logger.info('INFO: Transfer-only results:')
    transfer_only_measurement = measure_tps_bps(nodes)
    test_passed = (transfer_only_measurement['bps'] > 0.5) and test_passed
    test_passed = check_tps(transfer_only_measurement, MAX_TPS) and test_passed
    test_passed = check_memory_usage(nodes[0]) and test_passed
    test_passed = check_slow_blocks(initial_metrics,
                                    transfer_final_metrics) and test_passed

    logger.info('INFO: Waiting for contracts to be deployed.')
    measurement_duration = transfer_final_metrics.timestamp - time.time()
    time.sleep(CONTRACT_DEPLOY_TIME - measurement_duration)

    logger.info('INFO: Waiting for random transactions period to complete.')
    all_tx_initial_metrics = mocknet.get_metrics(nodes[-1])
    time.sleep(ALL_TX_TIMEOUT)
    final_metrics = mocknet.get_metrics(nodes[-1])

    time.sleep(5)
    logger.info('INFO: All transaction types results:')
    all_tx_measurement = measure_tps_bps(nodes)
    test_passed = (all_tx_measurement['bps'] > 0.5) and test_passed
    test_passed = check_memory_usage(nodes[0]) and test_passed
    test_passed = check_slow_blocks(all_tx_initial_metrics,
                                    final_metrics) and test_passed

    final_validator_accounts = mocknet.list_validators(nodes[0])
    assert initial_validator_accounts == final_validator_accounts

    assert test_passed

    logger.info('INFO: Load test complete.')
