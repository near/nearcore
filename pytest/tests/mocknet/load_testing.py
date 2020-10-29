# Force each node to submit many transactions for
# about 20 minutes. Monitor the block production time
# stays consistent.

import sys, time
from rc import pmap

from load_testing_helper import (ALL_TX_TIMEOUT, TRANSFER_ONLY_TIMEOUT,
                                 CONTRACT_DEPLOY_TIME, MAX_TPS)

sys.path.append('lib')
import mocknet
from metrics import Metrics
import data, utils


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
    print(f'INFO: {result}')
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
    print(f'INFO: Memory usage (MB) = {mem_usage}')
    return mem_usage < 4500


def check_slow_blocks(initial_metrics, final_metrics):
    delta = Metrics.diff(final_metrics, initial_metrics)
    slow_process_blocks = delta.block_processing_time[
        'le +Inf'] - delta.block_processing_time['le 1']
    print(
        f'INFO: Number of blocks processing for more than 1s: {slow_process_blocks}'
    )
    return slow_process_blocks == 0


if __name__ == '__main__':
    print('INFO: Sleeping for 6 hours.')
    time.sleep(21600)
    print('INFO: Done.')
