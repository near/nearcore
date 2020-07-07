# Force each node to submit many transactions for
# about 20 minutes. Monitor the block production time
# stays consistent.

import sys, time
from rc import pmap

from load_testing_helper import TIMEOUT, TRANSFER_ONLY_TIMEOUT

sys.path.append('lib')
import mocknet
from metrics import Metrics
import utils

nodes = mocknet.get_nodes()
initial_validator_accounts = mocknet.list_validators(nodes[0])


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


def check_stats(initial_metrics=None,
                final_metrics=None,
                duration=120,
                include_tps=False):
    if initial_metrics is None:
        initial_metrics = mocknet.get_metrics(nodes[-1])
        time.sleep(duration)
        final_metrics = mocknet.get_metrics(nodes[-1])

    delta = Metrics.diff(final_metrics, initial_metrics)

    bps = delta.total_blocks / delta.timestamp
    tps = delta.total_transactions / delta.timestamp
    slow_process_blocks = delta.block_processing_time[
        'le +Inf'] - delta.block_processing_time['le 1']

    print(f'INFO: Blocks per second: {bps}')
    print(f'INFO: Transactions per second: {tps}')
    print(
        f'INFO: Number of blocks processing for more than 1s: {slow_process_blocks}'
    )

    assert slow_process_blocks == 0
    assert bps > 0.5
    if include_tps:
        assert tps > 100


print('INFO: Starting Load test.')

print('INFO: Performing baseline block time measurement')
# We do not include tps here because there are no transactions on mocknet normally.
check_stats(include_tps=False)
print('INFO: Baseline block time measurement complete')

print('INFO: Setting remote python environments.')
mocknet.setup_python_environments(nodes, wasm_contract())
print('INFO: Starting transaction spamming scripts.')
mocknet.start_load_test_helpers(nodes)

initial_metrics = mocknet.get_metrics(nodes[-1])
print('INFO: Waiting for transfer only period to complete.')
time.sleep(TRANSFER_ONLY_TIMEOUT)
transfer_final_metrics = mocknet.get_metrics(nodes[-1])
print('INFO: Waiting for random transactions period to complete.')
time.sleep(TIMEOUT - TRANSFER_ONLY_TIMEOUT)
final_metrics = mocknet.get_metrics(nodes[-1])

check_stats(initial_metrics=initial_metrics,
            final_metrics=transfer_final_metrics,
            include_tps=True)
check_stats(initial_metrics=transfer_final_metrics,
            final_metrics=final_metrics,
            include_tps=False)

final_validator_accounts = mocknet.list_validators(nodes[0])
assert initial_validator_accounts == final_validator_accounts

print('INFO: Load test complete.')
