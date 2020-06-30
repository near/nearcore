# Force each node to submit many transactions for
# about 20 minutes. Monitor the block production time
# stays consistent.

import sys, time
from rc import pmap

from load_testing_helper import TIMEOUT

sys.path.append('lib')
import mocknet

nodes = mocknet.get_nodes()


def check_stats(include_tps=False):
    stats = mocknet.measure_chain_stats(nodes[-1])
    (block_time, stdev) = stats['block_time']
    assert block_time < 1.01
    assert stdev < 0.1
    if include_tps:
        (tps, tps_stdev) = stats['tps']
        assert tps > 190
        assert tps_stdev < 60


print('INFO: Starting Load test.')

print('INFO: Performing baseline block time measurement')
# We do not include tps here because there are no transactions on mocknet normally.
check_stats(include_tps=False)
print('INFO: Baseline block time measurement complete')

print('INFO: Setting remote python environments.')
mocknet.setup_python_environments(nodes)
print('INFO: Starting transaction spamming scripts.')
mocknet.start_load_test_helpers(nodes)

start_time = time.time()
while time.time() - start_time < TIMEOUT:
    check_stats(include_tps=True)

    # TODO: other metrics

print('INFO: Load test complete.')
