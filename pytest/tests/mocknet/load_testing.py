# Force each node to submit many transactions for
# about 20 minutes. Monitor the block production time
# stays consistent.

import sys, time
from rc import pmap

from load_testing_helper import TIMEOUT

sys.path.append('lib')
import mocknet

nodes = mocknet.get_nodes()

print('INFO: Starting Load test.')

print('INFO: Performing baseline block time measurement')
(block_time, stdev) = mocknet.measure_average_block_time(nodes[-1])
assert block_time < 1.01
assert stdev < 0.1
print('INFO: Baseline block time measurement complete')

print('INFO: Setting remote python environments.')
mocknet.setup_python_environments(nodes)
print('INFO: Starting transaction spamming scripts.')
mocknet.start_load_test_helpers(nodes)

start_time = time.time()
while time.time() - start_time < TIMEOUT:
    (block_time, stdev) = mocknet.measure_average_block_time(nodes[-1])
    assert block_time < 1.01
    assert stdev < 0.1

    # TODO: other metrics

print('INFO: Load test complete.')
