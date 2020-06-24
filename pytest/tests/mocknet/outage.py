# Stop fewer than one third of mocknet nodes for two full epochs.
# During this time the network should continue to function.
# The stopped nodes are then restarted, should come back up,
# sync successfully and become operational again.

import sys, time
from rc import pmap

sys.path.append('lib')
import mocknet

nodes = mocknet.get_nodes()

validators = nodes[0].get_validators()['result']
validator_accounts = set(
    map(lambda v: v['account_id'], validators['current_validators']))
# Nodes 0, 1, 2, 3 are initially validators
assert validator_accounts == {'node0', 'node1', 'node2', 'node3'}

epoch_length = mocknet.get_epoch_length_in_blocks(nodes[0])

# Function to do a transfer, but not using the first node
check_transfer = lambda: mocknet.transfer_between_nodes(nodes[1:])


# Function to wait until the next epoch
def wait_until_next_epoch(current_start_height, query_node):
    while query_node.get_validators(
    )['result']['epoch_start_height'] <= current_start_height:
        time.sleep(5)


print('INFO: Starting Outage test.')

# Ensure we start the test closer to the beginning of an epoch than the end.
current_height = nodes[0].get_status()['sync_info']['latest_block_height']
if current_height > validators['epoch_start_height'] + (epoch_length / 2):
    print(
        'INFO: Presently over half way through epoch, waiting for the next epoch to start'
    )
    wait_until_next_epoch(validators['epoch_start_height'], nodes[0])

# Nodes 0, 1, 2, and 3 all have roughly equal stake.
# We stop node 0, thus stopping 25% of the stake.
mocknet.stop_node(nodes[0])

# Wait a moment
time.sleep(1)

# Check the network still functions
check_transfer()

# Wait an epoch
print('INFO: Waiting until next epoch.')
validators = nodes[1].get_validators()['result']
wait_until_next_epoch(validators['epoch_start_height'], nodes[1])

# Check the network still functions
check_transfer()

# Confirm node0 was kicked out for being offline
validators = nodes[1].get_validators()['result']
kick_out = validators['prev_epoch_kickout'][0]
assert kick_out['account_id'] == 'node0'
assert 'NotEnoughBlocks' in kick_out['reason'].keys()

# Wait another epoch
print('INFO: Waiting until next epoch.')
wait_until_next_epoch(validators['epoch_start_height'], nodes[1])

# Check the network still functions
check_transfer()

# Bring the node back up
mocknet.start_node(nodes[0])

# Wait a moment
time.sleep(5)

# Let the node catch up
sync_start = time.time()
sync_timeout = 300  # allow up to 5 minutes for syncing
print('INFO: Waiting for node0 to sync.')
while nodes[0].get_status()['sync_info']['syncing']:
    if time.time() - sync_start >= sync_timeout:
        raise TimeoutError('nodes[0] seems to be stalled while syncing')
    time.sleep(5)

# Check the node functions by using it as the basis for transfer
mocknet.transfer_between_nodes(nodes)

# re-stake node0 since it was kicked out
mocknet.stake_node(nodes[0])
validators = nodes[0].get_validators()['result']
print('INFO: Waiting until next epoch.')
wait_until_next_epoch(validators['epoch_start_height'], nodes[0])

# Confirm node0 will be a validator in the next epoch
validators = nodes[0].get_validators()['result']
assert 'node0' in map(lambda v: v['account_id'], validators['next_validators'])
print('INFO: node0 in next epoch validator set.')
print('INFO: Waiting until next epoch.')
wait_until_next_epoch(validators['epoch_start_height'], nodes[0])

# Confirm node0 is a validator again
validators = nodes[0].get_validators()['result']
assert 'node0' in map(lambda v: v['account_id'],
                      validators['current_validators'])
print('INFO: node0 in current epoch validator set.')
print('INFO: Outage test complete.')
