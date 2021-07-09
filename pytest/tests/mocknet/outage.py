# Stop fewer than one third of mocknet nodes for two full epochs.
# During this time the network should continue to function.
# The stopped nodes are then restarted, should come back up,
# sync successfully and become operational again.

import sys, time
from rc import pmap

sys.path.append('lib')
import mocknet
from configured_logger import logger

nodes = mocknet.get_nodes()

# Get the list of current validators, sorted by node index
validators = sorted(nodes[0].get_validators()['result']['current_validators'],
                    key=lambda v: int(v['account_id'][4:]))
num_rpc_nodes = min(mocknet.NUM_NODES // 2, 4)
assert len(validators) == mocknet.NUM_NODES - num_rpc_nodes

# Take down ~25% of stake.
total_stake = sum(map(lambda v: int(v['stake']), validators))
offline_validators = []
offline_stake = 0
for v in validators:
    next_stake = int(v['stake'])
    if len(offline_validators) > 0 and (offline_stake +
                                        next_stake) / total_stake >= 0.25:
        break
    offline_validators.append(v['account_id'])
    offline_stake += next_stake
first_online_index = len(offline_validators)

epoch_length = mocknet.get_epoch_length_in_blocks(nodes[0])

# Function to do a transfer, but not using the offline nodes
check_transfer = lambda: mocknet.transfer_between_nodes(nodes[
    first_online_index:])


# Function to wait until the next epoch
def wait_until_next_epoch(current_start_height, query_node):
    while query_node.get_validators(
    )['result']['epoch_start_height'] <= current_start_height:
        time.sleep(5)


logger.info('Starting Outage test.')

# Ensure we start the test closer to the beginning of an epoch than the end.
epoch_start_height = nodes[0].get_validators()['result']['epoch_start_height']
current_height = nodes[0].get_status()['sync_info']['latest_block_height']
if current_height > epoch_start_height + (epoch_length / 2):
    logger.info(
        'Presently over half way through epoch, waiting for the next epoch to start'
    )
    wait_until_next_epoch(epoch_start_height, nodes[0])

# Stop nodes we want to take offline
pmap(lambda i: mocknet.stop_node(nodes[i]), range(len(offline_validators)))
query_node = nodes[first_online_index]

# Wait some time to ensure nodes are down
time.sleep(10)

# Check the network still functions
check_transfer()

# Wait an epoch
logger.info('Waiting until next epoch.')
validators = query_node.get_validators()['result']
wait_until_next_epoch(validators['epoch_start_height'], query_node)

# Check the network still functions
check_transfer()

# Confirm nodes were kicked out for being offline
validators = query_node.get_validators()['result']
kick_out = validators['prev_epoch_kickout']
assert len(kick_out) == len(offline_validators)
for k in kick_out:
    assert k['account_id'] in offline_validators
    assert 'NotEnoughBlocks' in k['reason']

# Wait another epoch
logger.info('Waiting until next epoch.')
wait_until_next_epoch(validators['epoch_start_height'], query_node)

# Check the network still functions
check_transfer()

# Bring the nodes back up
pmap(lambda i: mocknet.start_node(nodes[i]), range(len(offline_validators)))

# Wait a minute for them to come back online
time.sleep(60)

# Let the node catch up
sync_start = time.time()
sync_timeout = 300  # allow up to 5 minutes for syncing
logger.info('Waiting for node0 to sync.')
while nodes[0].get_status()['sync_info']['syncing']:
    if time.time() - sync_start >= sync_timeout:
        raise TimeoutError('nodes[0] seems to be stalled while syncing')
    time.sleep(5)

# Check the node functions by using it as the basis for transfer
mocknet.transfer_between_nodes(nodes)

# re-stake offline nodes since they were kicked out
for i in range(len(offline_validators)):
    mocknet.stake_node(nodes[i])
validators = nodes[0].get_validators()['result']
logger.info('Waiting until next epoch.')
wait_until_next_epoch(validators['epoch_start_height'], nodes[0])

# Confirm previously offline nodes will be validators in the next epoch
validators = nodes[0].get_validators()['result']
next_validators = set(
    map(lambda v: v['account_id'], validators['next_validators']))
for v in offline_validators:
    assert v in next_validators
logger.info('previously offline nodes in next epoch validator set.')
logger.info('Waiting until next epoch.')
wait_until_next_epoch(validators['epoch_start_height'], nodes[0])

# Confirm nodes are a validators again
validators = nodes[0].get_validators()['result']
current_validators = set(
    map(lambda v: v['account_id'], validators['current_validators']))
for v in offline_validators:
    assert v in current_validators
logger.info('previously offline nodes now in current epoch validator set.')
logger.info('Outage test complete.')
