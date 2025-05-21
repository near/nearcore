#!/usr/bin/env python3
# Launches a cluster with 1 block+chunk producer, 3 chunk-only validators, and 1 failover node.
# Moves the validator key from a chunk-only validator to the failover node.
# Ensures that the backup node begins endorsing immediately (without waiting for a new epoch).

import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import utils

# Make sure to update the genesis `total_supply` if you change the number of nodes below.
NUM_PRODUCERS = 1
NUM_CHUNK_ONLY_VALIDATORS = 3
NUM_BACKUP_NODES = 1

NUM_VALIDATORS = NUM_PRODUCERS + NUM_CHUNK_ONLY_VALIDATORS
NUM_NODES = NUM_VALIDATORS + NUM_BACKUP_NODES

EPOCH_LENGTH = 15
KICKOUT_THRESHOLD = 70
# Assign a higher balance to the block producer account than to chunk-only validators
# to ensure proper role distribution.
BLOCK_PRODUCER_BALANCE = str(10**30)
CHUNK_VALIDATOR_BALANCE = str(10**28)

node_config = {"tracked_shards_config": "AllShards"}


def find_account(validators, account_id):
    """Returns the validator information for the given account_id or None if not found"""
    for validator in validators:
        if validator["account_id"] == account_id:
            return validator
    return None


def assert_block_and_chunk_producer(validator):
    """Asserts that the given validator information is for a block (and chunk) producer"""
    assert validator is not None, "Not an active validator"
    assert validator["num_expected_blocks"] > 0, validator
    assert validator["num_expected_chunks"] > 0, validator
    assert validator["num_expected_endorsements"] > 0, validator


def assert_chunk_validator_only(validator):
    """Asserts that the given validator is a chunk-only validator (not producing blocks)"""
    assert validator is not None, "Not an active validator"
    assert validator["num_expected_blocks"] == 0, validator
    assert validator["num_expected_chunks"] == 0, validator
    assert validator["num_expected_endorsements"] > 0, validator


def get_epoch_info(node, block_hash):
    """Returns a tuple: (previous kickouts, current validators, next validators)"""
    block = node.get_block(block_hash)
    assert 'result' in block, block
    header = block['result']['header']
    epoch_id = header['epoch_id']
    epoch_info = node.get_validators(epoch_id=epoch_id)
    assert 'result' in epoch_info, epoch_info
    assert 'prev_epoch_kickout' in epoch_info['result'], epoch_info
    prev_kickouts = epoch_info['result']['prev_epoch_kickout']
    assert 'current_validators' in epoch_info['result'], epoch_info
    current_validators = epoch_info['result']['current_validators']
    assert 'next_validators' in epoch_info['result'], epoch_info
    next_validators = epoch_info['result']['next_validators']
    return (prev_kickouts, current_validators, next_validators)


def assert_roles_as_expected(rpc_node, hash):
    """Check the expected roles of the nodes in the current epoch."""
    (_kickouts, current_validators,
     _next_validators) = get_epoch_info(rpc_node, hash)
    for i in range(NUM_PRODUCERS):
        assert_block_and_chunk_producer(
            find_account(current_validators, f"test{i}"))
    for i in range(NUM_PRODUCERS, NUM_VALIDATORS):
        assert_chunk_validator_only(find_account(current_validators,
                                                 f"test{i}"))


nodes = start_cluster(
    NUM_VALIDATORS, NUM_BACKUP_NODES, 1,
    None, [["epoch_length", EPOCH_LENGTH],
           ["num_block_producer_seats", NUM_PRODUCERS],
           ["num_block_producer_seats_per_shard", [NUM_PRODUCERS]],
           ["num_chunk_producer_seats", NUM_PRODUCERS],
           ["num_chunk_validator_seats", NUM_VALIDATORS],
           ["block_producer_kickout_threshold", KICKOUT_THRESHOLD],
           ["chunk_producer_kickout_threshold", KICKOUT_THRESHOLD],
           ["chunk_validator_only_kickout_threshold", KICKOUT_THRESHOLD],
           ["minimum_validators_per_shard", 1],
           ["total_supply", "5801030000000000000000000000000000"]] +
    [[
        "validators", i, "amount",
        BLOCK_PRODUCER_BALANCE if i < NUM_PRODUCERS else CHUNK_VALIDATOR_BALANCE
    ] for i in range(NUM_VALIDATORS)] + [[
        "records", 2 * i, "Account", "account", "locked",
        BLOCK_PRODUCER_BALANCE if i < NUM_PRODUCERS else CHUNK_VALIDATOR_BALANCE
    ] for i in range(NUM_VALIDATORS)],
    {i: node_config for i in range(NUM_NODES)})

rpc_node = nodes[0]
failover_node = nodes[-1]
chunk_validator_index = NUM_VALIDATORS - 1
chunk_validator_node = nodes[chunk_validator_index]

logger.info("Running chain with all validators")
# Wait until 1/3 into the next epoch
_height, hash = utils.wait_for_blocks(rpc_node,
                                      target=EPOCH_LENGTH + EPOCH_LENGTH // 3)
assert_roles_as_expected(rpc_node, hash)

chunk_validator_account = f"test{chunk_validator_index}"
logger.info(
    f"Moving validator key to a failover node, from chunk-only validator ({chunk_validator_account})"
)

failover_node.kill()
failover_node.reset_validator_key(chunk_validator_node.validator_key)
chunk_validator_node.kill()
failover_node.start()

_height, hash = utils.wait_for_blocks(rpc_node, target=EPOCH_LENGTH * 2 - 2)
assert_roles_as_expected(rpc_node, hash)

_height, hash = utils.wait_for_blocks(rpc_node, target=EPOCH_LENGTH * 3)
# Check that chunk_validator_account is not kicked out in the previous epoch and will be a validator in the next epoch.
(kickouts, _current_validators,
 next_validators) = get_epoch_info(rpc_node, hash)

assert find_account(kickouts, chunk_validator_account) is None
assert find_account(
    next_validators, chunk_validator_account
) is not None, f"{chunk_validator_account} must be in next validators"
