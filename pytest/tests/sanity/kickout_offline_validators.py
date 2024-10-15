#!/usr/bin/env python3
# Spins up 4 block+chunk producers and 4 chunk-only validators.
# Kills one of the block+chunk producers and one of the chunk validators and
# then checks that these validators are kicked out and removed from
# the current validators after 2 epochs. We set the balance of the first 4 validators
# more than the last 4 validators to ensure that the former 4 befome block+chunk producers
# and the latter 4 become chunk validator only.

import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import utils

EPOCH_LENGTH = 15

# Use very low threshold to target offline validators only.
KICKOUT_THRESHOLD = 10

# Make block producer account's balance larger than chunk validators to establish the intended distribution of roles.
BLOCK_PRODUCER_BALANCE = str(10**30)
CHUNK_VALIDATOR_BALANCE = str(10**28)

node_config = {
    "tracked_shards": [0],  # Track all shards.
}


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
    """Asserts that the given validator information is for a chunk validator-only validator"""
    assert validator is not None, "Not an active validator"
    assert validator["num_expected_blocks"] == 0, validator
    assert validator["num_expected_chunks"] == 0, validator
    assert validator["num_expected_endorsements"] > 0, validator


def assert_kicked_out(validator, reason):
    """Asserts that the validator is kicked out for the given reason and zero production"""
    assert validator is not None, "Not kicked out"
    assert 'reason' in validator, validator
    kickout_reasons = validator['reason']
    assert reason in kickout_reasons, validator
    kickout_stats = kickout_reasons[reason]
    assert 'produced' in kickout_stats, validator
    assert kickout_stats['produced'] == 0


def get_epoch_info(node, block_hash):
    """Returns a tuple of previous kickouts, current validators, and next validators."""
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


nodes = start_cluster(
    8, 0, 1,
    None, [["epoch_length", EPOCH_LENGTH], ["num_block_producer_seats", 4],
           ["num_block_producer_seats_per_shard", [4]],
           ["num_chunk_producer_seats", 4], ["num_chunk_validator_seats", 8],
           ["block_producer_kickout_threshold", KICKOUT_THRESHOLD],
           ["chunk_producer_kickout_threshold", KICKOUT_THRESHOLD],
           ["chunk_validator_only_kickout_threshold", KICKOUT_THRESHOLD],
           ["minimum_validators_per_shard", 1],
           ["total_supply", "8604040000000000000000000000000000"]] + [[
               "validators", i, "amount",
               BLOCK_PRODUCER_BALANCE if i < 4 else CHUNK_VALIDATOR_BALANCE
           ] for i in range(8)] + [[
               "records", 2 * i, "Account", "account", "locked",
               BLOCK_PRODUCER_BALANCE if i < 4 else CHUNK_VALIDATOR_BALANCE
           ] for i in range(8)], {i: node_config for i in range(8)})

rpc_node = nodes[0]

logger.info("Running chain with all validators")
# Kill the validators before the next epoch starts to ensure that
# they do not generate anything in the next epoch (asserted below).
_height, hash = utils.wait_for_blocks(rpc_node, target=EPOCH_LENGTH - 1)

# Check the expected roles of the nodes in the current epoch.
(kickouts, current_validators,
 _next_validators) = get_epoch_info(rpc_node, hash)
for i in range(4):
    assert_block_and_chunk_producer(find_account(current_validators,
                                                 f"test{i}"))
for i in range(4, 8):
    assert_chunk_validator_only(find_account(current_validators, f"test{i}"))

logger.info("Killing a block producer (test3) and a chunk validator (test7)")
nodes[3].kill()  # Block producer
nodes[7].kill()  # Chunk validator

_height, hash = utils.wait_for_blocks(rpc_node, target=EPOCH_LENGTH * 4 + 3)

# Check that test3 and test7 are kicked out in the previous epoch and will not be validators in the next epoch.
(kickouts, _current_validators,
 next_validators) = get_epoch_info(rpc_node, hash)
assert_kicked_out(find_account(kickouts, "test3"), 'NotEnoughBlocks')
assert_kicked_out(find_account(kickouts, "test7"), 'NotEnoughChunkEndorsements')
assert find_account(next_validators,
                    "test3") is None, "test3 must not be in next validators"
assert find_account(next_validators,
                    "test7") is None, "test7 must not be in next validators"
