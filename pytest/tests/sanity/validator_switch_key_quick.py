#!/usr/bin/env python3
# Starts three validating nodes and one non-validating node
# Set a new validator key that has the same account id as one of
# the validating nodes. Stake that account with the new key
# and make sure that the network doesn't stall even after
# the non-validating node becomes a validator.

import unittest
import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from cluster import start_cluster
from utils import wait_for_blocks

EPOCH_LENGTH = 20
TIMEOUT = 100


class ValidatorSwitchKeyQuickTest(unittest.TestCase):

    def test_validator_switch_key_quick(self):
        # It is important for the non-validating node to already track shards
        # that it will be assigned to when becoming a validator.
        config_map = {
            2: {
                "tracked_shadow_validator": "test0",
                "store.load_mem_tries_for_tracked_shards": True,
            }
        }

        # Key will be moved from old_validator to new_validator,
        # while the other_validator remains untouched.
        [
            other_validator,
            old_validator,
            new_validator,
        ] = start_cluster(2, 1, 3, None,
                          [["epoch_length", EPOCH_LENGTH],
                           ["block_producer_kickout_threshold", 10],
                           ["chunk_producer_kickout_threshold", 10]],
                          config_map)
        wait_for_blocks(old_validator, count=5)

        new_validator.reset_validator_key(other_validator.validator_key)
        other_validator.kill()
        new_validator.reload_updateable_config()
        new_validator.stop_checking_store()
        wait_for_blocks(old_validator, count=2)

        block = old_validator.get_latest_block()
        max_height = block.height + 4 * EPOCH_LENGTH
        target_height = max_height - EPOCH_LENGTH // 2
        start_time = time.time()

        while True:
            self.assertLess(time.time() - start_time, TIMEOUT,
                            'Validators got stuck')

            info = old_validator.json_rpc('validators', 'latest')
            next_validators = info['result']['next_validators']
            account_ids = [v['account_id'] for v in next_validators]
            # We copied over 'test0' validator key, along with validator account ID.
            # Therefore, despite nodes[0] being stopped, 'test0' still figures as active validator.
            self.assertEqual(sorted(account_ids), ['test0', 'test1'])

            last_block_per_node = [
                new_validator.get_latest_block(),
                old_validator.get_latest_block()
            ]
            height_per_node = list(
                map(lambda block: block.height, last_block_per_node))
            logger.info(height_per_node)

            self.assertLess(max(height_per_node), max_height,
                            'Nodes are not synced')

            synchronized = True
            for i, node in enumerate([new_validator, old_validator]):
                try:
                    node.get_block(last_block_per_node[1 - i].hash)
                except Exception:
                    synchronized = False
                    break

            # Both validators should be synchronized
            logger.info(f'Synchronized {synchronized}')
            if synchronized and height_per_node[0] > target_height:
                # If nodes are synchronized and the current height is close to `max_height` we can finish.
                return

            wait_for_blocks(old_validator, count=1)


if __name__ == '__main__':
    unittest.main()
