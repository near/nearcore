#!/usr/bin/env python3
# Starts one validating node and one non-validating node
# Dynamically remove the key from the validator node
# and make sure that the network stalls.
# Dynamically reload the key at the validator node
# and make sure that the network progress again.

import unittest
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from cluster import start_cluster
from utils import wait_for_blocks
import state_sync_lib

EPOCH_LENGTH = 20
TIMEOUT = 100


class ValidatorRemoveKeyQuickTest(unittest.TestCase):

    def test_validator_remove_key_quick(self):
        logger.info("Validator remove key quick test")
        validator_config, rpc_config = state_sync_lib.get_state_sync_configs_pair(
        )

        validator_config.update({
            "tracked_shards": [0],
            "store.load_mem_tries_for_tracked_shards": True,
        })

        rpc_config.update({
            "tracked_shards": [0],
        })

        [validator,
         rpc] = start_cluster(1, 1, 1, None,
                              [["epoch_length", EPOCH_LENGTH],
                               ["block_producer_kickout_threshold", 80],
                               ["chunk_producer_kickout_threshold", 80]], {
                                   0: validator_config,
                                   1: rpc_config
                               })

        wait_for_blocks(rpc, target=EPOCH_LENGTH)
        validator_key = validator.validator_key
        validator.remove_validator_key()
        wait_for_blocks(rpc, target=EPOCH_LENGTH * 2)
        validator.reload_updateable_config()
        validator.stop_checking_store()
        try:
            wait_for_blocks(rpc, count=5, timeout=10)
        except:
            pass
        else:
            self.fail('Blocks are not supposed to be produced')

        validator.reset_validator_key(validator_key)
        validator.reload_updateable_config()
        validator.stop_checking_store()
        wait_for_blocks(rpc, count=EPOCH_LENGTH * 2)


if __name__ == '__main__':
    unittest.main()
