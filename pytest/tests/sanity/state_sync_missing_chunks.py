#!/usr/bin/env python3
# This test checks that state sync works correctly when some chunks are missing.
# In particular it checks what happens when there are no chunks in a few blocks
# leading up to the epoch boundary. It's an important corner case because the
# state sync needs to find the latest new chunk and start from there.
import unittest
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import load_config, start_cluster
from utils import poll_blocks, wait_for_blocks
from state_sync_lib import get_state_sync_configs_pair
from configured_logger import logger


class StateSyncMissingChunks(unittest.TestCase):

    # Configure one validator and one rpc node. Start the validator and have it
    # skip chunks before each epoch boundary. Start the rpc node and check that
    # it is able to sync.
    def test(self):
        logger.info(f"Starting the epoch boundary test.")

        config = load_config()

        epoch_length = 10
        genesis_config_changes = [("epoch_length", epoch_length)]

        # Get the state sync configs and decrease fetch horizons to trigger
        # state sync earlier.
        (config_dump, config_sync) = get_state_sync_configs_pair()
        config_sync["consensus"] = {
            "block_fetch_horizon": epoch_length,
            "block_header_fetch_horizon": epoch_length,
        }
        client_config_changes = {0: config_dump, 1: config_sync}

        [val_node, rpc_node] = start_cluster(
            num_nodes=1,
            num_observers=1,
            num_shards=1,
            config=config,
            genesis_config_changes=genesis_config_changes,
            client_config_changes=client_config_changes,
        )

        # disable checking store, as store seems inconsistent during state sync
        rpc_node.stop_checking_store()
        rpc_node.kill()

        epoch_id = None
        missing_chunks_count = 0

        target_height = 100
        rpc_start_height = 50

        for height, hash in poll_blocks(val_node, __target=target_height):
            chunk_mask = self.__get_chunk_mask(val_node, hash)
            new_epoch_id = self.__get_epoch_id(val_node, hash)

            [has_chunk] = chunk_mask
            if not has_chunk:
                missing_chunks_count += 1

            # Make sure that the epoch boundary math is correct.
            if height % epoch_length == 1:
                self.assertNotEqual(epoch_id, new_epoch_id)

            epoch_id = new_epoch_id
            logger.debug(f"#{height} mask {chunk_mask} epoch_id {epoch_id}")

            # configure missing chunks

            # blocks starting at height = 8 mod epoch_length will have no chunks
            if height % epoch_length == 6:
                self.__stop_chunk_produce(val_node)

            # blocks starting at height = 2 mod epoch length will have chunks
            if height % epoch_length == 0:
                self.__start_chunk_produce(val_node)

            # Wait until the validator and rpc node heads are far enough to
            # trigger state sync in the rpc node. Otherwise it would just do
            # block sync. Once it's true restart the rpc node.

            if height == rpc_start_height:
                logger.info("Starting the rpc node")
                rpc_node.start()

            if height <= rpc_start_height:
                continue

        # Make sure that the rpc node is synced and can reach the target height
        # At this point the node should be fully synced so set small timeout.
        logger.info("Waiting for rpc to reach the target height.")
        wait_for_blocks(rpc_node, target=target_height, timeout=10)

        # Make sure that the test is actually testing what it says it's testing
        # - state sync with missing chunks.
        self.assertGreater(missing_chunks_count, 0)

        val_node.kill()
        rpc_node.kill()

    def __stop_chunk_produce(self, node):
        logger.debug("stop chunk produce")
        res = node.json_rpc('adv_produce_chunks', "StopProduce")
        self.assertIn('result', res, res)

    def __start_chunk_produce(self, node):
        logger.debug("start chunk produce")
        res = node.json_rpc('adv_produce_chunks', "Valid")
        self.assertIn('result', res, res)

    def __get_epoch_id(self, node, block_hash):
        block = node.json_rpc("block", {"block_id": block_hash})
        return block['result']['header']['epoch_id']

    def __get_chunk_mask(self, node, block_hash):
        block = node.json_rpc("block", {"block_id": block_hash})
        return block['result']['header']['chunk_mask']


if __name__ == '__main__':
    unittest.main()
