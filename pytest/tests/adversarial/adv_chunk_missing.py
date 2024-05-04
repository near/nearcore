#!/usr/bin/env python3
# A small test that disables chunk production for a few blocks and then
# re-enables it. This is more of an example how to disable chunk production than
# an actual test.
# Usage:
# python3 pytest/tests/adversarial/adv_chunk_missing.py

import unittest
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from utils import poll_blocks, wait_for_blocks


class AdvChunkMissingTest(unittest.TestCase):

    def test(self):
        logger.info("starting adv chunk missing test")

        [node] = start_cluster(
            num_nodes=1,
            num_observers=0,
            num_shards=1,
            config=None,
            genesis_config_changes=[],
            client_config_changes={},
            message_handler=None,
        )

        for _, hash in poll_blocks(node, __target=10):
            self.__check_new_chunks(node, hash)

        self.__stop_chunk_produce(node)

        for height, hash in poll_blocks(node, __target=20):
            # Do not check the first two blocks. Block #10 was produced in the
            # old loop and has all chunks. Block #11 contains chunks produced on
            # top of block #10 when chunk production wasn't stopped yet.
            if height <= 11:
                continue
            self.__check_old_chunks(node, hash)

        self.__start_chunk_produce(node)

        for _, hash in poll_blocks(node, __target=30):
            # Do not check the first two blocks. Block #20 was produced in the
            # old loop and has no chunks. Block #21 chunks should have been
            # produced on top of ten but chunk production was still disabled
            # then.
            if height <= 21:
                continue
            self.__check_new_chunks(node, hash)

    def __stop_chunk_produce(self, node):
        res = node.json_rpc('adv_produce_chunks', "StopProduce")
        self.assertIn('result', res, res)

    def __start_chunk_produce(self, node):
        res = node.json_rpc('adv_produce_chunks', "Valid")
        self.assertIn('result', res, res)

    def __check_new_chunks(self, node, block_hash):
        block = node.json_rpc("block", {"block_id": block_hash})

        height_included = self.__get_height_included(block)
        height = self.__get_height(block)

        # A chunk that was correctly produced should have height_included equal
        # to the block height.
        self.assertEqual(height_included, height)

    def __check_old_chunks(self, node, block_hash):
        block = node.json_rpc("block", {"block_id": block_hash})

        height_included = self.__get_height_included(block)
        height = self.__get_height(block)

        # A missing chunk is a copy of the previous chunk. In this case the
        # height included should not be equal to the block height.
        self.assertNotEqual(height_included, height)

    def __get_height_included(self, block):
        self.assertIn('result', block, block)
        block = block['result']

        self.assertIn('chunks', block, block)
        chunks = block['chunks']

        self.assertEqual(len(chunks), 1)
        [chunk] = chunks

        self.assertIn('height_included', chunk, chunk)
        height_included = chunk['height_included']

        return height_included

    def __get_height(self, block):
        self.assertIn('result', block, block)
        block = block['result']

        self.assertIn('header', block, block)
        header = block['header']

        self.assertIn('height', header, header)
        height = header['height']

        return height


if __name__ == '__main__':
    unittest.main()
