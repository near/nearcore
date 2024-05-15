#!/usr/bin/env python3
# This test checks the ultimate undercharding scenario where a chunk takes
# long time to apply but consumes little gas. This is to simulate real
# undercharing in a more controlled manner.

import sys
import json
import unittest
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from transaction import sign_deploy_contract_tx, sign_function_call_tx
from configured_logger import logger
from cluster import start_cluster
from utils import load_test_contract, poll_blocks

GGAS = 10**9


class SlowChunkTest(unittest.TestCase):

    # Spin up multiple validator nodes in a multi shard chain. Deploy a contract
    # to one of the shards and call a function that sleeps for a long time.
    # Check that the shard is able to recover and that new chunks appear.
    def test(self):
        # The number of validators and the number of shards.
        n = 4

        client_config_changes = {i: {'tracked_shards': [0]} for i in range(n)}
        genesis_config_changes = [["epoch_length", 10]]
        [node1, node2, node3, node4] = start_cluster(
            n,
            0,
            n,
            None,
            genesis_config_changes,
            client_config_changes,
        )

        self.__deploy_contract(node1)

        self.__call_contract(node1)

        recovered = False

        for height, hash in poll_blocks(node1, __target=20):
            chunk_mask = self.__get_chunk_mask(node1, hash)
            logger.info(f"#{height} chunk mask: {chunk_mask}")

            if all(chunk_mask):
                logger.info("The chain recovered. All chunks are present.")
                recovered = True
                break

        self.assertTrue(recovered)

    def __deploy_contract(self, node):
        logger.info("Deploying contract.")

        block_hash = node.get_latest_block().hash_bytes
        contract = load_test_contract('test_contract_rs.wasm')

        tx = sign_deploy_contract_tx(node.signer_key, contract, 10, block_hash)
        node.send_tx(tx)

    def __call_contract(self, node):
        logger.info("Calling contract.")

        block_hash = node.get_latest_block().hash_bytes

        # duration is measured in nanoseconds
        second = int(1e9)
        duration_nanos = 5 * second
        duration_bytes = duration_nanos.to_bytes(8, byteorder="little")

        tx = sign_function_call_tx(
            node.signer_key,
            node.signer_key.account_id,
            'sleep',
            duration_bytes,
            150 * GGAS,
            1,
            20,
            block_hash,
        )
        result = node.send_tx(tx)
        self.assertIn('result', result, result)

        logger.info(json.dumps(result['result'], indent=2))

    def __get_chunk_mask(self, node, block_hash):
        block = node.json_rpc("block", {"block_id": block_hash})
        return block['result']['header']['chunk_mask']


if __name__ == '__main__':
    unittest.main()
