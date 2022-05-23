#!/usr/bin/env python3
# Spins up two nodes; Let's them build the chain for several epochs;
# Spins up two more nodes, and makes the two new nodes to stake, and the old two to unstake;
# Makes the two new nodes to build for couple more epochs;
# Spins up one more node. Makes sure it can sync.
# For the last node to be able to sync, it needs to learn to download chunks from the other
# archival nodes, as opposed to the validators of the corresponding epoch, since the validators
# of the old epochs are long gone by the time the archival node is syncing.

import sys, time, logging, base58
import multiprocessing
from functools import partial
import pathlib

from requests.api import request

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config
from configured_logger import logger
from messages.block import ShardChunkHeaderV1, ShardChunkHeaderV2, ShardChunkHeaderV3
from transaction import sign_staking_tx
from proxy import ProxyHandler, NodesProxy
import utils

TIMEOUT = 200
EPOCH_LENGTH = 10
HEIGHTS_BEFORE_ROTATE = 35
HEIGHTS_BEFORE_CHECK = 25


class Handler(ProxyHandler):

    def __init__(self,
                 *args,
                 hash_to_metadata={},
                 requests={},
                 responses={},
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.hash_to_metadata = hash_to_metadata
        self.requests = requests
        self.responses = responses

    async def handle(self, msg, fr, to):
        if msg.enum == 'Routed':
            msg_kind = msg.Routed.body.enum
            if msg_kind == 'PartialEncodedChunk':
                header = msg.Routed.body.PartialEncodedChunk.header
                height = header.inner.height_created
                shard_id = header.inner.shard_id
                hash_ = header.chunk_hash()
                self.hash_to_metadata[hash_] = (height, shard_id)

            if msg_kind == 'VersionedPartialEncodedChunk':
                header = msg.Routed.body.VersionedPartialEncodedChunk.inner_header(
                )
                header_version = msg.Routed.body.VersionedPartialEncodedChunk.header_version(
                )
                if header_version == 'V3':
                    height = header.V2.height_created
                    shard_id = header.V2.shard_id
                else:
                    height = header.height_created
                    shard_id = header.shard_id

                if header_version == 'V1':
                    hash_ = ShardChunkHeaderV1.chunk_hash(header)
                elif header_version == 'V2':
                    hash_ = ShardChunkHeaderV2.chunk_hash(header)
                elif header_version == 'V3':
                    hash_ = ShardChunkHeaderV3.chunk_hash(header)
                self.hash_to_metadata[hash_] = (height, shard_id)

            if msg_kind == 'PartialEncodedChunkRequest':
                if fr == 4:
                    hash_ = msg.Routed.body.PartialEncodedChunkRequest.chunk_hash
                    assert hash_ in self.hash_to_metadata, "chunk hash %s is not present" % base58.b58encode(
                        hash_)
                    (height, shard_id) = self.hash_to_metadata[hash_]
                    logger.info("REQ %s %s %s %s" % (height, shard_id, fr, to))
                    self.requests[(height, shard_id, to)] = 1

            if msg_kind == 'PartialEncodedChunkResponse':
                if to == 4:
                    hash_ = msg.Routed.body.PartialEncodedChunkResponse.chunk_hash
                    (height, shard_id) = self.hash_to_metadata[hash_]
                    logger.info("RESP %s %s %s %s" % (height, shard_id, fr, to))
                    self.responses[(height, shard_id, fr)] = 1

        return True


# TODO(mina86): Make it a utility class
class Timeout:

    def __init__(self, sesconds: float) -> None:
        if sesconds <= 0:
            raise ValueError('sesconds must be positive')
        self.__start = time.monotonic()
        self.__end = self.__start + sesconds

    def check(self) -> bool:
        return time.monotonic() < self.__end

    def elapsed_seconds(self) -> float:
        return time.monotonic() - self.__start

    def left_seconds(self) -> float:
        return self.__end - time.monotonic()


if __name__ == '__main__':
    manager = multiprocessing.Manager()
    hash_to_metadata = manager.dict()
    requests = manager.dict()
    responses = manager.dict()

    proxy = NodesProxy(
        partial(Handler,
                hash_to_metadata=hash_to_metadata,
                requests=requests,
                responses=responses))

    timeout = Timeout(TIMEOUT)

    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    config = load_config()
    near_root, node_dirs = init_cluster(
        2,
        3,
        2,
        config,
        [
            ["min_gas_price", 0],
            ["max_inflation_rate", [0, 1]],
            ["epoch_length", EPOCH_LENGTH],
            ['num_block_producer_seats', 4],
            ["block_producer_kickout_threshold", 20],
            ["chunk_producer_kickout_threshold", 20],
            ["validators", 0, "amount", "110000000000000000000000000000000"],
            ["validators", 1, "amount", "110000000000000000000000000000000"],
            [
                "records", 0, "Account", "account", "locked",
                "110000000000000000000000000000000"
            ],
            # each validator account is two records, thus the index of a record for the second is 2, not 1
            [
                "records", 2, "Account", "account", "locked",
                "110000000000000000000000000000000"
            ],
            ["total_supply", "6120000000000000000000000000000000"]
        ],
        {
            4: {
                "tracked_shards": [0, 1],
                "archive": True
            },
            3: {
                "archive": True,
                "tracked_shards": [1],
                "network": {
                    "ttl_account_id_router": {
                        "secs": 1,
                        "nanos": 0
                    }
                }
            },
            2: {
                "archive": True,
                "tracked_shards": [0],
                "network": {
                    "ttl_account_id_router": {
                        "secs": 1,
                        "nanos": 0
                    }
                }
            }
        })

    boot_node = spin_up_node(config, near_root, node_dirs[0], 0, proxy=proxy)
    node1 = spin_up_node(config,
                         near_root,
                         node_dirs[1],
                         1,
                         boot_node=boot_node,
                         proxy=proxy)

    def get_validators(node):
        return set([x['account_id'] for x in node.get_status()['validators']])

    logging.info(f'Getting to height {HEIGHTS_BEFORE_ROTATE}')
    utils.wait_for_blocks(boot_node,
                          target=HEIGHTS_BEFORE_ROTATE,
                          timeout=timeout.left_seconds())

    node2 = spin_up_node(config,
                         near_root,
                         node_dirs[2],
                         2,
                         boot_node=boot_node,
                         proxy=proxy)
    node3 = spin_up_node(config,
                         near_root,
                         node_dirs[3],
                         3,
                         boot_node=boot_node,
                         proxy=proxy)

    hash_ = boot_node.get_latest_block().hash_bytes

    logging.info("Waiting for the new nodes to sync")
    while True:
        if (not node2.get_status()['sync_info']['syncing'] and
                not node3.get_status()['sync_info']['syncing']):
            break
        time.sleep(1)

    for stake, nodes, expected_vals in [
        (100000000000000000000000000000000, [node2, node3],
         ["test0", "test1", "test2", "test3"]),
        (0, [boot_node, node1], ["test2", "test3"]),
    ]:
        logging.info("Rotating validators")
        for ord_, node in enumerate(reversed(nodes)):
            tx = sign_staking_tx(node.signer_key, node.validator_key, stake, 10,
                                 hash_)
            boot_node.send_tx(tx)

        logging.info("Waiting for rotation to occur")
        while True:
            assert timeout.check(), get_validators(boot_node)
            if set(get_validators(boot_node)) == set(expected_vals):
                break
            else:
                time.sleep(1)

    start_height = boot_node.get_latest_block().height

    logging.info("Killing old nodes")
    boot_node.kill()
    node1.kill()

    target = start_height + HEIGHTS_BEFORE_CHECK
    logging.info(f'Getting to height {target}')
    height_to_sync_to, _ = utils.wait_for_blocks(node2,
                                                 target=target,
                                                 timeout=timeout.left_seconds())

    logging.info("Spinning up one more node")
    node4 = spin_up_node(config, near_root, node_dirs[4], 4, boot_node=node2)

    logging.info('Waiting for the new node to sync.  '
                 f'We are {timeout.elapsed_seconds()} seconds in')
    while True:
        assert timeout.check()
        sync_info = node4.get_status()['sync_info']
        if not sync_info['syncing']:
            new_height = sync_info['latest_block_height']
            assert new_height > height_to_sync_to, (
                f'new height {new_height} height to sync to {height_to_sync_to}'
            )
            break
        time.sleep(1)

    logging.info("Checking the messages sent and received")

    # The first two blocks are certainly more than two epochs in the past
    # compared to head, and thus should be requested from archival nodes. Check
    # that it's the case.  Start from 10 to account for possibly skipped blocks
    # while the nodes were starting.
    for height in range(12, HEIGHTS_BEFORE_ROTATE):
        for shard in (0, 1):
            for node in (2, 3):
                other = 5 - node
                if (height, shard, node) in requests:
                    assert (height, shard, node) in responses, (height, shard,
                                                                node)
                    assert (height, shard,
                            other) not in requests, (height, shard, other)
                    assert (height, shard,
                            other) not in responses, (height, shard, other)
                    break
            else:
                assert False, f'Missing request for shard {shard} in block {height}'

    # The last 5 blocks with epoch_length=10 will certainly be in the same epoch
    # as head, or in the previous epoch, and thus should be requested from the
    # block producers.  Note that in our case block producers are also archival
    # nodes so we’re again checking nodes 2 and 3.
    for height in range(new_height - 5, new_height - 1):
        for shard in (0, 1):
            found = False
            for node in (2, 3):
                if (height, shard, node) in requests:
                    assert (height, shard, node) in responses, (height, shard,
                                                                node)
                    found = True
            assert found, f'Missing request for shard {shard} in block {height}'

    logging.info(f'Done.  Took {timeout.elapsed_seconds()} seconds')
