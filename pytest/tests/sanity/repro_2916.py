#!/usr/bin/env python3
# Spins up two nodes with two shards, waits for couple blocks, snapshots the
# latest chunks, and requests both chunks from the first node, asking for
# receipts for both shards in both requests. We expect the first node to
# respond to exactly one of the requests, for the shard it tracks (for the
# shard it doesn't track it will only have the receipts to the shard it does
# track).
#
# We then kill both nodes, and restart the first node, and do the same
# requests. We expect it to resond the same way. Before 2916 is fixed, it
# fails to respond to the request it was previously responding to due to
# incorrect reconstruction of the receipts.

import asyncio, sys, time
import socket, base58
import nacl.signing, hashlib
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from peer import *
import utils

from messages.tx import *
from messages.block import *
from messages.crypto import *
from messages.network import *


async def main():
    # start a cluster with two shards
    nodes = start_cluster(2, 0, 2, None, [], {})

    height, hash_ = utils.wait_for_blocks(nodes[0], target=3)
    block = nodes[0].get_block(hash_)['result']
    chunk_hashes = [base58.b58decode(x['chunk_hash']) for x in block['chunks']]
    assert len(chunk_hashes) == 2
    assert all([len(x) == 32 for x in chunk_hashes])

    my_key_pair_nacl = nacl.signing.SigningKey.generate()
    received_responses = [None, None]

    # step = 0: before the node is killed
    # step = 1: after the node is killed
    for step in range(2):

        conn0 = await connect(nodes[0].addr())
        await run_handshake(conn0, nodes[0].node_key.pk, my_key_pair_nacl)
        for shard_ord, chunk_hash in enumerate(chunk_hashes):

            request = PartialEncodedChunkRequestMsg()
            request.chunk_hash = chunk_hash
            request.part_ords = []
            request.tracking_shards = [0, 1]

            routed_msg_body = RoutedMessageBody()
            routed_msg_body.enum = 'PartialEncodedChunkRequest'
            routed_msg_body.PartialEncodedChunkRequest = request

            peer_message = create_and_sign_routed_peer_message(
                routed_msg_body, nodes[0], my_key_pair_nacl)

            await conn0.send(peer_message)

            received_response = False

            def predicate(response):
                return response.enum == 'Routed' and response.Routed.body.enum == 'PartialEncodedChunkResponse'

            try:
                response = await asyncio.wait_for(conn0.recv(predicate), 5)
            except (concurrent.futures._base.TimeoutError,
                    asyncio.exceptions.TimeoutError):
                response = None

            if response is not None:
                logger.info("Received response for shard %s" % shard_ord)
                received_response = True
            else:
                logger.info("Didn't receive response for shard %s" % shard_ord)

            if step == 0:
                received_responses[shard_ord] = received_response
            else:
                assert received_responses[
                    shard_ord] == received_response, "The response doesn't match for the chunk in shard %s. Received response before node killed: %s, after: %s" % (
                        shard_ord, received_responses[shard_ord],
                        received_response)

        # we expect first node to only respond to one of the chunk requests, for the shard assigned to it
        assert received_responses[0] != received_responses[1], received_responses

        if step == 0:
            logger.info("Killing and restarting nodes")
            nodes[1].kill()
            nodes[0].kill()
            nodes[0].start()
            time.sleep(1)


asyncio.run(main())
