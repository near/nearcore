#!/usr/bin/env python3
# Spins up two nodes with two shards, waits for couple blocks, snapshots the
# latest chunks, and requests both chunks from the first node, asking for
# receipts for both shards in both requests. We expect the first node have full
# responses for only one of these shards -- the shard it tracks (for the
# shard it doesn't track it will only have the receipts to the shard it does
# track).
#
# We then kill both nodes, and restart the first node, and do the same
# requests. We expect it to resond the same way. Before 2916 is fixed, it
# fails to respond to the request it was previously responding to due to
# incorrect reconstruction of the receipts.

import asyncio, sys, time
import base58
import nacl.signing
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
    tracking_shards_scenario = None  # will be either [0, 1] or [1, 0]; we'll detect

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

            received_receipt_shards = set()

            def predicate(response):
                return response.enum == 'Routed' and response.Routed.body.enum == 'PartialEncodedChunkResponse'

            try:
                response = await asyncio.wait_for(conn0.recv(predicate), 5)
            except (concurrent.futures._base.TimeoutError,
                    asyncio.exceptions.TimeoutError):
                assert False, "A response is always expected for partial encoded chunk request."

            for receipt_proof in response.Routed.body.PartialEncodedChunkResponse.receipts:
                shard_proof = receipt_proof.f2
                assert shard_proof.from_shard_id == shard_ord, \
                    "Basic correctness check failed: the receipt for chunk of shard {} has the wrong from_shard_id {}".format(shard_ord, shard_proof.from_shard_id)
                received_receipt_shards.add(shard_proof.to_shard_id)

            if step == 0 and shard_ord == 0:
                # detect how the two validators decided who tracks which shard.
                if received_receipt_shards == set([1]):
                    # if the first validator only responded receipt to shard 1, then
                    # it's only tracking shard 1. (Otherwise it should respond with [0, 1]
                    # since it tracks all receipts coming from shard 0.)
                    tracking_shards_scenario = [1, 0]
                else:
                    tracking_shards_scenario = [0, 1]

            if tracking_shards_scenario == [0, 1]:
                if shard_ord == 0:
                    assert received_receipt_shards == set([0, 1]), \
                        "Request to node 0 (tracks shard 0), chunk 0, expected receipts to [0, 1], actual {}".format(received_receipt_shards)
                else:
                    assert received_receipt_shards == set([0]), \
                        "Request to node 0 (tracks shard 0), chunk 1, expected receipts to [0], actual {}".format(received_receipt_shards)
            else:  # [1, 0]
                if shard_ord == 0:
                    assert received_receipt_shards == set([1]), \
                        "Request to node 0 (tracks shard 1), chunk 0, expected receipts to [1], actual {}".format(received_receipt_shards)
                else:
                    assert received_receipt_shards == set([0, 1]), \
                        "Request to node 0 (tracks shard 1), chunk 1, expected receipts to [0, 1], actual {}".format(received_receipt_shards)

        if step == 0:
            logger.info("Killing and restarting nodes")
            nodes[1].kill()
            nodes[0].kill()
            nodes[0].start()
            time.sleep(1)


asyncio.run(main())
