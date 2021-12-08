#!/usr/bin/env python3
"""
Saturate routing table with edges.

Spin a node and connect to it with the python client. Fake several identities in the network and send
edges to the node. Measure the impact of these messages in the node with respect to the time the PeerManagerActor
is blocked.
"""
import asyncio
import socket
import sys
import time
import struct
import hashlib
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import nacl.signing
from cluster import start_cluster
from configured_logger import logger
from peer import connect, run_handshake, Connection
from utils import LogTracker
from messages.network import Edge, SyncData, PeerMessage
from messages.crypto import PublicKey, Signature
from random import randint, seed
import base58

seed(0)


def key_seed():
    return bytes([randint(0, 255) for _ in range(32)])


async def consume(conn: Connection):
    while True:
        message = await conn.recv()


def create_sync_data(accounts=[], edges=[]):
    sync_data = SyncData()
    sync_data.accounts = accounts
    sync_data.edges = edges

    peer_message = PeerMessage()
    peer_message.enum = 'Sync'
    peer_message.Sync = sync_data
    return peer_message


def create_edge(key0, key1, nonce):
    if bytes(key1.verify_key) < bytes(key0.verify_key):
        key0, key1 = key1, key0

    edge = Edge()
    edge.peer0 = PublicKey()
    edge.peer0.keyType = 0
    edge.peer0.data = bytes(key0.verify_key)

    edge.peer1 = PublicKey()
    edge.peer1.keyType = 0
    edge.peer1.data = bytes(key1.verify_key)

    edge.nonce = nonce

    val = bytes([0]) + bytes(edge.peer0.data) + bytes([0]) + bytes(
        edge.peer1.data) + struct.pack('Q', nonce)
    hsh = hashlib.sha256(val).digest()
    enc58 = base58.b58encode(hsh)

    edge.signature0 = Signature()
    edge.signature0.keyType = 0
    edge.signature0.data = key0.sign(hashlib.sha256(val).digest()).signature

    edge.signature1 = Signature()
    edge.signature1.keyType = 0
    edge.signature1.data = key1.sign(hashlib.sha256(val).digest()).signature

    edge.removal_info = None

    return edge


async def main():
    key_pair_0 = nacl.signing.SigningKey(key_seed())
    tracker = LogTracker(nodes[0])
    conn = await connect(nodes[0].addr())
    await run_handshake(conn,
                        nodes[0].node_key.pk,
                        key_pair_0,
                        listen_port=12345)

    num_nodes = 300

    def create_update():
        key_pairs = [key_pair_0] + [
            nacl.signing.SigningKey(key_seed()) for _ in range(num_nodes - 1)
        ]
        nonces = [[1] * num_nodes for _ in range(num_nodes)]

        edges = []
        for i in range(num_nodes):
            for j in range(i):
                edge = create_edge(key_pairs[i], key_pairs[j], nonces[i][j])
                edges.append(edge)
        return create_sync_data(edges=edges)

    asyncio.get_event_loop().create_task(consume(conn))

    for i in range(3):
        update = create_update()
        logger.info("Sending update...")
        await conn.send(update)
        logger.info("Sent...")
        await asyncio.sleep(1)

    assert tracker.check("delay_detector: LONG DELAY!") is False


nodes = start_cluster(1, 0, 4, None, [], {})
asyncio.run(main())
