#!/usr/bin/env python3
"""
Handshake from the Future

Start a real node and connect to it. Send handshake with different layout that it knows
but with valid future version, and expect to receive HandshakeFailure with current version.
"""
import asyncio
import socket
import sys
import time
import random
import pathlib

import base58
import nacl.signing

sys.path.append(str(pathlib.Path(__file__).resolve().parents[3] / 'lib'))
from cluster import start_cluster
from peer import ED_PREFIX, connect, create_handshake, sign_handshake, BinarySerializer, schema

nodes = start_cluster(1, 0, 4, None, [], {})


async def main():
    random.seed(0)

    my_key_pair_nacl = nacl.signing.SigningKey.generate()

    conn = await connect(nodes[0].addr())

    handshake = create_handshake(my_key_pair_nacl, nodes[0].node_key.pk, 12345)
    # Use future version
    handshake.Handshake.version = 2**32 - 1
    raw_message = BinarySerializer(schema).serialize(handshake)

    # Keep header (9 bytes)
    # - 1 byte  PeerMessage::Handshake enum id
    # - 4 bytes version
    # - 4 bytes oldest_supported_version
    raw_message = raw_message[:9] + bytes(
        [random.randint(0, 255) for _ in range(random.randint(1, 32))])

    # First handshake attempt. Should fail with Protocol Version Mismatch
    await conn.send_raw(raw_message)
    response = await conn.recv()

    assert response.enum == 'HandshakeFailure', response.enum
    assert response.HandshakeFailure[
        1].enum == 'ProtocolVersionMismatch', response.HandshakeFailure[1].enum
    pvm = response.HandshakeFailure[1].ProtocolVersionMismatch.version
    handshake.Handshake.version = pvm


asyncio.run(main())
