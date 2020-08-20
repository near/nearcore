"""
Handshake

Start a real node and connect to it. Send handshake with wrong genesis and version and
expect receiving HandshakeFailure. Use that information to send valid handshake and
connect to the node.
"""
import asyncio
import socket
import sys
import time

sys.path.append('lib')

import base58
import nacl.signing
from cluster import start_cluster
from peer import ED_PREFIX, connect, create_handshake, sign_handshake
from utils import obj_to_string


nodes = start_cluster(1, 0, 4, None, [], {})


async def main():
    my_key_pair_nacl = nacl.signing.SigningKey.generate()

    conn = await connect(nodes[0].addr())

    # First handshake attempt. Should fail with Genesis Mismatch
    handshake = create_handshake(my_key_pair_nacl, nodes[0].node_key.pk, 12345)
    handshake.Handshake.edge_info.nonce = 1505
    sign_handshake(my_key_pair_nacl, handshake.Handshake)

    await conn.send(handshake)
    response = await conn.recv()

    assert response.enum == 'HandshakeFailure', response.enum
    assert response.HandshakeFailure[1].enum == 'GenesisMismatch', response.HandshakeFailure[1].enum

    # Second handshake attempt. Should fail with Protocol Version Mismatch
    gm = response.HandshakeFailure[1].GenesisMismatch
    handshake.Handshake.chain_info.genesis_id.chain_id = gm.chain_id
    handshake.Handshake.chain_info.genesis_id.hash = gm.hash
    sign_handshake(my_key_pair_nacl, handshake.Handshake)

    await conn.send(handshake)
    response = await conn.recv()

    assert response.enum == 'HandshakeFailure', response.enum
    assert response.HandshakeFailure[1].enum == 'ProtocolVersionMismatch', response.HandshakeFailure[1].enum

    # Third handshake attempt.
    pvm = response.HandshakeFailure[1].ProtocolVersionMismatch
    handshake.Handshake.version = pvm
    sign_handshake(my_key_pair_nacl, handshake.Handshake)

    await conn.send(handshake)
    response = await conn.recv()

    # Connection should be closed by other peer because too large nonce on handshake.
    assert response is None


asyncio.run(main())
