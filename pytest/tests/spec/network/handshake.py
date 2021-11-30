#!/usr/bin/env python3
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
import pathlib

import base58
import nacl.signing

sys.path.append(str(pathlib.Path(__file__).resolve().parents[3] / 'lib'))
from cluster import start_cluster
from peer import ED_PREFIX, connect, create_handshake, sign_handshake

nodes = start_cluster(1, 0, 4, None, [], {})


async def main():
    my_key_pair_nacl = nacl.signing.SigningKey.generate()

    conn = await connect(nodes[0].addr())

    handshake = create_handshake(my_key_pair_nacl, nodes[0].node_key.pk, 12345)

    # First handshake attempt. Should fail with Protocol Version Mismatch
    sign_handshake(my_key_pair_nacl, handshake.Handshake)
    await conn.send(handshake)
    response = await conn.recv()

    assert response.enum == 'HandshakeFailure', response.enum
    assert response.HandshakeFailure[
        1].enum == 'ProtocolVersionMismatch', response.HandshakeFailure[1].enum
    pvm = response.HandshakeFailure[1].ProtocolVersionMismatch.version
    handshake.Handshake.version = pvm

    # Second handshake attempt. Should fail with Genesis Mismatch
    sign_handshake(my_key_pair_nacl, handshake.Handshake)
    await conn.send(handshake)
    response = await conn.recv()

    assert response.enum == 'HandshakeFailure', response.enum
    assert response.HandshakeFailure[
        1].enum == 'GenesisMismatch', response.HandshakeFailure[1].enum
    gm = response.HandshakeFailure[1].GenesisMismatch
    handshake.Handshake.chain_info.genesis_id.chain_id = gm.chain_id
    handshake.Handshake.chain_info.genesis_id.hash = gm.hash

    # Third handshake attempt. Should succeed
    sign_handshake(my_key_pair_nacl, handshake.Handshake)
    await conn.send(handshake)
    response = await conn.recv()

    assert response.enum == 'Handshake', response.enum
    assert response.Handshake.chain_info.genesis_id.chain_id == handshake.Handshake.chain_info.genesis_id.chain_id
    assert response.Handshake.chain_info.genesis_id.hash == handshake.Handshake.chain_info.genesis_id.hash
    assert response.Handshake.edge_info.nonce == 1
    assert response.Handshake.peer_id.keyType == 0
    assert response.Handshake.peer_id.data == base58.b58decode(
        nodes[0].node_key.pk[len(ED_PREFIX):])
    assert response.Handshake.target_peer_id.keyType == 0
    assert response.Handshake.target_peer_id.data == bytes(
        my_key_pair_nacl.verify_key)
    assert response.Handshake.listen_port == nodes[0].addr()[1]
    assert response.Handshake.version == handshake.Handshake.version


asyncio.run(main())
