"""
PeersRequest

Start one real node. Create a connection (conn0) to real node, send PeersRequest and wait for the response.
Create a new connection (conn1) to real node, send PeersRequest and wait for the response. In the latter
response there must exist an entry with information from the first connection that was established.
"""
import asyncio
import socket
import sys
import time

sys.path.append('lib')

import nacl.signing
from cluster import start_cluster
from peer import ED_PREFIX, connect, run_handshake, create_peer_request
from utils import obj_to_string
from messages import schema

nodes = start_cluster(1, 0, 4, None, [], {})


async def main():
    key_pair_0 = nacl.signing.SigningKey.generate()
    conn0 = await connect(nodes[0].addr())
    await run_handshake(conn0,
                        nodes[0].node_key.pk,
                        key_pair_0,
                        listen_port=12345)
    peer_request = create_peer_request()
    await conn0.send(peer_request)
    response = await conn0.recv('PeersResponse')
    assert response.enum == 'PeersResponse', obj_to_string(response)

    key_pair_1 = nacl.signing.SigningKey.generate()
    conn1 = await connect(nodes[0].addr())
    await run_handshake(conn1,
                        nodes[0].node_key.pk,
                        key_pair_1,
                        listen_port=12346)
    peer_request = create_peer_request()
    await conn1.send(peer_request)
    response = await conn1.recv('PeersResponse')
    assert response.enum == 'PeersResponse', obj_to_string(response)
    assert any(peer_info.addr.V4[1] == 12345
               for peer_info in response.PeersResponse), obj_to_string(response)


asyncio.run(main())
