import sys, time
import socket, base58
import nacl.signing

sys.path.append('lib')

from cluster import start_cluster
from peer import *
from utils import obj_to_string
from messages.crypto import *
from messages.network import *

schema = dict(crypto_schema + network_schema)
my_key_pair_nacl = nacl.signing.SigningKey.generate()

nodes = start_cluster(1, 0, 4, None, [], {})

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect(nodes[0].addr())
    handshake = create_handshake(my_key_pair_nacl, nodes[0].node_key.pk, 12345)
    sign_handshake(my_key_pair_nacl, handshake.Handshake)

    send_obj(s, schema, handshake)

    response = recv_obj(s, schema, PeerMessage)
    assert response.enum == 'HandshakeFailure'
    assert response.HandshakeFailure[1].enum == 'GenesisMismatch'

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect(nodes[0].addr())
    gm = response.HandshakeFailure[1].GenesisMismatch
    handshake.Handshake.chain_info.genesis_id.chain_id = gm.chain_id
    handshake.Handshake.chain_info.genesis_id.hash = gm.hash
    sign_handshake(my_key_pair_nacl, handshake.Handshake)

    send_obj(s, schema, handshake)
    response = recv_obj(s, schema, PeerMessage)

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
    assert response.Handshake.version == handshake.Handshake.version
    assert response.Handshake.listen_port == nodes[0].addr()[1]
