import struct, hashlib, base58, socket, select

from serializer import BinarySerializer
from messages.network import *

ED_PREFIX = "ed25519:"


def create_handshake(my_key_pair_nacl, their_pk_serialized, listen_port):
    handshake = Handshake()
    handshake.version = 28
    handshake.peer_id = PublicKey()
    handshake.target_peer_id = PublicKey()
    handshake.listen_port = listen_port
    handshake.chain_info = PeerChainInfo()
    handshake.edge_info = EdgeInfo()

    handshake.peer_id.keyType = 0
    handshake.peer_id.data = bytes(my_key_pair_nacl.verify_key)

    handshake.target_peer_id.keyType = 0
    handshake.target_peer_id.data = base58.b58decode(
        their_pk_serialized[len(ED_PREFIX):])

    handshake.chain_info.genesis_id = GenesisId()
    handshake.chain_info.height = 0
    handshake.chain_info.tracked_shards = []

    handshake.chain_info.genesis_id.chain_id = 'moo'
    handshake.chain_info.genesis_id.hash = bytes([0] * 32)

    handshake.edge_info.nonce = 1
    handshake.edge_info.signature = Signature()

    handshake.edge_info.signature.keyType = 0
    handshake.edge_info.signature.data = bytes([0] * 64)

    peer_message = PeerMessage()
    peer_message.enum = 'Handshake'
    peer_message.Handshake = handshake

    return peer_message


def sign_handshake(my_key_pair_nacl, handshake):
    peer0 = handshake.peer_id
    peer1 = handshake.target_peer_id
    if peer1.data < peer0.data:
        peer0, peer1 = peer1, peer0

    arr = bytes(
        bytearray([0]) + peer0.data + bytearray([0]) + peer1.data +
        struct.pack('Q', handshake.edge_info.nonce))
    handshake.edge_info.signature.data = my_key_pair_nacl.sign(
        hashlib.sha256(arr).digest()).signature


def send_msg(sock, msg):
    sock.sendall(struct.pack('I', len(msg)))
    sock.sendall(msg)


def send_obj(sock, schema, obj):
    send_msg(sock, BinarySerializer(schema).serialize(obj))


def recv_msg(sock, timeout=None):
    ready = select.select([sock], [], [], timeout)
    if ready[0]:
        len_ = struct.unpack('I', sock.recv(4))[0]
        return sock.recv(len_)
    else:
        return None


def recv_obj(sock, schema, type_, timeout=None):
    raw = recv_msg(sock, timeout)
    if raw is None:
        return None
    return BinarySerializer(schema).deserialize(raw, type_)


def create_and_sign_routed_peer_message(routed_msg_body, target_node, my_key_pair_nacl, schema):
    routed_msg = RoutedMessage()
    routed_msg.target = PeerIdOrHash()
    routed_msg.target.enum = 'PeerId'
    routed_msg.target.PeerId = PublicKey()
    routed_msg.target.PeerId.keyType = 0
    routed_msg.target.PeerId.data = base58.b58decode(target_node.node_key.pk[len(ED_PREFIX):])
    routed_msg.author = PublicKey()
    routed_msg.author.keyType = 0
    routed_msg.author.data = bytes(my_key_pair_nacl.verify_key)
    routed_msg.ttl = 100
    routed_msg.body = routed_msg_body
    routed_msg.signature = Signature()
    routed_msg.signature.keyType = 0

    routed_msg_arr = bytes(bytearray([0, 0]) + routed_msg.target.PeerId.data + bytearray([0]) + routed_msg.author.data + BinarySerializer(schema).serialize(routed_msg.body))
    routed_msg_hash = hashlib.sha256(routed_msg_arr).digest() # MOO extract into a function
    routed_msg.signature.data = my_key_pair_nacl.sign(routed_msg_hash).signature

    peer_message = PeerMessage()
    peer_message.enum = 'Routed'
    peer_message.Routed = routed_msg

    return peer_message


def perform_handshake(node, my_key_pair_nacl, schema):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(node.addr())
        handshake = create_handshake(my_key_pair_nacl, node.node_key.pk, 12345)
        sign_handshake(my_key_pair_nacl, handshake.Handshake)

        send_obj(s, schema, handshake)

        response = recv_obj(s, schema, PeerMessage)
        assert response.enum == 'HandshakeFailure'
        assert response.HandshakeFailure[1].enum == 'GenesisMismatch'

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(node.addr())
    gm = response.HandshakeFailure[1].GenesisMismatch
    handshake.Handshake.chain_info.genesis_id.chain_id = gm.chain_id
    handshake.Handshake.chain_info.genesis_id.hash = gm.hash
    sign_handshake(my_key_pair_nacl, handshake.Handshake)

    send_obj(s, schema, handshake)
    response = recv_obj(s, schema, PeerMessage)

    assert response.enum == 'Handshake', response.enum

    return s
