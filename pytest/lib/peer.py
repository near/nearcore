import struct, hashlib, base58

from serializer import BinarySerializer
from messages.network import *

ED_PREFIX = "ed25519:"


def create_handshake(my_key_pair_nacl, their_pk_serialized, listen_port):
    handshake = Handshake()
    handshake.version = 26
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


def recv_msg(sock):
    len_ = struct.unpack('I', sock.recv(4))[0]
    return sock.recv(len_)


def recv_obj(sock, schema, type_):
    return BinarySerializer(schema).deserialize(recv_msg(sock), type_)
