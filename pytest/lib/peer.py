import asyncio
import hashlib
import struct

import base58
from messages import schema
from messages.crypto import PublicKey, Signature
from messages.network import (EdgeInfo, GenesisId, Handshake, PeerChainInfo,
                              PeerMessage)
from serializer import BinarySerializer

ED_PREFIX = "ed25519:"


class Connection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    async def send(self, message):
        raw_message = BinarySerializer(schema).serialize(message)
        await self.send_raw(raw_message)

    async def send_raw(self, raw_message):
        length = struct.pack('I', len(raw_message))
        self.writer.write(length)
        self.writer.write(raw_message)
        await self.writer.drain()

    async def recv(self):
        response_raw = await self.recv_raw()
        response = BinarySerializer(schema).deserialize(
            response_raw, PeerMessage)
        return response

    async def recv_raw(self):
        length = await self.reader.read(4)
        length = struct.unpack('I', length)[0]
        response = await self.reader.read(length)
        return response

    def do_send(self, message):
        loop = asyncio.get_event_loop()
        loop.create_task(self.send(message))

    def do_send_raw(self, raw_message):
        loop = asyncio.get_event_loop()
        loop.create_task(self.send_raw(raw_message))


async def connect(addr, raw=False) -> Connection:
    reader, writer = await asyncio.open_connection(*addr)
    conn = Connection(reader, writer)

    if not raw:
        # Make handshake
        pass

    return conn


def create_handshake(my_key_pair_nacl, their_pk_serialized, listen_port):
    handshake = Handshake()
    handshake.version = 29
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
