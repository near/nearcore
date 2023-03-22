import asyncio
import concurrent
import hashlib
import struct

import base58

from configured_logger import logger
from messages import schema
from messages.crypto import PublicKey, Signature
from messages.network import (EdgeInfo, GenesisId, Handshake, PeerChainInfoV2,
                              PeerMessage, RoutedMessage, PeerIdOrHash)
from serializer import BinarySerializer
from nacl.signing import SigningKey
from typing import Optional

ED_PREFIX = "ed25519:"


class Connection:

    def __init__(self, reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.is_closed = False

    async def send(self, message):
        raw_message = BinarySerializer(schema).serialize(message)
        await self.send_raw(raw_message)

    async def send_raw(self, raw_message):
        length = struct.pack('I', len(raw_message))
        self.writer.write(length)
        self.writer.write(raw_message)
        await self.writer.drain()

    # returns None on timeout
    async def recv(self, expected=None):
        while True:
            response_raw = await self.recv_raw()

            # Connection was closed on the other side
            if response_raw is None:
                return None
            # TODO(CP-85): when removing borsh support, fix this to use protobufs,
            # (or preferably reimplement the test in rust).
            try:
                response = BinarySerializer(schema).deserialize(
                    response_raw, PeerMessage)
            except IndexError:
                # unparsable message, ignore.
                continue

            if expected is None or response.enum == expected or (
                    callable(expected) and expected(response)):
                return response

    async def recv_raw(self):
        length = await self.reader.read(4)

        if len(length) == 0:
            self.is_closed = True
            return None
        else:
            length = struct.unpack('I', length)[0]
            response = b''

            while len(response) < length:
                response += await self.reader.read(length - len(response))
                if len(response) < length:
                    logger.info(f"Downloading message {len(response)}/{length}")

            return response

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()

    def do_send(self, message):
        loop = asyncio.get_event_loop()
        loop.create_task(self.send(message))

    def do_send_raw(self, raw_message):
        loop = asyncio.get_event_loop()
        loop.create_task(self.send_raw(raw_message))


async def connect(addr) -> Connection:
    reader, writer = await asyncio.open_connection(*addr)
    conn = Connection(reader, writer)
    return conn


def create_handshake(my_key_pair_nacl,
                     their_pk_serialized,
                     listen_port,
                     version=0):
    """
    Create handshake message but with placeholders in:
        - version
        - genesis_id.chain_id
        - genesis_id.hash
        - edge_info.signature
    """
    handshake = Handshake()
    handshake.version = version
    handshake.oldest_supported_version = version
    handshake.peer_id = PublicKey()
    handshake.target_peer_id = PublicKey()
    handshake.listen_port = listen_port
    handshake.chain_info = PeerChainInfoV2()
    handshake.edge_info = EdgeInfo()

    handshake.peer_id.keyType = 0
    handshake.peer_id.data = bytes(my_key_pair_nacl.verify_key)

    handshake.target_peer_id.keyType = 0
    handshake.target_peer_id.data = base58.b58decode(
        their_pk_serialized[len(ED_PREFIX):])

    handshake.chain_info.genesis_id = GenesisId()
    handshake.chain_info.height = 0
    handshake.chain_info.tracked_shards = []
    handshake.chain_info.archival = False

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


def create_peer_request():
    peer_message = PeerMessage()
    peer_message.enum = 'PeersRequest'
    peer_message.PeersRequest = ()
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


async def run_handshake(conn: Connection,
                        target_public_key: PublicKey,
                        key_pair: SigningKey,
                        listen_port=12345):
    handshake = create_handshake(key_pair, target_public_key, listen_port)

    async def send_handshake():
        sign_handshake(key_pair, handshake.Handshake)
        await conn.send(handshake)
        # The peer might sent us an unsolicited message before replying to
        # a successful handshake.  This is because node is multi-threaded and
        # peers are added to PeerManager before the reply is sent.  Since we
        # don’t care about those messages, ignore them and wait for some kind of
        # Handshake reply.
        return await conn.recv(lambda msg: msg.enum.startswith('Handshake'))

    response = await send_handshake()

    if response.enum == 'HandshakeFailure' and response.HandshakeFailure[
            1].enum == 'ProtocolVersionMismatch':
        pvm = response.HandshakeFailure[1].ProtocolVersionMismatch.version
        handshake.Handshake.version = pvm
        response = await send_handshake()

    if response.enum == 'HandshakeFailure' and response.HandshakeFailure[
            1].enum == 'GenesisMismatch':
        gm = response.HandshakeFailure[1].GenesisMismatch
        handshake.Handshake.chain_info.genesis_id.chain_id = gm.chain_id
        handshake.Handshake.chain_info.genesis_id.hash = gm.hash
        response = await send_handshake()

    assert response.enum == 'Handshake', response.enum if response.enum != 'HandshakeFailure' else response.HandshakeFailure[
        1].enum


def create_and_sign_routed_peer_message(routed_msg_body, target_node,
                                        my_key_pair_nacl):
    routed_msg = RoutedMessage()
    routed_msg.target = PeerIdOrHash()
    routed_msg.target.enum = 'PeerId'
    routed_msg.target.PeerId = PublicKey()
    routed_msg.target.PeerId.keyType = 0
    routed_msg.target.PeerId.data = base58.b58decode(
        target_node.node_key.pk[len(ED_PREFIX):])
    routed_msg.author = PublicKey()
    routed_msg.author.keyType = 0
    routed_msg.author.data = bytes(my_key_pair_nacl.verify_key)
    routed_msg.ttl = 100
    routed_msg.body = routed_msg_body
    routed_msg.signature = Signature()
    routed_msg.signature.keyType = 0

    routed_msg_arr = bytes(
        bytearray([0, 0]) + routed_msg.target.PeerId.data + bytearray([0]) +
        routed_msg.author.data +
        BinarySerializer(schema).serialize(routed_msg.body))
    routed_msg_hash = hashlib.sha256(routed_msg_arr).digest()
    routed_msg.signature.data = my_key_pair_nacl.sign(routed_msg_hash).signature

    peer_message = PeerMessage()
    peer_message.enum = 'Routed'
    peer_message.Routed = routed_msg

    return peer_message
