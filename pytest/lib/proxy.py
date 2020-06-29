# A library providing a node wrapper that intercepts all the incoming messages and
# outgoing responses (but not the initiated outgoing messages and incoming responses)
# from a node, and calls a handler on them. The handler can then decide to drop the
# message, deliver the message, or change the message.
#
# Usage:
# 1. Create a proxy = NodesProxy(handler).
#    handler takes two arguments, the message (deserialized), the ordinal of the sending
#    peer and the ordinal of the receiving peer. The ordinal is derived from the port,
#    and assumes no tampering with ports was done
# 2. Call proxy.proxify_node(node) before the node is started.
#    proxify_node will start a process that receives the connections on the port 100
#    larger than the original node port. It will change the port of the `Node` object
#    passed to it.
#
# Note that since the handler is called on incoming messages and outgoing responses, if
# all the nodes in the cluster are proxified, each message exchanged in the cluster will
# have the handler called exactly once.
#
# The proxy will register an atexit function that will gracefully shut down all the
# processes, and fail the test if any of the processes failed.
#
# `start_cluster` accepts a handler as its last argument, and automatically proxifies
# all the nodes if the parameter is not None
#
# See `tests/sanity/nodes_proxy.py` for an example usage

import asyncio
import atexit
import functools
import multiprocessing
import select
import socket
import struct
import time

from messages.block import *
from messages.crypto import *
from messages.network import *
from messages.tx import *
from serializer import BinarySerializer

MSG_TIMEOUT = 10
_MY_PORT = [None]

schema = dict(crypto_schema + network_schema + block_schema + tx_schema)


def proxy_cleanup(proxy):
    proxy.stopped.value = 1
    for p in proxy.ps:
        p.join()
    if proxy.error.value != 0:
        assert False, "One of the proxy processes failed, search for the stacktraces above"


def port_holder_to_node_ord(holder):
    return None if holder[0] is None else (holder[0] - 24477) % 100


class ProxyHandler:
    def __init__(self, ordinal):
        self.ordinal = ordinal
        self.recv_from_map = {}
        self.send_to_map = {}
        self.loop = asyncio.get_event_loop()

    @property
    def me(self):
        return self.ordinal

    def other(self, ordinal_a, ordinal_b):
        assert self.me == ordinal_a or self.me == ordinal_b
        if ordinal_a == self.me:
            return ordinal_b
        else:
            return ordinal_a

    async def _handle(self, raw_message, *, writer, sender_port_holder, receiver_port_holder, ordinal_to_writer):
        try:
            message = BinarySerializer(schema).deserialize(
                raw_message, PeerMessage)
            assert BinarySerializer(schema).serialize(message) == raw_message

            if message.enum == 'Handshake':
                message.Handshake.listen_port += 100
                if sender_port_holder[0] is None:
                    sender_port_holder[0] = message.Handshake.listen_port

            sender_ordinal = port_holder_to_node_ord(sender_port_holder)
            receiver_ordinal = port_holder_to_node_ord(receiver_port_holder)
            other_ordinal = self.other(sender_ordinal, receiver_ordinal)

            if other_ordinal is not None and not other_ordinal in ordinal_to_writer:
                ordinal_to_writer[other_ordinal] = writer

            decision = await self.handle(message, sender_ordinal, receiver_ordinal)

            if decision is True and message.enum == 'Handshake':
                decision = message

            if not isinstance(decision, bool):
                decision = BinarySerializer(schema).serialize(decision)

            return decision
        except:
            # TODO: Remove this
            if raw_message[0] == 13:
                # raw_message[0] == 13 is RoutedMessage. Skip leading fields to get to the RoutedMessageBody
                ser = BinarySerializer(schema)
                ser.array = bytearray(raw_message)
                ser.offset = 1
                ser.deserialize_field(PeerIdOrHash)
                ser.deserialize_field(PublicKey)
                ser.deserialize_field(Signature)
                ser.deserialize_field('u8')

                # The next byte is the variant ordinal of the `RoutedMessageBody`.
                # Skip if it's the ordinal of a variant for which the schema is not ported yet
                if raw_message[ser.offset] in [3, 4, 5, 7, 10]:
                    # Allow the handler determine if the message should be passed even when it couldn't be deserialized
                    return await self.handle(None, sender_ordinal, receiver_ordinal) is not False
                print("ERROR 13", int(raw_message[ser.offset]))

            else:
                print("ERROR", int(raw_message[0]))

            raise

        return True

    def get_writer(self, to, fr=None):
        if to == self.ordinal:
            if fr is None and len(self.recv_from_map) > 0:
                fr = next(iter(self.recv_from_map.keys()))
            return self.recv_from_map.get(fr)
        else:
            return self.send_to_map.get(to)

    async def send_binary(self, raw_message, to, fr=None):
        writer = self.get_writer(to, fr)
        if writer is None:
            print(
                f"Writer not known: to={to}, fr={fr}, send={self.send_to_map.keys()}, recv={self.recv_from_map.keys()}")
        else:
            writer.write(struct.pack('I', len(raw_message)))
            writer.write(raw_message)
            await writer.drain()

    async def send_message(self, message, to, fr=None):
        raw_message = BinarySerializer(schema).serialize(message)
        await self.send_binary(raw_message, to, fr)

    def do_send_binary(self, raw_message, to, fr=None):
        self.loop.create_task(self.send_binary(raw_message, to, fr))

    def do_send_message(self, msg, to, fr=None):
        self.loop.create_task(self.send_message(msg, to, fr))

    async def handle(self, msg, fr, to):
        return True


class NodesProxy:
    def __init__(self, handler):
        assert handler is not None
        self.handler = handler
        self.stopped = multiprocessing.Value('i', 0)
        self.error = multiprocessing.Value('i', 0)
        self.ps = []
        atexit.register(proxy_cleanup, self)

    def proxify_node(self, node):
        p = proxify_node(node, self.handler, self.stopped, self.error)
        self.ps.append(p)


async def stop_server(server):
    server.close()
    await server.wait_closed()


def check_finish(server, stopped, error):
    loop = asyncio.get_running_loop()
    if 0 == stopped.value and 0 == error.value:
        loop.call_later(1, check_finish, server, stopped, error)
    else:
        server.close()

async def bridge(reader, writer, handler_fn, stopped, error):
    while 0 == stopped.value and 0 == error.value:
        header = await reader.read(4)
        if not header:
            continue

        assert len(header) == 4, header
        raw_message = await reader.read(struct.unpack('I', header)[0])
        decision = await handler_fn(raw_message)

        if isinstance(decision, bytes):
            raw_message = decision
            decision = True

        if decision:
            writer.write(struct.pack('I', len(raw_message)))
            writer.write(raw_message)
            await writer.drain()


async def handle_connection(outer_reader, outer_writer, inner_port, outer_port, handler, stopped, error):
    try:
        inner_reader, inner_writer = await asyncio.open_connection(
            '127.0.0.1', inner_port)

        my_port = [outer_port]
        peer_port_holder = [None]

        inner_to_outer = bridge(inner_reader, outer_writer, functools.partial(
            handler._handle, writer=inner_writer, sender_port_holder=my_port, receiver_port_holder=peer_port_holder,
            ordinal_to_writer=handler.recv_from_map,
        ), stopped, error)

        outer_to_inner = bridge(outer_reader, inner_writer, functools.partial(
            handler._handle, writer=outer_writer, sender_port_holder=peer_port_holder, receiver_port_holder=my_port,
            ordinal_to_writer=handler.send_to_map,
        ), stopped, error)

        await asyncio.gather(inner_to_outer, outer_to_inner)

    except:
        error.value = 1
        raise


async def listener(inner_port, outer_port, handler_ctr, stopped, error):
    try:
        handler = handler_ctr(port_holder_to_node_ord([outer_port]))

        async def start_connection(reader, writer):
            await handle_connection(reader, writer, inner_port, outer_port, handler, stopped, error)

        server = await asyncio.start_server(
            start_connection, '127.0.0.1', outer_port)

        check_finish(server, stopped, error)

        async with server:
            await server.serve_forever()
    except asyncio.CancelledError:
        stopped.value = 1
    except:
        error.value = 1
        raise


def start_server(inner_port, outer_port, handler_ctr, stopped, error):
    _MY_PORT[0] = outer_port
    asyncio.run(listener(inner_port, outer_port, handler_ctr, stopped, error))


def proxify_node(node, handler, stopped, error):
    inner_port = node.port
    outer_port = inner_port + 100

    p = multiprocessing.Process(target=start_server, args=(
        inner_port, outer_port, handler, stopped, error))
    p.start()

    node.port = outer_port
    return p
