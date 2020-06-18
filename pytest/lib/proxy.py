# A library providing a node wrapper that intercepts all the incoming messages and
# outgoing responces (but not the initiated outgoing messages and incoming responces)
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
# Note that since the handler is called on incoming messages and outgoing responces, if
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

import multiprocessing, struct, socket, select, atexit

from serializer import BinarySerializer

from messages.crypto import *
from messages.network import *
from messages.block import *
from messages.tx import *

MSG_TIMEOUT = 10

schema = dict(crypto_schema + network_schema + block_schema + tx_schema)


def proxy_cleanup(proxy):
    proxy.stopped.value = 1
    for p in proxy.ps:
        p.join()
    if proxy.error.value != 0:
        assert False, "One of the proxy processes failed, search for the stacktraces above"


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


def proxify_node(node, handler, stopped, error):
    old_port = node.port
    new_port = old_port + 100

    def handle_connection(conn, addr, stopped, error):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_node:
                s_node.connect(("127.0.0.1", old_port))

                conn.setblocking(0)
                s_node.setblocking(0)

                peer_port_holder = [None]

                while 0 == stopped.value and 0 == error.value:
                    ready = select.select([conn, s_node], [], [], MSG_TIMEOUT)
                    if not ready[0]:
                        print("TIMEOUT")
                        time.sleep(1)
                        continue

                    if conn in ready[0]:
                        data = conn.recv(4)

                        if not data:
                            continue

                        data = conn.recv(struct.unpack('I', data)[0])
                        decision = call_handler(data, handler, peer_port_holder,
                                                [node.port])
                        if type(decision) == bytes:
                            data = decision
                            decision = True
                        assert type(decision) == bool, type(decision)

                        if decision:
                            s_node.sendall(struct.pack('I', len(data)))
                            s_node.sendall(data)

                    elif s_node in ready[0]:
                        data = s_node.recv(4)

                        if not data:
                            continue

                        data = s_node.recv(struct.unpack('I', data)[0])
                        decision = call_handler(data, handler, [node.port],
                                                peer_port_holder)

                        if type(decision) == bytes:
                            data = decision
                            decision = True
                        assert type(decision) == bool, type(decision)

                        if decision:
                            conn.sendall(struct.pack('I', len(data)))
                            conn.sendall(data)

                    else:
                        assert False, (conn, s_node, ready[0])
        except ConnectionResetError:
            print("Connection closed, ignoring the error")
            pass
        except:
            error.value = 1
            raise

    def listener():
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_inc:
                s_inc.settimeout(1)
                s_inc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s_inc.bind(("127.0.0.1", new_port))
                s_inc.listen()
                while 0 == stopped.value and 0 == error.value:
                    try:
                        conn, addr = s_inc.accept()
                    except socket.timeout:
                        continue
                    print('Connected by', addr)
                    p = multiprocessing.Process(target=handle_connection,
                                                args=(conn, addr, stopped,
                                                      error))
                    p.start()

        except:
            error.value = 1
            raise

    p = multiprocessing.Process(target=listener, args=())
    p.start()

    node.port = new_port
    return p


def port_holder_to_node_ord(holder):
    return None if holder[0] is None else (holder[0] - 24477) % 100


def call_handler(raw_msg, handler, sender_port_holder, receiver_port_holder):
    try:
        obj = BinarySerializer(schema).deserialize(raw_msg, PeerMessage)
        assert BinarySerializer(schema).serialize(obj) == raw_msg

        if obj.enum == 'Handshake':
            obj.Handshake.listen_port += 100
            if sender_port_holder[0] is None:
                sender_port_holder[0] = obj.Handshake.listen_port

        decision = handler(obj, port_holder_to_node_ord(sender_port_holder),
                           port_holder_to_node_ord(receiver_port_holder))

        if decision == True and obj.enum == 'Handshake':
            decision = obj

        if type(decision) != bool:
            decision = BinarySerializer(schema).serialize(decision)

        return decision

    except:
        if raw_msg[0] == 13:
            # raw_msg[0] == 13 is RoutedMessage. Skip leading fields to get to the RoutedMessageBody
            ser = BinarySerializer(schema)
            ser.array = bytearray(raw_msg)
            ser.offset = 1
            ser.deserialize_field(PeerIdOrHash)
            ser.deserialize_field(PublicKey)
            ser.deserialize_field(Signature)
            ser.deserialize_field('u8')

            # The next byte is the variant ordinal of the `RoutedMessageBody`.
            # Skip if it's the ordinal of a variant for which the schema is not ported yet
            if raw_msg[ser.offset] in [3, 4, 5, 7, 10]:
                return True
            print("ERROR 13", int(raw_msg[ser.offset]))

        else:
            print("ERROR", int(raw_msg[0]))

        raise

    return True
