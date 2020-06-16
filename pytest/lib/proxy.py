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


class NodesProxy:
    def __init__(self, handler):
        assert handler is not None
        self.handler = handler
        self.stopped = multiprocessing.Value('i', 0)
        self.error = multiprocessing.Value('i', 0)
        self.ps = []
        atexit.register(proxy_cleanup, self)

    def proxify_node(self, node):
        p = proxify_node(node, self.handler, self.handler, self.stopped, self.error)
        self.ps.append(p)
        

def proxify_node(node, incoming_handler, response_handler, stopped, error):
    old_port = node.port
    new_port = old_port + 100

    def handle_connection(conn, addr, stopped, error):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_node:
                s_node.connect(("127.0.0.1", old_port))

                #s_node.setblocking(0)

                while 0 == stopped.value and 0 == error.value:
                    print("BEFORE")
                    ready = select.select([conn], [], [], MSG_TIMEOUT)
                    if not ready[0]:
                        print("TIMEOUT")
                        break

                    data = conn.recv(4)
                    print("AFTER")

                    if not data:
                        break
                    data = conn.recv(struct.unpack('I', data)[0])
                    decision = call_handler(data, incoming_handler, addr)
                    if type(decision) == bytes:
                        data = decision
                        decision = True
                    assert type(decision) == bool, type(decision)

                    if decision:
                        s_node.sendall(struct.pack('I', len(data)))
                        s_node.sendall(data)

                        print("BEFORE2")
                        ready = select.select([s_node], [], [], MSG_TIMEOUT)
                        if not ready[0]:
                            print("TIMEOUT2")
                            continue

                        len_bytes = s_node.recv(4)
                        print("AFTER2")
                        if not len_bytes:
                            continue

                        len_ = struct.unpack('I', len_bytes)[0]
                        response = s_node.recv(len_)

                        decision = call_handler(response, response_handler, addr)

                        if type(decision) == bytes:
                            response = decision
                            decision = True
                        assert type(decision) == bool, type(decision)

                        if decision:
                            conn.sendall(struct.pack('I', len(response)))
                            conn.sendall(response)
                        else:
                            break
                    else:
                        break
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
                    p = multiprocessing.Process(target=handle_connection, args=(conn, addr, stopped, error))
                    p.start()
                    
        except:
            error.value = 1
            raise

    p = multiprocessing.Process(target=listener, args=())
    p.start()

    node.port = new_port
    return p


def call_handler(raw_msg, handler, addr):
    try:
        obj = BinarySerializer(schema).deserialize(raw_msg, PeerMessage)
        assert BinarySerializer(schema).serialize(obj) == raw_msg

        if obj.enum != 'Routed':
            print("   >", obj.enum)
        else:
            print("   > Routed >", obj.Routed.body.enum)

        if obj.enum == 'Handshake':
            obj.Handshake.listen_port += 100

        decision = handler(obj, addr)

        if decision == True and obj.enum == 'Handshake':
            decision = obj

        if type(decision) != bool:
            decision = BinarySerializer(schema).serialize(decision)

        return decision

    except:
        if raw_msg[0] == 13:
            print("ERROR 13", int(raw_msg[1]), addr)
        else:
            print("ERROR", int(raw_msg[0]), addr)

        raise

    return True


